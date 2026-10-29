use crate::{
    config::DatabaseConfig,
    store::{invalid, Error, Store},
};
use clap::Subcommand;
use rusqlite::{types::Value, Connection, OpenFlags};
use sha2::{Digest, Sha256};
use sqlx::{MySql, QueryBuilder, Row};
use std::{
    collections::BTreeMap,
    io::Read,
    path::{Path, PathBuf},
};

#[derive(Subcommand)]
pub enum DbCommand {
    /// Apply versioned schema migrations
    Migrate,
    /// Check schema and search configuration
    Check,
    /// Import an offline SQLite backup, resuming the same source if interrupted
    ImportSqlite {
        #[arg(long)]
        source: PathBuf,
    },
    /// Compare every base-table value with an offline SQLite backup
    ValidateSqlite {
        #[arg(long)]
        source: PathBuf,
    },
    /// Recompute catalog totals while services are stopped
    Reconcile,
    /// Rebuild derived ngram documents while services are stopped
    RebuildSearch,
    /// Refresh optimizer and full-text statistics after restoring a dump
    Analyze,
}
pub async fn run(config: &DatabaseConfig, command: DbCommand) -> Result<(), Error> {
    let store = Store::connect_with_timeout(config, false, 0).await?;
    match command {
        DbCommand::Migrate => store.migrate().await?,
        DbCommand::Check => store.check().await?,
        DbCommand::ImportSqlite { source } => import(&store, &source).await?,
        DbCommand::ValidateSqlite { source } => validate(&store, &source).await?,
        DbCommand::Reconcile => {
            store.check().await?;
            store.reconcile().await?;
        }
        DbCommand::RebuildSearch => {
            store.check().await?;
            store.rebuild_search_index().await?;
        }
        DbCommand::Analyze => {
            store.check().await?;
            store.analyze().await?;
        }
    }
    println!("database operation completed");
    Ok(())
}
struct Table {
    name: &'static str,
    columns: &'static str,
    keys: &'static str,
}
const TABLES:&[Table]=&[
    Table{name:"torrents",columns:"info_hash:t,name:t,piece_length:i,total_size:i,file_count:i,source:t,first_seen:i,last_seen:i,last_attempt:i,metadata_complete:i",keys:"info_hash"},
    Table{name:"files",columns:"id:i,torrent_id:t,path:t,size:i",keys:"id"},
    Table{name:"crawl_stats",columns:"id:i,timestamp:i,nodes_known:i,queries_sent:i,info_hashes_found:i,metadata_fetched:i",keys:"id"},
    Table{name:"metadata_peer_attempts",columns:"info_hash:t,peer_addr:t,last_attempt:i,last_error:t",keys:"info_hash,peer_addr"},
    Table{name:"metadata_failure_counts",columns:"reason:t,count:i",keys:"reason"},
    Table{name:"dht_nodes",columns:"id:t,addr:t,last_seen:i",keys:"id"},
    Table{name:"hash_jobs",columns:"info_hash:t,status:t,attempt_count:i,next_attempt_at:i,last_failure:t,last_success_at:i,last_new_peer_at:i,discovery_sources:t,updated_at:i",keys:"info_hash"},
    Table{name:"sample_observations",columns:"info_hash:t,first_seen:i,last_seen:i,sample_count:i,sampling_node_id:t,sampling_addr:t,sweep_id:i",keys:"info_hash"},
    Table{name:"app_state",columns:"key:t,value:b",keys:"key"},
    Table{name:"hash_job_peers",columns:"info_hash:t,peer_addr:t,last_seen:i",keys:"info_hash,peer_addr"},
];
impl Table {
    fn columns(&self) -> Vec<(&str, &str)> {
        self.columns
            .split(',')
            .map(|s| s.split_once(':').expect("static column declaration"))
            .collect()
    }
    fn projection(&self, mysql: bool) -> String {
        self.columns()
            .iter()
            .map(|(name, kind)| {
                if mysql && *kind == "i" {
                    format!("CAST(`{name}` AS SIGNED) AS `{name}`")
                } else {
                    format!("`{name}`")
                }
            })
            .collect::<Vec<_>>()
            .join(",")
    }
    fn key_indices(&self) -> Vec<usize> {
        self.keys
            .split(',')
            .map(|k| {
                self.columns()
                    .iter()
                    .position(|(n, _)| *n == k)
                    .expect("static key exists")
            })
            .collect()
    }
}
fn source_connection(path: &Path) -> Result<Connection, Error> {
    for suffix in ["-wal", "-journal"] {
        let sidecar = PathBuf::from(format!("{}{suffix}", path.display()));
        if sidecar.metadata().is_ok_and(|m| m.len() > 0) {
            return Err(invalid(
                "source has an active journal/WAL; use a standalone SQLite .backup snapshot",
            ));
        }
    }
    let conn = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY)
        .map_err(|e| invalid(e.to_string()))?;
    let version: i64 = conn
        .pragma_query_value(None, "user_version", |r| r.get(0))
        .map_err(|e| invalid(e.to_string()))?;
    if version != 4 {
        return Err(invalid(format!(
            "expected SQLite schema 4, found {version}"
        )));
    }
    Ok(conn)
}
async fn fingerprint(path: &Path) -> Result<String, Error> {
    let path = path.to_owned();
    tokio::task::spawn_blocking(move || {
        let conn = source_connection(&path)?;
        let check: String = conn
            .query_row("PRAGMA quick_check", [], |r| r.get(0))
            .map_err(|e| invalid(e.to_string()))?;
        if check != "ok" {
            return Err(invalid(format!("SQLite health check failed: {check}")));
        }
        let mut fk = conn
            .prepare("PRAGMA foreign_key_check")
            .map_err(|e| invalid(e.to_string()))?;
        if fk.exists([]).map_err(|e| invalid(e.to_string()))? {
            return Err(invalid("SQLite foreign key violations"));
        }
        let mut file = std::fs::File::open(path)?;
        let mut hash = Sha256::new();
        let mut buffer = [0u8; 65536];
        loop {
            let n = file.read(&mut buffer)?;
            if n == 0 {
                break;
            }
            hash.update(&buffer[..n]);
        }
        Ok(hex::encode(hash.finalize()))
    })
    .await
    .map_err(|e| invalid(e.to_string()))?
}
async fn batch(path: &Path, index: usize, after: i64) -> Result<Vec<(i64, Vec<Value>)>, Error> {
    let path = path.to_owned();
    tokio::task::spawn_blocking(move || {
        let conn = source_connection(&path)?;
        let table = &TABLES[index];
        let mut stmt = conn
            .prepare(&format!(
                "SELECT rowid,{} FROM `{}` WHERE rowid>? ORDER BY rowid LIMIT 128",
                table.projection(false),
                table.name
            ))
            .map_err(|e| invalid(e.to_string()))?;
        let rows = stmt
            .query_map([after], |r| {
                Ok((
                    r.get::<_, i64>(0)?,
                    (0..table.columns().len())
                        .map(|i| r.get::<_, Value>(i + 1))
                        .collect::<rusqlite::Result<Vec<_>>>()?,
                ))
            })
            .map_err(|e| invalid(e.to_string()))?;
        let mut out = Vec::new();
        let mut bytes = 0;
        for row in rows {
            let row = row.map_err(|e| invalid(e.to_string()))?;
            for ((_, kind), v) in table.columns().iter().zip(&row.1) {
                if !matches!(
                    (v, *kind),
                    (Value::Null, _)
                        | (Value::Integer(_), "i")
                        | (Value::Text(_), "t")
                        | (Value::Blob(_), "b")
                ) {
                    return Err(invalid(format!(
                        "invalid SQLite column type in {}",
                        table.name
                    )));
                }
            }
            bytes += row
                .1
                .iter()
                .map(|v| match v {
                    Value::Text(s) => s.len(),
                    Value::Blob(b) => b.len(),
                    _ => 8,
                })
                .sum::<usize>();
            out.push(row);
            if bytes >= 1024 * 1024 {
                break;
            }
        }
        Ok(out)
    })
    .await
    .map_err(|e| invalid(e.to_string()))?
}
fn bind(q: &mut QueryBuilder<'_, MySql>, v: &Value) {
    match v {
        Value::Null => {
            q.push_bind(Option::<String>::None);
        }
        Value::Integer(n) => {
            q.push_bind(*n);
        }
        Value::Text(s) => {
            q.push_bind(s.clone());
        }
        Value::Blob(b) => {
            q.push_bind(b.clone());
        }
        Value::Real(_) => unreachable!("source types are validated"),
    }
}
pub async fn import(store: &Store, path: &Path) -> Result<(), Error> {
    store.check_schema().await?;
    let fp = fingerprint(path).await?;
    let mut guard = store.pool.acquire().await?.detach();
    let locked: Option<i64> = sqlx::query_scalar("SELECT GET_LOCK(CONCAT(DATABASE(),':import'),0)")
        .fetch_one(&mut guard)
        .await?;
    if locked != Some(1) {
        return Err(invalid("another import is running"));
    }
    let result = import_locked(store, path, &fp).await;
    let unlock = sqlx::query("SELECT RELEASE_LOCK(CONCAT(DATABASE(),':import'))")
        .execute(&mut guard)
        .await;
    if let Err(e) = unlock {
        tracing::warn!("failed to release import lock: {e}");
    }
    result
}
async fn import_locked(store: &Store, path: &Path, fp: &str) -> Result<(), Error> {
    let markers: Vec<(String, String)> =
        sqlx::query_as("SELECT table_name,fingerprint FROM import_progress")
            .fetch_all(&store.pool)
            .await?;
    if markers.iter().any(|(_, f)| f != fp) {
        return Err(invalid("target belongs to a different SQLite snapshot"));
    }
    let completed: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM import_progress WHERE table_name='__derived' AND finished=1",
    )
    .fetch_one(&store.pool)
    .await?;
    if completed == 1 {
        tracing::info!("snapshot was already imported; no changes made");
        return Ok(());
    }
    if markers.is_empty() {
        for table in TABLES {
            let count: i64 = sqlx::query_scalar(&format!("SELECT COUNT(*) FROM `{}`", table.name))
                .fetch_one(&store.pool)
                .await?;
            if count != 0 {
                return Err(invalid("target is not empty; use a new database"));
            }
        }
        let mut tx = store.pool.begin().await?;
        for name in TABLES.iter().map(|t| t.name).chain(["__derived"]) {
            sqlx::query("INSERT INTO import_progress VALUES (?,?,?,0)")
                .bind(name)
                .bind(fp)
                .bind(i64::MIN)
                .execute(&mut *tx)
                .await?;
        }
        tx.commit().await?;
    }
    for (index, table) in TABLES.iter().enumerate() {
        let (mut after, finished): (i64, bool) =
            sqlx::query_as("SELECT last_rowid,finished FROM import_progress WHERE table_name=?")
                .bind(table.name)
                .fetch_one(&store.pool)
                .await?;
        if finished {
            continue;
        }
        loop {
            let rows = batch(path, index, after).await?;
            if rows.is_empty() {
                sqlx::query("UPDATE import_progress SET finished=1 WHERE table_name=?")
                    .bind(table.name)
                    .execute(&store.pool)
                    .await?;
                break;
            }
            let mut tx = store.pool.begin().await?;
            for (_, values) in &rows {
                let mut q = QueryBuilder::<MySql>::new(format!(
                    "INSERT INTO `{}` ({}) VALUES (",
                    table.name,
                    table.projection(false)
                ));
                for (i, v) in values.iter().enumerate() {
                    if i > 0 {
                        q.push(",");
                    }
                    bind(&mut q, v);
                }
                q.push(")");
                q.build().execute(&mut *tx).await?;
            }
            after = rows.last().expect("nonempty batch").0;
            sqlx::query("UPDATE import_progress SET last_rowid=? WHERE table_name=?")
                .bind(after)
                .bind(table.name)
                .execute(&mut *tx)
                .await?;
            tx.commit().await?;
            tracing::info!(table = table.name, last_rowid = after, "import checkpoint");
        }
    }
    let source = path.to_owned();
    let sequences: Vec<(String, i64)> = tokio::task::spawn_blocking(move || {
        let conn = source_connection(&source)?;
        let mut stmt = conn
            .prepare("SELECT name,seq FROM sqlite_sequence WHERE name IN ('files','crawl_stats')")
            .map_err(|e| invalid(e.to_string()))?;
        let rows = stmt
            .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))
            .map_err(|e| invalid(e.to_string()))?;
        rows.collect::<rusqlite::Result<Vec<_>>>()
            .map_err(|e| invalid(e.to_string()))
    })
    .await
    .map_err(|e| invalid(e.to_string()))??;
    for (table, sequence) in sequences {
        let next = sequence
            .checked_add(1)
            .filter(|n| *n > 0)
            .ok_or_else(|| invalid("invalid SQLite auto-increment sequence"))?;
        sqlx::query(&format!("ALTER TABLE `{table}` AUTO_INCREMENT={next}"))
            .execute(&store.pool)
            .await?;
    }
    store.rebuild_search_index().await?;
    store.reconcile().await?;
    validate(store, path).await?;
    if fingerprint(path).await? != fp {
        return Err(invalid("source snapshot changed during import"));
    }
    sqlx::query("UPDATE import_progress SET finished=1 WHERE table_name='__derived'")
        .execute(&store.pool)
        .await?;
    Ok(())
}
fn canonical(values: &[Value]) -> Vec<u8> {
    let mut out = Vec::new();
    for v in values {
        let (tag, bytes) = match v {
            Value::Null => (0, Vec::new()),
            Value::Integer(n) => (1, n.to_be_bytes().to_vec()),
            Value::Text(s) => (2, s.as_bytes().to_vec()),
            Value::Blob(b) => (3, b.clone()),
            Value::Real(_) => unreachable!("validated types"),
        };
        out.push(tag);
        out.extend_from_slice(&(bytes.len() as u64).to_be_bytes());
        out.extend(bytes);
    }
    out
}
pub async fn validate(store: &Store, path: &Path) -> Result<(), Error> {
    store.check_schema().await?;
    let fp = fingerprint(path).await?;
    let markers: Vec<String> =
        sqlx::query_scalar("SELECT DISTINCT fingerprint FROM import_progress")
            .fetch_all(&store.pool)
            .await?;
    if markers != [fp.clone()] {
        return Err(invalid(
            "source fingerprint does not match the imported snapshot",
        ));
    }
    for (index, table) in TABLES.iter().enumerate() {
        let mut after = i64::MIN;
        let mut count = 0i64;
        let mut source_hash = Sha256::new();
        let mut target_hash = Sha256::new();
        loop {
            let rows = batch(path, index, after).await?;
            if rows.is_empty() {
                break;
            }
            let keys = table.key_indices();
            let mut q = QueryBuilder::<MySql>::new(format!(
                "SELECT {} FROM `{}` WHERE ",
                table.projection(true),
                table.name
            ));
            for (n, (_, values)) in rows.iter().enumerate() {
                if n > 0 {
                    q.push(" OR ");
                }
                q.push("(");
                for (k, i) in keys.iter().enumerate() {
                    if k > 0 {
                        q.push(" AND ");
                    }
                    q.push(format!("`{}`=", table.columns()[*i].0));
                    bind(&mut q, &values[*i]);
                }
                q.push(")");
            }
            let mut targets = BTreeMap::new();
            for row in q.build().fetch_all(&store.pool).await? {
                let mut values = Vec::new();
                for (i, (_, kind)) in table.columns().iter().enumerate() {
                    values.push(match *kind {
                        "i" => row
                            .try_get::<Option<i64>, _>(i)?
                            .map_or(Value::Null, Value::Integer),
                        "b" => row
                            .try_get::<Option<Vec<u8>>, _>(i)?
                            .map_or(Value::Null, Value::Blob),
                        _ => row
                            .try_get::<Option<String>, _>(i)?
                            .map_or(Value::Null, Value::Text),
                    });
                }
                targets.insert(
                    canonical(&keys.iter().map(|i| values[*i].clone()).collect::<Vec<_>>()),
                    values,
                );
            }
            for (_, values) in &rows {
                let key = canonical(&keys.iter().map(|i| values[*i].clone()).collect::<Vec<_>>());
                let target = targets
                    .get(&key)
                    .ok_or_else(|| invalid(format!("missing row in {}", table.name)))?;
                source_hash.update(canonical(values));
                target_hash.update(canonical(target));
                if values != target {
                    return Err(invalid(format!("value mismatch in {}", table.name)));
                }
            }
            count += rows.len() as i64;
            after = rows.last().expect("nonempty batch").0;
        }
        let target_count: i64 =
            sqlx::query_scalar(&format!("SELECT COUNT(*) FROM `{}`", table.name))
                .fetch_one(&store.pool)
                .await?;
        if count != target_count {
            return Err(invalid(format!("row count mismatch in {}", table.name)));
        }
        let source_digest = source_hash.finalize();
        let target_digest = target_hash.finalize();
        if source_digest != target_digest {
            return Err(invalid("checksum mismatch"));
        }
        tracing::info!(table=table.name,count,sha256=%hex::encode(source_digest),"validated");
    }
    let inconsistent:i64=sqlx::query_scalar("SELECT COUNT(*) FROM torrents t WHERE metadata_complete=1 AND (file_count<>(SELECT COUNT(*) FROM files f WHERE f.torrent_id=t.info_hash) OR total_size<>(SELECT COALESCE(SUM(size),0) FROM files f WHERE f.torrent_id=t.info_hash))").fetch_one(&store.pool).await?;
    if inconsistent > 0 {
        return Err(invalid(format!(
            "{inconsistent} complete torrents have inconsistent file counts/sizes"
        )));
    }
    let expected:crate::store::CrawlStats=sqlx::query_as("SELECT COUNT(*) AS total_torrents,CAST(COALESCE(SUM(metadata_complete),0) AS SIGNED) AS total_metadata_complete,(SELECT COUNT(*) FROM files) AS total_files,CAST(COALESCE(SUM(total_size),0) AS SIGNED) AS total_size FROM torrents").fetch_one(&store.pool).await?;
    if expected != store.get_stats().await? {
        return Err(invalid("aggregate counters differ"));
    }
    if fingerprint(path).await? != fp {
        return Err(invalid("snapshot changed during validation"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    struct Snapshot(PathBuf);
    impl Drop for Snapshot {
        fn drop(&mut self) {
            if let Err(e) = std::fs::remove_file(&self.0) {
                eprintln!("temporary snapshot cleanup: {e}");
            }
        }
    }
    fn snapshot(count: u32) -> Snapshot {
        let path =
            std::env::temp_dir().join(format!("btfind-snapshot-{}.db", uuid::Uuid::new_v4()));
        let conn = Connection::open(&path).unwrap();
        conn.execute_batch(include_str!("fixtures/sqlite-v4.sql"))
            .unwrap();
        for n in 0..count {
            conn.execute("INSERT INTO torrents(info_hash,name,total_size,file_count,source,first_seen,last_seen,metadata_complete) VALUES (?1,'東京 中文',1,1,'dht',1,2,1)",[format!("{n:040x}")]).unwrap();
            conn.execute(
                "INSERT INTO files(torrent_id,path,size) VALUES (?1,'中文/東京.iso',1)",
                [format!("{n:040x}")],
            )
            .unwrap();
        }
        conn.execute_batch("INSERT INTO crawl_stats VALUES (7,100,1,2,3,4);
            INSERT INTO metadata_peer_attempts VALUES ('0000000000000000000000000000000000000000','1.1.1.1:6881',123,'timeout');
            INSERT INTO metadata_failure_counts VALUES ('connect',7);
            INSERT INTO dht_nodes VALUES ('1111111111111111111111111111111111111111','8.8.8.8:6881',111);
            INSERT INTO hash_jobs(info_hash,status,attempt_count,next_attempt_at,last_failure,updated_at) VALUES ('0000000000000000000000000000000000000000','retry_at',2,888,'timeout',100);
            INSERT INTO sample_observations(info_hash,first_seen,last_seen,sample_count) VALUES ('0000000000000000000000000000000000000000',1,2,3);
            INSERT INTO app_state VALUES ('node_id',X'0101010101010101010101010101010101010101');
            INSERT INTO hash_job_peers VALUES ('0000000000000000000000000000000000000000','1.1.1.1:6881',99);").unwrap();
        conn.execute("UPDATE sqlite_sequence SET seq=900 WHERE name='files'", [])
            .unwrap();
        conn.close().unwrap();
        Snapshot(path)
    }
    #[test]
    fn snapshot_types_and_canonical_values() {
        let file = snapshot(1);
        assert!(source_connection(&file.0).is_ok());
        assert_ne!(
            canonical(&[Value::Text("1".into())]),
            canonical(&[Value::Integer(1)])
        );
        assert_ne!(
            canonical(&[Value::Null]),
            canonical(&[Value::Text("".into())])
        );
    }
    #[tokio::test]
    #[ignore = "requires BTFIND_TEST_DATABASE_URL; run scripts/test-mysql.sh"]
    async fn mysql_import_resume_and_validation() {
        let file = snapshot(130);
        let store = Store::open_test_database().await.unwrap();
        store.execute_batch_for_test("CREATE TRIGGER stop_import BEFORE INSERT ON torrents FOR EACH ROW BEGIN IF NEW.info_hash='0000000000000000000000000000000000000080' THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='test interruption'; END IF; END").await.unwrap();
        assert!(import(&store, &file.0).await.is_err());
        let imported: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM torrents")
            .fetch_one(&store.pool)
            .await
            .unwrap();
        assert_eq!(imported, 128);
        assert!(store.check().await.is_err());
        let wrong = snapshot(1);
        assert!(import(&store, &wrong.0)
            .await
            .unwrap_err()
            .to_string()
            .contains("different"));
        store
            .execute_batch_for_test("DROP TRIGGER stop_import")
            .await
            .unwrap();
        import(&store, &file.0).await.unwrap();
        store.check().await.unwrap();
        validate(&store, &file.0).await.unwrap();
        assert_eq!(store.get_stats().await.unwrap().total_torrents, 130);
        assert_eq!(store.load_or_create_node_id().await.unwrap(), [1; 20]);
        assert_eq!(store.hash_job_peers(&[0; 20], 10).await.unwrap().len(), 1);
        assert_eq!(
            store
                .search_torrents("東京", 500, "rank")
                .await
                .unwrap()
                .len(),
            130
        );
        store.execute_batch_for_test("CREATE TRIGGER no_rebuild BEFORE DELETE ON search_documents FOR EACH ROW SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='unexpected rebuild'").await.unwrap();
        import(&store, &file.0).await.unwrap();
        store
            .execute_batch_for_test("DROP TRIGGER no_rebuild")
            .await
            .unwrap();
        assert_eq!(store.get_stats().await.unwrap().total_files, 130);
        let next:u64=sqlx::query_scalar("SELECT AUTO_INCREMENT FROM information_schema.tables WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='files'").fetch_one(&store.pool).await.unwrap();
        assert!(next >= 901);
        sqlx::query("UPDATE files SET path='corrupted' WHERE id=1")
            .execute(&store.pool)
            .await
            .unwrap();
        assert!(validate(&store, &file.0)
            .await
            .unwrap_err()
            .to_string()
            .contains("mismatch"));
    }
    #[tokio::test]
    #[ignore = "requires BTFIND_TEST_DATABASE_URL; run scripts/test-mysql.sh"]
    async fn mysql_import_rejects_existing_data() {
        let file = snapshot(1);
        let store = Store::open_test_database().await.unwrap();
        store
            .persist_hash_discovery(&[1; 20], "dht", false)
            .await
            .unwrap();
        assert!(import(&store, &file.0)
            .await
            .unwrap_err()
            .to_string()
            .contains("not empty"));
    }
}
