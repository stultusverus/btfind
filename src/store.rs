use crate::types::{CrawlStatsEvent, InfoHash, NodeContact, NodeId, PeerContact};
use sqlx::{
    mysql::{MySqlConnectOptions, MySqlPoolOptions, MySqlRow, MySqlSslMode},
    MySql, MySqlConnection, MySqlPool, QueryBuilder, Row,
};
use std::{
    collections::BTreeMap,
    net::SocketAddrV4,
    str::FromStr,
    time::{Duration, Instant},
};

use futures_util::TryStreamExt;
use unicode_normalization::UnicodeNormalization;

pub type Error = sqlx::Error;
pub fn invalid(message: impl Into<String>) -> Error {
    Error::Protocol(message.into())
}
pub fn now() -> i64 {
    chrono::Utc::now().timestamp()
}
pub fn decode_hash(s: &str) -> Result<InfoHash, Error> {
    hex::decode(s)
        .map_err(|_| invalid("invalid stored hash"))?
        .try_into()
        .map_err(|_| invalid("invalid stored hash length"))
}
#[derive(Debug)]
pub struct TorrentRecord {
    pub info_hash: InfoHash,
    pub name: Option<String>,
    #[allow(dead_code)]
    pub piece_length: Option<i64>,
    pub total_size: i64,
    pub file_count: i64,
    #[allow(dead_code)]
    pub source: String,
    pub first_seen: i64,
    pub last_seen: i64,
    #[allow(dead_code)]
    pub last_attempt: i64,
    pub metadata_complete: bool,
}
#[derive(Debug, serde::Serialize, sqlx::FromRow, PartialEq, Eq)]
pub struct CrawlStats {
    pub total_torrents: i64,
    pub total_metadata_complete: i64,
    pub total_files: i64,
    pub total_size: i64,
}
#[derive(Debug, sqlx::FromRow)]
#[allow(dead_code)]
pub struct CrawlStatRecord {
    pub id: i64,
    pub timestamp: i64,
    pub nodes_known: Option<i64>,
    pub queries_sent: Option<i64>,
    pub info_hashes_found: Option<i64>,
    pub metadata_fetched: Option<i64>,
}
#[derive(Debug, PartialEq, Eq, sqlx::FromRow)]
pub struct MetadataFailureCount {
    pub reason: String,
    pub count: i64,
}
#[derive(Debug, Clone)]
pub struct HashJobRecord {
    pub info_hash: InfoHash,
    pub status: String,
    pub attempt_count: u32,
    pub next_attempt_at: i64,
    pub last_failure: Option<String>,
}
#[derive(Debug, Clone, Copy)]
pub struct RuntimeSnapshot {
    pub nodes_known: i64,
    pub queries_sent: i64,
    pub info_hashes_found: i64,
    pub metadata_fetched: i64,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PeerRetryEligibility {
    Eligible,
    RetryAt(i64),
    Rejected,
}
#[derive(Clone, Debug)]
pub struct Store {
    pub(crate) pool: MySqlPool,
}
#[derive(Debug)]
pub struct TorrentSearchPage {
    pub results: Vec<TorrentRecord>,
    pub has_next: bool,
}
#[derive(Debug)]
pub struct FileSearchPage {
    pub results: Vec<(String, i64)>,
    pub has_next: bool,
}
#[derive(Clone, Copy, Debug, Default)]
pub struct TorrentSearchFilters {
    pub complete_only: bool,
    pub min_size: Option<i64>,
    pub max_size: Option<i64>,
}

impl Store {
    pub async fn connect(config: &crate::config::DatabaseConfig, web: bool) -> Result<Self, Error> {
        Self::connect_with_timeout(config, web, 9000).await
    }
    pub(crate) async fn connect_with_timeout(
        config: &crate::config::DatabaseConfig,
        web: bool,
        select_timeout_ms: u32,
    ) -> Result<Self, Error> {
        let url = std::env::var("BTFIND_DATABASE_URL")
            .map_err(|_| invalid("set BTFIND_DATABASE_URL to a MySQL connection URL"))?;
        let mut options = MySqlConnectOptions::from_str(&url)
            .map_err(|_| invalid("invalid BTFIND_DATABASE_URL"))?;
        options = options.ssl_mode(if config.allow_insecure_local {
            MySqlSslMode::Disabled
        } else {
            MySqlSslMode::VerifyIdentity
        });
        if config.allow_insecure_local
            && !matches!(options.get_host(), "127.0.0.1" | "localhost" | "::1")
        {
            return Err(invalid(
                "allow_insecure_local is restricted to loopback hosts",
            ));
        }
        if let Some(ca) = &config.tls_ca {
            options = options.ssl_ca(ca);
        }
        Self::connect_options(
            options,
            if web {
                config.web_pool_size
            } else {
                config.pool_size
            },
            select_timeout_ms,
        )
        .await
    }
    async fn connect_options(
        options: MySqlConnectOptions,
        size: u32,
        select_timeout_ms: u32,
    ) -> Result<Self, Error> {
        let pool = MySqlPoolOptions::new()
            .max_connections(size)
            .acquire_timeout(Duration::from_secs(5))
            .after_connect(move |conn, _| {
                Box::pin(async move {
                    sqlx::query(&format!(
                        "SET SESSION max_execution_time={select_timeout_ms}"
                    ))
                    .execute(&mut *conn)
                    .await?;
                    for statement in [
                        "SET SESSION innodb_lock_wait_timeout=5",
                        "SET SESSION innodb_ft_enable_stopword=OFF",
                        "SET SESSION time_zone='+00:00'",
                    ] {
                        sqlx::query(statement).execute(&mut *conn).await?;
                    }
                    Ok(())
                })
            })
            .connect_with(options)
            .await?;
        Ok(Self { pool })
    }
    pub async fn migrate(&self) -> Result<(), Error> {
        self.check_parser().await?;
        sqlx::migrate!()
            .run(&self.pool)
            .await
            .map_err(|e| invalid(e.to_string()))
    }
    async fn check_parser(&self) -> Result<(), Error> {
        let size: i64 = sqlx::query_scalar("SELECT @@ngram_token_size")
            .fetch_one(&self.pool)
            .await?;
        if size != 2 {
            return Err(invalid("ngram_token_size must be 2"));
        }
        Ok(())
    }
    pub(crate) async fn check_schema(&self) -> Result<(), Error> {
        self.check_parser().await?;
        let rows =
            sqlx::query("SELECT version, checksum, success FROM _sqlx_migrations ORDER BY version")
                .fetch_all(&self.pool)
                .await?;
        let migrations = sqlx::migrate!();
        if rows.len() != migrations.iter().count() {
            return Err(invalid(
                "schema mismatch; run btfind db migrate with the matching binary",
            ));
        }
        for (row, migration) in rows.iter().zip(migrations.iter()) {
            if row.try_get::<i64, _>("version")? != migration.version
                || !row.try_get::<bool, _>("success")?
                || row.try_get::<Vec<u8>, _>("checksum")?.as_slice() != migration.checksum.as_ref()
            {
                return Err(invalid(
                    "schema migration checksum mismatch or incomplete migration",
                ));
            }
        }
        Ok(())
    }
    pub async fn check(&self) -> Result<(), Error> {
        self.check_schema().await
    }
    #[cfg(test)]
    pub async fn get_torrent(&self, hash: &InfoHash) -> Result<Option<TorrentRecord>, Error> {
        self.get_torrent_by_hex(&hex::encode(hash)).await
    }
    pub async fn get_torrent_by_hex(&self, hash: &str) -> Result<Option<TorrentRecord>, Error> {
        sqlx::query("SELECT * FROM torrents WHERE info_hash=?")
            .bind(hash.to_ascii_lowercase())
            .fetch_optional(&self.pool)
            .await?
            .as_ref()
            .map(row_to_torrent)
            .transpose()
    }
    pub async fn get_stats(&self) -> Result<CrawlStats, Error> {
        sqlx::query_as("SELECT total_torrents,total_metadata_complete,total_files,total_size FROM catalog_totals WHERE id=1").fetch_one(&self.pool).await
    }
    pub async fn commit_metadata(
        &self,
        hash: &InfoHash,
        name: &str,
        piece_length: i64,
        total_size: i64,
        files: &[(String, i64)],
    ) -> Result<(), Error> {
        if files.iter().any(|(p, s)| p.is_empty() || *s < 0) || total_size < 0 || piece_length < 0 {
            return Err(invalid("invalid metadata file or size"));
        }
        let count = i64::try_from(files.len()).map_err(|_| invalid("too many files"))?;
        let h = hex::encode(hash);
        for attempt in 0..4 {
            let result = async {
                let mut tx = self.pool.begin().await?;
                lock_totals(&mut tx).await?;
                discover(&mut tx, hash, "dht", false).await?;
                let old = sqlx::query("SELECT total_size,metadata_complete FROM torrents WHERE info_hash=? FOR UPDATE").bind(&h).fetch_one(&mut *tx).await?;
                let deleted = sqlx::query("DELETE FROM files WHERE torrent_id=?").bind(&h).execute(&mut *tx).await?.rows_affected() as i64;
                sqlx::query("UPDATE torrents SET name=?,piece_length=?,total_size=?,file_count=?,last_seen=?,metadata_complete=1 WHERE info_hash=?")
                    .bind(name).bind(piece_length).bind(total_size).bind(count).bind(now()).bind(&h).execute(&mut *tx).await?;
                for chunk in files.chunks(128) {
                    let mut q = QueryBuilder::<MySql>::new("INSERT INTO files(torrent_id,path,size) ");
                    q.push_values(chunk, |mut b,(p,s)| { b.push_bind(&h).push_bind(p).push_bind(*s); });
                    q.build().execute(&mut *tx).await?;
                }
                refresh_search(&mut tx, &h).await?;
                sqlx::query("UPDATE hash_jobs SET status='complete',last_failure=NULL,last_success_at=?,updated_at=? WHERE info_hash=?").bind(now()).bind(now()).bind(&h).execute(&mut *tx).await?;
                sqlx::query("UPDATE catalog_totals SET total_metadata_complete=total_metadata_complete+?,total_files=total_files+?,total_size=total_size+? WHERE id=1")
                    .bind(i64::from(!old.try_get::<bool,_>("metadata_complete")?)).bind(count-deleted).bind(total_size-old.try_get::<i64,_>("total_size")?).execute(&mut *tx).await?;
                tx.commit().await
            }.await;
            match result {
                Err(e) if retryable(&e) && attempt < 3 => backoff(attempt).await,
                other => return other,
            }
        }
        unreachable!("bounded retry loop returns")
    }
    pub async fn persist_hash_discovery(
        &self,
        hash: &InfoHash,
        source: &str,
        peers: &[PeerContact],
        limit: usize,
    ) -> Result<(), Error> {
        let addresses: Vec<String> = peers
            .iter()
            .take(limit)
            .map(|p| p.addr.to_string())
            .collect();
        let h = hex::encode(hash);
        let mut tx = self.pool.begin().await?;
        lock_totals(&mut tx).await?;
        discover(&mut tx, hash, source, !addresses.is_empty()).await?;
        for chunk in addresses.chunks(128) {
            let mut insert = QueryBuilder::<MySql>::new(
                "INSERT INTO hash_job_peers(info_hash,peer_addr,last_seen) ",
            );
            insert.push_values(chunk, |mut row, addr| {
                row.push_bind(&h).push_bind(addr).push_bind(now());
            });
            insert.push(" ON DUPLICATE KEY UPDATE last_seen=VALUES(last_seen)");
            insert.build().execute(&mut *tx).await?;
        }
        let excess: Vec<String> = sqlx::query_scalar("SELECT peer_addr FROM hash_job_peers WHERE info_hash=? ORDER BY last_seen DESC,peer_addr LIMIT 18446744073709551615 OFFSET ?")
            .bind(&h).bind(limit as u64).fetch_all(&mut *tx).await?;
        for chunk in excess.chunks(128) {
            let mut delete =
                QueryBuilder::<MySql>::new("DELETE FROM hash_job_peers WHERE info_hash=");
            delete.push_bind(&h).push(" AND peer_addr IN (");
            let mut values = delete.separated(",");
            for addr in chunk {
                values.push_bind(addr);
            }
            delete.push(")");
            delete.build().execute(&mut *tx).await?;
        }
        tx.commit().await
    }
    pub async fn persist_runtime_batch(
        &self,
        events: &[CrawlStatsEvent],
        snapshot: Option<RuntimeSnapshot>,
        receipt: &str,
    ) -> Result<(), Error> {
        let mut tx = self.pool.begin().await?;
        lock_totals(&mut tx).await?;
        let exists: Option<String> =
            sqlx::query_scalar("SELECT id FROM persistence_receipts WHERE id=?")
                .bind(receipt)
                .fetch_optional(&mut *tx)
                .await?;
        if exists.is_some() {
            tx.rollback().await?;
            return Ok(());
        }
        sqlx::query("INSERT INTO persistence_receipts(id,created_at) VALUES (?,?)")
            .bind(receipt)
            .bind(now())
            .execute(&mut *tx)
            .await?;
        let mut nodes = BTreeMap::new();
        for event in events {
            match event {
                CrawlStatsEvent::MetadataFetchFailed { reason } => {
                    sqlx::query("INSERT INTO metadata_failure_counts(reason,count) VALUES (?,1) ON DUPLICATE KEY UPDATE count=count+1").bind(reason.as_str()).execute(&mut *tx).await?;
                }
                CrawlStatsEvent::DhtNodeSeen { id, addr } => {
                    nodes.insert(*id, *addr);
                }
                CrawlStatsEvent::HashObserved {
                    info_hash,
                    source,
                    has_peers: false,
                } => discover(&mut tx, info_hash, source.as_str(), false).await?,
                CrawlStatsEvent::SampleObserved {
                    info_hash,
                    node_id,
                    addr,
                    round_id,
                } => {
                    discover(&mut tx, info_hash, "sample_infohashes", false).await?;
                    sqlx::query("INSERT INTO sample_observations(info_hash,first_seen,last_seen,sampling_node_id,sampling_addr,sweep_id) VALUES (?,?,?,?,?,?) ON DUPLICATE KEY UPDATE last_seen=VALUES(last_seen),sample_count=sample_count+1,sampling_node_id=VALUES(sampling_node_id),sampling_addr=VALUES(sampling_addr),sweep_id=VALUES(sweep_id)")
                        .bind(hex::encode(info_hash)).bind(now()).bind(now()).bind(hex::encode(node_id)).bind(addr.to_string()).bind(i64::try_from(*round_id).unwrap_or(i64::MAX)).execute(&mut *tx).await?;
                }
                _ => {}
            }
        }
        for (id, addr) in nodes {
            sqlx::query("INSERT INTO dht_nodes VALUES (?,?,?) ON DUPLICATE KEY UPDATE addr=VALUES(addr),last_seen=VALUES(last_seen)").bind(hex::encode(id)).bind(addr.to_string()).bind(now()).execute(&mut *tx).await?;
        }
        if let Some(s) = snapshot {
            sqlx::query("INSERT INTO crawl_stats(timestamp,nodes_known,queries_sent,info_hashes_found,metadata_fetched) VALUES (?,?,?,?,?)").bind(now()).bind(s.nodes_known).bind(s.queries_sent).bind(s.info_hashes_found).bind(s.metadata_fetched).execute(&mut *tx).await?;
        }
        tx.commit().await
    }
    pub async fn acknowledge_receipt(&self, receipt: &str) -> Result<(), Error> {
        sqlx::query("UPDATE persistence_receipts SET acknowledged_at=? WHERE id=? AND acknowledged_at IS NULL")
            .bind(now()).bind(receipt).execute(&self.pool).await?;
        Ok(())
    }
    pub async fn prune_receipts(&self) -> Result<i64, Error> {
        self.prune_rows("persistence_receipts", "acknowledged_at", now() - 7 * 86400)
            .await
    }
    pub async fn update_hash_job_failure(
        &self,
        hash: &InfoHash,
        status: &str,
        attempts: u32,
        next: i64,
        failure: &str,
    ) -> Result<(), Error> {
        sqlx::query("UPDATE hash_jobs SET status=?,attempt_count=?,next_attempt_at=?,last_failure=?,updated_at=? WHERE info_hash=? AND status<>'complete'").bind(status).bind(i64::from(attempts)).bind(next).bind(failure).bind(now()).bind(hex::encode(hash)).execute(&self.pool).await?;
        Ok(())
    }
    pub async fn hash_job_peers(
        &self,
        hash: &InfoHash,
        limit: usize,
    ) -> Result<Vec<PeerContact>, Error> {
        let addresses:Vec<String>=sqlx::query_scalar("SELECT peer_addr FROM hash_job_peers WHERE info_hash=? ORDER BY last_seen DESC,peer_addr LIMIT ?").bind(hex::encode(hash)).bind(limit as u64).fetch_all(&self.pool).await?;
        addresses
            .iter()
            .map(|a| {
                Ok(PeerContact {
                    addr: a
                        .parse()
                        .map_err(|_| invalid("invalid stored peer address"))?,
                })
            })
            .collect()
    }
    pub async fn hash_job_is_terminal(&self, hash: &InfoHash) -> Result<bool, Error> {
        let terminal: Option<bool> = sqlx::query_scalar("SELECT status IN ('complete','invalid','unsupported') FROM hash_jobs WHERE info_hash=?")
            .bind(hex::encode(hash)).fetch_optional(&self.pool).await?;
        Ok(terminal.unwrap_or(false))
    }
    pub async fn due_hash_jobs(&self, limit: usize) -> Result<Vec<HashJobRecord>, Error> {
        sqlx::query("SELECT j.* FROM hash_jobs j WHERE j.status IN ('queued','retry_at') AND j.next_attempt_at<=? AND EXISTS (SELECT 1 FROM hash_job_peers p WHERE p.info_hash=j.info_hash) ORDER BY j.next_attempt_at,j.updated_at,j.info_hash LIMIT ?").bind(now()).bind(limit as u64).fetch_all(&self.pool).await?.iter().map(|r| Ok(HashJobRecord {
            info_hash:decode_hash(r.try_get("info_hash")?)?,status:r.try_get("status")?,attempt_count:u32::try_from(r.try_get::<i64,_>("attempt_count")?).map_err(|_| invalid("invalid attempt count"))?,next_attempt_at:r.try_get("next_attempt_at")?,last_failure:r.try_get("last_failure")?
        })).collect()
    }
    pub async fn incomplete_info_hashes(&self, limit: usize) -> Result<Vec<InfoHash>, Error> {
        let hashes:Vec<String>=sqlx::query_scalar("SELECT info_hash FROM torrents WHERE metadata_complete=0 ORDER BY last_seen DESC,info_hash DESC LIMIT ?").bind(limit as u64).fetch_all(&self.pool).await?;
        hashes.iter().map(|h| decode_hash(h)).collect()
    }
    pub async fn load_or_create_node_id(&self) -> Result<NodeId, Error> {
        let id = crate::types::random_node_id();
        sqlx::query("INSERT INTO app_state (`key`,value) VALUES ('node_id',?) ON DUPLICATE KEY UPDATE `key`=`key`").bind(id.as_slice()).execute(&self.pool).await?;
        let bytes: Vec<u8> =
            sqlx::query_scalar("SELECT value FROM app_state WHERE `key`='node_id'")
                .fetch_one(&self.pool)
                .await?;
        bytes
            .try_into()
            .map_err(|_| invalid("stored node_id has an invalid length"))
    }
    pub async fn recent_dht_nodes(
        &self,
        age: Duration,
        limit: usize,
    ) -> Result<Vec<NodeContact>, Error> {
        sqlx::query("SELECT id,addr,last_seen FROM dht_nodes WHERE last_seen>=? ORDER BY last_seen DESC,id LIMIT ?").bind(now()-age.as_secs() as i64).bind(limit as u64).fetch_all(&self.pool).await?.iter().map(|r| Ok(NodeContact {
            id:decode_hash(r.try_get("id")?)?, addr:r.try_get::<&str,_>("addr")?.parse::<SocketAddrV4>().map_err(|_| invalid("invalid stored node address"))?, last_seen:Instant::now(), last_seen_unix:r.try_get("last_seen")?,
        })).collect()
    }
    async fn prune_rows(&self, table: &str, field: &str, cutoff: i64) -> Result<i64, Error> {
        let mut total = 0;
        loop {
            let n = sqlx::query(&format!("DELETE FROM {table} WHERE {field}<? LIMIT 500"))
                .bind(cutoff)
                .execute(&self.pool)
                .await?
                .rows_affected();
            total += n as i64;
            if n < 500 {
                return Ok(total);
            }
            tokio::task::yield_now().await;
        }
    }
    pub async fn prune_stale_dht_nodes(&self, age: Duration) -> Result<i64, Error> {
        self.prune_rows("dht_nodes", "last_seen", now() - age.as_secs() as i64)
            .await
    }
    pub async fn prune_stale_peer_attempts(&self, age: Duration) -> Result<i64, Error> {
        self.prune_rows(
            "metadata_peer_attempts",
            "last_attempt",
            now() - age.as_secs() as i64,
        )
        .await
    }
    pub async fn set_peer_attempt(
        &self,
        hash: &InfoHash,
        addr: &str,
        error: Option<&str>,
    ) -> Result<(), Error> {
        let mut tx = self.pool.begin().await?;
        let h = hex::encode(hash);
        sqlx::query("UPDATE torrents SET last_attempt=? WHERE info_hash=?")
            .bind(now())
            .bind(&h)
            .execute(&mut *tx)
            .await?;
        sqlx::query("INSERT INTO metadata_peer_attempts VALUES (?,?,?,?) ON DUPLICATE KEY UPDATE last_attempt=VALUES(last_attempt),last_error=VALUES(last_error)").bind(&h).bind(addr).bind(now()).bind(error).execute(&mut *tx).await?;
        tx.commit().await
    }
    pub async fn peer_retry_eligibility(
        &self,
        hash: &InfoHash,
        addr: &str,
        hours: u32,
    ) -> Result<PeerRetryEligibility, Error> {
        let r:Option<(i64,Option<String>)>=sqlx::query_as("SELECT last_attempt,last_error FROM metadata_peer_attempts WHERE info_hash=? AND peer_addr=?").bind(hex::encode(hash)).bind(addr).fetch_optional(&self.pool).await?;
        match r {
            None => Ok(PeerRetryEligibility::Eligible),
            Some((_, Some(e))) if e == "metadata hash mismatch" => {
                Ok(PeerRetryEligibility::Rejected)
            }
            Some((t, _)) => {
                let until = t.saturating_add(i64::from(hours) * 3600);
                Ok(if hours > 0 && until > now() {
                    PeerRetryEligibility::RetryAt(until)
                } else {
                    PeerRetryEligibility::Eligible
                })
            }
        }
    }
    pub async fn get_crawl_history(&self, limit: usize) -> Result<Vec<CrawlStatRecord>, Error> {
        sqlx::query_as("SELECT * FROM crawl_stats ORDER BY id DESC LIMIT ?")
            .bind(limit as u64)
            .fetch_all(&self.pool)
            .await
    }
    pub async fn get_metadata_failure_counts(&self) -> Result<Vec<MetadataFailureCount>, Error> {
        sqlx::query_as(
            "SELECT reason,count FROM metadata_failure_counts ORDER BY count DESC,reason",
        )
        .fetch_all(&self.pool)
        .await
    }
    pub async fn prune_old_torrents(&self, days: i64) -> Result<i64, Error> {
        let cutoff = now().saturating_sub(days.saturating_mul(86400));
        let mut total = 0;
        loop {
            let mut tx = self.pool.begin().await?;
            lock_totals(&mut tx).await?;
            let rows=sqlx::query("SELECT info_hash,total_size,metadata_complete FROM torrents WHERE last_seen<? ORDER BY last_seen,info_hash LIMIT 128 FOR UPDATE").bind(cutoff).fetch_all(&mut *tx).await?;
            if rows.is_empty() {
                tx.rollback().await?;
                return Ok(total);
            }
            for r in rows {
                let h: String = r.try_get("info_hash")?;
                let files = sqlx::query("DELETE FROM files WHERE torrent_id=?")
                    .bind(&h)
                    .execute(&mut *tx)
                    .await?
                    .rows_affected() as i64;
                sqlx::query("DELETE FROM torrents WHERE info_hash=?")
                    .bind(&h)
                    .execute(&mut *tx)
                    .await?;
                sqlx::query("UPDATE catalog_totals SET total_torrents=total_torrents-1,total_metadata_complete=total_metadata_complete-?,total_files=total_files-?,total_size=total_size-? WHERE id=1").bind(i64::from(r.try_get::<bool,_>("metadata_complete")?)).bind(files).bind(r.try_get::<i64,_>("total_size")?).execute(&mut *tx).await?;
                total += 1;
            }
            tx.commit().await?;
        }
    }
    pub async fn search_torrents(
        &self,
        query: &str,
        limit: usize,
        sort: &str,
    ) -> Result<Vec<TorrentRecord>, Error> {
        Ok(self
            .search_torrents_filtered_page(query, limit, 0, sort, TorrentSearchFilters::default())
            .await?
            .results)
    }
    pub async fn search_torrents_filtered_page(
        &self,
        query: &str,
        limit: usize,
        offset: usize,
        sort: &str,
        filters: TorrentSearchFilters,
    ) -> Result<TorrentSearchPage, Error> {
        let terms = search_query(query).map_err(invalid)?;
        let order = match sort {
            "rank" if !terms.is_empty() => "score DESC,t.last_seen DESC,t.info_hash",
            "rank" | "last_seen" => "t.last_seen DESC,t.info_hash DESC",
            "first_seen" => "t.first_seen DESC,t.info_hash DESC",
            "total_size" => "t.total_size DESC,t.info_hash DESC",
            "name" => "t.name,t.info_hash",
            _ => return Err(invalid("invalid sort")),
        };
        let mut q = QueryBuilder::<MySql>::new("SELECT /*+ MAX_EXECUTION_TIME(9000) */ t.*");
        if !terms.is_empty() && sort == "rank" {
            q.push(", MATCH(s.name,s.paths) AGAINST (")
                .push_bind(&terms)
                .push(" IN BOOLEAN MODE)+5*MATCH(s.name) AGAINST (")
                .push_bind(&terms)
                .push(" IN BOOLEAN MODE) AS score");
        }
        q.push(" FROM torrents t");
        if !terms.is_empty() {
            q.push(" JOIN search_documents s ON s.info_hash=t.info_hash WHERE MATCH(s.name,s.paths) AGAINST (").push_bind(&terms).push(" IN BOOLEAN MODE)");
        } else {
            q.push(" WHERE 1=1");
        }
        if filters.complete_only {
            q.push(" AND t.metadata_complete=1");
        }
        if let Some(s) = filters.min_size {
            q.push(" AND t.total_size>=").push_bind(s);
        }
        if let Some(s) = filters.max_size {
            q.push(" AND t.total_size<=").push_bind(s);
        }
        q.push(" ORDER BY ")
            .push(order)
            .push(" LIMIT ")
            .push_bind(limit.saturating_add(1) as u64)
            .push(" OFFSET ")
            .push_bind(offset as u64);
        let mut results = q
            .build()
            .fetch_all(&self.pool)
            .await?
            .iter()
            .map(row_to_torrent)
            .collect::<Result<Vec<_>, _>>()?;
        let has_next = results.len() > limit;
        results.truncate(limit);
        Ok(TorrentSearchPage { results, has_next })
    }
    pub async fn search_files_page(
        &self,
        hash: &InfoHash,
        query: &str,
        limit: usize,
        offset: usize,
    ) -> Result<FileSearchPage, Error> {
        let pattern = format!(
            "%{}%",
            query
                .replace('!', "!!")
                .replace('%', "!%")
                .replace('_', "!_")
        );
        let mut results:Vec<(String,i64)>=sqlx::query_as("SELECT /*+ MAX_EXECUTION_TIME(9000) */ path,size FROM files WHERE torrent_id=? AND path LIKE ? ESCAPE '!' ORDER BY path,id LIMIT ? OFFSET ?").bind(hex::encode(hash)).bind(pattern).bind(limit.saturating_add(1) as u64).bind(offset as u64).fetch_all(&self.pool).await?;
        let has_next = results.len() > limit;
        results.truncate(limit);
        Ok(FileSearchPage { results, has_next })
    }
    pub async fn reconcile(&self) -> Result<(), Error> {
        let mut tx = self.pool.begin().await?;
        lock_totals(&mut tx).await?;
        sqlx::query("UPDATE catalog_totals SET total_torrents=(SELECT COUNT(*) FROM torrents),total_metadata_complete=(SELECT COUNT(*) FROM torrents WHERE metadata_complete=1),total_files=(SELECT COUNT(*) FROM files),total_size=(SELECT COALESCE(SUM(total_size),0) FROM torrents) WHERE id=1").execute(&mut *tx).await?;
        tx.commit().await
    }
    pub async fn rebuild_search_index(&self) -> Result<(), Error> {
        let limits = SearchLimits::load(&mut *self.pool.acquire().await?).await?;
        sqlx::query("DELETE s FROM search_documents s JOIN torrents t ON t.info_hash=s.info_hash WHERE (t.metadata_complete=0 OR t.name IS NULL) AND NOT EXISTS (SELECT 1 FROM files f WHERE f.torrent_id=s.info_hash)")
            .execute(&self.pool).await?;
        let mut after = String::new();
        let mut rebuilt = 0usize;
        loop {
            let hashes: Vec<String> = sqlx::query_scalar(
                "(SELECT info_hash FROM torrents WHERE metadata_complete=1 AND name IS NOT NULL AND info_hash>? ORDER BY info_hash LIMIT 128) UNION (SELECT DISTINCT torrent_id AS info_hash FROM files WHERE torrent_id>? ORDER BY torrent_id LIMIT 128) ORDER BY info_hash LIMIT 128",
            )
            .bind(&after)
            .bind(&after)
            .fetch_all(&self.pool)
            .await?;
            if hashes.is_empty() {
                return self.analyze().await;
            }
            let mut documents = Vec::new();
            let mut bytes = 0;
            for hash in &hashes {
                let Some(document) = search_document(
                    &mut *self.pool.acquire().await?,
                    hash,
                    limits.document_bytes,
                )
                .await?
                else {
                    continue;
                };
                let size = document.0.len() + document.1.len() + document.2.len() + 128;
                if !documents.is_empty() && bytes + size > limits.batch_bytes {
                    self.replace_search_documents(&documents).await?;
                    rebuilt += documents.len();
                    documents.clear();
                    bytes = 0;
                }
                bytes += size;
                documents.push(document);
                if bytes >= limits.batch_bytes {
                    self.replace_search_documents(&documents).await?;
                    rebuilt += documents.len();
                    documents.clear();
                    bytes = 0;
                }
            }
            if !documents.is_empty() {
                self.replace_search_documents(&documents).await?;
                rebuilt += documents.len();
            }
            after = hashes.last().expect("nonempty search batch").clone();
            tracing::info!(rebuilt, "rebuilt search documents");
        }
    }
    async fn replace_search_documents(
        &self,
        documents: &[(String, String, String)],
    ) -> Result<(), Error> {
        let mut tx = self.pool.begin().await?;
        let mut delete =
            QueryBuilder::<MySql>::new("DELETE FROM search_documents WHERE info_hash IN (");
        let mut parameters = delete.separated(",");
        for (hash, _, _) in documents {
            parameters.push_bind(hash);
        }
        delete.push(")");
        delete.build().execute(&mut *tx).await?;
        let mut insert =
            QueryBuilder::<MySql>::new("INSERT INTO search_documents(info_hash,name,paths) ");
        insert.push_values(documents, |mut row, (hash, name, paths)| {
            row.push_bind(hash).push_bind(name).push_bind(paths);
        });
        insert.build().execute(&mut *tx).await?;
        tx.commit().await
    }
    pub async fn analyze(&self) -> Result<(), Error> {
        let rows = sqlx::query("ANALYZE TABLE torrents,files,search_documents,hash_jobs")
            .fetch_all(&self.pool)
            .await?;
        for row in rows {
            let kind: String = row.try_get(2)?;
            let message: String = row.try_get(3)?;
            if kind == "error" {
                return Err(invalid(format!("ANALYZE TABLE failed: {message}")));
            }
            if kind == "warning" {
                tracing::warn!("ANALYZE TABLE: {message}");
            }
        }
        Ok(())
    }
}
async fn lock_totals(conn: &mut MySqlConnection) -> Result<(), Error> {
    sqlx::query("SELECT id FROM catalog_totals WHERE id=1 FOR UPDATE")
        .fetch_one(conn)
        .await?;
    Ok(())
}
async fn discover(
    conn: &mut MySqlConnection,
    hash: &InfoHash,
    source: &str,
    peers: bool,
) -> Result<(), Error> {
    let h = hex::encode(hash);
    let exists: Option<String> =
        sqlx::query_scalar("SELECT info_hash FROM torrents WHERE info_hash=?")
            .bind(&h)
            .fetch_optional(&mut *conn)
            .await?;
    sqlx::query("INSERT INTO torrents(info_hash,source,first_seen,last_seen) VALUES (?,?,?,?) ON DUPLICATE KEY UPDATE source=VALUES(source),last_seen=VALUES(last_seen)").bind(&h).bind(source).bind(now()).bind(now()).execute(&mut *conn).await?;
    if exists.is_none() {
        sqlx::query("UPDATE catalog_totals SET total_torrents=total_torrents+1 WHERE id=1")
            .execute(&mut *conn)
            .await?;
    }
    sqlx::query("INSERT INTO hash_jobs(info_hash,status,last_new_peer_at,discovery_sources,updated_at) VALUES (?,'queued',?,?,?) ON DUPLICATE KEY UPDATE status=CASE WHEN status IN ('complete','invalid','unsupported') THEN status WHEN status='waiting_peers' AND VALUES(last_new_peer_at) IS NULL THEN status ELSE 'queued' END,next_attempt_at=IF(VALUES(last_new_peer_at) IS NULL,next_attempt_at,0),last_new_peer_at=COALESCE(VALUES(last_new_peer_at),last_new_peer_at),discovery_sources=IF(FIND_IN_SET(VALUES(discovery_sources),discovery_sources)>0,discovery_sources,CONCAT_WS(',',NULLIF(discovery_sources,''),VALUES(discovery_sources))),updated_at=VALUES(updated_at)").bind(&h).bind(peers.then_some(now())).bind(source).bind(now()).execute(&mut *conn).await?;
    Ok(())
}
const SEARCH_DOCUMENT_BYTES: usize = 16 * 1024 * 1024;
const SEARCH_BATCH_BYTES: usize = 1024 * 1024;
const SEARCH_PACKET_HEADROOM: u64 = 16 * 1024;

struct SearchLimits {
    document_bytes: usize,
    batch_bytes: usize,
}
impl SearchLimits {
    fn for_packet(packet: u64) -> Result<Self, Error> {
        let document_bytes = packet
            .saturating_sub(SEARCH_PACKET_HEADROOM)
            .min(SEARCH_DOCUMENT_BYTES as u64) as usize;
        if document_bytes < 1024 {
            return Err(invalid(
                "max_allowed_packet is too small for search indexing",
            ));
        }
        Ok(Self {
            document_bytes,
            batch_bytes: document_bytes.min(SEARCH_BATCH_BYTES),
        })
    }
    async fn load(conn: &mut MySqlConnection) -> Result<Self, Error> {
        let packet: u64 = sqlx::query_scalar("SELECT @@max_allowed_packet")
            .fetch_one(conn)
            .await?;
        Self::for_packet(packet)
    }
}
#[derive(Debug)]
struct SearchDocumentTooLarge {
    hash: String,
    budget: usize,
}
impl std::fmt::Display for SearchDocumentTooLarge {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "search document {} exceeds document byte limit {}; document was not truncated",
            self.hash, self.budget
        )
    }
}
impl std::error::Error for SearchDocumentTooLarge {}

pub fn is_unsupported_search_document(error: &Error) -> bool {
    matches!(error, Error::Encode(cause) if cause.is::<SearchDocumentTooLarge>())
}
fn document_too_large(hash: &str, budget: usize) -> Error {
    Error::Encode(Box::new(SearchDocumentTooLarge {
        hash: hash.to_owned(),
        budget,
    }))
}
fn append_search_text(output: &mut String, input: &str, budget: usize) -> bool {
    let mut separator = !output.is_empty();
    for c in input.nfkc() {
        if !c.is_alphanumeric() {
            separator = !output.is_empty();
            continue;
        }
        let size = c.len_utf8() + usize::from(separator);
        if output.len() + size > budget {
            return false;
        }
        if separator {
            output.push(' ');
            separator = false;
        }
        output.push(c);
    }
    true
}
async fn search_document(
    conn: &mut MySqlConnection,
    hash: &str,
    budget: usize,
) -> Result<Option<(String, String, String)>, Error> {
    let row = sqlx::query("SELECT CASE WHEN metadata_complete=1 THEN OCTET_LENGTH(name) ELSE 0 END AS bytes, CASE WHEN metadata_complete=1 AND OCTET_LENGTH(name)<=? THEN name ELSE NULL END AS name FROM torrents WHERE info_hash=?")
        .bind(budget as u64).bind(hash).fetch_one(&mut *conn).await?;
    let raw_bytes: Option<i64> = row.try_get("bytes")?;
    let too_large = || document_too_large(hash, budget);
    if raw_bytes.unwrap_or(0) > budget as i64 {
        return Err(too_large());
    }
    let raw_name: Option<&str> = row.try_get("name")?;
    let mut present = raw_name.is_some();
    let content_budget = budget.checked_sub(hash.len() + 128).ok_or_else(too_large)?;
    let mut name = String::new();
    if !append_search_text(&mut name, raw_name.unwrap_or_default(), content_budget) {
        return Err(too_large());
    }
    drop(row);
    let mut paths = String::new();
    let mut rows = sqlx::query("SELECT CASE WHEN OCTET_LENGTH(path)<=? THEN path ELSE NULL END AS path FROM files WHERE torrent_id=? ORDER BY id")
        .bind(budget as u64).bind(hash).fetch(conn);
    while let Some(row) = rows.try_next().await? {
        present = true;
        let path: Option<&str> = row.try_get("path")?;
        if !append_search_text(
            &mut paths,
            path.ok_or_else(too_large)?,
            content_budget - name.len(),
        ) {
            return Err(too_large());
        }
    }
    Ok(present.then(|| (hash.to_owned(), name, paths)))
}
async fn refresh_search(conn: &mut MySqlConnection, h: &str) -> Result<(), Error> {
    let limits = SearchLimits::load(conn).await?;
    match search_document(conn, h, limits.document_bytes).await? {
        Some((hash, name, paths)) => {
            sqlx::query("INSERT INTO search_documents VALUES (?,?,?) ON DUPLICATE KEY UPDATE name=VALUES(name),paths=VALUES(paths)")
                .bind(hash).bind(name).bind(paths).execute(conn).await?;
        }
        None => {
            sqlx::query("DELETE FROM search_documents WHERE info_hash=?")
                .bind(h)
                .execute(conn)
                .await?;
        }
    }
    Ok(())
}
pub fn normalize_search(s: &str) -> String {
    s.nfkc()
        .collect::<String>()
        .split(|c: char| !c.is_alphanumeric())
        .filter(|s| !s.is_empty())
        .collect::<Vec<_>>()
        .join(" ")
}
pub fn search_query(s: &str) -> Result<String, String> {
    if s.trim().is_empty() {
        return Ok(String::new());
    }
    if s.len() > 1024 {
        return Err("search query is too long (maximum 1024 bytes)".into());
    }
    let normalized = normalize_search(s);
    let terms: Vec<_> = normalized.split_whitespace().collect();
    if terms.is_empty() || terms.iter().any(|s| s.chars().count() < 2) {
        return Err("use at least two letters or characters in each search term".into());
    }
    Ok(terms
        .iter()
        .map(|s| format!("+\"{s}\""))
        .collect::<Vec<_>>()
        .join(" "))
}
fn row_to_torrent(r: &MySqlRow) -> Result<TorrentRecord, Error> {
    Ok(TorrentRecord {
        info_hash: decode_hash(r.try_get("info_hash")?)?,
        name: r.try_get("name")?,
        piece_length: r.try_get("piece_length")?,
        total_size: r.try_get("total_size")?,
        file_count: r.try_get("file_count")?,
        source: r.try_get("source")?,
        first_seen: r.try_get("first_seen")?,
        last_seen: r.try_get("last_seen")?,
        last_attempt: r.try_get("last_attempt")?,
        metadata_complete: r.try_get("metadata_complete")?,
    })
}
fn retryable(e: &Error) -> bool {
    matches!(e,Error::Database(d) if matches!(d.code().as_deref(),Some("40001")))
        || matches!(e, Error::Database(d) if d.try_downcast_ref::<sqlx::mysql::MySqlDatabaseError>().is_some_and(|e| e.number() == 1205))
        || matches!(e, Error::Io(_) | Error::PoolTimedOut)
}
async fn backoff(attempt: u32) {
    tokio::time::sleep(Duration::from_millis(100 * (1 << attempt))).await;
}

#[cfg(test)]
mod tests;
