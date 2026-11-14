use super::*;

#[tokio::test]
#[ignore = "requires BTFIND_TEST_DATABASE_URL; run scripts/test-mysql.sh"]
async fn mysql_incomplete_expiry_rechecks_changes_after_preview() {
    let store = Store::open_test_database().await.unwrap();
    for hash in [[91; 20], [92; 20], [93; 20]] {
        store
            .persist_hash_discovery(&hash, "dht", &[], 64)
            .await
            .unwrap();
    }
    sqlx::query("UPDATE torrents SET last_seen=1")
        .execute(&store.pool)
        .await
        .unwrap();
    let policy = ExpiryPolicy::new(30, 7, 128, 10).unwrap();
    assert_eq!(
        store
            .expire_incomplete(&policy, None, false)
            .await
            .unwrap()
            .candidates,
        3
    );
    store
        .commit_metadata(
            &[91; 20],
            "retained title",
            1,
            42,
            &[("retained/file.txt".into(), 42)],
        )
        .await
        .unwrap();
    store
        .persist_hash_discovery(&[92; 20], "dht", &[], 64)
        .await
        .unwrap();
    assert_eq!(
        store
            .expire_incomplete(&policy, None, true)
            .await
            .unwrap()
            .deleted,
        1
    );
    assert_eq!(
        store
            .expire_incomplete(&policy, None, true)
            .await
            .unwrap()
            .deleted,
        0
    );
    assert!(
        store
            .get_torrent(&[91; 20])
            .await
            .unwrap()
            .unwrap()
            .metadata_complete
    );
    assert!(store.get_torrent(&[92; 20]).await.unwrap().is_some());
    assert!(store.get_torrent(&[93; 20]).await.unwrap().is_none());
    assert_eq!(
        store
            .search_torrents("retained", 10, "rank")
            .await
            .unwrap()
            .len(),
        1
    );
    let before = store.get_stats().await.unwrap();
    assert_eq!(
        (
            before.total_torrents,
            before.total_metadata_complete,
            before.total_files,
            before.total_size
        ),
        (2, 1, 1, 42)
    );
    store.reconcile().await.unwrap();
    assert_eq!(store.get_stats().await.unwrap(), before);
}

#[tokio::test]
#[ignore = "requires BTFIND_TEST_DATABASE_URL; run scripts/test-mysql.sh"]
async fn mysql_incomplete_expiry_eligibility_and_rediscovery() {
    let store = Store::open_test_database().await.unwrap();
    let policy = ExpiryPolicy {
        last_seen_before: 100,
        inactive_before: 200,
        started_at: 300,
        batch_size: 4,
        max_batches: 100,
    };
    let cases = [
        ("peerless", "", true),
        ("waiting with stale state", "INSERT INTO hash_jobs(info_hash,status,updated_at) VALUES ('$h','waiting_peers',1); INSERT INTO hash_job_peers VALUES ('$h','127.0.0.1:1',1); INSERT INTO metadata_peer_attempts VALUES ('$h','127.0.0.1:1',1,'timeout'); INSERT INTO sample_observations(info_hash,first_seen,last_seen) VALUES ('$h',1,1)", true),
        ("invalid", "INSERT INTO hash_jobs(info_hash,status,updated_at) VALUES ('$h','invalid',1)", true),
        ("unsupported", "INSERT INTO hash_jobs(info_hash,status,updated_at) VALUES ('$h','unsupported',1); INSERT INTO hash_job_peers VALUES ('$h','127.0.0.1:1',1)", true),
        ("queued without peers", "INSERT INTO hash_jobs(info_hash,status,updated_at) VALUES ('$h','queued',1)", true),
        ("overdue without peers", "INSERT INTO hash_jobs(info_hash,status,updated_at,next_attempt_at) VALUES ('$h','retry_at',1,1)", true),
        ("queued with old peers", "INSERT INTO hash_jobs(info_hash,status,updated_at) VALUES ('$h','queued',1); INSERT INTO hash_job_peers VALUES ('$h','127.0.0.1:1',1)", false),
        ("retry with old peers", "INSERT INTO hash_jobs(info_hash,status,updated_at) VALUES ('$h','retry_at',1); INSERT INTO hash_job_peers VALUES ('$h','127.0.0.1:1',1)", false),
        ("future retry", "INSERT INTO hash_jobs(info_hash,status,updated_at,next_attempt_at) VALUES ('$h','retry_at',1,300)", false),
        ("recent sighting boundary", "UPDATE torrents SET last_seen=100 WHERE info_hash='$h'", false),
        ("recent attempt boundary", "UPDATE torrents SET last_attempt=200 WHERE info_hash='$h'", false),
        ("recent job boundary", "INSERT INTO hash_jobs(info_hash,status,updated_at) VALUES ('$h','waiting_peers',200)", false),
        ("recent new peer boundary", "INSERT INTO hash_jobs(info_hash,status,updated_at,last_new_peer_at) VALUES ('$h','waiting_peers',1,200)", false),
        ("complete metadata", "UPDATE torrents SET metadata_complete=1 WHERE info_hash='$h'", false),
        ("complete job", "INSERT INTO hash_jobs(info_hash,status,updated_at) VALUES ('$h','complete',1)", false),
        ("unknown job status", "INSERT INTO hash_jobs(info_hash,status,updated_at) VALUES ('$h','running',1)", false),
        ("name", "UPDATE torrents SET name='valuable' WHERE info_hash='$h'", false),
        ("files", "INSERT INTO files(torrent_id,path,size) VALUES ('$h','valuable',1)", false),
        ("search content", "INSERT INTO search_documents VALUES ('$h','valuable','valuable')", false),
        ("size", "UPDATE torrents SET total_size=42 WHERE info_hash='$h'", false),
        ("file count", "UPDATE torrents SET file_count=1 WHERE info_hash='$h'", false),
        ("piece length", "UPDATE torrents SET piece_length=1 WHERE info_hash='$h'", false),
        ("recent peer attempt", "INSERT INTO metadata_peer_attempts VALUES ('$h','127.0.0.1:1',200,'timeout')", false),
        ("recent sample", "INSERT INTO sample_observations(info_hash,first_seen,last_seen) VALUES ('$h',1,200)", false),
        ("recent success", "INSERT INTO hash_jobs(info_hash,status,updated_at,last_success_at) VALUES ('$h','waiting_peers',1,200)", false),
        ("recent retained peer", "INSERT INTO hash_job_peers VALUES ('$h','127.0.0.1:1',200)", false),
    ];
    for (i, (_, sql, _)) in cases.iter().enumerate() {
        let hash = format!("{i:040x}");
        sqlx::query("INSERT INTO torrents(info_hash,first_seen,last_seen) VALUES (?,1,1)")
            .bind(&hash)
            .execute(&store.pool)
            .await
            .unwrap();
        if !sql.is_empty() {
            store
                .execute_batch_for_test(&sql.replace("$h", &hash))
                .await
                .unwrap();
        }
    }
    store.reconcile().await.unwrap();
    let before = store.get_stats().await.unwrap();
    let preview = store.expire_incomplete(&policy, None, false).await.unwrap();
    assert_eq!(preview.candidates, 6);
    assert_eq!(preview.deleted, 0);
    assert!(preview.exhausted);
    assert_eq!(store.get_stats().await.unwrap(), before);
    let applied = store.expire_incomplete(&policy, None, true).await.unwrap();
    assert_eq!(applied.candidates, preview.candidates);
    assert_eq!(applied.deleted, 6);
    for (i, (label, _, expired)) in cases.iter().enumerate() {
        assert_eq!(
            store
                .get_torrent_by_hex(&format!("{i:040x}"))
                .await
                .unwrap()
                .is_none(),
            *expired,
            "{label}"
        );
    }
    let after = store.get_stats().await.unwrap();
    assert_eq!(after.total_torrents, before.total_torrents - 6);
    assert_eq!(
        after.total_metadata_complete,
        before.total_metadata_complete
    );
    assert_eq!(after.total_files, before.total_files);
    assert_eq!(after.total_size, before.total_size);
    store.reconcile().await.unwrap();
    assert_eq!(store.get_stats().await.unwrap(), after);
    for table in [
        "hash_jobs",
        "hash_job_peers",
        "metadata_peer_attempts",
        "sample_observations",
    ] {
        let n: i64 = sqlx::query_scalar(&format!("SELECT COUNT(*) FROM {table} WHERE info_hash=?"))
            .bind(format!("{:040x}", 1))
            .fetch_one(&store.pool)
            .await
            .unwrap();
        assert_eq!(n, 0, "{table}");
    }
    let rediscovered = decode_hash(&format!("{:040x}", 1)).unwrap();
    let peers = [PeerContact {
        addr: "127.0.0.1:1234".parse().unwrap(),
    }];
    let discovery = store
        .persist_hash_discovery(&rediscovered, "dht", &peers, 64)
        .await
        .unwrap();
    assert!(discovery.torrent_inserted && discovery.hash_job_created);
    store
        .persist_hash_discovery(&rediscovered, "dht", &peers, 64)
        .await
        .unwrap();
    assert_eq!(
        store.get_stats().await.unwrap().total_torrents,
        after.total_torrents + 1
    );
    assert_eq!(
        store.hash_job_peers(&rediscovered, 64).await.unwrap().len(),
        1
    );
}

#[tokio::test]
#[ignore = "requires BTFIND_TEST_DATABASE_URL; run scripts/test-mysql.sh"]
async fn mysql_incomplete_expiry_bounded_cursor_and_rollback() {
    let store = Store::open_test_database().await.unwrap();
    for i in 0..8 {
        sqlx::query("INSERT INTO torrents(info_hash,first_seen,last_seen,name) VALUES (?,1,?,?)")
            .bind(format!("{i:040x}"))
            .bind(1 + i / 6)
            .bind(if i < 3 { Some("keep") } else { None })
            .execute(&store.pool)
            .await
            .unwrap();
    }
    store.reconcile().await.unwrap();
    let policy = ExpiryPolicy::new(30, 7, 3, 1).unwrap();
    let first = store.expire_incomplete(&policy, None, true).await.unwrap();
    assert_eq!(
        (first.scanned, first.deleted, first.exhausted),
        (3, 0, false)
    );
    let cursor: ExpiryCursor = first.next_cursor.unwrap().parse().unwrap();
    let preview = store
        .expire_incomplete(&policy, Some(cursor.clone()), false)
        .await
        .unwrap();
    assert_eq!(
        (preview.scanned, preview.candidates, preview.deleted),
        (3, 3, 0)
    );
    let second = store
        .expire_incomplete(&policy, Some(cursor.clone()), true)
        .await
        .unwrap();
    assert_eq!(second.deleted, 3);
    let last = store
        .expire_incomplete(
            &policy,
            Some(second.next_cursor.unwrap().parse().unwrap()),
            true,
        )
        .await
        .unwrap();
    assert_eq!((last.scanned, last.deleted, last.exhausted), (2, 2, true));
    assert_eq!(store.get_stats().await.unwrap().total_torrents, 3);

    store
        .persist_hash_discovery(&[99; 20], "dht", &[], 64)
        .await
        .unwrap();
    sqlx::query("UPDATE torrents SET last_seen=1 WHERE info_hash=?")
        .bind(hex::encode([99; 20]))
        .execute(&store.pool)
        .await
        .unwrap();
    store.execute_batch_for_test("CREATE TRIGGER expiry_failure BEFORE UPDATE ON catalog_totals FOR EACH ROW SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='test rollback'").await.unwrap();
    assert!(store
        .expire_incomplete(&policy, Some(cursor), true)
        .await
        .is_err());
    assert!(store.get_torrent(&[99; 20]).await.unwrap().is_some());
    assert_eq!(store.get_stats().await.unwrap().total_torrents, 4);
}

#[tokio::test]
#[ignore = "requires BTFIND_TEST_DATABASE_URL; run scripts/test-mysql.sh"]
async fn mysql_torrent_indexes_after_migration() {
    let store = Store::open_test_database().await.unwrap();
    let indexes: Vec<String> = sqlx::query_scalar(
        "SELECT DISTINCT INDEX_NAME FROM information_schema.STATISTICS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'torrents' ORDER BY INDEX_NAME",
    )
    .fetch_all(&store.pool)
    .await
    .unwrap();
    assert_eq!(
        indexes,
        [
            "idx_complete_last",
            "idx_complete_size",
            "idx_last",
            "idx_size",
            "PRIMARY"
        ]
    );
}

#[tokio::test]
#[ignore = "requires BTFIND_TEST_DATABASE_URL; run scripts/test-mysql.sh"]
async fn mysql_metadata_retries_lock_timeout() {
    let store = Store::open_test_database().await.unwrap();
    let mut lock = store.pool.begin().await.unwrap();
    lock_totals(&mut lock).await.unwrap();
    let worker = store.clone();
    let task = tokio::spawn(async move {
        worker
            .commit_metadata(&[41; 20], "retry metadata", 1, 1, &[("file".into(), 1)])
            .await
    });
    tokio::time::sleep(Duration::from_millis(5500)).await;
    assert!(
        !task.is_finished(),
        "metadata commit returned instead of retrying lock timeout"
    );
    lock.commit().await.unwrap();
    task.await.unwrap().unwrap();
    assert_eq!(store.get_stats().await.unwrap().total_metadata_complete, 1);
}

#[tokio::test]
#[ignore = "requires BTFIND_TEST_DATABASE_URL; run scripts/test-mysql.sh"]
async fn mysql_receipt_retention_preserves_retries() {
    let store = Store::open_test_database().await.unwrap();
    let event = CrawlStatsEvent::MetadataFetchFailed {
        reason: crate::types::MetadataFailureReason::Connect,
    };
    for receipt in ["old-ack", "recent-ack", "pending"] {
        store
            .persist_runtime_batch(&[event], None, receipt)
            .await
            .unwrap();
    }
    store.acknowledge_receipt("old-ack").await.unwrap();
    store.acknowledge_receipt("recent-ack").await.unwrap();
    sqlx::query("UPDATE persistence_receipts SET acknowledged_at=1 WHERE id='old-ack'")
        .execute(&store.pool)
        .await
        .unwrap();
    sqlx::query("UPDATE persistence_receipts SET created_at=1 WHERE id='pending'")
        .execute(&store.pool)
        .await
        .unwrap();
    assert_eq!(store.prune_receipts().await.unwrap(), 1);
    for receipt in ["recent-ack", "pending"] {
        store
            .persist_runtime_batch(&[event], None, receipt)
            .await
            .unwrap();
    }
    assert_eq!(
        store.get_metadata_failure_counts().await.unwrap()[0].count,
        3
    );
}

#[tokio::test]
#[ignore = "requires BTFIND_TEST_DATABASE_URL; run scripts/test-mysql.sh"]
async fn mysql_rebuild_byte_budget() {
    let store = Store::open_test_database().await.unwrap();
    for i in 1..=5u64 {
        let hash = format!("{i:040x}");
        sqlx::query("INSERT INTO torrents(info_hash,name,first_seen,last_seen,metadata_complete) VALUES (?, 'large', 1, 1, 1)")
            .bind(&hash).execute(&store.pool).await.unwrap();
        for _ in 0..8 {
            sqlx::query("INSERT INTO files(torrent_id,path,size) VALUES (?,?,1)")
                .bind(&hash)
                .bind("文".repeat(12_000))
                .execute(&store.pool)
                .await
                .unwrap();
        }
    }
    store.rebuild_search_index().await.unwrap();
    let count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM search_documents WHERE OCTET_LENGTH(paths)>280000",
    )
    .fetch_one(&store.pool)
    .await
    .unwrap();
    assert_eq!(count, 5);
    let hash = format!("{:040x}", 1);
    sqlx::query("INSERT INTO files(torrent_id,path,size) VALUES (?,?,1)")
        .bind(&hash)
        .bind("a".repeat(1024 * 1024))
        .execute(&store.pool)
        .await
        .unwrap();
    store.rebuild_search_index().await.unwrap();
    let size: i64 =
        sqlx::query_scalar("SELECT OCTET_LENGTH(paths) FROM search_documents WHERE info_hash=?")
            .bind(hash)
            .fetch_one(&store.pool)
            .await
            .unwrap();
    assert!(size > 1024 * 1024);
}

#[tokio::test]
#[ignore = "requires BTFIND_TEST_DATABASE_URL; run scripts/test-mysql.sh"]
async fn mysql_catalogue_concurrent_load() {
    let store = Store::open_test_database().await.unwrap();
    let before: (String, String) = sqlx::query_as("SHOW GLOBAL STATUS LIKE 'Innodb_row_lock_time'")
        .fetch_one(&store.pool)
        .await
        .unwrap();
    let start = Instant::now();
    let mut workers = tokio::task::JoinSet::new();
    for worker in 0..8u8 {
        let store = store.clone();
        workers.spawn(async move {
            let mut latencies = Vec::new();
            for n in 0..32u8 {
                let mut hash = [worker; 20];
                hash[19] = n;
                let start = Instant::now();
                store
                    .persist_hash_discovery(&hash, "load", &[], 64)
                    .await
                    .unwrap();
                if n % 8 == 0 {
                    store
                        .commit_metadata(&hash, "load metadata", 1, 1, &[("path".into(), 1)])
                        .await
                        .unwrap();
                }
                latencies.push(start.elapsed().as_secs_f64());
            }
            latencies
        });
    }
    let mut latencies = Vec::new();
    while let Some(result) = workers.join_next().await {
        latencies.extend(result.unwrap());
    }
    latencies.sort_by(f64::total_cmp);
    let after: (String, String) = sqlx::query_as("SHOW GLOBAL STATUS LIKE 'Innodb_row_lock_time'")
        .fetch_one(&store.pool)
        .await
        .unwrap();
    let wait_ms = after.1.parse::<u64>().unwrap() - before.1.parse::<u64>().unwrap();
    eprintln!("catalogue load: 8 writers, 256 discoveries + 32 metadata commits, {:.1} discoveries/s, p95 {:.1} ms, aggregate row-lock waits {} ms", 256.0/start.elapsed().as_secs_f64(), latencies[243]*1000.0, wait_ms);
    let stats = store.get_stats().await.unwrap();
    assert_eq!(stats.total_torrents, 256);
    assert_eq!(stats.total_metadata_complete, 32);
}

#[tokio::test]
#[ignore = "requires BTFIND_TEST_DATABASE_URL; run scripts/test-mysql.sh"]
async fn mysql_batched_rebuild_and_browse_order() {
    let store = Store::open_test_database().await.unwrap();
    for i in 1..=132u64 {
        sqlx::query("INSERT INTO torrents(info_hash,name,first_seen,last_seen,total_size,metadata_complete) VALUES (?,?,1,1,1,?)")
            .bind(format!("{i:040x}"))
            .bind("東京 中文")
            .bind(i <= 130)
            .execute(&store.pool).await.unwrap();
    }
    sqlx::query("INSERT INTO files(torrent_id,path,size) VALUES (?, '資料/文件.txt', 1)")
        .bind(format!("{:040x}", 131))
        .execute(&store.pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO search_documents VALUES (?, 'obsolete', '')")
        .bind(format!("{:040x}", 132))
        .execute(&store.pool)
        .await
        .unwrap();
    store.rebuild_search_index().await.unwrap();
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM search_documents")
        .fetch_one(&store.pool)
        .await
        .unwrap();
    assert_eq!(count, 131);
    assert_eq!(
        store
            .search_torrents("東京", 500, "rank")
            .await
            .unwrap()
            .len(),
        130
    );
    let files = store.search_torrents("文件", 10, "rank").await.unwrap();
    assert_eq!(files.len(), 1);
    assert_eq!(hex::encode(files[0].info_hash), format!("{:040x}", 131));
    assert!(store
        .search_torrents("obsolete", 10, "rank")
        .await
        .unwrap()
        .is_empty());
    sqlx::query("UPDATE torrents SET name='大阪' WHERE info_hash=?")
        .bind(format!("{:040x}", 1))
        .execute(&store.pool)
        .await
        .unwrap();
    store.rebuild_search_index().await.unwrap();
    assert_eq!(
        store
            .search_torrents("大阪", 10, "rank")
            .await
            .unwrap()
            .len(),
        1
    );
    for sort in ["last_seen", "first_seen", "total_size", "rank"] {
        let page = store
            .search_torrents_filtered_page("", 2, 1, sort, TorrentSearchFilters::default())
            .await
            .unwrap();
        assert_eq!(
            hex::encode(page.results[0].info_hash),
            format!("{:040x}", 131)
        );
        assert_eq!(
            hex::encode(page.results[1].info_hash),
            format!("{:040x}", 130)
        );
        assert!(page.has_next);
    }
}

impl Store {
    pub async fn open_test_database() -> Result<Self, Error> {
        let url = std::env::var("BTFIND_TEST_DATABASE_URL")
            .map_err(|_| invalid("BTFIND_TEST_DATABASE_URL is required"))?;
        let options = MySqlConnectOptions::from_str(&url)
            .map_err(|_| invalid("invalid test URL"))?
            .ssl_mode(MySqlSslMode::Disabled);
        if !matches!(options.get_host(), "127.0.0.1" | "localhost" | "::1") {
            return Err(invalid("tests require a local disposable server"));
        }
        let admin = Self::connect_options(options.clone(), 2, 9000).await?;
        let name = format!("btfind_test_{}", uuid::Uuid::new_v4().simple());
        sqlx::query(&format!("CREATE DATABASE `{name}` CHARACTER SET utf8mb4"))
            .execute(&admin.pool)
            .await?;
        admin.pool.close().await;
        let store = Self::connect_options(options.database(&name), 8, 9000).await?;
        store.migrate().await?;
        Ok(store)
    }
    pub async fn execute_batch_for_test(&self, sql: &str) -> Result<(), Error> {
        sqlx::raw_sql(sql).execute(&self.pool).await?;
        Ok(())
    }
    pub async fn upsert_torrent(
        &self,
        hash: InfoHash,
        name: Option<String>,
        size: i64,
        count: i64,
        source: &str,
    ) -> Result<i64, Error> {
        self.persist_hash_discovery(&hash, source, &[], 64).await?;
        sqlx::query("UPDATE torrents SET name=?,total_size=?,file_count=? WHERE info_hash=?")
            .bind(name)
            .bind(size)
            .bind(count)
            .bind(hex::encode(hash))
            .execute(&self.pool)
            .await?;
        self.reconcile().await?;
        Ok(1)
    }
    pub async fn mark_metadata_complete(
        &self,
        hash: &InfoHash,
        name: &str,
        piece: i64,
        size: i64,
        count: i64,
    ) -> Result<(), Error> {
        sqlx::query("UPDATE torrents SET name=?,piece_length=?,total_size=?,file_count=?,metadata_complete=1 WHERE info_hash=?").bind(name).bind(piece).bind(size).bind(count).bind(hex::encode(hash)).execute(&self.pool).await?;
        self.refresh_torrent_fts(hash).await?;
        self.reconcile().await
    }
    pub async fn insert_files(
        &self,
        hash: &InfoHash,
        files: &[(String, i64)],
    ) -> Result<(), Error> {
        let record = self.get_torrent(hash).await?.ok_or(Error::RowNotFound)?;
        self.commit_metadata(
            hash,
            record.name.as_deref().unwrap_or(""),
            record.piece_length.unwrap_or(0),
            record.total_size,
            files,
        )
        .await
    }
    pub async fn refresh_torrent_fts(&self, hash: &InfoHash) -> Result<(), Error> {
        let mut conn = self.pool.acquire().await?;
        refresh_search(&mut conn, &hex::encode(hash)).await
    }
    pub async fn insert_crawl_stat(
        &self,
        nodes: i64,
        queries: i64,
        hashes: i64,
        metadata: i64,
    ) -> Result<i64, Error> {
        Ok(sqlx::query("INSERT INTO crawl_stats(timestamp,nodes_known,queries_sent,info_hashes_found,metadata_fetched) VALUES (?,?,?,?,?)").bind(now()).bind(nodes).bind(queries).bind(hashes).bind(metadata).execute(&self.pool).await?.last_insert_id() as i64)
    }
    pub async fn upsert_dht_node(&self, id: &NodeId, addr: SocketAddrV4) -> Result<(), Error> {
        sqlx::query("INSERT INTO dht_nodes VALUES (?,?,?) ON DUPLICATE KEY UPDATE addr=VALUES(addr),last_seen=VALUES(last_seen)").bind(hex::encode(id)).bind(addr.to_string()).bind(now()).execute(&self.pool).await?;
        Ok(())
    }
    pub async fn should_skip_peer_retry(
        &self,
        hash: &InfoHash,
        addr: &str,
        hours: u32,
    ) -> Result<bool, Error> {
        Ok(self.peer_retry_eligibility(hash, addr, hours).await? != PeerRetryEligibility::Eligible)
    }
    pub async fn hash_job_schedule_for_test(
        &self,
        hash: &InfoHash,
    ) -> Result<(String, u32, i64), Error> {
        let (status, count, next): (String, i64, i64) = sqlx::query_as(
            "SELECT status,attempt_count,next_attempt_at FROM hash_jobs WHERE info_hash=?",
        )
        .bind(hex::encode(hash))
        .fetch_one(&self.pool)
        .await?;
        Ok((status, count as u32, next))
    }
}

#[test]
fn safe_multilingual_query() {
    assert_eq!(
        search_query("東京 中文 ubuntu-24").unwrap(),
        "+\"東京\" +\"中文\" +\"ubuntu\" +\"24\""
    );
    assert_eq!(search_query("+ubuntu*").unwrap(), "+\"ubuntu\"");
    assert!(search_query("日").is_err());
    assert!(search_query("foo a").is_err());
    assert!(search_query("++").is_err());
    assert_eq!(search_query(" ").unwrap(), "");
    assert_eq!(normalize_search("中文/東京_file.iso"), "中文 東京 file iso");
    assert_eq!(normalize_search("ｶﾞﾝﾀﾞﾑ"), "ガンダム");
}

#[tokio::test]
#[ignore = "requires BTFIND_TEST_DATABASE_URL; run scripts/test-mysql.sh"]
async fn mysql_metadata_search_and_counters() {
    let store = Store::open_test_database().await.unwrap();
    store.check().await.unwrap();
    store
        .commit_metadata(
            &[1; 20],
            "東京 中文 Ubuntu",
            16,
            4,
            &[("movie.iso".into(), 4)],
        )
        .await
        .unwrap();
    store
        .commit_metadata(
            &[2; 20],
            "Other title",
            16,
            5,
            &[("東京/中文/ubuntu.iso".into(), 5)],
        )
        .await
        .unwrap();
    for query in ["東京", "中文", "Ubuntu", "東京 中文", "iso"] {
        let results = store.search_torrents(query, 50, "rank").await.unwrap();
        assert_eq!(results.len(), 2, "{query}");
    }
    assert_eq!(
        store.search_torrents("東京", 50, "rank").await.unwrap()[0].info_hash,
        [1; 20]
    );
    let page = store
        .search_torrents_filtered_page("", 1, 0, "last_seen", TorrentSearchFilters::default())
        .await
        .unwrap();
    assert!(page.has_next);
    let before = store.get_stats().await.unwrap();
    store
        .commit_metadata(
            &[1; 20],
            "Changed title",
            16,
            7,
            &[("changed.txt".into(), 7)],
        )
        .await
        .unwrap();
    assert_eq!(
        store
            .search_torrents("東京", 50, "rank")
            .await
            .unwrap()
            .len(),
        1
    );
    let after = store.get_stats().await.unwrap();
    assert_eq!(after.total_torrents, before.total_torrents);
    assert_eq!(after.total_files, 2);
    assert_eq!(after.total_size, 12);
    store.reconcile().await.unwrap();
    assert_eq!(store.get_stats().await.unwrap(), after);
    assert!(store
        .commit_metadata(&[1; 20], "bad", 16, 0, &[("".into(), 0)])
        .await
        .is_err());
    assert_eq!(store.get_stats().await.unwrap(), after);
    sqlx::query("UPDATE torrents SET last_seen=0 WHERE info_hash=?")
        .bind(hex::encode([1; 20]))
        .execute(&store.pool)
        .await
        .unwrap();
    let report = store
        .expire_incomplete(&ExpiryPolicy::new(1, 1, 128, 10).unwrap(), None, true)
        .await
        .unwrap();
    assert_eq!(report.deleted, 0);
    assert_eq!(store.get_stats().await.unwrap(), after);
}

#[tokio::test]
#[ignore = "requires BTFIND_TEST_DATABASE_URL; run scripts/test-mysql.sh"]
async fn mysql_receipts_and_concurrent_commits() {
    let store = Store::open_test_database().await.unwrap();
    let event = CrawlStatsEvent::MetadataFetchFailed {
        reason: crate::types::MetadataFailureReason::Connect,
    };
    let receipt = uuid::Uuid::new_v4().to_string();
    store
        .persist_runtime_batch(&[event], None, &receipt)
        .await
        .unwrap();
    store
        .persist_runtime_batch(&[event], None, &receipt)
        .await
        .unwrap();
    assert_eq!(
        store.get_metadata_failure_counts().await.unwrap()[0].count,
        1
    );
    let mut workers = tokio::task::JoinSet::new();
    for _ in 0..8 {
        let store = store.clone();
        workers.spawn(async move {
            store
                .commit_metadata(&[8; 20], "parallel", 16, 1, &[("one.txt".into(), 1)])
                .await
        });
    }
    while let Some(result) = workers.join_next().await {
        result.unwrap().unwrap();
    }
    let stats = store.get_stats().await.unwrap();
    assert_eq!(stats.total_torrents, 1);
    assert_eq!(stats.total_files, 1);
    assert_eq!(stats.total_metadata_complete, 1);
    assert_eq!(
        store.load_or_create_node_id().await.unwrap(),
        store.load_or_create_node_id().await.unwrap()
    );
}

#[tokio::test]
#[ignore = "requires BTFIND_TEST_DATABASE_URL; run scripts/test-mysql.sh"]
async fn mysql_readonly_web_and_query_timeout() {
    let store = Store::open_test_database().await.unwrap();
    store
        .commit_metadata(&[3; 20], "日本語 中文", 16, 1, &[("東京.iso".into(), 1)])
        .await
        .unwrap();
    let database: String = sqlx::query_scalar("SELECT DATABASE()")
        .fetch_one(&store.pool)
        .await
        .unwrap();
    let user = format!("web_{}", &uuid::Uuid::new_v4().simple().to_string()[..20]);
    sqlx::query(&format!(
        "CREATE USER '{user}'@'%' IDENTIFIED BY 'local-test-only'"
    ))
    .execute(&store.pool)
    .await
    .unwrap();
    sqlx::query(&format!("GRANT SELECT ON `{database}`.* TO '{user}'@'%'"))
        .execute(&store.pool)
        .await
        .unwrap();
    let options =
        MySqlConnectOptions::from_str(&std::env::var("BTFIND_TEST_DATABASE_URL").unwrap())
            .unwrap()
            .username(&user)
            .password("local-test-only")
            .database(&database)
            .ssl_mode(MySqlSslMode::Disabled);
    let web = Store::connect_options(options, 4, 9000).await.unwrap();
    web.check().await.unwrap();
    assert_eq!(
        web.search_torrents("日本語", 50, "rank")
            .await
            .unwrap()
            .len(),
        1
    );
    assert_eq!(web.get_stats().await.unwrap().total_torrents, 1);
    assert!(web
        .persist_hash_discovery(&[8; 20], "dht", &[], 64)
        .await
        .is_err());
    let started = Instant::now();
    let interrupted: i64 = sqlx::query_scalar("SELECT /*+ MAX_EXECUTION_TIME(20) */ SLEEP(2)")
        .fetch_one(&web.pool)
        .await
        .unwrap();
    assert_eq!(interrupted, 1);
    assert!(started.elapsed() < Duration::from_secs(1));
    web.pool.close().await;
    assert!(web.get_stats().await.is_err());
    sqlx::query(&format!("DROP USER '{user}'@'%'"))
        .execute(&store.pool)
        .await
        .unwrap();
}

#[tokio::test]
#[ignore = "requires BTFIND_TEST_DATABASE_URL; run scripts/test-mysql.sh"]
async fn mysql_rollback_schema_mismatch_and_large_document() {
    let store = Store::open_test_database().await.unwrap();
    let path = format!("日本語/{}.iso", "large".repeat(20_000));
    store
        .commit_metadata(&[1; 20], "日本語", 16, 2, &[(path.clone(), 2)])
        .await
        .unwrap();
    let page = store.search_files_page(&[1; 20], "", 10, 0).await.unwrap();
    assert_eq!(page.results[0].0, path);
    let before = store.get_stats().await.unwrap();
    store.execute_batch_for_test("CREATE TRIGGER force_rollback BEFORE INSERT ON files FOR EACH ROW SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='rollback test'").await.unwrap();
    assert!(store
        .commit_metadata(&[1; 20], "new title", 16, 4, &[("new.iso".into(), 4)])
        .await
        .is_err());
    assert_eq!(store.get_stats().await.unwrap(), before);
    assert_eq!(
        store
            .search_torrents("日本語", 10, "rank")
            .await
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        store
            .search_files_page(&[1; 20], "", 10, 0)
            .await
            .unwrap()
            .results[0]
            .0,
        path
    );
    store
        .execute_batch_for_test("UPDATE _sqlx_migrations SET checksum=X'01'")
        .await
        .unwrap();
    assert!(store.check().await.is_err());
}

#[test]
fn search_limits_and_normalization() {
    let limits = SearchLimits::for_packet(64 * 1024 * 1024).unwrap();
    assert_eq!(limits.document_bytes, SEARCH_DOCUMENT_BYTES);
    assert_eq!(limits.batch_bytes, SEARCH_BATCH_BYTES);
    let limits = SearchLimits::for_packet(128 * 1024).unwrap();
    assert_eq!(limits.document_bytes, 112 * 1024);
    assert_eq!(limits.batch_bytes, 112 * 1024);
    assert!(SearchLimits::for_packet(SEARCH_PACKET_HEADROOM).is_err());
    let mut text = String::new();
    assert!(append_search_text(&mut text, "  Ａ-東京", 10));
    assert!(append_search_text(&mut text, " /中文 ", 15));
    assert_eq!(text, normalize_search("  Ａ-東京 /中文 "));
    let mut expansion = String::new();
    assert!(!append_search_text(&mut expansion, "㍿", 11));
    assert!(expansion.len() <= 11);
    let mut exact = String::new();
    assert!(append_search_text(&mut exact, "㍿", 12));
    assert_eq!(exact, "株式会社");
}

#[tokio::test]
#[ignore = "requires BTFIND_TEST_DATABASE_URL; run scripts/test-mysql.sh"]
async fn mysql_ingested_large_documents_rebuild_exactly() {
    let store = Store::open_test_database().await.unwrap();
    let files = vec![
        (format!("{} 終端文件", "東京/".repeat(180_000)), 1),
        ("㍿/ ＡＢＣ /last-file".into(), 2),
    ];
    let large_name = format!("{} 名前末尾", "日本語 ＡＢＣ ".repeat(80_000));
    for hash in [[51; 20], [52; 20]] {
        store
            .commit_metadata(&hash, &large_name, 1, 3, &files)
            .await
            .unwrap();
    }
    let before: Vec<(String, String, String)> =
        sqlx::query_as("SELECT info_hash,name,paths FROM search_documents ORDER BY info_hash")
            .fetch_all(&store.pool)
            .await
            .unwrap();
    assert_eq!(before.len(), 2);
    assert!(before
        .iter()
        .all(|(_, _, paths)| paths.len() > SEARCH_BATCH_BYTES));
    store.rebuild_search_index().await.unwrap();
    let after: Vec<(String, String, String)> =
        sqlx::query_as("SELECT info_hash,name,paths FROM search_documents ORDER BY info_hash")
            .fetch_all(&store.pool)
            .await
            .unwrap();
    assert_eq!(before, after);
    assert_eq!(
        store
            .search_torrents("終端文件", 10, "rank")
            .await
            .unwrap()
            .len(),
        2
    );
    let too_large = "a".repeat(SEARCH_DOCUMENT_BYTES + 1);
    let error = store
        .commit_metadata(&[51; 20], "replacement", 1, 1, &[(too_large.clone(), 1)])
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("exceeds document byte limit"));
    let unchanged: Vec<(String, String, String)> =
        sqlx::query_as("SELECT info_hash,name,paths FROM search_documents ORDER BY info_hash")
            .fetch_all(&store.pool)
            .await
            .unwrap();
    assert_eq!(before, unchanged);
    sqlx::query("UPDATE files SET path=? WHERE torrent_id=?")
        .bind(too_large)
        .bind(hex::encode([51; 20]))
        .execute(&store.pool)
        .await
        .unwrap();
    assert!(store
        .rebuild_search_index()
        .await
        .unwrap_err()
        .to_string()
        .contains("exceeds document byte limit"));
    let unchanged: Vec<(String, String, String)> =
        sqlx::query_as("SELECT info_hash,name,paths FROM search_documents ORDER BY info_hash")
            .fetch_all(&store.pool)
            .await
            .unwrap();
    assert_eq!(before, unchanged);
}

#[tokio::test]
#[ignore = "requires BTFIND_TEST_DATABASE_URL; run scripts/test-mysql.sh"]
async fn mysql_discovery_peers_are_atomic_and_replayable() {
    let store = Store::open_test_database().await.unwrap();
    let hash = [61; 20];
    let peers = vec![
        PeerContact {
            addr: "8.8.8.8:6881".parse().unwrap(),
        },
        PeerContact {
            addr: "8.8.4.4:6881".parse().unwrap(),
        },
    ];
    store
        .persist_runtime_batch(
            &[CrawlStatsEvent::HashObserved {
                info_hash: hash,
                source: crate::types::DiscoverySource::AnnouncePeer,
                has_peers: true,
                accepted: true,
                peers: peers.len(),
            }],
            None,
            "observation-before-discovery",
        )
        .await
        .unwrap();
    assert_eq!(store.get_stats().await.unwrap().total_torrents, 0);
    store.execute_batch_for_test("CREATE TRIGGER reject_peer BEFORE INSERT ON hash_job_peers FOR EACH ROW SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='peer failure'").await.unwrap();
    assert!(store
        .persist_hash_discovery(&hash, "announce_peer", &peers, 8)
        .await
        .is_err());
    assert!(store.get_torrent(&hash).await.unwrap().is_none());
    assert!(store.due_hash_jobs(10).await.unwrap().is_empty());
    assert!(store.hash_job_peers(&hash, 8).await.unwrap().is_empty());
    assert_eq!(store.get_stats().await.unwrap().total_torrents, 0);
    store
        .execute_batch_for_test("DROP TRIGGER reject_peer")
        .await
        .unwrap();
    for _ in 0..2 {
        store
            .persist_hash_discovery(&hash, "announce_peer", &peers, 8)
            .await
            .unwrap();
    }
    let reopened = Store::connect_options((*store.pool.connect_options()).clone(), 2, 9000)
        .await
        .unwrap();
    assert_eq!(reopened.get_stats().await.unwrap().total_torrents, 1);
    assert_eq!(reopened.due_hash_jobs(10).await.unwrap().len(), 1);
    let mut recovered = reopened.hash_job_peers(&hash, 8).await.unwrap();
    recovered.sort_by_key(|p| p.addr);
    let mut expected = peers.clone();
    expected.sort_by_key(|p| p.addr);
    assert_eq!(recovered, expected);
    let sources: String =
        sqlx::query_scalar("SELECT discovery_sources FROM hash_jobs WHERE info_hash=?")
            .bind(hex::encode(hash))
            .fetch_one(&store.pool)
            .await
            .unwrap();
    assert_eq!(sources, "announce_peer");
    store
        .commit_metadata(&hash, "complete", 1, 1, &[("file".into(), 1)])
        .await
        .unwrap();
    store
        .persist_hash_discovery(&hash, "announce_peer", &peers, 8)
        .await
        .unwrap();
    assert_eq!(
        store.hash_job_schedule_for_test(&hash).await.unwrap().0,
        "complete"
    );
    assert_eq!(store.get_stats().await.unwrap().total_torrents, 1);
}

#[tokio::test]
#[ignore = "requires BTFIND_TEST_DATABASE_URL; run scripts/test-mysql.sh"]
async fn mysql_peerless_hash_jobs_are_not_due() {
    let store = Store::open_test_database().await.unwrap();
    let peerless = [71; 20];
    let actionable = [72; 20];
    store
        .persist_hash_discovery(&peerless, "sample_infohashes", &[], 8)
        .await
        .unwrap();
    let peer = PeerContact {
        addr: "8.8.8.8:6881".parse().unwrap(),
    };
    store
        .persist_hash_discovery(&actionable, "announce_peer", std::slice::from_ref(&peer), 8)
        .await
        .unwrap();

    let due = store.due_hash_jobs(1).await.unwrap();
    assert_eq!(due.len(), 1);
    assert_eq!(due[0].info_hash, actionable);
    let due = store.due_hash_jobs(10).await.unwrap();
    assert_eq!(due.len(), 1);
    assert_eq!(due[0].info_hash, actionable);

    store
        .persist_hash_discovery(&peerless, "announce_peer", &[peer], 8)
        .await
        .unwrap();
    let due = store.due_hash_jobs(10).await.unwrap();
    assert_eq!(due.len(), 2);
}

#[tokio::test]
#[ignore = "requires BTFIND_TEST_DATABASE_URL; run scripts/test-mysql.sh"]
async fn mysql_discovery_retention_failure_rolls_back_and_concurrent_peers_merge() {
    let store = Store::open_test_database().await.unwrap();
    let hash = [62; 20];
    let first = PeerContact {
        addr: "8.8.8.8:6881".parse().unwrap(),
    };
    store
        .persist_hash_discovery(&hash, "first", std::slice::from_ref(&first), 1)
        .await
        .unwrap();
    store
        .update_hash_job_failure(&hash, "waiting_peers", 3, 123, "waiting")
        .await
        .unwrap();
    store.execute_batch_for_test("CREATE TRIGGER reject_retention BEFORE DELETE ON hash_job_peers FOR EACH ROW SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='retention failure'").await.unwrap();
    let second = PeerContact {
        addr: "1.1.1.1:6881".parse().unwrap(),
    };
    assert!(store
        .persist_hash_discovery(&hash, "second", &[second], 1)
        .await
        .is_err());
    assert_eq!(store.hash_job_peers(&hash, 8).await.unwrap(), vec![first]);
    assert_eq!(
        store.hash_job_schedule_for_test(&hash).await.unwrap(),
        ("waiting_peers".into(), 3, 123)
    );
    let source: String = sqlx::query_scalar("SELECT source FROM torrents WHERE info_hash=?")
        .bind(hex::encode(hash))
        .fetch_one(&store.pool)
        .await
        .unwrap();
    assert_eq!(source, "first");
    store
        .execute_batch_for_test("DROP TRIGGER reject_retention")
        .await
        .unwrap();
    let mut workers = tokio::task::JoinSet::new();
    for port in 7000..7008 {
        let worker = store.clone();
        workers.spawn(async move {
            let peer = PeerContact {
                addr: format!("9.9.9.9:{port}").parse().unwrap(),
            };
            worker
                .persist_hash_discovery(&hash, "concurrent", &[peer], 16)
                .await
                .unwrap();
        });
    }
    while let Some(result) = workers.join_next().await {
        result.unwrap();
    }
    assert_eq!(store.hash_job_peers(&hash, 16).await.unwrap().len(), 9);
    assert_eq!(
        store.hash_job_schedule_for_test(&hash).await.unwrap().0,
        "queued"
    );
    assert_eq!(store.get_stats().await.unwrap().total_torrents, 1);
    store
        .persist_hash_discovery(&hash, "peerless", &[], 4)
        .await
        .unwrap();
    assert_eq!(store.hash_job_peers(&hash, 16).await.unwrap().len(), 4);
}

#[test]
fn search_size_error_is_typed() {
    assert!(is_unsupported_search_document(&document_too_large(
        "hash", 1024
    )));
    assert!(!is_unsupported_search_document(&invalid(
        "search document hash exceeds document byte limit 1024; document was not truncated"
    )));
    assert!(!is_unsupported_search_document(&Error::PoolTimedOut));
}

#[tokio::test]
#[ignore = "requires BTFIND_TEST_DATABASE_URL; run scripts/test-mysql.sh"]
async fn mysql_due_jobs_keep_schedule_order_and_retry_eligibility() {
    let store = Store::open_test_database().await.unwrap();
    let older = [81; 20];
    let fresh = [82; 20];
    let future = [83; 20];
    let terminal = [84; 20];
    let tied = [85; 20];
    let later = [86; 20];
    let peer = PeerContact {
        addr: "192.0.2.1:6881".parse().unwrap(),
    };
    for hash in [older, fresh, future, terminal, tied, later] {
        store
            .persist_hash_discovery(&hash, "test", std::slice::from_ref(&peer), 8)
            .await
            .unwrap();
    }
    sqlx::query("UPDATE hash_jobs SET next_attempt_at=10,last_new_peer_at=100,updated_at=1")
        .execute(&store.pool)
        .await
        .unwrap();
    sqlx::query("UPDATE hash_jobs SET last_new_peer_at=200,updated_at=2 WHERE info_hash=?")
        .bind(hex::encode(fresh))
        .execute(&store.pool)
        .await
        .unwrap();
    sqlx::query("UPDATE hash_jobs SET updated_at=2 WHERE info_hash=?")
        .bind(hex::encode(tied))
        .execute(&store.pool)
        .await
        .unwrap();
    sqlx::query("UPDATE hash_jobs SET next_attempt_at=20,updated_at=0,last_new_peer_at=300 WHERE info_hash=?")
        .bind(hex::encode(later)).execute(&store.pool).await.unwrap();
    store
        .update_hash_job_failure(&future, "retry_at", 1, now() + 3600, "timeout")
        .await
        .unwrap();
    store
        .update_hash_job_failure(&terminal, "unsupported", 1, 0, "unsupported")
        .await
        .unwrap();
    let due = store.due_hash_jobs(10).await.unwrap();
    assert_eq!(
        due.iter().map(|j| j.info_hash).collect::<Vec<_>>(),
        vec![older, fresh, tied, later]
    );
    assert_eq!(store.due_hash_jobs(1).await.unwrap()[0].info_hash, older);
    store
        .persist_hash_discovery(&older, "get_peers", std::slice::from_ref(&peer), 8)
        .await
        .unwrap();
    assert_eq!(store.due_hash_jobs(1).await.unwrap()[0].info_hash, older);
    let newer_peer = PeerContact {
        addr: "192.0.2.2:6881".parse().unwrap(),
    };
    sqlx::query("UPDATE hash_job_peers SET last_seen=1 WHERE info_hash=?")
        .bind(hex::encode(older))
        .execute(&store.pool)
        .await
        .unwrap();
    store
        .persist_hash_discovery(&older, "get_peers", std::slice::from_ref(&newer_peer), 8)
        .await
        .unwrap();
    assert_eq!(
        store.hash_job_peers(&older, 8).await.unwrap(),
        vec![newer_peer, peer]
    );
}

#[tokio::test]
#[ignore = "requires BTFIND_TEST_DATABASE_URL; run scripts/test-mysql.sh"]
async fn mysql_peerless_discovery_skips_job_until_peer_bearing_arrival() {
    let store = Store::open_test_database().await.unwrap();
    let hash = [91; 20];
    let peerless = store
        .persist_hash_discovery(&hash, "sample_infohashes", &[], 8)
        .await
        .unwrap();
    assert!(peerless.torrent_inserted);
    assert!(peerless.peerless_job_skipped);
    assert!(!peerless.hash_job_created);
    assert!(store.get_torrent(&hash).await.unwrap().is_some());
    assert_eq!(store.get_stats().await.unwrap().total_torrents, 1);
    let jobs: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM hash_jobs WHERE info_hash=?")
        .bind(hex::encode(hash))
        .fetch_one(&store.pool)
        .await
        .unwrap();
    assert_eq!(jobs, 0);
    let peer = PeerContact {
        addr: "8.8.8.8:6881".parse().unwrap(),
    };
    let with_peers = store
        .persist_hash_discovery(&hash, "announce_peer", std::slice::from_ref(&peer), 8)
        .await
        .unwrap();
    assert!(!with_peers.torrent_inserted);
    assert!(with_peers.hash_job_created);
    assert!(!with_peers.peerless_job_skipped);
    assert_eq!(with_peers.peers_inserted, 1);
    let jobs: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM hash_jobs WHERE info_hash=?")
        .bind(hex::encode(hash))
        .fetch_one(&store.pool)
        .await
        .unwrap();
    assert_eq!(jobs, 1);
    let peerless_again = store
        .persist_hash_discovery(&hash, "sample_infohashes", &[], 8)
        .await
        .unwrap();
    assert!(!peerless_again.torrent_inserted);
    assert!(!peerless_again.hash_job_created);
    assert!(!peerless_again.peerless_job_skipped);
    let sources: String =
        sqlx::query_scalar("SELECT discovery_sources FROM hash_jobs WHERE info_hash=?")
            .bind(hex::encode(hash))
            .fetch_one(&store.pool)
            .await
            .unwrap();
    assert_eq!(sources, "announce_peer,sample_infohashes");
}

#[tokio::test]
#[ignore = "requires BTFIND_TEST_DATABASE_URL; run scripts/test-mysql.sh"]
async fn mysql_peer_discovery_accounting_tracks_inserts_updates_and_retention() {
    let store = Store::open_test_database().await.unwrap();
    let hash = [92; 20];
    let first = PeerContact {
        addr: "1.1.1.1:6881".parse().unwrap(),
    };
    let second = PeerContact {
        addr: "8.8.8.8:6881".parse().unwrap(),
    };
    let third = PeerContact {
        addr: "9.9.9.9:6881".parse().unwrap(),
    };
    let outcome = store
        .persist_hash_discovery(&hash, "announce_peer", &[first.clone(), second], 2)
        .await
        .unwrap();
    assert_eq!(
        (
            outcome.peers_inserted,
            outcome.peers_updated,
            outcome.peers_deleted
        ),
        (2, 0, 0)
    );
    assert_eq!(store.hash_job_peers(&hash, 8).await.unwrap().len(), 2);
    let outcome = store
        .persist_hash_discovery(&hash, "announce_peer", std::slice::from_ref(&first), 1)
        .await
        .unwrap();
    assert_eq!(
        (
            outcome.peers_inserted,
            outcome.peers_updated,
            outcome.peers_deleted
        ),
        (0, 1, 1)
    );
    assert_eq!(
        store.hash_job_peers(&hash, 8).await.unwrap(),
        vec![first.clone()]
    );
    let outcome = store
        .persist_hash_discovery(&hash, "announce_peer", &[first, third], 2)
        .await
        .unwrap();
    assert_eq!(
        (
            outcome.peers_inserted,
            outcome.peers_updated,
            outcome.peers_deleted
        ),
        (1, 1, 0)
    );
    assert_eq!(store.hash_job_peers(&hash, 8).await.unwrap().len(), 2);
}

#[tokio::test]
#[ignore = "requires BTFIND_TEST_DATABASE_URL; run scripts/test-mysql.sh"]
async fn mysql_runtime_batch_persists_only_rejected_hash_fallbacks() {
    let store = Store::open_test_database().await.unwrap();
    let rejected = [93; 20];
    let accepted = [94; 20];
    let outcome = store
        .persist_runtime_batch(
            &[
                CrawlStatsEvent::HashObserved {
                    info_hash: rejected,
                    source: crate::types::DiscoverySource::AnnouncePeer,
                    has_peers: true,
                    accepted: false,
                    peers: 3,
                },
                CrawlStatsEvent::HashObserved {
                    info_hash: accepted,
                    source: crate::types::DiscoverySource::AnnouncePeer,
                    has_peers: true,
                    accepted: true,
                    peers: 1,
                },
            ],
            None,
            "rejected-fallback",
        )
        .await
        .unwrap();
    assert_eq!(
        outcome,
        RuntimeBatchPersistence {
            torrents_inserted: 1,
            hash_jobs_created: 0,
            peerless_jobs_skipped: 1,
        }
    );
    assert!(store.get_torrent(&rejected).await.unwrap().is_some());
    assert!(store.get_torrent(&accepted).await.unwrap().is_none());
    assert_eq!(store.get_stats().await.unwrap().total_torrents, 1);
    let duplicate = store
        .persist_runtime_batch(&[], None, "rejected-fallback")
        .await
        .unwrap();
    assert_eq!(
        duplicate,
        RuntimeBatchPersistence {
            torrents_inserted: 1,
            hash_jobs_created: 0,
            peerless_jobs_skipped: 1,
        },
        "a committed receipt must replay the originally committed counts"
    );
    assert_eq!(
        store.get_stats().await.unwrap().total_torrents,
        1,
        "replaying a committed batch must not write the rows again"
    );
    let sampled = [95; 20];
    let outcome = store
        .persist_runtime_batch(
            &[CrawlStatsEvent::SampleObserved {
                info_hash: sampled,
                node_id: [7; 20],
                addr: "8.8.4.4:6881".parse().unwrap(),
                round_id: 3,
            }],
            None,
            "sample-observation",
        )
        .await
        .unwrap();
    assert_eq!(
        outcome,
        RuntimeBatchPersistence {
            torrents_inserted: 1,
            hash_jobs_created: 0,
            peerless_jobs_skipped: 1,
        }
    );
    assert!(store.get_torrent(&sampled).await.unwrap().is_some());
    assert_eq!(store.get_stats().await.unwrap().total_torrents, 2);
}

#[tokio::test]
#[ignore = "requires BTFIND_TEST_DATABASE_URL; run scripts/test-mysql.sh"]
async fn mysql_update_hash_job_failure_reports_rows_affected() {
    let store = Store::open_test_database().await.unwrap();
    let hash = [96; 20];
    let peer = PeerContact {
        addr: "8.8.8.8:6881".parse().unwrap(),
    };
    store
        .persist_hash_discovery(&hash, "announce_peer", std::slice::from_ref(&peer), 8)
        .await
        .unwrap();
    assert_eq!(
        store
            .update_hash_job_failure(&hash, "retry_at", 1, now() + 60, "timeout")
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        store
            .update_hash_job_failure(&[97; 20], "retry_at", 1, now() + 60, "missing")
            .await
            .unwrap(),
        0
    );
    store
        .commit_metadata(&hash, "complete", 1, 1, &[("file".into(), 1)])
        .await
        .unwrap();
    assert_eq!(
        store
            .update_hash_job_failure(&hash, "unsupported", 2, 0, "complete")
            .await
            .unwrap(),
        0
    );
}

#[tokio::test]
#[ignore = "requires BTFIND_TEST_DATABASE_URL; run scripts/test-mysql.sh"]
async fn mysql_metadata_completion_establishes_terminal_job() {
    let store = Store::open_test_database().await.unwrap();
    let hash = [98; 20];
    store
        .commit_metadata(&hash, "complete", 16, 10, &[("file".into(), 10)])
        .await
        .unwrap();
    assert_eq!(
        store.hash_job_schedule_for_test(&hash).await.unwrap().0,
        "complete"
    );
    assert!(store.hash_job_is_terminal(&hash).await.unwrap());
    assert!(store.due_hash_jobs(10).await.unwrap().is_empty());

    let peer = PeerContact {
        addr: "8.8.8.8:6881".parse().unwrap(),
    };
    let outcome = store
        .persist_hash_discovery(&hash, "announce_peer", std::slice::from_ref(&peer), 8)
        .await
        .unwrap();
    assert!(!outcome.torrent_inserted);
    assert!(!outcome.hash_job_created);
    assert_eq!(
        store.hash_job_schedule_for_test(&hash).await.unwrap().0,
        "complete",
        "a peer-bearing discovery must not reopen a completed metadata job"
    );
    assert!(store.hash_job_is_terminal(&hash).await.unwrap());
    assert!(store.due_hash_jobs(10).await.unwrap().is_empty());
    assert_eq!(store.get_stats().await.unwrap().total_torrents, 1);
    assert_eq!(store.hash_job_peers(&hash, 8).await.unwrap(), vec![peer]);
}

#[tokio::test]
#[ignore = "requires BTFIND_TEST_DATABASE_URL; run scripts/test-mysql.sh"]
async fn mysql_discovery_receipt_replay_returns_committed_counts() {
    let store = Store::open_test_database().await.unwrap();
    let hash = [99; 20];
    let peers = vec![
        PeerContact {
            addr: "1.1.1.1:6881".parse().unwrap(),
        },
        PeerContact {
            addr: "8.8.8.8:6881".parse().unwrap(),
        },
    ];
    let receipt = "recovered-discovery-receipt";
    let committed = DiscoveryPersistence {
        torrent_inserted: true,
        hash_job_created: true,
        peerless_job_skipped: false,
        peers_inserted: 2,
        peers_updated: 0,
        peers_deleted: 0,
    };
    sqlx::query(
        "INSERT INTO persistence_receipts(id,created_at,acknowledged_at,result) VALUES (?,?,?,?)",
    )
    .bind(receipt)
    .bind(now())
    .bind(now())
    .bind(serde_json::to_string(&committed).unwrap())
    .execute(&store.pool)
    .await
    .unwrap();
    let replayed = store
        .persist_hash_discovery_with_receipt(&hash, "announce_peer", &peers, 8, receipt)
        .await
        .unwrap();
    assert_eq!(replayed, committed);
    assert!(
        store.get_torrent(&hash).await.unwrap().is_none(),
        "replaying a committed receipt must not repeat the transaction"
    );

    let fresh_receipt = "fresh-discovery-receipt";
    let first = store
        .persist_hash_discovery_with_receipt(&hash, "announce_peer", &peers, 8, fresh_receipt)
        .await
        .unwrap();
    assert!(first.torrent_inserted);
    assert!(first.hash_job_created);
    assert_eq!(first.peers_inserted, 2);
    let receipt_rows: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM persistence_receipts WHERE id=?")
            .bind(fresh_receipt)
            .fetch_one(&store.pool)
            .await
            .unwrap();
    assert_eq!(receipt_rows, 0, "processed receipts are cleaned up");
}

#[tokio::test]
#[ignore = "requires BTFIND_TEST_DATABASE_URL; run scripts/test-mysql.sh"]
async fn mysql_metadata_backlog_separates_status_and_due_work() {
    let store = Store::open_test_database().await.unwrap();
    let peer = PeerContact {
        addr: "8.8.8.8:6881".parse().unwrap(),
    };
    let hashes = [[201u8; 20], [202u8; 20], [203u8; 20], [204u8; 20]];
    for hash in hashes {
        store
            .persist_hash_discovery(&hash, "test", std::slice::from_ref(&peer), 8)
            .await
            .unwrap();
    }
    let stamp = now();
    sqlx::query(
        "UPDATE hash_jobs SET status='queued',next_attempt_at=?,updated_at=? WHERE info_hash=?",
    )
    .bind(stamp - 30)
    .bind(stamp - 30)
    .bind(hex::encode(hashes[0]))
    .execute(&store.pool)
    .await
    .unwrap();
    sqlx::query("UPDATE hash_jobs SET status='retry_at',next_attempt_at=? WHERE info_hash=?")
        .bind(stamp + 3600)
        .bind(hex::encode(hashes[1]))
        .execute(&store.pool)
        .await
        .unwrap();
    sqlx::query("UPDATE hash_jobs SET status='waiting_peers',next_attempt_at=0 WHERE info_hash=?")
        .bind(hex::encode(hashes[2]))
        .execute(&store.pool)
        .await
        .unwrap();
    sqlx::query("UPDATE hash_jobs SET status='queued',next_attempt_at=0 WHERE info_hash=?")
        .bind(hex::encode(hashes[3]))
        .execute(&store.pool)
        .await
        .unwrap();
    sqlx::query("DELETE FROM hash_job_peers WHERE info_hash=?")
        .bind(hex::encode(hashes[3]))
        .execute(&store.pool)
        .await
        .unwrap();

    let backlog = store.metadata_backlog().await.unwrap();
    assert_eq!(backlog.queued, 2);
    assert_eq!(backlog.retry_at, 1);
    assert_eq!(backlog.waiting_peers, 1);
    assert_eq!(backlog.due, 1);
    assert!(
        backlog.oldest_due_age_seconds >= 30,
        "oldest due age was {}",
        backlog.oldest_due_age_seconds
    );
}

#[tokio::test]
#[ignore = "requires BTFIND_TEST_DATABASE_URL; run scripts/test-mysql.sh"]
async fn mysql_repeated_identical_discovery_does_not_report_job_creation() {
    let store = Store::open_test_database().await.unwrap();
    let peer = PeerContact {
        addr: "8.8.8.8:6881".parse().unwrap(),
    };
    let mut verified_same_second = false;
    for attempt in 0..10u8 {
        let mut hash = [100u8; 20];
        hash[19] = attempt;
        let before = now();
        let first = store
            .persist_hash_discovery(&hash, "announce_peer", std::slice::from_ref(&peer), 8)
            .await
            .unwrap();
        let second = store
            .persist_hash_discovery(&hash, "announce_peer", std::slice::from_ref(&peer), 8)
            .await
            .unwrap();
        let after = now();
        assert!(first.torrent_inserted);
        assert!(first.hash_job_created);
        assert!(!second.torrent_inserted);
        assert!(
            !second.hash_job_created,
            "an unchanged existing job must not be counted as newly created"
        );
        let jobs: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM hash_jobs WHERE info_hash=?")
            .bind(hex::encode(hash))
            .fetch_one(&store.pool)
            .await
            .unwrap();
        assert_eq!(jobs, 1);
        if before == after {
            verified_same_second = true;
            break;
        }
    }
    assert!(
        verified_same_second,
        "could not exercise the same-second unchanged-row path"
    );
}
