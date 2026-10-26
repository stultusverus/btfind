use super::*;

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
        self.persist_hash_discovery(&hash, source, false).await?;
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
    assert_eq!(store.prune_old_torrents(1).await.unwrap(), 1);
    assert_eq!(store.get_stats().await.unwrap().total_size, 5);
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
        .persist_hash_discovery(&[8; 20], "dht", false)
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
