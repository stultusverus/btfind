use crate::types::{InfoHash, NodeContact, NodeId};
use rusqlite::types::Value;
use rusqlite::{params, params_from_iter, Connection};
use std::net::SocketAddrV4;
use std::path::Path;
use std::sync::Mutex;
use std::time::{Duration, Instant};

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

#[derive(Debug, serde::Serialize)]
pub struct CrawlStats {
    pub total_torrents: i64,
    pub total_metadata_complete: i64,
    pub total_files: i64,
    pub total_size: i64,
}

#[derive(Debug)]
#[allow(dead_code)]
pub struct CrawlStatRecord {
    #[allow(dead_code)]
    pub id: i64,
    #[allow(dead_code)]
    pub timestamp: i64,
    pub nodes_known: Option<i64>,
    #[allow(dead_code)]
    pub queries_sent: Option<i64>,
    #[allow(dead_code)]
    pub info_hashes_found: Option<i64>,
    pub metadata_fetched: Option<i64>,
}

#[derive(Debug, PartialEq, Eq)]
pub struct MetadataFailureCount {
    pub reason: String,
    pub count: i64,
}

pub struct Store {
    conn: Mutex<Connection>,
}

#[derive(Debug)]
pub struct TorrentSearchPage {
    pub results: Vec<TorrentRecord>,
    pub total: i64,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct TorrentSearchFilters {
    pub complete_only: bool,
    pub min_size: Option<i64>,
    pub max_size: Option<i64>,
}

impl Store {
    pub fn open(path: &Path) -> Result<Self, rusqlite::Error> {
        let conn = Connection::open(path)?;
        let store = Store {
            conn: Mutex::new(conn),
        };
        store.init_schema()?;
        store.backfill_fts_if_needed()?;
        Ok(store)
    }

    #[allow(dead_code)]
    pub fn open_in_memory() -> Result<Self, rusqlite::Error> {
        let conn = Connection::open_in_memory()?;
        let store = Store {
            conn: Mutex::new(conn),
        };
        store.init_schema()?;
        Ok(store)
    }

    fn init_schema(&self) -> Result<(), rusqlite::Error> {
        let conn = self.conn.lock().expect("store connection mutex poisoned");
        conn.execute_batch(
            "
            CREATE TABLE IF NOT EXISTS torrents (
                info_hash         TEXT PRIMARY KEY,
                name              TEXT,
                piece_length      INTEGER,
                total_size        INTEGER NOT NULL DEFAULT 0,
                file_count        INTEGER NOT NULL DEFAULT 0,
                source            TEXT NOT NULL DEFAULT 'unknown',
                first_seen        INTEGER NOT NULL,
                last_seen         INTEGER NOT NULL,
                last_attempt      INTEGER NOT NULL DEFAULT 0,
                metadata_complete INTEGER NOT NULL DEFAULT 0
            );

            CREATE TABLE IF NOT EXISTS files (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                torrent_id  TEXT NOT NULL REFERENCES torrents(info_hash) ON DELETE CASCADE,
                path        TEXT NOT NULL,
                size        INTEGER NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_torrents_first_seen ON torrents(first_seen);
            CREATE INDEX IF NOT EXISTS idx_torrents_last_seen ON torrents(last_seen);
            CREATE INDEX IF NOT EXISTS idx_torrents_name ON torrents(name);
            CREATE INDEX IF NOT EXISTS idx_files_torrent ON files(torrent_id);

            CREATE TABLE IF NOT EXISTS crawl_stats (
                id                INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp         INTEGER NOT NULL,
                nodes_known       INTEGER,
                queries_sent      INTEGER,
                info_hashes_found INTEGER,
                metadata_fetched  INTEGER
            );

            CREATE TABLE IF NOT EXISTS metadata_peer_attempts (
                info_hash    TEXT NOT NULL REFERENCES torrents(info_hash) ON DELETE CASCADE,
                peer_addr    TEXT NOT NULL,
                last_attempt INTEGER NOT NULL,
                last_error   TEXT,
                PRIMARY KEY (info_hash, peer_addr)
            );

            CREATE TABLE IF NOT EXISTS metadata_failure_counts (
                reason TEXT PRIMARY KEY,
                count  INTEGER NOT NULL DEFAULT 0
            );

            CREATE INDEX IF NOT EXISTS idx_metadata_peer_attempts_hash
                ON metadata_peer_attempts(info_hash);

            CREATE TABLE IF NOT EXISTS dht_nodes (
                id        TEXT PRIMARY KEY,
                addr      TEXT NOT NULL,
                last_seen INTEGER NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_dht_nodes_last_seen ON dht_nodes(last_seen);

            CREATE VIRTUAL TABLE IF NOT EXISTS torrent_fts USING fts5(
                info_hash UNINDEXED,
                name,
                paths,
                tokenize = 'trigram'
            );

            PRAGMA journal_mode=WAL;
            PRAGMA foreign_keys=ON;
            ",
        )?;

        let has_last_attempt = {
            let mut stmt = conn.prepare("PRAGMA table_info(torrents)")?;
            let columns = stmt.query_map([], |row| row.get::<_, String>(1))?;
            let mut found = false;
            for column in columns {
                if column? == "last_attempt" {
                    found = true;
                    break;
                }
            }
            found
        };

        if !has_last_attempt {
            conn.execute(
                "ALTER TABLE torrents ADD COLUMN last_attempt INTEGER NOT NULL DEFAULT 0",
                [],
            )?;
        }

        Ok(())
    }

    pub fn upsert_torrent(
        &self,
        info_hash: InfoHash,
        name: Option<String>,
        total_size: i64,
        file_count: i64,
        source: &str,
    ) -> Result<i64, rusqlite::Error> {
        let conn = self.conn.lock().expect("store connection mutex poisoned");
        let hash_hex = hex::encode(info_hash);
        let now = chrono::Utc::now().timestamp();

        let existing: Option<(i64, bool)> = match conn.query_row(
            "SELECT first_seen, metadata_complete FROM torrents WHERE info_hash = ?1",
            params![hash_hex],
            |row| Ok((row.get(0)?, row.get::<_, i32>(1)? != 0)),
        ) {
            Ok(row) => Some(row),
            Err(rusqlite::Error::QueryReturnedNoRows) => None,
            Err(e) => return Err(e),
        };

        let (first_seen, metadata_complete) = match existing {
            Some((fs, mc)) => (fs, mc),
            None => (now, false),
        };

        if existing.is_some() && name.is_none() && total_size == 0 && file_count == 0 {
            conn.execute(
                "UPDATE torrents SET last_seen = ?1, source = ?2 WHERE info_hash = ?3",
                params![now, source, hash_hex],
            )?;
            return Ok(0);
        }

        conn.execute(
            "INSERT INTO torrents (info_hash, name, total_size, file_count, source, first_seen, last_seen, metadata_complete)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
             ON CONFLICT(info_hash) DO UPDATE SET
                 name = COALESCE(?2, name),
                 total_size = CASE WHEN ?3 != 0 THEN ?3 ELSE total_size END,
                 file_count = CASE WHEN ?4 != 0 THEN ?4 ELSE file_count END,
                 source = ?5,
                 last_seen = ?7,
                 metadata_complete = MAX(metadata_complete, ?8)",
            params![
                hash_hex,
                name,
                total_size,
                file_count,
                source,
                first_seen,
                now,
                if metadata_complete { 1i32 } else { 0i32 },
            ],
        )?;
        let rowid = conn.last_insert_rowid();
        Self::refresh_torrent_fts_locked(&conn, &hash_hex)?;
        Ok(rowid)
    }

    pub fn mark_metadata_complete(
        &self,
        info_hash: &InfoHash,
        name: &str,
        piece_length: i64,
        total_size: i64,
        file_count: i64,
    ) -> Result<(), rusqlite::Error> {
        let conn = self.conn.lock().expect("store connection mutex poisoned");
        let hash_hex = hex::encode(info_hash);
        let updated = conn.execute(
            "UPDATE torrents SET name = ?2, piece_length = ?3, total_size = ?4, file_count = ?5, metadata_complete = 1, last_seen = unixepoch()
             WHERE info_hash = ?1",
            params![hash_hex, name, piece_length, total_size, file_count],
        )?;
        if updated == 0 {
            return Err(rusqlite::Error::QueryReturnedNoRows);
        }
        Self::refresh_torrent_fts_locked(&conn, &hash_hex)?;
        Ok(())
    }

    pub fn insert_files(
        &self,
        info_hash: &InfoHash,
        files: &[(String, i64)],
    ) -> Result<(), rusqlite::Error> {
        let mut conn = self.conn.lock().expect("store connection mutex poisoned");
        let hash_hex = hex::encode(info_hash);

        let tx = conn.transaction()?;
        tx.execute("DELETE FROM files WHERE torrent_id = ?1", params![hash_hex])?;

        {
            let mut stmt =
                tx.prepare("INSERT INTO files (torrent_id, path, size) VALUES (?1, ?2, ?3)")?;
            for (path, size) in files {
                stmt.execute(params![hash_hex, path, size])?;
            }
        }

        Self::refresh_torrent_fts_locked(&tx, &hash_hex)?;
        tx.commit()?;
        Ok(())
    }

    pub fn get_torrent(
        &self,
        info_hash: &InfoHash,
    ) -> Result<Option<TorrentRecord>, rusqlite::Error> {
        let conn = self.conn.lock().expect("store connection mutex poisoned");
        let hash_hex = hex::encode(info_hash);

        let mut stmt = conn.prepare(
            "SELECT info_hash, name, piece_length, total_size, file_count, source, first_seen, last_seen, last_attempt, metadata_complete
             FROM torrents WHERE info_hash = ?1",
        )?;

        let mut rows = stmt.query_map(params![hash_hex], row_to_torrent)?;
        match rows.next() {
            Some(Ok(record)) => Ok(Some(record)),
            Some(Err(e)) => Err(e),
            None => Ok(None),
        }
    }

    pub fn search_torrents(
        &self,
        query: &str,
        limit: usize,
        sort_by: &str,
    ) -> Result<Vec<TorrentRecord>, rusqlite::Error> {
        self.search_torrents_filtered(query, limit, sort_by, TorrentSearchFilters::default())
    }

    pub fn search_torrents_filtered(
        &self,
        query: &str,
        limit: usize,
        sort_by: &str,
        filters: TorrentSearchFilters,
    ) -> Result<Vec<TorrentRecord>, rusqlite::Error> {
        Ok(self
            .search_torrents_filtered_page(query, limit, 0, sort_by, filters)?
            .results)
    }

    pub fn search_torrents_filtered_page(
        &self,
        query: &str,
        limit: usize,
        offset: usize,
        sort_by: &str,
        filters: TorrentSearchFilters,
    ) -> Result<TorrentSearchPage, rusqlite::Error> {
        let conn = self.conn.lock().expect("store connection mutex poisoned");
        let query_trimmed = query.trim();

        if query_trimmed.is_empty() {
            let order = sort_order_sql(sort_by);
            let mut base_sql =
                "SELECT info_hash, name, piece_length, total_size, file_count, source, first_seen, last_seen, last_attempt, metadata_complete
                 FROM torrents t"
                    .to_string();
            let mut params = Vec::new();
            append_filter_clauses(&mut base_sql, &mut params, false, "t", filters);
            let mut count_sql = "SELECT COUNT(*) FROM torrents t".to_string();
            let mut count_params = Vec::new();
            append_filter_clauses(&mut count_sql, &mut count_params, false, "t", filters);
            let total = count_search_results(&conn, &count_sql, count_params)?;
            let mut sql = base_sql;
            sql.push_str(&format!(
                " ORDER BY {}
                 LIMIT ? OFFSET ?",
                order
            ));
            params.push(Value::from(limit as i64));
            params.push(Value::from(offset as i64));
            let mut stmt = conn.prepare(&sql)?;
            let rows = stmt.query_map(params_from_iter(params), row_to_torrent)?;
            let mut results = Vec::new();
            for row in rows {
                results.push(row?);
            }
            return Ok(TorrentSearchPage { results, total });
        }

        if query_trimmed.chars().count() < 3 {
            let pattern = format!("%{}%", escape_like_literal(query_trimmed));
            let effective_sort = if sort_by == "rank" {
                "last_seen"
            } else {
                sort_by
            };
            let order = sort_order_sql(effective_sort);
            let mut base_sql =
                "SELECT info_hash, name, piece_length, total_size, file_count, source, first_seen, last_seen, last_attempt, metadata_complete
                 FROM torrents t
                 WHERE (COALESCE(t.name, '') LIKE ?1 ESCAPE '\\'
                    OR EXISTS (
                        SELECT 1 FROM files f
                        WHERE f.torrent_id = t.info_hash
                          AND f.path LIKE ?1 ESCAPE '\\'
                    ))"
                    .to_string();
            let mut params = vec![Value::from(pattern.clone())];
            append_filter_clauses(&mut base_sql, &mut params, true, "t", filters);
            let mut count_sql = "SELECT COUNT(*) FROM torrents t
                 WHERE (COALESCE(t.name, '') LIKE ?1 ESCAPE '\\'
                    OR EXISTS (
                        SELECT 1 FROM files f
                        WHERE f.torrent_id = t.info_hash
                          AND f.path LIKE ?1 ESCAPE '\\'
                    ))"
            .to_string();
            let mut count_params = vec![Value::from(pattern)];
            append_filter_clauses(&mut count_sql, &mut count_params, true, "t", filters);
            let total = count_search_results(&conn, &count_sql, count_params)?;
            let mut sql = base_sql;
            sql.push_str(&format!(
                " ORDER BY {}
                 LIMIT ? OFFSET ?",
                order
            ));
            params.push(Value::from(limit as i64));
            params.push(Value::from(offset as i64));
            let mut stmt = conn.prepare(&sql)?;
            let rows = stmt.query_map(params_from_iter(params), row_to_torrent)?;
            let mut results = Vec::new();
            for row in rows {
                results.push(row?);
            }
            return Ok(TorrentSearchPage { results, total });
        }

        let fts_query = fts_literal_query(query_trimmed);

        if sort_by == "rank" {
            let mut base_sql = "SELECT t.info_hash, t.name, t.piece_length, t.total_size, t.file_count, t.source, t.first_seen, t.last_seen, t.last_attempt, t.metadata_complete
                       FROM torrent_fts f
                       JOIN torrents t ON t.info_hash = f.info_hash
                       WHERE torrent_fts MATCH ?"
                .to_string();
            let mut params = vec![Value::from(fts_query.clone())];
            append_filter_clauses(&mut base_sql, &mut params, true, "t", filters);
            let mut count_sql = "SELECT COUNT(*)
                       FROM torrent_fts f
                       JOIN torrents t ON t.info_hash = f.info_hash
                       WHERE torrent_fts MATCH ?"
                .to_string();
            let mut count_params = vec![Value::from(fts_query)];
            append_filter_clauses(&mut count_sql, &mut count_params, true, "t", filters);
            let total = count_search_results(&conn, &count_sql, count_params)?;
            let mut sql = base_sql;
            sql.push_str(
                " ORDER BY bm25(torrent_fts, 1.0, 5.0, 1.0), t.last_seen DESC
                       LIMIT ? OFFSET ?",
            );
            params.push(Value::from(limit as i64));
            params.push(Value::from(offset as i64));
            let mut stmt = conn.prepare(&sql)?;
            let rows = stmt.query_map(params_from_iter(params), row_to_torrent)?;
            let mut results = Vec::new();
            for row in rows {
                results.push(row?);
            }
            return Ok(TorrentSearchPage { results, total });
        }

        let order = sort_order_sql(sort_by);
        let mut base_sql =
            "SELECT t.info_hash, t.name, t.piece_length, t.total_size, t.file_count, t.source, t.first_seen, t.last_seen, t.last_attempt, t.metadata_complete
             FROM torrent_fts f
             JOIN torrents t ON t.info_hash = f.info_hash
             WHERE torrent_fts MATCH ?"
                .to_string();
        let mut params = vec![Value::from(fts_query.clone())];
        append_filter_clauses(&mut base_sql, &mut params, true, "t", filters);
        let mut count_sql = "SELECT COUNT(*)
             FROM torrent_fts f
             JOIN torrents t ON t.info_hash = f.info_hash
             WHERE torrent_fts MATCH ?"
            .to_string();
        let mut count_params = vec![Value::from(fts_query)];
        append_filter_clauses(&mut count_sql, &mut count_params, true, "t", filters);
        let total = count_search_results(&conn, &count_sql, count_params)?;
        let mut sql = base_sql;
        sql.push_str(&format!(
            " ORDER BY {}
             LIMIT ? OFFSET ?",
            order
        ));
        params.push(Value::from(limit as i64));
        params.push(Value::from(offset as i64));

        let mut stmt = conn.prepare(&sql)?;
        let rows = stmt.query_map(params_from_iter(params), row_to_torrent)?;
        let mut results = Vec::new();
        for row in rows {
            results.push(row?);
        }
        Ok(TorrentSearchPage { results, total })
    }

    pub fn get_stats(&self) -> Result<CrawlStats, rusqlite::Error> {
        let conn = self.conn.lock().expect("store connection mutex poisoned");
        conn.query_row(
            "SELECT
                COUNT(*) as total,
                COALESCE(SUM(CASE WHEN metadata_complete = 1 THEN 1 ELSE 0 END), 0) as complete,
                COALESCE((SELECT COUNT(*) FROM files), 0) as files_total,
                COALESCE(SUM(total_size), 0) as total_size
             FROM torrents",
            [],
            |row| {
                Ok(CrawlStats {
                    total_torrents: row.get(0)?,
                    total_metadata_complete: row.get(1)?,
                    total_files: row.get(2)?,
                    total_size: row.get(3)?,
                })
            },
        )
    }

    #[allow(dead_code)]
    pub fn insert_crawl_stat(
        &self,
        nodes_known: i64,
        queries_sent: i64,
        info_hashes_found: i64,
        metadata_fetched: i64,
    ) -> Result<i64, rusqlite::Error> {
        let conn = self.conn.lock().expect("store connection mutex poisoned");
        let now = chrono::Utc::now().timestamp();
        conn.execute(
            "INSERT INTO crawl_stats (timestamp, nodes_known, queries_sent, info_hashes_found, metadata_fetched)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            params![now, nodes_known, queries_sent, info_hashes_found, metadata_fetched],
        )?;
        Ok(conn.last_insert_rowid())
    }

    #[allow(dead_code)]
    pub fn get_crawl_history(&self, limit: usize) -> Result<Vec<CrawlStatRecord>, rusqlite::Error> {
        let conn = self.conn.lock().expect("store connection mutex poisoned");
        let mut stmt = conn.prepare(
            "SELECT id, timestamp, nodes_known, queries_sent, info_hashes_found, metadata_fetched
             FROM crawl_stats
             ORDER BY timestamp DESC
             LIMIT ?1",
        )?;
        let rows = stmt.query_map(params![limit as i64], |row| {
            Ok(CrawlStatRecord {
                id: row.get(0)?,
                timestamp: row.get(1)?,
                nodes_known: row.get(2)?,
                queries_sent: row.get(3)?,
                info_hashes_found: row.get(4)?,
                metadata_fetched: row.get(5)?,
            })
        })?;
        let mut results = Vec::new();
        for row in rows {
            results.push(row?);
        }
        Ok(results)
    }

    pub fn prune_old_torrents(&self, days: i64) -> Result<i64, rusqlite::Error> {
        let conn = self.conn.lock().expect("store connection mutex poisoned");
        let cutoff = chrono::Utc::now().timestamp() - days * 86400;

        let mut hashes = Vec::new();
        {
            let mut stmt = conn.prepare("SELECT info_hash FROM torrents WHERE last_seen < ?1")?;
            let rows = stmt.query_map(params![cutoff], |row| row.get::<_, String>(0))?;
            for row in rows {
                hashes.push(row?);
            }
        }

        for h in &hashes {
            conn.execute("DELETE FROM torrent_fts WHERE info_hash = ?1", params![h])?;
        }
        let removed = conn.execute("DELETE FROM torrents WHERE last_seen < ?1", params![cutoff])?;
        Ok(removed as i64)
    }

    pub fn incomplete_info_hashes(&self, limit: usize) -> Result<Vec<InfoHash>, rusqlite::Error> {
        let conn = self.conn.lock().expect("store connection mutex poisoned");
        let mut stmt = conn.prepare(
            "SELECT info_hash
             FROM torrents
             WHERE metadata_complete = 0
             ORDER BY last_seen DESC, first_seen DESC, info_hash DESC
             LIMIT ?1",
        )?;
        let rows = stmt.query_map(params![limit as i64], |row| {
            let hash_hex: String = row.get(0)?;
            info_hash_from_db_hex(&hash_hex)
        })?;
        let mut hashes = Vec::new();
        for row in rows {
            hashes.push(row?);
        }
        Ok(hashes)
    }

    pub fn upsert_dht_node(
        &self,
        node_id: &NodeId,
        addr: SocketAddrV4,
    ) -> Result<(), rusqlite::Error> {
        let conn = self.conn.lock().expect("store connection mutex poisoned");
        let now = chrono::Utc::now().timestamp();
        conn.execute(
            "INSERT INTO dht_nodes (id, addr, last_seen)
             VALUES (?1, ?2, ?3)
             ON CONFLICT(id) DO UPDATE SET
                addr = excluded.addr,
                last_seen = excluded.last_seen",
            params![hex::encode(node_id), addr.to_string(), now],
        )?;
        Ok(())
    }

    pub fn recent_dht_nodes(
        &self,
        max_age: Duration,
        limit: usize,
    ) -> Result<Vec<NodeContact>, rusqlite::Error> {
        let conn = self.conn.lock().expect("store connection mutex poisoned");
        let cutoff = chrono::Utc::now().timestamp() - max_age.as_secs() as i64;
        let mut stmt = conn.prepare(
            "SELECT id, addr
             FROM dht_nodes
             WHERE last_seen >= ?1
             ORDER BY last_seen DESC
             LIMIT ?2",
        )?;
        let rows = stmt.query_map(params![cutoff, limit as i64], row_to_dht_node)?;
        let mut nodes = Vec::new();
        for row in rows {
            nodes.push(row?);
        }
        Ok(nodes)
    }

    pub fn prune_stale_dht_nodes(&self, max_age: Duration) -> Result<i64, rusqlite::Error> {
        let conn = self.conn.lock().expect("store connection mutex poisoned");
        let cutoff = chrono::Utc::now().timestamp() - max_age.as_secs() as i64;
        let removed = conn.execute(
            "DELETE FROM dht_nodes WHERE last_seen < ?1",
            params![cutoff],
        )?;
        Ok(removed as i64)
    }

    pub fn set_peer_attempt(
        &self,
        info_hash: &InfoHash,
        peer_addr: &str,
        last_error: Option<&str>,
    ) -> Result<(), rusqlite::Error> {
        let conn = self.conn.lock().expect("store connection mutex poisoned");
        let hash_hex = hex::encode(info_hash);
        let now = chrono::Utc::now().timestamp();

        conn.execute(
            "INSERT INTO metadata_peer_attempts (info_hash, peer_addr, last_attempt, last_error)
             VALUES (?1, ?2, ?3, ?4)
             ON CONFLICT(info_hash, peer_addr) DO UPDATE SET
                 last_attempt = excluded.last_attempt,
                 last_error = excluded.last_error",
            params![hash_hex, peer_addr, now, last_error],
        )?;
        conn.execute(
            "UPDATE torrents SET last_attempt = ?1 WHERE info_hash = ?2",
            params![now, hash_hex],
        )?;
        Ok(())
    }

    pub fn should_skip_peer_retry(
        &self,
        info_hash: &InfoHash,
        peer_addr: &str,
        retry_after_hours: u32,
    ) -> Result<bool, rusqlite::Error> {
        if retry_after_hours == 0 {
            return Ok(false);
        }

        let conn = self.conn.lock().expect("store connection mutex poisoned");
        let hash_hex = hex::encode(info_hash);
        let cutoff = chrono::Utc::now().timestamp() - (retry_after_hours as i64) * 3600;
        let count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM metadata_peer_attempts
             WHERE info_hash = ?1 AND peer_addr = ?2 AND last_attempt > ?3",
            params![hash_hex, peer_addr, cutoff],
            |row| row.get(0),
        )?;
        Ok(count > 0)
    }

    pub fn prune_stale_peer_attempts(&self, max_age: Duration) -> Result<i64, rusqlite::Error> {
        let conn = self.conn.lock().expect("store connection mutex poisoned");
        let cutoff = chrono::Utc::now().timestamp() - max_age.as_secs() as i64;
        let removed = conn.execute(
            "DELETE FROM metadata_peer_attempts WHERE last_attempt < ?1",
            params![cutoff],
        )?;
        Ok(removed as i64)
    }

    pub fn increment_metadata_failure(&self, reason: &str) -> Result<(), rusqlite::Error> {
        let conn = self.conn.lock().expect("store connection mutex poisoned");
        conn.execute(
            "INSERT INTO metadata_failure_counts (reason, count)
             VALUES (?1, 1)
             ON CONFLICT(reason) DO UPDATE SET count = count + 1",
            params![reason],
        )?;
        Ok(())
    }

    pub fn get_metadata_failure_counts(
        &self,
    ) -> Result<Vec<MetadataFailureCount>, rusqlite::Error> {
        let conn = self.conn.lock().expect("store connection mutex poisoned");
        let mut stmt = conn.prepare(
            "SELECT reason, count
             FROM metadata_failure_counts
             ORDER BY count DESC, reason ASC",
        )?;
        let rows = stmt.query_map([], |row| {
            Ok(MetadataFailureCount {
                reason: row.get(0)?,
                count: row.get(1)?,
            })
        })?;
        let mut counts = Vec::new();
        for row in rows {
            counts.push(row?);
        }
        Ok(counts)
    }

    fn refresh_torrent_fts_locked(
        conn: &Connection,
        info_hash_hex: &str,
    ) -> Result<(), rusqlite::Error> {
        conn.execute(
            "DELETE FROM torrent_fts WHERE info_hash = ?1",
            params![info_hash_hex],
        )?;

        let name: Option<String> = conn
            .query_row(
                "SELECT name FROM torrents WHERE info_hash = ?1 AND metadata_complete = 1",
                params![info_hash_hex],
                |row| row.get(0),
            )
            .ok();

        if name.is_none() || name.as_deref() == Some("") {
            let file_count: i64 = conn.query_row(
                "SELECT COUNT(*) FROM files WHERE torrent_id = ?1",
                params![info_hash_hex],
                |row| row.get(0),
            )?;
            if file_count == 0 {
                return Ok(());
            }
        }

        let mut paths = Vec::new();
        {
            let mut stmt =
                conn.prepare("SELECT path FROM files WHERE torrent_id = ?1 ORDER BY path")?;
            let rows = stmt.query_map(params![info_hash_hex], |row| row.get::<_, String>(0))?;
            for row in rows {
                paths.push(row?);
            }
        }
        let paths_joined = paths.join(" ");

        let name_val = name.unwrap_or_default();

        conn.execute(
            "INSERT INTO torrent_fts (info_hash, name, paths) VALUES (?1, ?2, ?3)",
            params![info_hash_hex, name_val, paths_joined],
        )?;

        Ok(())
    }

    pub fn refresh_torrent_fts(&self, info_hash: &InfoHash) -> Result<(), rusqlite::Error> {
        let conn = self.conn.lock().expect("store connection mutex poisoned");
        let hash_hex = hex::encode(info_hash);
        Self::refresh_torrent_fts_locked(&conn, &hash_hex)
    }

    pub fn rebuild_search_index(&self) -> Result<(), rusqlite::Error> {
        let conn = self.conn.lock().expect("store connection mutex poisoned");
        conn.execute("DELETE FROM torrent_fts", [])?;

        let mut hashes = Vec::new();
        {
            let mut stmt =
                conn.prepare("SELECT info_hash FROM torrents WHERE metadata_complete = 1")?;
            let rows = stmt.query_map([], |row| row.get::<_, String>(0))?;
            for row in rows {
                hashes.push(row?);
            }
        }

        for hash_hex in &hashes {
            Self::refresh_torrent_fts_locked(&conn, hash_hex)?;
        }

        Ok(())
    }

    fn backfill_fts_if_needed(&self) -> Result<(), rusqlite::Error> {
        let conn = self.conn.lock().expect("store connection mutex poisoned");

        let fts_count: i64 =
            conn.query_row("SELECT COUNT(*) FROM torrent_fts", [], |row| row.get(0))?;
        if fts_count > 0 {
            return Ok(());
        }

        let complete_count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM torrents WHERE metadata_complete = 1",
            [],
            |row| row.get(0),
        )?;

        if complete_count == 0 {
            return Ok(());
        }

        drop(conn);
        self.rebuild_search_index()
    }

    #[allow(dead_code)]
    pub fn get_files(&self, info_hash: &InfoHash) -> Result<Vec<(String, i64)>, rusqlite::Error> {
        let conn = self.conn.lock().expect("store connection mutex poisoned");
        let hash_hex = hex::encode(info_hash);
        let mut stmt =
            conn.prepare("SELECT path, size FROM files WHERE torrent_id = ?1 ORDER BY path")?;
        let rows = stmt.query_map(params![hash_hex], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
        })?;
        let mut results = Vec::new();
        for row in rows {
            results.push(row?);
        }
        Ok(results)
    }

    #[allow(dead_code)]
    pub fn get_torrent_by_hex(
        &self,
        info_hash_hex: &str,
    ) -> Result<Option<TorrentRecord>, rusqlite::Error> {
        let conn = self.conn.lock().expect("store connection mutex poisoned");
        let mut stmt = conn.prepare(
            "SELECT info_hash, name, piece_length, total_size, file_count, source, first_seen, last_seen, last_attempt, metadata_complete
             FROM torrents WHERE info_hash = ?1",
        )?;
        let mut rows = stmt.query_map(params![info_hash_hex], row_to_torrent)?;
        match rows.next() {
            Some(Ok(record)) => Ok(Some(record)),
            Some(Err(e)) => Err(e),
            None => Ok(None),
        }
    }
}

#[cfg(test)]
impl Store {
    pub(crate) fn execute_batch_for_test(&self, sql: &str) -> Result<(), rusqlite::Error> {
        self.conn
            .lock()
            .expect("store connection mutex poisoned")
            .execute_batch(sql)
    }
}

fn sort_order_sql(sort_by: &str) -> &'static str {
    match sort_by {
        "first_seen" => "first_seen DESC",
        "total_size" => "total_size DESC",
        "name" => "name ASC",
        _ => "last_seen DESC",
    }
}

fn append_filter_clauses(
    sql: &mut String,
    params: &mut Vec<Value>,
    has_where: bool,
    alias: &str,
    filters: TorrentSearchFilters,
) {
    let mut needs_and = has_where;
    if filters.complete_only {
        append_filter_prefix(sql, &mut needs_and);
        sql.push_str(alias);
        sql.push_str(".metadata_complete = 1");
    }
    if let Some(min_size) = filters.min_size {
        append_filter_prefix(sql, &mut needs_and);
        sql.push_str(alias);
        sql.push_str(".total_size >= ?");
        params.push(Value::from(min_size));
    }
    if let Some(max_size) = filters.max_size {
        append_filter_prefix(sql, &mut needs_and);
        sql.push_str(alias);
        sql.push_str(".total_size <= ?");
        params.push(Value::from(max_size));
    }
}

fn append_filter_prefix(sql: &mut String, needs_and: &mut bool) {
    if *needs_and {
        sql.push_str(" AND ");
    } else {
        sql.push_str(" WHERE ");
        *needs_and = true;
    }
}

fn count_search_results(
    conn: &Connection,
    sql: &str,
    params: Vec<Value>,
) -> Result<i64, rusqlite::Error> {
    conn.query_row(sql, params_from_iter(params), |row| row.get(0))
}

fn fts_literal_query(input: &str) -> String {
    format!("\"{}\"", input.replace('"', "\"\""))
}

fn escape_like_literal(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    for ch in input.chars() {
        match ch {
            '%' | '_' | '\\' => {
                out.push('\\');
                out.push(ch);
            }
            _ => out.push(ch),
        }
    }
    out
}

fn row_to_torrent(row: &rusqlite::Row) -> rusqlite::Result<TorrentRecord> {
    let hash_hex: String = row.get(0)?;
    let info_hash = info_hash_from_db_hex(&hash_hex)?;

    Ok(TorrentRecord {
        info_hash,
        name: row.get(1)?,
        piece_length: row.get(2)?,
        total_size: row.get(3)?,
        file_count: row.get(4)?,
        source: row.get(5)?,
        first_seen: row.get(6)?,
        last_seen: row.get(7)?,
        last_attempt: row.get(8)?,
        metadata_complete: row.get::<_, i32>(9)? != 0,
    })
}

fn info_hash_from_db_hex(hash_hex: &str) -> rusqlite::Result<InfoHash> {
    let hash_bytes = hex::decode(hash_hex).map_err(|e| {
        rusqlite::Error::FromSqlConversionFailure(0, rusqlite::types::Type::Text, Box::new(e))
    })?;
    if hash_bytes.len() != 20 {
        return Err(rusqlite::Error::FromSqlConversionFailure(
            0,
            rusqlite::types::Type::Text,
            Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "info_hash must be 20 bytes",
            )),
        ));
    }
    let mut info_hash: InfoHash = [0u8; 20];
    info_hash.copy_from_slice(&hash_bytes);
    Ok(info_hash)
}

fn row_to_dht_node(row: &rusqlite::Row) -> rusqlite::Result<NodeContact> {
    let node_hex: String = row.get(0)?;
    let node_bytes = hex::decode(&node_hex).map_err(|e| {
        rusqlite::Error::FromSqlConversionFailure(0, rusqlite::types::Type::Text, Box::new(e))
    })?;
    if node_bytes.len() != 20 {
        return Err(rusqlite::Error::FromSqlConversionFailure(
            0,
            rusqlite::types::Type::Text,
            Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "node id must be 20 bytes",
            )),
        ));
    }
    let mut id: NodeId = [0u8; 20];
    id.copy_from_slice(&node_bytes);

    let addr_text: String = row.get(1)?;
    let addr = addr_text.parse::<SocketAddrV4>().map_err(|e| {
        rusqlite::Error::FromSqlConversionFailure(1, rusqlite::types::Type::Text, Box::new(e))
    })?;

    Ok(NodeContact {
        id,
        addr,
        last_seen: Instant::now(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_db() -> Store {
        Store::open_in_memory().expect("should open in-memory db")
    }

    #[test]
    fn test_insert_and_get_torrent() {
        let store = test_db();
        let info_hash: InfoHash = [1u8; 20];

        let id = store
            .upsert_torrent(info_hash, Some("test torrent".into()), 1024, 2, "dht")
            .expect("should insert");
        assert!(id > 0);

        let torrent = store.get_torrent(&info_hash).expect("should get");
        assert!(torrent.is_some());
        let t = torrent.unwrap();
        assert_eq!(t.name, Some("test torrent".to_string()));
        assert_eq!(t.total_size, 1024);
        assert_eq!(t.file_count, 2);
        assert!(!t.metadata_complete);
    }

    #[test]
    fn test_insert_duplicate_info_hash() {
        let store = test_db();
        let info_hash = [2u8; 20];

        store.upsert_torrent(info_hash, None, 0, 0, "dht").unwrap();
        store
            .upsert_torrent(info_hash, Some("updated".into()), 2048, 3, "dht")
            .unwrap();

        let t = store.get_torrent(&info_hash).unwrap().unwrap();
        assert_eq!(t.name, Some("updated".to_string()));
        assert_eq!(t.total_size, 2048);
    }

    #[test]
    fn test_search_torrents() {
        let store = test_db();
        let h1 = [1u8; 20];
        let h2 = [2u8; 20];
        let h3 = [3u8; 20];

        store
            .upsert_torrent(h1, Some("ubuntu iso".into()), 1_000_000, 1, "dht")
            .unwrap();
        store
            .upsert_torrent(h2, Some("debian iso".into()), 2_000_000, 1, "dht")
            .unwrap();
        store
            .upsert_torrent(h3, Some("fedora iso".into()), 3_000_000, 1, "dht")
            .unwrap();

        store
            .mark_metadata_complete(&h1, "ubuntu iso", 262144, 1_000_000, 1)
            .unwrap();
        store
            .insert_files(&h1, &[("ubuntu.iso".into(), 1_000_000)])
            .unwrap();
        store.refresh_torrent_fts(&h1).unwrap();

        store
            .mark_metadata_complete(&h2, "debian iso", 262144, 2_000_000, 1)
            .unwrap();
        store
            .insert_files(&h2, &[("debian.iso".into(), 2_000_000)])
            .unwrap();
        store.refresh_torrent_fts(&h2).unwrap();

        store
            .mark_metadata_complete(&h3, "fedora iso", 262144, 3_000_000, 1)
            .unwrap();
        store
            .insert_files(&h3, &[("fedora.iso".into(), 3_000_000)])
            .unwrap();
        store.refresh_torrent_fts(&h3).unwrap();

        let results = store.search_torrents("ubuntu", 10, "last_seen").unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].name.as_deref(), Some("ubuntu iso"));

        let results = store.search_torrents("iso", 10, "last_seen").unwrap();
        assert_eq!(results.len(), 3);
    }

    #[test]
    fn test_prune() {
        let store = test_db();
        store.upsert_torrent([1u8; 20], None, 0, 0, "dht").unwrap();

        // Manually set last_seen to old
        store
            .conn
            .lock()
            .expect("store connection mutex poisoned")
            .execute(
                "UPDATE torrents SET last_seen = unixepoch() - 86400 * 100 WHERE info_hash = ?1",
                rusqlite::params![hex::encode([1u8; 20])],
            )
            .unwrap();

        let removed = store.prune_old_torrents(90).unwrap();
        assert_eq!(removed, 1);
    }

    #[test]
    fn test_stats() {
        let store = test_db();
        store
            .upsert_torrent([1u8; 20], Some("a".into()), 1000, 1, "dht")
            .unwrap();
        store
            .upsert_torrent([2u8; 20], Some("b".into()), 2000, 2, "dht")
            .unwrap();

        let stats = store.get_stats().unwrap();
        assert!(stats.total_torrents >= 2);
    }

    #[test]
    fn test_crawl_stat_insert() {
        let store = test_db();
        let id = store.insert_crawl_stat(100, 50, 25, 5).unwrap();
        assert!(id > 0);

        let history = store.get_crawl_history(10).unwrap();
        assert_eq!(history.len(), 1);
        assert_eq!(history[0].nodes_known, Some(100));
        assert_eq!(history[0].metadata_fetched, Some(5));
    }

    #[test]
    fn test_fk_cascade() {
        let store = test_db();
        let info_hash = [0xAAu8; 20];
        let hash_hex = hex::encode(info_hash);

        store
            .upsert_torrent(info_hash, Some("test".into()), 1000, 1, "dht")
            .unwrap();
        store
            .insert_files(
                &info_hash,
                &[
                    ("file1.txt".to_string(), 500),
                    ("file2.txt".to_string(), 500),
                ],
            )
            .unwrap();

        // Verify files exist
        let count: i64 = store
            .conn
            .lock()
            .expect("store connection mutex poisoned")
            .query_row(
                "SELECT COUNT(*) FROM files WHERE torrent_id = ?1",
                rusqlite::params![hash_hex],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 2);

        // Delete torrent, verify files cascade
        store
            .conn
            .lock()
            .expect("store connection mutex poisoned")
            .execute(
                "DELETE FROM torrents WHERE info_hash = ?1",
                rusqlite::params![hash_hex],
            )
            .unwrap();

        let count: i64 = store
            .conn
            .lock()
            .expect("store connection mutex poisoned")
            .query_row(
                "SELECT COUNT(*) FROM files WHERE torrent_id = ?1",
                rusqlite::params![hash_hex],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 0);
    }

    #[test]
    fn test_upsert_preserves_files() {
        let store = test_db();
        let info_hash = [0xBBu8; 20];
        let hash_hex = hex::encode(info_hash);

        store
            .upsert_torrent(info_hash, Some("test".into()), 1000, 1, "dht")
            .unwrap();
        store
            .insert_files(&info_hash, &[("file1.txt".to_string(), 500)])
            .unwrap();

        store
            .upsert_torrent(info_hash, Some("updated name".into()), 2000, 2, "dht")
            .unwrap();

        let count: i64 = store
            .conn
            .lock()
            .expect("store connection mutex poisoned")
            .query_row(
                "SELECT COUNT(*) FROM files WHERE torrent_id = ?1",
                rusqlite::params![hash_hex],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 1, "files should survive upsert");

        let t = store.get_torrent(&info_hash).unwrap().unwrap();
        assert_eq!(t.name, Some("updated name".to_string()));
        assert_eq!(t.total_size, 2000);
    }

    #[test]
    fn test_invalid_stored_hash_returns_error() {
        let store = test_db();
        let bad_hex = "not_a_hex_string!!";
        // Insert a row with an invalid info_hash directly
        store
            .conn
            .lock()
            .expect("store connection mutex poisoned")
            .execute(
                "INSERT INTO torrents (info_hash, name, first_seen, last_seen) VALUES (?1, ?2, unixepoch(), unixepoch())",
                rusqlite::params![bad_hex, "bad hash test"],
            )
            .unwrap();
        let result = store.conn.lock().expect("store connection mutex poisoned").query_row(
            "SELECT info_hash, name, piece_length, total_size, file_count, source, first_seen, last_seen, last_attempt, metadata_complete FROM torrents WHERE info_hash = ?1",
            rusqlite::params![bad_hex],
            row_to_torrent,
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_open_migrates_old_schema_with_last_attempt() {
        fn remove_old_schema_temp_files() {
            if let Ok(entries) = std::fs::read_dir(std::env::temp_dir()) {
                for entry in entries.flatten() {
                    let path = entry.path();
                    if path
                        .file_name()
                        .and_then(|name| name.to_str())
                        .is_some_and(|name| {
                            name.starts_with("btfind-old-schema-") && name.contains(".sqlite")
                        })
                    {
                        std::fs::remove_file(path).ok();
                    }
                }
            }
        }

        struct TempSqliteFiles {
            path: std::path::PathBuf,
        }

        impl Drop for TempSqliteFiles {
            fn drop(&mut self) {
                std::fs::remove_file(&self.path).ok();
                std::fs::remove_file(format!("{}-wal", self.path.display())).ok();
                std::fs::remove_file(format!("{}-shm", self.path.display())).ok();
                remove_old_schema_temp_files();
            }
        }

        remove_old_schema_temp_files();
        let temp = TempSqliteFiles {
            path: std::env::temp_dir().join(format!(
                "btfind-old-schema-{}.sqlite",
                chrono::Utc::now().timestamp_nanos_opt().unwrap_or_default()
            )),
        };
        {
            let conn = Connection::open(&temp.path).unwrap();
            conn.execute_batch(
                "
                CREATE TABLE torrents (
                    info_hash         TEXT PRIMARY KEY,
                    name              TEXT,
                    piece_length      INTEGER,
                    total_size        INTEGER NOT NULL DEFAULT 0,
                    file_count        INTEGER NOT NULL DEFAULT 0,
                    source            TEXT NOT NULL DEFAULT 'unknown',
                    first_seen        INTEGER NOT NULL,
                    last_seen         INTEGER NOT NULL,
                    metadata_complete INTEGER NOT NULL DEFAULT 0
                );
                INSERT INTO torrents (info_hash, name, first_seen, last_seen)
                VALUES ('dddddddddddddddddddddddddddddddddddddddd', 'old', unixepoch(), unixepoch());
                ",
            )
            .unwrap();
        }

        let store = Store::open(&temp.path).unwrap();
        let torrent = store.get_torrent(&[0xDDu8; 20]).unwrap().unwrap();
        assert_eq!(torrent.name.as_deref(), Some("old"));
        assert_eq!(torrent.last_attempt, 0);
    }

    #[test]
    fn test_insert_files_rolls_back_after_insert_error() {
        let store = test_db();
        let info_hash = [0xEEu8; 20];
        let hash_hex = hex::encode(info_hash);
        store
            .upsert_torrent(info_hash, Some("test".into()), 1000, 1, "dht")
            .unwrap();
        store
            .insert_files(&info_hash, &[("keep.txt".to_string(), 1000)])
            .unwrap();
        store
            .conn
            .lock()
            .expect("store connection mutex poisoned")
            .execute_batch(
                "
                CREATE TRIGGER fail_file_insert
                BEFORE INSERT ON files
                BEGIN
                    SELECT RAISE(ABORT, 'forced insert failure');
                END;
                ",
            )
            .unwrap();

        let result = store.insert_files(&info_hash, &[("replace.txt".to_string(), 1)]);
        assert!(result.is_err());

        let files: Vec<(String, i64)> = {
            let conn = store.conn.lock().expect("store connection mutex poisoned");
            let mut stmt = conn
                .prepare("SELECT path, size FROM files WHERE torrent_id = ?1 ORDER BY path")
                .unwrap();
            stmt.query_map(rusqlite::params![hash_hex], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
            })
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap()
        };
        assert_eq!(files, vec![("keep.txt".to_string(), 1000)]);
    }

    #[test]
    fn test_peer_retry_suppression_is_per_peer() {
        let store = test_db();
        let info_hash: InfoHash = [0xCDu8; 20];
        let peer_a = "1.1.1.1:6881";
        let peer_b = "8.8.8.8:51413";

        store.upsert_torrent(info_hash, None, 0, 0, "dht").unwrap();

        assert!(!store
            .should_skip_peer_retry(&info_hash, peer_a, 24)
            .unwrap());

        store
            .set_peer_attempt(&info_hash, peer_a, Some("connect error"))
            .unwrap();

        assert!(store
            .should_skip_peer_retry(&info_hash, peer_a, 24)
            .unwrap());
        assert!(!store
            .should_skip_peer_retry(&info_hash, peer_b, 24)
            .unwrap());
        assert!(!store.should_skip_peer_retry(&info_hash, peer_a, 0).unwrap());
    }

    #[test]
    fn test_recent_dht_nodes_filters_and_prunes_stale_nodes() {
        let store = test_db();
        let fresh_id = [0x10u8; 20];
        let stale_id = [0x20u8; 20];
        let fresh_addr = "8.8.8.8:6881".parse().unwrap();
        let stale_addr = "1.1.1.1:6881".parse().unwrap();

        store.upsert_dht_node(&fresh_id, fresh_addr).unwrap();
        store.upsert_dht_node(&stale_id, stale_addr).unwrap();
        store
            .execute_batch_for_test(&format!(
                "UPDATE dht_nodes SET last_seen = unixepoch() - 7200 WHERE id = '{}'",
                hex::encode(stale_id)
            ))
            .unwrap();

        let nodes = store
            .recent_dht_nodes(std::time::Duration::from_secs(3600), 10)
            .unwrap();
        assert_eq!(nodes.len(), 1);
        assert_eq!(nodes[0].id, fresh_id);
        assert_eq!(nodes[0].addr, fresh_addr);

        let pruned = store
            .prune_stale_dht_nodes(std::time::Duration::from_secs(3600))
            .unwrap();

        assert_eq!(pruned, 1);
        assert_eq!(
            store
                .recent_dht_nodes(std::time::Duration::from_secs(10_000), 10)
                .unwrap()
                .len(),
            1
        );
    }

    #[test]
    fn test_incomplete_info_hashes_returns_only_unfinished_rows() {
        let store = test_db();
        let incomplete_old = [0x31u8; 20];
        let incomplete_new = [0x32u8; 20];
        let complete = [0x33u8; 20];

        store
            .upsert_torrent(incomplete_old, None, 0, 0, "dht")
            .unwrap();
        store
            .upsert_torrent(incomplete_new, None, 0, 0, "dht")
            .unwrap();
        store.upsert_torrent(complete, None, 0, 0, "dht").unwrap();
        store
            .mark_metadata_complete(&complete, "done", 16384, 1, 1)
            .unwrap();

        let hashes = store.incomplete_info_hashes(10).unwrap();

        assert_eq!(hashes, vec![incomplete_new, incomplete_old]);
    }

    #[test]
    fn test_prune_stale_peer_attempts_removes_old_attempts() {
        let store = test_db();
        let info_hash = [0x34u8; 20];
        let fresh_peer = "8.8.8.8:6881";
        let stale_peer = "1.1.1.1:6881";
        store.upsert_torrent(info_hash, None, 0, 0, "dht").unwrap();
        store
            .set_peer_attempt(&info_hash, fresh_peer, Some("timeout"))
            .unwrap();
        store
            .set_peer_attempt(&info_hash, stale_peer, Some("connect"))
            .unwrap();
        store
            .execute_batch_for_test(&format!(
                "UPDATE metadata_peer_attempts SET last_attempt = unixepoch() - 7200 WHERE peer_addr = '{}'",
                stale_peer
            ))
            .unwrap();

        let pruned = store
            .prune_stale_peer_attempts(std::time::Duration::from_secs(3600))
            .unwrap();

        assert_eq!(pruned, 1);
        assert!(!store
            .should_skip_peer_retry(&info_hash, stale_peer, 24)
            .unwrap());
        assert!(store
            .should_skip_peer_retry(&info_hash, fresh_peer, 24)
            .unwrap());
    }

    #[test]
    fn test_metadata_failure_counts_increment_by_reason() {
        let store = test_db();

        store.increment_metadata_failure("connect").unwrap();
        store.increment_metadata_failure("connect").unwrap();
        store.increment_metadata_failure("timeout").unwrap();

        let counts = store.get_metadata_failure_counts().unwrap();

        assert_eq!(
            counts
                .iter()
                .map(|record| (record.reason.as_str(), record.count))
                .collect::<Vec<_>>(),
            vec![("connect", 2), ("timeout", 1)]
        );
    }

    #[test]
    fn test_mark_metadata_complete_errors_for_unknown_hash() {
        let store = test_db();
        let result = store.mark_metadata_complete(&[0xFAu8; 20], "missing", 16384, 42, 1);

        assert!(matches!(result, Err(rusqlite::Error::QueryReturnedNoRows)));
    }

    #[test]
    fn test_fts5_available() {
        let store = test_db();
        let conn = store.conn.lock().expect("store connection mutex poisoned");
        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='torrent_fts'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 1);
    }

    #[test]
    fn test_fts_search_by_name() {
        let store = test_db();
        let info_hash = [0xA1u8; 20];

        store
            .upsert_torrent(
                info_hash,
                Some("Ubuntu 24.04 LTS".into()),
                4_000_000_000,
                1,
                "dht",
            )
            .unwrap();
        store
            .mark_metadata_complete(&info_hash, "Ubuntu 24.04 LTS", 262144, 4_000_000_000, 1)
            .unwrap();
        store
            .insert_files(&info_hash, &[("ubuntu-24.04.iso".into(), 4_000_000_000)])
            .unwrap();
        store.refresh_torrent_fts(&info_hash).unwrap();

        let results = store.search_torrents("Ubuntu", 10, "rank").unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].name.as_deref(), Some("Ubuntu 24.04 LTS"));
    }

    #[test]
    fn test_fts_search_by_file_path() {
        let store = test_db();
        let info_hash = [0xA2u8; 20];

        store
            .upsert_torrent(
                info_hash,
                Some("Some Movie".into()),
                2_000_000_000,
                1,
                "dht",
            )
            .unwrap();
        store
            .mark_metadata_complete(&info_hash, "Some Movie", 262144, 2_000_000_000, 1)
            .unwrap();
        store
            .insert_files(
                &info_hash,
                &[("movies/action/some_movie.mkv".into(), 2_000_000_000)],
            )
            .unwrap();
        store.refresh_torrent_fts(&info_hash).unwrap();

        let results = store.search_torrents("action", 10, "rank").unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].name.as_deref(), Some("Some Movie"));
    }

    #[test]
    fn test_fts_search_empty_query_returns_all() {
        let store = test_db();

        for i in 0u8..5 {
            let info_hash = [i; 20];
            store
                .upsert_torrent(info_hash, Some(format!("torrent_{i}")), 1000, 1, "dht")
                .unwrap();
        }

        let results = store.search_torrents("", 10, "last_seen").unwrap();
        assert!(results.len() >= 5);
    }

    #[test]
    fn test_fts_search_rank_sort() {
        let store = test_db();

        let h1 = [0xB1u8; 20];
        let h2 = [0xB2u8; 20];

        store
            .upsert_torrent(h1, Some("Ubuntu Server LTS".into()), 1_000_000, 1, "dht")
            .unwrap();
        store
            .mark_metadata_complete(&h1, "Ubuntu Server LTS", 262144, 1_000_000, 1)
            .unwrap();
        store
            .insert_files(&h1, &[("ubuntu.iso".into(), 1_000_000)])
            .unwrap();
        store.refresh_torrent_fts(&h1).unwrap();

        store
            .upsert_torrent(h2, Some("Debian Linux".into()), 1_000_000, 1, "dht")
            .unwrap();
        store
            .mark_metadata_complete(&h2, "Debian Linux", 262144, 1_000_000, 1)
            .unwrap();
        store
            .insert_files(&h2, &[("debian/ubuntu.iso".into(), 1_000_000)])
            .unwrap();
        store.refresh_torrent_fts(&h2).unwrap();

        let results = store.search_torrents("ubuntu", 10, "rank").unwrap();
        assert!(!results.is_empty());
        assert_eq!(results[0].name.as_deref(), Some("Ubuntu Server LTS"));
    }

    #[test]
    fn test_fts_rebuild_index() {
        let store = test_db();
        let info_hash = [0xC1u8; 20];

        store
            .upsert_torrent(info_hash, Some("Test Torrent".into()), 1_000, 1, "dht")
            .unwrap();
        store
            .mark_metadata_complete(&info_hash, "Test Torrent", 262144, 1_000, 1)
            .unwrap();
        store
            .insert_files(&info_hash, &[("test/file.dat".into(), 1_000)])
            .unwrap();

        store
            .conn
            .lock()
            .expect("store connection mutex poisoned")
            .execute("DELETE FROM torrent_fts", [])
            .unwrap();

        store.rebuild_search_index().unwrap();

        let results = store.search_torrents("file", 10, "rank").unwrap();
        assert_eq!(results.len(), 1);
    }

    #[test]
    fn test_fts_search_invalid_query_does_not_panic() {
        let store = test_db();
        let result = store.search_torrents("\"", 10, "rank");
        assert!(result.is_err() || result.is_ok());
    }

    #[test]
    fn test_get_files() {
        let store = test_db();
        let info_hash = [0xD1u8; 20];

        store
            .upsert_torrent(info_hash, Some("test".into()), 1000, 2, "dht")
            .unwrap();
        store
            .insert_files(
                &info_hash,
                &[("a.txt".to_string(), 500), ("b.txt".to_string(), 500)],
            )
            .unwrap();

        let files = store.get_files(&info_hash).unwrap();
        assert_eq!(files.len(), 2);
        assert_eq!(files[0].0, "a.txt");
        assert_eq!(files[1].0, "b.txt");
    }

    #[test]
    fn test_get_torrent_by_hex() {
        let store = test_db();
        let info_hash = [0xE1u8; 20];
        let hash_hex = hex::encode(info_hash);

        store
            .upsert_torrent(info_hash, Some("test".into()), 1000, 1, "dht")
            .unwrap();

        let t = store.get_torrent_by_hex(&hash_hex).unwrap().unwrap();
        assert_eq!(t.name.as_deref(), Some("test"));

        let missing = store
            .get_torrent_by_hex("ffffffffffffffffffffffffffffffffffffffff")
            .unwrap();
        assert!(missing.is_none());
    }

    #[test]
    fn test_fts_search_punctuation_ubuntu_version() {
        let store = test_db();
        let hash = [0xA6u8; 20];
        store
            .upsert_torrent(hash, Some("ubuntu-24.04".into()), 1_000_000, 1, "dht")
            .unwrap();
        store
            .mark_metadata_complete(&hash, "ubuntu-24.04", 262144, 1_000_000, 1)
            .unwrap();
        store
            .insert_files(&hash, &[("ubuntu.iso".into(), 1_000_000)])
            .unwrap();

        let results = store.search_torrents("ubuntu-24.04", 10, "rank").unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].name.as_deref(), Some("ubuntu-24.04"));
    }

    #[test]
    fn test_fts_search_punctuation_dot() {
        let store = test_db();
        let hash = [0xA7u8; 20];
        store
            .upsert_torrent(hash, Some("foo.bar".into()), 1_000_000, 1, "dht")
            .unwrap();
        store
            .mark_metadata_complete(&hash, "foo.bar", 262144, 1_000_000, 1)
            .unwrap();
        store
            .insert_files(&hash, &[("foo.bar.zip".into(), 1_000_000)])
            .unwrap();

        let results = store.search_torrents("foo.bar", 10, "rank").unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].name.as_deref(), Some("foo.bar"));
    }

    #[test]
    fn test_fts_search_punctuation_plus() {
        let store = test_db();
        let hash = [0xA8u8; 20];
        store
            .upsert_torrent(hash, Some("C++ Primer".into()), 1_000_000, 1, "dht")
            .unwrap();
        store
            .mark_metadata_complete(&hash, "C++ Primer", 262144, 1_000_000, 1)
            .unwrap();
        store
            .insert_files(&hash, &[("cpp_primer.pdf".into(), 1_000_000)])
            .unwrap();

        let results = store.search_torrents("C++", 10, "rank").unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].name.as_deref(), Some("C++ Primer"));
    }

    #[test]
    fn test_fts_search_punctuation_brackets() {
        let store = test_db();
        let hash = [0xA9u8; 20];
        store
            .upsert_torrent(hash, Some("[SubsPlease] Show".into()), 1_000_000, 1, "dht")
            .unwrap();
        store
            .mark_metadata_complete(&hash, "[SubsPlease] Show", 262144, 1_000_000, 1)
            .unwrap();
        store
            .insert_files(&hash, &[("[SubsPlease] Show.mkv".into(), 1_000_000)])
            .unwrap();

        let results = store.search_torrents("[SubsPlease]", 10, "rank").unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].name.as_deref(), Some("[SubsPlease] Show"));
    }

    #[test]
    fn test_fts_search_punctuation_slash() {
        let store = test_db();
        let hash = [0x55u8; 20];
        store
            .upsert_torrent(hash, Some("Movie".into()), 1_000_000, 1, "dht")
            .unwrap();
        store
            .mark_metadata_complete(&hash, "Movie", 262144, 1_000_000, 1)
            .unwrap();
        store
            .insert_files(&hash, &[("dir/file.mkv".into(), 1_000_000)])
            .unwrap();

        let results = store.search_torrents("dir/file.mkv", 10, "rank").unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].name.as_deref(), Some("Movie"));
    }

    #[test]
    fn test_fts_freshness_immediate_file_path_search() {
        let store = test_db();
        let hash = [0xABu8; 20];
        store
            .upsert_torrent(hash, Some("Fresh Test".into()), 1_000, 1, "dht")
            .unwrap();
        store
            .mark_metadata_complete(&hash, "Fresh Test", 262144, 1_000, 1)
            .unwrap();
        store
            .insert_files(&hash, &[("video/unique-file-token-xyz.mkv".into(), 1_000)])
            .unwrap();

        let results = store
            .search_torrents("unique-file-token-xyz", 10, "rank")
            .unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].name.as_deref(), Some("Fresh Test"));
    }

    #[test]
    fn test_fts_freshness_file_path_replacement() {
        let store = test_db();
        let hash = [0xACu8; 20];
        store
            .upsert_torrent(hash, Some("Replace Test".into()), 1_000, 1, "dht")
            .unwrap();
        store
            .mark_metadata_complete(&hash, "Replace Test", 262144, 1_000, 1)
            .unwrap();
        store
            .insert_files(&hash, &[("old-token-abc.txt".into(), 500)])
            .unwrap();

        let results = store.search_torrents("old-token-abc", 10, "rank").unwrap();
        assert_eq!(results.len(), 1);

        store
            .insert_files(&hash, &[("new-token-xyz.txt".into(), 500)])
            .unwrap();

        let results = store.search_torrents("new-token-xyz", 10, "rank").unwrap();
        assert_eq!(results.len(), 1);

        let results = store.search_torrents("old-token-abc", 10, "rank").unwrap();
        assert_eq!(results.len(), 0);
    }

    #[test]
    fn test_fts_prune_removes_stale_entries() {
        let store = test_db();
        let hash = [0xADu8; 20];
        let hash_hex = hex::encode(hash);

        store
            .upsert_torrent(hash, Some("PruneFts".into()), 1_000, 1, "dht")
            .unwrap();
        store
            .mark_metadata_complete(&hash, "PruneFts", 262144, 1_000, 1)
            .unwrap();
        store
            .insert_files(&hash, &[("unique-stale-token-qwe.mkv".into(), 1_000)])
            .unwrap();

        let results = store
            .search_torrents("unique-stale-token-qwe", 10, "rank")
            .unwrap();
        assert_eq!(results.len(), 1);

        store
            .conn
            .lock()
            .expect("store connection mutex poisoned")
            .execute(
                "UPDATE torrents SET last_seen = unixepoch() - 86400 * 100 WHERE info_hash = ?1",
                rusqlite::params![hash_hex],
            )
            .unwrap();

        store.prune_old_torrents(90).unwrap();

        let results = store
            .search_torrents("unique-stale-token-qwe", 10, "rank")
            .unwrap();
        assert_eq!(results.len(), 0);
    }

    #[test]
    fn test_fts_rank_weights_name_higher_than_paths() {
        let store = test_db();
        let h1 = [0xAEu8; 20];
        let h2 = [0xAFu8; 20];

        store
            .upsert_torrent(h1, Some("ubuntu release".into()), 1_000_000, 1, "dht")
            .unwrap();
        store
            .mark_metadata_complete(&h1, "ubuntu release", 262144, 1_000_000, 1)
            .unwrap();
        store
            .insert_files(&h1, &[("release.iso".into(), 1_000_000)])
            .unwrap();

        store
            .upsert_torrent(h2, Some("random release".into()), 1_000_000, 1, "dht")
            .unwrap();
        store
            .mark_metadata_complete(&h2, "random release", 262144, 1_000_000, 1)
            .unwrap();
        store
            .insert_files(&h2, &[("ubuntu.iso".into(), 1_000_000)])
            .unwrap();

        let results = store.search_torrents("ubuntu", 10, "rank").unwrap();
        assert_eq!(results.len(), 2);
        assert_eq!(
            results[0].name.as_deref(),
            Some("ubuntu release"),
            "name match should rank higher than path match"
        );
    }

    #[test]
    fn test_filtered_search_complete_only_applies_before_limit() {
        let store = test_db();
        let incomplete = [0xC0u8; 20];
        let complete = [0xC1u8; 20];

        store
            .upsert_torrent(incomplete, Some("alpha unknown".into()), 999, 0, "dht")
            .unwrap();
        store
            .upsert_torrent(complete, Some("alpha complete".into()), 1_000, 1, "dht")
            .unwrap();
        store
            .mark_metadata_complete(&complete, "alpha complete", 262144, 1_000, 1)
            .unwrap();
        store
            .insert_files(&complete, &[("alpha/file.dat".into(), 1_000)])
            .unwrap();

        let results = store
            .search_torrents_filtered(
                "alpha",
                1,
                "last_seen",
                TorrentSearchFilters {
                    complete_only: true,
                    min_size: None,
                    max_size: None,
                },
            )
            .unwrap();

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].name.as_deref(), Some("alpha complete"));
        assert!(results[0].metadata_complete);
    }

    #[test]
    fn test_filtered_empty_search_complete_only_applies_before_limit() {
        let store = test_db();
        let incomplete = [0xC5u8; 20];
        let complete = [0xC6u8; 20];

        store
            .upsert_torrent(incomplete, Some("unknown".into()), 999, 0, "dht")
            .unwrap();
        store
            .upsert_torrent(complete, Some("complete".into()), 1_000, 1, "dht")
            .unwrap();
        store
            .mark_metadata_complete(&complete, "complete", 262144, 1_000, 1)
            .unwrap();
        store
            .insert_files(&complete, &[("file.dat".into(), 1_000)])
            .unwrap();

        let results = store
            .search_torrents_filtered(
                "",
                1,
                "last_seen",
                TorrentSearchFilters {
                    complete_only: true,
                    min_size: None,
                    max_size: None,
                },
            )
            .unwrap();

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].name.as_deref(), Some("complete"));
        assert!(results[0].metadata_complete);
    }

    #[test]
    fn test_filtered_search_page_returns_total_and_offset_results() {
        let store = test_db();

        for (idx, name) in ["alpha", "bravo", "charlie"].into_iter().enumerate() {
            let hash = [0xD5u8 + idx as u8; 20];
            store
                .upsert_torrent(hash, Some(name.into()), 1_000, 1, "dht")
                .unwrap();
            store
                .mark_metadata_complete(&hash, name, 262144, 1_000, 1)
                .unwrap();
            store
                .insert_files(&hash, &[("file.dat".into(), 1_000)])
                .unwrap();
        }

        let page = store
            .search_torrents_filtered_page(
                "",
                2,
                1,
                "name",
                TorrentSearchFilters {
                    complete_only: true,
                    min_size: None,
                    max_size: None,
                },
            )
            .unwrap();

        assert_eq!(page.total, 3);
        assert_eq!(page.results.len(), 2);
        assert_eq!(page.results[0].name.as_deref(), Some("bravo"));
        assert_eq!(page.results[1].name.as_deref(), Some("charlie"));
    }

    #[test]
    fn test_filtered_search_size_range() {
        let store = test_db();
        let small = [0xC2u8; 20];
        let medium = [0xC3u8; 20];
        let large = [0xC4u8; 20];

        for (hash, name, size) in [
            (small, "linux small", 500),
            (medium, "linux medium", 2_000),
            (large, "linux large", 5_000),
        ] {
            store
                .upsert_torrent(hash, Some(name.into()), size, 1, "dht")
                .unwrap();
            store
                .mark_metadata_complete(&hash, name, 262144, size, 1)
                .unwrap();
            store
                .insert_files(&hash, &[("linux/file.bin".into(), size)])
                .unwrap();
        }

        let results = store
            .search_torrents_filtered(
                "linux",
                10,
                "total_size",
                TorrentSearchFilters {
                    complete_only: true,
                    min_size: Some(1_000),
                    max_size: Some(3_000),
                },
            )
            .unwrap();

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].name.as_deref(), Some("linux medium"));
    }

    #[test]
    fn test_search_short_query_like_name() {
        let store = test_db();
        let hash = [0xB0u8; 20];
        store
            .upsert_torrent(hash, Some("aBc".into()), 1_000, 1, "dht")
            .unwrap();
        store
            .mark_metadata_complete(&hash, "aBc", 262144, 1_000, 1)
            .unwrap();
        store
            .insert_files(&hash, &[("file.dat".into(), 1_000)])
            .unwrap();

        let results = store.search_torrents("a", 10, "last_seen").unwrap();
        assert_eq!(results.len(), 1);
    }

    #[test]
    fn test_search_short_query_like_path() {
        let store = test_db();
        let hash = [0x5Cu8; 20];
        store
            .upsert_torrent(hash, Some("Some Movie".into()), 1_000, 1, "dht")
            .unwrap();
        store
            .mark_metadata_complete(&hash, "Some Movie", 262144, 1_000, 1)
            .unwrap();
        store
            .insert_files(&hash, &[("xy/video.mkv".into(), 1_000)])
            .unwrap();

        let results = store.search_torrents("xy", 10, "last_seen").unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].name.as_deref(), Some("Some Movie"));
    }

    #[test]
    fn test_search_literal_wildcard_percent() {
        let store = test_db();
        let hash_pct = [0x5Du8; 20];
        let hash_reg = [0x5Eu8; 20];

        store
            .upsert_torrent(hash_pct, Some("100% done".into()), 1_000, 1, "dht")
            .unwrap();
        store
            .mark_metadata_complete(&hash_pct, "100% done", 262144, 1_000, 1)
            .unwrap();
        store
            .insert_files(&hash_pct, &[("file.dat".into(), 1_000)])
            .unwrap();

        store
            .upsert_torrent(hash_reg, Some("other thing".into()), 1_000, 1, "dht")
            .unwrap();
        store
            .mark_metadata_complete(&hash_reg, "other thing", 262144, 1_000, 1)
            .unwrap();
        store
            .insert_files(&hash_reg, &[("data.bin".into(), 1_000)])
            .unwrap();

        let results = store.search_torrents("%", 10, "last_seen").unwrap();
        assert_eq!(
            results.len(),
            1,
            "should match only the torrent with literal %"
        );
        assert_eq!(results[0].name.as_deref(), Some("100% done"));
    }

    #[test]
    fn test_search_literal_wildcard_underscore() {
        let store = test_db();
        let hash_us = [0xB4u8; 20];
        let hash_reg = [0xB5u8; 20];

        store
            .upsert_torrent(hash_us, Some("my_file".into()), 1_000, 1, "dht")
            .unwrap();
        store
            .mark_metadata_complete(&hash_us, "my_file", 262144, 1_000, 1)
            .unwrap();
        store
            .insert_files(&hash_us, &[("data.bin".into(), 1_000)])
            .unwrap();

        store
            .upsert_torrent(hash_reg, Some("another".into()), 1_000, 1, "dht")
            .unwrap();
        store
            .mark_metadata_complete(&hash_reg, "another", 262144, 1_000, 1)
            .unwrap();
        store
            .insert_files(&hash_reg, &[("file.bin".into(), 1_000)])
            .unwrap();

        let results = store.search_torrents("_", 10, "last_seen").unwrap();
        assert_eq!(
            results.len(),
            1,
            "should match only the torrent with literal _"
        );
        assert_eq!(results[0].name.as_deref(), Some("my_file"));
    }
}
