use super::*;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExpiryCursor {
    last_seen: i64,
    info_hash: String,
}

impl FromStr for ExpiryCursor {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let (timestamp, hash) = value
            .split_once(':')
            .ok_or("expected TIMESTAMP:INFO_HASH")?;
        let last_seen = timestamp.parse().map_err(|_| "invalid cursor timestamp")?;
        decode_hash(hash).map_err(|_| "invalid cursor info hash")?;
        Ok(Self {
            last_seen,
            info_hash: hash.to_ascii_lowercase(),
        })
    }
}

impl std::fmt::Display for ExpiryCursor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}", self.last_seen, self.info_hash)
    }
}

#[derive(Clone, Debug)]
pub struct ExpiryPolicy {
    pub last_seen_before: i64,
    pub inactive_before: i64,
    pub started_at: i64,
    pub batch_size: u32,
    pub max_batches: u32,
}

impl ExpiryPolicy {
    pub fn new(
        older_than_days: u32,
        inactive_days: u32,
        batch_size: u32,
        max_batches: u32,
    ) -> Result<Self, Error> {
        if older_than_days == 0 || inactive_days == 0 {
            return Err(invalid("retention periods must be positive"));
        }
        let started_at = now();
        let policy = Self {
            last_seen_before: started_at - i64::from(older_than_days) * 86400,
            inactive_before: started_at - i64::from(inactive_days) * 86400,
            started_at,
            batch_size,
            max_batches,
        };
        policy.validate()?;
        Ok(policy)
    }

    fn validate(&self) -> Result<(), Error> {
        if !(1..=500).contains(&self.batch_size) || !(1..=10000).contains(&self.max_batches) {
            return Err(invalid(
                "batch_size must be 1..=500 and max_batches 1..=10000",
            ));
        }
        if self.last_seen_before >= self.started_at || self.inactive_before >= self.started_at {
            return Err(invalid("expiry cutoffs must precede the run start"));
        }
        Ok(())
    }
}

#[derive(Debug, serde::Serialize)]
pub struct ExpiryReport {
    pub apply: bool,
    pub last_seen_before: i64,
    pub inactive_before: i64,
    pub scanned: u64,
    pub candidates: u64,
    pub deleted: u64,
    pub exhausted: bool,
    pub next_cursor: Option<String>,
}

impl Store {
    pub async fn expire_incomplete(
        &self,
        policy: &ExpiryPolicy,
        cursor: Option<ExpiryCursor>,
        apply: bool,
    ) -> Result<ExpiryReport, Error> {
        policy.validate()?;
        let mut report = ExpiryReport {
            apply,
            last_seen_before: policy.last_seen_before,
            inactive_before: policy.inactive_before,
            scanned: 0,
            candidates: 0,
            deleted: 0,
            exhausted: false,
            next_cursor: cursor.as_ref().map(ToString::to_string),
        };
        let mut cursor = cursor;
        for _ in 0..policy.max_batches {
            let mut tx = self.pool.begin().await?;
            if apply {
                lock_totals(&mut tx).await?;
            }
            let mut scan = QueryBuilder::<MySql>::new(
                "SELECT info_hash,last_seen FROM torrents FORCE INDEX (idx_complete_last) WHERE metadata_complete=0 AND last_seen<",
            );
            scan.push_bind(policy.last_seen_before);
            if let Some(after) = &cursor {
                scan.push(" AND (last_seen>")
                    .push_bind(after.last_seen)
                    .push(" OR (last_seen=")
                    .push_bind(after.last_seen)
                    .push(" AND info_hash>")
                    .push_bind(&after.info_hash)
                    .push("))");
            }
            scan.push(" ORDER BY last_seen,info_hash LIMIT ")
                .push_bind(policy.batch_size);
            if apply {
                scan.push(" FOR UPDATE");
            }
            let rows = scan.build().fetch_all(&mut *tx).await?;
            let mut candidates = 0;
            let mut deleted = 0;
            let next_cursor = if let Some(last) = rows.last() {
                Some(ExpiryCursor {
                    last_seen: last.try_get("last_seen")?,
                    info_hash: last.try_get("info_hash")?,
                })
            } else {
                cursor.clone()
            };
            if !rows.is_empty() {
                let mut eligible = QueryBuilder::<MySql>::new(
                    "SELECT t.info_hash FROM torrents t WHERE t.info_hash IN (",
                );
                let hashes: Vec<String> = rows
                    .iter()
                    .map(|row| row.try_get("info_hash"))
                    .collect::<Result<_, _>>()?;
                let mut values = eligible.separated(",");
                for hash in &hashes {
                    values.push_bind(hash);
                }
                eligible.push(") AND t.metadata_complete=0 AND t.last_seen<").push_bind(policy.last_seen_before)
                    .push(" AND t.last_attempt<").push_bind(policy.inactive_before)
                    .push(" AND t.name IS NULL AND t.piece_length IS NULL AND t.total_size=0 AND t.file_count=0
                        AND NOT EXISTS (SELECT 1 FROM files f WHERE f.torrent_id=t.info_hash)
                        AND NOT EXISTS (SELECT 1 FROM search_documents s WHERE s.info_hash=t.info_hash)
                        AND NOT EXISTS (SELECT 1 FROM hash_jobs j WHERE j.info_hash=t.info_hash AND (
                            j.status NOT IN ('queued','retry_at','waiting_peers','invalid','unsupported')
                            OR j.updated_at>=").push_bind(policy.inactive_before)
                    .push(" OR j.last_new_peer_at>=").push_bind(policy.inactive_before)
                    .push(" OR j.last_success_at>=").push_bind(policy.inactive_before)
                    .push(" OR j.next_attempt_at>=").push_bind(policy.started_at)
                    .push(" OR (j.status IN ('queued','retry_at') AND EXISTS
                        (SELECT 1 FROM hash_job_peers p WHERE p.info_hash=t.info_hash))))
                        AND NOT EXISTS (SELECT 1 FROM hash_job_peers p WHERE p.info_hash=t.info_hash AND p.last_seen>=")
                    .push_bind(policy.inactive_before).push(")
                        AND NOT EXISTS (SELECT 1 FROM metadata_peer_attempts a WHERE a.info_hash=t.info_hash AND a.last_attempt>=")
                    .push_bind(policy.inactive_before).push(")
                        AND NOT EXISTS (SELECT 1 FROM sample_observations o WHERE o.info_hash=t.info_hash AND o.last_seen>=")
                    .push_bind(policy.inactive_before).push(")");
                if apply {
                    eligible.push(" FOR UPDATE");
                }
                let hashes: Vec<String> = eligible.build_query_scalar().fetch_all(&mut *tx).await?;
                candidates = hashes.len() as u64;
                if apply && !hashes.is_empty() {
                    let mut delete =
                        QueryBuilder::<MySql>::new("DELETE FROM torrents WHERE info_hash IN (");
                    let mut values = delete.separated(",");
                    for hash in &hashes {
                        values.push_bind(hash);
                    }
                    delete.push(")");
                    deleted = delete.build().execute(&mut *tx).await?.rows_affected();
                    sqlx::query(
                        "UPDATE catalog_totals SET total_torrents=total_torrents-? WHERE id=1",
                    )
                    .bind(deleted)
                    .execute(&mut *tx)
                    .await?;
                }
            }
            if apply {
                tx.commit().await?;
            } else {
                tx.rollback().await?;
            }
            report.scanned += rows.len() as u64;
            report.candidates += candidates;
            report.deleted += deleted;
            report.exhausted = rows.len() < policy.batch_size as usize;
            cursor = next_cursor;
            report.next_cursor = cursor.as_ref().map(ToString::to_string);
            tracing::info!(
                event = "incomplete_expiry_batch",
                apply,
                scanned = report.scanned,
                candidates = report.candidates,
                deleted = report.deleted,
                last_seen_before = report.last_seen_before,
                inactive_before = report.inactive_before,
                next_cursor = report.next_cursor.as_deref(),
                exhausted = report.exhausted,
                "incomplete torrent expiry progress"
            );
            if report.exhausted {
                break;
            }
            tokio::task::yield_now().await;
        }
        Ok(report)
    }
}
