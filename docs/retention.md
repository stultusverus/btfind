# Inactive incomplete torrent expiry

Issue [#21](https://github.com/stultusverus/btfind/issues/21) authorizes the expiry
mechanism. The owner still chooses both retention periods, cadence and each
destructive production activation. This change installs no timer, changes no
production rows, and leaves the DHT lookup interval and database allocation alone.

## Storage evidence

Existing production observation snapshots on `vm-sub01` were read without
refreshing statistics: `day-00/database.json` at 2026-11-06 00:45:06 UTC and
`day-07/database.json` at 2026-11-13 00:42:06 UTC, under
`/home/opc/btfind-observations/2026-11-06/`. These are the same campaign as #15.
The catalog added 2,860,173 hashes, 12,054 complete records and 760,282 files.

Values below are MiB. Data/index columns are InnoDB estimates from
`information_schema.TABLES`, not measured physical allocation or exact row sizes.

| Table | Nov 13 data | Nov 13 indexes | Data change | Index change | Nov 13 free |
| --- | ---: | ---: | ---: | ---: | ---: |
| torrents | 2219.84 | 3557.78 | +734.88 | +662.95 | 5 |
| hash_jobs | 683.09 | 77.02 | -7.00 | -190.00 | 1535 |
| hash_job_peers | 195.77 | 139.69 | +51.92 | +37.78 | 5 |
| files | 188.73 | 248.95 | +87.09 | +114.00 | 6 |
| search_documents | 112.61 | 1.52 | +56.05 | 0 | 7 |
| metadata_peer_attempts | 139.25 | 77.38 | +81.09 | +59.30 | 6 |
| persistence_receipts | 231.02 | 62.20 | +141.75 | +7.02 | 7 |
| dht_nodes | 71.62 | 39.30 | +71.25 | +39.09 | 7 |
| crawl_stats | 10.52 | 0 | +1.00 | 0 | 4 |

Other catalog tables were essentially unchanged. Estimated aggregate data/index
growth was 1.9025 GiB; torrents contributed 1397.83 MiB (about 72%). Decreased
job estimates and 1535 MiB free in that table illustrate why row counts and
allocation must be assessed separately. Free space in one file-per-table
tablespace cannot be treated as free capacity for another table.

A separate read of `information_schema.INNODB_TABLESPACES` on November 14 at
04:59 UTC measured allocated bytes, including full-text auxiliary tables:

| Object | Allocated bytes |
| --- | ---: |
| torrents | 6,283,096,064 |
| hash_jobs | 2,441,097,216 |
| files | 545,263,616 |
| hash_job_peers | 381,685,760 |
| persistence_receipts | 327,159,808 |
| full-text auxiliaries | 279,379,968 |
| metadata_peer_attempts | 264,245,248 |
| dht_nodes | 146,804,736 |
| search_documents base table | 138,416,128 |
| crawl_stats | 19,922,944 |

`INNODB_INDEXES` maps full-text index IDs `0x536` and `0x53D` to
`search_documents.ft_content` and `ft_name`. Their auxiliary tables are additional
to the base table estimate. There is no historical physical-allocation snapshot,
so the seven-day physical growth is unknown. These schema files exclude system,
undo, redo, temporary files and managed-service overhead; their sum is not the
50 GiB allocation's remaining capacity.

`btfind db storage` emits timestamped JSON table estimates; `--physical` also
reports file and allocated bytes for schema-named tablespaces, including FTS
auxiliaries. The latter requires the existing maintenance account's PROCESS
privilege and fails explicitly if unavailable. It does not report shared general
tablespaces under unrelated names. Compare snapshots at known times without
running ANALYZE or OPTIMIZE merely to collect a baseline. See MySQL's
[tablespace column definitions](https://dev.mysql.com/doc/refman/8.4/en/information-schema-innodb-tablespaces-table.html).

## Eligibility and owner choices

Two required CLI settings are expressed in whole positive days:

- `--older-than`: time since the torrent's last sighting.
- `--inactive-for`: time since job updates, new peers, successful attempts,
  torrent/peer attempts, retained peer sightings and sampling observations.

All relevant timestamps must be strictly before their cutoffs. Equality retains
the row. Cutoffs are calculated once per invocation and printed as Unix seconds.
The owner could evaluate 30/7 days against 90/30 days in preview; these are
comparison scenarios, not selected production thresholds. Preview counts across
the entire scan, admission/backlog diagnostics from #15, and subsequent storage
growth should inform that choice. Repeated sightings retain a hash indefinitely.

Expiry requires `metadata_complete=0`, no name or piece length, zero size/file
count and no file or search-document rows. All completed metadata and associated
name/path search, largest-torrent results and file lists are retained. Recent
sightings stay in browse results; expired empty hashes disappear from browse and
detail results, reducing the current catalog total.

A job must be absent or in `queued`, `retry_at`, `waiting_peers`, `invalid` or
`unsupported`, with old activity and no retry scheduled at/after the run start.
Queued/retry jobs with any retained peer are always protected, including overdue
and cooling-down retries with old peers. Complete and unknown job statuses are
protected. Stale waiting/terminal jobs can expire with their stale peers and
attempts. No retry eligibility, scheduler ownership or admission rule changes in
#19/#20 are assumed. This intentionally leaves the growing peer-bearing backlog
for those issues; expiry alone cannot guarantee a hard cap on all database data.
New job statuses or durable ownership fields must be reviewed against this rule.

## Bounded maintenance and restart

Use the existing database connection environment and matching binary. Preview
can run while services are running; concurrent activity makes it advisory.

```bash
btfind db storage --physical > storage-before.json
btfind prune --older-than "$SIGHTING_DAYS" --inactive-for "$ACTIVITY_DAYS" \
  --batch-size 128 --max-batches 10
```

JSON stdout reports `scanned`, `candidates`, `deleted`, `exhausted`, both cutoffs
and `next_cursor`. Candidate counts cover only the examined old incomplete rows,
not the whole database. Protected rows consume scan budget. If `exhausted=false`,
pass `--after 'TIMESTAMP:INFO_HASH'` from that report to scan the next range.
Cursor ordering uses the existing `(metadata_complete,last_seen,info_hash)` index;
it progresses even across a fully protected batch. Start a new full pass without
`--after` after reaching exhaustion. Later passes can find rows made eligible
by new cutoffs or changed job state. A preview cursor advances past candidates;
to apply them, restart at the preview's input cursor, not its output cursor.

After owner approval of periods and the maintenance window, gracefully stop
`btfind.service` and `btfind-web.service`, wait for shutdown, and ensure no other
crawler, metadata worker or catalog maintenance process uses this database.
The scheduler owns in-memory work that SQL cannot reliably identify. The CLI
requires `--apply --services-stopped`; the flag is an operator assertion, not an
automatic remote-service check. Keep services stopped across the entire pass.

```bash
btfind prune --older-than "$SIGHTING_DAYS" --inactive-for "$ACTIVITY_DAYS" \
  --batch-size 128 --max-batches 10 --apply --services-stopped
```

Each apply batch locks catalog totals before at most `batch_size` torrent rows,
rechecks eligibility, deletes hashes with existing foreign-key cascades, and
decrements only `total_torrents` in the same transaction. Complete/file/size totals
remain unchanged. Cascades remove dependent jobs, peers, attempts and samples.
The scan-row bound is not a bound on dependent rows or transaction time: historical
attempts per hash may be large. Use small batches and review latency on the fixed
production instance before increasing the limit.

Committed batches emit structured `incomplete_expiry_batch` progress with counts,
cutoffs and cursor on stderr. Errors return a nonzero exit status. The failing
transaction rolls back; earlier commits remain. Resume from the last successful
apply cursor, or restart from the beginning to recheck retained rows. Replays do
not double-decrement totals. A crash after commit but before progress output is
also safe to replay. There is no persistent cursor table or automatic scheduling.

Rediscovery recreates an expired hash with new first/last-seen timestamps. A
peerless sighting creates only the torrent; a peer-bearing sighting creates a
fresh queued job and retained peers. Old failures, cooldowns and invalid/unsupported
terminal states are lost on expiry, so a rediscovered hash may be attempted again.
Historical runtime counters/receipts are retained and do not represent the current
catalog size. After the pass, compare catalog/storage reports and restart services.

## Recovery and space reuse

Deletion is irreversible without existing recovery media. Review the existing
backup/managed recovery coverage and its recovery point before authorizing a
production pass; this feature adds no backup or recovery automation. Restoring an
older database resurrects expired hashes and retry state. Follow the existing
restore procedure and reassess retention before another pass.

InnoDB deletion makes space reusable after purge; it does not immediately shrink
tablespace files. Physical compaction is a separate rebuild with its own temporary
space and operational requirements, not part of this command. Nothing runs
OPTIMIZE, rebuilds indexes, or expands the 50 GiB allocation. See MySQL's
[file-space management](https://dev.mysql.com/doc/refman/8.4/en/innodb-file-space.html)
and [OPTIMIZE behavior](https://dev.mysql.com/doc/refman/8.4/en/optimize-table.html).
