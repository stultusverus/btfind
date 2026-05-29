# btfind Dirty-Tree Review Findings

## Review Target

- Base commit: `a456120` - `fix: address post-commit review -- validated wire/dht parsers, HMAC tokens, IP filtering, shutdown, transaction robustness`
- Current target: uncommitted worktree changes on top of `a456120`.

## Resolution Status

- All findings listed below were addressed by the dirty-tree fix implementation.
- Final verification after fixes: `cargo check`, `cargo test`, `cargo clippy`, `cargo fmt --check`, and `git diff --check` all passed; `cargo test` reported 77 passed and 0 failed.

## Verification Findings

- `cargo check`: failed. `src/bencode.rs:242` and `src/bencode.rs:252` are missing lifetime specifiers; `src/bencode.rs:189` dereferences an `i64`; `src/bencode.rs:194` passes `Vec<u8>` where `&[u8]` is required.
- `cargo test`: failed with the same compile errors, so no tests ran.
- `cargo clippy`: failed with the same compile errors, so no lint findings were produced.
- `cargo fmt --check`: failed. Formatting diffs were reported in `src/dht.rs` and `src/metadata.rs`.
- `git diff --check`: passed with no whitespace errors.

## Bugs

- **Critical** `src/bencode.rs:239-253`: `get_dict` and `get_list` now return borrowed containers but do not declare the returned lifetime. Impact: the crate does not compile. Suggested fix: add an explicit lifetime tied only to `dict`, for example `fn get_dict<'a>(dict: &'a HashMap<...>, key: &str) -> Option<&'a HashMap<...>>`.
- **Critical** `src/bencode.rs:187-195`: the error-list parser was not updated correctly after borrowing list values; `*i` no longer compiles and `String::from_utf8_lossy(b)` needs a borrowed byte slice. Impact: the crate does not compile. Suggested fix: use the compiler-suggested borrowed forms, removing the invalid dereference and passing `&b`.
- **High** `src/dht.rs:760-783`: response `nodes` are added to the routing table before `handle_pending_lookup_response` checks whether `src` is an expected responder. Impact: a spoofed response with a guessed transaction ID can still seed the routing table even when peer publication is rejected. Suggested fix: validate the responder before processing response nodes for tracked transactions.
- **High** `src/dht.rs:448-492` and `src/dht.rs:736-747`: `announce_peer` accepts and publishes peers from any source address once the token/port validate; it does not require the source endpoint itself to be public. Impact: private, loopback, or link-local peers can enter the metadata fetch queue through announces. Suggested fix: reject non-public `src` with a KRPC error before token and port processing.
- **Medium** `src/metadata.rs:523-534`: `MetadataFetched` is emitted inside `mark_metadata_complete_and_emit` before file rows are inserted, and the caller continues to insert files even if marking metadata complete fails. Impact: stats can count metadata as fetched before full persistence succeeds. Suggested fix: emit the event only after `mark_metadata_complete` and `insert_files` both succeed.
- **Medium** `src/main.rs:253-290` and `src/metadata.rs:463`: aborting the fetcher task does not track or cancel metadata worker tasks spawned inside `run_metadata_fetcher`. Impact: shutdown tests do not cover active fetch workers, and detached workers can keep running until runtime teardown. Suggested fix: use `JoinSet` or a cancellation token for fetch workers.

## Missing Features

- **Low** `src/main.rs:216-231`: crawl history now records stats events, but the `stats` subcommand still only prints aggregate torrent/file totals and does not expose the new crawl history fields. Impact: `queries_sent`, `info_hashes_found`, and `metadata_fetched` are written but not visible from the CLI. Suggested fix: extend `cmd_stats` to print recent crawl history or current crawl counters.

## Error Handling

- **Medium** `src/main.rs:176`: Ctrl+C errors are silently discarded with `let _ = tokio::signal::ctrl_c().await`. Impact: signal-listener setup failures are hidden. Suggested fix: match the result and log a warning before shutdown.
- **Medium** `src/store.rs:195-209`: `mark_metadata_complete` ignores the row count from `UPDATE`. Impact: callers can treat a nonexistent torrent as successfully marked complete. Suggested fix: return `QueryReturnedNoRows` or a custom error when `execute` returns `0`.
- **Low** `src/store.rs:644-676`: the old-schema migration test writes to `std::env::temp_dir()` and removes only the main DB file on the success path. Impact: panics or WAL mode can leave temporary `-wal`/`-shm` files behind. Suggested fix: add a small cleanup guard or remove all SQLite sidecar files.

## Dead Code

- No new dead-code finding beyond existing `#[allow(dead_code)]` markers. The previous `PendingLookup.sent` dead state has been removed.

## Protocol Issues

- **Medium** `src/dht.rs:20-55`: `is_public_ipv4` still allows some special-use ranges, including `192.0.0.0/24` except the documentation subnet checks already listed. Impact: non-globally-routable endpoints can still enter peer/node queues. Suggested fix: centralize an explicit reject list for all RFC 6890 non-global IPv4 ranges used by DHT endpoints.
- **Low** `src/dht.rs:340-345`: the token time bucket casts Unix seconds to `u32` before dividing. Impact: tokens behave incorrectly after the 2106 wrap boundary. Suggested fix: keep the bucket as `u64` through hashing and validation.

## Performance

- **Medium** `src/main.rs:221-229`: `run_stats_persistence` inserts a `crawl_stats` row for every `MetadataFetched` event. Impact: successful metadata bursts can create high-frequency SQLite writes and noisy history rows. Suggested fix: increment the in-memory counter on `MetadataFetched`, but persist rows only on DHT snapshot/timer events.
- **Low** `src/metadata.rs:370-385`: metadata bencode helper lookups still allocate `key.as_bytes().to_vec()`. Impact: avoidable allocations remain in metadata parsing. Suggested fix: use borrowed slice lookup as was attempted in `src/bencode.rs`.
- **Low** `src/wire.rs:316-349` and `src/wire.rs:395-410`: wire parser lookups still allocate key vectors for every dictionary lookup. Impact: avoidable allocations remain in BEP 9 parsing. Suggested fix: replace `dict.get(&b\"...\".to_vec())` with borrowed slice lookups.

## Test Gaps

- **High** `src/dht.rs:1070-1105`: the unexpected-responder test only verifies peer publication is blocked; it does not prove malicious `nodes` in the same unexpected response are ignored. Impact: the routing-table pollution bug is untested. Suggested fix: add a test that feeds a response containing both `nodes` and `values` from an unexpected address and asserts routing count remains unchanged.
- **Medium** `src/dht.rs:1162-1198`: announce tests cover invalid announces, but no test covers a valid announce success path or rejection of a valid-token announce from a non-public source. Impact: valid announce behavior and private-source rejection can regress. Suggested fix: add both tests around `handle_announce_peer_query`.
- **Medium** `src/main.rs:523-537`: `RuntimeStats` is unit-tested, but `run_stats_persistence` is not tested against `Store`. Impact: database persistence of `queries_sent`, `info_hashes_found`, and `metadata_fetched` can regress. Suggested fix: add an async test using `Store::open_in_memory()` and `get_crawl_history`.
- **Medium** `src/main.rs:483-520`: shutdown tests use already-completed crawler/fetcher tasks and do not cover active metadata workers spawned below `run_metadata_fetcher`. Impact: detached worker shutdown behavior remains untested. Suggested fix: add a worker-running shutdown test after introducing worker tracking.
