# btfind Dirty-Tree Fix Plan

## Phase 1: Restore Build and Formatting

### Task 1.1
- [x] Edit `src/bencode.rs:239-253` so `get_dict` and `get_list` declare an explicit lifetime tied to `dict`, not `key`.
- Verification: run `cargo check` and confirm the `missing lifetime specifier` errors at `src/bencode.rs:242` and `src/bencode.rs:252` are gone.

### Task 1.2
- [x] Edit `src/bencode.rs:187-195` so the borrowed error-list match returns the integer without invalid dereference and passes a byte slice to `String::from_utf8_lossy`.
- Verification: run `cargo check` and confirm the `E0614` and `E0308` errors at `src/bencode.rs:189` and `src/bencode.rs:194` are gone.

### Task 1.3
- [x] Edit formatting in `src/dht.rs:375-379`, `src/dht.rs:663-754`, `src/dht.rs:1152-1160`, `src/metadata.rs:523-529`, `src/metadata.rs:789-796`, and `src/metadata.rs:966-971` to match `cargo fmt`.
- Verification: run `cargo fmt --check` and confirm it exits 0.

### Task 1.4
- [x] Run `cargo check`, `cargo test`, `cargo clippy`, and `cargo fmt --check` after Tasks 1.1-1.3.
- Verification: all four commands must exit 0 before any lower-priority phase is started.

## Phase 2: DHT Trust Boundary Fixes

### Task 2.1
- [x] Edit `src/dht.rs:760-783` so pending-response source validation happens before compact `nodes` are decoded or added for a tracked transaction.
- Verification: add a unit test near `src/dht.rs:1070-1105` where an unexpected responder sends both `nodes` and peers, and assert the pending lookup remains plus `routing.node_count() == 0`.

### Task 2.2
- [x] Edit `src/dht.rs:448-492` so `handle_announce_peer_query` rejects non-public `src` endpoints before token validation, port selection, or `info_hash_tx.send`.
- Verification: add a unit test near `src/dht.rs:1162-1198` with a valid token from `127.0.0.1:6881` and assert it returns `KrpcMessage::Error` and sends no peers.

### Task 2.3
- [x] Edit `src/dht.rs:20-55` to reject the remaining non-global IPv4 ranges needed for DHT endpoints, including `192.0.0.0/24`, while keeping known public fixtures accepted.
- Verification: extend `test_decode_compact_filters_non_public_ipv4_ranges` near `src/dht.rs:1021-1046` with `192.0.0.1` and a public accepted case.

### Task 2.4
- [x] Edit `src/dht.rs:340-345`, `src/dht.rs:375-383`, and `src/dht.rs:401-409` so token buckets are `u64` and are hashed as eight-byte big-endian values.
- Verification: update token tests near `src/dht.rs:1108-1129` to call the `u64` bucket helper directly.

## Phase 3: Metadata Persistence and Stats Semantics

### Task 3.1
- [x] Edit `src/store.rs:195-209` so `mark_metadata_complete` checks the affected row count and returns an error when no torrent row exists.
- Verification: add a store test near `src/store.rs:621-640` that marking an unknown info hash returns an error.

### Task 3.2
- [x] Edit `src/metadata.rs:416-432` and `src/metadata.rs:523-534` so `MetadataFetched` is sent only after metadata completion and file insertion both succeed.
- Verification: add a metadata helper test near `src/metadata.rs:965-987` that a forced `insert_files` error does not emit `CrawlStatsEvent::MetadataFetched`.

### Task 3.3
- [x] Edit `src/main.rs:216-231` so `run_stats_persistence` persists rows only for `DhtSnapshot` events; `MetadataFetched` should update the in-memory counter without immediately writing a row.
- Verification: add an async test near `src/main.rs:523-537` using `Store::open_in_memory()` and `get_crawl_history` to verify one snapshot row includes accumulated metadata count.

### Task 3.4
- [x] Edit `src/main.rs:346-360` so `cmd_stats` also reports the newest crawl-history row's `nodes_known`, `queries_sent`, `info_hashes_found`, and `metadata_fetched` when present.
- Verification: add a stats formatting or helper test near `src/main.rs:398-538` that verifies nonzero crawl-history fields are exposed.

## Phase 4: Shutdown and Error Handling

### Task 4.1
- [x] Edit `src/metadata.rs:451-560` to track spawned metadata workers with `tokio::task::JoinSet` or a cancellation token instead of detached `tokio::spawn` calls.
- Verification: add an async test near `src/metadata.rs:954-987` proving a running worker can be cancelled or joined during fetcher shutdown.

### Task 4.2
- [x] Edit `src/main.rs:253-290` to integrate metadata worker shutdown reporting after Task 4.1, so `shutdown_tasks` does not claim fetcher shutdown while child workers are still active.
- Verification: extend `src/main.rs:483-520` shutdown tests with a fetcher task that owns an active child worker.

### Task 4.3
- [x] Edit `src/main.rs:176` to replace `let _ = tokio::signal::ctrl_c().await` with a match that logs signal-listener errors.
- Verification: run `rg -n "let _ =" src/main.rs` and confirm no silent result discard remains in production shutdown code.

### Task 4.4
- [x] Edit `src/store.rs:644-676` so the old-schema migration test removes the main SQLite file and any `-wal`/`-shm` sidecar files even when assertions fail.
- Verification: run `cargo test store::tests::test_open_migrates_old_schema_with_last_attempt` repeatedly and check no `btfind-old-schema-*.sqlite*` files are left in `std::env::temp_dir()`.

## Phase 5: Parser Allocation Cleanup

### Task 5.1
- [x] Edit `src/metadata.rs:370-385` to replace `dict.get(&key.as_bytes().to_vec())` with borrowed slice lookups.
- Verification: run `cargo test metadata::tests::test_fetch_from_stream_success`.

### Task 5.2
- [x] Edit `src/wire.rs:316-349` and `src/wire.rs:395-410` to replace bencoded dictionary key allocations with borrowed slice lookups.
- Verification: run `cargo test wire::tests`.

### Task 5.3
- [x] Edit `src/bencode.rs:228-253` after the build fix to keep borrowed lookup helpers while avoiding cloned dictionary/list containers.
- Verification: run `cargo test bencode::tests` and `cargo clippy`.
