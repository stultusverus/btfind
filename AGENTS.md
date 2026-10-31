# AGENTS.md — btfind

## Build & Verify

```bash
cargo check          # Fast compile check (no binary output)
cargo build          # Full debug build
cargo build --release
cargo test           # Run all tests
cargo clippy         # Lint
cargo fmt --check    # Format check
```

Run tests and clippy before committing. All tests must pass, clippy must have 0 warnings.

Current architecture: [docs/architecture.md](docs/architecture.md). The dated
plans/specifications and root review logs are historical archives, not active
implementation instructions.

## Project Structure

```
src/
├── main.rs          # CLI entry point (clap), subcommand dispatch, cmd_* functions
├── db.rs            # MySQL schema and maintenance subcommands
├── config.rs        # Config structs, TOML loading, XDG paths, defaults
├── types.rs         # NodeId, InfoHash, PeerContact, NodeContact, helpers
├── bencode.rs       # KRPC message types, bencode serialize/deserialize
├── routing.rs       # Kademlia bucket-based routing table (in-memory)
├── dht.rs           # DHT crawler: bootstrap, find_node, get_peers, discovery loop
├── wire.rs          # BitTorrent wire protocol (handshake, messages, extensions, ut_metadata)
├── metadata.rs      # Metadata fetcher: connect to peers, fetch torrent metadata
├── store.rs         # MySQL: async CRUD, ngram search, stats, prune
├── magnet.rs        # Magnet URI validation and formatting
├── web.rs           # Axum HTTP API and dashboard routes
├── web_dashboard.html # Embedded local dashboard markup
├── web_dashboard.css  # Embedded dashboard styles
├── web_dashboard.js   # Embedded dashboard behavior
```

## Architecture

Three async subsystems connected by bounded `tokio::sync::mpsc` channels:

```
DhtCrawler ──(InfoHash, Vec<PeerContact>)──→ run_metadata_fetcher ──→ Store
```

- **DHT Crawler** (`dht.rs`): Single `tokio::net::UdpSocket`. Bootstrap via `find_node`, periodic `get_peers` to random targets. Handles incoming queries (`ping`, `find_node`, `get_peers`, `announce_peer`). Publishes discovered `(info_hash, peers)` tuples to channel.
- **Metadata Fetcher** (`metadata.rs`): Consumes discoveries, dedupes work by info hash, persists retry state and peers, and limits concurrent hash workers. Connects to peers via TCP, performs the BitTorrent handshake, negotiates `ut_metadata` (BEP 9), and downloads torrent info dictionaries.
- **MySQL Store** (`store.rs`): Async SQLx pool, InnoDB foreign keys, and explicit versioned migrations. Persists torrents, files, crawler state, jobs/peers, statistics, and ngram search documents.

## Code Conventions

- **Error handling**: Use `store::Error` (SQLx) or `String`. Don't use `unwrap()` in production code paths; use `expect()` with a message for invariants. Log errors at `warn` or `debug` level, never silently drop with `let _ =`.
- **Transactions**: Catalog mutations acquire the totals row before torrent rows. Keep metadata, files, search documents, jobs, and counter changes atomic.
- **Async**: Database I/O uses SQLx futures.
- **Channel types**: The `info_hash_tx`/`info_hash_rx` channel carries `(InfoHash, Vec<PeerContact>)`. When adding new data, update both sender and receiver sides.
- **Database**: info_hash stored as hex text (not blob). Bind SQL parameters through SQLx. Dynamic ORDER BY uses whitelist validation, not raw user input. Do not run migrations or index backfills in HTTP requests.
- **Config**: Defaults via serde `#[serde(default = "...")]` functions. CLI flags override config values. Port is `Option<u16>` in CLI, resolved in `cmd_run` with config fallback.
- **No comments** unless essential. Code should be self-documenting.

## Testing

- Unit tests in `#[cfg(test)]` modules at the bottom of each source file.
- Database integration tests use isolated databases on a disposable local MySQL server, selected by BTFIND_TEST_DATABASE_URL.
- Integration tests in `src/main.rs` under `mod integration_tests`.
- Don't write tests that hit the network (real DHT/peers).
- Run offline tests with `cargo test` and database tests with `bash scripts/test-mysql.sh`; every test must pass. Database tests are explicitly ignored in the offline suite, not silently skipped when their server is unavailable.

## Production Monitoring & Logging

- Production runs on OCI instance `vm-sub01` in `ap-osaka-1`. Use the OCI CLI with `--region ap-osaka-1`. SSH access is `ssh vm-sub01`; if the connection stalls during banner exchange, use `ssh -o IPQoS=none vm-sub01`.
- `btfind.service` (crawler) and `btfind-web.service` (HTTP server) are systemd **user** services. Both emit structured JSON to persistent systemd-journald, including status metrics every 60 seconds. Event names and metric fields are nested under `fields`.
- The system-level `btfind-observability.timer` invokes a one-shot Python exporter using instance-principal authentication. It uploads logs to OCI Logging group `btfind-production`, custom logs `btfind-crawler` and `btfind-web`, and metrics to OCI Monitoring namespace `btfind` with `instance=vm-sub01`.
- Metrics cover DHT activity, metadata fetching, queue utilisation, backpressure, HTTP traffic and latency. Crawler counters are exported as interval deltas; zero deltas are omitted, so an absent counter stream alone does not establish an exporter failure.
- Production resource OCIDs and exporter settings are in `/etc/btfind/observability.env`; do not commit real OCIDs or credentials. Exporter code is in `/opt/btfind-observability/`, with checkpoints in `/var/lib/btfind-observability/`.
- On the host, inspect application logs with `journalctl --user-unit=btfind.service` and `journalctl --user-unit=btfind-web.service`. Inspect exporter JSON diagnostics with `sudo journalctl -t btfind-observability`; inspect the JSON `level` field rather than relying only on journal priority filtering.
- Check application state with `systemctl --user status btfind.service btfind-web.service`. Check exporter scheduling with `sudo systemctl status btfind-observability.timer btfind-observability.service`. The one-shot service is normally inactive between runs; the timer should be enabled and active. Its schedule is one minute after the preceding run finishes, with up to ten seconds of timer accuracy slack.
- To enable or restore scheduled exports when requested: `sudo systemctl enable --now btfind-observability.timer`. Verify subsequent successful log and metric commits in the exporter journal and fresh events/datapoints in OCI; active applications alone do not establish healthy export.
- See [docs/observability.md](docs/observability.md) for schemas, retention, delivery semantics and deployment details.
