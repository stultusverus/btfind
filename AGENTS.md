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

Run tests and clippy before committing. All 26 tests must pass, clippy must have 0 warnings.

## Project Structure

```
src/
├── main.rs          # CLI entry point (clap), subcommand dispatch, cmd_* functions
├── config.rs        # Config structs, TOML loading, XDG paths, defaults
├── types.rs         # NodeId, InfoHash, PeerContact, NodeContact, helpers
├── bencode.rs       # KRPC message types, bencode serialize/deserialize
├── routing.rs       # Kademlia bucket-based routing table (in-memory)
├── dht.rs           # DHT crawler: bootstrap, find_node, get_peers, discovery loop
├── wire.rs          # BitTorrent wire protocol (handshake, messages, extensions, ut_metadata)
├── metadata.rs      # Metadata fetcher: connect to peers, fetch torrent metadata
├── store.rs         # SQLite: schema, CRUD, search, stats, prune
```

## Architecture

Three async subsystems connected by `tokio::sync::mpsc` channels:

```
DhtCrawler ──(InfoHash, Vec<PeerContact>)──→ run_metadata_fetcher ──→ Store
```

- **DHT Crawler** (`dht.rs`): Single `tokio::net::UdpSocket`. Bootstrap via `find_node`, periodic `get_peers` to random targets. Handles incoming queries (`ping`, `find_node`, `get_peers`, `announce_peer`). Publishes discovered `(info_hash, peers)` tuples to channel.
- **Metadata Fetcher** (`metadata.rs`): Consumes channel. Connects to peers via TCP, performs BitTorrent handshake, negotiates `ut_metadata` extension (BEP 9), downloads and assembles torrent info dict. Rate-limited via `Semaphore`.
- **SQLite Store** (`store.rs`): WAL mode, foreign keys enabled. Tables: `torrents`, `files`, `crawl_stats`. All access through `Store` struct with `Mutex<Connection>`.

## Code Conventions

- **Error handling**: Use `Result<T, E>` where E is `rusqlite::Error` or `String`. Don't use `unwrap()` in production code paths; use `expect()` with a message for invariants. Log errors at `warn` or `debug` level, never silently drop with `let _ =`.
- **Mutex**: `Store.conn` is `Mutex<Connection>`. Lock scope is per-method call (lock, query, drop). No nested locks.
- **Async**: All I/O is async (`tokio`). CPU-bound work (SQLite) is synchronous behind the mutex — callers are in `tokio::spawn` tasks.
- **Channel types**: The `info_hash_tx`/`info_hash_rx` channel carries `(InfoHash, Vec<PeerContact>)`. When adding new data, update both sender and receiver sides.
- **Database**: info_hash stored as hex text (not blob). SQL parameters via `rusqlite::params!`. Dynamic ORDER BY uses whitelist validation, not raw user input.
- **Config**: Defaults via serde `#[serde(default = "...")]` functions. CLI flags override config values. Port is `Option<u16>` in CLI, resolved in `cmd_run` with config fallback.
- **No comments** unless essential. Code should be self-documenting.

## Testing

- Unit tests in `#[cfg(test)]` modules at the bottom of each source file.
- Store tests use `Store::open_in_memory()`.
- Integration tests in `src/main.rs` under `mod integration_tests`.
- Don't write tests that hit the network (real DHT/peers).
- Run with `cargo test` (should be 26 passing).
