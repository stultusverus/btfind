# btfind — BitTorrent DHT Sniffer Design

A BitTorrent sniffer that discovers torrents from the DHT network, fetches their metadata, stores them in a local SQLite database, and provides CLI search/query capabilities.

## Architecture Overview

Three core subsystems driven by a Tokio async runtime:

```
+----------------------------------------+
|              CLI (clap)                 |
|   run | search | stats | prune          |
+------------------+---------------------+
                   |
+------------------+---------------------+
|            App Controller               |
|     orchestrates crawl + fetch          |
+----------+----------+----------+--------+
           |          |          |
           v          v          v
     +----------+ +--------+ +---------+
     |   DHT    | |Metadata| | SQLite  |
     | Crawler  | |Fetcher | |  Store  |
     +----------+ +--------+ +---------+
```

- **CLI layer** — `clap` with subcommands: `run`, `search`, `stats`, `prune`
- **DHT Crawler** — Bootstrap into DHT, walk via `find_node`, discover info_hashes via `get_peers` responses. Publish to internal channel.
- **Metadata Fetcher** — Consume info_hashes, connect to peers via wire protocol, request metadata (BEP 9). On success, write full torrent info to store.
- **SQLite Store** — Tables for torrents, files within torrents, and crawl stats. Deduplication by info_hash.

### Dependencies (Cargo)

- `tokio` — async runtime, UDP/TCP networking, channels, timers
- `clap` — CLI argument parsing with derive macros
- `serde` / `serde_json` — JSON output for search results
- `serde_bencode` — KRPC message and torrent metadata parsing
- `rusqlite` — SQLite bindings
- `sha1` — SHA-1 hashing for node IDs and info_hashes
- `rand` — random node IDs and info_hash generation
- `tracing` / `tracing-subscriber` — structured logging
- `toml` — config file parsing
- `chrono` — timestamp formatting
- `dirs` — XDG path resolution for config/data dirs

## DHT Crawler

Uses `tokio::net::UdpSocket` for KRPC (Kademlia Remote Procedure Call) over UDP. Messages are bencoded dictionaries.

### Bootstrap

- Hardcoded well-known bootstrap nodes:
  - `router.bittorrent.com:6881`
  - `dht.transmissionbt.com:6881`
  - `router.utorrent.com:6881`
- Send `find_node` queries with our own node ID as target to discover other nodes
- Recursively `find_node` newly discovered nodes to populate routing table

### Routing Table

- Bucket-based Kademlia table, 160-bit node IDs (SHA-1), stored in memory
- Buckets refreshed every 15 minutes
- Track last-seen timestamps to evict stale nodes

### Discovery Loop

- Every N seconds, pick a target info_hash, issue `get_peers` to K closest nodes
- Nodes respond with either peers (confirming active info_hash) or closer nodes
- Both `get_peers` responses and unsolicited `announce_peer` messages yield info_hashes
- Publish discovered info_hashes to `tokio::sync::mpsc` channel

### Concurrency

- Single UDP socket, multiplexed request/response with transaction IDs
- Fan-out to many in-flight queries via `tokio::select!` over socket reads and query timers

## Metadata Fetcher

Consumes info_hashes from mpsc channel. For each, connects to peers and requests metadata via BEP 9/BEP 10.

### Per-info_hash flow

1. Receive info_hash from channel
2. Check SQLite — skip if already fetched or attempted within last 24h
3. Get peer list for this info_hash from DHT crawler
4. Connect to up to 8 peers concurrently via TCP (wire protocol)
5. Handshake, negotiate extensions, request `ut_metadata` pieces
6. Assemble and decode bencoded info dict
7. Extract: name, file list with sizes, piece length, total size
8. Write to SQLite

### Timeouts and retry

- 8 peers per info_hash max
- 30s connect timeout, 60s metadata fetch timeout
- Prefer peers advertising `ut_metadata` in extension handshake
- Fall back to next peer on failure

### Rate limiting

- Max N concurrent metadata fetches (configurable, default: 10)
- Configurable via CLI flag and config file

## SQLite Schema

```sql
CREATE TABLE IF NOT EXISTS torrents (
    info_hash         TEXT PRIMARY KEY,
    name              TEXT,
    piece_length      INTEGER,
    total_size        INTEGER,
    file_count        INTEGER,
    source            TEXT,
    first_seen        INTEGER NOT NULL,
    last_seen         INTEGER NOT NULL,
    metadata_complete INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS files (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    torrent_id  TEXT NOT NULL REFERENCES torrents(info_hash),
    path        TEXT NOT NULL,
    size        INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS crawl_stats (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp         INTEGER NOT NULL,
    nodes_known       INTEGER,
    queries_sent      INTEGER,
    info_hashes_found INTEGER,
    metadata_fetched  INTEGER
);

CREATE INDEX IF NOT EXISTS idx_torrents_first_seen ON torrents(first_seen);
CREATE INDEX IF NOT EXISTS idx_torrents_last_seen ON torrents(last_seen);
CREATE INDEX IF NOT EXISTS idx_torrents_name ON torrents(name);
```

## CLI

```
btfind run           Start crawler (one-shot default, --continuous for daemon)
      --duration <secs>    Run duration (one-shot)
      --continuous         Run indefinitely
      --max-concurrent <n> Max simultaneous metadata fetches [default: 10]

btfind search        Search collected torrents
      --query <text>       Text search on name
      --limit <n>          Max results [default: 50]
      --sort <field>       first_seen|last_seen|total_size|name
      --json               Output as JSON

btfind stats         Show crawler statistics

btfind prune         Remove torrents not seen in N days
      --older-than <days>  [default: 90]
```

## Configuration

Optional config at `~/.config/btfind/config.toml`. CLI flags override config values.

```toml
[network]
port = 6881
bootstrap_nodes = ["router.bittorrent.com:6881", "dht.transmissionbt.com:6881"]

[metadata]
max_concurrent = 10
peer_timeout_secs = 30
retry_after_hours = 24

[database]
path = "~/.local/share/btfind/torrents.db"

[crawl]
get_peers_interval_secs = 5
bucket_refresh_mins = 15
```

## Error Handling

- DHT timeouts: log warning, remove node from routing table if unresponsive 3x
- Metadata fetch failures: log info, mark attempted in DB, skip for 24h
- SQLite errors: propagate as fatal (database is core to operation)
- UDP/TCP socket errors: retry with backoff, after 3 failures log error and continue

## Testing Strategy

- Unit tests for KRPC message encoding/decoding, bencode handling
- Unit tests for routing table operations
- Unit tests for SQLite queries and deduplication
- Integration test: run crawler against real DHT for N seconds, verify SQLite has entries
