# btfind

BitTorrent DHT sniffer — crawls the DHT network, discovers torrents, fetches metadata (BEP 9), stores results in SQLite, and provides CLI search, magnet link export, and a local HTTP dashboard.

## Installation

```bash
cargo install --path .
```

Requires Rust 1.73+.

## Usage

```
btfind run       Start the crawler
btfind search    Search collected torrents
btfind magnet    Print a magnet URI for an info hash
btfind stats     Show crawler statistics
btfind serve     Start local HTTP API/dashboard
btfind prune     Remove old torrents
```

### btfind run

```bash
# Run for 60 seconds
btfind run --duration 60

# Run continuously (Ctrl+C to stop)
btfind run

# Custom port and concurrency
btfind run --port 6882 --max-concurrent 20
```

### btfind search

Search treats the query as a literal substring-style term. Advanced FTS5 query syntax is not exposed by default. Normal queries (3+ characters) use SQLite FTS5 with the trigram tokenizer, with results ranked by relevance. Short queries (1-2 characters) fall back to a `LIKE` substring search with deterministic ordering (no relevance rank).

```bash
# Search by name or file path
btfind search --query ubuntu

# Sort by rank (relevance)
btfind search --query iso --sort rank

# Sort by size
btfind search --query iso --sort total_size

# JSON output with magnet URIs
btfind search --query debian --json --magnet

# Show all, sorted by name
btfind search --sort name --limit 100
```

### btfind magnet

```bash
btfind magnet 0123456789abcdef0123456789abcdef01234567
btfind magnet <info_hash> --name "Ubuntu ISO"
```

### btfind serve

```bash
btfind serve --port 8080
```

The dashboard binds to 127.0.0.1 by default and is intended as a local read-only interface.

### btfind stats

```
btfind stats
```

Outputs: total torrents, metadata complete count, files indexed, total data size.

### btfind prune

```bash
# Remove torrents not seen in 90 days
btfind prune --older-than 90
```

## HTTP API

When `btfind serve` is running:

```
GET /                         Dashboard HTML page
GET /api/search?q=ubuntu      Search torrents (literal, FTS5/ranked for 3+ char queries)
                            Optional: sort, limit, complete_only, min_size, max_size
GET /api/stats                Crawler statistics
GET /api/torrents/<info_hash>  Torrent detail
GET /api/torrents/<info_hash>/files  Paginated file list; optional q, limit, offset
GET /api/torrents/<info_hash>/magnet  Magnet URI (text/plain)
```

## Configuration

Optional config at `~/.config/btfind/config.toml`:

```toml
[network]
port = 6881
bootstrap_nodes = [
    "67.215.246.10:6881",
    "87.98.162.88:6881",
    "82.221.103.244:6881",
]

[metadata]
max_concurrent = 10
peer_timeout_secs = 30
retry_after_hours = 24
peer_attempt_retention_hours = 72

[database]
path = "~/.local/share/btfind/torrents.db"

[crawl]
get_peers_interval_secs = 5
bucket_refresh_mins = 15
resume_info_hash_limit = 5000
dht_node_resume_limit = 1024
dht_node_max_age_hours = 24

[web]
host = "127.0.0.1"
port = 8080
```

CLI flags override config values.

## How It Works

Three subsystems run on a Tokio async runtime:

- **DHT Crawler** — Bootstraps into the BitTorrent DHT via Kademlia, walks the network with `find_node`, and discovers info_hashes from `get_peers` and `announce_peer` messages (KRPC over UDP).
- **Metadata Fetcher** — Receives discovered info_hashes, connects to peers via TCP (BitTorrent wire protocol), negotiates BEP 9 extensions (`ut_metadata`), and downloads torrent metadata (name, size, file list).
- **SQLite Store** — Persists torrents and files with deduplication by info_hash. Uses FTS5 full-text search (trigram tokenizer) over torrent names and file paths, provides statistics, and pruning.

Data flows through `tokio::sync::mpsc` channels: DHT crawler → metadata fetcher → SQLite.

## Database

Stored at `~/.local/share/btfind/torrents.db` (SQLite, WAL mode).

## License

MIT
