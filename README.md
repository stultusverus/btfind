# btfind

BitTorrent DHT sniffer — crawls the DHT network, discovers torrents, fetches metadata (BEP 9), stores results in MySQL, and provides CLI search, magnet link export, and a local HTTP dashboard.

## Installation

```bash
cargo install --path .
```

Requires current stable Rust and MySQL 8.4 or newer with the built-in ngram parser.

## Usage

```
btfind run       Start the crawler
btfind search    Search collected torrents
btfind magnet    Print a magnet URI for an info hash
btfind stats     Show crawler statistics
btfind serve     Start local HTTP API/dashboard
btfind prune     Remove old torrents
btfind db        Migrate/check schema, import/validate SQLite, rebuild search, reconcile statistics
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

Search uses MySQL ngram indexes with two-character tokens for Japanese, Chinese,
and Latin text. Punctuation and filename separators split terms; every entered
term must match and must contain at least two alphanumeric characters. Names
receive a relevance boost over file paths. Raw Boolean operators are not exposed.
Empty queries browse the catalog. This intentionally changes SQLite search semantics.

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
GET /api/search?q=ubuntu      Search torrents (ngram, minimum 2 characters per term)
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
max_peers_per_hash = 64
max_active_hash_jobs = 1024
max_metadata_size_bytes = 8388608
max_peer_attempts_per_round = 8

[database]
pool_size = 8
web_pool_size = 4
batch_size = 128
flush_interval_ms = 1000
# tls_ca = "/absolute/path/mysql-ca.pem"

[crawl]
get_peers_interval_secs = 5
bucket_refresh_mins = 15
resume_info_hash_limit = 5000
dht_node_resume_limit = 1024
dht_node_max_age_hours = 24
info_hash_channel_capacity = 1024
stats_channel_capacity = 4096
max_discovery_hashes = 50000
max_pending_rpcs = 4096
max_candidate_nodes = 8192
rpc_timeout_secs = 30
sampling_enabled = false
sampling_interval_secs = 5
sampling_min_remote_interval_secs = 300
sampling_requests_per_tick = 1
max_samples_per_response = 256
announced_peer_hash_capacity = 10000
announced_peers_per_hash = 64
announced_peer_ttl_secs = 1800
shutdown_drain_secs = 10

[web]
host = "127.0.0.1"
port = 8080
```

CLI flags override config values. Set BTFIND_DATABASE_URL in a protected service
environment file. TLS certificate and hostname verification are enabled by
default; tls_ca supplies a private CA when required. database.path is obsolete.
Run `btfind db migrate` explicitly before starting services.

All queue, job, RPC, contact, peer, metadata, and sampling limits are validated before the runtime starts. Full discovery queues are coalesced into durable hash-job rows where admitted by the metadata scheduler; excess in-memory jobs remain in MySQL for the retry scan. `sampling_enabled` enables bounded random BEP 51 probing of validated live nodes. It is an opt-in discovery aid, not a keyspace traversal or coverage-complete sweep.

## How It Works

Three subsystems run on a Tokio async runtime:

- **DHT Crawler** — Bootstraps into the BitTorrent DHT via Kademlia, validates every response against its outbound RPC, and discovers info_hashes passively or through optional bounded random BEP 51 sampling.
- **Metadata Fetcher** — Receives discovered info_hashes, connects to peers via TCP (BitTorrent wire protocol), negotiates BEP 9 extensions (`ut_metadata`), and downloads torrent metadata (name, size, file list).
- **MySQL Store** — Persists torrents, files, and crawler state using an async connection pool. Uses ngram full-text search over names and paths, transactional statistics, and bounded pruning.

Data flows through bounded `tokio::sync::mpsc` channels. Metadata work is deduplicated per info hash, runtime persistence is batched according to the configured database batch size and flush interval, and retry state plus peer candidates survive process restarts. Stable transaction receipts prevent replayed batches from incrementing counters twice. SQLite is used only for reading migration snapshots.

## Database

Connection: BTFIND_DATABASE_URL. Versioned schema: migrations/.
Search and file-list API responses use has_next instead of exact totals.
Run `cargo test` for offline tests, and `bash scripts/test-mysql.sh` with
BTFIND_TEST_DATABASE_URL for the required local MySQL integration suite.
Run `node tests/dashboard.test.cjs` for dashboard behavior tests (Node.js 18+).
After restoring a logical dump, run `btfind db analyze` with the maintenance
account before serving searches.

## License

MIT
