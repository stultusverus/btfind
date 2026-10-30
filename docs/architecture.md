# Current architecture

Updated 2026-10-29 after removal of SQLite import support. This document describes
the maintained implementation; the dated files under `superpowers/` and the root
review logs are historical records. See [README](../README.md) for configuration,
commands, maintenance constraints, and test instructions.

## Processes and data flow

`btfind run` starts the DHT crawler, metadata scheduler, and runtime statistics
persistence worker in one Tokio runtime. It runs continuously unless `--duration`
is supplied; there is no `--continuous` option. `btfind serve` is a separate Axum
HTTP dashboard/API process, using its own SQLx pool and the same MySQL database.

The DHT crawler publishes `(InfoHash, Vec<PeerContact>)` through a bounded channel
to the metadata fetcher. Crawler and fetcher statistics flow through a separate
bounded channel to the persistence worker. Database I/O is asynchronous.

| Module | Responsibility |
| --- | --- |
| `src/main.rs` | CLI dispatch, process startup/shutdown, runtime event batching |
| `src/logging.rs` | JSON logging initialisation, service identity, panic hook |
| `src/metrics.rs` | Bounded crawler measurements and web request/latency snapshots |
| `src/config.rs` | TOML configuration, defaults, validation, XDG config paths |
| `src/dht.rs`, `src/routing.rs` | UDP KRPC, bootstrap, routing, peer/hash discovery, optional BEP 51 sampling |
| `src/bencode.rs`, `src/types.rs` | KRPC encoding and shared types |
| `src/metadata.rs`, `src/wire.rs` | Durable metadata scheduling, peer attempts, BitTorrent handshakes and BEP 9 downloads |
| `src/store.rs` | SQLx pools, transactions, queries, counters, search documents, pruning |
| `src/db.rs` | MySQL migrate/check, reconcile, rebuild-search, and analyze commands |
| `src/web.rs`, `src/web_dashboard.*` | Read-only API and embedded dashboard |
| `src/magnet.rs` | Magnet validation and formatting |

## Discovery and metadata

The crawler uses one IPv4 UDP socket and an in-memory Kademlia routing table.
Configured bootstrap endpoints seed discovery. Pending RPCs track transaction
IDs and expected responders; validated nodes enter the live routing table.
Incoming queries, peer lookups, announces, and optional sampling produce work.
Sampling is bounded and disabled by default.

The metadata scheduler deduplicates hashes and limits concurrent hash workers.
Discovery persistence writes the torrent, job, supplied peer addresses, and peer
retention changes in one transaction, locking catalogue totals before the torrent.
Peer upserts and retention deletes use bounded batches. Replays preserve unique
torrent/peer keys and do not increment catalogue totals twice; completed jobs stay
complete. Concurrent discoveries serialize through the existing catalogue lock.
Accepted peer-bearing discoveries belong to the metadata worker, which has the
actual addresses. If its channel is full or closed, the crawler marks the
statistics observation as peerless so the statistics worker can retain the hash
and source for future lookup. This fallback retains no peer addresses and requires
the bounded statistics channel to accept the event and its worker to commit it.
Committed retained peer candidates and retry state survive restarts. Workers
negotiate `ut_metadata`, retrieve and validate the info dictionary, and commit
metadata, files, search documents, job completion, and counters atomically.
`MetadataFetched` is emitted after persistence succeeds. Timeout, retry, queue,
and size limits are configured in `src/config.rs`; tests use simulated peers.

## MySQL storage

MySQL 8.4 or newer with InnoDB and the ngram parser is required. Connections come
from `BTFIND_DATABASE_URL`; TLS verifies the certificate and hostname, with an
optional private CA. The default pools have eight crawler and four web
connections. Local disposable tests can explicitly disable TLS for loopback.

Schema SQL in `migrations/` is embedded in the binary. `btfind db migrate` applies
pending versions with a maintenance account; service startup checks the exact
migration versions and checksums. Previously applied migration files must not be
rewritten. SQLite drivers, snapshot import/validation, and fingerprint/checkpoint
code have been removed. The historical `import_progress` table is unused but
remains in migration 0001 and existing databases.

Info hashes are stored as hex text. Foreign keys cascade dependent torrent data.
Catalogue mutations lock the singleton `catalog_totals` row before torrent rows,
keeping catalogue changes and aggregate counters atomic. This serializes writes;
local concurrency tests do not establish remote HeatWave throughput. Metadata
transactions retry deadlocks, MySQL lock-wait error 1205, I/O failures, and pool
timeouts within their retry limit.

Runtime event batches have stable receipt IDs so uncertain-commit retries do not
increment counters twice. The single persistence worker retries until success,
acknowledges receipts, and serially prunes acknowledgements older than seven days.
Unacknowledged and legacy receipts are retained. Run one crawler persistence
worker per database.

## Search and maintenance

Search documents contain normalized torrent names and file paths. Full-text
indexes use two-character ngrams with stopwords disabled on application
connections. This supports Japanese, Chinese, and Latin text. Every query term
must have at least two alphanumeric characters and match; names receive a
relevance boost. Empty queries browse the catalogue. Sort fields are whitelisted,
SQL values are bound, and pagination returns `has_next` instead of exact totals.

Ingestion and rebuilds share a streamed document builder. The per-document limit
is 16 MiB, reduced to fit `max_allowed_packet` with 16 KiB of headroom; raw fields
are bounded too, and normalization checks its output incrementally. The separate
rebuild batching target is 128 documents or approximately 1 MiB. A valid document
larger than that target is written alone. Both paths preserve all searchable
content or fail explicitly without truncation. An ingestion failure rolls back
metadata changes. A typed document-size failure marks the metadata job
`unsupported`; the worker persists that terminal state without fetching from
another peer and checks terminal states before subsequent downloads, including
rediscoveries. Database write failures remain retryable. Unsupported jobs require
explicit requeuing after a limit change. Completed rebuild batches remain
committed; a rebuild can be
rerun. See the README for exact limits and memory behavior. Rebuilds, schema migrations, and counter reconciliation
require stopped services and never run within HTTP requests. After a logical
restore, `db analyze` refreshes optimizer/full-text statistics.

## Verification

`cargo test` runs offline tests; database integration tests are explicitly ignored
there. `scripts/test-mysql.sh` runs them against isolated databases on a disposable
local MySQL server selected by `BTFIND_TEST_DATABASE_URL`. No tests contact real
DHT nodes or peers. Run clippy and formatting checks before commits, and
`node tests/dashboard.test.cjs` for dashboard behavior changes.
