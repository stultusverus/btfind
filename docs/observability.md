# Observability

Production runs two systemd user services on Oracle Linux 9.8: `btfind.service`
(the crawler) and `btfind-web.service` (the read-only HTTP process). This
document covers the checked-in logging, metrics, deployment examples, and the
one-shot OCI exporter.

OCI's Custom Logs Monitoring plugin is **not supported** on the
`VM.Standard.E2.1.Micro` shape used by the production host. The exporter therefore
calls OCI Logging and Monitoring directly with instance principals. No cloud
agent, local proxy, Prometheus server, or resident collector is required. The
production infrastructure (log groups, custom logs, dynamic group, policies)
is deliberately **not** created by this repository; those actions follow after
the code changes are merged.

## Architecture

```
btfind run ──JSON──┐
                   ├── journald (persistent, bounded) ──> btfind-observability.service
btfind serve ─JSON─┘                                         │
                                                             ├── OCI Logging ingestion
                                                             └── OCI Monitoring ingestion
```

- Both Rust processes write one JSON object per line. systemd sends stdout/stderr
  to journald with distinct `SyslogIdentifier` values.
- journald provides bounded local buffering and rotation. The applications do not
  write JSONL files and there is no logrotate configuration.
- `btfind-observability.timer` starts a one-shot Python exporter. It keeps
  independent log and metric cursors, so a failure at one destination does not
  block the other.
- The crawler and HTTP process never call OCI. If OCI is unavailable, they keep
  running and journald retains records until its configured limits are reached.

## Rust JSON event schema

Logging is configured by `src/logging.rs`. The default filter is
`RUST_LOG=btfind=info`. The formatter emits a single JSON object per event with
these top-level keys:

| Key | Meaning |
| --- | --- |
| `timestamp` | UTC RFC 3339 timestamp with microseconds |
| `level` | `TRACE`, `DEBUG`, `INFO`, `WARN`, or `ERROR` |
| `target` | Rust module target |
| `service` | `crawler`, `web`, or `btfind` for CLI commands |
| `version` | `CARGO_PKG_VERSION` |
| `revision` | Build-time `BTFIND_GIT_REVISION`; defaults to the Git commit, with an explicit environment override for archive builds |
| `run_id` | Process-lifetime random UUID for counter reset detection |
| `fields` | Event message and structured fields |

Event fields are nested under `fields`, matching `tracing-subscriber`'s JSON
formatter; this is not a flat schema. Event names are stable strings in
`fields.event`. Example:

```json
{"timestamp":"2026-10-30T09:51:35.824568Z","level":"INFO","target":"btfind","service":"crawler","version":"0.1.0","run_id":"eaf0eb1a-440d-4fcd-8fb6-9c073f8bfa08","fields":{"event":"crawler_status","known_nodes":42,"queries_sent_total":100,"metadata_failed_total":3,"interval_seconds":60,"message":"crawler status"}}
```

Lifecycle events include `process_started`, `configuration_failed`,
`configuration_loaded`, `database_connected`, `crawler_listening`,
`crawler_started`, `crawler_shutdown_received`, `crawler_shutdown`, `web_started`,
`web_listening`, `web_shutdown`, and `process_panic`. Startup failures use
`startup_failed` with a bounded `component` and `reason`. High-volume expected
conditions (malformed KRPC, individual peer timeouts, per-request diagnostics)
are `DEBUG`; aggregate counters are reported through the status events.

CLI commands initialise logging on stderr so `btfind search --json` and other
machine-readable stdout remains uncontaminated. The crawler and web service log to
stdout, which systemd captures in the journal.

## Metrics

`crawler_status` is emitted every 60 seconds and once more on graceful shutdown.
Counters are cumulative since process start (`*_total`); gauges are instantaneous.
The `run_id`, `process_started_unix`, and `uptime_seconds` fields identify the
run so a restart is never mistaken for counter continuity. The field set is fixed
by design; discovery sources and metadata outcomes are encoded in field names
rather than runtime labels, and no info hashes or peer addresses are logged.

**Crawler and RPC**

| Field | Kind | Unit | Meaning |
| --- | --- | --- | --- |
| `known_nodes` | gauge | count | Nodes currently in the live routing table |
| `queries_sent_total` | counter | count | Outbound KRPC queries sent |
| `rpc_responses_total` | counter | count | Validated RPC responses |
| `rpc_timeouts_total` | counter | count | RPCs that timed out |
| `rpc_malformed_total` | counter | count | Malformed KRPC packets |
| `rpc_unsolicited_total` | counter | count | Unsolicited response packets |
| `rpc_unexpected_source_total` | counter | count | Responses received from an unexpected source |
| `hashes_discovered_total` | counter | count | Info-hash observations recorded by the crawler (cumulative; repeated observations are counted again) |
| `sampling_responses_total` | counter | count | BEP 51 sampling responses processed |
| `backpressure_events_total` | counter | count | Discovery-channel send failures attributed to backpressure |

**Metadata retrieval and queue**

| Field | Kind | Unit | Meaning |
| --- | --- | --- | --- |
| `metadata_fetched_total` | counter | count | Successful metadata retrievals since process start |
| `metadata_fetched_interval` | interval count | count | Successful metadata retrievals completed since the previous `crawler_status` emission |
| `metadata_failed_total` | counter | count | Failed metadata fetch attempts (reasons are persisted in `metadata_failure_counts`) |
| `active_metadata_jobs` | gauge | count | Metadata jobs with an active worker |
| `queued_metadata_jobs` | gauge | count | Metadata jobs waiting for a worker |
| `pending_metadata_peers` | gauge | count | Peer addresses pending across tracked metadata jobs |

**Discovery observations**

| Field | Kind | Unit | Meaning |
| --- | --- | --- | --- |
| `discovery_observations_total` | counter | count | Info-hash observations counted in-process, regardless of the bounded discovery-channel send outcome |
| `discovery_observed_inbound_get_peers_total` | counter | count | Observations sourced from inbound `get_peers` queries |
| `discovery_observed_announce_peer_total` | counter | count | Observations sourced from inbound `announce_peer` queries |
| `discovery_observed_resume_total` | counter | count | Observations sourced from persisted-hash resume |
| `discovery_observed_sample_infohashes_total` | counter | count | Observations sourced from optional BEP 51 `sample_infohashes` probing |
| `discovery_observed_outbound_get_peers_total` | counter | count | Observations sourced from the crawler's own outbound `get_peers` lookups |
| `discovery_peer_bearing_observations_total` | counter | count | Observations that carried at least one peer address |
| `discovery_peerless_observations_total` | counter | count | Observations that carried no peer address |
| `discovery_channel_accepted_total` | counter | count | Bounded discovery-channel sends that succeeded |
| `discovery_channel_rejected_total` | counter | count | Bounded discovery-channel sends rejected because the channel was full or closed |
| `discovery_peer_bearing_rejected_total` | counter | count | Peer-bearing observations rejected by the bounded discovery channel |
| `discovery_peer_addresses_lost_total` | counter | count | Peer addresses lost with rejected peer-bearing observations |
| `discovery_queue_depth` | gauge | count | Queued hashes in the bounded discovery / real-hash lookup queue |
| `discovery_queue_capacity` | gauge | count | Configured capacity of that queue (`max_discovery_hashes`) |

**DHT lookup**

| Field | Kind | Unit | Meaning |
| --- | --- | --- | --- |
| `lookup_queue_accepted_total` | counter | count | Hashes admitted to the bounded discovery / real-hash lookup queue |
| `lookup_queue_duplicate_total` | counter | count | Admissions skipped because the hash was already queued |
| `lookup_queue_capacity_rejected_total` | counter | count | Admissions rejected because the queue was at `max_discovery_hashes` |
| `real_lookups_initiated_total` | counter | count | Real hash lookups that sent at least one initial `get_peers` RPC |
| `real_lookups_succeeded_total` | counter | count | Real lookups that closed after at least one response carried peer values |
| `real_lookups_failed_total` | counter | count | Real lookups that closed without any response carrying peer values |
| `real_lookup_rpc_timeouts_total` | counter | count | `get_peers` RPCs belonging to real lookups that timed out |
| `real_lookup_rpc_failures_total` | counter | count | `get_peers` RPCs belonging to real lookups that failed otherwise |
| `get_peers_responses_with_peers_total` | counter | count | `get_peers` responses that carried peer values |
| `get_peers_responses_nodes_only_total` | counter | count | `get_peers` responses that carried nodes only |

**Metadata scheduler and retrieval outcomes**

| Field | Kind | Unit | Meaning |
| --- | --- | --- | --- |
| `metadata_job_admissions_accepted_total` | counter | count | Admission attempts that created a job or added new peer addresses to a tracked job |
| `metadata_job_admissions_unchanged_total` | counter | count | Admission attempts for a tracked hash that added no new peer addresses |
| `metadata_job_admissions_no_peers_total` | counter | count | Admission attempts for a hash without usable peers |
| `metadata_job_admissions_at_capacity_total` | counter | count | Admission attempts declined because the in-memory scheduler was at capacity |
| `metadata_retrieval_complete_total` | counter | count | Completed rounds where metadata was committed |
| `metadata_retrieval_retry_at_total` | counter | count | Completed rounds where the job was parked for retry at a future time |
| `metadata_retrieval_waiting_peers_total` | counter | count | Completed rounds where the job was parked waiting for usable peers |
| `metadata_retrieval_invalid_total` | counter | count | Terminal invalid metadata/parse outcomes |
| `metadata_retrieval_unsupported_total` | counter | count | Terminal unsupported metadata/retrieval outcomes |

**Database persistence**

| Field | Kind | Unit | Meaning |
| --- | --- | --- | --- |
| `persistence_errors_total` | counter | count | Failed database operations on persistence/retry-state paths (retained; now covers more paths) |
| `torrents_inserted_total` | counter | count | Newly inserted torrent rows |
| `hash_jobs_created_total` | counter | count | Newly created metadata `hash_jobs` rows |
| `peerless_jobs_skipped_total` | counter | count | Peerless persistence attempts (including peer-bearing fallbacks rejected by the discovery channel) that retained/updated the torrent row without creating a new `hash_jobs` row |
| `peer_bearing_discoveries_persisted_total` | counter | count | Peer-bearing discoveries whose torrent/job/peer transaction committed |
| `peer_records_inserted_total` | counter | count | Peer rows inserted by committed persistence transactions |
| `peer_records_updated_total` | counter | count | Peer rows updated by committed persistence transactions |
| `peer_records_deleted_total` | counter | count | Peer rows deleted by retention in committed persistence transactions |

### Crawler measurement semantics

- **Cumulative counters, gauges, and interval counts.** Every `*_total` field is
  cumulative since process start and resets to zero on restart; `run_id`
  identifies the run. Gauges are instantaneous samples. `metadata_fetched_interval`
  is the only interval count: successful metadata retrievals completed since the
  previous `crawler_status` emission. `metadata_fetched_total` remains the
  cumulative counter.
- **Observed versus accepted versus persisted.** "Observed" counts info-hash
  observations in-process, regardless of whether the bounded discovery-channel
  send succeeds. "Accepted" and "rejected" describe that send outcome. "Persisted"
  means the database transaction committed. Keeping these stages distinct makes
  discovery loss and backpressure measurable. Discovery sources are the fixed
  five-value set above; their cardinality is encoded in fixed field names, never
  in a runtime label or log field, and no info hashes or peer addresses are
  logged.
- **Discovery backpressure.** Observations and their source are counted
  regardless of the channel send outcome. A peer-bearing discovery rejected by
  backpressure loses its peer addresses:
  `discovery_peer_bearing_rejected_total` counts rejected discoveries, while
  `discovery_peer_addresses_lost_total` counts the addresses they carried
  (addresses, not discoveries). The rejected discovery is still handed to the
  statistics path as a peerless fallback: the hash and source are retained but the
  peer addresses are not, and the fallback is persisted as a torrent row without
  a new `hash_jobs` row. It is not counted in
  `discovery_peerless_observations_total`, because the original observation was
  peer-bearing.
- **Lookup queue and per-hash lookups.** Admission counters cover the bounded
  discovery / real-hash lookup queue: duplicate means the hash was already
  queued, and capacity-rejected means the queue was already at
  `max_discovery_hashes`. A real lookup is initiated when at least one initial
  `get_peers` RPC is actually sent, and it closes when all its RPCs complete. A
  lookup succeeds when at least one response carried peer values and fails when
  it completed without peers. Per-hash lookup success rate over an interval is
  `Δreal_lookups_succeeded_total / (Δreal_lookups_succeeded_total +
  Δreal_lookups_failed_total)`, where `Δ` is the change over the interval. This
  rate is distinct from the per-RPC response counters
  (`get_peers_responses_*_total`) and `real_lookup_rpc_*_total`.
- **Metadata admission and retrieval outcomes.** Admission counters count each
  in-memory scheduler admission attempt, whether it arrives from a discovery
  delivery or from the 30-second retry-scan restore
  (accepted/unchanged/no_peers/at_capacity). Retrieval outcome counters are
  durable job-state transitions, counted once per completed round and only after
  the state write succeeded; failed or no-op writes are not counted. `complete`
  means metadata was committed; `retry_at` and `waiting_peers` mean the job was
  parked for retry; `invalid` and `unsupported` are terminal metadata/parse
  outcomes.
- **Persistence accounting.** Torrent, job, and peer counters count work
  committed by persistence transactions: newly inserted torrent rows, newly
  created job rows, peer-bearing discoveries whose transaction committed, and
  committed peer-row inserts, updates, and deletes. An observation persisted as peerless (a genuinely
  peerless observation, or a peer-bearing fallback whose addresses were rejected
  by the discovery channel) durably inserts or updates the torrent row but
  deliberately does not create a new `hash_jobs` row, counted in
  `peerless_jobs_skipped_total`; the job row is created when a later peer-bearing
  discovery arrives. Failed, rolled-back, or retried
  operations are never counted as successful. `persistence_errors_total` counts
  failed database operations on persistence/retry-state paths. Uncertain-commit
  retries are deduplicated by stable receipts, so committed work is not counted
  twice.
- **Collection timing.** Crawler and metadata workers update the in-process
  metric registry, including critical counters, before any bounded
  statistics-channel send. Measurement therefore continues when the statistics
  channel is saturated or the database writer is failing/backing off; it does not
  depend on the database writer draining its queue. Interval deltas are derived
  downstream by the OCI exporter from the cumulative counters.

### Web request measurements

`web_status` aggregates a fixed 60-second window. `web_route_status` carries the
same status and duration fields per route template, and `web_latency_bucket`
carries `le` and `count` for non-empty bounded latency buckets.

| Field | Kind | Unit |
| --- | --- | --- |
| `window_start_unix`, `window_end_unix`, `interval_seconds` | window boundaries | seconds |
| `requests_total`, `requests_2xx`, `requests_3xx`, `requests_4xx`, `requests_5xx` | window count | count |
| `duration_count` | window count | count |
| `duration_sum_ms` | window sum | milliseconds |
| `active_requests` | gauge | count |
| `database_errors` | window count | count |
| `route` | bounded route template | label |
| `le` | latency upper bound in ms, or `+Inf` | label |

Latency buckets are 5, 10, 25, 50, 100, 250, 500, 1000, 2500, and 5000 ms, plus
`+Inf`. Mean is `duration_sum_ms / duration_count`; approximate percentiles come
from the bucket boundaries. Routes are the server's fixed templates, not concrete
info hashes.

### OCI Monitoring mapping

The crawler metric set covers discovery, DHT lookup, metadata
admission/retrieval, and database-persistence measurements in addition to the
original RPC, metadata, and queue counters. Each `crawler_status` field maps to a
bounded CamelCase OCI metric name, for example `discovery_observations_total` →
`CrawlerDiscoveryObservations` and `peer_records_inserted_total` →
`CrawlerPeerRecordsInserted`. `metadata_fetched_interval` is published directly as
`CrawlerMetadataFetchedInterval` with `window_count` semantics. Crawler metrics
carry only the `instance` and `service` dimensions. Web metrics additionally carry
`route`, `status_class`, and `le`; OCI Monitoring dimensions overall remain
limited to `instance`, `service`, `route`, `status_class`, and `le`. Metric
metadata records `unit` and `semantics` (`gauge`, `interval_delta`,
`window_count`, or `window_sum`). The exporter converts crawler cumulative
counters to interval deltas using a stored `run_id` baseline; a new run starts at
a zero delta rather than republishing reset counters.

### Volume controls

- Normal operation emits at most a few INFO status records per minute per
  service. A `web_status` plus per-route and non-empty bucket records are bounded
  by the fixed route and bucket sets.
- Individual DHT requests, peer addresses, malformed packets, and retry decisions
  are DEBUG. INFO does not contain full search queries, torrent names, file paths,
  database URLs, credentials, or high-cardinality peer identifiers.
- journald is capped at 64 MiB with 1 GiB kept free, 8 MiB files, and a three-day
  age cap. Actual retention is the shorter of age and available space.

### Metadata failure diagnosis

`btfind stats` reports persisted failure counts. New attempts distinguish
`connect_timeout`, `handshake_timeout`, `extension_timeout`, and
`metadata_timeout`; transport errors use `connect`, `handshake_io`,
`extension_io`, and `metadata_io`. Invalid handshakes use `handshake`, invalid
extension negotiation uses `extension_protocol`, and invalid metadata frames use
`protocol`. EOF and truncated reads retain their I/O stage and source error in
the stored peer failure text.

Unavailable metadata is split into `extensions_unsupported`,
`metadata_unsupported`, `metadata_disabled`, and `metadata_size_missing`.
`metadata_too_large` identifies the configured size limit; `rejected` identifies
an explicit peer rejection. These availability/size/rejection failures use the
existing long retry base (900 seconds); transport and malformed-protocol failures
use the short base (30 seconds). Exponential backoff, jitter, and the configured
peer retry interval still apply. Historical `timeout` and `extension` counts
remain readable alongside the new categories; no migration rewrites old data.

## Expected OCI resources

The later manual infrastructure stage should create resources with these names:

| Resource | Suggested name |
| --- | --- |
| Log group | `btfind-production` |
| Crawler custom log | `btfind-crawler` |
| Web custom log | `btfind-web` |
| Metric namespace | `btfind` |
| Metric compartment | configured separately, supplied as `OCI_METRICS_COMPARTMENT_OCID` |

The exporter authenticates with
`oci.auth.signers.InstancePrincipalsSecurityTokenSigner`. A dynamic group should
match the production instance, for example
`instance.id = '<instance-ocid>'`. Example least-privilege policy statements,
for documentation only:

```text
Allow dynamic-group btfind-observability to use log-content in compartment <observability-compartment>
Allow dynamic-group btfind-observability to use metrics in compartment <observability-compartment>
```

No API signing key, private key, or secret belongs on the host. Never commit real
log, compartment, tenancy, or instance OCIDs; `deploy/observability.env.example`
contains placeholders only.

## Oracle Linux 9 setup

Install the system-wide journald policy. Oracle Linux 9 may store user-service
output in per-user journal files, so `journalctl --user` uses the user manager's
view while the exporter reads the aggregate journal with explicit UID/unit
filters. Enable lingering so the user services start without an interactive
login.

```bash
sudo install -d -m 0755 /etc/systemd/journald.conf.d
sudo install -m 0644 deploy/journald/30-btfind.conf \
  /etc/systemd/journald.conf.d/30-btfind.conf
sudo systemctl restart systemd-journald
sudo loginctl enable-linger opc
```

The configuration enables persistent storage and bounds total usage. The
exporter queries the aggregate journal as root with explicit `_UID` and
`_SYSTEMD_USER_UNIT` filters; it deliberately does not pass `--system`, because
persistent journald with `SplitMode=uid` can store user-service records in
per-user journal files. It also does not use `journalctl --user`'s user-manager
view. Use:

```bash
journalctl --user-unit btfind.service
journalctl --user-unit btfind-web.service
journalctl _SYSTEMD_USER_UNIT=btfind.service
journalctl -t btfind-observability
```

Install the user units from `deploy/systemd/` under
`/home/opc/.config/systemd/user/`, then:

```bash
systemctl --user daemon-reload
systemctl --user enable --now btfind.service btfind-web.service
```

The units preserve `KillSignal=SIGINT`, `TimeoutStopSec=45`,
`Restart=on-failure`, the service environment files, and add
`StandardOutput=journal`, `StandardError=journal`, `SyslogIdentifier`, and
`Environment=RUST_LOG=btfind=info`. The service environment files are expected at
`/home/opc/.config/btfind/btfind.env` and `btfind-web.env`.

## Install and run the exporter

```bash
sudo install -d -m 0755 /opt/btfind-observability
sudo cp -a scripts/oci-observability/. /opt/btfind-observability/
sudo python3 -m venv /opt/btfind-observability/venv
sudo /opt/btfind-observability/venv/bin/pip install \
  --requirement /opt/btfind-observability/requirements.txt
sudo install -d -m 0700 /var/lib/btfind-observability
sudo install -d -m 0755 /etc/btfind
sudo install -m 0600 deploy/observability.env.example \
  /etc/btfind/observability.env
sudo install -m 0644 deploy/systemd/btfind-observability.service \
  /etc/systemd/system/
sudo install -m 0644 deploy/systemd/btfind-observability.timer \
  /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now btfind-observability.timer
```

Edit `/etc/btfind/observability.env` before enabling the timer. The unit runs as
root only to read the aggregate journal; it has no resident daemon, uses
`StateDirectory=btfind-observability`, `UMask=0077`, and a 192 MiB memory cap.
The exporter's diagnostics use `SyslogIdentifier=btfind-observability`, which is
not among the two source identifiers and therefore cannot be ingested
recursively.

State is `/var/lib/btfind-observability/state.json`, mode `0600`. It stores a
format version, separate `logs`/`metrics` cursors, last-success timestamps,
metric baselines, and up to ten gap diagnostics per stream. Checkpoints use a
same-directory temp file, `fsync`, atomic rename, and directory `fsync`. An
exclusive lock prevents overlapping runs. Route and metric state are bounded.

The `scripts/oci-observability/README.md` documents all environment variables and
exit codes. `deploy/observability.env.example` supplies the production defaults
for `ap-osaka-1`, `vm-sub01`, batching, lookback, and the freshness cutoff.

## Delivery and failure semantics

- **At least once, not exactly once.** Log partitions are grouped by service and
  the log cursor advances only after every non-empty partition is acknowledged.
  A partial failure can resend an already accepted partition. The stable UUIDv5
  entry id is derived from machine, boot, and journal cursor; it helps local
  analysis deduplicate retries, but OCI does not guarantee server-side dedupe.
- **Independent streams.** Log and metric cursors, retries, and checkpoint updates
  are separate. A metrics failure does not block log shipping, and vice versa.
  Each destination also runs as a separately terminable child process with its own
  cooperative budget (`LOG_EXPORT_BUDGET_SECONDS`,
  `METRICS_EXPORT_BUDGET_SECONDS`) and hard parent deadline
  (`LOG_EXPORT_HARD_TIMEOUT_SECONDS`, `METRICS_EXPORT_HARD_TIMEOUT_SECONDS`).
  Log retries stop at the cooperative budget; the parent kills a child that
  outlives its hard deadline, then starts the metric child with its own deadline.
  This handles in-flight requests because a socket read timeout is not a total
  request deadline. The OCI per-request timeout is clamped to one third of the
  smaller cooperative budget, and requests are chunked so the budget is checked
  between calls. Default hard deadlines sum to 70 seconds, leaving room under the
  example 90-second systemd timeout; raise them together only after reviewing
  that limit.
  Each child validates only the resources for its destination and constructs only
  the required OCI client. The logs child needs both custom log OCIDs; the metrics
  child needs only the metric compartment OCID. A missing log OCID, Logging client
  failure, missing metric compartment, or Monitoring client failure is isolated
  to that destination.
- **Bounded retries.** Throttling, 5xx, timeout, network, and connection failures
  use bounded exponential backoff. Authentication, authorization, missing
  resource, and other permanent errors are reported distinctly and do not advance
  the cursor.
- **No full backfill.** A first run without state looks back five minutes. A
  missing cursor is not treated as permission to upload the entire journal.
- **Gaps are explicit.** If journald vacuums the saved cursor, the exporter records
  a local `cursor_invalid` gap and resumes from the earliest retained eligible
  record. If journal storage fills, records may be lost; that loss is bounded by
  the journald policy, not recovered later.
- **Oversized entries.** A record whose OCI `LogEntry.data` would exceed 10,000
  characters is written to the root-only dead-letter file and replaced by a small
  `log_entry_oversized` diagnostic. This avoids silent truncation and prevents one
  record from blocking the queue. Dead-letter storage is capped by
  `DEAD_LETTER_MAX_BYTES` and rotates to `dead-letter.jsonl.1`, bounding total
  on-disk use. If a record cannot be preserved because the cap is too small or
  the write fails, the exporter records a `dead_letter_failed` gap and does not
  advance the log cursor rather than silently dropping the original.
- **Metric rejection.** Metric batches use OCI `ATOMIC` validation, and the
  exporter inspects `failed_metrics_count` in every response. If OCI rejects a
  point, the metric cursor and counter baselines do not advance.
- **Metrics freshness.** OCI Monitoring accepts datapoints less than two hours
  old. The default cutoff is 6,900 seconds; older summaries are recorded as
  `freshness_skip` gaps and the metrics cursor advances without posting. Stale
  crawler summaries still advance the counter baseline. No API call is made when a
  scanned batch has no summary events.

## Resource expectations

On the 1 GiB E2.1.Micro host, the Rust additions are in-memory atomics and one
bounded metrics event per minute; there is no extra MySQL query on the snapshot
path. The exporter is a short-lived Python process once per minute. It reads at
most ten bounded journal batches per stream and keeps only the current batch plus
a small metric payload. The OCI SDK import dominates startup memory; the systemd
unit caps it at 192 MiB. No continuous Prometheus collector or managed scraper is
required.

## References

- [OCI Logging ingestion Python API](https://docs.oracle.com/en-us/iaas/tools/python/latest/api/loggingingestion.html)
- [OCI `put_logs` example](https://docs.oracle.com/en-us/iaas/tools/python-sdk-examples/latest/loggingingestion/put_logs.py.html)
- [OCI `post_metric_data` example](https://docs.oracle.com/en-us/iaas/tools/python-sdk-examples/latest/monitoring/post_metric_data.py.html)
- [journalctl manual](https://www.freedesktop.org/software/systemd/man/journalctl.html)
