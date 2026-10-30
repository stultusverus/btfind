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
| `revision` | Optional build-time `BTFIND_GIT_REVISION` |
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
run so a restart is never mistaken for counter continuity.

| Field | Kind | Unit |
| --- | --- | --- |
| `known_nodes` | gauge | count |
| `queries_sent_total` | counter | count |
| `rpc_responses_total` | counter | count |
| `rpc_timeouts_total` | counter | count |
| `rpc_malformed_total` | counter | count |
| `rpc_unsolicited_total` | counter | count |
| `rpc_unexpected_source_total` | counter | count |
| `hashes_discovered_total` | counter | count |
| `sampling_responses_total` | counter | count |
| `metadata_fetched_total` | counter | count |
| `metadata_failed_total` | counter | count |
| `backpressure_events_total` | counter | count |
| `persistence_errors_total` | counter | count |
| `active_metadata_jobs` | gauge | count |
| `queued_metadata_jobs` | gauge | count |
| `pending_metadata_peers` | gauge | count |

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

OCI Monitoring dimensions are limited to `instance`, `service`, `route`,
`status_class`, and `le`. Metric metadata records `unit` and `semantics`
(`gauge`, `interval_delta`, `window_count`, or `window_sum`). The exporter
converts crawler cumulative counters to interval deltas using a stored
`run_id` baseline; a new run starts at a zero delta rather than republishing reset
counters.

### Volume controls

- Normal operation emits at most a few INFO status records per minute per
  service. A `web_status` plus per-route and non-empty bucket records are bounded
  by the fixed route and bucket sets.
- Individual DHT requests, peer addresses, malformed packets, and retry decisions
  are DEBUG. INFO does not contain full search queries, torrent names, file paths,
  database URLs, credentials, or high-cardinality peer identifiers.
- journald is capped at 64 MiB with 1 GiB kept free, 8 MiB files, and a three-day
  age cap. Actual retention is the shorter of age and available space.

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

Install the system-wide journald policy. Oracle Linux 9 stores user-service output
in the system journal, but `journalctl --user` uses the user manager's view and may
appear empty before lingering or when queried from the wrong context.

```bash
sudo install -d -m 0755 /etc/systemd/journald.conf.d
sudo install -m 0644 deploy/journald/30-btfind.conf \
  /etc/systemd/journald.conf.d/30-btfind.conf
sudo systemctl restart systemd-journald
sudo loginctl enable-linger opc
```

The configuration enables persistent storage and bounds total usage. The system
journal is the source for the exporter; use:

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
root only to read the system journal; it has no resident daemon, uses
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
  record from blocking the queue.
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
