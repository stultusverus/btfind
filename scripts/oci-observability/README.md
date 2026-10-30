# btfind OCI observability exporter

`export.py` is a one-shot exporter. It reads the **system** journal for the two
btfind user units, ships application JSON records to OCI Logging, and converts
the periodic `crawler_status` / `web_status` records into OCI Monitoring metric
datapoints. It exits when the bounded pass completes; `btfind-observability.timer`
starts the next pass.

The exporter is deliberately separate from the crawler and web processes. If OCI
is unavailable, journald keeps buffering locally and the Rust services continue
running. There is no resident agent, proxy, or Prometheus listener.

## Files

| File | Responsibility |
| --- | --- |
| `export.py` | configuration, OCI clients, retries, independent stream orchestration |
| `journal.py` | bounded forward-only `journalctl -o json` reader and record normalisation |
| `logging_export.py` | OCI Logging batches, stable UUIDs, oversized-record dead letters |
| `metrics_export.py` | summary-event to OCI Monitoring mapping and counter baselines |
| `checkpoint.py` | versioned state, atomic checkpoints, exclusive lock |
| `diagnostics.py` | one-line JSON diagnostics on the exporter's own journal identifier |

## Installation

Install into a dedicated, pinned virtual environment. Do not install into the
system Python and do not share the crawler/web runtime.

```bash
sudo install -d -m 0755 /opt/btfind-observability
sudo cp -a scripts/oci-observability/. /opt/btfind-observability/
sudo python3 -m venv /opt/btfind-observability/venv
sudo /opt/btfind-observability/venv/bin/pip install \
  --requirement /opt/btfind-observability/requirements.txt
sudo install -d -m 0700 /var/lib/btfind-observability
sudo install -d -m 0755 /etc/btfind
sudo install -m 0600 /dev/null /etc/btfind/observability.env
```

Copy `deploy/observability.env.example` to `/etc/btfind/observability.env`,
replace the placeholder OCIDs, and keep permissions at `0600`. The process uses
instance principals, so no API signing key or private key belongs on the host.

Example invocation:

```bash
sudo -u root /opt/btfind-observability/venv/bin/python \
  /opt/btfind-observability/export.py
```

## Configuration

| Variable | Default | Meaning |
| --- | --- | --- |
| `OCI_REGION` | `ap-osaka-1` | OCI Logging/Monitoring region |
| `INSTANCE_NAME` | `vm-sub01` | Log `source` and Monitoring `instance` dimension |
| `BTFIND_CRAWLER_LOG_OCID` | required | Custom log for `btfind-crawler` records |
| `BTFIND_WEB_LOG_OCID` | required | Custom log for `btfind-web` records |
| `OCI_METRICS_COMPARTMENT_OCID` | required | Compartment for the `btfind` metric namespace |
| `BTFIND_JOURNAL_UID` | `opc` UID, else `1000` | UID filter for journal records |
| `BTFIND_INSTANCE_USER` | `opc` | User name used when `BTFIND_JOURNAL_UID` is unset |
| `JOURNAL_LOOKBACK_SECONDS` | `300` | First-run lookback only, never a full backfill |
| `JOURNAL_MAX_RECORDS` | `200` | Records per batch |
| `JOURNAL_MAX_BYTES` | `262144` | Encoded journal-line budget per batch |
| `JOURNAL_MAX_BATCHES` | `10` | Batches per invocation and per stream |
| `LOG_ENTRY_MAX_CHARS` | `10000` | OCI `LogEntry.data` character limit |
| `LOG_BATCH_MAX_ENTRIES` | `200` | Entries per `put_logs` call |
| `METRICS_MAX_AGE_SECONDS` | `6900` | OCI Monitoring freshness cutoff (OCI accepts less than two hours old) |
| `OCI_MONITORING_ENDPOINT` | derived | Override the `telemetry-ingestion.<region>` endpoint |
| `METRICS_BATCH_MAX_POINTS` | `40` | Metric datapoints per `post_metric_data` call (OCI allows 50 unique streams) |
| `OCI_MAX_ATTEMPTS` | `5` | Transient retry attempts per operation |
| `OCI_API_TIMEOUT_SECONDS` | `8` | Per-request connect/read timeout; clamped to one third of each stream budget |
| `LOG_EXPORT_BUDGET_SECONDS` | `25` | Independent log-stream execution budget |
| `METRICS_EXPORT_BUDGET_SECONDS` | `25` | Independent metric-stream execution budget |
| `STATE_DIR` | `/var/lib/btfind-observability` | Checkpoints, lock, and dead letters |
| `DEAD_LETTER_PATH` | `$STATE_DIR/dead-letter.jsonl` | Oversized record diagnostics |
| `DEAD_LETTER_MAX_BYTES` | `8388608` | Per-file cap; rotated to `.1` before appending |

The source journal identifiers are `btfind-crawler` and `btfind-web`. The
exporter itself uses `btfind-observability`, which is intentionally excluded from
the journal filters to avoid recursive ingestion.

## Journal selection and backlog

`journal.py` runs `journalctl` **without** `--system` or `--user`, filters
`_UID` and `_SYSTEMD_USER_UNIT=btfind.service|btfind-web.service`, and
corroborates `SYSLOG_IDENTIFIER` before selecting a destination. Omitting
`--system` is required: with persistent journald and `SplitMode=uid`, regular
user records live in per-user journal files that `--system` excludes. It reads
forward from
`--after-cursor` in journal order. A batch is bounded by record count **and**
encoded line size; up to ten batches are read per invocation. It does not use
`journalctl -n`, whose latest-record semantics would skip backlog.

With no checkpoint, the first run starts at a documented five-minute lookback.
Missing state is not treated as a full historical backfill. If journald has
vacuumed the saved cursor, `journalctl` fails to seek; the exporter records a
`cursor_invalid` gap, resumes at the earliest retained eligible record, and
continues. The journal can rotate independently of the exporter, so finite loss
is possible whenever the backlog exceeds journald retention.

`journalctl --cursor-file` is not used: it commits progress before OCI confirms
ingestion. Only `__CURSOR` values returned by this reader are stored.

## OCI Logging

Records are partitioned by service and sent with `LoggingClient.put_logs` to the
corresponding log OCID. Each `LogEntry` contains the original application JSON,
its UTC journal timestamp, and a UUIDv5 derived from
`_MACHINE_ID`/`_BOOT_ID`/`__CURSOR`. The UUID is a stable identifier, not an OCID,
and helps local analysis deduplicate retries; OCI does not guarantee server-side
deduplication.

Application JSON is sent as-is. Non-JSON or non-object messages are wrapped as
`{"message": "..."}`. If the encoded payload exceeds 10,000 characters, the full
record is appended to the root-only dead-letter file and a small
`log_entry_oversized` diagnostic is sent in its place. This keeps one large
event from blocking the queue forever; it is not silent truncation.

Dead-letter storage is bounded to `DEAD_LETTER_MAX_BYTES` per file. When the
current file would exceed the cap, it is rotated to `dead-letter.jsonl.1`
(replacing the previous backup), so total on-disk use stays bounded at roughly
twice the cap. If the record cannot be preserved because the cap is too small
for one record or the filesystem write fails, the exporter records a
`dead_letter_failed` gap and does **not** advance the log cursor. It reports the
condition on its own journal identifier instead of silently replacing and
checkpointing the record.

Logs and metrics have independent cursors. A log batch may contain both service
partitions; the log cursor advances only after every non-empty partition is
acknowledged. A partial success can resend the acknowledged partition on the
next run. Delivery is **at-least-once**, not exactly-once.

Transient throttling, 5xx, network, timeout, and connection failures use bounded
exponential backoff. Authentication, permission, missing-resource, and other
permanent OCI errors are reported distinctly and do not advance the cursor.

## OCI Monitoring

`crawler_status` records become interval deltas for cumulative counters and
gauges for current values. Deltas are computed from a stored per-service
`run_id` baseline, so a restarted crawler starts a new baseline instead of
publishing its reset counters as continuity. Web status records already contain
fixed-window counts, sums, and latency buckets.

Metric namespace: `btfind`. Dimensions are bounded to `instance`, `service`,
`route` (server route templates only), `status_class`, and `le`. Metric metadata
records `unit` (`count` or `milliseconds`) and `semantics` (`gauge`,
`interval_delta`, `window_count`, or `window_sum`).

Metric batches use OCI's `ATOMIC` validation, and the exporter also inspects
`failed_metrics_count` in every response. A rejected point prevents metric
checkpoint and baseline advancement. Metrics have their own journal cursor and
retry policy, so log shipping can fail without blocking metrics and vice versa.
If a summary is older than
`METRICS_MAX_AGE_SECONDS`, the exporter records a `freshness_skip` gap and
advances the metrics cursor without posting it. OCI cannot accept arbitrarily old
datapoints. No API call is made when a scanned batch contains no summary events.

## Checkpoints and recovery

State is `/var/lib/btfind-observability/state.json`, mode `0600`, directory mode
`0700`. It contains a format version, separate `logs` and `metrics` cursors,
last-success timestamps, metric counter baselines, and the last ten gap
diagnostics per stream. Writes use a same-directory temporary file, `fsync`,
atomic rename, and directory `fsync`. A corrupt or unsupported state file is
renamed aside and a diagnostic is emitted. An exclusive `flock` prevents
overlapping exporter executions.

The systemd oneshot/timer lifecycle is the primary serialization mechanism; the
lock is a second guard. Exporter diagnostics go to stderr, which systemd records
under its own identifier rather than the two source identifiers.

## Execution budgets

Logs and metrics run sequentially but each stream gets a fresh independent
`Budget`. A logging outage that exhausts `LOG_EXPORT_BUDGET_SECONDS` cannot stop
the metrics pass, which starts with its own `METRICS_EXPORT_BUDGET_SECONDS`.
Retries check the remaining budget before every API attempt and before sleeping,
so a failing destination cannot consume the other destination's time.

The OCI per-request timeout is also clamped to one third of the smaller stream
budget, and metric points are posted in bounded chunks so the budget is checked
between API calls. With the defaults, the worst-case per-stream overrun is one
8-second request beyond its 25-second budget; both streams plus SDK startup stay
inside the example `TimeoutStartSec=90`. If you raise the budgets, keep their
sum plus startup and one in-flight request below the systemd timeout.

## Failure semantics

- `0`: both streams completed their bounded pass.
- `1`: one or both streams failed after retries; checkpoints advance only for
  acknowledged work.
- `2`: required configuration is missing or invalid.
- `3`: instance-principal authentication or client creation failed.
- `75`: another exporter invocation holds the state lock.

Journal storage exhaustion, journald vacuuming, or a long OCI outage can create
gaps. Gaps are local diagnostics, not synthetic cloud records. Restore OCI access
and rerun the timer; the saved cursor resumes the remaining retained backlog.
