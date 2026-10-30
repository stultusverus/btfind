#!/usr/bin/env python3
"""One-shot btfind journal -> OCI Logging and Monitoring exporter.

Journal logs and metric summaries use independent durable cursors.  The default
invocation is a parent that runs each stream in a separately terminable child
process with its own hard deadline.  The script exits after one bounded pass;
systemd schedules the next pass with a timer.
"""

import os
import random
import subprocess
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone

import checkpoint
import journal
import logging_export
import metrics_export
from diagnostics import emit


STREAMS = ("logs", "metrics")
DEFAULT_HARD_TIMEOUT_SECONDS = 35


class PublishError(Exception):
    pass


class Budget:
    def __init__(self, seconds):
        self.deadline = time.monotonic() + max(0, seconds)

    def remaining(self):
        return max(0.0, self.deadline - time.monotonic())

    def expired(self):
        return self.remaining() <= 0


@dataclass
class Config:
    region: str
    instance: str
    crawler_log_ocid: str
    web_log_ocid: str
    metrics_compartment_ocid: str
    state_dir: str
    journal_lookback_seconds: int = 300
    journal_max_batches: int = 10
    journal_max_records: int = 200
    journal_max_bytes: int = 262144
    metrics_max_age_seconds: int = 6900
    max_attempts: int = 5
    retry_base_seconds: float = 1.0
    retry_max_seconds: float = 30.0
    api_timeout_seconds: int = 8
    log_budget_seconds: int = 25
    metrics_budget_seconds: int = 25
    log_ocids: dict = field(default_factory=dict)


def env_int(name, default):
    value = os.environ.get(name)
    if value is None or value == "":
        return default
    return int(value)


def env_float(name, default):
    value = os.environ.get(name)
    if value is None or value == "":
        return default
    return float(value)


def load_config():
    required = {
        "BTFIND_CRAWLER_LOG_OCID": "crawler log OCID",
        "BTFIND_WEB_LOG_OCID": "web log OCID",
        "OCI_METRICS_COMPARTMENT_OCID": "metric compartment OCID",
    }
    missing = [description for name, description in required.items() if not os.environ.get(name)]
    if missing:
        raise ValueError("missing required configuration: " + ", ".join(missing))

    config = Config(
        region=os.environ.get("OCI_REGION", "ap-osaka-1"),
        instance=os.environ.get("INSTANCE_NAME", "vm-sub01"),
        crawler_log_ocid=os.environ["BTFIND_CRAWLER_LOG_OCID"],
        web_log_ocid=os.environ["BTFIND_WEB_LOG_OCID"],
        metrics_compartment_ocid=os.environ["OCI_METRICS_COMPARTMENT_OCID"],
        state_dir=os.environ.get("STATE_DIR", "/var/lib/btfind-observability"),
        journal_lookback_seconds=env_int("JOURNAL_LOOKBACK_SECONDS", 300),
        journal_max_batches=env_int("JOURNAL_MAX_BATCHES", 10),
        journal_max_records=env_int("JOURNAL_MAX_RECORDS", 200),
        journal_max_bytes=env_int("JOURNAL_MAX_BYTES", 262144),
        metrics_max_age_seconds=env_int("METRICS_MAX_AGE_SECONDS", 6900),
        max_attempts=env_int("OCI_MAX_ATTEMPTS", 5),
        retry_base_seconds=env_float("OCI_RETRY_BASE_SECONDS", 1.0),
        retry_max_seconds=env_float("OCI_RETRY_MAX_SECONDS", 30.0),
        api_timeout_seconds=env_int("OCI_API_TIMEOUT_SECONDS", 8),
        log_budget_seconds=env_int("LOG_EXPORT_BUDGET_SECONDS", 25),
        metrics_budget_seconds=env_int("METRICS_EXPORT_BUDGET_SECONDS", 25),
    )
    config.log_ocids = {
        "btfind-crawler": config.crawler_log_ocid,
        "btfind-web": config.web_log_ocid,
    }
    return config


def with_retry(label, operation, config, budget):
    import oci

    delay = config.retry_base_seconds
    last_error = None
    for attempt in range(1, config.max_attempts + 1):
        if budget.expired():
            raise PublishError(
                f"{label}: time budget exhausted before attempt {attempt}: {last_error}"
            )
        try:
            return operation()
        except oci.exceptions.ServiceError as error:
            status = getattr(error, "status", None)
            transient = status in (408, 429, 500, 502, 503, 504)
            last_error = error
            if not transient:
                raise PublishError(
                    f"{label}: permanent OCI error status={status} "
                    f"code={getattr(error, 'code', 'unknown')}"
                ) from error
        except (oci.exceptions.RequestException, TimeoutError, ConnectionError, OSError) as error:
            last_error = error
        if attempt == config.max_attempts:
            break
        sleep_for = delay + random.uniform(0, delay / 4)
        if sleep_for >= budget.remaining():
            raise PublishError(
                f"{label}: time budget exhausted after attempt {attempt}: {last_error}"
            )
        emit(
            "publish_retry",
            level="warning",
            destination=label,
            attempt=attempt,
            error=str(last_error),
        )
        time.sleep(sleep_for)
        delay = min(delay * 2, config.retry_max_seconds)

    raise PublishError(f"{label}: retries exhausted: {last_error}")


def make_clients(config):
    import oci

    signer = oci.auth.signers.InstancePrincipalsSecurityTokenSigner()
    client_config = {"region": config.region}
    api_timeout = max(
        1,
        min(
            config.api_timeout_seconds,
            config.log_budget_seconds // 3,
            config.metrics_budget_seconds // 3,
        ),
    )
    timeout = (api_timeout, api_timeout)
    monitoring_endpoint = os.environ.get(
        "OCI_MONITORING_ENDPOINT",
        f"https://telemetry-ingestion.{config.region}.oraclecloud.com",
    )
    logging_client = oci.loggingingestion.LoggingClient(
        client_config,
        signer=signer,
        timeout=timeout,
    )
    monitoring_client = oci.monitoring.MonitoringClient(
        client_config,
        signer=signer,
        service_endpoint=monitoring_endpoint,
        timeout=timeout,
    )
    return logging_client, monitoring_client


def ship_log_batch(client, batch, config, budget):
    grouped = logging_export.entries_by_service(batch.records)
    for service, entries in grouped.items():
        log_ocid = config.log_ocids.get(service)
        if not log_ocid:
            raise PublishError(f"no OCI log configured for {service}")
        for chunk in logging_export.chunked(entries, logging_export.max_entries()):
            if budget.expired():
                raise PublishError("logging: time budget exhausted")
            with_retry(
                f"logging:{service}",
                lambda log_ocid=log_ocid, service=service, chunk=chunk: logging_export.put_logs(
                    client,
                    log_ocid,
                    service,
                    chunk,
                    config.instance,
                ),
                config,
                budget,
            )


def ship_log_stream(store, client, config, cursor, lookback_seconds, budget):
    for batch in journal.read_batches(
        cursor,
        lookback_seconds,
        config.journal_max_batches,
        config.journal_max_records,
        config.journal_max_bytes,
    ):
        if budget.expired():
            raise PublishError("logging: time budget exhausted")
        ship_log_batch(client, batch, config, budget)
        store.advance("logs", batch.cursor)
        store.save()
        emit(
            "log_batch_committed",
            records=len(batch.records),
            cursor=batch.cursor,
        )


def run_logs(store, client, config):
    state = store.state["logs"]
    cursor = state.get("cursor")
    lookback = config.journal_lookback_seconds if not state.get("initialized") else None
    budget = Budget(config.log_budget_seconds)

    try:
        ship_log_stream(store, client, config, cursor, lookback, budget)
        return True
    except journal.JournalCursorError as error:
        if store.state["logs"].get("cursor") != cursor:
            raise PublishError(f"journal cursor failed after progress: {error}") from error
        store.record_gap("logs", "cursor_invalid", cursor=cursor, error=str(error))
        store.state["logs"]["cursor"] = None
        store.state["logs"]["initialized"] = False
        store.save()
        emit(
            "journal_cursor_recovered",
            level="warning",
            stream="logs",
            reason="cursor_invalid",
        )
        try:
            ship_log_stream(store, client, config, None, None, budget)
        except logging_export.DeadLetterError as dead_letter:
            store.record_gap(
                "logs",
                "dead_letter_failed",
                cursor=None,
                error=str(dead_letter),
            )
            store.save()
            raise PublishError(
                f"dead-letter storage failed: {dead_letter}"
            ) from dead_letter
        return True
    except logging_export.DeadLetterError as error:
        store.record_gap("logs", "dead_letter_failed", cursor=cursor, error=str(error))
        store.save()
        raise PublishError(f"dead-letter storage failed: {error}") from error
    except PublishError:
        raise
    except journal.JournalError as error:
        raise PublishError(f"journal read failed: {error}") from error


def ship_metrics_batch(store, client, batch, config, budget):
    points, baselines, skipped = metrics_export.map_batch(
        batch.records,
        config.instance,
        store.baselines(),
        config.metrics_max_age_seconds,
        datetime.now(timezone.utc),
    )
    for gap in skipped:
        store.record_gap("metrics", "freshness_skip", **gap)
    if points:
        for chunk in logging_export.chunked(
            points, metrics_export.max_points_per_request()
        ):
            if budget.expired():
                raise PublishError("metrics: time budget exhausted")
            responses = with_retry(
                "monitoring",
                lambda chunk=chunk: metrics_export.publish_metrics(
                    client,
                    chunk,
                    config.metrics_compartment_ocid,
                ),
                config,
                budget,
            )
            failed = sum(
                int(getattr(getattr(response, "data", None), "failed_metrics_count", 0) or 0)
                for response in responses or []
            )
            if failed:
                emit(
                    "metric_points_rejected",
                    level="error",
                    count=failed,
                    cursor=batch.cursor,
                )
                raise PublishError(
                    f"monitoring rejected {failed} metric point(s); checkpoint not advanced"
                )
    store.advance("metrics", batch.cursor, baselines=baselines)
    store.save()
    emit(
        "metric_batch_committed",
        points=len(points),
        skipped=len(skipped),
        cursor=batch.cursor,
    )


def ship_metric_stream(store, client, config, cursor, lookback_seconds, budget):
    for batch in journal.read_batches(
        cursor,
        lookback_seconds,
        config.journal_max_batches,
        config.journal_max_records,
        config.journal_max_bytes,
    ):
        if budget.expired():
            raise PublishError("metrics: time budget exhausted")
        ship_metrics_batch(store, client, batch, config, budget)


def run_metrics(store, client, config):
    state = store.state["metrics"]
    cursor = state.get("cursor")
    lookback = config.journal_lookback_seconds if not state.get("initialized") else None
    budget = Budget(config.metrics_budget_seconds)

    try:
        ship_metric_stream(store, client, config, cursor, lookback, budget)
        return True
    except journal.JournalCursorError as error:
        if store.state["metrics"].get("cursor") != cursor:
            raise PublishError(f"journal cursor failed after progress: {error}") from error
        store.record_gap("metrics", "cursor_invalid", cursor=cursor, error=str(error))
        store.state["metrics"]["cursor"] = None
        store.state["metrics"]["initialized"] = False
        store.save()
        emit(
            "journal_cursor_recovered",
            level="warning",
            stream="metrics",
            reason="cursor_invalid",
        )
        ship_metric_stream(store, client, config, None, None, budget)
        return True
    except PublishError:
        raise
    except journal.JournalError as error:
        raise PublishError(f"journal read failed: {error}") from error


def run_stream(stream):
    try:
        config = load_config()
    except (ValueError, TypeError) as error:
        emit(
            "configuration_invalid",
            level="error",
            stream=stream,
            error=str(error),
        )
        return 2

    try:
        store = checkpoint.CheckpointStore(config.state_dir)
        with store:
            try:
                logging_client, monitoring_client = make_clients(config)
            except Exception as error:
                emit(
                    "instance_principal_failed",
                    level="error",
                    stream=stream,
                    error=str(error),
                )
                return 3

            try:
                if stream == "logs":
                    run_logs(store, logging_client, config)
                else:
                    run_metrics(store, monitoring_client, config)
            except PublishError as error:
                emit(
                    f"{stream}_export_failed",
                    level="error",
                    stream=stream,
                    error=str(error),
                )
                return 1

            emit("export_stream_completed", stream=stream)
            return 0
    except RuntimeError as error:
        emit("exporter_locked", level="warning", stream=stream, error=str(error))
        return 75


def run_parent():
    failed = []
    hard_timeout_env = {
        "logs": "LOG_EXPORT_HARD_TIMEOUT_SECONDS",
        "metrics": "METRICS_EXPORT_HARD_TIMEOUT_SECONDS",
    }
    for stream in STREAMS:
        timeout = env_int(
            hard_timeout_env[stream],
            DEFAULT_HARD_TIMEOUT_SECONDS,
        )
        emit("export_stream_started", stream=stream, hard_timeout_seconds=timeout)
        try:
            completed = subprocess.run(
                [
                    sys.executable,
                    os.path.abspath(__file__),
                    "--stream",
                    stream,
                ],
                timeout=timeout,
                check=False,
            )
        except subprocess.TimeoutExpired:
            emit(
                "export_stream_timeout",
                level="error",
                stream=stream,
                hard_timeout_seconds=timeout,
            )
            failed.append(stream)
            continue

        if completed.returncode != 0:
            emit(
                "export_stream_failed",
                level="error",
                stream=stream,
                exit_code=completed.returncode,
            )
            failed.append(stream)

    emit("export_completed", failed_streams=failed)
    return 1 if failed else 0


def main(argv=None):
    arguments = sys.argv[1:] if argv is None else argv
    if not arguments:
        return run_parent()
    if len(arguments) == 2 and arguments[0] == "--stream" and arguments[1] in STREAMS:
        return run_stream(arguments[1])
    emit(
        "configuration_invalid",
        level="error",
        error="usage: export.py [--stream logs|metrics]",
    )
    return 2


if __name__ == "__main__":
    sys.exit(main())
