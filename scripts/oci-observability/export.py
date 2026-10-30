#!/usr/bin/env python3
"""One-shot btfind journal -> OCI Logging and Monitoring exporter.

Journal logs and metric summaries use independent durable cursors.  The script
exits after one bounded pass; systemd schedules the next pass with a timer.
"""

import os
import random
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone

import checkpoint
import journal
import logging_export
import metrics_export
from diagnostics import emit


class PublishError(Exception):
    pass


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
    metrics_max_age_seconds: int = 7200
    max_attempts: int = 5
    retry_base_seconds: float = 1.0
    retry_max_seconds: float = 30.0
    api_timeout_seconds: int = 20
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
        api_timeout_seconds=env_int("OCI_API_TIMEOUT_SECONDS", 20),
    )
    config.log_ocids = {
        "btfind-crawler": config.crawler_log_ocid,
        "btfind-web": config.web_log_ocid,
    }
    return config


def with_retry(label, operation, config):
    import oci

    delay = config.retry_base_seconds
    last_error = None
    for attempt in range(1, config.max_attempts + 1):
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
        emit(
            "publish_retry",
            level="warning",
            destination=label,
            attempt=attempt,
            error=str(last_error),
        )
        time.sleep(delay + random.uniform(0, delay / 4))
        delay = min(delay * 2, config.retry_max_seconds)

    raise PublishError(f"{label}: retries exhausted: {last_error}")


def make_clients(config):
    import oci

    signer = oci.auth.signers.InstancePrincipalsSecurityTokenSigner()
    client_config = {"region": config.region}
    timeout = (config.api_timeout_seconds, config.api_timeout_seconds)
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


def ship_log_batch(client, batch, config):
    grouped = logging_export.entries_by_service(batch.records)
    for service, entries in grouped.items():
        log_ocid = config.log_ocids.get(service)
        if not log_ocid:
            raise PublishError(f"no OCI log configured for {service}")
        with_retry(
            f"logging:{service}",
            lambda log_ocid=log_ocid, service=service, entries=entries: logging_export.put_logs(
                client,
                log_ocid,
                service,
                entries,
                config.instance,
            ),
            config,
        )


def ship_log_stream(store, client, config, cursor, lookback_seconds):
    for batch in journal.read_batches(
        cursor,
        lookback_seconds,
        config.journal_max_batches,
        config.journal_max_records,
        config.journal_max_bytes,
    ):
        ship_log_batch(client, batch, config)
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

    try:
        ship_log_stream(store, client, config, cursor, lookback)
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
            ship_log_stream(store, client, config, None, None)
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


def ship_metrics_batch(store, client, batch, config):
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
        responses = with_retry(
            "monitoring",
            lambda: metrics_export.publish_metrics(
                client,
                points,
                config.metrics_compartment_ocid,
            ),
            config,
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


def ship_metric_stream(store, client, config, cursor, lookback_seconds):
    for batch in journal.read_batches(
        cursor,
        lookback_seconds,
        config.journal_max_batches,
        config.journal_max_records,
        config.journal_max_bytes,
    ):
        ship_metrics_batch(store, client, batch, config)


def run_metrics(store, client, config):
    state = store.state["metrics"]
    cursor = state.get("cursor")
    lookback = config.journal_lookback_seconds if not state.get("initialized") else None

    try:
        ship_metric_stream(store, client, config, cursor, lookback)
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
        ship_metric_stream(store, client, config, None, None)
        return True
    except PublishError:
        raise
    except journal.JournalError as error:
        raise PublishError(f"journal read failed: {error}") from error


def main():
    try:
        config = load_config()
    except (ValueError, TypeError) as error:
        emit("configuration_invalid", level="error", error=str(error))
        return 2

    try:
        store = checkpoint.CheckpointStore(config.state_dir)
        with store:
            try:
                logging_client, monitoring_client = make_clients(config)
            except Exception as error:
                emit("instance_principal_failed", level="error", error=str(error))
                return 3

            logs_ok = False
            metrics_ok = False
            try:
                run_logs(store, logging_client, config)
                logs_ok = True
            except PublishError as error:
                emit("log_export_failed", level="error", error=str(error))

            try:
                run_metrics(store, monitoring_client, config)
                metrics_ok = True
            except PublishError as error:
                emit("metric_export_failed", level="error", error=str(error))

            emit(
                "export_completed",
                logs_ok=logs_ok,
                metrics_ok=metrics_ok,
            )
            return 0 if logs_ok and metrics_ok else 1
    except RuntimeError as error:
        emit("exporter_locked", level="warning", error=str(error))
        return 75


if __name__ == "__main__":
    sys.exit(main())
