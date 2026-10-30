"""OCI Logging payload construction."""

import json
import os
import uuid

from diagnostics import emit

DEFAULT_MAX_DATA_CHARS = 10000
DEFAULT_MAX_ENTRIES = 200
DEFAULT_STATE_DIR = "/var/lib/btfind-observability"


def max_data_chars():
    return int(os.environ.get("LOG_ENTRY_MAX_CHARS", DEFAULT_MAX_DATA_CHARS))


def max_entries():
    return int(os.environ.get("LOG_BATCH_MAX_ENTRIES", DEFAULT_MAX_ENTRIES))


def dead_letter_path():
    configured = os.environ.get("DEAD_LETTER_PATH")
    if configured:
        return configured
    return os.path.join(os.environ.get("STATE_DIR", DEFAULT_STATE_DIR), "dead-letter.jsonl")


def chunked(values, size):
    for index in range(0, len(values), size):
        yield values[index : index + size]


def stable_entry_id(record):
    identity = f"{record.machine_id}:{record.boot_id}:{record.cursor}"
    return str(uuid.uuid5(uuid.NAMESPACE_URL, f"btfind:{identity}"))


def write_dead_letter(record, data_chars):
    path = dead_letter_path()
    try:
        os.makedirs(os.path.dirname(path), mode=0o700, exist_ok=True)
        line = json.dumps(
            {
                "cursor": record.cursor,
                "timestamp": record.timestamp.isoformat(),
                "service": record.service,
                "data_chars": data_chars,
                "content": record.content,
            },
            separators=(",", ":"),
            default=str,
        )
        descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
        with os.fdopen(descriptor, "a", encoding="utf-8") as handle:
            handle.write(line + "\n")
        emit(
            "log_entry_oversized",
            level="warning",
            reason="data_limit",
            data_chars=data_chars,
            max_chars=max_data_chars(),
        )
    except OSError as error:
        emit(
            "dead_letter_write_failed",
            level="error",
            reason="filesystem",
            error=str(error),
        )


def serialise(record):
    data = json.dumps(record.content, separators=(",", ":"), ensure_ascii=False)
    limit = max_data_chars()
    if len(data) <= limit:
        return data

    write_dead_letter(record, len(data))
    diagnostic = {
        "event": "log_entry_oversized",
        "reason": "data_limit",
        "original_chars": len(data),
        "max_chars": limit,
        "cursor_id": stable_entry_id(record),
    }
    return json.dumps(diagnostic, separators=(",", ":"))


def millisecond_timestamp(value):
    return value.replace(microsecond=(value.microsecond // 1000) * 1000)


def entries_by_service(records):
    grouped = {}
    for record in records:
        grouped.setdefault(record.service, []).append(
            {
                "data": serialise(record),
                "id": stable_entry_id(record),
                "time": millisecond_timestamp(record.timestamp),
            }
        )
    return grouped


def put_logs(client, log_ocid, service, entries, instance):
    import oci

    response = None
    for chunk in chunked(entries, max_entries()):
        log_entries = [
            oci.loggingingestion.models.LogEntry(
                data=entry["data"],
                id=entry["id"],
                time=entry["time"],
            )
            for entry in chunk
        ]
        batch = oci.loggingingestion.models.LogEntryBatch(
            entries=log_entries,
            source=instance,
            type="btfind.application",
            subject=service,
            defaultlogentrytime=chunk[0]["time"],
        )
        details = oci.loggingingestion.models.PutLogsDetails(
            specversion="1.0",
            log_entry_batches=[batch],
        )
        response = client.put_logs(log_id=log_ocid, put_logs_details=details)
    return response
