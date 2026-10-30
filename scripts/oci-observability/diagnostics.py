"""One-line JSON diagnostics for the btfind OCI exporter.

The exporter runs under SyslogIdentifier=btfind-observability.  Its diagnostics
must never match the two source identifiers, otherwise journald ingestion would
feed the exporter its own output.
"""

import json
import sys
from datetime import datetime, timezone


def _timestamp():
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def emit(event, level="info", **fields):
    record = {"timestamp": _timestamp(), "event": event, "level": level}
    record.update(fields)
    print(
        json.dumps(record, separators=(",", ":"), default=str, sort_keys=True),
        file=sys.stderr,
        flush=True,
    )
