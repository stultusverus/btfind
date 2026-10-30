"""Map btfind summary events to bounded OCI Monitoring metric streams."""

import copy
import os
from collections import namedtuple
from datetime import timezone

from logging_export import chunked

NAMESPACE = "btfind"

MetricPoint = namedtuple(
    "MetricPoint",
    ["timestamp", "name", "value", "unit", "semantics", "dimensions"],
)

SUMMARY_EVENTS = {
    "crawler_status",
    "web_status",
    "web_route_status",
    "web_latency_bucket",
}

CRAWLER_COUNTERS = (
    ("queries_sent_total", "CrawlerQueriesSent"),
    ("rpc_responses_total", "CrawlerRpcResponses"),
    ("rpc_timeouts_total", "CrawlerRpcTimeouts"),
    ("rpc_malformed_total", "CrawlerRpcMalformed"),
    ("rpc_unsolicited_total", "CrawlerRpcUnsolicited"),
    ("rpc_unexpected_source_total", "CrawlerRpcUnexpectedSource"),
    ("hashes_discovered_total", "CrawlerHashesDiscovered"),
    ("sampling_responses_total", "CrawlerSamplingResponses"),
    ("metadata_fetched_total", "CrawlerMetadataFetched"),
    ("metadata_failed_total", "CrawlerMetadataFailures"),
    ("backpressure_events_total", "CrawlerBackpressureEvents"),
    ("persistence_errors_total", "CrawlerPersistenceErrors"),
)

CRAWLER_GAUGES = (
    ("known_nodes", "CrawlerKnownNodes"),
    ("active_metadata_jobs", "CrawlerActiveMetadataJobs"),
    ("queued_metadata_jobs", "CrawlerQueuedMetadataJobs"),
    ("pending_metadata_peers", "CrawlerPendingMetadataPeers"),
)

STATUS_CLASSES = (
    ("requests_2xx", "2xx"),
    ("requests_3xx", "3xx"),
    ("requests_4xx", "4xx"),
    ("requests_5xx", "5xx"),
)


def max_points_per_request():
    return int(os.environ.get("METRICS_BATCH_MAX_POINTS", "40"))


def _fields(content):
    fields = content.get("fields")
    return fields if isinstance(fields, dict) else content


def field(content, name, default=None):
    fields = _fields(content)
    if name in fields:
        return fields[name]
    return content.get(name, default)


def _int(content, name):
    try:
        return int(field(content, name, 0))
    except (TypeError, ValueError):
        return 0


def _string(content, name):
    value = field(content, name, "")
    return "" if value is None else str(value)


def _dimensions(instance, service, **extra):
    dimensions = {"instance": instance, "service": service}
    dimensions.update({key: str(value) for key, value in extra.items()})
    return dimensions


def _event(content):
    return _string(content, "event")


def _run_id(content):
    return _string(content, "run_id") or "unknown"


def _timestamp_fields(content):
    return {
        "window_start_unix": _int(content, "window_start_unix"),
        "window_end_unix": _int(content, "window_end_unix"),
    }


def map_crawler(content, instance, timestamp, baselines):
    points = []
    run_id = _run_id(content)
    service = "crawler"
    dimensions = _dimensions(instance, service)

    for field_name, metric_name in CRAWLER_GAUGES:
        points.append(
            MetricPoint(
                timestamp,
                metric_name,
                _int(content, field_name),
                "count",
                "gauge",
                dimensions,
            )
        )

    previous = baselines.get("crawler") or {}
    previous_run_id = previous.get("run_id")
    counters = dict(previous.get("counters") or {})

    for field_name, metric_name in CRAWLER_COUNTERS:
        value = _int(content, field_name)
        if previous_run_id == run_id:
            before = int(counters.get(field_name, value))
            delta = value - before
            if delta < 0:
                delta = value
        else:
            delta = 0
        counters[field_name] = value
        if delta > 0:
            points.append(
                MetricPoint(
                    timestamp,
                    metric_name,
                    delta,
                    "count",
                    "interval_delta",
                    dimensions,
                )
            )

    baselines["crawler"] = {"run_id": run_id, "counters": counters}
    return points


def map_web_status(content, instance, timestamp):
    dimensions = _dimensions(instance, "web")
    points = [
        MetricPoint(
            timestamp,
            "WebRequestsTotal",
            _int(content, "requests_total"),
            "count",
            "window_count",
            dimensions,
        )
    ]

    for field_name, status_class in STATUS_CLASSES:
        points.append(
            MetricPoint(
                timestamp,
                "WebRequests",
                _int(content, field_name),
                "count",
                "window_count",
                _dimensions(instance, "web", status_class=status_class),
            )
        )

    points.extend(
        [
            MetricPoint(
                timestamp,
                "WebRequestDurationCount",
                _int(content, "duration_count"),
                "count",
                "window_count",
                _dimensions(instance, "web"),
            ),
            MetricPoint(
                timestamp,
                "WebRequestDurationSum",
                _int(content, "duration_sum_ms"),
                "milliseconds",
                "window_sum",
                _dimensions(instance, "web"),
            ),
            MetricPoint(
                timestamp,
                "WebActiveRequests",
                _int(content, "active_requests"),
                "count",
                "gauge",
                _dimensions(instance, "web"),
            ),
            MetricPoint(
                timestamp,
                "WebDatabaseErrors",
                _int(content, "database_errors"),
                "count",
                "window_count",
                _dimensions(instance, "web"),
            ),
        ]
    )
    return points


def map_web_route(content, instance, timestamp):
    route = _string(content, "route") or "unmatched"
    points = []
    for field_name, status_class in STATUS_CLASSES:
        points.append(
            MetricPoint(
                timestamp,
                "WebRouteRequests",
                _int(content, field_name),
                "count",
                "window_count",
                _dimensions(instance, "web", route=route, status_class=status_class),
            )
        )
    points.extend(
        [
            MetricPoint(
                timestamp,
                "WebRouteRequestDurationCount",
                _int(content, "duration_count"),
                "count",
                "window_count",
                _dimensions(instance, "web", route=route),
            ),
            MetricPoint(
                timestamp,
                "WebRouteRequestDurationSum",
                _int(content, "duration_sum_ms"),
                "milliseconds",
                "window_sum",
                _dimensions(instance, "web", route=route),
            ),
        ]
    )
    return points


def map_web_latency_bucket(content, instance, timestamp):
    le = _string(content, "le") or "+Inf"
    return [
        MetricPoint(
            timestamp,
            "WebRequestDurationBucket",
            _int(content, "count"),
            "count",
            "window_count",
            _dimensions(instance, "web", le=le),
        )
    ]


def map_batch(records, instance, baselines, max_age_seconds, now):
    working = copy.deepcopy(baselines)
    points = []
    skipped = []

    for record in records:
        content = record.content
        if not isinstance(content, dict):
            continue
        event = _event(content)
        if event not in SUMMARY_EVENTS:
            continue

        age_seconds = (now - record.timestamp.astimezone(timezone.utc)).total_seconds()
        stale = age_seconds > max_age_seconds

        if event == "crawler_status":
            # Even a stale summary advances the counter baseline, so a later
            # fresh event cannot republish counters from the skipped interval.
            mapped = map_crawler(content, instance, record.timestamp, working)
        elif stale:
            mapped = []
        elif event == "web_status":
            mapped = map_web_status(content, instance, record.timestamp)
        elif event == "web_route_status":
            mapped = map_web_route(content, instance, record.timestamp)
        else:
            mapped = map_web_latency_bucket(content, instance, record.timestamp)

        if stale:
            skipped.append(
                {
                    "event": event,
                    "timestamp": record.timestamp.isoformat(),
                    "age_seconds": int(age_seconds),
                    **_timestamp_fields(content),
                }
            )
            continue

        points.extend(mapped)

    return points, working, skipped


def publish_metrics(client, points, compartment_id):
    import oci

    responses = []
    for chunk in chunked(points, max_points_per_request()):
        grouped = {}
        for point in chunk:
            key = (point.name, tuple(sorted(point.dimensions.items())))
            grouped.setdefault(key, []).append(point)

        metric_data = [
            oci.monitoring.models.MetricDataDetails(
                namespace=NAMESPACE,
                compartment_id=compartment_id,
                name=group[0].name,
                dimensions=group[0].dimensions,
                datapoints=[
                    oci.monitoring.models.Datapoint(
                        timestamp=point.timestamp,
                        value=float(point.value),
                    )
                    for point in group
                ],
                metadata={
                    "unit": group[0].unit,
                    "semantics": group[0].semantics,
                },
            )
            for group in grouped.values()
        ]
        details = oci.monitoring.models.PostMetricDataDetails(
            metric_data=metric_data,
            batch_atomicity="ATOMIC",
        )
        responses.append(client.post_metric_data(post_metric_data_details=details))
    return responses
