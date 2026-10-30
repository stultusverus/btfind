use crate::types::CrawlStatsEvent;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::time::{interval, MissedTickBehavior};

pub const SNAPSHOT_INTERVAL_SECONDS: u64 = 60;

pub const ROUTES: &[&str] = &[
    "/",
    "/dashboard.css",
    "/dashboard.js",
    "/api/stats",
    "/api/search",
    "/api/torrents/{info_hash}",
    "/api/torrents/{info_hash}/files",
    "/api/torrents/{info_hash}/magnet",
    "unmatched",
];

pub const LATENCY_BUCKETS_MS: &[u64] = &[5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000];

fn unix_now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

pub struct CrawlerMetrics {
    process_started_unix: i64,
    known_nodes: AtomicU64,
    queries_sent: AtomicU64,
    rpc_responses: AtomicU64,
    rpc_timeouts: AtomicU64,
    rpc_malformed: AtomicU64,
    rpc_unsolicited: AtomicU64,
    rpc_unexpected_source: AtomicU64,
    hashes_discovered: AtomicU64,
    sampling_responses: AtomicU64,
    metadata_fetched: AtomicU64,
    metadata_failed: AtomicU64,
    backpressure_events: AtomicU64,
    persistence_errors: AtomicU64,
    active_metadata_jobs: AtomicU64,
    queued_metadata_jobs: AtomicU64,
    pending_metadata_peers: AtomicU64,
}

impl Default for CrawlerMetrics {
    fn default() -> Self {
        Self {
            process_started_unix: unix_now(),
            known_nodes: AtomicU64::new(0),
            queries_sent: AtomicU64::new(0),
            rpc_responses: AtomicU64::new(0),
            rpc_timeouts: AtomicU64::new(0),
            rpc_malformed: AtomicU64::new(0),
            rpc_unsolicited: AtomicU64::new(0),
            rpc_unexpected_source: AtomicU64::new(0),
            hashes_discovered: AtomicU64::new(0),
            sampling_responses: AtomicU64::new(0),
            metadata_fetched: AtomicU64::new(0),
            metadata_failed: AtomicU64::new(0),
            backpressure_events: AtomicU64::new(0),
            persistence_errors: AtomicU64::new(0),
            active_metadata_jobs: AtomicU64::new(0),
            queued_metadata_jobs: AtomicU64::new(0),
            pending_metadata_peers: AtomicU64::new(0),
        }
    }
}

impl CrawlerMetrics {
    pub fn apply(&self, event: &CrawlStatsEvent) {
        match event {
            CrawlStatsEvent::DhtSnapshot {
                nodes_known,
                queries_sent,
                info_hashes_found,
            } => {
                self.known_nodes
                    .store(*nodes_known as u64, Ordering::Relaxed);
                self.queries_sent
                    .store(*queries_sent as u64, Ordering::Relaxed);
                self.hashes_discovered
                    .store(*info_hashes_found as u64, Ordering::Relaxed);
            }
            CrawlStatsEvent::MetadataFetched => {
                self.metadata_fetched.fetch_add(1, Ordering::Relaxed);
            }
            CrawlStatsEvent::MetadataFetchFailed { .. } => {
                self.metadata_failed.fetch_add(1, Ordering::Relaxed);
            }
            CrawlStatsEvent::RpcAnswered => {
                self.rpc_responses.fetch_add(1, Ordering::Relaxed);
            }
            CrawlStatsEvent::RpcTimedOut => {
                self.rpc_timeouts.fetch_add(1, Ordering::Relaxed);
            }
            CrawlStatsEvent::RpcMalformed => {
                self.rpc_malformed.fetch_add(1, Ordering::Relaxed);
            }
            CrawlStatsEvent::RpcUnsolicited => {
                self.rpc_unsolicited.fetch_add(1, Ordering::Relaxed);
            }
            CrawlStatsEvent::RpcUnexpectedSource => {
                self.rpc_unexpected_source.fetch_add(1, Ordering::Relaxed);
            }
            CrawlStatsEvent::DiscoveryBackpressure => {
                self.backpressure_events.fetch_add(1, Ordering::Relaxed);
            }
            CrawlStatsEvent::HashObserved { .. } => {
                self.hashes_discovered.fetch_add(1, Ordering::Relaxed);
            }
            CrawlStatsEvent::SamplingResponse { .. } => {
                self.sampling_responses.fetch_add(1, Ordering::Relaxed);
            }
            CrawlStatsEvent::MetadataQueue {
                active_jobs,
                queued_jobs,
                pending_peers,
            } => {
                self.active_metadata_jobs
                    .store(*active_jobs as u64, Ordering::Relaxed);
                self.queued_metadata_jobs
                    .store(*queued_jobs as u64, Ordering::Relaxed);
                self.pending_metadata_peers
                    .store(*pending_peers as u64, Ordering::Relaxed);
            }
            CrawlStatsEvent::DhtNodeSeen { .. } | CrawlStatsEvent::SampleObserved { .. } => {}
        }
    }

    pub fn record_persistence_error(&self) {
        self.persistence_errors.fetch_add(1, Ordering::Relaxed);
    }

    pub fn emit(&self, final_snapshot: bool) {
        tracing::info!(
            event = "crawler_status",
            final_snapshot = final_snapshot,
            process_started_unix = self.process_started_unix,
            uptime_seconds = unix_now().saturating_sub(self.process_started_unix),
            interval_seconds = SNAPSHOT_INTERVAL_SECONDS,
            known_nodes = self.known_nodes.load(Ordering::Relaxed),
            queries_sent_total = self.queries_sent.load(Ordering::Relaxed),
            rpc_responses_total = self.rpc_responses.load(Ordering::Relaxed),
            rpc_timeouts_total = self.rpc_timeouts.load(Ordering::Relaxed),
            rpc_malformed_total = self.rpc_malformed.load(Ordering::Relaxed),
            rpc_unsolicited_total = self.rpc_unsolicited.load(Ordering::Relaxed),
            rpc_unexpected_source_total = self.rpc_unexpected_source.load(Ordering::Relaxed),
            hashes_discovered_total = self.hashes_discovered.load(Ordering::Relaxed),
            sampling_responses_total = self.sampling_responses.load(Ordering::Relaxed),
            metadata_fetched_total = self.metadata_fetched.load(Ordering::Relaxed),
            metadata_failed_total = self.metadata_failed.load(Ordering::Relaxed),
            backpressure_events_total = self.backpressure_events.load(Ordering::Relaxed),
            persistence_errors_total = self.persistence_errors.load(Ordering::Relaxed),
            active_metadata_jobs = self.active_metadata_jobs.load(Ordering::Relaxed),
            queued_metadata_jobs = self.queued_metadata_jobs.load(Ordering::Relaxed),
            pending_metadata_peers = self.pending_metadata_peers.load(Ordering::Relaxed),
            "crawler status"
        );
    }
}

#[derive(Clone, Default)]
struct RouteWindow {
    status: [u64; 4],
    duration_count: u64,
    duration_sum_ms: u64,
    latency_buckets: [u64; LATENCY_BUCKETS_MS.len() + 1],
}

struct WebWindow {
    started_unix: i64,
    routes: BTreeMap<&'static str, RouteWindow>,
    database_errors: u64,
}

impl WebWindow {
    fn new(started_unix: i64) -> Self {
        Self {
            started_unix,
            routes: BTreeMap::new(),
            database_errors: 0,
        }
    }
}

pub struct WebMetrics {
    process_started_unix: i64,
    active_requests: AtomicU64,
    window: Mutex<WebWindow>,
}

impl Default for WebMetrics {
    fn default() -> Self {
        Self {
            process_started_unix: unix_now(),
            active_requests: AtomicU64::new(0),
            window: Mutex::new(WebWindow::new(unix_now())),
        }
    }
}

struct WebWindowSnapshot {
    started_unix: i64,
    ended_unix: i64,
    routes: BTreeMap<&'static str, RouteWindow>,
    database_errors: u64,
}

impl WebMetrics {
    pub fn begin_request(&self) {
        self.active_requests.fetch_add(1, Ordering::Relaxed);
    }

    pub fn end_request(&self) {
        let _ = self
            .active_requests
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |active| {
                Some(active.saturating_sub(1))
            });
    }

    pub fn record_request(&self, route: &str, status: u16, elapsed: Duration) {
        let route = route_label(Some(route));
        let class = status_class(status);
        let bucket = latency_bucket_index(elapsed.as_millis() as u64);
        let mut window = lock(&self.window);
        let route = window.routes.entry(route).or_default();
        route.status[class] = route.status[class].saturating_add(1);
        route.duration_count = route.duration_count.saturating_add(1);
        route.duration_sum_ms = route
            .duration_sum_ms
            .saturating_add(elapsed.as_millis().min(u128::from(u64::MAX)) as u64);
        route.latency_buckets[bucket] = route.latency_buckets[bucket].saturating_add(1);
    }

    pub fn record_database_error(&self) {
        let mut window = lock(&self.window);
        window.database_errors = window.database_errors.saturating_add(1);
    }

    fn snapshot_and_reset(&self) -> WebWindowSnapshot {
        let ended_unix = unix_now();
        let mut window = lock(&self.window);
        let previous = std::mem::replace(&mut *window, WebWindow::new(ended_unix));
        WebWindowSnapshot {
            started_unix: previous.started_unix,
            ended_unix,
            routes: previous.routes,
            database_errors: previous.database_errors,
        }
    }

    pub fn emit(&self, final_snapshot: bool) {
        let snapshot = self.snapshot_and_reset();
        let interval_seconds = snapshot.ended_unix.saturating_sub(snapshot.started_unix);
        let mut status = [0u64; 4];
        let mut duration_count = 0u64;
        let mut duration_sum_ms = 0u64;
        let mut latency_buckets = [0u64; LATENCY_BUCKETS_MS.len() + 1];

        for route in snapshot.routes.values() {
            for (index, value) in route.status.iter().enumerate() {
                status[index] = status[index].saturating_add(*value);
            }
            duration_count = duration_count.saturating_add(route.duration_count);
            duration_sum_ms = duration_sum_ms.saturating_add(route.duration_sum_ms);
            for (index, value) in route.latency_buckets.iter().enumerate() {
                latency_buckets[index] = latency_buckets[index].saturating_add(*value);
            }
        }

        tracing::info!(
            event = "web_status",
            final_snapshot = final_snapshot,
            process_started_unix = self.process_started_unix,
            uptime_seconds = unix_now().saturating_sub(self.process_started_unix),
            window_start_unix = snapshot.started_unix,
            window_end_unix = snapshot.ended_unix,
            interval_seconds = interval_seconds,
            requests_total = status.iter().sum::<u64>(),
            requests_2xx = status[0],
            requests_3xx = status[1],
            requests_4xx = status[2],
            requests_5xx = status[3],
            duration_count = duration_count,
            duration_sum_ms = duration_sum_ms,
            active_requests = self.active_requests.load(Ordering::Relaxed),
            database_errors = snapshot.database_errors,
            "web status"
        );

        for (route, values) in &snapshot.routes {
            if values.duration_count == 0 {
                continue;
            }
            tracing::info!(
                event = "web_route_status",
                final_snapshot = final_snapshot,
                window_start_unix = snapshot.started_unix,
                window_end_unix = snapshot.ended_unix,
                interval_seconds = interval_seconds,
                route = %route,
                requests_2xx = values.status[0],
                requests_3xx = values.status[1],
                requests_4xx = values.status[2],
                requests_5xx = values.status[3],
                duration_count = values.duration_count,
                duration_sum_ms = values.duration_sum_ms,
                "web route status"
            );
        }

        for (index, count) in latency_buckets.iter().enumerate() {
            if *count == 0 {
                continue;
            }
            let le = LATENCY_BUCKETS_MS
                .get(index)
                .map(u64::to_string)
                .unwrap_or_else(|| "+Inf".to_string());
            tracing::info!(
                event = "web_latency_bucket",
                final_snapshot = final_snapshot,
                window_start_unix = snapshot.started_unix,
                window_end_unix = snapshot.ended_unix,
                interval_seconds = interval_seconds,
                le = %le,
                count = *count,
                "web latency bucket"
            );
        }
    }
}

pub fn route_label(matched_path: Option<&str>) -> &'static str {
    let Some(path) = matched_path else {
        return "unmatched";
    };
    ROUTES
        .iter()
        .copied()
        .find(|route| *route == path)
        .unwrap_or("unmatched")
}

fn status_class(status: u16) -> usize {
    match status / 100 {
        2 => 0,
        3 => 1,
        4 => 2,
        _ => 3,
    }
}

fn latency_bucket_index(milliseconds: u64) -> usize {
    LATENCY_BUCKETS_MS
        .iter()
        .position(|bucket| milliseconds <= *bucket)
        .unwrap_or(LATENCY_BUCKETS_MS.len())
}

pub async fn run_crawler_reporter(metrics: Arc<CrawlerMetrics>) {
    let mut ticker = interval(Duration::from_secs(SNAPSHOT_INTERVAL_SECONDS));
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
    ticker.tick().await;
    loop {
        ticker.tick().await;
        metrics.emit(false);
    }
}

pub async fn run_web_reporter(metrics: Arc<WebMetrics>) {
    let mut ticker = interval(Duration::from_secs(SNAPSHOT_INTERVAL_SECONDS));
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
    ticker.tick().await;
    loop {
        ticker.tick().await;
        metrics.emit(false);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn route_templates_are_bounded() {
        assert_eq!(route_label(Some("/api/search")), "/api/search");
        assert_eq!(
            route_label(Some("/api/torrents/0123456789abcdef")),
            "unmatched"
        );
        assert_eq!(route_label(None), "unmatched");
    }

    #[test]
    fn web_window_records_status_and_latency() {
        let metrics = WebMetrics::default();
        metrics.record_request("/api/search", 200, Duration::from_millis(12));
        metrics.record_request("/api/search", 503, Duration::from_millis(3));
        metrics.record_database_error();
        let snapshot = metrics.snapshot_and_reset();
        let route = snapshot.routes.get("/api/search").unwrap();
        assert_eq!(route.status, [1, 0, 0, 1]);
        assert_eq!(route.duration_count, 2);
        assert_eq!(route.duration_sum_ms, 15);
        assert_eq!(route.latency_buckets[0], 1);
        assert_eq!(route.latency_buckets[2], 1);
        assert_eq!(snapshot.database_errors, 1);
    }

    #[test]
    fn crawler_metrics_track_counters_and_gauges() {
        let metrics = CrawlerMetrics::default();
        metrics.apply(&CrawlStatsEvent::DhtSnapshot {
            nodes_known: 12,
            queries_sent: 34,
            info_hashes_found: 5,
        });
        metrics.apply(&CrawlStatsEvent::MetadataFetched);
        metrics.apply(&CrawlStatsEvent::RpcTimedOut);
        metrics.apply(&CrawlStatsEvent::MetadataQueue {
            active_jobs: 2,
            queued_jobs: 3,
            pending_peers: 4,
        });
        metrics.record_persistence_error();
        assert_eq!(metrics.known_nodes.load(Ordering::Relaxed), 12);
        assert_eq!(metrics.queries_sent.load(Ordering::Relaxed), 34);
        assert_eq!(metrics.hashes_discovered.load(Ordering::Relaxed), 5);
        assert_eq!(metrics.metadata_fetched.load(Ordering::Relaxed), 1);
        assert_eq!(metrics.rpc_timeouts.load(Ordering::Relaxed), 1);
        assert_eq!(metrics.active_metadata_jobs.load(Ordering::Relaxed), 2);
        assert_eq!(metrics.queued_metadata_jobs.load(Ordering::Relaxed), 3);
        assert_eq!(metrics.pending_metadata_peers.load(Ordering::Relaxed), 4);
        assert_eq!(metrics.persistence_errors.load(Ordering::Relaxed), 1);
    }
}
