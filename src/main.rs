mod bencode;
mod config;
mod dht;
mod magnet;
mod metadata;
mod routing;
mod store;
mod types;
mod web;
mod wire;

use clap::{Parser, Subcommand};
use std::net::SocketAddrV4;
use std::sync::Arc;
use tokio::net::UdpSocket;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::Duration;

#[derive(Parser)]
#[command(name = "btfind", version = "0.1.0", about = "BitTorrent DHT sniffer")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Start the crawler
    Run {
        /// Run duration in seconds (one-shot mode)
        #[arg(long)]
        duration: Option<u64>,

        /// Max simultaneous metadata fetches
        #[arg(long)]
        max_concurrent: Option<usize>,

        /// Bind port for DHT
        #[arg(long)]
        port: Option<u16>,
    },

    /// Search collected torrents
    Search {
        /// Text query to search torrent names
        #[arg(long)]
        query: Option<String>,

        /// Max results to return
        #[arg(long, default_value = "50")]
        limit: usize,

        /// Sort by: first_seen, last_seen, total_size, name, rank
        #[arg(long, default_value = "last_seen")]
        sort: String,

        /// Output as JSON
        #[arg(long)]
        json: bool,

        /// Include magnet URI in output
        #[arg(long)]
        magnet: bool,
    },

    /// Print a magnet URI for an info hash
    Magnet {
        /// 40-character hex info hash
        info_hash: String,

        /// Optional display name
        #[arg(long)]
        name: Option<String>,
    },

    /// Show crawler statistics
    Stats,

    /// Remove torrents not seen in N days
    Prune {
        /// Remove torrents older than this many days
        #[arg(long, default_value = "90")]
        older_than: u32,
    },

    /// Start local HTTP API and dashboard
    Serve {
        /// Bind host
        #[arg(long)]
        host: Option<String>,

        /// Bind port
        #[arg(long)]
        port: Option<u16>,
    },
}

fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "btfind=info".into()),
        )
        .init();

    let cli = Cli::parse();
    let config = config::Config::load();

    let rt = tokio::runtime::Runtime::new().expect("failed to create tokio runtime");
    rt.block_on(async {
        match cli.command {
            Commands::Run {
                duration,
                max_concurrent,
                port,
            } => cmd_run(&config, duration, max_concurrent, port).await,
            Commands::Search {
                query,
                limit,
                sort,
                json,
                magnet,
            } => cmd_search(&config, query, limit, &sort, json, magnet),
            Commands::Stats => cmd_stats(&config),
            Commands::Prune { older_than } => cmd_prune(&config, older_than),
            Commands::Magnet { info_hash, name } => cmd_magnet(&info_hash, name.as_deref()),
            Commands::Serve { host, port } => {
                let host = host.unwrap_or_else(|| config.web.host.clone());
                let port = port.unwrap_or(config.web.port);
                cmd_serve(&config, &host, port).await
            }
        }
    });
}

async fn cmd_run(
    config: &config::Config,
    duration: Option<u64>,
    max_concurrent: Option<usize>,
    port: Option<u16>,
) {
    let db_path = config.database_path();
    let store = Arc::new(open_store(&db_path));
    let resume_state = match load_resume_state(&store, config) {
        Ok(state) => state,
        Err(e) => {
            tracing::warn!("failed to load resume state: {}", e);
            ResumeState::default()
        }
    };

    let port = port.unwrap_or(config.network.port);
    let max_concurrent = max_concurrent.unwrap_or(config.metadata.max_concurrent);
    let max_concurrent = if max_concurrent == 0 {
        tracing::warn!("max_concurrent is 0, clamping to 1");
        1
    } else {
        max_concurrent
    };
    let addr: SocketAddrV4 = format!("0.0.0.0:{}", port)
        .parse()
        .expect("0.0.0.0:<port> should parse to a valid IPv4 address");
    let socket = Arc::new(
        UdpSocket::bind(addr)
            .await
            .expect("failed to bind UDP socket"),
    );

    tracing::info!("bound to {}", addr);

    let (info_hash_tx, info_hash_rx) =
        mpsc::unbounded_channel::<(types::InfoHash, Vec<types::PeerContact>)>();
    let (stats_tx, stats_rx) = mpsc::unbounded_channel::<types::CrawlStatsEvent>();

    let get_peers_interval = Duration::from_secs(config.crawl.get_peers_interval_secs);
    let bucket_refresh = Duration::from_secs(config.crawl.bucket_refresh_mins * 60);

    let bootstrap_nodes = config.network.bootstrap_nodes.clone();
    let mut crawler = dht::DhtCrawler::new(
        socket.clone(),
        info_hash_tx,
        stats_tx.clone(),
        bootstrap_nodes,
    );
    let resumed_hashes = crawler.seed_info_hashes(resume_state.info_hashes);
    let resumed_nodes = crawler.seed_nodes(resume_state.dht_nodes);
    if resumed_hashes > 0 || resumed_nodes > 0 {
        tracing::info!(
            "resumed {} incomplete hashes and {} DHT nodes",
            resumed_hashes,
            resumed_nodes
        );
    }
    let crawler_handle = tokio::spawn(async move {
        crawler.run(get_peers_interval, bucket_refresh).await;
    });

    let store_stats = store.clone();
    let stats_handle = tokio::spawn(async move {
        run_stats_persistence(store_stats, stats_rx).await;
    });

    let store_clone = store.clone();
    let peer_timeout = config.metadata.peer_timeout_secs;
    let retry_after_hours = config.metadata.retry_after_hours;
    let fetcher_handle = tokio::spawn(async move {
        metadata::run_metadata_fetcher(
            info_hash_rx,
            store_clone,
            max_concurrent,
            peer_timeout,
            retry_after_hours,
            stats_tx,
        )
        .await;
    });

    tracing::info!("crawler started");

    if let Some(secs) = duration {
        tracing::info!("running for {} seconds", secs);
        tokio::time::sleep(Duration::from_secs(secs)).await;
        tracing::info!("duration reached, shutting down");
    } else {
        match tokio::signal::ctrl_c().await {
            Ok(()) => tracing::info!("received Ctrl+C, shutting down"),
            Err(e) => tracing::warn!("failed to listen for Ctrl+C: {}, shutting down", e),
        }
    }

    shutdown_tasks(
        crawler_handle,
        fetcher_handle,
        stats_handle,
        Duration::from_secs(5),
    )
    .await;
}

#[derive(Default)]
struct RuntimeStats {
    nodes_known: i64,
    queries_sent: i64,
    info_hashes_found: i64,
    metadata_fetched: i64,
}

impl RuntimeStats {
    fn apply(&mut self, event: types::CrawlStatsEvent) {
        match event {
            types::CrawlStatsEvent::DhtSnapshot {
                nodes_known,
                queries_sent,
                info_hashes_found,
            } => {
                self.nodes_known = nodes_known as i64;
                self.queries_sent = queries_sent as i64;
                self.info_hashes_found = info_hashes_found as i64;
            }
            types::CrawlStatsEvent::MetadataFetched => {
                self.metadata_fetched += 1;
            }
            types::CrawlStatsEvent::DhtNodeSeen { .. } => {}
            types::CrawlStatsEvent::MetadataFetchFailed { .. } => {}
        }
    }
}

async fn run_stats_persistence(
    store: Arc<store::Store>,
    mut stats_rx: mpsc::UnboundedReceiver<types::CrawlStatsEvent>,
) {
    let mut stats = RuntimeStats::default();
    while let Some(event) = stats_rx.recv().await {
        let should_persist = matches!(event, types::CrawlStatsEvent::DhtSnapshot { .. });
        let failure_reason = match event {
            types::CrawlStatsEvent::MetadataFetchFailed { reason } => Some(reason),
            _ => None,
        };
        let dht_node = match event {
            types::CrawlStatsEvent::DhtNodeSeen { id, addr } => Some((id, addr)),
            _ => None,
        };
        stats.apply(event);
        if let Some(reason) = failure_reason {
            if let Err(e) = store.increment_metadata_failure(reason.as_str()) {
                tracing::warn!("failed to persist metadata failure count: {}", e);
            }
        }
        if let Some((id, addr)) = dht_node {
            if let Err(e) = store.upsert_dht_node(&id, addr) {
                tracing::warn!("failed to persist DHT node: {}", e);
            }
        }
        if should_persist {
            if let Err(e) = store.insert_crawl_stat(
                stats.nodes_known,
                stats.queries_sent,
                stats.info_hashes_found,
                stats.metadata_fetched,
            ) {
                tracing::warn!("failed to persist crawl stats: {}", e);
            }
        }
    }
}

#[derive(Default)]
struct ShutdownReport {
    #[allow(dead_code)]
    crawler_join_error: bool,
    #[allow(dead_code)]
    fetcher_join_error: bool,
    stats_join_error: bool,
    stats_timed_out: bool,
}

async fn join_aborted_task(name: &str, handle: JoinHandle<()>) -> bool {
    match handle.await {
        Ok(()) => false,
        Err(e) if e.is_cancelled() => false,
        Err(e) => {
            tracing::warn!("{} task join error: {}", name, e);
            true
        }
    }
}

async fn shutdown_tasks(
    crawler_handle: JoinHandle<()>,
    fetcher_handle: JoinHandle<()>,
    mut stats_handle: JoinHandle<()>,
    stats_timeout: Duration,
) -> ShutdownReport {
    crawler_handle.abort();
    fetcher_handle.abort();

    let crawler_join_error = join_aborted_task("crawler", crawler_handle).await;
    let fetcher_join_error = join_aborted_task("metadata fetcher", fetcher_handle).await;

    let mut report = ShutdownReport {
        crawler_join_error,
        fetcher_join_error,
        ..ShutdownReport::default()
    };

    match tokio::time::timeout(stats_timeout, &mut stats_handle).await {
        Ok(Ok(())) => {}
        Ok(Err(e)) => {
            tracing::warn!("stats task join error: {}", e);
            report.stats_join_error = true;
        }
        Err(_) => {
            tracing::warn!("stats task did not stop within {:?}", stats_timeout);
            stats_handle.abort();
            if let Err(e) = stats_handle.await {
                if !e.is_cancelled() {
                    tracing::warn!("stats task join error after abort: {}", e);
                    report.stats_join_error = true;
                }
            }
            report.stats_timed_out = true;
        }
    }

    report
}

fn open_store(db_path: &std::path::Path) -> store::Store {
    if let Some(parent) = db_path.parent() {
        std::fs::create_dir_all(parent)
            .unwrap_or_else(|e| tracing::warn!("could not create db dir: {}", e));
    }
    store::Store::open(db_path).expect("failed to open database")
}

#[derive(Default)]
struct ResumeState {
    info_hashes: Vec<types::InfoHash>,
    dht_nodes: Vec<types::NodeContact>,
}

fn hours_duration(hours: u32) -> Duration {
    Duration::from_secs(u64::from(hours.max(1)) * 3600)
}

fn load_resume_state(
    store: &store::Store,
    config: &config::Config,
) -> Result<ResumeState, rusqlite::Error> {
    let dht_node_max_age = hours_duration(config.crawl.dht_node_max_age_hours);
    let peer_attempt_retention = hours_duration(config.metadata.peer_attempt_retention_hours);

    let pruned_nodes = store.prune_stale_dht_nodes(dht_node_max_age)?;
    let pruned_peer_attempts = store.prune_stale_peer_attempts(peer_attempt_retention)?;
    if pruned_nodes > 0 || pruned_peer_attempts > 0 {
        tracing::info!(
            "pruned {} stale DHT nodes and {} stale peer attempts",
            pruned_nodes,
            pruned_peer_attempts
        );
    }

    Ok(ResumeState {
        info_hashes: store.incomplete_info_hashes(config.crawl.resume_info_hash_limit)?,
        dht_nodes: store.recent_dht_nodes(dht_node_max_age, config.crawl.dht_node_resume_limit)?,
    })
}

fn cmd_search(
    config: &config::Config,
    query: Option<String>,
    limit: usize,
    sort: &str,
    json: bool,
    show_magnet: bool,
) {
    let db_path = config.database_path();
    let store = open_store(&db_path);

    let results = match query {
        Some(q) => store
            .search_torrents(&q, limit, sort)
            .expect("search failed"),
        None => store
            .search_torrents("", limit, sort)
            .expect("search failed"),
    };

    if json {
        let output: Vec<serde_json::Value> = results
            .iter()
            .map(|t| {
                let hash_hex = hex::encode(t.info_hash);
                let mut obj = serde_json::json!({
                    "info_hash": hash_hex,
                    "name": t.name,
                    "total_size": t.total_size,
                    "file_count": t.file_count,
                    "first_seen": t.first_seen,
                    "last_seen": t.last_seen,
                });
                if show_magnet {
                    let dn = t.name.as_deref();
                    obj["magnet"] =
                        serde_json::Value::String(magnet::magnet_uri_from_hash(&t.info_hash, dn));
                }
                obj
            })
            .collect();
        println!("{}", serde_json::to_string_pretty(&output).unwrap());
    } else {
        for t in &results {
            let hash_preview = hex::encode(&t.info_hash[..4]);
            let name = t.name.as_deref().unwrap_or("<unknown>");
            let size_str = format_size(t.total_size);
            print!("{hash_preview}..  {size_str:>10}  {name}");
            if show_magnet {
                let dn = t.name.as_deref();
                let uri = magnet::magnet_uri_from_hash(&t.info_hash, dn);
                print!("  {uri}");
            }
            println!();
        }
        println!("--- {} results ---", results.len());
    }
}

fn cmd_magnet(info_hash: &str, name: Option<&str>) {
    match magnet::magnet_uri(info_hash, name) {
        Ok(uri) => println!("{uri}"),
        Err(e) => {
            eprintln!("error: {e}");
            std::process::exit(2);
        }
    }
}

fn cmd_stats(config: &config::Config) {
    let db_path = config.database_path();
    let store = open_store(&db_path);
    let stats = store.get_stats().expect("failed to get stats");

    println!("Torrents total:           {}", stats.total_torrents);
    println!(
        "With metadata:            {}",
        stats.total_metadata_complete
    );
    println!("Files indexed:            {}", stats.total_files);
    println!(
        "Total size:               {}",
        format_size(stats.total_size)
    );

    match store.get_crawl_history(1) {
        Ok(history) => {
            for line in latest_crawl_stat_lines(history.first()) {
                println!("{}", line);
            }
        }
        Err(e) => tracing::warn!("failed to get crawl history: {}", e),
    }

    match store.get_metadata_failure_counts() {
        Ok(counts) => {
            for line in metadata_failure_count_lines(&counts) {
                println!("{}", line);
            }
        }
        Err(e) => tracing::warn!("failed to get metadata failure counts: {}", e),
    }
}

fn latest_crawl_stat_lines(record: Option<&store::CrawlStatRecord>) -> Vec<String> {
    let Some(record) = record else {
        return Vec::new();
    };

    vec![
        format!(
            "DHT nodes known:          {}",
            record.nodes_known.unwrap_or(0)
        ),
        format!(
            "DHT queries sent:         {}",
            record.queries_sent.unwrap_or(0)
        ),
        format!(
            "Info hashes queued:       {}",
            record.info_hashes_found.unwrap_or(0)
        ),
        format!(
            "Metadata fetched:         {}",
            record.metadata_fetched.unwrap_or(0)
        ),
    ]
}

fn metadata_failure_count_lines(counts: &[store::MetadataFailureCount]) -> Vec<String> {
    if counts.is_empty() {
        return Vec::new();
    }

    let total: i64 = counts.iter().map(|count| count.count).sum();
    let mut lines = vec![format!("Metadata failures:        {}", total)];
    for count in counts {
        let label = format!("{}:", count.reason);
        lines.push(format!("  {label:<23}{}", count.count));
    }
    lines
}

async fn cmd_serve(config: &config::Config, host: &str, port: u16) {
    let db_path = config.database_path();
    if !db_path.exists() {
        if let Some(parent) = db_path.parent() {
            if !parent.exists() {
                std::fs::create_dir_all(parent).expect("failed to create database directory");
            }
        }
    }

    let addr = format!("{host}:{port}");
    tracing::info!("serving dashboard on http://{addr}");

    let app = web::router(db_path);
    let listener = tokio::net::TcpListener::bind(&addr)
        .await
        .expect("failed to bind HTTP server");

    axum::serve(listener, app).await.expect("HTTP server error");
}

fn cmd_prune(config: &config::Config, older_than: u32) {
    let db_path = config.database_path();
    let store = open_store(&db_path);
    let removed = store
        .prune_old_torrents(older_than as i64)
        .expect("prune failed");
    println!(
        "Removed {} torrents older than {} days",
        removed, older_than
    );
}

fn format_size(bytes: i64) -> String {
    if bytes < 0 {
        return "0 B".to_string();
    }
    let bytes = bytes as u64;
    const UNITS: &[&str] = &["B", "KB", "MB", "GB", "TB"];
    let mut size = bytes as f64;
    let mut unit_idx = 0;
    while size >= 1024.0 && unit_idx < UNITS.len() - 1 {
        size /= 1024.0;
        unit_idx += 1;
    }
    if unit_idx == 0 {
        format!("{} {}", bytes, UNITS[unit_idx])
    } else {
        format!("{:.1} {}", size, UNITS[unit_idx])
    }
}

#[cfg(test)]
mod integration_tests {
    use super::*;

    #[test]
    fn test_full_search_sort_prune_pipeline() {
        use crate::store::Store;

        let store = Store::open_in_memory().expect("open in-memory store");

        // Insert torrents
        store
            .upsert_torrent(
                [1u8; 20],
                Some("Ubuntu 24.04 LTS".into()),
                4_000_000_000,
                1,
                "dht",
            )
            .unwrap();
        store
            .upsert_torrent([2u8; 20], Some("Debian 12".into()), 3_500_000_000, 1, "dht")
            .unwrap();
        store
            .upsert_torrent([3u8; 20], Some("Fedora 40".into()), 2_000_000_000, 2, "dht")
            .unwrap();
        store
            .upsert_torrent(
                [4u8; 20],
                Some("Arch Linux".into()),
                1_200_000_000,
                1,
                "dht",
            )
            .unwrap();

        // Mark one as metadata complete
        store
            .mark_metadata_complete(&[1u8; 20], "Ubuntu 24.04 LTS", 262144, 4_000_000_000, 1)
            .unwrap();
        store
            .insert_files(&[1u8; 20], &[("ubuntu-24.04.iso".into(), 4_000_000_000)])
            .unwrap();
        store.refresh_torrent_fts(&[1u8; 20]).unwrap();

        // Search by name
        let results = store.search_torrents("Ubuntu", 10, "last_seen").unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].name.as_deref(), Some("Ubuntu 24.04 LTS"));
        assert!(results[0].metadata_complete);

        // Search by sort: total_size
        let results = store.search_torrents("", 10, "total_size").unwrap();
        assert!(results.len() >= 3);
        assert!(results[0].total_size >= results[1].total_size);

        // Search by sort: name
        let results = store.search_torrents("", 10, "name").unwrap();
        assert!(results.len() >= 3);
        assert_eq!(results[0].name.as_deref(), Some("Arch Linux"));

        // Stats
        let stats = store.get_stats().unwrap();
        assert!(stats.total_torrents >= 4);
        assert!(stats.total_metadata_complete >= 1);
        assert!(stats.total_size > 0);
        assert!(stats.total_files >= 1);

        // Crawl stats
        let id = store.insert_crawl_stat(50, 200, 20, 3).unwrap();
        assert!(id > 0);
        let history = store.get_crawl_history(10).unwrap();
        assert_eq!(history.len(), 1);
        assert_eq!(history[0].nodes_known, Some(50));
        assert_eq!(history[0].metadata_fetched, Some(3));

        // Prune old torrents (won't prune anything since all are recent)
        let removed = store.prune_old_torrents(365).unwrap();
        assert_eq!(removed, 0);
    }

    #[test]
    fn test_format_size() {
        assert_eq!(format_size(0), "0 B");
        assert_eq!(format_size(500), "500 B");
        assert_eq!(format_size(1024), "1.0 KB");
        assert_eq!(format_size(1048576), "1.0 MB");
        assert_eq!(format_size(1073741824), "1.0 GB");
    }

    #[tokio::test]
    async fn test_shutdown_tasks_successful_stats_join() {
        let crawler_handle = tokio::spawn(async {});
        let fetcher_handle = tokio::spawn(async {});
        let stats_handle = tokio::spawn(async {});

        let report = shutdown_tasks(
            crawler_handle,
            fetcher_handle,
            stats_handle,
            Duration::from_millis(50),
        )
        .await;

        assert!(!report.stats_timed_out);
        assert!(!report.stats_join_error);
        assert!(!report.crawler_join_error);
        assert!(!report.fetcher_join_error);
    }

    #[tokio::test]
    async fn test_shutdown_tasks_aborts_stats_on_timeout() {
        let crawler_handle = tokio::spawn(async {});
        let fetcher_handle = tokio::spawn(async {});
        let stats_handle = tokio::spawn(async {
            tokio::time::sleep(Duration::from_secs(60)).await;
        });

        let report = shutdown_tasks(
            crawler_handle,
            fetcher_handle,
            stats_handle,
            Duration::from_millis(1),
        )
        .await;

        assert!(report.stats_timed_out);
    }

    #[tokio::test]
    async fn test_shutdown_tasks_aborts_fetcher_owned_worker() {
        struct NotifyOnDrop(Option<tokio::sync::oneshot::Sender<()>>);

        impl Drop for NotifyOnDrop {
            fn drop(&mut self) {
                if let Some(tx) = self.0.take() {
                    if tx.send(()).is_err() {}
                }
            }
        }

        let crawler_handle = tokio::spawn(async {});
        let (worker_started_tx, worker_started_rx) = tokio::sync::oneshot::channel();
        let (worker_dropped_tx, worker_dropped_rx) = tokio::sync::oneshot::channel();
        let fetcher_handle = tokio::spawn(async move {
            let mut workers = tokio::task::JoinSet::new();
            workers.spawn(async move {
                let _notify = NotifyOnDrop(Some(worker_dropped_tx));
                worker_started_tx.send(()).unwrap();
                std::future::pending::<()>().await;
            });
            std::future::pending::<()>().await;
        });
        worker_started_rx.await.unwrap();
        let stats_handle = tokio::spawn(async {});

        let report = shutdown_tasks(
            crawler_handle,
            fetcher_handle,
            stats_handle,
            Duration::from_millis(50),
        )
        .await;

        tokio::time::timeout(Duration::from_secs(1), worker_dropped_rx)
            .await
            .unwrap()
            .unwrap();
        assert!(!report.fetcher_join_error);
    }

    #[test]
    fn test_runtime_stats_records_queries_and_metadata_fetches() {
        let mut stats = RuntimeStats::default();
        stats.apply(types::CrawlStatsEvent::DhtSnapshot {
            nodes_known: 12,
            queries_sent: 34,
            info_hashes_found: 5,
        });
        stats.apply(types::CrawlStatsEvent::MetadataFetched);
        stats.apply(types::CrawlStatsEvent::MetadataFetched);

        assert_eq!(stats.nodes_known, 12);
        assert_eq!(stats.queries_sent, 34);
        assert_eq!(stats.info_hashes_found, 5);
        assert_eq!(stats.metadata_fetched, 2);
    }

    #[tokio::test]
    async fn test_stats_persistence_writes_snapshot_with_metadata_count() {
        let store = Arc::new(store::Store::open_in_memory().unwrap());
        let (stats_tx, stats_rx) = mpsc::unbounded_channel();
        let handle = tokio::spawn(run_stats_persistence(store.clone(), stats_rx));

        stats_tx
            .send(types::CrawlStatsEvent::MetadataFetched)
            .unwrap();
        stats_tx
            .send(types::CrawlStatsEvent::MetadataFetched)
            .unwrap();
        stats_tx
            .send(types::CrawlStatsEvent::MetadataFetchFailed {
                reason: types::MetadataFailureReason::Connect,
            })
            .unwrap();
        stats_tx
            .send(types::CrawlStatsEvent::MetadataFetchFailed {
                reason: types::MetadataFailureReason::Connect,
            })
            .unwrap();
        stats_tx
            .send(types::CrawlStatsEvent::MetadataFetchFailed {
                reason: types::MetadataFailureReason::Timeout,
            })
            .unwrap();
        stats_tx
            .send(types::CrawlStatsEvent::DhtSnapshot {
                nodes_known: 12,
                queries_sent: 34,
                info_hashes_found: 5,
            })
            .unwrap();
        drop(stats_tx);
        handle.await.unwrap();

        let history = store.get_crawl_history(10).unwrap();
        assert_eq!(history.len(), 1);
        assert_eq!(history[0].nodes_known, Some(12));
        assert_eq!(history[0].queries_sent, Some(34));
        assert_eq!(history[0].info_hashes_found, Some(5));
        assert_eq!(history[0].metadata_fetched, Some(2));
        assert_eq!(
            store.get_metadata_failure_counts().unwrap(),
            vec![
                store::MetadataFailureCount {
                    reason: "connect".to_string(),
                    count: 2,
                },
                store::MetadataFailureCount {
                    reason: "timeout".to_string(),
                    count: 1,
                },
            ]
        );
    }

    #[test]
    fn test_latest_crawl_stat_lines_exposes_counters() {
        let record = store::CrawlStatRecord {
            id: 1,
            timestamp: 123,
            nodes_known: Some(12),
            queries_sent: Some(34),
            info_hashes_found: Some(5),
            metadata_fetched: Some(2),
        };

        let lines = latest_crawl_stat_lines(Some(&record));

        assert_eq!(
            lines,
            vec![
                "DHT nodes known:          12".to_string(),
                "DHT queries sent:         34".to_string(),
                "Info hashes queued:       5".to_string(),
                "Metadata fetched:         2".to_string(),
            ]
        );
    }

    #[test]
    fn test_metadata_failure_count_lines_exposes_failure_reasons() {
        let counts = vec![
            store::MetadataFailureCount {
                reason: "connect".to_string(),
                count: 3,
            },
            store::MetadataFailureCount {
                reason: "timeout".to_string(),
                count: 2,
            },
        ];

        let lines = metadata_failure_count_lines(&counts);

        assert_eq!(
            lines,
            vec![
                "Metadata failures:        5".to_string(),
                "  connect:               3".to_string(),
                "  timeout:               2".to_string(),
            ]
        );
    }

    #[test]
    fn test_load_resume_state_prunes_stale_data_and_loads_progress() {
        let store = store::Store::open_in_memory().unwrap();
        let incomplete = [0x41u8; 20];
        let complete = [0x42u8; 20];
        let fresh_node_id = [0x43u8; 20];
        let stale_node_id = [0x44u8; 20];
        let fresh_node_addr = "8.8.8.8:6881".parse().unwrap();
        let stale_node_addr = "1.1.1.1:6881".parse().unwrap();
        let fresh_peer = "8.8.4.4:6881";
        let stale_peer = "9.9.9.9:6881";

        store.upsert_torrent(incomplete, None, 0, 0, "dht").unwrap();
        store.upsert_torrent(complete, None, 0, 0, "dht").unwrap();
        store
            .mark_metadata_complete(&complete, "done", 16384, 1, 1)
            .unwrap();
        store
            .upsert_dht_node(&fresh_node_id, fresh_node_addr)
            .unwrap();
        store
            .upsert_dht_node(&stale_node_id, stale_node_addr)
            .unwrap();
        store
            .set_peer_attempt(&incomplete, fresh_peer, Some("timeout"))
            .unwrap();
        store
            .set_peer_attempt(&incomplete, stale_peer, Some("connect"))
            .unwrap();
        store
            .execute_batch_for_test(&format!(
                "
                UPDATE dht_nodes SET last_seen = unixepoch() - 7200 WHERE id = '{}';
                UPDATE metadata_peer_attempts SET last_attempt = unixepoch() - 7200 WHERE peer_addr = '{}';
                ",
                hex::encode(stale_node_id),
                stale_peer
            ))
            .unwrap();
        let config = config::Config {
            metadata: config::MetadataConfig {
                peer_attempt_retention_hours: 1,
                ..config::MetadataConfig::default()
            },
            crawl: config::CrawlConfig {
                resume_info_hash_limit: 10,
                dht_node_resume_limit: 10,
                dht_node_max_age_hours: 1,
                ..config::CrawlConfig::default()
            },
            ..config::Config::default()
        };

        let resume = load_resume_state(&store, &config).unwrap();

        assert_eq!(resume.info_hashes, vec![incomplete]);
        assert_eq!(resume.dht_nodes.len(), 1);
        assert_eq!(resume.dht_nodes[0].id, fresh_node_id);
        assert_eq!(resume.dht_nodes[0].addr, fresh_node_addr);
        assert!(store
            .should_skip_peer_retry(&incomplete, fresh_peer, 24)
            .unwrap());
        assert!(!store
            .should_skip_peer_retry(&incomplete, stale_peer, 24)
            .unwrap());
    }
}
