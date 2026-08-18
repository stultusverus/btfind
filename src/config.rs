use serde::Deserialize;
use std::path::PathBuf;

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    #[serde(default)]
    pub network: NetworkConfig,

    #[serde(default)]
    pub metadata: MetadataConfig,

    #[serde(default)]
    pub database: DatabaseConfig,

    #[serde(default)]
    pub crawl: CrawlConfig,

    #[serde(default)]
    pub web: WebConfig,
}

#[derive(Debug, Clone, Deserialize)]
pub struct NetworkConfig {
    #[serde(default = "default_port")]
    pub port: u16,

    #[serde(default = "default_bootstrap_nodes")]
    pub bootstrap_nodes: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct MetadataConfig {
    #[allow(dead_code)]
    #[serde(default = "default_max_concurrent")]
    pub max_concurrent: usize,

    #[serde(default = "default_peer_timeout_secs")]
    pub peer_timeout_secs: u64,

    #[serde(default = "default_retry_after_hours")]
    pub retry_after_hours: u32,

    #[serde(default = "default_peer_attempt_retention_hours")]
    pub peer_attempt_retention_hours: u32,

    #[serde(default = "default_max_peers_per_hash")]
    pub max_peers_per_hash: usize,

    #[serde(default = "default_max_active_hash_jobs")]
    pub max_active_hash_jobs: usize,

    #[serde(default = "default_max_metadata_size_bytes")]
    pub max_metadata_size_bytes: u32,

    #[serde(default = "default_max_peer_attempts_per_round")]
    pub max_peer_attempts_per_round: usize,
}

#[derive(Debug, Clone, Deserialize)]
pub struct DatabaseConfig {
    #[serde(default = "default_db_path")]
    pub path: String,

    #[serde(default = "default_database_batch_size")]
    pub batch_size: usize,

    #[serde(default = "default_database_flush_interval_ms")]
    pub flush_interval_ms: u64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct CrawlConfig {
    #[serde(default = "default_get_peers_interval_secs")]
    pub get_peers_interval_secs: u64,

    #[serde(default = "default_bucket_refresh_mins")]
    pub bucket_refresh_mins: u64,

    #[serde(default = "default_resume_info_hash_limit")]
    pub resume_info_hash_limit: usize,

    #[serde(default = "default_dht_node_resume_limit")]
    pub dht_node_resume_limit: usize,

    #[serde(default = "default_dht_node_max_age_hours")]
    pub dht_node_max_age_hours: u32,

    #[serde(default = "default_info_hash_channel_capacity")]
    pub info_hash_channel_capacity: usize,

    #[serde(default = "default_stats_channel_capacity")]
    pub stats_channel_capacity: usize,

    #[serde(default = "default_max_discovery_hashes")]
    pub max_discovery_hashes: usize,

    #[serde(default = "default_max_pending_rpcs")]
    pub max_pending_rpcs: usize,

    #[serde(default = "default_max_candidate_nodes")]
    pub max_candidate_nodes: usize,

    #[serde(default = "default_rpc_timeout_secs")]
    pub rpc_timeout_secs: u64,

    #[serde(default = "default_sampling_enabled")]
    pub sampling_enabled: bool,

    #[serde(default = "default_sampling_interval_secs")]
    pub sampling_interval_secs: u64,

    #[serde(default = "default_sampling_min_remote_interval_secs")]
    pub sampling_min_remote_interval_secs: u64,

    #[serde(default = "default_sampling_requests_per_tick")]
    pub sampling_requests_per_tick: usize,

    #[serde(default = "default_max_samples_per_response")]
    pub max_samples_per_response: usize,

    #[serde(default = "default_announced_peer_hash_capacity")]
    pub announced_peer_hash_capacity: usize,

    #[serde(default = "default_announced_peers_per_hash")]
    pub announced_peers_per_hash: usize,

    #[serde(default = "default_announced_peer_ttl_secs")]
    pub announced_peer_ttl_secs: u64,

    #[serde(default = "default_shutdown_drain_secs")]
    pub shutdown_drain_secs: u64,
}

// Default implementations
fn default_port() -> u16 {
    6881
}

fn default_bootstrap_nodes() -> Vec<String> {
    vec![
        "router.bittorrent.com:6881".to_string(),
        "router.utorrent.com:6881".to_string(),
        "dht.transmissionbt.com:6881".to_string(),
        "dht.libtorrent.org:25401".to_string(),
        "dht.aelitis.com:6881".to_string(),
    ]
}

fn default_max_concurrent() -> usize {
    10
}
fn default_peer_timeout_secs() -> u64 {
    30
}
fn default_retry_after_hours() -> u32 {
    24
}
fn default_peer_attempt_retention_hours() -> u32 {
    72
}
fn default_max_peers_per_hash() -> usize {
    64
}
fn default_max_active_hash_jobs() -> usize {
    1024
}
fn default_max_metadata_size_bytes() -> u32 {
    8 * 1024 * 1024
}
fn default_max_peer_attempts_per_round() -> usize {
    8
}

fn default_db_path() -> String {
    "~/.local/share/btfind/torrents.db".to_string()
}
fn default_database_batch_size() -> usize {
    128
}
fn default_database_flush_interval_ms() -> u64 {
    1000
}

fn default_get_peers_interval_secs() -> u64 {
    5
}
fn default_bucket_refresh_mins() -> u64 {
    15
}
fn default_resume_info_hash_limit() -> usize {
    5000
}
fn default_dht_node_resume_limit() -> usize {
    1024
}
fn default_dht_node_max_age_hours() -> u32 {
    24
}
fn default_info_hash_channel_capacity() -> usize {
    1024
}
fn default_stats_channel_capacity() -> usize {
    4096
}
fn default_max_discovery_hashes() -> usize {
    50_000
}
fn default_max_pending_rpcs() -> usize {
    4096
}
fn default_max_candidate_nodes() -> usize {
    8192
}
fn default_rpc_timeout_secs() -> u64 {
    30
}
fn default_sampling_enabled() -> bool {
    false
}
fn default_sampling_interval_secs() -> u64 {
    5
}
fn default_sampling_min_remote_interval_secs() -> u64 {
    300
}
fn default_sampling_requests_per_tick() -> usize {
    1
}
fn default_max_samples_per_response() -> usize {
    256
}
fn default_announced_peer_hash_capacity() -> usize {
    10_000
}
fn default_announced_peers_per_hash() -> usize {
    64
}
fn default_announced_peer_ttl_secs() -> u64 {
    1800
}
fn default_shutdown_drain_secs() -> u64 {
    10
}

#[derive(Debug, Clone, Deserialize)]
pub struct WebConfig {
    #[serde(default = "default_web_host")]
    pub host: String,

    #[serde(default = "default_web_port")]
    pub port: u16,
}

fn default_web_host() -> String {
    "127.0.0.1".to_string()
}

fn default_web_port() -> u16 {
    8080
}

impl Default for NetworkConfig {
    fn default() -> Self {
        NetworkConfig {
            port: default_port(),
            bootstrap_nodes: default_bootstrap_nodes(),
        }
    }
}

impl Default for MetadataConfig {
    fn default() -> Self {
        MetadataConfig {
            max_concurrent: default_max_concurrent(),
            peer_timeout_secs: default_peer_timeout_secs(),
            retry_after_hours: default_retry_after_hours(),
            peer_attempt_retention_hours: default_peer_attempt_retention_hours(),
            max_peers_per_hash: default_max_peers_per_hash(),
            max_active_hash_jobs: default_max_active_hash_jobs(),
            max_metadata_size_bytes: default_max_metadata_size_bytes(),
            max_peer_attempts_per_round: default_max_peer_attempts_per_round(),
        }
    }
}

impl Default for DatabaseConfig {
    fn default() -> Self {
        DatabaseConfig {
            path: default_db_path(),
            batch_size: default_database_batch_size(),
            flush_interval_ms: default_database_flush_interval_ms(),
        }
    }
}

impl Default for CrawlConfig {
    fn default() -> Self {
        CrawlConfig {
            get_peers_interval_secs: default_get_peers_interval_secs(),
            bucket_refresh_mins: default_bucket_refresh_mins(),
            resume_info_hash_limit: default_resume_info_hash_limit(),
            dht_node_resume_limit: default_dht_node_resume_limit(),
            dht_node_max_age_hours: default_dht_node_max_age_hours(),
            info_hash_channel_capacity: default_info_hash_channel_capacity(),
            stats_channel_capacity: default_stats_channel_capacity(),
            max_discovery_hashes: default_max_discovery_hashes(),
            max_pending_rpcs: default_max_pending_rpcs(),
            max_candidate_nodes: default_max_candidate_nodes(),
            rpc_timeout_secs: default_rpc_timeout_secs(),
            sampling_enabled: default_sampling_enabled(),
            sampling_interval_secs: default_sampling_interval_secs(),
            sampling_min_remote_interval_secs: default_sampling_min_remote_interval_secs(),
            sampling_requests_per_tick: default_sampling_requests_per_tick(),
            max_samples_per_response: default_max_samples_per_response(),
            announced_peer_hash_capacity: default_announced_peer_hash_capacity(),
            announced_peers_per_hash: default_announced_peers_per_hash(),
            announced_peer_ttl_secs: default_announced_peer_ttl_secs(),
            shutdown_drain_secs: default_shutdown_drain_secs(),
        }
    }
}

impl Default for WebConfig {
    fn default() -> Self {
        WebConfig {
            host: default_web_host(),
            port: default_web_port(),
        }
    }
}

#[allow(clippy::derivable_impls)]
impl Default for Config {
    fn default() -> Self {
        Config {
            network: NetworkConfig::default(),
            metadata: MetadataConfig::default(),
            database: DatabaseConfig::default(),
            crawl: CrawlConfig::default(),
            web: WebConfig::default(),
        }
    }
}

impl Config {
    pub fn load() -> Result<Self, String> {
        let config_path = dirs::config_dir()
            .unwrap_or_else(|| PathBuf::from("."))
            .join("btfind")
            .join("config.toml");

        if config_path.exists() {
            match std::fs::read_to_string(&config_path) {
                Ok(contents) => match toml::from_str(&contents) {
                    Ok(config) => {
                        tracing::info!("loaded config from {}", config_path.display());
                        return Ok(config);
                    }
                    Err(error) => {
                        return Err(format!(
                            "failed to parse {}: {}",
                            config_path.display(),
                            error
                        ))
                    }
                },
                Err(error) => {
                    return Err(format!(
                        "failed to read {}: {}",
                        config_path.display(),
                        error
                    ))
                }
            }
        }

        Ok(Config::default())
    }

    pub fn database_path(&self) -> PathBuf {
        let path = self.database.path.clone();
        if path == "~" {
            dirs::home_dir().unwrap_or_else(|| PathBuf::from("."))
        } else if path.starts_with("~/") {
            let home = dirs::home_dir().unwrap_or_else(|| PathBuf::from("."));
            home.join(path.strip_prefix("~/").expect("path starts with ~/"))
        } else {
            PathBuf::from(path)
        }
    }

    pub fn validate(&self) -> Result<(), String> {
        validate_range(
            "metadata.max_concurrent",
            self.metadata.max_concurrent,
            1,
            4096,
        )?;
        validate_range(
            "metadata.max_peers_per_hash",
            self.metadata.max_peers_per_hash,
            1,
            4096,
        )?;
        validate_range(
            "metadata.max_active_hash_jobs",
            self.metadata.max_active_hash_jobs,
            1,
            1_000_000,
        )?;
        validate_range(
            "metadata.max_peer_attempts_per_round",
            self.metadata.max_peer_attempts_per_round,
            1,
            self.metadata.max_peers_per_hash,
        )?;
        validate_range(
            "crawl.info_hash_channel_capacity",
            self.crawl.info_hash_channel_capacity,
            1,
            1_000_000,
        )?;
        validate_range(
            "crawl.stats_channel_capacity",
            self.crawl.stats_channel_capacity,
            1,
            1_000_000,
        )?;
        validate_range(
            "crawl.max_discovery_hashes",
            self.crawl.max_discovery_hashes,
            1,
            10_000_000,
        )?;
        validate_range(
            "crawl.max_pending_rpcs",
            self.crawl.max_pending_rpcs,
            1,
            u16::MAX as usize,
        )?;
        validate_range(
            "crawl.max_candidate_nodes",
            self.crawl.max_candidate_nodes,
            8,
            1_000_000,
        )?;
        validate_range(
            "crawl.sampling_requests_per_tick",
            self.crawl.sampling_requests_per_tick,
            1,
            256,
        )?;
        validate_range(
            "crawl.max_samples_per_response",
            self.crawl.max_samples_per_response,
            1,
            4096,
        )?;
        validate_range(
            "crawl.announced_peer_hash_capacity",
            self.crawl.announced_peer_hash_capacity,
            1,
            1_000_000,
        )?;
        validate_range(
            "crawl.announced_peers_per_hash",
            self.crawl.announced_peers_per_hash,
            1,
            4096,
        )?;
        validate_range("database.batch_size", self.database.batch_size, 1, 10_000)?;

        validate_u64_range(
            "metadata.peer_timeout_secs",
            self.metadata.peer_timeout_secs,
            1,
            600,
        )?;
        validate_u64_range(
            "crawl.get_peers_interval_secs",
            self.crawl.get_peers_interval_secs,
            1,
            3600,
        )?;
        validate_u64_range(
            "crawl.bucket_refresh_mins",
            self.crawl.bucket_refresh_mins,
            1,
            1440,
        )?;
        validate_u64_range(
            "crawl.rpc_timeout_secs",
            self.crawl.rpc_timeout_secs,
            1,
            600,
        )?;
        validate_u64_range(
            "crawl.sampling_interval_secs",
            self.crawl.sampling_interval_secs,
            1,
            3600,
        )?;
        validate_u64_range(
            "crawl.sampling_min_remote_interval_secs",
            self.crawl.sampling_min_remote_interval_secs,
            1,
            86_400,
        )?;
        validate_u64_range(
            "crawl.shutdown_drain_secs",
            self.crawl.shutdown_drain_secs,
            1,
            300,
        )?;
        validate_u64_range(
            "database.flush_interval_ms",
            self.database.flush_interval_ms,
            1,
            60_000,
        )?;
        validate_u64_range(
            "crawl.announced_peer_ttl_secs",
            self.crawl.announced_peer_ttl_secs,
            1,
            86_400,
        )?;

        if !(16 * 1024..=64 * 1024 * 1024).contains(&self.metadata.max_metadata_size_bytes) {
            return Err(
                "metadata.max_metadata_size_bytes must be between 16384 and 67108864".to_string(),
            );
        }

        self.crawl
            .bucket_refresh_mins
            .checked_mul(60)
            .ok_or_else(|| "crawl.bucket_refresh_mins overflows seconds".to_string())?;
        u64::from(self.crawl.dht_node_max_age_hours)
            .checked_mul(3600)
            .ok_or_else(|| "crawl.dht_node_max_age_hours overflows seconds".to_string())?;
        u64::from(self.metadata.peer_attempt_retention_hours)
            .checked_mul(3600)
            .ok_or_else(|| "metadata.peer_attempt_retention_hours overflows seconds".to_string())?;

        Ok(())
    }
}

fn validate_u64_range(name: &str, value: u64, min: u64, max: u64) -> Result<(), String> {
    if (min..=max).contains(&value) {
        Ok(())
    } else {
        Err(format!("{} must be between {} and {}", name, min, max))
    }
}

fn validate_range(name: &str, value: usize, min: usize, max: usize) -> Result<(), String> {
    if (min..=max).contains(&value) {
        Ok(())
    } else {
        Err(format!("{} must be between {} and {}", name, min, max))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config_with_db_path(path: &str) -> Config {
        Config {
            database: DatabaseConfig {
                path: path.to_string(),
                ..DatabaseConfig::default()
            },
            ..Config::default()
        }
    }

    #[test]
    fn test_database_path_expands_home_only() {
        let expected = dirs::home_dir().unwrap_or_else(|| PathBuf::from("."));
        assert_eq!(config_with_db_path("~").database_path(), expected);
    }

    #[test]
    fn test_database_path_expands_home_prefix() {
        let home = dirs::home_dir().unwrap_or_else(|| PathBuf::from("."));
        assert_eq!(
            config_with_db_path("~/btfind.db").database_path(),
            home.join("btfind.db")
        );
    }

    #[test]
    fn test_database_path_accepts_absolute_path() {
        assert_eq!(
            config_with_db_path("/tmp/btfind.db").database_path(),
            PathBuf::from("/tmp/btfind.db")
        );
    }

    #[test]
    fn test_resume_defaults_are_bounded() {
        let config = Config::default();

        assert_eq!(config.crawl.resume_info_hash_limit, 5000);
        assert_eq!(config.crawl.dht_node_resume_limit, 1024);
        assert_eq!(config.crawl.dht_node_max_age_hours, 24);
        assert_eq!(config.metadata.peer_attempt_retention_hours, 72);
        assert_eq!(config.crawl.max_pending_rpcs, 4096);
        assert_eq!(config.metadata.max_metadata_size_bytes, 8 * 1024 * 1024);
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_validation_rejects_zero_queue_capacity() {
        let mut config = Config::default();
        config.crawl.info_hash_channel_capacity = 0;
        assert_eq!(
            config.validate().unwrap_err(),
            "crawl.info_hash_channel_capacity must be between 1 and 1000000"
        );
    }

    #[test]
    fn test_validation_rejects_excessive_metadata_size() {
        let mut config = Config::default();
        config.metadata.max_metadata_size_bytes = 64 * 1024 * 1024 + 1;
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_validation_rejects_zero_announced_peer_capacity() {
        let mut config = Config::default();
        config.crawl.announced_peers_per_hash = 0;
        assert_eq!(
            config.validate().unwrap_err(),
            "crawl.announced_peers_per_hash must be between 1 and 4096"
        );

        config.crawl.announced_peers_per_hash = 1;
        config.crawl.announced_peer_hash_capacity = 0;
        assert_eq!(
            config.validate().unwrap_err(),
            "crawl.announced_peer_hash_capacity must be between 1 and 1000000"
        );
    }
}
