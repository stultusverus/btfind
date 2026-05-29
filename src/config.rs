use serde::Deserialize;
use std::path::PathBuf;

#[derive(Debug, Deserialize)]
pub struct Config {
    #[serde(default)]
    pub network: NetworkConfig,

    #[serde(default)]
    pub metadata: MetadataConfig,

    #[serde(default)]
    pub database: DatabaseConfig,

    #[serde(default)]
    pub crawl: CrawlConfig,
}

#[derive(Debug, Deserialize)]
pub struct NetworkConfig {
    #[serde(default = "default_port")]
    pub port: u16,

    #[serde(default = "default_bootstrap_nodes")]
    pub bootstrap_nodes: Vec<String>,
}

#[derive(Debug, Deserialize)]
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
}

#[derive(Debug, Deserialize)]
pub struct DatabaseConfig {
    #[serde(default = "default_db_path")]
    pub path: String,
}

#[derive(Debug, Deserialize)]
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

fn default_db_path() -> String {
    "~/.local/share/btfind/torrents.db".to_string()
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
        }
    }
}

impl Default for DatabaseConfig {
    fn default() -> Self {
        DatabaseConfig {
            path: default_db_path(),
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
        }
    }
}

impl Config {
    pub fn load() -> Self {
        let config_path = dirs::config_dir()
            .unwrap_or_else(|| PathBuf::from("."))
            .join("btfind")
            .join("config.toml");

        if config_path.exists() {
            match std::fs::read_to_string(&config_path) {
                Ok(contents) => match toml::from_str(&contents) {
                    Ok(config) => {
                        tracing::info!("loaded config from {}", config_path.display());
                        return config;
                    }
                    Err(e) => {
                        tracing::warn!("failed to parse config: {}, using defaults", e);
                    }
                },
                Err(e) => {
                    tracing::warn!("failed to read config: {}, using defaults", e);
                }
            }
        }

        Config::default()
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
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config_with_db_path(path: &str) -> Config {
        Config {
            database: DatabaseConfig {
                path: path.to_string(),
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
    }
}
