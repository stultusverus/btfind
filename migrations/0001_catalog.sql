CREATE TABLE torrents (
    info_hash CHAR(40) CHARACTER SET ascii COLLATE ascii_general_ci PRIMARY KEY,
    name LONGTEXT, piece_length BIGINT, total_size BIGINT NOT NULL DEFAULT 0,
    file_count BIGINT NOT NULL DEFAULT 0, source VARCHAR(255) NOT NULL DEFAULT 'unknown',
    first_seen BIGINT NOT NULL, last_seen BIGINT NOT NULL, last_attempt BIGINT NOT NULL DEFAULT 0,
    metadata_complete BOOLEAN NOT NULL DEFAULT FALSE,
    INDEX idx_first(first_seen, info_hash), INDEX idx_last(last_seen, info_hash),
    INDEX idx_size(total_size, info_hash), INDEX idx_name(name(191), info_hash),
    INDEX idx_complete_last(metadata_complete, last_seen, info_hash),
    INDEX idx_complete_first(metadata_complete, first_seen, info_hash),
    INDEX idx_complete_size(metadata_complete, total_size, info_hash)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
CREATE TABLE files (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    torrent_id CHAR(40) CHARACTER SET ascii COLLATE ascii_general_ci NOT NULL,
    path LONGTEXT NOT NULL, size BIGINT NOT NULL,
    FOREIGN KEY(torrent_id) REFERENCES torrents(info_hash) ON DELETE CASCADE,
    INDEX idx_files(torrent_id, path(191), id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
CREATE TABLE crawl_stats (
    id BIGINT PRIMARY KEY AUTO_INCREMENT, timestamp BIGINT NOT NULL,
    nodes_known BIGINT, queries_sent BIGINT, info_hashes_found BIGINT, metadata_fetched BIGINT
) ENGINE=InnoDB;
CREATE TABLE metadata_peer_attempts (
    info_hash CHAR(40) CHARACTER SET ascii COLLATE ascii_general_ci NOT NULL,
    peer_addr VARCHAR(64) CHARACTER SET ascii COLLATE ascii_general_ci NOT NULL,
    last_attempt BIGINT NOT NULL, last_error TEXT,
    PRIMARY KEY(info_hash, peer_addr), INDEX idx_attempt(last_attempt),
    FOREIGN KEY(info_hash) REFERENCES torrents(info_hash) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
CREATE TABLE metadata_failure_counts (
    reason VARCHAR(255) PRIMARY KEY, count BIGINT NOT NULL DEFAULT 0
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_as_cs;
CREATE TABLE dht_nodes (
    id CHAR(40) CHARACTER SET ascii COLLATE ascii_general_ci PRIMARY KEY,
    addr VARCHAR(64) NOT NULL, last_seen BIGINT NOT NULL, INDEX idx_nodes_seen(last_seen)
) ENGINE=InnoDB;
CREATE TABLE hash_jobs (
    info_hash CHAR(40) CHARACTER SET ascii COLLATE ascii_general_ci PRIMARY KEY,
    status VARCHAR(32) NOT NULL DEFAULT 'queued', attempt_count BIGINT NOT NULL DEFAULT 0,
    next_attempt_at BIGINT NOT NULL DEFAULT 0, last_failure TEXT, last_success_at BIGINT,
    last_new_peer_at BIGINT, discovery_sources VARCHAR(255) NOT NULL DEFAULT '', updated_at BIGINT NOT NULL,
    FOREIGN KEY(info_hash) REFERENCES torrents(info_hash) ON DELETE CASCADE,
    INDEX idx_schedule(status, next_attempt_at, updated_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
CREATE TABLE sample_observations (
    info_hash CHAR(40) CHARACTER SET ascii COLLATE ascii_general_ci PRIMARY KEY,
    first_seen BIGINT NOT NULL, last_seen BIGINT NOT NULL, sample_count BIGINT NOT NULL DEFAULT 1,
    sampling_node_id VARCHAR(40), sampling_addr VARCHAR(64), sweep_id BIGINT,
    FOREIGN KEY(info_hash) REFERENCES torrents(info_hash) ON DELETE CASCADE,
    INDEX idx_sample_seen(last_seen)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
CREATE TABLE app_state (`key` VARCHAR(255) PRIMARY KEY, value LONGBLOB NOT NULL)
ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_as_cs;
CREATE TABLE hash_job_peers (
    info_hash CHAR(40) CHARACTER SET ascii COLLATE ascii_general_ci NOT NULL,
    peer_addr VARCHAR(64) CHARACTER SET ascii COLLATE ascii_general_ci NOT NULL, last_seen BIGINT NOT NULL,
    PRIMARY KEY(info_hash, peer_addr), INDEX idx_peer_seen(info_hash, last_seen),
    FOREIGN KEY(info_hash) REFERENCES torrents(info_hash) ON DELETE CASCADE
) ENGINE=InnoDB;
CREATE TABLE search_documents (
    info_hash CHAR(40) CHARACTER SET ascii COLLATE ascii_general_ci PRIMARY KEY,
    name LONGTEXT NOT NULL, paths LONGTEXT NOT NULL,
    FOREIGN KEY(info_hash) REFERENCES torrents(info_hash) ON DELETE CASCADE,
    FULLTEXT INDEX ft_content(name, paths) WITH PARSER ngram,
    FULLTEXT INDEX ft_name(name) WITH PARSER ngram
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
CREATE TABLE catalog_totals (
    id INT PRIMARY KEY, total_torrents BIGINT NOT NULL, total_metadata_complete BIGINT NOT NULL,
    total_files BIGINT NOT NULL, total_size BIGINT NOT NULL
) ENGINE=InnoDB;
INSERT INTO catalog_totals VALUES (1,0,0,0,0);
CREATE TABLE persistence_receipts (id CHAR(36) CHARACTER SET ascii PRIMARY KEY, created_at BIGINT NOT NULL)
ENGINE=InnoDB;
CREATE TABLE import_progress (
    table_name VARCHAR(64) PRIMARY KEY, fingerprint CHAR(64) NOT NULL, last_rowid BIGINT NOT NULL,
    finished BOOLEAN NOT NULL DEFAULT FALSE
) ENGINE=InnoDB;
