
CREATE TABLE IF NOT EXISTS torrents (
info_hash         TEXT PRIMARY KEY,
name              TEXT,
piece_length      INTEGER,
total_size        INTEGER NOT NULL DEFAULT 0,
file_count        INTEGER NOT NULL DEFAULT 0,
source            TEXT NOT NULL DEFAULT 'unknown',
first_seen        INTEGER NOT NULL,
last_seen         INTEGER NOT NULL,
last_attempt      INTEGER NOT NULL DEFAULT 0,
metadata_complete INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS files (
id          INTEGER PRIMARY KEY AUTOINCREMENT,
torrent_id  TEXT NOT NULL REFERENCES torrents(info_hash) ON DELETE CASCADE,
path        TEXT NOT NULL,
size        INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_torrents_first_seen ON torrents(first_seen);
CREATE INDEX IF NOT EXISTS idx_torrents_last_seen ON torrents(last_seen);
CREATE INDEX IF NOT EXISTS idx_torrents_name ON torrents(name);
CREATE INDEX IF NOT EXISTS idx_files_torrent ON files(torrent_id);

CREATE TABLE IF NOT EXISTS crawl_stats (
id                INTEGER PRIMARY KEY AUTOINCREMENT,
timestamp         INTEGER NOT NULL,
nodes_known       INTEGER,
queries_sent      INTEGER,
info_hashes_found INTEGER,
metadata_fetched  INTEGER
);

CREATE TABLE IF NOT EXISTS metadata_peer_attempts (
info_hash    TEXT NOT NULL REFERENCES torrents(info_hash) ON DELETE CASCADE,
peer_addr    TEXT NOT NULL,
last_attempt INTEGER NOT NULL,
last_error   TEXT,
PRIMARY KEY (info_hash, peer_addr)
);

CREATE TABLE IF NOT EXISTS metadata_failure_counts (
reason TEXT PRIMARY KEY,
count  INTEGER NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_metadata_peer_attempts_hash
ON metadata_peer_attempts(info_hash);

CREATE TABLE IF NOT EXISTS dht_nodes (
id        TEXT PRIMARY KEY,
addr      TEXT NOT NULL,
last_seen INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_dht_nodes_last_seen ON dht_nodes(last_seen);

CREATE VIRTUAL TABLE IF NOT EXISTS torrent_fts USING fts5(
info_hash UNINDEXED,
name,
paths,
tokenize = 'trigram'
);


CREATE TABLE IF NOT EXISTS hash_jobs (
info_hash             TEXT PRIMARY KEY REFERENCES torrents(info_hash) ON DELETE CASCADE,
status                TEXT NOT NULL DEFAULT 'queued',
attempt_count         INTEGER NOT NULL DEFAULT 0,
next_attempt_at       INTEGER NOT NULL DEFAULT 0,
last_failure          TEXT,
last_success_at       INTEGER,
last_new_peer_at      INTEGER,
discovery_sources     TEXT NOT NULL DEFAULT '',
updated_at            INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS sample_observations (
info_hash        TEXT PRIMARY KEY REFERENCES torrents(info_hash) ON DELETE CASCADE,
first_seen       INTEGER NOT NULL,
last_seen        INTEGER NOT NULL,
sample_count     INTEGER NOT NULL DEFAULT 1,
sampling_node_id TEXT,
sampling_addr    TEXT,
sweep_id         INTEGER
);

CREATE TABLE IF NOT EXISTS app_state (
key   TEXT PRIMARY KEY,
value BLOB NOT NULL
);


CREATE INDEX IF NOT EXISTS idx_hash_jobs_schedule
ON hash_jobs(status, next_attempt_at);
CREATE INDEX IF NOT EXISTS idx_sample_observations_last_seen
ON sample_observations(last_seen);


CREATE TABLE IF NOT EXISTS hash_job_peers (
info_hash TEXT NOT NULL REFERENCES torrents(info_hash) ON DELETE CASCADE,
peer_addr TEXT NOT NULL,
last_seen INTEGER NOT NULL,
PRIMARY KEY (info_hash, peer_addr)
);
CREATE INDEX IF NOT EXISTS idx_hash_job_peers_seen
ON hash_job_peers(info_hash, last_seen DESC);

PRAGMA user_version=4;

