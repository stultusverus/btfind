use crate::store::{PeerRetryEligibility, Store};
use crate::types::{CrawlStatsEvent, HashDiscovery, InfoHash, MetadataFailureReason, PeerContact};
use crate::wire::{self, ExtendedHandshake, Handshake, WireMessage, HANDSHAKE_LEN};
use rand::Rng;
use sha1::{Digest, Sha1};
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::task::JoinSet;
use tokio::time::timeout;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimeoutStage {
    Connect,
    HandshakeRead,
    HandshakeWrite,
    ExtensionRead,
    ExtensionWrite,
    MetadataRead,
    MetadataWrite,
}

#[derive(Debug)]
pub enum MetadataFetchError {
    Connect(String),
    Timeout { stage: TimeoutStage },
    InvalidHandshake,
    ExtensionUnsupported,
    MetadataRejected,
    MalformedProtocol(String),
}

impl MetadataFetchError {
    fn from_protocol_message(message: String) -> Self {
        if message == "connect timeout" {
            Self::Timeout {
                stage: TimeoutStage::Connect,
            }
        } else if message.starts_with("connect error") {
            Self::Connect(message)
        } else if message.contains("handshake read timeout") {
            Self::Timeout {
                stage: TimeoutStage::HandshakeRead,
            }
        } else if message.contains("handshake write timeout") {
            Self::Timeout {
                stage: TimeoutStage::HandshakeWrite,
            }
        } else if message.contains("extended handshake") && message.contains("timeout") {
            Self::Timeout {
                stage: if message.contains("write") {
                    TimeoutStage::ExtensionWrite
                } else {
                    TimeoutStage::ExtensionRead
                },
            }
        } else if message.contains("metadata") && message.contains("write timeout") {
            Self::Timeout {
                stage: TimeoutStage::MetadataWrite,
            }
        } else if message.contains("timeout") {
            Self::Timeout {
                stage: TimeoutStage::MetadataRead,
            }
        } else if message.contains("invalid handshake") || message.contains("info_hash mismatch") {
            Self::InvalidHandshake
        } else if message.contains("extensions")
            || message.contains("ut_metadata")
            || message.contains("metadata_size")
            || message.contains("extended handshake")
        {
            Self::ExtensionUnsupported
        } else if message.contains("rejected metadata") {
            Self::MetadataRejected
        } else {
            Self::MalformedProtocol(message)
        }
    }

    fn reason(&self) -> MetadataFailureReason {
        match self {
            MetadataFetchError::Connect(_) => MetadataFailureReason::Connect,
            MetadataFetchError::Timeout { .. } => MetadataFailureReason::Timeout,
            MetadataFetchError::InvalidHandshake => MetadataFailureReason::Handshake,
            MetadataFetchError::ExtensionUnsupported => MetadataFailureReason::Extension,
            MetadataFetchError::MetadataRejected => MetadataFailureReason::Rejected,
            MetadataFetchError::MalformedProtocol(_) => MetadataFailureReason::Protocol,
        }
    }

    fn long_backoff(&self) -> bool {
        matches!(
            self,
            MetadataFetchError::ExtensionUnsupported | MetadataFetchError::MetadataRejected
        )
    }
}

impl std::fmt::Display for MetadataFetchError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MetadataFetchError::Connect(message)
            | MetadataFetchError::MalformedProtocol(message) => formatter.write_str(message),
            MetadataFetchError::Timeout { stage } => {
                write!(formatter, "timeout during {:?}", stage)
            }
            MetadataFetchError::InvalidHandshake => formatter.write_str("invalid handshake"),
            MetadataFetchError::ExtensionUnsupported => {
                formatter.write_str("metadata extension unsupported")
            }
            MetadataFetchError::MetadataRejected => formatter.write_str("metadata rejected"),
        }
    }
}

#[cfg(test)]
fn classify_metadata_failure(message: &str) -> MetadataFailureReason {
    MetadataFetchError::from_protocol_message(message.to_string()).reason()
}

pub fn metadata_piece_count(total_size: u32) -> u32 {
    total_size.div_ceil(16384)
}

fn metadata_piece_len(total_size: u32, total_pieces: u32, piece_idx: u32) -> usize {
    if piece_idx + 1 == total_pieces {
        (total_size - (piece_idx * 16384)) as usize
    } else {
        16384
    }
}

#[allow(dead_code)]
pub async fn fetch_from_peer(
    info_hash: &InfoHash,
    peer: &PeerContact,
    peer_timeout: Duration,
) -> Result<Vec<u8>, MetadataFetchError> {
    fetch_from_peer_with_limit(info_hash, peer, peer_timeout, 8 * 1024 * 1024).await
}

pub async fn fetch_from_peer_with_limit(
    info_hash: &InfoHash,
    peer: &PeerContact,
    peer_timeout: Duration,
    max_metadata_size: u32,
) -> Result<Vec<u8>, MetadataFetchError> {
    let addr = std::net::SocketAddr::V4(peer.addr);
    let stream = timeout(peer_timeout, TcpStream::connect(addr))
        .await
        .map_err(|_| MetadataFetchError::Timeout {
            stage: TimeoutStage::Connect,
        })?
        .map_err(|error| MetadataFetchError::Connect(format!("connect error: {}", error)))?;

    fetch_from_stream_with_limit(info_hash, stream, peer_timeout, max_metadata_size)
        .await
        .map_err(MetadataFetchError::from_protocol_message)
}

#[cfg_attr(not(test), allow(dead_code))]
async fn fetch_from_stream<S>(
    info_hash: &InfoHash,
    stream: S,
    peer_timeout: Duration,
) -> Result<Vec<u8>, String>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    fetch_from_stream_with_limit(info_hash, stream, peer_timeout, 8 * 1024 * 1024).await
}

async fn fetch_from_stream_with_limit<S>(
    info_hash: &InfoHash,
    mut stream: S,
    peer_timeout: Duration,
    max_metadata_size: u32,
) -> Result<Vec<u8>, String>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let our_peer_id = crate::types::random_node_id();

    let hs = Handshake::new(*info_hash, our_peer_id);
    write_all_timeout(&mut stream, &hs.to_bytes(), peer_timeout, "handshake").await?;

    let mut handshake_buf = vec![0u8; HANDSHAKE_LEN];
    timeout(peer_timeout, stream.read_exact(&mut handshake_buf))
        .await
        .map_err(|_| "handshake read timeout".to_string())?
        .map_err(|e| format!("read handshake: {}", e))?;

    let hs_resp = Handshake::from_bytes(&handshake_buf)
        .ok_or_else(|| "invalid handshake response".to_string())?;

    if hs_resp.info_hash != *info_hash {
        return Err("handshake info_hash mismatch".to_string());
    }

    if !hs_resp.supports_extensions() {
        return Err("peer doesn't support extensions".to_string());
    }

    let mut m = std::collections::BTreeMap::new();
    m.insert("ut_metadata".to_string(), 1u32);

    let ext_hs = ExtendedHandshake {
        m,
        metadata_size: None,
        v: Some("btfind 0.1".to_string()),
        your_ip: None,
        reqq: None,
    };

    let ext_hs_bytes =
        serde_bencode::to_bytes(&serde_bencode::value::Value::Dict(ext_hs.to_dict()))
            .map_err(|e| format!("encode ext hs: {}", e))?;

    let ext_msg = WireMessage::Extended {
        id: 0,
        payload: ext_hs_bytes,
    };

    write_all_timeout(
        &mut stream,
        &ext_msg.to_bytes(),
        peer_timeout,
        "extended handshake",
    )
    .await?;

    let peer_ext_hs = read_extended_handshake(&mut stream, peer_timeout).await?;

    let ut_metadata_id = peer_ext_hs
        .m
        .get("ut_metadata")
        .copied()
        .ok_or_else(|| "peer doesn't support ut_metadata".to_string())?;

    if ut_metadata_id == 0 || ut_metadata_id > 255 {
        return Err(format!("invalid ut_metadata id: {}", ut_metadata_id));
    }
    let ut_metadata_id = ut_metadata_id as u8;

    let metadata_size = peer_ext_hs
        .metadata_size
        .ok_or_else(|| "peer didn't advertise metadata_size".to_string())?;

    if metadata_size == 0 {
        return Err("peer advertised metadata_size = 0".to_string());
    }

    if metadata_size > max_metadata_size {
        return Err("metadata too large".to_string());
    }

    let total_pieces = metadata_piece_count(metadata_size);
    let mut pieces: Vec<Option<Vec<u8>>> = vec![None; total_pieces as usize];
    let mut received = 0u32;

    let max_outstanding = peer_ext_hs.reqq.unwrap_or(1).clamp(1, 64) as usize;
    let mut next_to_request = 0u32;
    let mut in_flight = 0u32;
    let mut requested: std::collections::HashSet<u32> = std::collections::HashSet::new();

    while next_to_request < total_pieces && in_flight < max_outstanding as u32 {
        let req = wire::build_metadata_request(next_to_request);
        let req_msg = WireMessage::Extended {
            id: ut_metadata_id,
            payload: req,
        };
        write_all_timeout(
            &mut stream,
            &req_msg.to_bytes(),
            peer_timeout,
            "metadata request",
        )
        .await?;
        requested.insert(next_to_request);
        next_to_request += 1;
        in_flight += 1;
    }

    let deadline = peer_timeout * 2;
    let start = std::time::Instant::now();
    let mut buf = Vec::new();

    while received < total_pieces {
        let remaining = deadline.saturating_sub(start.elapsed());
        if remaining.is_zero() {
            return Err("metadata receive timeout".to_string());
        }

        match read_next_frame(&mut stream, &mut buf, remaining).await? {
            Some(WireMessage::Extended { id, payload }) if id == ut_metadata_id => {
                match wire::parse_metadata_response(&payload) {
                    Some(Ok((piece_idx, total_size, data))) => {
                        if requested.remove(&piece_idx) {
                            in_flight -= 1;
                        }
                        if total_size != metadata_size {
                            return Err("metadata total_size mismatch".to_string());
                        }
                        if piece_idx >= total_pieces {
                            return Err("metadata piece out of range".to_string());
                        }
                        let expected_len =
                            metadata_piece_len(metadata_size, total_pieces, piece_idx);
                        if data.len() != expected_len {
                            return Err(format!(
                                "invalid metadata piece length: got {}, expected {}",
                                data.len(),
                                expected_len
                            ));
                        }
                        if pieces[piece_idx as usize].is_none() {
                            pieces[piece_idx as usize] = Some(data);
                            received += 1;
                        }
                    }
                    Some(Err(())) => {
                        return Err("peer rejected metadata request".to_string());
                    }
                    None => {}
                }
            }
            Some(_) => {}
            None => break,
        }

        while next_to_request < total_pieces && in_flight < max_outstanding as u32 {
            let req = wire::build_metadata_request(next_to_request);
            let req_msg = WireMessage::Extended {
                id: ut_metadata_id,
                payload: req,
            };
            write_all_timeout(
                &mut stream,
                &req_msg.to_bytes(),
                peer_timeout,
                "metadata request",
            )
            .await?;
            requested.insert(next_to_request);
            next_to_request += 1;
            in_flight += 1;
        }
    }

    let mut all_data = Vec::with_capacity(metadata_size as usize);
    for piece in &pieces {
        match piece {
            Some(data) => all_data.extend_from_slice(data),
            None => return Err("missing metadata piece".to_string()),
        }
    }

    if all_data.is_empty() {
        return Err("empty metadata".to_string());
    }

    if all_data.len() != metadata_size as usize {
        return Err(format!(
            "metadata size mismatch: got {}, expected {}",
            all_data.len(),
            metadata_size
        ));
    }

    Ok(all_data)
}

async fn write_all_timeout<S>(
    stream: &mut S,
    bytes: &[u8],
    timeout_dur: Duration,
    stage: &str,
) -> Result<(), String>
where
    S: AsyncWrite + Unpin,
{
    timeout(timeout_dur, stream.write_all(bytes))
        .await
        .map_err(|_| format!("{} write timeout", stage))?
        .map_err(|error| format!("write {}: {}", stage, error))
}

async fn read_extended_handshake<S>(
    stream: &mut S,
    peer_timeout: Duration,
) -> Result<ExtendedHandshake, String>
where
    S: AsyncRead + Unpin,
{
    let deadline = peer_timeout;
    let start = std::time::Instant::now();
    let mut buf = Vec::new();

    loop {
        let remaining = deadline.saturating_sub(start.elapsed());
        if remaining.is_zero() {
            return Err("extended handshake read timeout".to_string());
        }

        match read_next_frame(stream, &mut buf, remaining).await? {
            Some(WireMessage::Extended { id: 0, payload }) => {
                return ExtendedHandshake::from_bytes(&payload)
                    .ok_or_else(|| "invalid extended handshake".to_string());
            }
            Some(_) => continue,
            None => return Err("connection closed before extended handshake".to_string()),
        }
    }
}

async fn read_frame_header<S>(stream: &mut S, timeout_dur: Duration) -> Result<u32, String>
where
    S: AsyncRead + Unpin,
{
    let mut header = [0u8; 4];
    timeout(timeout_dur, stream.read_exact(&mut header))
        .await
        .map_err(|_| "frame header read timeout".to_string())?
        .map_err(|e| format!("read frame header: {}", e))?;
    Ok(u32::from_be_bytes(header))
}

async fn read_next_frame<S>(
    stream: &mut S,
    buf: &mut Vec<u8>,
    timeout_dur: Duration,
) -> Result<Option<WireMessage>, String>
where
    S: AsyncRead + Unpin,
{
    let len = match read_frame_header(stream, timeout_dur).await {
        Ok(l) => l,
        Err(e) if e.contains("timeout") => return Err(e),
        Err(_) => return Ok(None),
    };

    if len == 0 {
        return Ok(Some(WireMessage::KeepAlive));
    }

    if len > 262_144 {
        return Err(format!("frame too large: {} bytes", len));
    }

    buf.resize(len as usize, 0);
    match timeout(timeout_dur, stream.read_exact(buf)).await {
        Ok(Ok(_)) => {}
        Ok(Err(e)) => return Err(format!("read frame body: {}", e)),
        Err(_) => return Err("frame body read timeout".to_string()),
    }

    Ok(WireMessage::from_bytes_frame(len, buf))
}

#[derive(Debug)]
pub struct TorrentInfo {
    pub name: String,
    pub piece_length: i64,
    pub total_size: i64,
    pub file_count: i64,
    pub files: Vec<(String, i64)>,
}

const MAX_FILE_COUNT: usize = 100_000;
const MAX_PATH_COMPONENTS: usize = 64;
const MAX_PATH_COMPONENT_BYTES: usize = 1024;
const MAX_NAME_BYTES: usize = 4096;
const MAX_PIECE_LENGTH: i64 = 64 * 1024 * 1024;

pub fn verify_metadata_hash(info_hash: &InfoHash, data: &[u8]) -> bool {
    let mut hasher = Sha1::new();
    hasher.update(data);
    let digest: [u8; 20] = hasher.finalize().into();
    &digest == info_hash
}

pub fn parse_torrent_info(data: &[u8]) -> Result<TorrentInfo, String> {
    let value: serde_bencode::value::Value =
        serde_bencode::from_bytes(data).map_err(|e| format!("bencode error: {}", e))?;

    let dict = match value {
        serde_bencode::value::Value::Dict(d) => d,
        _ => return Err("not a dict".to_string()),
    };

    let name = get_bencode_preferred_str(&dict, "name.utf-8", "name")
        .unwrap_or_else(|| "unknown".to_string());
    if name.len() > MAX_NAME_BYTES {
        return Err("torrent name exceeds limit".to_string());
    }
    let piece_length =
        get_bencode_int(&dict, "piece length").ok_or_else(|| "missing piece length".to_string())?;
    if !(1..=MAX_PIECE_LENGTH).contains(&piece_length) {
        return Err("invalid piece length".to_string());
    }

    if get_bencode_int(&dict, "meta version").is_some_and(|version| version != 2) {
        return Err("unsupported torrent meta version".to_string());
    }
    if get_bencode_int(&dict, "meta version") == Some(2) && !dict.contains_key(b"pieces".as_slice())
    {
        return Err("pure v2 torrents are unsupported".to_string());
    }

    let (total_size, files) =
        if let Some(serde_bencode::value::Value::List(file_list)) = dict.get(b"files".as_slice()) {
            let mut files_out = Vec::new();
            let mut total = 0i64;
            if file_list.is_empty() || file_list.len() > MAX_FILE_COUNT {
                return Err("invalid file count".to_string());
            }
            for file_val in file_list {
                let serde_bencode::value::Value::Dict(file_dict) = file_val else {
                    return Err("invalid file entry".to_string());
                };
                let file_size = get_bencode_int(file_dict, "length")
                    .ok_or_else(|| "missing file length".to_string())?;
                if file_size < 0 {
                    return Err("negative file length".to_string());
                }
                total = total
                    .checked_add(file_size)
                    .ok_or_else(|| "torrent total size overflow".to_string())?;

                let path_value = file_dict
                    .get(b"path.utf-8".as_slice())
                    .or_else(|| file_dict.get(b"path".as_slice()));
                let Some(serde_bencode::value::Value::List(parts)) = path_value else {
                    return Err("missing file path".to_string());
                };
                if parts.is_empty() || parts.len() > MAX_PATH_COMPONENTS {
                    return Err("invalid file path component count".to_string());
                }
                let mut path_parts = Vec::with_capacity(parts.len());
                for part in parts {
                    let serde_bencode::value::Value::Bytes(bytes) = part else {
                        return Err("invalid file path component".to_string());
                    };
                    if bytes.is_empty() || bytes.len() > MAX_PATH_COMPONENT_BYTES {
                        return Err("invalid file path component length".to_string());
                    }
                    let component = String::from_utf8_lossy(bytes).to_string();
                    if component == "." || component == ".." || component.contains('/') {
                        return Err("invalid file path component".to_string());
                    }
                    path_parts.push(component);
                }
                files_out.push((path_parts.join("/"), file_size));
            }
            (total, files_out)
        } else if let Some(length) = get_bencode_int(&dict, "length") {
            if length < 0 {
                return Err("negative torrent length".to_string());
            }
            (length, vec![(name.clone(), length)])
        } else {
            return Err("no files or length in info dict".to_string());
        };

    Ok(TorrentInfo {
        name,
        piece_length,
        total_size,
        file_count: files.len() as i64,
        files,
    })
}

fn get_bencode_preferred_str(
    dict: &std::collections::HashMap<Vec<u8>, serde_bencode::value::Value>,
    preferred: &str,
    fallback: &str,
) -> Option<String> {
    get_bencode_str(dict, preferred).or_else(|| get_bencode_str(dict, fallback))
}

fn get_bencode_str(
    dict: &std::collections::HashMap<Vec<u8>, serde_bencode::value::Value>,
    key: &str,
) -> Option<String> {
    match dict.get(key.as_bytes())? {
        serde_bencode::value::Value::Bytes(b) => Some(String::from_utf8_lossy(b).to_string()),
        _ => None,
    }
}

fn get_bencode_int(
    dict: &std::collections::HashMap<Vec<u8>, serde_bencode::value::Value>,
    key: &str,
) -> Option<i64> {
    match dict.get(key.as_bytes())? {
        serde_bencode::value::Value::Int(i) => Some(*i),
        _ => None,
    }
}

struct PeerRetryFilter {
    eligible: Vec<PeerContact>,
    next_eligible_at: Option<i64>,
}

fn filter_peers_for_retry(
    store: &Store,
    info_hash: &InfoHash,
    peers: &[PeerContact],
    retry_after_hours: u32,
) -> PeerRetryFilter {
    let mut eligible = Vec::new();
    let mut next_eligible_at: Option<i64> = None;
    for peer in peers {
        let peer_addr = peer.addr.to_string();
        match store.peer_retry_eligibility(info_hash, &peer_addr, retry_after_hours) {
            Ok(PeerRetryEligibility::Eligible) => eligible.push(peer.clone()),
            Ok(PeerRetryEligibility::RetryAt(retry_at)) => {
                next_eligible_at =
                    Some(next_eligible_at.map_or(retry_at, |current| current.min(retry_at)));
                tracing::debug!(
                    "delaying metadata fetch for {} from {} until {}",
                    hex::encode(info_hash),
                    peer_addr,
                    retry_at
                );
            }
            Ok(PeerRetryEligibility::Rejected) => {
                tracing::debug!(
                    "rejecting metadata peer {} for {} after hash mismatch",
                    peer_addr,
                    hex::encode(info_hash),
                );
            }
            Err(error) => {
                tracing::warn!(
                    "peer retry eligibility database error for {} from {}: {}, delaying",
                    hex::encode(info_hash),
                    peer_addr,
                    error
                );
                let retry_at = chrono::Utc::now().timestamp().saturating_add(300);
                next_eligible_at =
                    Some(next_eligible_at.map_or(retry_at, |current| current.min(retry_at)));
            }
        }
    }
    PeerRetryFilter {
        eligible,
        next_eligible_at,
    }
}

fn emit_metadata_failure(stats_tx: &mpsc::Sender<CrawlStatsEvent>, reason: MetadataFailureReason) {
    if stats_tx
        .try_send(CrawlStatsEvent::MetadataFetchFailed { reason })
        .is_err()
    {
        tracing::warn!("stats_tx send failed after metadata fetch failure");
    }
}

fn log_storage_result(
    operation: &str,
    result: Result<Result<(), rusqlite::Error>, tokio::task::JoinError>,
) {
    match result {
        Ok(Ok(())) => {}
        Ok(Err(error)) => tracing::warn!("{} failed: {}", operation, error),
        Err(error) => tracing::warn!("{} worker failed: {}", operation, error),
    }
}

struct HashJob {
    peers: VecDeque<PeerContact>,
    peer_set: HashSet<std::net::SocketAddrV4>,
    running: bool,
    attempt_count: u32,
}

impl HashJob {
    fn new(attempt_count: u32) -> Self {
        Self {
            peers: VecDeque::new(),
            peer_set: HashSet::new(),
            running: false,
            attempt_count,
        }
    }

    fn merge_peers(&mut self, peers: Vec<PeerContact>, limit: usize) -> usize {
        let mut added = 0;
        for peer in peers {
            if self.peers.len() >= limit {
                break;
            }
            if self.peer_set.insert(peer.addr) {
                self.peers.push_back(peer);
                added += 1;
            }
        }
        added
    }

    fn take_round(&mut self, limit: usize) -> Vec<PeerContact> {
        let mut peers = Vec::new();
        while peers.len() < limit {
            let Some(peer) = self.peers.pop_front() else {
                break;
            };
            self.peer_set.remove(&peer.addr);
            peers.push(peer);
        }
        peers
    }
}

struct JobOutcome {
    info_hash: InfoHash,
    attempts: u32,
    complete: bool,
    terminal: bool,
    parked: bool,
}

struct FetchRoundConfig {
    peer_timeout: Duration,
    retry_after_hours: u32,
    max_metadata_size: u32,
    prior_attempt_count: u32,
}

async fn process_hash_round(
    info_hash: InfoHash,
    peers: Vec<PeerContact>,
    store: Arc<Store>,
    stats_tx: mpsc::Sender<CrawlStatsEvent>,
    config: FetchRoundConfig,
) -> JobOutcome {
    let hash_hex = hex::encode(info_hash);
    let filter_store = store.clone();
    let filtered = tokio::task::spawn_blocking(move || {
        filter_peers_for_retry(&filter_store, &info_hash, &peers, config.retry_after_hours)
    })
    .await
    .unwrap_or(PeerRetryFilter {
        eligible: Vec::new(),
        next_eligible_at: Some(chrono::Utc::now().timestamp().saturating_add(300)),
    });
    let mut attempts = 0u32;
    let mut last_error = "no eligible peers".to_string();
    let mut long_backoff = false;
    let mut retryable_attempts = 0u32;

    for peer in filtered.eligible {
        attempts = attempts.saturating_add(1);
        let peer_addr = peer.addr.to_string();
        let attempt_store = store.clone();
        let attempt_hash = info_hash;
        let attempt_addr = peer_addr.clone();
        if tokio::task::spawn_blocking(move || {
            attempt_store.set_peer_attempt(&attempt_hash, &attempt_addr, None)
        })
        .await
        .is_err()
        {
            last_error = "storage worker failed to record peer attempt".to_string();
            retryable_attempts = retryable_attempts.saturating_add(1);
            continue;
        }

        match fetch_from_peer_with_limit(
            &info_hash,
            &peer,
            config.peer_timeout,
            config.max_metadata_size,
        )
        .await
        {
            Ok(bytes) if !verify_metadata_hash(&info_hash, &bytes) => {
                last_error = "metadata hash mismatch".to_string();
                let failure_store = store.clone();
                let failure_hash = info_hash;
                let failure_addr = peer_addr.clone();
                let recorded = tokio::task::spawn_blocking(move || {
                    failure_store.set_peer_attempt(
                        &failure_hash,
                        &failure_addr,
                        Some("metadata hash mismatch"),
                    )
                })
                .await;
                log_storage_result("record metadata hash mismatch", recorded);
                emit_metadata_failure(&stats_tx, MetadataFailureReason::HashMismatch);
            }
            Ok(bytes) => match parse_torrent_info(&bytes) {
                Ok(info) => {
                    tracing::info!(
                        "validated metadata for {} with {} files",
                        hash_hex,
                        info.file_count
                    );
                    let commit_store = store.clone();
                    let committed = tokio::task::spawn_blocking(move || {
                        commit_store.commit_metadata(
                            &info_hash,
                            &info.name,
                            info.piece_length,
                            info.total_size,
                            &info.files,
                        )
                    })
                    .await;
                    match committed {
                        Ok(Ok(())) => {
                            if stats_tx.try_send(CrawlStatsEvent::MetadataFetched).is_err() {
                                tracing::warn!("metadata completion statistic was not accepted");
                            }
                            return JobOutcome {
                                info_hash,
                                attempts,
                                complete: true,
                                terminal: false,
                                parked: false,
                            };
                        }
                        Ok(Err(error)) => {
                            last_error = format!("storage: {}", error);
                            retryable_attempts = retryable_attempts.saturating_add(1);
                        }
                        Err(error) => {
                            last_error = format!("storage worker: {}", error);
                            retryable_attempts = retryable_attempts.saturating_add(1);
                        }
                    }
                }
                Err(error) => {
                    let unsupported = error.contains("unsupported");
                    let status = if unsupported {
                        "unsupported"
                    } else {
                        "invalid"
                    };
                    let terminal_store = store.clone();
                    let terminal_error = error.clone();
                    let recorded = tokio::task::spawn_blocking(move || {
                        terminal_store.update_hash_job_failure(
                            &info_hash,
                            status,
                            config.prior_attempt_count.saturating_add(attempts),
                            0,
                            &terminal_error,
                        )
                    })
                    .await;
                    log_storage_result("record terminal metadata state", recorded);
                    emit_metadata_failure(&stats_tx, MetadataFailureReason::Parse);
                    return JobOutcome {
                        info_hash,
                        attempts,
                        complete: false,
                        terminal: true,
                        parked: false,
                    };
                }
            },
            Err(error) => {
                retryable_attempts = retryable_attempts.saturating_add(1);
                long_backoff |= error.long_backoff();
                last_error = error.to_string();
                let reason = error.reason();
                let failure_store = store.clone();
                let failure_hash = info_hash;
                let failure_addr = peer_addr.clone();
                let failure_text = last_error.clone();
                let recorded = tokio::task::spawn_blocking(move || {
                    failure_store.set_peer_attempt(
                        &failure_hash,
                        &failure_addr,
                        Some(&failure_text),
                    )
                })
                .await;
                log_storage_result("record metadata peer failure", recorded);
                emit_metadata_failure(&stats_tx, reason);
            }
        }
    }

    let attempt_count = config.prior_attempt_count.saturating_add(attempts);
    let now = chrono::Utc::now().timestamp();
    let mut next_peer_eligibility = filtered.next_eligible_at;
    if retryable_attempts > 0 && config.retry_after_hours > 0 {
        let retry_at = now.saturating_add(i64::from(config.retry_after_hours) * 3600);
        next_peer_eligibility =
            Some(next_peer_eligibility.map_or(retry_at, |current| current.min(retry_at)));
    }
    let has_retryable_peer = next_peer_eligibility.is_some()
        || (retryable_attempts > 0 && config.retry_after_hours == 0);
    if !has_retryable_peer {
        let retry_store = store.clone();
        let retry_error = last_error.clone();
        let recorded = tokio::task::spawn_blocking(move || {
            retry_store.update_hash_job_failure(
                &info_hash,
                "waiting_peers",
                attempt_count,
                0,
                &retry_error,
            )
        })
        .await;
        log_storage_result("park metadata job", recorded);
        return JobOutcome {
            info_hash,
            attempts,
            complete: false,
            terminal: false,
            parked: true,
        };
    }
    let exponent = attempt_count.min(10);
    let base = if long_backoff { 900u64 } else { 30u64 };
    let delay = base.saturating_mul(1u64 << exponent).min(24 * 60 * 60);
    let jitter = rand::thread_rng().gen_range(0..=(delay / 4).max(1));
    let backoff_at =
        now.saturating_add(i64::try_from(delay.saturating_add(jitter)).unwrap_or(i64::MAX));
    let next_attempt_at =
        next_peer_eligibility.map_or(backoff_at, |peer_retry_at| backoff_at.max(peer_retry_at));
    let retry_store = store.clone();
    let retry_error = last_error.clone();
    let recorded = tokio::task::spawn_blocking(move || {
        retry_store.update_hash_job_failure(
            &info_hash,
            "retry_at",
            attempt_count,
            next_attempt_at,
            &retry_error,
        )
    })
    .await;
    log_storage_result("schedule metadata retry", recorded);
    tracing::debug!("metadata round failed for {}: {}", hash_hex, last_error);
    JobOutcome {
        info_hash,
        attempts,
        complete: false,
        terminal: false,
        parked: false,
    }
}

#[cfg(test)]
fn mark_metadata_complete_and_emit(
    store: &Store,
    info_hash: &InfoHash,
    info: &TorrentInfo,
    stats_tx: &mpsc::Sender<CrawlStatsEvent>,
) -> Result<(), rusqlite::Error> {
    store.commit_metadata(
        info_hash,
        &info.name,
        info.piece_length,
        info.total_size,
        &info.files,
    )?;
    if stats_tx.try_send(CrawlStatsEvent::MetadataFetched).is_err() {
        tracing::warn!("stats_tx send failed after metadata fetch");
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub async fn run_metadata_fetcher(
    mut info_hash_rx: mpsc::Receiver<HashDiscovery>,
    store: Arc<Store>,
    max_concurrent: usize,
    peer_timeout_secs: u64,
    retry_after_hours: u32,
    max_metadata_size: u32,
    max_peer_attempts_per_round: usize,
    max_active_hash_jobs: usize,
    max_peers_per_hash: usize,
    stats_tx: mpsc::Sender<CrawlStatsEvent>,
) {
    let mut jobs: HashMap<InfoHash, HashJob> = HashMap::new();
    let mut ready = VecDeque::new();
    let mut ready_set = HashSet::new();
    let mut workers: JoinSet<JobOutcome> = JoinSet::new();
    let mut retry_tick = tokio::time::interval(Duration::from_secs(30));

    loop {
        while workers.len() < max_concurrent {
            let Some(info_hash) = ready.pop_front() else {
                break;
            };
            ready_set.remove(&info_hash);
            let Some(job) = jobs.get_mut(&info_hash) else {
                continue;
            };
            if job.running {
                continue;
            }
            let peers = job.take_round(max_peer_attempts_per_round);
            if peers.is_empty() {
                continue;
            }
            job.running = true;
            let round_store = store.clone();
            let round_stats = stats_tx.clone();
            let round_config = FetchRoundConfig {
                peer_timeout: Duration::from_secs(peer_timeout_secs),
                retry_after_hours,
                max_metadata_size,
                prior_attempt_count: job.attempt_count,
            };
            workers.spawn(process_hash_round(
                info_hash,
                peers,
                round_store,
                round_stats,
                round_config,
            ));
        }

        tokio::select! {
            message = info_hash_rx.recv() => {
                let Some(discovery) = message else {
                    break;
                };
                let info_hash = discovery.info_hash;
                let peers = discovery.peers;
                let has_peers = !peers.is_empty();
                let source = discovery.source;
                let persist_store = store.clone();
                let durable_peers = peers.clone();
                let persistence = tokio::task::spawn_blocking(move || {
                    persist_store.persist_hash_discovery(
                        &info_hash,
                        source.as_str(),
                        has_peers,
                    )?;
                    persist_store.persist_hash_job_peers(
                        &info_hash,
                        &durable_peers,
                        max_peers_per_hash,
                    )
                }).await;
                if !matches!(persistence, Ok(Ok(()))) {
                    tracing::warn!("failed to persist discovery for {}", hex::encode(info_hash));
                }
                if !jobs.contains_key(&info_hash) && jobs.len() >= max_active_hash_jobs {
                    if stats_tx
                        .try_send(CrawlStatsEvent::DiscoveryBackpressure)
                        .is_err()
                    {
                        tracing::warn!("metadata backpressure statistic was not accepted");
                    }
                    continue;
                }
                let job = jobs.entry(info_hash).or_insert_with(|| HashJob::new(0));
                let added = job.merge_peers(peers, max_peers_per_hash);
                if added > 0 && !job.running && ready_set.insert(info_hash) {
                    ready.push_back(info_hash);
                }
            }
            _ = retry_tick.tick() => {
                let due_store = store.clone();
                let due = tokio::task::spawn_blocking(move || {
                    let records = due_store.due_hash_jobs(max_active_hash_jobs)?;
                    let mut loaded = Vec::with_capacity(records.len());
                    for record in records {
                        let peers = due_store.hash_job_peers(&record.info_hash, max_peers_per_hash)?;
                        loaded.push((record, peers));
                    }
                    Ok::<_, rusqlite::Error>(loaded)
                }).await;
                if let Ok(Ok(records)) = due {
                    for (record, peers) in records {
                        if jobs.len() >= max_active_hash_jobs && !jobs.contains_key(&record.info_hash) {
                            break;
                        }
                        tracing::debug!(
                            "restoring {} metadata job {} after {:?}",
                            record.status,
                            hex::encode(record.info_hash),
                            (record.next_attempt_at, record.last_failure)
                        );
                        let job = jobs
                            .entry(record.info_hash)
                            .or_insert_with(|| HashJob::new(record.attempt_count));
                        job.attempt_count = job.attempt_count.max(record.attempt_count);
                        let added = job.merge_peers(peers, max_peers_per_hash);
                        if added > 0 && !job.running && ready_set.insert(record.info_hash) {
                            ready.push_back(record.info_hash);
                        }
                    }
                }
            }
            result = workers.join_next(), if !workers.is_empty() => {
                match result {
                    Some(Ok(outcome)) => {
                        if outcome.complete || outcome.terminal || outcome.parked {
                            jobs.remove(&outcome.info_hash);
                            ready_set.remove(&outcome.info_hash);
                        } else if let Some(job) = jobs.get_mut(&outcome.info_hash) {
                            job.running = false;
                            job.attempt_count = job.attempt_count.saturating_add(outcome.attempts);
                            if !job.peers.is_empty() && ready_set.insert(outcome.info_hash) {
                                ready.push_back(outcome.info_hash);
                            }
                        }
                    }
                    Some(Err(error)) => tracing::warn!("metadata worker join error: {}", error),
                    None => {}
                }
            }
        }
    }

    while let Some(result) = workers.join_next().await {
        if let Err(e) = result {
            tracing::warn!("metadata worker join error: {}", e);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metadata_piece_count() {
        assert_eq!(metadata_piece_count(2048), 1);
        assert_eq!(metadata_piece_count(20000), 2);
    }

    #[test]
    fn test_verify_metadata_hash_match() {
        let info_dict = b"d6:lengthi42e4:name8:test.txte";
        let expected_hash: InfoHash = {
            let mut hasher = Sha1::new();
            hasher.update(info_dict);
            hasher.finalize().into()
        };
        assert!(verify_metadata_hash(&expected_hash, info_dict));
    }

    #[test]
    fn test_verify_metadata_hash_mismatch() {
        let info_dict = b"d6:lengthi42e4:name8:test.txte";
        let wrong_hash: InfoHash = [0xABu8; 20];
        assert!(!verify_metadata_hash(&wrong_hash, info_dict));
    }

    #[tokio::test]
    async fn durable_retry_waits_until_peer_is_eligible() {
        let store = Arc::new(Store::open_in_memory().unwrap());
        let info_hash = [0xACu8; 20];
        let peer = PeerContact {
            addr: "8.8.8.8:6881".parse().unwrap(),
        };
        store
            .persist_hash_discovery(&info_hash, "test", true)
            .unwrap();
        store
            .persist_hash_job_peers(&info_hash, std::slice::from_ref(&peer), 8)
            .unwrap();
        store
            .set_peer_attempt(&info_hash, &peer.addr.to_string(), Some("timeout"))
            .unwrap();
        let earliest_expected = chrono::Utc::now().timestamp() + 23 * 3600;
        let (stats_tx, _stats_rx) = mpsc::channel(4);

        let outcome = process_hash_round(
            info_hash,
            vec![peer],
            store.clone(),
            stats_tx,
            FetchRoundConfig {
                peer_timeout: Duration::from_millis(1),
                retry_after_hours: 24,
                max_metadata_size: 1024,
                prior_attempt_count: 0,
            },
        )
        .await;

        assert_eq!(outcome.attempts, 0);
        assert!(!outcome.parked);
        let (status, attempt_count, next_attempt_at) =
            store.hash_job_schedule_for_test(&info_hash).unwrap();
        assert_eq!(status, "retry_at");
        assert_eq!(attempt_count, 0);
        assert!(next_attempt_at >= earliest_expected);
    }

    #[tokio::test]
    async fn permanently_rejected_peer_parks_job_until_new_discovery() {
        let store = Arc::new(Store::open_in_memory().unwrap());
        let info_hash = [0xADu8; 20];
        let peer = PeerContact {
            addr: "8.8.4.4:6881".parse().unwrap(),
        };
        store
            .persist_hash_discovery(&info_hash, "test", true)
            .unwrap();
        store
            .set_peer_attempt(
                &info_hash,
                &peer.addr.to_string(),
                Some("metadata hash mismatch"),
            )
            .unwrap();
        let (stats_tx, _stats_rx) = mpsc::channel(4);

        let outcome = process_hash_round(
            info_hash,
            vec![peer],
            store.clone(),
            stats_tx,
            FetchRoundConfig {
                peer_timeout: Duration::from_millis(1),
                retry_after_hours: 24,
                max_metadata_size: 1024,
                prior_attempt_count: 1,
            },
        )
        .await;

        assert_eq!(outcome.attempts, 0);
        assert!(outcome.parked);
        let (status, attempt_count, next_attempt_at) =
            store.hash_job_schedule_for_test(&info_hash).unwrap();
        assert_eq!(status, "waiting_peers");
        assert_eq!(attempt_count, 1);
        assert_eq!(next_attempt_at, 0);

        store
            .persist_hash_discovery(&info_hash, "sample_infohashes", false)
            .unwrap();
        let (status, _, _) = store.hash_job_schedule_for_test(&info_hash).unwrap();
        assert_eq!(status, "waiting_peers");

        store
            .persist_hash_discovery(&info_hash, "announce_peer", true)
            .unwrap();
        let (status, _, _) = store.hash_job_schedule_for_test(&info_hash).unwrap();
        assert_eq!(status, "queued");
    }

    #[tokio::test]
    async fn test_fetch_from_stream_success() {
        let info_dict = b"d6:lengthi42e4:name8:test.txt12:piece lengthi16384ee";
        let info_hash: InfoHash = {
            let mut hasher = Sha1::new();
            hasher.update(info_dict);
            hasher.finalize().into()
        };

        let (client_stream, mut server_stream) = tokio::io::duplex(64 * 1024);
        let server_info_hash = info_hash;

        let _server = tokio::spawn(async move {
            // read handshake
            let mut buf = vec![0u8; HANDSHAKE_LEN];
            server_stream.read_exact(&mut buf).await.unwrap();
            let _client_hs = Handshake::from_bytes(&buf).unwrap();

            // respond with valid handshake
            let peer_id = crate::types::random_node_id();
            let mut hs = Handshake::new(server_info_hash, peer_id);
            hs.reserved[5] |= 0x10;
            server_stream.write_all(&hs.to_bytes()).await.unwrap();

            // read extended handshake
            let mut len_buf = [0u8; 4];
            server_stream.read_exact(&mut len_buf).await.unwrap();
            let len = u32::from_be_bytes(len_buf) as usize;
            let mut body = vec![0u8; len];
            server_stream.read_exact(&mut body).await.unwrap();

            // send extended handshake with ut_metadata
            let mut m = std::collections::BTreeMap::new();
            m.insert("ut_metadata".to_string(), 1u32);
            let ext_hs = ExtendedHandshake {
                m,
                metadata_size: Some(info_dict.len() as u32),
                v: None,
                your_ip: None,
                reqq: None,
            };
            let ext_bytes =
                serde_bencode::to_bytes(&serde_bencode::value::Value::Dict(ext_hs.to_dict()))
                    .unwrap();
            let ext_msg = WireMessage::Extended {
                id: 0,
                payload: ext_bytes,
            };
            server_stream.write_all(&ext_msg.to_bytes()).await.unwrap();

            // read metadata request
            let mut len_buf = [0u8; 4];
            server_stream.read_exact(&mut len_buf).await.unwrap();
            let len = u32::from_be_bytes(len_buf) as usize;
            let mut body = vec![0u8; len];
            server_stream.read_exact(&mut body).await.unwrap();

            // send metadata piece with trailing data
            let mut dict = std::collections::HashMap::new();
            dict.insert(
                b"msg_type".to_vec(),
                serde_bencode::value::Value::Int(wire::UT_METADATA_DATA as i64),
            );
            dict.insert(b"piece".to_vec(), serde_bencode::value::Value::Int(0));
            dict.insert(
                b"total_size".to_vec(),
                serde_bencode::value::Value::Int(info_dict.len() as i64),
            );
            let mut payload =
                serde_bencode::to_bytes(&serde_bencode::value::Value::Dict(dict)).unwrap();
            payload.extend_from_slice(info_dict);

            let data_msg = WireMessage::Extended { id: 1, payload };
            server_stream.write_all(&data_msg.to_bytes()).await.unwrap();
        });

        let result = fetch_from_stream(&info_hash, client_stream, Duration::from_secs(5)).await;
        assert!(
            result.is_ok(),
            "fetch_from_stream failed: {:?}",
            result.err()
        );
        let data = result.unwrap();
        assert_eq!(data, info_dict);
        // Verify parse_torrent_info works on the raw info dict
        let info = parse_torrent_info(&data).unwrap();
        assert_eq!(info.name, "test.txt");
        assert_eq!(info.total_size, 42);
    }

    #[tokio::test]
    async fn test_fetch_from_stream_wrong_handshake_rejected() {
        let info_hash: InfoHash = [0xAAu8; 20];
        let wrong_hash: InfoHash = [0xBBu8; 20];

        let (client_stream, mut server_stream) = tokio::io::duplex(4096);

        let _server = tokio::spawn(async move {
            let mut buf = vec![0u8; HANDSHAKE_LEN];
            server_stream.read_exact(&mut buf).await.unwrap();

            // respond with WRONG info_hash
            let peer_id = crate::types::random_node_id();
            let mut hs = Handshake::new(wrong_hash, peer_id);
            hs.reserved[5] |= 0x10;
            server_stream.write_all(&hs.to_bytes()).await.unwrap();

            // keep connection alive briefly
            tokio::time::sleep(Duration::from_millis(100)).await;
        });

        let result = fetch_from_stream(&info_hash, client_stream, Duration::from_secs(5)).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("info_hash mismatch"));
    }

    async fn fetch_single_piece_from_fake_peer(
        info_dict: &'static [u8],
        advertised_size: u32,
        response_total_size: i64,
        piece_data: Vec<u8>,
    ) -> Result<Vec<u8>, String> {
        let info_hash: InfoHash = {
            let mut hasher = Sha1::new();
            hasher.update(info_dict);
            hasher.finalize().into()
        };

        let (client_stream, mut server_stream) = tokio::io::duplex(128 * 1024);
        let server_info_hash = info_hash;

        let _server = tokio::spawn(async move {
            let mut buf = vec![0u8; HANDSHAKE_LEN];
            server_stream.read_exact(&mut buf).await.unwrap();

            let peer_id = crate::types::random_node_id();
            let mut hs = Handshake::new(server_info_hash, peer_id);
            hs.reserved[5] |= 0x10;
            server_stream.write_all(&hs.to_bytes()).await.unwrap();

            let mut len_buf = [0u8; 4];
            server_stream.read_exact(&mut len_buf).await.unwrap();
            let len = u32::from_be_bytes(len_buf) as usize;
            let mut body = vec![0u8; len];
            server_stream.read_exact(&mut body).await.unwrap();

            let mut m = std::collections::BTreeMap::new();
            m.insert("ut_metadata".to_string(), 1u32);
            let ext_hs = ExtendedHandshake {
                m,
                metadata_size: Some(advertised_size),
                v: None,
                your_ip: None,
                reqq: None,
            };
            let ext_bytes =
                serde_bencode::to_bytes(&serde_bencode::value::Value::Dict(ext_hs.to_dict()))
                    .unwrap();
            server_stream
                .write_all(
                    &WireMessage::Extended {
                        id: 0,
                        payload: ext_bytes,
                    }
                    .to_bytes(),
                )
                .await
                .unwrap();

            let mut len_buf = [0u8; 4];
            server_stream.read_exact(&mut len_buf).await.unwrap();
            let len = u32::from_be_bytes(len_buf) as usize;
            let mut body = vec![0u8; len];
            server_stream.read_exact(&mut body).await.unwrap();

            let mut dict = std::collections::HashMap::new();
            dict.insert(
                b"msg_type".to_vec(),
                serde_bencode::value::Value::Int(wire::UT_METADATA_DATA as i64),
            );
            dict.insert(b"piece".to_vec(), serde_bencode::value::Value::Int(0));
            dict.insert(
                b"total_size".to_vec(),
                serde_bencode::value::Value::Int(response_total_size),
            );
            let mut payload =
                serde_bencode::to_bytes(&serde_bencode::value::Value::Dict(dict)).unwrap();
            payload.extend_from_slice(&piece_data);

            server_stream
                .write_all(&WireMessage::Extended { id: 1, payload }.to_bytes())
                .await
                .unwrap();
        });

        fetch_from_stream(&info_hash, client_stream, Duration::from_secs(5)).await
    }

    async fn send_metadata_piece<S>(stream: &mut S, piece_idx: u32, total_size: usize, data: &[u8])
    where
        S: AsyncWrite + Unpin,
    {
        let mut dict = std::collections::HashMap::new();
        dict.insert(
            b"msg_type".to_vec(),
            serde_bencode::value::Value::Int(wire::UT_METADATA_DATA as i64),
        );
        dict.insert(
            b"piece".to_vec(),
            serde_bencode::value::Value::Int(piece_idx as i64),
        );
        dict.insert(
            b"total_size".to_vec(),
            serde_bencode::value::Value::Int(total_size as i64),
        );
        let mut payload =
            serde_bencode::to_bytes(&serde_bencode::value::Value::Dict(dict)).unwrap();
        payload.extend_from_slice(data);
        stream
            .write_all(&WireMessage::Extended { id: 1, payload }.to_bytes())
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_fetch_from_stream_completes_after_duplicate_piece() {
        let mut metadata = vec![b'a'; 16_384];
        metadata.push(b'z');
        let info_hash: InfoHash = {
            let mut hasher = Sha1::new();
            hasher.update(&metadata);
            hasher.finalize().into()
        };
        let (client_stream, mut server_stream) = tokio::io::duplex(128 * 1024);
        let server_metadata = metadata.clone();

        let _server = tokio::spawn(async move {
            let mut buf = vec![0u8; HANDSHAKE_LEN];
            server_stream.read_exact(&mut buf).await.unwrap();

            let peer_id = crate::types::random_node_id();
            let mut hs = Handshake::new(info_hash, peer_id);
            hs.reserved[5] |= 0x10;
            server_stream.write_all(&hs.to_bytes()).await.unwrap();

            let mut len_buf = [0u8; 4];
            server_stream.read_exact(&mut len_buf).await.unwrap();
            let len = u32::from_be_bytes(len_buf) as usize;
            let mut body = vec![0u8; len];
            server_stream.read_exact(&mut body).await.unwrap();

            let mut m = std::collections::BTreeMap::new();
            m.insert("ut_metadata".to_string(), 1u32);
            let ext_hs = ExtendedHandshake {
                m,
                metadata_size: Some(server_metadata.len() as u32),
                v: None,
                your_ip: None,
                reqq: Some(1),
            };
            let ext_bytes =
                serde_bencode::to_bytes(&serde_bencode::value::Value::Dict(ext_hs.to_dict()))
                    .unwrap();
            server_stream
                .write_all(
                    &WireMessage::Extended {
                        id: 0,
                        payload: ext_bytes,
                    }
                    .to_bytes(),
                )
                .await
                .unwrap();

            let mut len_buf = [0u8; 4];
            server_stream.read_exact(&mut len_buf).await.unwrap();
            let len = u32::from_be_bytes(len_buf) as usize;
            let mut body = vec![0u8; len];
            server_stream.read_exact(&mut body).await.unwrap();

            send_metadata_piece(
                &mut server_stream,
                0,
                server_metadata.len(),
                &server_metadata[..16_384],
            )
            .await;
            send_metadata_piece(
                &mut server_stream,
                0,
                server_metadata.len(),
                &server_metadata[..16_384],
            )
            .await;

            let mut len_buf = [0u8; 4];
            server_stream.read_exact(&mut len_buf).await.unwrap();
            let len = u32::from_be_bytes(len_buf) as usize;
            let mut body = vec![0u8; len];
            server_stream.read_exact(&mut body).await.unwrap();

            send_metadata_piece(
                &mut server_stream,
                1,
                server_metadata.len(),
                &server_metadata[16_384..],
            )
            .await;
        });

        let result = fetch_from_stream(&info_hash, client_stream, Duration::from_secs(5))
            .await
            .unwrap();
        assert_eq!(result, metadata);
    }

    #[tokio::test]
    async fn test_fetch_rejects_oversized_single_piece() {
        let info_dict = b"d6:lengthi42e4:name8:test.txte";
        let result = fetch_single_piece_from_fake_peer(
            info_dict,
            info_dict.len() as u32,
            info_dict.len() as i64,
            vec![b'x'; 32 * 1024],
        )
        .await;

        assert!(result.is_err(), "oversized piece was accepted");
    }

    #[tokio::test]
    async fn test_fetch_rejects_short_metadata_body() {
        let info_dict = b"d6:lengthi42e4:name8:test.txte";
        let advertised_size = info_dict.len() as u32 + 1;
        let result = fetch_single_piece_from_fake_peer(
            info_dict,
            advertised_size,
            advertised_size as i64,
            info_dict.to_vec(),
        )
        .await;

        assert!(result.is_err(), "short metadata was accepted");
    }

    #[tokio::test]
    async fn test_fetch_rejects_response_total_size_mismatch() {
        let info_dict = b"d6:lengthi42e4:name8:test.txte";
        let result = fetch_single_piece_from_fake_peer(
            info_dict,
            info_dict.len() as u32,
            info_dict.len() as i64 + 1,
            info_dict.to_vec(),
        )
        .await;

        assert!(result.is_err(), "mismatched total_size was accepted");
    }

    #[test]
    fn test_filter_peers_for_retry_keeps_new_peers_for_seen_hash() {
        let store = Store::open_in_memory().unwrap();
        let info_hash = [0xDAu8; 20];
        let attempted_peer = PeerContact {
            addr: "1.1.1.1:6881".parse().unwrap(),
        };
        let new_peer = PeerContact {
            addr: "8.8.8.8:51413".parse().unwrap(),
        };
        store.upsert_torrent(info_hash, None, 0, 0, "dht").unwrap();
        store
            .set_peer_attempt(
                &info_hash,
                &attempted_peer.addr.to_string(),
                Some("timeout"),
            )
            .unwrap();

        let peers = filter_peers_for_retry(
            &store,
            &info_hash,
            &[attempted_peer.clone(), new_peer.clone()],
            24,
        );

        assert_eq!(peers.eligible, vec![new_peer]);
        assert!(peers.next_eligible_at.is_some());
    }

    #[test]
    fn test_filter_peers_for_retry_disabled_window_keeps_all_peers() {
        let store = Store::open_in_memory().unwrap();
        let info_hash = [0xDBu8; 20];
        let attempted_peer = PeerContact {
            addr: "1.1.1.1:6881".parse().unwrap(),
        };
        store.upsert_torrent(info_hash, None, 0, 0, "dht").unwrap();
        store
            .set_peer_attempt(
                &info_hash,
                &attempted_peer.addr.to_string(),
                Some("timeout"),
            )
            .unwrap();

        let peers =
            filter_peers_for_retry(&store, &info_hash, std::slice::from_ref(&attempted_peer), 0);

        assert_eq!(peers.eligible, vec![attempted_peer]);
        assert_eq!(peers.next_eligible_at, None);
    }

    #[test]
    fn test_classify_metadata_failure_reasons() {
        assert_eq!(
            classify_metadata_failure("connect error: connection refused"),
            crate::types::MetadataFailureReason::Connect
        );
        assert_eq!(
            classify_metadata_failure("metadata receive timeout"),
            crate::types::MetadataFailureReason::Timeout
        );
        assert_eq!(
            classify_metadata_failure("handshake info_hash mismatch"),
            crate::types::MetadataFailureReason::Handshake
        );
        assert_eq!(
            classify_metadata_failure("peer doesn't support ut_metadata"),
            crate::types::MetadataFailureReason::Extension
        );
        assert_eq!(
            classify_metadata_failure("peer rejected metadata request"),
            crate::types::MetadataFailureReason::Rejected
        );
        assert_eq!(
            classify_metadata_failure("invalid metadata piece length"),
            crate::types::MetadataFailureReason::Protocol
        );
    }

    #[test]
    fn test_mark_metadata_complete_emits_metadata_fetched_stat() {
        let store = Store::open_in_memory().unwrap();
        let info_hash = [0xABu8; 20];
        store.upsert_torrent(info_hash, None, 0, 0, "dht").unwrap();
        let info = TorrentInfo {
            name: "test".to_string(),
            piece_length: 16384,
            total_size: 42,
            file_count: 1,
            files: vec![("test".to_string(), 42)],
        };
        let (stats_tx, mut stats_rx) = mpsc::channel(8);

        mark_metadata_complete_and_emit(&store, &info_hash, &info, &stats_tx).unwrap();

        assert_eq!(
            stats_rx.try_recv().unwrap(),
            CrawlStatsEvent::MetadataFetched
        );
    }

    #[test]
    fn test_mark_metadata_complete_does_not_emit_when_file_insert_fails() {
        let store = Store::open_in_memory().unwrap();
        let info_hash = [0xACu8; 20];
        store.upsert_torrent(info_hash, None, 0, 0, "dht").unwrap();
        store
            .execute_batch_for_test(
                "
                CREATE TRIGGER fail_file_insert
                BEFORE INSERT ON files
                BEGIN
                    SELECT RAISE(ABORT, 'forced insert failure');
                END;
                ",
            )
            .unwrap();
        let info = TorrentInfo {
            name: "test".to_string(),
            piece_length: 16384,
            total_size: 42,
            file_count: 1,
            files: vec![("test".to_string(), 42)],
        };
        let (stats_tx, mut stats_rx) = mpsc::channel(8);

        let result = mark_metadata_complete_and_emit(&store, &info_hash, &info, &stats_tx);

        assert!(result.is_err());
        assert!(stats_rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn test_metadata_worker_joinset_aborts_running_worker() {
        let mut workers = JoinSet::new();
        workers.spawn(async {
            tokio::time::sleep(Duration::from_secs(60)).await;
        });

        workers.abort_all();
        let result = workers.join_next().await.unwrap();

        assert!(result.unwrap_err().is_cancelled());
        assert!(workers.is_empty());
    }
}
