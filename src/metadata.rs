use crate::metrics::CrawlerMetrics;
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

const LOCAL_METADATA_ID: u8 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IoStage {
    Connect,
    HandshakeRead,
    HandshakeWrite,
    ExtensionRead,
    ExtensionWrite,
    MetadataRead,
    MetadataWrite,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExtensionUnavailable {
    ExtensionsUnsupported,
    MetadataUnsupported,
    MetadataDisabled,
    MetadataSizeMissing,
}

#[derive(Debug)]
pub enum MetadataFetchError {
    Io {
        stage: IoStage,
        source: std::io::Error,
    },
    Timeout {
        stage: IoStage,
    },
    InvalidHandshake,
    ExtensionUnavailable(ExtensionUnavailable),
    MetadataTooLarge {
        size: u32,
        limit: u32,
    },
    MetadataRejected,
    Protocol {
        stage: IoStage,
        message: String,
    },
}

impl MetadataFetchError {
    fn reason(&self) -> MetadataFailureReason {
        use IoStage::*;
        match self {
            Self::Io { stage, .. } => match stage {
                Connect => MetadataFailureReason::Connect,
                HandshakeRead | HandshakeWrite => MetadataFailureReason::HandshakeIo,
                ExtensionRead | ExtensionWrite => MetadataFailureReason::ExtensionIo,
                MetadataRead | MetadataWrite => MetadataFailureReason::MetadataIo,
            },
            Self::Timeout { stage } => match stage {
                Connect => MetadataFailureReason::ConnectTimeout,
                HandshakeRead | HandshakeWrite => MetadataFailureReason::HandshakeTimeout,
                ExtensionRead | ExtensionWrite => MetadataFailureReason::ExtensionTimeout,
                MetadataRead | MetadataWrite => MetadataFailureReason::MetadataTimeout,
            },
            Self::InvalidHandshake => MetadataFailureReason::Handshake,
            Self::ExtensionUnavailable(reason) => match reason {
                ExtensionUnavailable::ExtensionsUnsupported => {
                    MetadataFailureReason::ExtensionsUnsupported
                }
                ExtensionUnavailable::MetadataUnsupported => {
                    MetadataFailureReason::MetadataUnsupported
                }
                ExtensionUnavailable::MetadataDisabled => MetadataFailureReason::MetadataDisabled,
                ExtensionUnavailable::MetadataSizeMissing => {
                    MetadataFailureReason::MetadataSizeMissing
                }
            },
            Self::MetadataTooLarge { .. } => MetadataFailureReason::MetadataTooLarge,
            Self::MetadataRejected => MetadataFailureReason::Rejected,
            Self::Protocol {
                stage: ExtensionRead | ExtensionWrite,
                ..
            } => MetadataFailureReason::ExtensionProtocol,
            Self::Protocol { .. } => MetadataFailureReason::Protocol,
        }
    }

    fn long_backoff(&self) -> bool {
        matches!(
            self,
            Self::ExtensionUnavailable(_) | Self::MetadataTooLarge { .. } | Self::MetadataRejected
        )
    }
}

impl std::fmt::Display for MetadataFetchError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io { stage, source } => write!(formatter, "I/O during {stage:?}: {source}"),
            Self::Timeout { stage } => write!(formatter, "timeout during {stage:?}"),
            Self::InvalidHandshake => {
                formatter.write_str("invalid handshake or info_hash mismatch")
            }
            Self::ExtensionUnavailable(reason) => formatter.write_str(match reason {
                ExtensionUnavailable::ExtensionsUnsupported => "peer does not support extensions",
                ExtensionUnavailable::MetadataUnsupported => "peer does not advertise ut_metadata",
                ExtensionUnavailable::MetadataDisabled => "peer disabled ut_metadata",
                ExtensionUnavailable::MetadataSizeMissing => "peer did not advertise metadata_size",
            }),
            Self::MetadataTooLarge { size, limit } => {
                write!(formatter, "metadata size {size} exceeds limit {limit}")
            }
            Self::MetadataRejected => formatter.write_str("metadata rejected"),
            Self::Protocol { stage, message } => {
                write!(formatter, "protocol error during {stage:?}: {message}")
            }
        }
    }
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
            stage: IoStage::Connect,
        })?
        .map_err(|source| MetadataFetchError::Io {
            stage: IoStage::Connect,
            source,
        })?;

    fetch_from_stream_with_limit(info_hash, stream, peer_timeout, max_metadata_size).await
}

#[cfg_attr(not(test), allow(dead_code))]
async fn fetch_from_stream<S>(
    info_hash: &InfoHash,
    stream: S,
    peer_timeout: Duration,
) -> Result<Vec<u8>, MetadataFetchError>
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
) -> Result<Vec<u8>, MetadataFetchError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let our_peer_id = crate::types::random_node_id();

    let hs = Handshake::new(*info_hash, our_peer_id);
    write_all_timeout(
        &mut stream,
        &hs.to_bytes(),
        peer_timeout,
        IoStage::HandshakeWrite,
    )
    .await?;

    let mut handshake_buf = vec![0u8; HANDSHAKE_LEN];
    read_exact_timeout(
        &mut stream,
        &mut handshake_buf,
        peer_timeout,
        IoStage::HandshakeRead,
    )
    .await?;

    let hs_resp =
        Handshake::from_bytes(&handshake_buf).ok_or(MetadataFetchError::InvalidHandshake)?;

    if hs_resp.info_hash != *info_hash {
        return Err(MetadataFetchError::InvalidHandshake);
    }

    if !hs_resp.supports_extensions() {
        return Err(MetadataFetchError::ExtensionUnavailable(
            ExtensionUnavailable::ExtensionsUnsupported,
        ));
    }

    let mut m = std::collections::BTreeMap::new();
    m.insert("ut_metadata".to_string(), u32::from(LOCAL_METADATA_ID));

    let ext_hs = ExtendedHandshake {
        m,
        metadata_size: None,
        v: Some("btfind 0.1".to_string()),
        your_ip: None,
        reqq: None,
    };

    let ext_hs_bytes = serde_bencode::to_bytes(&serde_bencode::value::Value::Dict(
        ext_hs.to_dict(),
    ))
    .map_err(|e| MetadataFetchError::Protocol {
        stage: IoStage::ExtensionWrite,
        message: format!("encode extended handshake: {e}"),
    })?;

    let ext_msg = WireMessage::Extended {
        id: 0,
        payload: ext_hs_bytes,
    };

    write_all_timeout(
        &mut stream,
        &ext_msg.to_bytes(),
        peer_timeout,
        IoStage::ExtensionWrite,
    )
    .await?;

    let peer_ext_hs = read_extended_handshake(&mut stream, peer_timeout).await?;

    let peer_metadata_id = peer_ext_hs.m.get("ut_metadata").copied().ok_or(
        MetadataFetchError::ExtensionUnavailable(ExtensionUnavailable::MetadataUnsupported),
    )?;
    if peer_metadata_id == 0 {
        return Err(MetadataFetchError::ExtensionUnavailable(
            ExtensionUnavailable::MetadataDisabled,
        ));
    }
    let peer_metadata_id =
        u8::try_from(peer_metadata_id).map_err(|_| MetadataFetchError::Protocol {
            stage: IoStage::ExtensionRead,
            message: format!("invalid ut_metadata id: {peer_metadata_id}"),
        })?;
    let metadata_size =
        peer_ext_hs
            .metadata_size
            .ok_or(MetadataFetchError::ExtensionUnavailable(
                ExtensionUnavailable::MetadataSizeMissing,
            ))?;
    if metadata_size == 0 {
        return Err(MetadataFetchError::Protocol {
            stage: IoStage::ExtensionRead,
            message: "metadata_size is zero".to_string(),
        });
    }
    if metadata_size > max_metadata_size {
        return Err(MetadataFetchError::MetadataTooLarge {
            size: metadata_size,
            limit: max_metadata_size,
        });
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
            id: peer_metadata_id,
            payload: req,
        };
        write_all_timeout(
            &mut stream,
            &req_msg.to_bytes(),
            peer_timeout,
            IoStage::MetadataWrite,
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
            return Err(MetadataFetchError::Timeout {
                stage: IoStage::MetadataRead,
            });
        }

        match read_next_frame(&mut stream, &mut buf, remaining, IoStage::MetadataRead).await? {
            Some(WireMessage::Extended { id, payload }) if id == LOCAL_METADATA_ID => {
                match wire::parse_metadata_response(&payload) {
                    Some(Ok((piece_idx, total_size, data))) => {
                        if requested.remove(&piece_idx) {
                            in_flight -= 1;
                        }
                        if total_size != metadata_size {
                            return Err(MetadataFetchError::Protocol {
                                stage: IoStage::MetadataRead,
                                message: "metadata total_size mismatch".to_string(),
                            });
                        }
                        if piece_idx >= total_pieces {
                            return Err(MetadataFetchError::Protocol {
                                stage: IoStage::MetadataRead,
                                message: "metadata piece out of range".to_string(),
                            });
                        }
                        let expected_len =
                            metadata_piece_len(metadata_size, total_pieces, piece_idx);
                        if data.len() != expected_len {
                            return Err(MetadataFetchError::Protocol {
                                stage: IoStage::MetadataRead,
                                message: format!(
                                    "invalid metadata piece length: got {}, expected {}",
                                    data.len(),
                                    expected_len
                                ),
                            });
                        }
                        if pieces[piece_idx as usize].is_none() {
                            pieces[piece_idx as usize] = Some(data);
                            received += 1;
                        }
                    }
                    Some(Err(())) => {
                        return Err(MetadataFetchError::MetadataRejected);
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
                id: peer_metadata_id,
                payload: req,
            };
            write_all_timeout(
                &mut stream,
                &req_msg.to_bytes(),
                peer_timeout,
                IoStage::MetadataWrite,
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
            None => {
                return Err(MetadataFetchError::Protocol {
                    stage: IoStage::MetadataRead,
                    message: "missing metadata piece".to_string(),
                })
            }
        }
    }

    if all_data.is_empty() {
        return Err(MetadataFetchError::Protocol {
            stage: IoStage::MetadataRead,
            message: "empty metadata".to_string(),
        });
    }

    if all_data.len() != metadata_size as usize {
        return Err(MetadataFetchError::Protocol {
            stage: IoStage::MetadataRead,
            message: format!(
                "metadata size mismatch: got {}, expected {}",
                all_data.len(),
                metadata_size
            ),
        });
    }

    Ok(all_data)
}

async fn write_all_timeout<S>(
    stream: &mut S,
    bytes: &[u8],
    timeout_dur: Duration,
    stage: IoStage,
) -> Result<(), MetadataFetchError>
where
    S: AsyncWrite + Unpin,
{
    timeout(timeout_dur, stream.write_all(bytes))
        .await
        .map_err(|_| MetadataFetchError::Timeout { stage })?
        .map_err(|source| MetadataFetchError::Io { stage, source })
}

async fn read_extended_handshake<S>(
    stream: &mut S,
    peer_timeout: Duration,
) -> Result<ExtendedHandshake, MetadataFetchError>
where
    S: AsyncRead + Unpin,
{
    let start = std::time::Instant::now();
    let mut buf = Vec::new();
    loop {
        let remaining = peer_timeout.saturating_sub(start.elapsed());
        if remaining.is_zero() {
            return Err(MetadataFetchError::Timeout {
                stage: IoStage::ExtensionRead,
            });
        }
        match read_next_frame(stream, &mut buf, remaining, IoStage::ExtensionRead).await? {
            Some(WireMessage::Extended { id: 0, payload }) => {
                return ExtendedHandshake::from_bytes(&payload).ok_or_else(|| {
                    MetadataFetchError::Protocol {
                        stage: IoStage::ExtensionRead,
                        message: "invalid extended handshake".to_string(),
                    }
                });
            }
            Some(_) => continue,
            None => {
                return Err(MetadataFetchError::Protocol {
                    stage: IoStage::ExtensionRead,
                    message: "invalid frame during extended handshake".to_string(),
                })
            }
        }
    }
}

async fn read_exact_timeout<S>(
    stream: &mut S,
    buf: &mut [u8],
    timeout_dur: Duration,
    stage: IoStage,
) -> Result<(), MetadataFetchError>
where
    S: AsyncRead + Unpin,
{
    timeout(timeout_dur, stream.read_exact(buf))
        .await
        .map_err(|_| MetadataFetchError::Timeout { stage })?
        .map_err(|source| MetadataFetchError::Io { stage, source })?;
    Ok(())
}

async fn read_next_frame<S>(
    stream: &mut S,
    buf: &mut Vec<u8>,
    timeout_dur: Duration,
    stage: IoStage,
) -> Result<Option<WireMessage>, MetadataFetchError>
where
    S: AsyncRead + Unpin,
{
    let mut header = [0u8; 4];
    read_exact_timeout(stream, &mut header, timeout_dur, stage).await?;
    let len = u32::from_be_bytes(header);
    if len == 0 {
        return Ok(Some(WireMessage::KeepAlive));
    }
    if len > 262_144 {
        return Err(MetadataFetchError::Protocol {
            stage,
            message: format!("frame too large: {len} bytes"),
        });
    }
    buf.resize(len as usize, 0);
    read_exact_timeout(stream, buf, timeout_dur, stage).await?;
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

async fn filter_peers_for_retry(
    store: &Store,
    info_hash: &InfoHash,
    peers: &[PeerContact],
    retry_after_hours: u32,
) -> PeerRetryFilter {
    let mut eligible = Vec::new();
    let mut next_eligible_at: Option<i64> = None;
    for peer in peers {
        let peer_addr = peer.addr.to_string();
        match store
            .peer_retry_eligibility(info_hash, &peer_addr, retry_after_hours)
            .await
        {
            Ok(PeerRetryEligibility::Eligible) => eligible.push(peer.clone()),
            Ok(PeerRetryEligibility::RetryAt(retry_at)) => {
                next_eligible_at =
                    Some(next_eligible_at.map_or(retry_at, |current| current.min(retry_at)));
                tracing::debug!(
                    event = "metadata_peer_delayed",
                    retry_at = retry_at,
                    "delaying metadata fetch until the peer retry time"
                );
            }
            Ok(PeerRetryEligibility::Rejected) => {
                tracing::debug!(
                    event = "metadata_peer_rejected",
                    reason = "hash_mismatch",
                    "rejecting metadata peer after hash mismatch"
                );
            }
            Err(error) => {
                tracing::warn!(
                    event = "metadata_retry_state_failed",
                    reason = "database",
                    error = %error,
                    "peer retry eligibility lookup failed; delaying"
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

fn emit_stat(
    metrics: &CrawlerMetrics,
    stats_tx: &mpsc::Sender<CrawlStatsEvent>,
    event: CrawlStatsEvent,
) -> bool {
    metrics.apply(&event);
    stats_tx.try_send(event).is_ok()
}

fn emit_metadata_failure(
    metrics: &CrawlerMetrics,
    stats_tx: &mpsc::Sender<CrawlStatsEvent>,
    reason: MetadataFailureReason,
) {
    if !emit_stat(
        metrics,
        stats_tx,
        CrawlStatsEvent::MetadataFetchFailed { reason },
    ) {
        tracing::debug!(
            event = "stats_channel_full",
            event_kind = "metadata_fetch_failed",
            "metadata failure statistic was not accepted"
        );
    }
}

fn log_storage_result(operation: &str, result: Result<(), crate::store::Error>) {
    match result {
        Ok(()) => {}
        Err(error) => tracing::warn!(
            event = "metadata_storage_failed",
            operation = operation,
            reason = "database",
            error = %error,
            "metadata storage operation failed"
        ),
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum JobAdmission {
    Accepted,
    NoPeers,
    AtCapacity,
}

#[allow(clippy::too_many_arguments)]
fn admit_job_peers(
    jobs: &mut HashMap<InfoHash, HashJob>,
    ready: &mut VecDeque<InfoHash>,
    ready_set: &mut HashSet<InfoHash>,
    info_hash: InfoHash,
    peers: Vec<PeerContact>,
    attempt_count: u32,
    max_active_hash_jobs: usize,
    max_peers_per_hash: usize,
) -> JobAdmission {
    if let Some(job) = jobs.get_mut(&info_hash) {
        job.attempt_count = job.attempt_count.max(attempt_count);
        let added = job.merge_peers(peers, max_peers_per_hash);
        if added > 0 && !job.running && ready_set.insert(info_hash) {
            ready.push_back(info_hash);
        }
        return JobAdmission::Accepted;
    }
    if peers.is_empty() {
        return JobAdmission::NoPeers;
    }
    if jobs.len() >= max_active_hash_jobs {
        return JobAdmission::AtCapacity;
    }
    let mut job = HashJob::new(attempt_count);
    if job.merge_peers(peers, max_peers_per_hash) == 0 {
        return JobAdmission::NoPeers;
    }
    jobs.insert(info_hash, job);
    ready_set.insert(info_hash);
    ready.push_back(info_hash);
    JobAdmission::Accepted
}

struct FetchRoundConfig {
    peer_timeout: Duration,
    retry_after_hours: u32,
    max_metadata_size: u32,
    prior_attempt_count: u32,
    metrics: Arc<CrawlerMetrics>,
}

fn apply_job_outcome(
    jobs: &mut HashMap<InfoHash, HashJob>,
    ready: &mut VecDeque<InfoHash>,
    ready_set: &mut HashSet<InfoHash>,
    outcome: JobOutcome,
) {
    let remove = if outcome.complete || outcome.terminal || outcome.parked {
        true
    } else if let Some(job) = jobs.get_mut(&outcome.info_hash) {
        job.running = false;
        job.attempt_count = job.attempt_count.saturating_add(outcome.attempts);
        if job.peers.is_empty() {
            true
        } else {
            if ready_set.insert(outcome.info_hash) {
                ready.push_back(outcome.info_hash);
            }
            false
        }
    } else {
        false
    };

    if remove {
        jobs.remove(&outcome.info_hash);
        ready_set.remove(&outcome.info_hash);
    }
}

async fn process_hash_round(
    info_hash: InfoHash,
    peers: Vec<PeerContact>,
    store: Arc<Store>,
    stats_tx: mpsc::Sender<CrawlStatsEvent>,
    config: FetchRoundConfig,
) -> JobOutcome {
    process_hash_round_with_fetch(
        info_hash,
        peers,
        store,
        stats_tx,
        config,
        |hash, peer, timeout, limit| async move {
            fetch_from_peer_with_limit(&hash, &peer, timeout, limit).await
        },
    )
    .await
}

async fn process_hash_round_with_fetch<F, Fut>(
    info_hash: InfoHash,
    peers: Vec<PeerContact>,
    store: Arc<Store>,
    stats_tx: mpsc::Sender<CrawlStatsEvent>,
    config: FetchRoundConfig,
    mut fetch: F,
) -> JobOutcome
where
    F: FnMut(InfoHash, PeerContact, Duration, u32) -> Fut,
    Fut: std::future::Future<Output = Result<Vec<u8>, MetadataFetchError>>,
{
    let mut retry_delay = Duration::from_millis(100);
    loop {
        match store.hash_job_is_terminal(&info_hash).await {
            Ok(true) => {
                return JobOutcome {
                    info_hash,
                    attempts: 0,
                    complete: false,
                    terminal: true,
                    parked: false,
                }
            }
            Ok(false) => break,
            Err(error) => {
                tracing::warn!(
                    event = "metadata_job_state_failed",
                    reason = "database",
                    error = %error,
                    "failed to check metadata job state; retrying"
                );
                tokio::time::sleep(retry_delay).await;
                retry_delay = retry_delay.saturating_mul(2).min(Duration::from_secs(5));
            }
        }
    }
    let filter_store = store.clone();
    let filtered = (async move {
        filter_peers_for_retry(&filter_store, &info_hash, &peers, config.retry_after_hours).await
    })
    .await;
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
        match (async move {
            attempt_store
                .set_peer_attempt(&attempt_hash, &attempt_addr, None)
                .await
        })
        .await
        {
            Ok(()) => {}
            Err(error) => {
                last_error = format!("storage: failed to record peer attempt: {}", error);
                retryable_attempts = retryable_attempts.saturating_add(1);
                continue;
            }
        }

        match fetch(
            info_hash,
            peer,
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
                let recorded = (async move {
                    failure_store
                        .set_peer_attempt(
                            &failure_hash,
                            &failure_addr,
                            Some("metadata hash mismatch"),
                        )
                        .await
                })
                .await;
                log_storage_result("record metadata hash mismatch", recorded);
                emit_metadata_failure(
                    &config.metrics,
                    &stats_tx,
                    MetadataFailureReason::HashMismatch,
                );
            }
            Ok(bytes) => match parse_torrent_info(&bytes) {
                Ok(info) => {
                    tracing::debug!(
                        event = "metadata_validated",
                        file_count = info.file_count,
                        total_size = info.total_size,
                        "validated torrent metadata"
                    );
                    let commit_store = store.clone();
                    let committed = (async move {
                        commit_store
                            .commit_metadata(
                                &info_hash,
                                &info.name,
                                info.piece_length,
                                info.total_size,
                                &info.files,
                            )
                            .await
                    })
                    .await;
                    match committed {
                        Ok(()) => {
                            if !emit_stat(
                                &config.metrics,
                                &stats_tx,
                                CrawlStatsEvent::MetadataFetched,
                            ) {
                                tracing::debug!(
                                    event = "stats_channel_full",
                                    event_kind = "metadata_fetched",
                                    "metadata completion statistic was not accepted"
                                );
                            }
                            return JobOutcome {
                                info_hash,
                                attempts,
                                complete: true,
                                terminal: false,
                                parked: false,
                            };
                        }
                        Err(error) if crate::store::is_unsupported_search_document(&error) => {
                            let message = error.to_string();
                            let mut retry_delay = Duration::from_millis(100);
                            loop {
                                match store
                                    .update_hash_job_failure(
                                        &info_hash,
                                        "unsupported",
                                        config.prior_attempt_count.saturating_add(attempts),
                                        0,
                                        &message,
                                    )
                                    .await
                                {
                                    Ok(()) => break,
                                    Err(error) => {
                                        tracing::warn!(
                                            event = "metadata_job_state_failed",
                                            reason = "database",
                                            error = %error,
                                            "failed to record unsupported search document; retrying"
                                        );
                                        tokio::time::sleep(retry_delay).await;
                                        retry_delay = retry_delay
                                            .saturating_mul(2)
                                            .min(Duration::from_secs(5));
                                    }
                                }
                            }
                            tracing::warn!(
                                event = "metadata_job_unsupported",
                                reason = "search_document",
                                "metadata job is unsupported"
                            );
                            return JobOutcome {
                                info_hash,
                                attempts,
                                complete: false,
                                terminal: true,
                                parked: false,
                            };
                        }
                        Err(error) => {
                            last_error = format!("storage: {}", error);
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
                    let recorded = (async move {
                        terminal_store
                            .update_hash_job_failure(
                                &info_hash,
                                status,
                                config.prior_attempt_count.saturating_add(attempts),
                                0,
                                &terminal_error,
                            )
                            .await
                    })
                    .await;
                    log_storage_result("record terminal metadata state", recorded);
                    emit_metadata_failure(&config.metrics, &stats_tx, MetadataFailureReason::Parse);
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
                let recorded = (async move {
                    failure_store
                        .set_peer_attempt(&failure_hash, &failure_addr, Some(&failure_text))
                        .await
                })
                .await;
                log_storage_result("record metadata peer failure", recorded);
                emit_metadata_failure(&config.metrics, &stats_tx, reason);
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
        let recorded = (async move {
            retry_store
                .update_hash_job_failure(
                    &info_hash,
                    "waiting_peers",
                    attempt_count,
                    0,
                    &retry_error,
                )
                .await
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
    let recorded = (async move {
        retry_store
            .update_hash_job_failure(
                &info_hash,
                "retry_at",
                attempt_count,
                next_attempt_at,
                &retry_error,
            )
            .await
    })
    .await;
    log_storage_result("schedule metadata retry", recorded);
    tracing::debug!(
        event = "metadata_round_failed",
        reason = "peer_attempts",
        error = %last_error,
        "metadata fetch round failed"
    );
    JobOutcome {
        info_hash,
        attempts,
        complete: false,
        terminal: false,
        parked: false,
    }
}

#[cfg(test)]
async fn mark_metadata_complete_and_emit(
    store: &Store,
    info_hash: &InfoHash,
    info: &TorrentInfo,
    stats_tx: &mpsc::Sender<CrawlStatsEvent>,
) -> Result<(), crate::store::Error> {
    store
        .commit_metadata(
            info_hash,
            &info.name,
            info.piece_length,
            info.total_size,
            &info.files,
        )
        .await?;
    if stats_tx.try_send(CrawlStatsEvent::MetadataFetched).is_err() {
        tracing::debug!(
            event = "stats_channel_full",
            event_kind = "metadata_fetched",
            "metadata completion statistic was not accepted"
        );
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
    metrics: Arc<CrawlerMetrics>,
) {
    let mut jobs: HashMap<InfoHash, HashJob> = HashMap::new();
    let mut ready = VecDeque::new();
    let mut ready_set = HashSet::new();
    let mut workers: JoinSet<JobOutcome> = JoinSet::new();
    let mut retry_tick = tokio::time::interval(Duration::from_secs(30));
    let mut status_tick = tokio::time::interval(Duration::from_secs(60));
    status_tick.tick().await;

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
                metrics: metrics.clone(),
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
                let source = discovery.source;
                let mut retry_delay = Duration::from_millis(100);
                loop {
                    let persistence = store.persist_hash_discovery(
                        &info_hash,
                        source.as_str(),
                        &peers,
                        max_peers_per_hash,
                    ).await;
                    match persistence {
                        Ok(()) => break,
                        Err(error) => {
                            tracing::warn!(
                                event = "discovery_persist_failed",
                                reason = "database",
                                retryable = true,
                                error = %error,
                                "failed to persist discovery; retrying"
                            );
                            tokio::time::sleep(retry_delay).await;
                            retry_delay = retry_delay.saturating_mul(2).min(Duration::from_secs(5));
                        }
                    }
                }
                if admit_job_peers(
                    &mut jobs,
                    &mut ready,
                    &mut ready_set,
                    info_hash,
                    peers,
                    0,
                    max_active_hash_jobs,
                    max_peers_per_hash,
                ) == JobAdmission::AtCapacity
                    && !emit_stat(
                        &metrics,
                        &stats_tx,
                        CrawlStatsEvent::DiscoveryBackpressure,
                    )
                {
                    tracing::debug!(
                        event = "stats_channel_full",
                        event_kind = "discovery_backpressure",
                        "metadata backpressure statistic was not accepted"
                    );
                }
            }
            _ = status_tick.tick() => {
                let pending_peers = jobs.values().map(|job| job.peers.len()).sum();
                if !emit_stat(
                    &metrics,
                    &stats_tx,
                    CrawlStatsEvent::MetadataQueue {
                        active_jobs: jobs.len(),
                        queued_jobs: ready.len(),
                        pending_peers,
                    },
                ) {
                    tracing::debug!(
                        event = "stats_channel_full",
                        event_kind = "metadata_queue",
                        "metadata queue statistic was not accepted"
                    );
                }
            }
            _ = retry_tick.tick() => {
                let due_store = store.clone();
                let due = (async move {
                    let records = due_store.due_hash_jobs(max_active_hash_jobs).await?;
                    let mut loaded = Vec::with_capacity(records.len());
                    for record in records {
                        let peers = due_store.hash_job_peers(&record.info_hash, max_peers_per_hash).await?;
                        loaded.push((record, peers));
                    }
                    Ok::<_, crate::store::Error>(loaded)
                }).await;
                if let Err(error) = &due {
                    tracing::warn!(
                        event = "metadata_job_load_failed",
                        reason = "database",
                        error = %error,
                        "failed to load due metadata jobs"
                    );
                }
                if let Ok(records) = due {
                    for (record, peers) in records {
                        let admission = admit_job_peers(
                            &mut jobs,
                            &mut ready,
                            &mut ready_set,
                            record.info_hash,
                            peers,
                            record.attempt_count,
                            max_active_hash_jobs,
                            max_peers_per_hash,
                        );
                        if admission == JobAdmission::Accepted {
                            tracing::debug!(
                                event = "metadata_job_restored",
                                status = %record.status,
                                attempt_count = record.attempt_count,
                                next_attempt_at = record.next_attempt_at,
                                has_last_failure = record.last_failure.is_some(),
                                "restored metadata job"
                            );
                        }
                    }
                }
            }
            result = workers.join_next(), if !workers.is_empty() => {
                match result {
                    Some(Ok(outcome)) => {
                        apply_job_outcome(&mut jobs, &mut ready, &mut ready_set, outcome);
                    }
                    Some(Err(error)) => tracing::warn!(
                        event = "metadata_worker_join_failed",
                        reason = "join",
                        error = %error,
                        "metadata worker join error"
                    ),
                    None => {}
                }
            }
        }
    }

    while let Some(result) = workers.join_next().await {
        if let Err(e) = result {
            tracing::warn!(
                event = "metadata_worker_join_failed",
                reason = "join",
                error = %e,
                "metadata worker join error"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn retry_outcome(info_hash: InfoHash) -> JobOutcome {
        JobOutcome {
            info_hash,
            attempts: 1,
            complete: false,
            terminal: false,
            parked: false,
        }
    }

    #[test]
    fn retry_without_pending_peers_releases_active_job_capacity() {
        let info_hash = [0x91; 20];
        let mut jobs = HashMap::from([(info_hash, HashJob::new(0))]);
        jobs.get_mut(&info_hash).unwrap().running = true;
        let mut ready = VecDeque::new();
        let mut ready_set = HashSet::new();

        apply_job_outcome(
            &mut jobs,
            &mut ready,
            &mut ready_set,
            retry_outcome(info_hash),
        );

        assert!(!jobs.contains_key(&info_hash));
        assert!(ready.is_empty());
    }

    #[test]
    fn retry_with_new_pending_peer_stays_ready() {
        let info_hash = [0x92; 20];
        let peer = PeerContact {
            addr: "8.8.8.8:6881".parse().unwrap(),
        };
        let mut job = HashJob::new(2);
        job.running = true;
        job.merge_peers(vec![peer], 8);
        let mut jobs = HashMap::from([(info_hash, job)]);
        let mut ready = VecDeque::new();
        let mut ready_set = HashSet::new();

        apply_job_outcome(
            &mut jobs,
            &mut ready,
            &mut ready_set,
            retry_outcome(info_hash),
        );

        assert_eq!(ready.pop_front(), Some(info_hash));
        assert_eq!(jobs.get(&info_hash).unwrap().attempt_count, 3);
        assert!(!jobs.get(&info_hash).unwrap().running);
    }

    #[test]
    fn peerless_discoveries_do_not_consume_active_job_capacity() {
        let max_active = 2;
        let mut jobs = HashMap::new();
        let mut ready = VecDeque::new();
        let mut ready_set = HashSet::new();

        for i in 0..=(max_active as u8) {
            assert_eq!(
                admit_job_peers(
                    &mut jobs,
                    &mut ready,
                    &mut ready_set,
                    [i; 20],
                    Vec::new(),
                    0,
                    max_active,
                    8,
                ),
                JobAdmission::NoPeers
            );
        }
        assert!(jobs.is_empty());
        assert!(ready.is_empty());

        let actionable = [0xA1; 20];
        let peer = PeerContact {
            addr: "8.8.8.8:6881".parse().unwrap(),
        };
        assert_eq!(
            admit_job_peers(
                &mut jobs,
                &mut ready,
                &mut ready_set,
                actionable,
                vec![peer],
                0,
                max_active,
                8,
            ),
            JobAdmission::Accepted
        );
        assert_eq!(jobs.len(), 1);
        assert_eq!(ready.pop_front(), Some(actionable));
    }

    #[test]
    fn peerless_hash_becomes_schedulable_when_peers_arrive() {
        let info_hash = [0xB2; 20];
        let mut jobs = HashMap::new();
        let mut ready = VecDeque::new();
        let mut ready_set = HashSet::new();

        assert_eq!(
            admit_job_peers(
                &mut jobs,
                &mut ready,
                &mut ready_set,
                info_hash,
                Vec::new(),
                0,
                8,
                8,
            ),
            JobAdmission::NoPeers
        );
        assert!(!jobs.contains_key(&info_hash));

        let peer = PeerContact {
            addr: "8.8.8.8:6881".parse().unwrap(),
        };
        assert_eq!(
            admit_job_peers(
                &mut jobs,
                &mut ready,
                &mut ready_set,
                info_hash,
                vec![peer],
                0,
                8,
                8,
            ),
            JobAdmission::Accepted
        );
        assert_eq!(jobs.len(), 1);
        assert_eq!(ready.pop_front(), Some(info_hash));
    }

    #[test]
    fn peerless_retry_restores_do_not_consume_active_job_capacity() {
        let mut jobs = HashMap::new();
        let mut ready = VecDeque::new();
        let mut ready_set = HashSet::new();

        for i in 0..4 {
            assert_eq!(
                admit_job_peers(
                    &mut jobs,
                    &mut ready,
                    &mut ready_set,
                    [0xC0 + i; 20],
                    Vec::new(),
                    4,
                    1,
                    8,
                ),
                JobAdmission::NoPeers
            );
        }
        assert!(jobs.is_empty());

        let info_hash = [0xD3; 20];
        let peer = PeerContact {
            addr: "8.8.8.8:6881".parse().unwrap(),
        };
        assert_eq!(
            admit_job_peers(
                &mut jobs,
                &mut ready,
                &mut ready_set,
                info_hash,
                vec![peer],
                7,
                1,
                8,
            ),
            JobAdmission::Accepted
        );
        assert_eq!(jobs[&info_hash].attempt_count, 7);
        assert_eq!(ready.pop_front(), Some(info_hash));
    }

    #[test]
    fn at_capacity_rejects_new_actionable_jobs() {
        let mut jobs = HashMap::new();
        let mut ready = VecDeque::new();
        let mut ready_set = HashSet::new();
        let peer = PeerContact {
            addr: "8.8.8.8:6881".parse().unwrap(),
        };

        assert_eq!(
            admit_job_peers(
                &mut jobs,
                &mut ready,
                &mut ready_set,
                [0xE1; 20],
                vec![peer.clone()],
                0,
                1,
                8,
            ),
            JobAdmission::Accepted
        );
        assert_eq!(
            admit_job_peers(
                &mut jobs,
                &mut ready,
                &mut ready_set,
                [0xE2; 20],
                vec![peer],
                0,
                1,
                8,
            ),
            JobAdmission::AtCapacity
        );
        assert_eq!(jobs.len(), 1);
    }

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

    #[test]
    fn parse_torrent_info_rejects_negative_single_file_length() {
        let result = parse_torrent_info(b"d6:lengthi-1e4:name3:bad12:piece lengthi16384ee");

        assert!(result.unwrap_err().contains("negative torrent length"));
    }

    #[test]
    fn parse_torrent_info_rejects_empty_multi_file_path() {
        let result =
            parse_torrent_info(b"d5:filesld6:lengthi1e4:pathleee4:name1:x12:piece lengthi16384ee");

        assert!(result
            .unwrap_err()
            .contains("invalid file path component count"));
    }

    #[test]
    fn parse_torrent_info_rejects_total_size_overflow() {
        let result = parse_torrent_info(
            b"d5:filesld6:lengthi9223372036854775807e4:pathl1:aeed6:lengthi1e4:pathl1:beee4:name1:x12:piece lengthi16384ee",
        );

        assert!(result.unwrap_err().contains("torrent total size overflow"));
    }

    #[tokio::test]
    #[ignore = "requires BTFIND_TEST_DATABASE_URL; run scripts/test-mysql.sh"]
    async fn oversized_search_document_stops_downloads_after_one_attempt() {
        let store = Arc::new(Store::open_test_database().await.unwrap());
        let path = "㍿".repeat(300);
        let file = format!("d6:lengthi1e4:pathl{}:{}ee", path.len(), path);
        let bytes = format!(
            "d5:filesl{}e4:name8:oversize12:piece lengthi16384ee",
            file.repeat(5000)
        )
        .into_bytes();
        assert!(bytes.len() < 64 * 1024 * 1024);
        assert!(parse_torrent_info(&bytes).is_ok());
        let info_hash: InfoHash = Sha1::digest(&bytes).into();
        let peers = vec![
            PeerContact {
                addr: "8.8.8.8:6881".parse().unwrap(),
            },
            PeerContact {
                addr: "8.8.4.4:6881".parse().unwrap(),
            },
        ];
        store
            .persist_hash_discovery(&info_hash, "test", &peers, 8)
            .await
            .unwrap();
        let (stats_tx, mut stats_rx) = mpsc::channel(8);
        let downloads = std::cell::Cell::new(0);
        for round in 0..2 {
            let outcome = process_hash_round_with_fetch(
                info_hash,
                peers.clone(),
                store.clone(),
                stats_tx.clone(),
                FetchRoundConfig {
                    peer_timeout: Duration::from_secs(1),
                    retry_after_hours: 0,
                    max_metadata_size: 64 * 1024 * 1024,
                    prior_attempt_count: 0,
                    metrics: Arc::new(CrawlerMetrics::default()),
                },
                |_, _, _, _| {
                    downloads.set(downloads.get() + 1);
                    std::future::ready(Ok(bytes.clone()))
                },
            )
            .await;
            assert!(outcome.terminal);
            assert!(!outcome.complete);
            assert_eq!(outcome.attempts, u32::from(round == 0));
            assert_eq!(
                store.hash_job_schedule_for_test(&info_hash).await.unwrap(),
                ("unsupported".into(), 1, 0)
            );
            assert!(store.due_hash_jobs(10).await.unwrap().is_empty());
            store
                .persist_hash_discovery(&info_hash, "rediscovered", &peers, 8)
                .await
                .unwrap();
        }
        assert_eq!(downloads.get(), 1);
        assert!(stats_rx.try_recv().is_err());
        assert_eq!(store.get_stats().await.unwrap().total_metadata_complete, 0);
    }

    #[tokio::test]
    #[ignore = "requires BTFIND_TEST_DATABASE_URL; run scripts/test-mysql.sh"]
    async fn database_commit_failure_still_schedules_metadata_retry() {
        let store = Arc::new(Store::open_test_database().await.unwrap());
        let bytes = b"d6:lengthi42e4:name8:test.txt12:piece lengthi16384ee".to_vec();
        let info_hash: InfoHash = Sha1::digest(&bytes).into();
        let peers = vec![PeerContact {
            addr: "8.8.8.8:6881".parse().unwrap(),
        }];
        store
            .persist_hash_discovery(&info_hash, "test", &peers, 8)
            .await
            .unwrap();
        store.execute_batch_for_test("CREATE TRIGGER fail_commit BEFORE INSERT ON files FOR EACH ROW SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='temporary storage failure'").await.unwrap();
        let (stats_tx, _stats_rx) = mpsc::channel(8);
        let outcome = process_hash_round_with_fetch(
            info_hash,
            peers,
            store.clone(),
            stats_tx,
            FetchRoundConfig {
                peer_timeout: Duration::from_secs(1),
                retry_after_hours: 0,
                max_metadata_size: 1024,
                prior_attempt_count: 0,
                metrics: Arc::new(CrawlerMetrics::default()),
            },
            |_, _, _, _| std::future::ready(Ok(bytes.clone())),
        )
        .await;
        assert!(!outcome.terminal);
        let (status, attempts, next) = store.hash_job_schedule_for_test(&info_hash).await.unwrap();
        assert_eq!(status, "retry_at");
        assert_eq!(attempts, 1);
        assert!(next > chrono::Utc::now().timestamp());
    }

    #[tokio::test]
    #[ignore = "requires BTFIND_TEST_DATABASE_URL; run scripts/test-mysql.sh"]
    async fn typed_failures_persist_the_correct_retry_backoff() {
        let store = Arc::new(Store::open_test_database().await.unwrap());
        let peer = PeerContact {
            addr: "192.0.2.1:6881".parse().unwrap(),
        };
        for (index, (error, base, expected_reason)) in [
            (
                MetadataFetchError::Timeout {
                    stage: IoStage::ExtensionRead,
                },
                30,
                MetadataFailureReason::ExtensionTimeout,
            ),
            (
                MetadataFetchError::Io {
                    stage: IoStage::ExtensionRead,
                    source: std::io::ErrorKind::UnexpectedEof.into(),
                },
                30,
                MetadataFailureReason::ExtensionIo,
            ),
            (
                MetadataFetchError::Protocol {
                    stage: IoStage::ExtensionRead,
                    message: "invalid extended handshake".into(),
                },
                30,
                MetadataFailureReason::ExtensionProtocol,
            ),
            (
                MetadataFetchError::ExtensionUnavailable(ExtensionUnavailable::MetadataDisabled),
                900,
                MetadataFailureReason::MetadataDisabled,
            ),
            (
                MetadataFetchError::MetadataRejected,
                900,
                MetadataFailureReason::Rejected,
            ),
            (
                MetadataFetchError::MetadataTooLarge {
                    size: 2048,
                    limit: 1024,
                },
                900,
                MetadataFailureReason::MetadataTooLarge,
            ),
        ]
        .into_iter()
        .enumerate()
        {
            let hash = [index as u8; 20];
            store
                .persist_hash_discovery(&hash, "test", std::slice::from_ref(&peer), 8)
                .await
                .unwrap();
            let (stats_tx, mut stats_rx) = mpsc::channel(4);
            let mut error = Some(error);
            let before = chrono::Utc::now().timestamp();
            let outcome = process_hash_round_with_fetch(
                hash,
                vec![peer.clone()],
                store.clone(),
                stats_tx,
                FetchRoundConfig {
                    peer_timeout: Duration::from_secs(1),
                    retry_after_hours: 0,
                    max_metadata_size: 1024,
                    prior_attempt_count: 0,
                    metrics: Arc::new(CrawlerMetrics::default()),
                },
                |_, _, _, _| std::future::ready(Err(error.take().unwrap())),
            )
            .await;
            let after = chrono::Utc::now().timestamp();
            assert_eq!(outcome.attempts, 1);
            assert!(!outcome.complete && !outcome.terminal && !outcome.parked);
            let (status, attempts, next) = store.hash_job_schedule_for_test(&hash).await.unwrap();
            assert_eq!((status.as_str(), attempts), ("retry_at", 1));
            assert!(
                (before + base * 2..=after + base * 2 + base / 2).contains(&next),
                "unexpected retry time {next}"
            );
            assert_eq!(
                stats_rx.recv().await,
                Some(CrawlStatsEvent::MetadataFetchFailed {
                    reason: expected_reason
                })
            );
        }
    }

    #[tokio::test]
    #[ignore = "requires BTFIND_TEST_DATABASE_URL; run scripts/test-mysql.sh"]
    async fn durable_retry_waits_until_peer_is_eligible() {
        let store = Arc::new(Store::open_test_database().await.unwrap());
        let info_hash = [0xACu8; 20];
        let peer = PeerContact {
            addr: "8.8.8.8:6881".parse().unwrap(),
        };
        store
            .persist_hash_discovery(&info_hash, "test", std::slice::from_ref(&peer), 8)
            .await
            .unwrap();
        store
            .set_peer_attempt(&info_hash, &peer.addr.to_string(), Some("timeout"))
            .await
            .unwrap();
        let earliest_expected = chrono::Utc::now().timestamp() + 23 * 3600;
        let (stats_tx, _stats_rx) = mpsc::channel(4);

        let outcome = process_hash_round(
            info_hash,
            vec![peer.clone()],
            store.clone(),
            stats_tx,
            FetchRoundConfig {
                peer_timeout: Duration::from_millis(1),
                retry_after_hours: 24,
                max_metadata_size: 1024,
                prior_attempt_count: 0,
                metrics: Arc::new(CrawlerMetrics::default()),
            },
        )
        .await;

        assert_eq!(outcome.attempts, 0);
        assert!(!outcome.parked);
        let (status, attempt_count, next_attempt_at) =
            store.hash_job_schedule_for_test(&info_hash).await.unwrap();
        assert_eq!(status, "retry_at");
        assert_eq!(attempt_count, 0);
        assert!(next_attempt_at >= earliest_expected);
    }

    #[tokio::test]
    #[ignore = "requires BTFIND_TEST_DATABASE_URL; run scripts/test-mysql.sh"]
    async fn permanently_rejected_peer_parks_job_until_new_discovery() {
        let store = Arc::new(Store::open_test_database().await.unwrap());
        let info_hash = [0xADu8; 20];
        let peer = PeerContact {
            addr: "8.8.4.4:6881".parse().unwrap(),
        };
        store
            .persist_hash_discovery(&info_hash, "test", std::slice::from_ref(&peer), 8)
            .await
            .unwrap();
        store
            .set_peer_attempt(
                &info_hash,
                &peer.addr.to_string(),
                Some("metadata hash mismatch"),
            )
            .await
            .unwrap();
        let (stats_tx, _stats_rx) = mpsc::channel(4);

        let outcome = process_hash_round(
            info_hash,
            vec![peer.clone()],
            store.clone(),
            stats_tx,
            FetchRoundConfig {
                peer_timeout: Duration::from_millis(1),
                retry_after_hours: 24,
                max_metadata_size: 1024,
                prior_attempt_count: 1,
                metrics: Arc::new(CrawlerMetrics::default()),
            },
        )
        .await;

        assert_eq!(outcome.attempts, 0);
        assert!(outcome.parked);
        let (status, attempt_count, next_attempt_at) =
            store.hash_job_schedule_for_test(&info_hash).await.unwrap();
        assert_eq!(status, "waiting_peers");
        assert_eq!(attempt_count, 1);
        assert_eq!(next_attempt_at, 0);

        store
            .persist_hash_discovery(&info_hash, "sample_infohashes", &[], 8)
            .await
            .unwrap();
        let (status, _, _) = store.hash_job_schedule_for_test(&info_hash).await.unwrap();
        assert_eq!(status, "waiting_peers");

        store
            .persist_hash_discovery(&info_hash, "announce_peer", std::slice::from_ref(&peer), 8)
            .await
            .unwrap();
        let (status, _, _) = store.hash_job_schedule_for_test(&info_hash).await.unwrap();
        assert_eq!(status, "queued");
    }

    async fn assert_fetch_with_peer_metadata_id(peer_metadata_id: u32) {
        let info_dict = b"d6:lengthi42e4:name8:test.txt12:piece lengthi16384ee";
        let info_hash: InfoHash = {
            let mut hasher = Sha1::new();
            hasher.update(info_dict);
            hasher.finalize().into()
        };

        let (client_stream, mut server_stream) = tokio::io::duplex(64 * 1024);
        let server_info_hash = info_hash;

        let server = tokio::spawn(async move {
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
            let Some(WireMessage::Extended { id: 0, payload }) =
                WireMessage::from_bytes_frame(len as u32, &body)
            else {
                panic!("expected local extended handshake");
            };
            let local = ExtendedHandshake::from_bytes(&payload).unwrap();
            assert_eq!(local.m.get("ut_metadata"), Some(&1));

            // send extended handshake with ut_metadata
            let mut m = std::collections::BTreeMap::new();
            m.insert("ut_metadata".to_string(), peer_metadata_id);
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
            assert!(
                matches!(WireMessage::from_bytes_frame(len as u32, &body), Some(WireMessage::Extended { id, .. }) if u32::from(id) == peer_metadata_id)
            );

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
        server.await.unwrap();
    }

    #[tokio::test]
    async fn test_fetch_from_stream_identical_extension_ids() {
        assert_fetch_with_peer_metadata_id(1).await;
    }

    #[tokio::test]
    async fn test_fetch_from_stream_different_extension_ids() {
        assert_fetch_with_peer_metadata_id(3).await;
        assert_fetch_with_peer_metadata_id(255).await;
    }

    fn negotiation_bytes(metadata_id: Option<u32>, size: Option<u32>) -> Vec<u8> {
        let mut m = std::collections::BTreeMap::new();
        if let Some(id) = metadata_id {
            m.insert("ut_metadata".to_string(), id);
        }
        let handshake = ExtendedHandshake {
            m,
            metadata_size: size,
            v: None,
            your_ip: None,
            reqq: None,
        };
        let payload =
            serde_bencode::to_bytes(&serde_bencode::value::Value::Dict(handshake.to_dict()))
                .unwrap();
        WireMessage::Extended { id: 0, payload }.to_bytes()
    }

    async fn error_from_peer_bytes(bytes: &[u8], close: bool) -> MetadataFetchError {
        let (client, mut peer) = tokio::io::duplex(4096);
        peer.write_all(bytes).await.unwrap();
        if close {
            peer.shutdown().await.unwrap();
        }
        let error =
            fetch_from_stream_with_limit(&[0xAA; 20], client, Duration::from_millis(10), 1024)
                .await
                .unwrap_err();
        drop(peer);
        error
    }

    #[tokio::test]
    async fn invalid_negotiation_has_specific_reason_and_backoff() {
        let handshake = Handshake::new([0xAA; 20], [0; 20]).to_bytes();
        for (id, size, expected, long_backoff) in [
            (
                None,
                Some(42),
                MetadataFailureReason::MetadataUnsupported,
                true,
            ),
            (
                Some(0),
                Some(42),
                MetadataFailureReason::MetadataDisabled,
                true,
            ),
            (
                Some(256),
                Some(42),
                MetadataFailureReason::ExtensionProtocol,
                false,
            ),
            (
                Some(3),
                None,
                MetadataFailureReason::MetadataSizeMissing,
                true,
            ),
            (
                Some(3),
                Some(0),
                MetadataFailureReason::ExtensionProtocol,
                false,
            ),
            (
                Some(3),
                Some(1025),
                MetadataFailureReason::MetadataTooLarge,
                true,
            ),
        ] {
            let mut bytes = handshake.clone();
            bytes.extend(negotiation_bytes(id, size));
            let error = error_from_peer_bytes(&bytes, true).await;
            assert_eq!(error.reason(), expected, "{error}");
            assert_eq!(error.long_backoff(), long_backoff, "{error}");
        }
        let mut unsupported = Handshake::new([0xAA; 20], [0; 20]);
        unsupported.reserved[5] = 0;
        let error = error_from_peer_bytes(&unsupported.to_bytes(), true).await;
        assert_eq!(error.reason(), MetadataFailureReason::ExtensionsUnsupported);
        assert!(error.long_backoff());

        for payload in [
            b"not bencode".as_slice(),
            b"d1:md11:ut_metadatai-1eee",
            b"d1:md11:ut_metadata1:xee",
            b"d1:mi1ee",
            b"d1:md11:ut_metadatai3ee13:metadata_size1:xe",
        ] {
            let mut malformed = handshake.clone();
            malformed.extend(
                WireMessage::Extended {
                    id: 0,
                    payload: payload.to_vec(),
                }
                .to_bytes(),
            );
            let error = error_from_peer_bytes(&malformed, true).await;
            assert_eq!(error.reason(), MetadataFailureReason::ExtensionProtocol);
            assert!(!error.long_backoff());
        }
    }

    #[tokio::test]
    async fn peer_read_failures_keep_their_protocol_stage() {
        let handshake = Handshake::new([0xAA; 20], [0; 20]).to_bytes();
        let mut negotiated = handshake.clone();
        negotiated.extend(negotiation_bytes(Some(3), Some(42)));
        for (prefix, stage, timeout_reason, io_reason) in [
            (
                Vec::new(),
                IoStage::HandshakeRead,
                MetadataFailureReason::HandshakeTimeout,
                MetadataFailureReason::HandshakeIo,
            ),
            (
                handshake,
                IoStage::ExtensionRead,
                MetadataFailureReason::ExtensionTimeout,
                MetadataFailureReason::ExtensionIo,
            ),
            (
                negotiated,
                IoStage::MetadataRead,
                MetadataFailureReason::MetadataTimeout,
                MetadataFailureReason::MetadataIo,
            ),
        ] {
            for partial_body in [false, true] {
                let mut bytes = prefix.clone();
                if partial_body {
                    bytes.extend([0, 0, 0, 8, 20, 0]);
                }
                let error = error_from_peer_bytes(&bytes, false).await;
                assert!(
                    matches!(error, MetadataFetchError::Timeout { stage: actual } if actual == stage)
                );
                assert_eq!(error.reason(), timeout_reason);
                assert!(!error.long_backoff());
                let error = error_from_peer_bytes(&bytes, true).await;
                assert!(
                    matches!(error, MetadataFetchError::Io { stage: actual, ref source } if actual == stage && source.kind() == std::io::ErrorKind::UnexpectedEof)
                );
                assert_eq!(error.reason(), io_reason);
                assert!(!error.long_backoff());
            }
        }
    }

    #[tokio::test]
    async fn write_failures_keep_their_protocol_stage() {
        for (stage, timeout_reason, io_reason) in [
            (
                IoStage::HandshakeWrite,
                MetadataFailureReason::HandshakeTimeout,
                MetadataFailureReason::HandshakeIo,
            ),
            (
                IoStage::ExtensionWrite,
                MetadataFailureReason::ExtensionTimeout,
                MetadataFailureReason::ExtensionIo,
            ),
            (
                IoStage::MetadataWrite,
                MetadataFailureReason::MetadataTimeout,
                MetadataFailureReason::MetadataIo,
            ),
        ] {
            let (mut client, peer) = tokio::io::duplex(1);
            let error = write_all_timeout(&mut client, &[0; 2], Duration::from_millis(10), stage)
                .await
                .unwrap_err();
            assert!(
                matches!(error, MetadataFetchError::Timeout { stage: actual } if actual == stage)
            );
            assert_eq!(error.reason(), timeout_reason);
            assert!(!error.long_backoff());
            drop(peer);
            let error = write_all_timeout(&mut client, &[0; 2], Duration::from_millis(10), stage)
                .await
                .unwrap_err();
            assert!(
                matches!(error, MetadataFetchError::Io { stage: actual, .. } if actual == stage)
            );
            assert_eq!(error.reason(), io_reason);
            assert!(!error.long_backoff());
        }
    }

    #[tokio::test]
    async fn metadata_rejection_uses_local_id_and_long_backoff() {
        let mut bytes = Handshake::new([0xAA; 20], [0; 20]).to_bytes();
        bytes.extend(negotiation_bytes(Some(3), Some(42)));
        bytes.extend(
            WireMessage::Extended {
                id: 1,
                payload: b"d8:msg_typei2e5:piecei0ee".to_vec(),
            }
            .to_bytes(),
        );
        let error = error_from_peer_bytes(&bytes, true).await;
        assert!(matches!(error, MetadataFetchError::MetadataRejected));
        assert_eq!(error.reason(), MetadataFailureReason::Rejected);
        assert!(error.long_backoff());
    }

    #[test]
    fn connection_errors_are_not_classified_by_message_text() {
        let error = MetadataFetchError::Io {
            stage: IoStage::Connect,
            source: std::io::Error::new(
                std::io::ErrorKind::ConnectionRefused,
                "extended handshake timeout",
            ),
        };
        assert_eq!(error.reason(), MetadataFailureReason::Connect);
        assert!(!error.long_backoff());
        let error = MetadataFetchError::Timeout {
            stage: IoStage::Connect,
        };
        assert_eq!(error.reason(), MetadataFailureReason::ConnectTimeout);
        assert!(!error.long_backoff());
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
        assert!(matches!(
            result.unwrap_err(),
            MetadataFetchError::InvalidHandshake
        ));
    }

    async fn fetch_single_piece_from_fake_peer(
        info_dict: &'static [u8],
        advertised_size: u32,
        response_total_size: i64,
        piece_data: Vec<u8>,
    ) -> Result<Vec<u8>, MetadataFetchError> {
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

    #[tokio::test]
    #[ignore = "requires BTFIND_TEST_DATABASE_URL; run scripts/test-mysql.sh"]
    async fn test_filter_peers_for_retry_keeps_new_peers_for_seen_hash() {
        let store = Store::open_test_database().await.unwrap();
        let info_hash = [0xDAu8; 20];
        let attempted_peer = PeerContact {
            addr: "1.1.1.1:6881".parse().unwrap(),
        };
        let new_peer = PeerContact {
            addr: "8.8.8.8:51413".parse().unwrap(),
        };
        store
            .upsert_torrent(info_hash, None, 0, 0, "dht")
            .await
            .unwrap();
        store
            .set_peer_attempt(
                &info_hash,
                &attempted_peer.addr.to_string(),
                Some("timeout"),
            )
            .await
            .unwrap();

        let peers = filter_peers_for_retry(
            &store,
            &info_hash,
            &[attempted_peer.clone(), new_peer.clone()],
            24,
        )
        .await;

        assert_eq!(peers.eligible, vec![new_peer]);
        assert!(peers.next_eligible_at.is_some());
    }

    #[tokio::test]
    #[ignore = "requires BTFIND_TEST_DATABASE_URL; run scripts/test-mysql.sh"]
    async fn test_filter_peers_for_retry_disabled_window_keeps_all_peers() {
        let store = Store::open_test_database().await.unwrap();
        let info_hash = [0xDBu8; 20];
        let attempted_peer = PeerContact {
            addr: "1.1.1.1:6881".parse().unwrap(),
        };
        store
            .upsert_torrent(info_hash, None, 0, 0, "dht")
            .await
            .unwrap();
        store
            .set_peer_attempt(
                &info_hash,
                &attempted_peer.addr.to_string(),
                Some("timeout"),
            )
            .await
            .unwrap();

        let peers =
            filter_peers_for_retry(&store, &info_hash, std::slice::from_ref(&attempted_peer), 0)
                .await;

        assert_eq!(peers.eligible, vec![attempted_peer]);
        assert_eq!(peers.next_eligible_at, None);
    }

    #[tokio::test]
    #[ignore = "requires BTFIND_TEST_DATABASE_URL; run scripts/test-mysql.sh"]
    async fn test_mark_metadata_complete_emits_metadata_fetched_stat() {
        let store = Store::open_test_database().await.unwrap();
        let info_hash = [0xABu8; 20];
        store
            .upsert_torrent(info_hash, None, 0, 0, "dht")
            .await
            .unwrap();
        let info = TorrentInfo {
            name: "test".to_string(),
            piece_length: 16384,
            total_size: 42,
            file_count: 1,
            files: vec![("test".to_string(), 42)],
        };
        let (stats_tx, mut stats_rx) = mpsc::channel(8);

        mark_metadata_complete_and_emit(&store, &info_hash, &info, &stats_tx)
            .await
            .unwrap();

        assert_eq!(
            stats_rx.try_recv().unwrap(),
            CrawlStatsEvent::MetadataFetched
        );
    }

    #[tokio::test]
    #[ignore = "requires BTFIND_TEST_DATABASE_URL; run scripts/test-mysql.sh"]
    async fn test_mark_metadata_complete_does_not_emit_when_file_insert_fails() {
        let store = Store::open_test_database().await.unwrap();
        let info_hash = [0xACu8; 20];
        store
            .upsert_torrent(info_hash, None, 0, 0, "dht")
            .await
            .unwrap();
        store
            .execute_batch_for_test(
                "
                CREATE TRIGGER fail_file_insert
                BEFORE INSERT ON files FOR EACH ROW
                SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='forced insert failure';
                ",
            )
            .await
            .unwrap();
        let info = TorrentInfo {
            name: "test".to_string(),
            piece_length: 16384,
            total_size: 42,
            file_count: 1,
            files: vec![("test".to_string(), 42)],
        };
        let (stats_tx, mut stats_rx) = mpsc::channel(8);

        let result = mark_metadata_complete_and_emit(&store, &info_hash, &info, &stats_tx).await;

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
