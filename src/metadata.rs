use crate::store::Store;
use crate::types::{CrawlStatsEvent, InfoHash, MetadataFailureReason, PeerContact};
use crate::wire::{self, ExtendedHandshake, Handshake, WireMessage, HANDSHAKE_LEN};
use sha1::{Digest, Sha1};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, Semaphore};
use tokio::task::JoinSet;
use tokio::time::timeout;

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

pub async fn fetch_from_peer(
    info_hash: &InfoHash,
    peer: &PeerContact,
    peer_timeout: Duration,
) -> Result<Vec<u8>, String> {
    let addr = std::net::SocketAddr::V4(peer.addr);
    let stream = timeout(peer_timeout, TcpStream::connect(addr))
        .await
        .map_err(|_| "connect timeout".to_string())?
        .map_err(|e| format!("connect error: {}", e))?;

    fetch_from_stream(info_hash, stream, peer_timeout).await
}

async fn fetch_from_stream<S>(
    info_hash: &InfoHash,
    mut stream: S,
    peer_timeout: Duration,
) -> Result<Vec<u8>, String>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let our_peer_id = crate::types::random_node_id();

    let hs = Handshake::new(*info_hash, our_peer_id);
    stream
        .write_all(&hs.to_bytes())
        .await
        .map_err(|e| format!("write handshake: {}", e))?;

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

    stream
        .write_all(&ext_msg.to_bytes())
        .await
        .map_err(|e| format!("write ext hs: {}", e))?;

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

    if metadata_size > 64 * 1024 * 1024 {
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
        stream
            .write_all(&req_msg.to_bytes())
            .await
            .map_err(|e| format!("write metadata request: {}", e))?;
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
            stream
                .write_all(&req_msg.to_bytes())
                .await
                .map_err(|e| format!("write metadata request: {}", e))?;
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

    let name = get_bencode_str(&dict, "name").unwrap_or_else(|| "unknown".to_string());
    let piece_length = get_bencode_int(&dict, "piece length").unwrap_or(0);

    let (total_size, files) =
        if let Some(serde_bencode::value::Value::List(file_list)) = dict.get(b"files".as_slice()) {
            let mut files_out = Vec::new();
            let mut total = 0i64;
            for file_val in file_list {
                if let serde_bencode::value::Value::Dict(file_dict) = file_val {
                    let file_size = get_bencode_int(file_dict, "length").unwrap_or(0);
                    total += file_size;

                    let mut path_parts = Vec::new();
                    if let Some(serde_bencode::value::Value::List(parts)) =
                        file_dict.get(b"path".as_slice())
                    {
                        for part in parts {
                            if let serde_bencode::value::Value::Bytes(b) = part {
                                path_parts.push(String::from_utf8_lossy(b).to_string());
                            }
                        }
                    }
                    let file_path = path_parts.join("/");
                    files_out.push((file_path, file_size));
                }
            }
            (total, files_out)
        } else if let Some(length) = get_bencode_int(&dict, "length") {
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

fn filter_peers_for_retry(
    store: &Store,
    info_hash: &InfoHash,
    peers: &[PeerContact],
    retry_after_hours: u32,
) -> Vec<PeerContact> {
    peers
        .iter()
        .filter_map(|peer| {
            let peer_addr = peer.addr.to_string();
            match store.should_skip_peer_retry(info_hash, &peer_addr, retry_after_hours) {
                Ok(true) => {
                    tracing::debug!(
                        "skipping metadata fetch for {} from {} (attempted within {}h)",
                        hex::encode(info_hash),
                        peer_addr,
                        retry_after_hours
                    );
                    None
                }
                Ok(false) => Some(peer.clone()),
                Err(e) => {
                    tracing::warn!(
                        "should_skip_peer_retry database error for {} from {}: {}, skipping",
                        hex::encode(info_hash),
                        peer_addr,
                        e
                    );
                    None
                }
            }
        })
        .collect()
}

fn classify_metadata_failure(error: &str) -> MetadataFailureReason {
    if error.contains("timeout") {
        MetadataFailureReason::Timeout
    } else if error.starts_with("connect") {
        MetadataFailureReason::Connect
    } else if error.contains("handshake") {
        MetadataFailureReason::Handshake
    } else if error.contains("ut_metadata")
        || error.contains("metadata_size")
        || error.contains("extended handshake")
        || error.contains("extensions")
    {
        MetadataFailureReason::Extension
    } else if error.contains("rejected") {
        MetadataFailureReason::Rejected
    } else if error.contains("metadata")
        || error.contains("piece")
        || error.contains("frame")
        || error.contains("bencode")
    {
        MetadataFailureReason::Protocol
    } else {
        MetadataFailureReason::Other
    }
}

fn emit_metadata_failure(
    stats_tx: &mpsc::UnboundedSender<CrawlStatsEvent>,
    reason: MetadataFailureReason,
) {
    if stats_tx
        .send(CrawlStatsEvent::MetadataFetchFailed { reason })
        .is_err()
    {
        tracing::warn!("stats_tx send failed after metadata fetch failure");
    }
}

fn mark_metadata_complete_and_emit(
    store: &Store,
    info_hash: &InfoHash,
    info: &TorrentInfo,
    stats_tx: &mpsc::UnboundedSender<CrawlStatsEvent>,
) -> Result<(), rusqlite::Error> {
    store.mark_metadata_complete(
        info_hash,
        &info.name,
        info.piece_length,
        info.total_size,
        info.file_count,
    )?;
    if !info.files.is_empty() {
        store.insert_files(info_hash, &info.files)?;
    }
    if stats_tx.send(CrawlStatsEvent::MetadataFetched).is_err() {
        tracing::warn!("stats_tx send failed after metadata fetch");
    }
    Ok(())
}

pub async fn run_metadata_fetcher(
    mut info_hash_rx: mpsc::UnboundedReceiver<(InfoHash, Vec<PeerContact>)>,
    store: Arc<Store>,
    max_concurrent: usize,
    peer_timeout_secs: u64,
    retry_after_hours: u32,
    stats_tx: mpsc::UnboundedSender<CrawlStatsEvent>,
) {
    let max_concurrent = if max_concurrent == 0 {
        tracing::warn!("run_metadata_fetcher called with max_concurrent=0, clamping to 1");
        1
    } else {
        max_concurrent
    };
    let semaphore = Arc::new(Semaphore::new(max_concurrent));
    let mut workers = JoinSet::new();

    loop {
        tokio::select! {
            message = info_hash_rx.recv() => {
                let Some((info_hash, peers)) = message else {
                    break;
                };
                let permit = match semaphore.clone().acquire_owned().await {
                    Ok(p) => p,
                    Err(_) => {
                        tracing::warn!("metadata semaphore closed, shutting down fetcher");
                        break;
                    }
                };
                let store = store.clone();
                let stats_tx = stats_tx.clone();
                let peer_timeout = Duration::from_secs(peer_timeout_secs);

                workers.spawn(async move {
                    let _permit = permit;

                    let hash_hex = hex::encode(info_hash);

                    match store.get_torrent(&info_hash) {
                        Ok(Some(record)) => {
                            if record.metadata_complete {
                                return;
                            }
                        }
                        Ok(None) => {}
                        Err(e) => {
                            tracing::warn!(
                                "get_torrent database error for {}: {}, skipping",
                                hash_hex,
                                e
                            );
                            return;
                        }
                    }

                    if let Err(e) = store.upsert_torrent(info_hash, None, 0, 0, "dht") {
                        tracing::warn!("failed to upsert torrent sighting for {}: {}", hash_hex, e);
                    }
                    tracing::info!("saw info_hash: {} with {} peers", hash_hex, peers.len());

                    if peers.is_empty() {
                        return;
                    }

                    let peers = filter_peers_for_retry(&store, &info_hash, &peers, retry_after_hours);
                    if peers.is_empty() {
                        return;
                    }

                    for peer in &peers {
                        let peer_addr = peer.addr.to_string();
                        if let Err(e) = store.set_peer_attempt(&info_hash, &peer_addr, None) {
                            tracing::warn!(
                                "failed to set peer attempt for {} from {}: {}",
                                hash_hex,
                                peer_addr,
                                e
                            );
                            continue;
                        }
                        match fetch_from_peer(&info_hash, peer, peer_timeout).await {
                            Ok(metadata_bytes) => {
                                if !verify_metadata_hash(&info_hash, &metadata_bytes) {
                                    tracing::warn!(
                                        "metadata hash mismatch for {}: got {} bytes that don't match info_hash",
                                        hash_hex, metadata_bytes.len()
                                    );
                                    if let Err(e) = store.set_peer_attempt(
                                        &info_hash,
                                        &peer_addr,
                                        Some("metadata hash mismatch"),
                                    ) {
                                        tracing::warn!(
                                            "failed to record peer failure for {} from {}: {}",
                                            hash_hex,
                                            peer_addr,
                                            e
                                        );
                                    }
                                    emit_metadata_failure(
                                        &stats_tx,
                                        MetadataFailureReason::HashMismatch,
                                    );
                                } else {
                                    match parse_torrent_info(&metadata_bytes) {
                                        Ok(info) => {
                                            tracing::info!(
                                                "got metadata for {}: name={}, size={}, files={}",
                                                hash_hex,
                                                info.name,
                                                info.total_size,
                                                info.file_count
                                            );
                                            if let Err(e) = mark_metadata_complete_and_emit(
                                                &store, &info_hash, &info, &stats_tx,
                                            ) {
                                                tracing::warn!("failed to mark metadata complete: {}", e);
                                            }
                                            return;
                                        }
                                        Err(e) => {
                                            tracing::debug!(
                                                "parse_torrent_info failed for {}: {}",
                                                hash_hex,
                                                e
                                            );
                                            if let Err(db_err) = store.set_peer_attempt(
                                                &info_hash,
                                                &peer_addr,
                                                Some(&e),
                                            ) {
                                                tracing::warn!(
                                                    "failed to record peer failure for {} from {}: {}",
                                                    hash_hex,
                                                    peer_addr,
                                                    db_err
                                                );
                                            }
                                            emit_metadata_failure(
                                                &stats_tx,
                                                MetadataFailureReason::Parse,
                                            );
                                        }
                                    }
                                }
                            }
                            Err(e) => {
                                tracing::debug!("fetch_from_peer failed for {}: {}", hash_hex, e);
                                if let Err(db_err) =
                                    store.set_peer_attempt(&info_hash, &peer_addr, Some(&e))
                                {
                                    tracing::warn!(
                                        "failed to record peer failure for {} from {}: {}",
                                        hash_hex,
                                        peer_addr,
                                        db_err
                                    );
                                }
                                emit_metadata_failure(&stats_tx, classify_metadata_failure(&e));
                            }
                        }
                    }
                    tracing::info!(
                        "failed to fetch metadata for {} from {} peers",
                        hash_hex,
                        peers.len()
                    );
                });
            }
            result = workers.join_next(), if !workers.is_empty() => {
                if let Some(Err(e)) = result {
                    tracing::warn!("metadata worker join error: {}", e);
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
    async fn test_fetch_from_stream_success() {
        let info_dict = b"d6:lengthi42e4:name8:test.txte";
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

        assert_eq!(peers, vec![new_peer]);
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

        assert_eq!(peers, vec![attempted_peer]);
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
        let (stats_tx, mut stats_rx) = mpsc::unbounded_channel();

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
        let (stats_tx, mut stats_rx) = mpsc::unbounded_channel();

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
