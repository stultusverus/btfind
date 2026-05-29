use crate::bencode::{KrpcError, KrpcMessage, KrpcValue};
use crate::routing::RoutingTable;
use crate::types::{
    random_node_id, CrawlStatsEvent, InfoHash, NodeContact, NodeId, PeerContact, NODE_ID_LEN,
    TRANSACTION_ID_LEN,
};
use rand::Rng;
use sha1::{Digest, Sha1};
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::net::SocketAddrV4;
use std::sync::atomic::{AtomicU16, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tokio::net::UdpSocket as TokioUdpSocket;
use tokio::sync::mpsc;
use tokio::time::{self, Duration};

static NEXT_TX: AtomicU16 = AtomicU16::new(1);

fn is_public_ipv4(ip: &std::net::Ipv4Addr) -> bool {
    let [a, b, c, _d] = ip.octets();
    if ip.is_unspecified()
        || ip.is_broadcast()
        || ip.is_multicast()
        || ip.is_loopback()
        || ip.is_private()
        || ip.is_link_local()
    {
        return false;
    }

    if a == 0 {
        return false;
    }
    if a == 100 && (64..=127).contains(&b) {
        return false;
    }
    if a == 192 && b == 0 && c == 0 {
        return false;
    }
    if a == 192 && b == 0 && c == 2 {
        return false;
    }
    if a == 192 && b == 88 && c == 99 {
        return false;
    }
    if a == 198 && b == 51 && c == 100 {
        return false;
    }
    if a == 203 && b == 0 && c == 113 {
        return false;
    }
    if a == 198 && (18..=19).contains(&b) {
        return false;
    }
    if a >= 240 {
        return false;
    }

    true
}

fn is_public_endpoint(addr: &SocketAddrV4) -> bool {
    addr.port() != 0 && is_public_ipv4(addr.ip())
}

fn next_transaction_id() -> u16 {
    NEXT_TX.fetch_add(1, Ordering::SeqCst)
}

fn transaction_id_bytes(id: u16) -> [u8; TRANSACTION_ID_LEN] {
    id.to_be_bytes()
}

fn transaction_id_from_bytes(tx: &[u8]) -> Option<u16> {
    if tx.len() != TRANSACTION_ID_LEN {
        return None;
    }
    Some(u16::from_be_bytes([tx[0], tx[1]]))
}

async fn resolve_v4(addr_str: &str) -> Option<SocketAddrV4> {
    if let Ok(addr) = addr_str.parse::<SocketAddrV4>() {
        return Some(addr);
    }
    match tokio::net::lookup_host(addr_str).await {
        Ok(addrs) => {
            for a in addrs {
                if let std::net::SocketAddr::V4(v4) = a {
                    return Some(v4);
                }
            }
            tracing::warn!("DNS resolved but no IPv4 found for '{}'", addr_str);
            None
        }
        Err(e) => {
            tracing::warn!("DNS resolution failed for '{}': {}", addr_str, e);
            None
        }
    }
}

struct PendingLookup {
    info_hash: InfoHash,
    is_real: bool,
    found_peers: Vec<PeerContact>,
    expected_responders: HashSet<SocketAddrV4>,
    started_at: Instant,
}

/// Build a "ping" query.
#[allow(dead_code)]
pub fn build_ping_query(node_id: &NodeId, tx_id: u16) -> KrpcMessage {
    let mut a = BTreeMap::new();
    a.insert("id".to_string(), KrpcValue::Bytes(node_id.to_vec()));

    KrpcMessage::Query {
        t: transaction_id_bytes(tx_id).to_vec(),
        y: "q".to_string(),
        q: "ping".to_string(),
        a,
    }
}

/// Build a "find_node" query.
pub fn build_find_node_query(node_id: &NodeId, target: &NodeId, tx_id: u16) -> KrpcMessage {
    let mut a = BTreeMap::new();
    a.insert("id".to_string(), KrpcValue::Bytes(node_id.to_vec()));
    a.insert("target".to_string(), KrpcValue::Bytes(target.to_vec()));

    KrpcMessage::Query {
        t: transaction_id_bytes(tx_id).to_vec(),
        y: "q".to_string(),
        q: "find_node".to_string(),
        a,
    }
}

/// Build a "get_peers" query.
pub fn build_get_peers_query(node_id: &NodeId, info_hash: &InfoHash, tx_id: u16) -> KrpcMessage {
    let mut a = BTreeMap::new();
    a.insert("id".to_string(), KrpcValue::Bytes(node_id.to_vec()));
    a.insert(
        "info_hash".to_string(),
        KrpcValue::Bytes(info_hash.to_vec()),
    );

    KrpcMessage::Query {
        t: transaction_id_bytes(tx_id).to_vec(),
        y: "q".to_string(),
        q: "get_peers".to_string(),
        a,
    }
}

/// Build a "find_node" response with compact node info.
pub fn build_find_node_response(node_id: &NodeId, nodes: &[u8], tx: Vec<u8>) -> KrpcMessage {
    let mut r = BTreeMap::new();
    r.insert("id".to_string(), KrpcValue::Bytes(node_id.to_vec()));
    r.insert("nodes".to_string(), KrpcValue::Bytes(nodes.to_vec()));

    KrpcMessage::Response {
        t: tx,
        y: "r".to_string(),
        r,
    }
}

/// Decode compact node info from a "nodes" response. Each entry is 26 bytes: 20-byte ID + 4-byte IP + 2-byte port.
pub fn decode_compact_nodes(data: &[u8]) -> Vec<NodeContact> {
    let mut contacts = Vec::new();
    let entry_size = NODE_ID_LEN + 6;

    for chunk in data.chunks(entry_size) {
        if chunk.len() < entry_size {
            break;
        }
        let mut id: NodeId = [0u8; NODE_ID_LEN];
        id.copy_from_slice(&chunk[..NODE_ID_LEN]);

        let ip = std::net::Ipv4Addr::new(chunk[20], chunk[21], chunk[22], chunk[23]);
        let addr = SocketAddrV4::new(ip, u16::from_be_bytes([chunk[24], chunk[25]]));
        if !is_public_endpoint(&addr) {
            continue;
        }

        contacts.push(NodeContact {
            id,
            addr,
            last_seen: std::time::Instant::now(),
        });
    }
    contacts
}

/// Encode node contacts into compact format for find_node responses.
pub fn encode_compact_nodes(nodes: &[NodeContact]) -> Vec<u8> {
    let mut data = Vec::with_capacity(nodes.len() * 26);
    for node in nodes {
        data.extend_from_slice(&node.id);
        data.extend_from_slice(&node.addr.ip().octets());
        data.extend_from_slice(&node.addr.port().to_be_bytes());
    }
    data
}

/// Parse a get_peers response. Returns (peers, nodes).
pub fn parse_get_peers_response(msg: &KrpcMessage) -> (Vec<PeerContact>, Vec<NodeContact>) {
    match msg {
        KrpcMessage::Response { r, .. } => {
            let peers = if let Some(KrpcValue::List(values)) = r.get("values") {
                decode_compact_peer_values(values)
            } else if let Some(KrpcValue::Bytes(data)) = r.get("values") {
                decode_compact_peers(data)
            } else {
                vec![]
            };

            let nodes = if let Some(KrpcValue::Bytes(data)) = r.get("nodes") {
                decode_compact_nodes(data)
            } else {
                vec![]
            };

            (peers, nodes)
        }
        _ => (vec![], vec![]),
    }
}

fn decode_compact_peer_values(values: &[KrpcValue]) -> Vec<PeerContact> {
    let mut peers = Vec::new();
    for val in values {
        if let KrpcValue::Bytes(data) = val {
            peers.extend(decode_compact_peers(data));
        }
    }
    peers
}

fn decode_compact_peers(data: &[u8]) -> Vec<PeerContact> {
    let mut peers = Vec::new();
    for chunk in data.chunks(6) {
        if chunk.len() < 6 {
            break;
        }
        let ip = std::net::Ipv4Addr::new(chunk[0], chunk[1], chunk[2], chunk[3]);
        let addr = SocketAddrV4::new(ip, u16::from_be_bytes([chunk[4], chunk[5]]));
        if !is_public_endpoint(&addr) {
            continue;
        }
        peers.push(PeerContact { addr });
    }
    peers
}

/// Extract the node ID from a KRPC message's "id" field.
pub fn extract_node_id(msg: &KrpcMessage) -> Option<NodeId> {
    let r = match msg {
        KrpcMessage::Response { r, .. } => Some(r),
        KrpcMessage::Query { a, .. } => Some(a),
        _ => None,
    }?;

    match r.get("id")? {
        KrpcValue::Bytes(b) if b.len() == NODE_ID_LEN => {
            let mut id: NodeId = [0u8; NODE_ID_LEN];
            id.copy_from_slice(b);
            Some(id)
        }
        _ => None,
    }
}

fn record_source_node(routing: &mut RoutingTable, msg: &KrpcMessage, src: SocketAddrV4) -> bool {
    if !is_public_endpoint(&src) {
        return false;
    }

    if let Some(node_id) = extract_node_id(msg) {
        let added = routing.add_node(node_id, src);
        routing.update_last_seen(&node_id);
        added
    } else {
        false
    }
}

fn dht_node_seen_event(msg: &KrpcMessage, src: SocketAddrV4) -> Option<CrawlStatsEvent> {
    if !is_public_endpoint(&src) {
        return None;
    }
    extract_node_id(msg).map(|id| CrawlStatsEvent::DhtNodeSeen { id, addr: src })
}

fn pop_real_hash(queue: &mut VecDeque<InfoHash>, set: &mut HashSet<InfoHash>) -> Option<InfoHash> {
    while let Some(ih) = queue.pop_front() {
        set.remove(&ih);
        if ih != [0u8; 20] {
            return Some(ih);
        }
    }
    None
}

fn seed_resume_info_hashes(
    discovery_queue: &mut VecDeque<InfoHash>,
    discovery_set: &mut HashSet<InfoHash>,
    hashes: impl IntoIterator<Item = InfoHash>,
) -> usize {
    let mut added = 0;
    for info_hash in hashes {
        if info_hash != [0u8; 20] && !discovery_set.contains(&info_hash) {
            discovery_queue.push_back(info_hash);
            discovery_set.insert(info_hash);
            added += 1;
        }
    }
    added
}

fn seed_resume_nodes(
    routing: &mut RoutingTable,
    nodes: impl IntoIterator<Item = NodeContact>,
) -> usize {
    let mut added = 0;
    for node in nodes {
        if routing.add_node(node.id, node.addr) {
            added += 1;
        }
    }
    added
}

fn handle_pending_lookup_response(
    pending_queries: &mut HashMap<Vec<u8>, PendingLookup>,
    info_hash_tx: &mpsc::UnboundedSender<(InfoHash, Vec<PeerContact>)>,
    tx: &[u8],
    src: SocketAddrV4,
    peers: &[PeerContact],
) {
    if let Some(lookup) = pending_queries.get_mut(tx) {
        if !lookup.expected_responders.contains(&src) {
            tracing::debug!(
                "ignoring response for {} from unexpected source {}",
                hex::encode(lookup.info_hash),
                src
            );
            return;
        }

        if !peers.is_empty() {
            let mut new_peers = Vec::new();
            for peer in peers {
                if !lookup.found_peers.contains(peer) {
                    lookup.found_peers.push(peer.clone());
                    new_peers.push(peer.clone());
                }
            }
            tracing::info!(
                "got {} peers for {} (total: {})",
                new_peers.len(),
                hex::encode(lookup.info_hash),
                lookup.found_peers.len()
            );
            if !new_peers.is_empty() && info_hash_tx.send((lookup.info_hash, new_peers)).is_err() {
                tracing::warn!(
                    "failed to send discovered peers for {}",
                    hex::encode(lookup.info_hash)
                );
            }
        }
    }
}

fn handle_response_message(
    routing: &mut RoutingTable,
    pending_queries: &mut HashMap<Vec<u8>, PendingLookup>,
    info_hash_tx: &mpsc::UnboundedSender<(InfoHash, Vec<PeerContact>)>,
    msg: &KrpcMessage,
    src: SocketAddrV4,
) -> bool {
    let KrpcMessage::Response { t, .. } = msg else {
        return false;
    };

    if let Some(lookup) = pending_queries.get(t) {
        if !lookup.expected_responders.contains(&src) {
            tracing::debug!(
                "ignoring response for {} from unexpected source {}",
                hex::encode(lookup.info_hash),
                src
            );
            return false;
        }
    }

    let (peers, nodes) = parse_get_peers_response(msg);
    for node in nodes {
        routing.add_node(node.id, node.addr);
    }

    handle_pending_lookup_response(pending_queries, info_hash_tx, t, src, &peers);
    true
}

fn select_lookup_continuation_nodes(
    routing: &RoutingTable,
    pending_queries: &HashMap<Vec<u8>, PendingLookup>,
    tx: &[u8],
    count: usize,
) -> Vec<NodeContact> {
    let Some(lookup) = pending_queries.get(tx) else {
        return Vec::new();
    };

    routing
        .closest_nodes(&lookup.info_hash, LOOKUP_CONTINUATION_CANDIDATES)
        .into_iter()
        .filter(|node| !lookup.expected_responders.contains(&node.addr))
        .take(count)
        .collect()
}

fn expire_pending_lookups(
    pending_queries: &mut HashMap<Vec<u8>, PendingLookup>,
    discovery_queue: &mut VecDeque<InfoHash>,
    discovery_set: &mut HashSet<InfoHash>,
) -> usize {
    let expired: Vec<Vec<u8>> = pending_queries
        .iter()
        .filter(|(_, lookup)| lookup.started_at.elapsed() >= PENDING_LOOKUP_TIMEOUT)
        .map(|(tx, _)| tx.clone())
        .collect();
    let expired_count = expired.len();

    for tx in expired {
        if let Some(lookup) = pending_queries.remove(&tx) {
            if lookup.is_real
                && lookup.found_peers.is_empty()
                && !discovery_set.contains(&lookup.info_hash)
            {
                discovery_queue.push_back(lookup.info_hash);
                discovery_set.insert(lookup.info_hash);
            }
        }
    }

    expired_count
}

fn current_token_bucket() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
        / 300
}

fn hmac_sha1(key: &[u8], data: &[u8]) -> [u8; 20] {
    let mut key_block = [0u8; 64];
    if key.len() > key_block.len() {
        let digest: [u8; 20] = Sha1::digest(key).into();
        key_block[..digest.len()].copy_from_slice(&digest);
    } else {
        key_block[..key.len()].copy_from_slice(key);
    }

    let mut ipad = [0x36u8; 64];
    let mut opad = [0x5cu8; 64];
    for i in 0..key_block.len() {
        ipad[i] ^= key_block[i];
        opad[i] ^= key_block[i];
    }

    let mut inner = Sha1::new();
    inner.update(ipad);
    inner.update(data);
    let inner_digest = inner.finalize();

    let mut outer = Sha1::new();
    outer.update(opad);
    outer.update(inner_digest);
    outer.finalize().into()
}

fn make_token_for_bucket(secret: &[u8; 16], src_ip: &std::net::Ipv4Addr, bucket: u64) -> [u8; 20] {
    let mut data = [0u8; 12];
    data[..4].copy_from_slice(&src_ip.octets());
    data[4..].copy_from_slice(&bucket.to_be_bytes());
    hmac_sha1(secret, &data)
}

fn make_token(secret: &[u8; 16], src_ip: &std::net::Ipv4Addr) -> Vec<u8> {
    make_token_for_bucket(secret, src_ip, current_token_bucket()).to_vec()
}

fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (left, right) in a.iter().zip(b.iter()) {
        diff |= left ^ right;
    }
    diff == 0
}

fn validate_token(secret: &[u8; 16], src_ip: &std::net::Ipv4Addr, token: &[u8]) -> bool {
    let current_bucket = current_token_bucket();
    for bucket in [current_bucket, current_bucket.saturating_sub(1)] {
        let expected = make_token_for_bucket(secret, src_ip, bucket);
        if constant_time_eq(&expected, token) {
            return true;
        }
    }
    false
}

fn select_announce_port(a: &BTreeMap<String, KrpcValue>, src: SocketAddrV4) -> Option<u16> {
    let implied_port = match a.get("implied_port") {
        Some(KrpcValue::Int(i)) => *i != 0,
        _ => false,
    };
    if implied_port {
        return Some(src.port());
    }

    match a.get("port") {
        Some(KrpcValue::Int(p)) if (1..=65535).contains(p) => Some(*p as u16),
        _ => None,
    }
}

fn announce_error(t: &[u8], description: &str) -> KrpcMessage {
    KrpcMessage::Error {
        t: t.to_vec(),
        y: "e".to_string(),
        e: KrpcError {
            code: 203,
            description: description.to_string(),
        },
    }
}

fn announce_success(our_id: &NodeId, t: &[u8]) -> KrpcMessage {
    let mut r = BTreeMap::new();
    r.insert("id".to_string(), KrpcValue::Bytes(our_id.to_vec()));
    KrpcMessage::Response {
        t: t.to_vec(),
        y: "r".to_string(),
        r,
    }
}

struct AnnounceContext<'a> {
    our_id: &'a NodeId,
    token_secret: &'a [u8; 16],
    info_hash_tx: &'a mpsc::UnboundedSender<(InfoHash, Vec<PeerContact>)>,
    discovery_queue: &'a mut VecDeque<InfoHash>,
    discovery_set: &'a mut HashSet<InfoHash>,
}

fn handle_announce_peer_query(
    ctx: AnnounceContext<'_>,
    src: SocketAddrV4,
    t: &[u8],
    a: &BTreeMap<String, KrpcValue>,
) -> KrpcMessage {
    if !is_public_endpoint(&src) {
        tracing::debug!("announce_peer from non-public source {}", src);
        return announce_error(t, "invalid source");
    }

    let Some(KrpcValue::Bytes(info_hash)) = a.get("info_hash") else {
        return announce_error(t, "missing info_hash");
    };
    if info_hash.len() != 20 {
        return announce_error(t, "invalid info_hash");
    }

    let token_valid = match a.get("token") {
        Some(KrpcValue::Bytes(token)) => validate_token(ctx.token_secret, src.ip(), token),
        _ => false,
    };
    if !token_valid {
        tracing::debug!("announce_peer from {} with invalid token", src);
        return announce_error(t, "invalid token");
    }

    let Some(port) = select_announce_port(a, src) else {
        tracing::debug!("announce_peer from {} with invalid or missing port", src);
        return announce_error(t, "invalid port");
    };

    let mut info_hash_arr: InfoHash = [0u8; 20];
    info_hash_arr.copy_from_slice(info_hash);
    let peer = PeerContact {
        addr: SocketAddrV4::new(*src.ip(), port),
    };
    if ctx.info_hash_tx.send((info_hash_arr, vec![peer])).is_err() {
        tracing::warn!("info_hash_tx send failed in announce_peer handler");
    }
    if !ctx.discovery_set.contains(&info_hash_arr) {
        ctx.discovery_queue.push_back(info_hash_arr);
        ctx.discovery_set.insert(info_hash_arr);
    }

    announce_success(ctx.our_id, t)
}

/// The DHT crawler state machine.
pub struct DhtCrawler {
    pub our_id: NodeId,
    pub socket: Arc<TokioUdpSocket>,
    pub routing: RoutingTable,
    pub info_hash_tx: mpsc::UnboundedSender<(InfoHash, Vec<PeerContact>)>,
    pub stats_tx: mpsc::UnboundedSender<CrawlStatsEvent>,
    token_secret: [u8; 16],
    bootstrap_nodes: Vec<String>,
    discovery_queue: VecDeque<InfoHash>,
    discovery_set: HashSet<InfoHash>,
    pending_queries: HashMap<Vec<u8>, PendingLookup>,
    queries_sent: usize,
}

const PENDING_LOOKUP_TIMEOUT: Duration = Duration::from_secs(30);
const LOOKUP_CONTINUATION_FANOUT: usize = 8;
const LOOKUP_CONTINUATION_CANDIDATES: usize = 64;

impl DhtCrawler {
    pub fn new(
        socket: Arc<TokioUdpSocket>,
        info_hash_tx: mpsc::UnboundedSender<(InfoHash, Vec<PeerContact>)>,
        stats_tx: mpsc::UnboundedSender<CrawlStatsEvent>,
        bootstrap_nodes: Vec<String>,
    ) -> Self {
        let our_id = random_node_id();
        let mut token_secret = [0u8; 16];
        rand::thread_rng().fill(&mut token_secret);
        DhtCrawler {
            our_id,
            socket,
            routing: RoutingTable::new(our_id),
            info_hash_tx,
            stats_tx,
            token_secret,
            bootstrap_nodes,
            discovery_queue: VecDeque::new(),
            discovery_set: HashSet::new(),
            pending_queries: HashMap::new(),
            queries_sent: 0,
        }
    }

    /// Bootstrap by sending find_node to configured bootstrap nodes.
    pub async fn bootstrap(&mut self) {
        tracing::info!("bootstrapping via {} nodes", self.bootstrap_nodes.len());
        for addr_str in &self.bootstrap_nodes.clone() {
            let addr: SocketAddrV4 = match resolve_v4(addr_str).await {
                Some(a) => a,
                None => {
                    tracing::warn!("cannot resolve bootstrap address '{}'", addr_str);
                    continue;
                }
            };
            let target = random_node_id();
            let msg = build_find_node_query(&self.our_id, &target, next_transaction_id());
            let data = msg.to_bytes();
            if let Err(e) = self.socket.send_to(&data, addr).await {
                tracing::warn!("bootstrap send error to {}: {}", addr, e);
            } else {
                self.queries_sent += 1;
                tracing::info!("bootstrap: sent find_node to {}", addr);
            }
        }
    }

    pub fn seed_info_hashes<I>(&mut self, hashes: I) -> usize
    where
        I: IntoIterator<Item = InfoHash>,
    {
        seed_resume_info_hashes(&mut self.discovery_queue, &mut self.discovery_set, hashes)
    }

    pub fn seed_nodes<I>(&mut self, nodes: I) -> usize
    where
        I: IntoIterator<Item = NodeContact>,
    {
        seed_resume_nodes(&mut self.routing, nodes)
    }

    /// Main crawl loop: read responses, send queries.
    pub async fn run(mut self, get_peers_interval: Duration, bucket_refresh: Duration) {
        let mut buf = vec![0u8; 8192];
        let mut peer_discover_tick = time::interval(get_peers_interval);
        let mut refresh_tick = time::interval(bucket_refresh);

        self.bootstrap().await;

        // Wait briefly for bootstrap responses to arrive
        tokio::time::sleep(Duration::from_secs(2)).await;

        peer_discover_tick.tick().await; // consume first immediate tick
        refresh_tick.tick().await;

        let mut stats_tick = tokio::time::interval(Duration::from_secs(60));

        loop {
            tokio::select! {
                result = self.socket.recv_from(&mut buf) => {
                    match result {
                        Ok((len, src)) => {
                            if len == 0 {
                                continue;
                            }
                            if self.routing.node_count() == 0 {
                                tracing::info!("received {} bytes from {} (no nodes yet)", len, src);
                            } else {
                                tracing::debug!("received {} bytes from {}", len, src);
                            }
                            if let std::net::SocketAddr::V4(src_v4) = src {
                                self.handle_packet(&buf[..len], src_v4).await;
                            } else {
                                tracing::debug!("ignoring IPv6 packet from {}", src);
                            }
                        }
                        Err(e) => {
                            tracing::error!("socket recv error: {}", e);
                        }
                    }
                }

                _ = peer_discover_tick.tick() => {
                    self.discover_peers().await;
                }

                _ = refresh_tick.tick() => {
                    let removed = self.routing.remove_stale_nodes(Duration::from_secs(900));
                    if removed > 0 {
                        tracing::debug!("removed {} stale nodes", removed);
                    }
                    let cleared = expire_pending_lookups(
                        &mut self.pending_queries,
                        &mut self.discovery_queue,
                        &mut self.discovery_set,
                    );
                    if cleared > 0 {
                        tracing::debug!("cleared {} stale pending lookups", cleared);
                    }
                }

                _ = stats_tick.tick() => {
                    let real_hashes = self.discovery_set.len();
                    let active_lookups = self.pending_queries.iter()
                        .filter(|(_, v)| v.started_at.elapsed() < PENDING_LOOKUP_TIMEOUT)
                        .count();
                    tracing::info!(
                        "crawl stats: {} nodes, {} real queued hashes, {} active lookups",
                        self.routing.node_count(),
                        real_hashes,
                        active_lookups,
                    );
                    if self
                        .stats_tx
                        .send(CrawlStatsEvent::DhtSnapshot {
                            nodes_known: self.routing.node_count(),
                            queries_sent: self.queries_sent,
                            info_hashes_found: real_hashes,
                        })
                        .is_err()
                    {
                        tracing::warn!("stats_tx send failed, receiver dropped");
                    }
                }
            }
        }
    }

    async fn handle_packet(&mut self, data: &[u8], src: SocketAddrV4) {
        let msg = match KrpcMessage::from_bytes(data) {
            Ok(m) => m,
            Err(e) => {
                tracing::debug!("bad krpc from {}: {}", src, e);
                return;
            }
        };

        let prev_count = self.routing.node_count();
        let added = record_source_node(&mut self.routing, &msg, src);
        if let Some(event) = dht_node_seen_event(&msg, src) {
            if self.stats_tx.send(event).is_err() {
                tracing::warn!("stats_tx send failed after DHT node sighting");
            }
        }
        let new_count = self.routing.node_count();
        if added && new_count <= 10 && prev_count != new_count {
            tracing::info!("added node {} (total: {})", src, new_count);
        } else if added && new_count.is_multiple_of(100) {
            tracing::info!("routing table: {} nodes", new_count);
        }

        match &msg {
            KrpcMessage::Query { q, a, t, .. } => match q.as_str() {
                "ping" => {
                    let mut r = BTreeMap::new();
                    r.insert("id".to_string(), KrpcValue::Bytes(self.our_id.to_vec()));
                    let resp = KrpcMessage::Response {
                        t: t.clone(),
                        y: "r".to_string(),
                        r,
                    };
                    if let Err(e) = self.socket.send_to(&resp.to_bytes(), src).await {
                        tracing::warn!("failed to send ping response to {}: {}", src, e);
                    }
                }
                "get_peers" => {
                    let mut info_hash_arr: InfoHash = [0u8; 20];
                    let has_hash = if let Some(KrpcValue::Bytes(info_hash)) = a.get("info_hash") {
                        if info_hash.len() != 20 {
                            false
                        } else {
                            info_hash_arr.copy_from_slice(info_hash);
                            true
                        }
                    } else {
                        false
                    };
                    if has_hash && !self.discovery_set.contains(&info_hash_arr) {
                        self.discovery_queue.push_back(info_hash_arr);
                        self.discovery_set.insert(info_hash_arr);
                    }
                    let target = if has_hash {
                        info_hash_arr
                    } else {
                        random_node_id()
                    };
                    let closest = self.routing.closest_nodes(&target, 8);
                    let nodes = encode_compact_nodes(&closest);
                    let mut r = BTreeMap::new();
                    r.insert("id".to_string(), KrpcValue::Bytes(self.our_id.to_vec()));
                    r.insert("nodes".to_string(), KrpcValue::Bytes(nodes));

                    let token = make_token(&self.token_secret, src.ip());
                    r.insert("token".to_string(), KrpcValue::Bytes(token));

                    let resp = KrpcMessage::Response {
                        t: t.clone(),
                        y: "r".to_string(),
                        r,
                    };
                    if let Err(e) = self.socket.send_to(&resp.to_bytes(), src).await {
                        tracing::warn!("failed to send get_peers response to {}: {}", src, e);
                    }
                }
                "find_node" => {
                    let target = if let Some(KrpcValue::Bytes(t)) = a.get("target") {
                        let mut tid: NodeId = [0u8; NODE_ID_LEN];
                        let len = t.len().min(NODE_ID_LEN);
                        tid[..len].copy_from_slice(&t[..len]);
                        tid
                    } else {
                        random_node_id()
                    };
                    let closest = self.routing.closest_nodes(&target, 8);
                    let nodes = encode_compact_nodes(&closest);
                    let resp = build_find_node_response(&self.our_id, &nodes, t.clone());
                    if let Err(e) = self.socket.send_to(&resp.to_bytes(), src).await {
                        tracing::warn!("failed to send find_node response to {}: {}", src, e);
                    }
                }
                "announce_peer" => {
                    let resp = handle_announce_peer_query(
                        AnnounceContext {
                            our_id: &self.our_id,
                            token_secret: &self.token_secret,
                            info_hash_tx: &self.info_hash_tx,
                            discovery_queue: &mut self.discovery_queue,
                            discovery_set: &mut self.discovery_set,
                        },
                        src,
                        t,
                        a,
                    );
                    if let Err(e) = self.socket.send_to(&resp.to_bytes(), src).await {
                        tracing::warn!("failed to send announce_peer response to {}: {}", src, e);
                    }
                }
                _ => {
                    tracing::debug!("unknown query type from {}: {}", src, q);
                }
            },
            KrpcMessage::Response { .. } => {
                let valid_response = handle_response_message(
                    &mut self.routing,
                    &mut self.pending_queries,
                    &self.info_hash_tx,
                    &msg,
                    src,
                );
                if valid_response {
                    if let KrpcMessage::Response { t, .. } = &msg {
                        self.send_lookup_continuation(t).await;
                    }
                }
            }
            KrpcMessage::Error { e, .. } => {
                tracing::debug!("krpc error from {}: {} ({})", src, e.description, e.code);
            }
        }
    }

    async fn send_lookup_continuation(&mut self, tx: &[u8]) {
        let Some(tx_id) = transaction_id_from_bytes(tx) else {
            return;
        };
        let Some(info_hash) = self.pending_queries.get(tx).map(|lookup| lookup.info_hash) else {
            return;
        };
        let nodes = select_lookup_continuation_nodes(
            &self.routing,
            &self.pending_queries,
            tx,
            LOOKUP_CONTINUATION_FANOUT,
        );

        for node in nodes {
            let msg = build_get_peers_query(&self.our_id, &info_hash, tx_id);
            let data = msg.to_bytes();
            if let Err(e) = self.socket.send_to(&data, node.addr).await {
                tracing::debug!("send continued get_peers to {}: {}", node.addr, e);
            } else {
                self.queries_sent += 1;
                if let Some(lookup) = self.pending_queries.get_mut(tx) {
                    lookup.expected_responders.insert(node.addr);
                }
            }
        }
    }

    async fn discover_peers(&mut self) {
        expire_pending_lookups(
            &mut self.pending_queries,
            &mut self.discovery_queue,
            &mut self.discovery_set,
        );

        let (info_hash, is_real) =
            if let Some(ih) = pop_real_hash(&mut self.discovery_queue, &mut self.discovery_set) {
                (ih, true)
            } else {
                (random_node_id(), false)
            };

        let closest = self.routing.closest_nodes(&info_hash, 16);

        if closest.is_empty() {
            if is_real {
                self.discovery_queue.push_back(info_hash);
                self.discovery_set.insert(info_hash);
            }
            self.bootstrap().await;
            return;
        }

        let tx = next_transaction_id();
        let tx_bytes = transaction_id_bytes(tx).to_vec();
        self.pending_queries.insert(
            tx_bytes,
            PendingLookup {
                info_hash,
                is_real,
                found_peers: Vec::new(),
                expected_responders: closest.iter().map(|node| node.addr).collect(),
                started_at: Instant::now(),
            },
        );

        for node in &closest {
            let msg = build_get_peers_query(&self.our_id, &info_hash, tx);
            let data = msg.to_bytes();
            if let Err(e) = self.socket.send_to(&data, node.addr).await {
                tracing::debug!("send get_peers to {}: {}", node.addr, e);
            } else {
                self.queries_sent += 1;
            }
        }

        tracing::debug!(
            "discover_peers: sent to {} nodes for {} hash, routing table has {} nodes, pending {}",
            closest.len(),
            if is_real { "real" } else { "random" },
            self.routing.node_count(),
            self.pending_queries.len()
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types;

    #[test]
    fn test_build_find_node_query() {
        let node_id: types::NodeId = [0xABu8; 20];
        let target: types::NodeId = [0xCDu8; 20];
        let msg = build_find_node_query(&node_id, &target, 42);
        assert!(matches!(msg, KrpcMessage::Query { .. }));
        if let KrpcMessage::Query { q, .. } = &msg {
            assert_eq!(q, "find_node");
        }
    }

    #[test]
    fn test_build_get_peers_query() {
        let node_id: types::NodeId = [0u8; 20];
        let info_hash: types::InfoHash = [0xFFu8; 20];
        let msg = build_get_peers_query(&node_id, &info_hash, 1);
        if let KrpcMessage::Query { q, .. } = &msg {
            assert_eq!(q, "get_peers");
        }
    }

    #[test]
    fn test_build_ping_query() {
        let node_id: types::NodeId = [0u8; 20];
        let msg = build_ping_query(&node_id, 99);
        if let KrpcMessage::Query { q, .. } = &msg {
            assert_eq!(q, "ping");
        }
    }

    #[test]
    fn test_parse_find_node_response() {
        let node_id: types::NodeId = [0x01u8; 20];
        let nodes = vec![
            1u8, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 1, 1, 1, 1,
            0x1A, 0xE1, // 1.1.1.1:6881
        ];
        let msg = build_find_node_response(&node_id, &nodes, transaction_id_bytes(42).to_vec());

        if let KrpcMessage::Response { r, .. } = &msg {
            assert!(r.contains_key("id"));
            assert!(r.contains_key("nodes"));
        } else {
            panic!("expected response");
        }
    }

    #[test]
    fn test_decode_compact_nodes() {
        let nodes = vec![
            1u8, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 1, 1, 1, 1,
            0x1A, 0xE1,
        ];
        let contacts = decode_compact_nodes(&nodes);
        assert_eq!(contacts.len(), 1);
        assert_eq!(
            contacts[0].id,
            [1u8, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20]
        );
    }

    #[test]
    fn test_parse_get_peers_response_values() {
        let node_id: types::NodeId = [0u8; 20];
        let mut r = std::collections::BTreeMap::new();
        r.insert("id".to_string(), KrpcValue::Bytes(node_id.to_vec()));
        r.insert("token".to_string(), KrpcValue::Bytes(b"token123".to_vec()));
        r.insert(
            "values".to_string(),
            KrpcValue::List(vec![KrpcValue::Bytes(vec![1, 1, 1, 1, 0x1A, 0xE1])]),
        );

        let msg = KrpcMessage::Response {
            t: transaction_id_bytes(1).to_vec(),
            y: "r".to_string(),
            r,
        };

        let (peers, _nodes) = parse_get_peers_response(&msg);
        assert_eq!(peers.len(), 1);
        assert_eq!(peers[0].addr.port(), 6881);
    }

    #[test]
    fn test_decode_compact_nodes_filters_invalid() {
        // port 0
        let nodes = vec![
            1u8, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 1, 1, 1, 1, 0,
            0, // 1.1.1.1:0
        ];
        let contacts = decode_compact_nodes(&nodes);
        assert_eq!(contacts.len(), 0, "port 0 should be filtered");

        // broadcast address
        let nodes = vec![
            1u8, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 0xff, 0xff,
            0xff, 0xff, 0x1A, 0xE1,
        ];
        let contacts = decode_compact_nodes(&nodes);
        assert_eq!(contacts.len(), 0, "broadcast should be filtered");

        // multicast address
        let nodes = vec![
            1u8, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 0xe0, 0, 0, 1,
            0x1A, 0xE1,
        ];
        let contacts = decode_compact_nodes(&nodes);
        assert_eq!(contacts.len(), 0, "multicast should be filtered");

        // unspecified (0.0.0.0)
        let nodes = vec![
            1u8, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 0, 0, 0, 0,
            0x1A, 0xE1,
        ];
        let contacts = decode_compact_nodes(&nodes);
        assert_eq!(contacts.len(), 0, "unspecified should be filtered");

        // loopback (127.0.0.1)
        let nodes = vec![
            1u8, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 0x7f, 0, 0, 1,
            0x1A, 0xE1,
        ];
        let contacts = decode_compact_nodes(&nodes);
        assert_eq!(contacts.len(), 0, "loopback should be filtered");

        // private (10.0.0.1)
        let nodes = vec![
            1u8, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 10, 0, 0, 1,
            0x1A, 0xE1,
        ];
        let contacts = decode_compact_nodes(&nodes);
        assert_eq!(contacts.len(), 0, "private should be filtered");

        // public IP (1.1.1.1) should be accepted
        let nodes = vec![
            1u8, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 1, 1, 1, 1,
            0x1A, 0xE1,
        ];
        let contacts = decode_compact_nodes(&nodes);
        assert_eq!(contacts.len(), 1, "public IP should be accepted");
    }

    #[test]
    fn test_decode_compact_peers_filters_invalid() {
        // port 0
        let peers_data = vec![0x7f, 0, 0, 1, 0, 0];
        let peers = decode_compact_peers(&peers_data);
        assert_eq!(peers.len(), 0, "port 0 should be filtered");

        // valid peer
        let peers_data = vec![1, 1, 1, 1, 0x1A, 0xE1];
        let peers = decode_compact_peers(&peers_data);
        assert_eq!(peers.len(), 1);
        assert_eq!(peers[0].addr.port(), 6881);
    }

    fn compact_node_with_ip(ip: std::net::Ipv4Addr) -> Vec<u8> {
        let mut data = vec![1u8; NODE_ID_LEN];
        data.extend_from_slice(&ip.octets());
        data.extend_from_slice(&6881u16.to_be_bytes());
        data
    }

    fn compact_peer_with_ip(ip: std::net::Ipv4Addr) -> Vec<u8> {
        let mut data = ip.octets().to_vec();
        data.extend_from_slice(&6881u16.to_be_bytes());
        data
    }

    #[test]
    fn test_decode_compact_filters_non_public_ipv4_ranges() {
        let rejected = [
            std::net::Ipv4Addr::new(192, 168, 0, 1),
            std::net::Ipv4Addr::new(172, 16, 0, 1),
            std::net::Ipv4Addr::new(169, 254, 1, 1),
            std::net::Ipv4Addr::new(100, 64, 0, 1),
            std::net::Ipv4Addr::new(192, 0, 0, 1),
            std::net::Ipv4Addr::new(192, 0, 2, 1),
            std::net::Ipv4Addr::new(198, 51, 100, 1),
            std::net::Ipv4Addr::new(203, 0, 113, 1),
            std::net::Ipv4Addr::new(198, 18, 0, 1),
        ];

        for ip in rejected {
            assert!(
                decode_compact_nodes(&compact_node_with_ip(ip)).is_empty(),
                "{} should be rejected as a node endpoint",
                ip
            );
            assert!(
                decode_compact_peers(&compact_peer_with_ip(ip)).is_empty(),
                "{} should be rejected as a peer endpoint",
                ip
            );
        }

        let public = std::net::Ipv4Addr::new(8, 8, 8, 8);
        assert_eq!(decode_compact_nodes(&compact_node_with_ip(public)).len(), 1);
        assert_eq!(decode_compact_peers(&compact_peer_with_ip(public)).len(), 1);
    }

    #[test]
    fn test_record_source_node_rejects_non_public_source() {
        let our_id: types::NodeId = [0xABu8; 20];
        let mut routing = RoutingTable::new(our_id);
        let mut r = BTreeMap::new();
        r.insert("id".to_string(), KrpcValue::Bytes([0xCDu8; 20].to_vec()));
        let msg = KrpcMessage::Response {
            t: transaction_id_bytes(1).to_vec(),
            y: "r".to_string(),
            r,
        };

        let added = record_source_node(
            &mut routing,
            &msg,
            SocketAddrV4::new(std::net::Ipv4Addr::new(127, 0, 0, 1), 6881),
        );

        assert!(!added);
        assert_eq!(routing.node_count(), 0);
    }

    #[test]
    fn test_dht_node_seen_event_from_public_source() {
        let src = SocketAddrV4::new(std::net::Ipv4Addr::new(8, 8, 8, 8), 6881);
        let node_id = [0xCDu8; 20];
        let mut r = BTreeMap::new();
        r.insert("id".to_string(), KrpcValue::Bytes(node_id.to_vec()));
        let msg = KrpcMessage::Response {
            t: transaction_id_bytes(1).to_vec(),
            y: "r".to_string(),
            r,
        };

        let event = dht_node_seen_event(&msg, src).unwrap();

        assert_eq!(
            event,
            CrawlStatsEvent::DhtNodeSeen {
                id: node_id,
                addr: src,
            }
        );
    }

    #[test]
    fn test_dht_node_seen_event_rejects_non_public_source() {
        let src = SocketAddrV4::new(std::net::Ipv4Addr::new(127, 0, 0, 1), 6881);
        let mut r = BTreeMap::new();
        r.insert("id".to_string(), KrpcValue::Bytes([0xCDu8; 20].to_vec()));
        let msg = KrpcMessage::Response {
            t: transaction_id_bytes(1).to_vec(),
            y: "r".to_string(),
            r,
        };

        assert!(dht_node_seen_event(&msg, src).is_none());
    }

    #[test]
    fn test_seed_resume_state_populates_queue_and_routing() {
        let mut discovery_queue = VecDeque::new();
        let mut discovery_set = HashSet::new();
        let mut routing = RoutingTable::new([0x11u8; 20]);
        let hashes = vec![[0xAAu8; 20], [0xBBu8; 20], [0xAAu8; 20]];
        let node = NodeContact {
            id: [0xCCu8; 20],
            addr: SocketAddrV4::new(std::net::Ipv4Addr::new(8, 8, 4, 4), 6881),
            last_seen: Instant::now(),
        };

        assert_eq!(
            seed_resume_info_hashes(&mut discovery_queue, &mut discovery_set, hashes),
            2
        );
        assert_eq!(seed_resume_nodes(&mut routing, vec![node]), 1);

        assert_eq!(discovery_queue.len(), 2);
        assert_eq!(discovery_set.len(), 2);
        assert_eq!(routing.node_count(), 1);
    }

    #[test]
    fn test_pending_lookup_ignores_unexpected_responder() {
        let tx = transaction_id_bytes(22).to_vec();
        let expected = SocketAddrV4::new(std::net::Ipv4Addr::new(1, 1, 1, 1), 6881);
        let unexpected = SocketAddrV4::new(std::net::Ipv4Addr::new(8, 8, 8, 8), 6881);
        let info_hash = [0xAAu8; 20];
        let peer = PeerContact {
            addr: SocketAddrV4::new(std::net::Ipv4Addr::new(9, 9, 9, 9), 51413),
        };
        let mut pending = HashMap::new();
        pending.insert(
            tx.clone(),
            PendingLookup {
                info_hash,
                is_real: true,
                found_peers: Vec::new(),
                expected_responders: [expected].into_iter().collect(),
                started_at: Instant::now(),
            },
        );
        let (info_hash_tx, mut info_hash_rx) = mpsc::unbounded_channel();

        handle_pending_lookup_response(&mut pending, &info_hash_tx, &tx, unexpected, &[peer]);

        assert!(pending.contains_key(&tx));
        assert!(info_hash_rx.try_recv().is_err());
    }

    #[test]
    fn test_pending_lookup_keeps_active_after_peer_response() {
        let tx = transaction_id_bytes(24).to_vec();
        let expected = SocketAddrV4::new(std::net::Ipv4Addr::new(1, 1, 1, 1), 6881);
        let info_hash = [0xAAu8; 20];
        let peer = PeerContact {
            addr: SocketAddrV4::new(std::net::Ipv4Addr::new(9, 9, 9, 9), 51413),
        };
        let mut pending = HashMap::new();
        pending.insert(
            tx.clone(),
            PendingLookup {
                info_hash,
                is_real: true,
                found_peers: Vec::new(),
                expected_responders: [expected].into_iter().collect(),
                started_at: Instant::now(),
            },
        );
        let (info_hash_tx, mut info_hash_rx) = mpsc::unbounded_channel();

        handle_pending_lookup_response(
            &mut pending,
            &info_hash_tx,
            &tx,
            expected,
            std::slice::from_ref(&peer),
        );

        assert!(pending.contains_key(&tx));
        assert_eq!(info_hash_rx.try_recv().unwrap(), (info_hash, vec![peer]));
    }

    #[test]
    fn test_select_lookup_continuation_nodes_returns_unqueried_nodes() {
        let tx = transaction_id_bytes(25).to_vec();
        let queried = SocketAddrV4::new(std::net::Ipv4Addr::new(1, 1, 1, 1), 6881);
        let returned = SocketAddrV4::new(std::net::Ipv4Addr::new(8, 8, 4, 4), 6881);
        let info_hash = [0xAAu8; 20];
        let mut pending = HashMap::new();
        pending.insert(
            tx.clone(),
            PendingLookup {
                info_hash,
                is_real: true,
                found_peers: Vec::new(),
                expected_responders: [queried].into_iter().collect(),
                started_at: Instant::now(),
            },
        );
        let mut routing = RoutingTable::new([0x11u8; 20]);
        routing.add_node([0x22u8; 20], queried);
        routing.add_node([0xAAu8; 20], returned);

        let selected = select_lookup_continuation_nodes(&routing, &pending, &tx, 8);

        assert_eq!(selected.len(), 1);
        assert_eq!(selected[0].addr, returned);
    }

    #[test]
    fn test_unexpected_lookup_response_does_not_add_nodes() {
        let tx = transaction_id_bytes(23).to_vec();
        let expected = SocketAddrV4::new(std::net::Ipv4Addr::new(1, 1, 1, 1), 6881);
        let unexpected = SocketAddrV4::new(std::net::Ipv4Addr::new(8, 8, 8, 8), 6881);
        let info_hash = [0xAAu8; 20];
        let mut pending = HashMap::new();
        pending.insert(
            tx.clone(),
            PendingLookup {
                info_hash,
                is_real: true,
                found_peers: Vec::new(),
                expected_responders: [expected].into_iter().collect(),
                started_at: Instant::now(),
            },
        );
        let mut routing = RoutingTable::new([0x11u8; 20]);
        let (info_hash_tx, mut info_hash_rx) = mpsc::unbounded_channel();
        let mut r = BTreeMap::new();
        r.insert("id".to_string(), KrpcValue::Bytes([0x22u8; 20].to_vec()));
        r.insert(
            "nodes".to_string(),
            KrpcValue::Bytes(compact_node_with_ip(std::net::Ipv4Addr::new(8, 8, 4, 4))),
        );
        r.insert(
            "values".to_string(),
            KrpcValue::List(vec![KrpcValue::Bytes(compact_peer_with_ip(
                std::net::Ipv4Addr::new(9, 9, 9, 9),
            ))]),
        );
        let msg = KrpcMessage::Response {
            t: tx.clone(),
            y: "r".to_string(),
            r,
        };

        handle_response_message(&mut routing, &mut pending, &info_hash_tx, &msg, unexpected);

        assert_eq!(routing.node_count(), 0);
        assert!(pending.contains_key(&tx));
        assert!(info_hash_rx.try_recv().is_err());
    }

    #[test]
    fn test_hmac_tokens_validate_current_and_previous_bucket() {
        let secret = [0x42u8; 16];
        let ip = std::net::Ipv4Addr::new(1, 2, 3, 4);
        let bucket = current_token_bucket();
        let current = make_token_for_bucket(&secret, &ip, bucket);
        let previous = make_token_for_bucket(&secret, &ip, bucket.saturating_sub(1));

        assert!(validate_token(&secret, &ip, &current));
        assert!(validate_token(&secret, &ip, &previous));
    }

    #[test]
    fn test_hmac_tokens_reject_forged_or_different_ip() {
        let secret = [0x42u8; 16];
        let ip = std::net::Ipv4Addr::new(1, 2, 3, 4);
        let other_ip = std::net::Ipv4Addr::new(1, 2, 3, 5);
        let token = make_token_for_bucket(&secret, &ip, current_token_bucket());

        assert!(!validate_token(&secret, &ip, b"btfn"));
        assert!(!validate_token(&secret, &other_ip, &token));
    }

    #[test]
    fn test_hmac_token_bucket_uses_u64_value() {
        let secret = [0x42u8; 16];
        let ip = std::net::Ipv4Addr::new(1, 2, 3, 4);
        let bucket = u32::MAX as u64 + 1;

        assert_ne!(
            make_token_for_bucket(&secret, &ip, bucket),
            make_token_for_bucket(&secret, &ip, bucket + 1)
        );
    }

    #[test]
    fn test_select_announce_port() {
        let src = SocketAddrV4::new(std::net::Ipv4Addr::new(1, 1, 1, 1), 6881);
        let mut args = BTreeMap::new();
        args.insert("implied_port".to_string(), KrpcValue::Int(1));
        assert_eq!(select_announce_port(&args, src), Some(6881));

        args.clear();
        args.insert("port".to_string(), KrpcValue::Int(51413));
        assert_eq!(select_announce_port(&args, src), Some(51413));

        args.clear();
        assert_eq!(select_announce_port(&args, src), None);

        for port in [-1, 0, 70000] {
            args.clear();
            args.insert("port".to_string(), KrpcValue::Int(port));
            assert_eq!(select_announce_port(&args, src), None);
        }
    }

    fn announce_args(
        info_hash: InfoHash,
        token: Vec<u8>,
        port: Option<i64>,
    ) -> BTreeMap<String, KrpcValue> {
        let mut args = BTreeMap::new();
        args.insert(
            "info_hash".to_string(),
            KrpcValue::Bytes(info_hash.to_vec()),
        );
        args.insert("token".to_string(), KrpcValue::Bytes(token));
        if let Some(port) = port {
            args.insert("port".to_string(), KrpcValue::Int(port));
        }
        args
    }

    #[test]
    fn test_invalid_announce_peer_returns_error_without_sending_peers() {
        let our_id = [0x11u8; 20];
        let secret = [0x42u8; 16];
        let src = SocketAddrV4::new(std::net::Ipv4Addr::new(1, 1, 1, 1), 6881);
        let info_hash = [0xAAu8; 20];
        let valid_token = make_token(&secret, src.ip());

        let cases = [
            announce_args(info_hash, b"bad-token".to_vec(), Some(6881)),
            announce_args(info_hash, valid_token.clone(), None),
            announce_args(info_hash, valid_token.clone(), Some(0)),
            announce_args(info_hash, valid_token.clone(), Some(-1)),
            announce_args(info_hash, valid_token, Some(70000)),
        ];

        for args in cases {
            let (info_hash_tx, mut info_hash_rx) = mpsc::unbounded_channel();
            let mut discovery_queue = VecDeque::new();
            let mut discovery_set = HashSet::new();

            let resp = handle_announce_peer_query(
                AnnounceContext {
                    our_id: &our_id,
                    token_secret: &secret,
                    info_hash_tx: &info_hash_tx,
                    discovery_queue: &mut discovery_queue,
                    discovery_set: &mut discovery_set,
                },
                src,
                &transaction_id_bytes(7),
                &args,
            );

            assert!(matches!(resp, KrpcMessage::Error { .. }));
            assert!(info_hash_rx.try_recv().is_err());
            assert!(discovery_queue.is_empty());
            assert!(discovery_set.is_empty());
        }
    }

    #[test]
    fn test_announce_peer_rejects_non_public_source() {
        let our_id = [0x11u8; 20];
        let secret = [0x42u8; 16];
        let src = SocketAddrV4::new(std::net::Ipv4Addr::new(127, 0, 0, 1), 6881);
        let info_hash = [0xAAu8; 20];
        let args = announce_args(info_hash, make_token(&secret, src.ip()), Some(6881));
        let (info_hash_tx, mut info_hash_rx) = mpsc::unbounded_channel();
        let mut discovery_queue = VecDeque::new();
        let mut discovery_set = HashSet::new();

        let resp = handle_announce_peer_query(
            AnnounceContext {
                our_id: &our_id,
                token_secret: &secret,
                info_hash_tx: &info_hash_tx,
                discovery_queue: &mut discovery_queue,
                discovery_set: &mut discovery_set,
            },
            src,
            &transaction_id_bytes(8),
            &args,
        );

        assert!(matches!(resp, KrpcMessage::Error { .. }));
        assert!(info_hash_rx.try_recv().is_err());
        assert!(discovery_queue.is_empty());
        assert!(discovery_set.is_empty());
    }
}
