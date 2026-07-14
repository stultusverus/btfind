use crate::bencode::{KrpcError, KrpcMessage, KrpcValue};
use crate::routing::RoutingTable;
use crate::types::{
    random_node_id, CrawlStatsEvent, DiscoverySource, HashDiscovery, InfoHash, NodeContact, NodeId,
    PeerContact, NODE_ID_LEN, TRANSACTION_ID_LEN,
};
use rand::Rng;
use sha1::{Digest, Sha1};
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::net::{Ipv4Addr, SocketAddrV4};
use std::sync::atomic::{AtomicU16, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tokio::net::UdpSocket as TokioUdpSocket;
use tokio::sync::mpsc;
use tokio::time::{self, Duration};

static NEXT_TX: AtomicU16 = AtomicU16::new(1);
const LOOKUP_FANOUT: usize = 8;
const LOOKUP_CONTINUATION_FANOUT: usize = 4;
const MAX_PACKET_SIZE: usize = 8192;

#[derive(Debug, Clone)]
pub struct DhtConfig {
    pub max_pending_rpcs: usize,
    pub max_discovery_hashes: usize,
    pub max_candidate_nodes: usize,
    pub max_peers_per_hash: usize,
    pub rpc_timeout: Duration,
    pub sampling_enabled: bool,
    pub sampling_interval: Duration,
    pub sampling_min_remote_interval: Duration,
    pub sampling_requests_per_tick: usize,
    pub max_samples_per_response: usize,
    pub announced_peer_hash_capacity: usize,
    pub announced_peers_per_hash: usize,
    pub announced_peer_ttl: Duration,
}

impl Default for DhtConfig {
    fn default() -> Self {
        Self {
            max_pending_rpcs: 4096,
            max_discovery_hashes: 50_000,
            max_candidate_nodes: 8192,
            max_peers_per_hash: 64,
            rpc_timeout: Duration::from_secs(30),
            sampling_enabled: false,
            sampling_interval: Duration::from_secs(5),
            sampling_min_remote_interval: Duration::from_secs(300),
            sampling_requests_per_tick: 1,
            max_samples_per_response: 256,
            announced_peer_hash_capacity: 10_000,
            announced_peers_per_hash: 64,
            announced_peer_ttl: Duration::from_secs(1800),
        }
    }
}

#[allow(dead_code)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RpcMethod {
    Ping,
    FindNode,
    GetPeers,
    SampleInfohashes,
}

#[derive(Debug, Clone)]
struct PendingRpc {
    expected_addr: SocketAddrV4,
    expected_node_id: Option<NodeId>,
    method: RpcMethod,
    target: NodeId,
    info_hash: Option<InfoHash>,
    is_real_lookup: bool,
    deadline: Instant,
    continuation_budget: u8,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CorrelationError {
    Unsolicited,
    UnexpectedSource,
    Malformed,
}

fn correlate_response(
    pending_rpcs: &mut HashMap<Vec<u8>, PendingRpc>,
    message: &KrpcMessage,
    source: SocketAddrV4,
) -> Result<(PendingRpc, NodeId), CorrelationError> {
    let transaction = message.transaction_id();
    let Some(pending) = pending_rpcs.get(transaction) else {
        return Err(CorrelationError::Unsolicited);
    };
    if pending.expected_addr != source {
        return Err(CorrelationError::UnexpectedSource);
    }
    let Some(response_id) = extract_node_id(message) else {
        pending_rpcs.remove(transaction);
        return Err(CorrelationError::Malformed);
    };
    if pending
        .expected_node_id
        .is_some_and(|expected| expected != response_id)
        || !response_has_shape(message, pending.method)
    {
        pending_rpcs.remove(transaction);
        return Err(CorrelationError::Malformed);
    }
    let pending = pending_rpcs
        .remove(transaction)
        .expect("correlated pending RPC exists until removal");
    Ok((pending, response_id))
}

#[derive(Debug)]
pub struct SampleInfohashesResponse {
    pub node_id: NodeId,
    pub interval: Duration,
    pub total_num: u64,
    pub samples: Vec<InfoHash>,
    pub nodes: Vec<NodeContact>,
}

#[derive(Clone)]
struct CachedPeer {
    peer: PeerContact,
    expires_at: Instant,
}

fn is_public_ipv4(ip: &Ipv4Addr) -> bool {
    let [a, b, c, _] = ip.octets();
    !(ip.is_unspecified()
        || ip.is_broadcast()
        || ip.is_multicast()
        || ip.is_loopback()
        || ip.is_private()
        || ip.is_link_local()
        || a == 0
        || (a == 100 && (64..=127).contains(&b))
        || (a == 192 && b == 0 && c == 0)
        || (a == 192 && b == 0 && c == 2)
        || (a == 192 && b == 88 && c == 99)
        || (a == 198 && b == 51 && c == 100)
        || (a == 203 && b == 0 && c == 113)
        || (a == 198 && (18..=19).contains(&b))
        || a >= 240)
}

fn is_public_endpoint(addr: &SocketAddrV4) -> bool {
    addr.port() != 0 && is_public_ipv4(addr.ip())
}

fn transaction_id_bytes(id: u16) -> [u8; TRANSACTION_ID_LEN] {
    id.to_be_bytes()
}

async fn resolve_v4(addr: &str) -> Option<SocketAddrV4> {
    if let Ok(addr) = addr.parse() {
        return Some(addr);
    }
    match tokio::net::lookup_host(addr).await {
        Ok(mut addresses) => addresses.find_map(|address| match address {
            std::net::SocketAddr::V4(address) => Some(address),
            std::net::SocketAddr::V6(_) => None,
        }),
        Err(error) => {
            tracing::warn!("DNS resolution failed for '{}': {}", addr, error);
            None
        }
    }
}

fn query_message(method: RpcMethod, our_id: &NodeId, target: &NodeId, tx: u16) -> KrpcMessage {
    let mut args = BTreeMap::new();
    args.insert("id".to_string(), KrpcValue::Bytes(our_id.to_vec()));
    let query = match method {
        RpcMethod::Ping => "ping",
        RpcMethod::FindNode => {
            args.insert("target".to_string(), KrpcValue::Bytes(target.to_vec()));
            "find_node"
        }
        RpcMethod::GetPeers => {
            args.insert("info_hash".to_string(), KrpcValue::Bytes(target.to_vec()));
            "get_peers"
        }
        RpcMethod::SampleInfohashes => {
            args.insert("target".to_string(), KrpcValue::Bytes(target.to_vec()));
            "sample_infohashes"
        }
    };
    let mut extra = BTreeMap::new();
    extra.insert("v".to_string(), KrpcValue::Bytes(b"BF01".to_vec()));
    KrpcMessage::Query {
        t: transaction_id_bytes(tx).to_vec(),
        y: "q".to_string(),
        q: query.to_string(),
        a: args,
        extra,
    }
}

#[allow(dead_code)]
pub fn build_ping_query(node_id: &NodeId, tx_id: u16) -> KrpcMessage {
    query_message(RpcMethod::Ping, node_id, node_id, tx_id)
}

#[allow(dead_code)]
pub fn build_find_node_query(node_id: &NodeId, target: &NodeId, tx_id: u16) -> KrpcMessage {
    query_message(RpcMethod::FindNode, node_id, target, tx_id)
}

#[allow(dead_code)]
pub fn build_get_peers_query(node_id: &NodeId, info_hash: &InfoHash, tx_id: u16) -> KrpcMessage {
    query_message(RpcMethod::GetPeers, node_id, info_hash, tx_id)
}

#[allow(dead_code)]
pub fn build_sample_infohashes_query(node_id: &NodeId, target: &NodeId, tx_id: u16) -> KrpcMessage {
    query_message(RpcMethod::SampleInfohashes, node_id, target, tx_id)
}

#[allow(dead_code)]
pub fn build_find_node_response(node_id: &NodeId, nodes: &[u8], tx: Vec<u8>) -> KrpcMessage {
    let mut response = BTreeMap::new();
    response.insert("id".to_string(), KrpcValue::Bytes(node_id.to_vec()));
    response.insert("nodes".to_string(), KrpcValue::Bytes(nodes.to_vec()));
    KrpcMessage::Response {
        t: tx,
        y: "r".to_string(),
        r: response,
        extra: BTreeMap::new(),
    }
}

pub fn decode_compact_nodes(data: &[u8]) -> Vec<NodeContact> {
    if !data.len().is_multiple_of(NODE_ID_LEN + 6) {
        return Vec::new();
    }
    data.chunks_exact(NODE_ID_LEN + 6)
        .filter_map(|chunk| {
            let mut id = [0; NODE_ID_LEN];
            id.copy_from_slice(&chunk[..NODE_ID_LEN]);
            let addr = SocketAddrV4::new(
                Ipv4Addr::new(chunk[20], chunk[21], chunk[22], chunk[23]),
                u16::from_be_bytes([chunk[24], chunk[25]]),
            );
            is_public_endpoint(&addr).then(|| NodeContact {
                id,
                addr,
                last_seen: Instant::now(),
                last_seen_unix: chrono::Utc::now().timestamp(),
            })
        })
        .collect()
}

pub fn encode_compact_nodes(nodes: &[NodeContact]) -> Vec<u8> {
    let mut data = Vec::with_capacity(nodes.len() * (NODE_ID_LEN + 6));
    for node in nodes {
        data.extend_from_slice(&node.id);
        data.extend_from_slice(&node.addr.ip().octets());
        data.extend_from_slice(&node.addr.port().to_be_bytes());
    }
    data
}

fn decode_compact_peers(data: &[u8]) -> Vec<PeerContact> {
    if !data.len().is_multiple_of(6) {
        return Vec::new();
    }
    data.chunks_exact(6)
        .filter_map(|chunk| {
            let addr = SocketAddrV4::new(
                Ipv4Addr::new(chunk[0], chunk[1], chunk[2], chunk[3]),
                u16::from_be_bytes([chunk[4], chunk[5]]),
            );
            is_public_endpoint(&addr).then_some(PeerContact { addr })
        })
        .collect()
}

fn encode_compact_peer(peer: &PeerContact) -> Vec<u8> {
    let mut data = peer.addr.ip().octets().to_vec();
    data.extend_from_slice(&peer.addr.port().to_be_bytes());
    data
}

pub fn parse_get_peers_response(msg: &KrpcMessage) -> (Vec<PeerContact>, Vec<NodeContact>) {
    let KrpcMessage::Response { r, .. } = msg else {
        return (Vec::new(), Vec::new());
    };
    let peers = match r.get("values") {
        Some(KrpcValue::List(values)) => values
            .iter()
            .filter_map(|value| match value {
                KrpcValue::Bytes(bytes) => Some(decode_compact_peers(bytes)),
                _ => None,
            })
            .flatten()
            .collect(),
        Some(KrpcValue::Bytes(bytes)) => decode_compact_peers(bytes),
        _ => Vec::new(),
    };
    let nodes = match r.get("nodes") {
        Some(KrpcValue::Bytes(bytes)) => decode_compact_nodes(bytes),
        _ => Vec::new(),
    };
    (peers, nodes)
}

pub fn parse_sample_infohashes_response(
    msg: &KrpcMessage,
    max_samples: usize,
) -> Result<SampleInfohashesResponse, String> {
    let KrpcMessage::Response { r, .. } = msg else {
        return Err("not a response".to_string());
    };
    let node_id = extract_node_id(msg).ok_or_else(|| "missing response node id".to_string())?;
    let interval = match r.get("interval") {
        Some(KrpcValue::Int(value)) if (1..=86_400).contains(value) => {
            Duration::from_secs(*value as u64)
        }
        _ => return Err("invalid sample interval".to_string()),
    };
    let total_num = match r.get("num") {
        Some(KrpcValue::Int(value)) if *value >= 0 => *value as u64,
        _ => return Err("invalid sample count".to_string()),
    };
    let bytes = match r.get("samples") {
        Some(KrpcValue::Bytes(bytes)) => bytes,
        _ => return Err("missing samples".to_string()),
    };
    if !bytes.len().is_multiple_of(20) || bytes.len() / 20 > max_samples {
        return Err("invalid samples payload length".to_string());
    }
    let samples = bytes
        .chunks_exact(20)
        .map(|chunk| {
            let mut hash = [0; 20];
            hash.copy_from_slice(chunk);
            hash
        })
        .collect();
    let nodes = match r.get("nodes") {
        Some(KrpcValue::Bytes(bytes)) => decode_compact_nodes(bytes),
        _ => Vec::new(),
    };
    Ok(SampleInfohashesResponse {
        node_id,
        interval,
        total_num,
        samples,
        nodes,
    })
}

pub fn extract_node_id(msg: &KrpcMessage) -> Option<NodeId> {
    let values = match msg {
        KrpcMessage::Query { a, .. } => a,
        KrpcMessage::Response { r, .. } => r,
        KrpcMessage::Error { .. } => return None,
    };
    let KrpcValue::Bytes(bytes) = values.get("id")? else {
        return None;
    };
    if bytes.len() != NODE_ID_LEN {
        return None;
    }
    let mut id = [0; NODE_ID_LEN];
    id.copy_from_slice(bytes);
    Some(id)
}

fn response_has_shape(msg: &KrpcMessage, method: RpcMethod) -> bool {
    let KrpcMessage::Response { r, .. } = msg else {
        return false;
    };
    if extract_node_id(msg).is_none() {
        return false;
    }
    match method {
        RpcMethod::Ping => true,
        RpcMethod::FindNode => matches!(r.get("nodes"), Some(KrpcValue::Bytes(_))),
        RpcMethod::GetPeers => {
            matches!(r.get("nodes"), Some(KrpcValue::Bytes(_)))
                || matches!(
                    r.get("values"),
                    Some(KrpcValue::List(_) | KrpcValue::Bytes(_))
                )
        }
        RpcMethod::SampleInfohashes => {
            matches!(r.get("interval"), Some(KrpcValue::Int(_)))
                && matches!(r.get("num"), Some(KrpcValue::Int(_)))
                && matches!(r.get("samples"), Some(KrpcValue::Bytes(_)))
        }
    }
}

fn current_token_bucket() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
        / 300
}

fn hmac_sha1(key: &[u8], data: &[u8]) -> [u8; 20] {
    const BLOCK_SIZE: usize = 64;
    let mut normalized = [0; BLOCK_SIZE];
    if key.len() > BLOCK_SIZE {
        let digest: [u8; 20] = Sha1::digest(key).into();
        normalized[..digest.len()].copy_from_slice(&digest);
    } else {
        normalized[..key.len()].copy_from_slice(key);
    }
    let mut inner_pad = [0x36; BLOCK_SIZE];
    let mut outer_pad = [0x5c; BLOCK_SIZE];
    for index in 0..BLOCK_SIZE {
        inner_pad[index] ^= normalized[index];
        outer_pad[index] ^= normalized[index];
    }
    let mut inner = Sha1::new();
    inner.update(inner_pad);
    inner.update(data);
    let inner_digest = inner.finalize();
    let mut outer = Sha1::new();
    outer.update(outer_pad);
    outer.update(inner_digest);
    outer.finalize().into()
}

fn make_token_for_bucket(secret: &[u8; 16], ip: &Ipv4Addr, bucket: u64) -> [u8; 20] {
    let mut input = ip.octets().to_vec();
    input.extend_from_slice(&bucket.to_be_bytes());
    hmac_sha1(secret, &input)
}

fn make_token(secret: &[u8; 16], ip: &Ipv4Addr) -> Vec<u8> {
    make_token_for_bucket(secret, ip, current_token_bucket()).to_vec()
}

fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    if left.len() != right.len() {
        return false;
    }
    left.iter()
        .zip(right)
        .fold(0u8, |difference, (a, b)| difference | (a ^ b))
        == 0
}

fn validate_token(secret: &[u8; 16], ip: &Ipv4Addr, token: &[u8]) -> bool {
    let bucket = current_token_bucket();
    constant_time_eq(token, &make_token_for_bucket(secret, ip, bucket))
        || constant_time_eq(
            token,
            &make_token_for_bucket(secret, ip, bucket.saturating_sub(1)),
        )
}

fn response_message(
    id: &NodeId,
    transaction: &[u8],
    mut values: BTreeMap<String, KrpcValue>,
) -> KrpcMessage {
    values.insert("id".to_string(), KrpcValue::Bytes(id.to_vec()));
    KrpcMessage::Response {
        t: transaction.to_vec(),
        y: "r".to_string(),
        r: values,
        extra: BTreeMap::new(),
    }
}

fn error_message(transaction: &[u8], description: &str) -> KrpcMessage {
    KrpcMessage::Error {
        t: transaction.to_vec(),
        y: "e".to_string(),
        e: KrpcError {
            code: 203,
            description: description.to_string(),
        },
        extra: BTreeMap::new(),
    }
}

pub struct DhtCrawler {
    pub our_id: NodeId,
    pub socket: Arc<TokioUdpSocket>,
    pub routing: RoutingTable,
    info_hash_tx: mpsc::Sender<HashDiscovery>,
    stats_tx: mpsc::Sender<CrawlStatsEvent>,
    token_secret: [u8; 16],
    bootstrap_nodes: Vec<String>,
    discovery_queue: VecDeque<InfoHash>,
    discovery_set: HashSet<InfoHash>,
    provisional: HashMap<SocketAddrV4, (NodeId, Instant)>,
    pending_rpcs: HashMap<Vec<u8>, PendingRpc>,
    retired_transactions: HashMap<Vec<u8>, Instant>,
    sample_eligible_at: HashMap<NodeId, Instant>,
    peer_cache: HashMap<InfoHash, Vec<CachedPeer>>,
    config: DhtConfig,
    queries_sent: usize,
    sampling_round_id: u64,
}

impl DhtCrawler {
    #[allow(dead_code)]
    pub fn new(
        socket: Arc<TokioUdpSocket>,
        info_hash_tx: mpsc::Sender<HashDiscovery>,
        stats_tx: mpsc::Sender<CrawlStatsEvent>,
        bootstrap_nodes: Vec<String>,
    ) -> Self {
        Self::with_config(
            random_node_id(),
            socket,
            info_hash_tx,
            stats_tx,
            bootstrap_nodes,
            DhtConfig::default(),
        )
    }

    pub fn with_config(
        our_id: NodeId,
        socket: Arc<TokioUdpSocket>,
        info_hash_tx: mpsc::Sender<HashDiscovery>,
        stats_tx: mpsc::Sender<CrawlStatsEvent>,
        bootstrap_nodes: Vec<String>,
        config: DhtConfig,
    ) -> Self {
        let mut token_secret = [0; 16];
        rand::thread_rng().fill(&mut token_secret);
        Self {
            our_id,
            socket,
            routing: RoutingTable::with_candidate_capacity(our_id, config.max_candidate_nodes),
            info_hash_tx,
            stats_tx,
            token_secret,
            bootstrap_nodes,
            discovery_queue: VecDeque::new(),
            discovery_set: HashSet::new(),
            provisional: HashMap::new(),
            pending_rpcs: HashMap::new(),
            retired_transactions: HashMap::new(),
            sample_eligible_at: HashMap::new(),
            peer_cache: HashMap::new(),
            config,
            queries_sent: 0,
            sampling_round_id: 0,
        }
    }

    fn emit_stat(&self, event: CrawlStatsEvent) {
        if matches!(
            self.stats_tx.try_send(event),
            Err(mpsc::error::TrySendError::Closed(_))
        ) {
            tracing::warn!("statistics receiver closed");
        }
    }

    fn allocate_transaction(&self) -> Option<u16> {
        if self.pending_rpcs.len() >= self.config.max_pending_rpcs {
            return None;
        }
        (0..=u16::MAX).find_map(|_| {
            let tx = NEXT_TX.fetch_add(1, Ordering::Relaxed);
            (!self
                .pending_rpcs
                .contains_key(transaction_id_bytes(tx).as_slice())
                && !self
                    .retired_transactions
                    .contains_key(transaction_id_bytes(tx).as_slice()))
            .then_some(tx)
        })
    }

    #[allow(clippy::too_many_arguments)]
    async fn send_rpc(
        &mut self,
        addr: SocketAddrV4,
        expected_node_id: Option<NodeId>,
        method: RpcMethod,
        target: NodeId,
        info_hash: Option<InfoHash>,
        is_real_lookup: bool,
        continuation_budget: u8,
    ) -> bool {
        let Some(transaction) = self.allocate_transaction() else {
            self.emit_stat(CrawlStatsEvent::DiscoveryBackpressure);
            return false;
        };
        let key = transaction_id_bytes(transaction).to_vec();
        self.pending_rpcs.insert(
            key.clone(),
            PendingRpc {
                expected_addr: addr,
                expected_node_id,
                method,
                target,
                info_hash,
                is_real_lookup,
                deadline: Instant::now() + self.config.rpc_timeout,
                continuation_budget,
            },
        );
        let message = query_message(method, &self.our_id, &target, transaction);
        match self.socket.send_to(&message.to_bytes(), addr).await {
            Ok(_) => {
                self.queries_sent += 1;
                true
            }
            Err(error) => {
                self.pending_rpcs.remove(&key);
                tracing::debug!("failed to send {:?} to {}: {}", method, addr, error);
                false
            }
        }
    }

    pub async fn bootstrap(&mut self) {
        for configured in self.bootstrap_nodes.clone() {
            if let Some(addr) = resolve_v4(&configured).await {
                self.send_rpc(addr, None, RpcMethod::FindNode, self.our_id, None, false, 2)
                    .await;
            }
        }
    }

    pub fn seed_info_hashes<I>(&mut self, hashes: I) -> usize
    where
        I: IntoIterator<Item = InfoHash>,
    {
        let mut added = 0;
        for hash in hashes {
            if self.queue_hash(hash) {
                added += 1;
                if self
                    .info_hash_tx
                    .try_send(HashDiscovery {
                        info_hash: hash,
                        peers: Vec::new(),
                        source: DiscoverySource::Resume,
                    })
                    .is_err()
                {
                    self.emit_stat(CrawlStatsEvent::DiscoveryBackpressure);
                }
            }
        }
        added
    }

    pub fn seed_nodes<I>(&mut self, nodes: I) -> usize
    where
        I: IntoIterator<Item = NodeContact>,
    {
        nodes
            .into_iter()
            .filter(|node| self.routing.add_node(node.id, node.addr))
            .count()
    }

    fn queue_hash(&mut self, hash: InfoHash) -> bool {
        if hash == [0; 20]
            || self.discovery_set.contains(&hash)
            || self.discovery_queue.len() >= self.config.max_discovery_hashes
        {
            return false;
        }
        self.discovery_queue.push_back(hash);
        self.discovery_set.insert(hash);
        true
    }

    fn publish_discovery(
        &mut self,
        info_hash: InfoHash,
        mut peers: Vec<PeerContact>,
        source: DiscoverySource,
    ) {
        peers.sort_by_key(|peer| peer.addr);
        peers.dedup();
        peers.truncate(self.config.max_peers_per_hash);
        self.emit_stat(CrawlStatsEvent::HashObserved {
            info_hash,
            source,
            has_peers: !peers.is_empty(),
        });
        self.queue_hash(info_hash);
        if self
            .info_hash_tx
            .try_send(HashDiscovery {
                info_hash,
                peers,
                source,
            })
            .is_err()
        {
            self.emit_stat(CrawlStatsEvent::DiscoveryBackpressure);
        }
    }

    pub async fn run(mut self, get_peers_interval: Duration, bucket_refresh: Duration) {
        let mut buffer = vec![0; MAX_PACKET_SIZE];
        let mut discovery_tick = time::interval(get_peers_interval);
        let mut refresh_tick = time::interval(bucket_refresh);
        let mut stats_tick = time::interval(Duration::from_secs(60));
        let mut sampling_tick = time::interval(self.config.sampling_interval);
        self.bootstrap().await;
        loop {
            tokio::select! {
                received = self.socket.recv_from(&mut buffer) => {
                    match received {
                        Ok((length, std::net::SocketAddr::V4(source))) if length > 0 => {
                            self.handle_packet(&buffer[..length], source).await;
                        }
                        Ok(_) => {}
                        Err(error) => tracing::warn!("DHT receive error: {}", error),
                    }
                }
                _ = discovery_tick.tick() => self.discover_peers().await,
                _ = sampling_tick.tick(), if self.config.sampling_enabled => self.sample_random_nodes().await,
                _ = refresh_tick.tick() => {
                    self.expire_pending();
                    self.expire_peer_cache();
                }
                _ = stats_tick.tick() => {
                    self.emit_stat(CrawlStatsEvent::DhtSnapshot {
                        nodes_known: self.routing.node_count(),
                        queries_sent: self.queries_sent,
                        info_hashes_found: self.discovery_set.len(),
                    });
                }
            }
        }
    }

    fn expire_pending(&mut self) {
        let now = Instant::now();
        self.retired_transactions
            .retain(|_, reusable_at| *reusable_at > now);
        let expired: Vec<Vec<u8>> = self
            .pending_rpcs
            .iter()
            .filter(|(_, pending)| pending.deadline <= now)
            .map(|(transaction, _)| transaction.clone())
            .collect();
        for transaction in expired {
            if let Some(pending) = self.pending_rpcs.remove(&transaction) {
                self.retired_transactions
                    .insert(transaction, now + self.config.rpc_timeout);
                if let Some(node_id) = pending.expected_node_id {
                    self.routing.record_failure(&node_id);
                }
                if pending.is_real_lookup {
                    if let Some(hash) = pending.info_hash {
                        self.queue_hash(hash);
                    }
                }
                self.emit_stat(CrawlStatsEvent::RpcTimedOut);
            }
        }
    }

    async fn handle_packet(&mut self, data: &[u8], source: SocketAddrV4) {
        let message = match KrpcMessage::from_bytes(data) {
            Ok(message) => message,
            Err(error) => {
                tracing::debug!("malformed KRPC from {}: {}", source, error);
                self.emit_stat(CrawlStatsEvent::RpcMalformed);
                return;
            }
        };
        match &message {
            KrpcMessage::Query { .. } => self.handle_query(&message, source).await,
            KrpcMessage::Response { .. } => self.handle_response(message, source).await,
            KrpcMessage::Error { .. } => self.handle_error(&message, source),
        }
    }

    async fn handle_query(&mut self, message: &KrpcMessage, source: SocketAddrV4) {
        if !is_public_endpoint(&source) {
            return;
        }
        let KrpcMessage::Query { q, a, t, .. } = message else {
            return;
        };
        if let Some(id) = extract_node_id(message) {
            if self.provisional.len() >= self.config.max_candidate_nodes {
                if let Some(oldest) = self
                    .provisional
                    .iter()
                    .min_by_key(|(_, (_, seen))| *seen)
                    .map(|(addr, _)| *addr)
                {
                    self.provisional.remove(&oldest);
                }
            }
            self.provisional.insert(source, (id, Instant::now()));
        }
        let response = match q.as_str() {
            "ping" => response_message(&self.our_id, t, BTreeMap::new()),
            "find_node" => {
                let target = field_id(a, "target").unwrap_or(self.our_id);
                let nodes = encode_compact_nodes(&self.routing.closest_nodes(&target, 8));
                response_message(
                    &self.our_id,
                    t,
                    BTreeMap::from([("nodes".to_string(), KrpcValue::Bytes(nodes))]),
                )
            }
            "get_peers" => self.get_peers_response(a, t, source),
            "announce_peer" => self.announce_peer_response(a, t, source),
            "sample_infohashes" => {
                let target = field_id(a, "target").unwrap_or(self.our_id);
                let nodes = encode_compact_nodes(&self.routing.closest_nodes(&target, 8));
                response_message(
                    &self.our_id,
                    t,
                    BTreeMap::from([
                        ("interval".to_string(), KrpcValue::Int(300)),
                        ("num".to_string(), KrpcValue::Int(0)),
                        ("samples".to_string(), KrpcValue::Bytes(Vec::new())),
                        ("nodes".to_string(), KrpcValue::Bytes(nodes)),
                    ]),
                )
            }
            _ => error_message(t, "unknown query"),
        };
        if let Err(error) = self.socket.send_to(&response.to_bytes(), source).await {
            tracing::debug!("failed to answer {} from {}: {}", q, source, error);
        }
    }

    fn get_peers_response(
        &mut self,
        args: &BTreeMap<String, KrpcValue>,
        transaction: &[u8],
        source: SocketAddrV4,
    ) -> KrpcMessage {
        let Some(info_hash) = field_id(args, "info_hash") else {
            return error_message(transaction, "invalid info_hash");
        };
        self.publish_discovery(info_hash, Vec::new(), DiscoverySource::InboundGetPeers);
        self.expire_peer_cache();
        let mut values = BTreeMap::new();
        values.insert(
            "token".to_string(),
            KrpcValue::Bytes(make_token(&self.token_secret, source.ip())),
        );
        if let Some(peers) = self.peer_cache.get(&info_hash) {
            values.insert(
                "values".to_string(),
                KrpcValue::List(
                    peers
                        .iter()
                        .map(|cached| KrpcValue::Bytes(encode_compact_peer(&cached.peer)))
                        .collect(),
                ),
            );
        } else {
            values.insert(
                "nodes".to_string(),
                KrpcValue::Bytes(encode_compact_nodes(
                    &self.routing.closest_nodes(&info_hash, 8),
                )),
            );
        }
        response_message(&self.our_id, transaction, values)
    }

    fn announce_peer_response(
        &mut self,
        args: &BTreeMap<String, KrpcValue>,
        transaction: &[u8],
        source: SocketAddrV4,
    ) -> KrpcMessage {
        let Some(info_hash) = field_id(args, "info_hash") else {
            return error_message(transaction, "invalid info_hash");
        };
        let valid_token = matches!(args.get("token"), Some(KrpcValue::Bytes(token)) if validate_token(&self.token_secret, source.ip(), token));
        if !valid_token {
            return error_message(transaction, "invalid token");
        }
        let implied =
            matches!(args.get("implied_port"), Some(KrpcValue::Int(value)) if *value != 0);
        let port = if implied {
            Some(source.port())
        } else {
            match args.get("port") {
                Some(KrpcValue::Int(port)) if (1..=u16::MAX as i64).contains(port) => {
                    Some(*port as u16)
                }
                _ => None,
            }
        };
        let Some(port) = port else {
            return error_message(transaction, "invalid port");
        };
        let peer = PeerContact {
            addr: SocketAddrV4::new(*source.ip(), port),
        };
        self.cache_peer(info_hash, peer.clone());
        self.publish_discovery(info_hash, vec![peer], DiscoverySource::AnnouncePeer);
        response_message(&self.our_id, transaction, BTreeMap::new())
    }

    async fn handle_response(&mut self, message: KrpcMessage, source: SocketAddrV4) {
        let transaction = message.transaction_id().to_vec();
        let (pending, response_id) =
            match correlate_response(&mut self.pending_rpcs, &message, source) {
                Ok(correlated) => correlated,
                Err(CorrelationError::Unsolicited) => {
                    self.emit_stat(CrawlStatsEvent::RpcUnsolicited);
                    return;
                }
                Err(CorrelationError::UnexpectedSource) => {
                    self.emit_stat(CrawlStatsEvent::RpcUnexpectedSource);
                    return;
                }
                Err(CorrelationError::Malformed) => {
                    self.retired_transactions
                        .insert(transaction, Instant::now() + self.config.rpc_timeout);
                    self.emit_stat(CrawlStatsEvent::RpcMalformed);
                    return;
                }
            };
        self.retired_transactions
            .insert(transaction, Instant::now() + self.config.rpc_timeout);
        self.routing.mark_validated(response_id, source);
        self.provisional.remove(&source);
        self.emit_stat(CrawlStatsEvent::DhtNodeSeen {
            id: response_id,
            addr: source,
        });
        self.emit_stat(CrawlStatsEvent::RpcAnswered);

        match pending.method {
            RpcMethod::Ping => {}
            RpcMethod::FindNode => {
                let nodes = response_nodes(&message);
                self.admit_candidates(&nodes);
                self.continue_find_node(nodes, pending).await;
            }
            RpcMethod::GetPeers => {
                let (peers, nodes) = parse_get_peers_response(&message);
                self.admit_candidates(&nodes);
                if let Some(hash) = pending.info_hash {
                    for peer in &peers {
                        self.cache_peer(hash, peer.clone());
                    }
                    if !peers.is_empty() || pending.is_real_lookup {
                        self.publish_discovery(hash, peers, DiscoverySource::OutboundGetPeers);
                    }
                    self.continue_get_peers(hash, nodes, pending).await;
                }
            }
            RpcMethod::SampleInfohashes => {
                let response = match parse_sample_infohashes_response(
                    &message,
                    self.config.max_samples_per_response,
                ) {
                    Ok(response) => response,
                    Err(error) => {
                        tracing::debug!("invalid sample_infohashes response: {}", error);
                        self.emit_stat(CrawlStatsEvent::RpcMalformed);
                        return;
                    }
                };
                let interval = response
                    .interval
                    .max(self.config.sampling_min_remote_interval);
                self.sample_eligible_at
                    .insert(response.node_id, Instant::now() + interval);
                tracing::debug!(
                    "sampling node {} reports {} indexed hashes",
                    source,
                    response.total_num
                );
                self.admit_candidates(&response.nodes);
                for hash in &response.samples {
                    self.publish_discovery(*hash, Vec::new(), DiscoverySource::SampleInfohashes);
                    self.emit_stat(CrawlStatsEvent::SampleObserved {
                        info_hash: *hash,
                        node_id: response.node_id,
                        addr: source,
                        round_id: self.sampling_round_id,
                    });
                }
                self.emit_stat(CrawlStatsEvent::SamplingResponse {
                    samples: response.samples.len(),
                });
            }
        }
    }

    fn handle_error(&mut self, message: &KrpcMessage, source: SocketAddrV4) {
        let transaction = message.transaction_id();
        let Some(pending) = self.pending_rpcs.get(transaction) else {
            self.emit_stat(CrawlStatsEvent::RpcUnsolicited);
            return;
        };
        if pending.expected_addr != source {
            self.emit_stat(CrawlStatsEvent::RpcUnexpectedSource);
            return;
        }
        let pending = self
            .pending_rpcs
            .remove(transaction)
            .expect("pending RPC was checked immediately before removal");
        self.retired_transactions.insert(
            transaction.to_vec(),
            Instant::now() + self.config.rpc_timeout,
        );
        if let Some(node_id) = pending.expected_node_id {
            self.routing.record_failure(&node_id);
        }
        self.emit_stat(CrawlStatsEvent::RpcAnswered);
    }

    fn admit_candidates(&mut self, nodes: &[NodeContact]) {
        for node in nodes.iter().take(self.config.max_candidate_nodes) {
            self.routing.add_candidate(node.clone());
        }
    }

    async fn continue_find_node(&mut self, nodes: Vec<NodeContact>, pending: PendingRpc) {
        if pending.continuation_budget == 0 {
            return;
        }
        for node in nodes.into_iter().take(LOOKUP_CONTINUATION_FANOUT) {
            self.send_rpc(
                node.addr,
                Some(node.id),
                RpcMethod::FindNode,
                pending.target,
                None,
                false,
                pending.continuation_budget - 1,
            )
            .await;
        }
    }

    async fn continue_get_peers(
        &mut self,
        hash: InfoHash,
        nodes: Vec<NodeContact>,
        pending: PendingRpc,
    ) {
        if pending.continuation_budget == 0 {
            return;
        }
        for node in nodes.into_iter().take(LOOKUP_CONTINUATION_FANOUT) {
            self.send_rpc(
                node.addr,
                Some(node.id),
                RpcMethod::GetPeers,
                hash,
                Some(hash),
                pending.is_real_lookup,
                pending.continuation_budget - 1,
            )
            .await;
        }
    }

    async fn discover_peers(&mut self) {
        self.expire_pending();
        let (hash, real) = match self.discovery_queue.pop_front() {
            Some(hash) => {
                self.discovery_set.remove(&hash);
                (hash, true)
            }
            None => (random_node_id(), false),
        };
        let mut nodes = self.routing.closest_nodes(&hash, LOOKUP_FANOUT);
        if nodes.len() < LOOKUP_FANOUT {
            nodes.extend(
                self.routing
                    .closest_candidates(&hash, LOOKUP_FANOUT - nodes.len()),
            );
        }
        if nodes.is_empty() {
            if real {
                self.queue_hash(hash);
            }
            self.bootstrap().await;
            return;
        }
        for node in nodes {
            self.send_rpc(
                node.addr,
                Some(node.id),
                RpcMethod::GetPeers,
                hash,
                Some(hash),
                real,
                2,
            )
            .await;
        }
    }

    async fn sample_random_nodes(&mut self) {
        self.sampling_round_id = self.sampling_round_id.wrapping_add(1);
        let now = Instant::now();
        let nodes: Vec<NodeContact> = self
            .routing
            .random_nodes(self.config.sampling_requests_per_tick * 4)
            .into_iter()
            .filter(|node| {
                self.sample_eligible_at
                    .get(&node.id)
                    .is_none_or(|eligible| *eligible <= now)
            })
            .take(self.config.sampling_requests_per_tick)
            .collect();
        for node in nodes {
            self.sample_eligible_at
                .insert(node.id, now + self.config.sampling_min_remote_interval);
            self.send_rpc(
                node.addr,
                Some(node.id),
                RpcMethod::SampleInfohashes,
                random_node_id(),
                None,
                false,
                0,
            )
            .await;
        }
    }

    fn cache_peer(&mut self, hash: InfoHash, peer: PeerContact) {
        if !self.peer_cache.contains_key(&hash)
            && self.peer_cache.len() >= self.config.announced_peer_hash_capacity
        {
            if let Some(oldest) = self.peer_cache.keys().next().copied() {
                self.peer_cache.remove(&oldest);
            }
        }
        let peers = self.peer_cache.entry(hash).or_default();
        peers.retain(|cached| cached.peer != peer);
        if peers.len() >= self.config.announced_peers_per_hash {
            peers.remove(0);
        }
        peers.push(CachedPeer {
            peer,
            expires_at: Instant::now() + self.config.announced_peer_ttl,
        });
    }

    fn expire_peer_cache(&mut self) {
        let now = Instant::now();
        self.peer_cache.retain(|_, peers| {
            peers.retain(|peer| peer.expires_at > now);
            !peers.is_empty()
        });
    }
}

fn field_id(values: &BTreeMap<String, KrpcValue>, key: &str) -> Option<NodeId> {
    let KrpcValue::Bytes(bytes) = values.get(key)? else {
        return None;
    };
    if bytes.len() != NODE_ID_LEN {
        return None;
    }
    let mut id = [0; NODE_ID_LEN];
    id.copy_from_slice(bytes);
    Some(id)
}

fn response_nodes(message: &KrpcMessage) -> Vec<NodeContact> {
    match message {
        KrpcMessage::Response { r, .. } => match r.get("nodes") {
            Some(KrpcValue::Bytes(nodes)) => decode_compact_nodes(nodes),
            _ => Vec::new(),
        },
        _ => Vec::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn response(
        transaction: u16,
        node_id: NodeId,
        fields: impl IntoIterator<Item = (&'static str, KrpcValue)>,
    ) -> KrpcMessage {
        let mut values = BTreeMap::new();
        values.insert("id".to_string(), KrpcValue::Bytes(node_id.to_vec()));
        values.extend(
            fields
                .into_iter()
                .map(|(key, value)| (key.to_string(), value)),
        );
        KrpcMessage::Response {
            t: transaction_id_bytes(transaction).to_vec(),
            y: "r".to_string(),
            r: values,
            extra: BTreeMap::new(),
        }
    }

    #[test]
    fn query_builders_use_distinct_methods() {
        let id = [1; 20];
        let target = [2; 20];
        for (message, expected) in [
            (build_ping_query(&id, 1), "ping"),
            (build_find_node_query(&id, &target, 2), "find_node"),
            (build_get_peers_query(&id, &target, 3), "get_peers"),
            (
                build_sample_infohashes_query(&id, &target, 4),
                "sample_infohashes",
            ),
        ] {
            assert!(matches!(message, KrpcMessage::Query { q, .. } if q == expected));
        }
    }

    #[test]
    fn compact_decoders_reject_partial_and_private_endpoints() {
        assert!(decode_compact_nodes(&[0; 25]).is_empty());
        let mut node = vec![1; 20];
        node.extend_from_slice(&[10, 0, 0, 1, 0x1a, 0xe1]);
        assert!(decode_compact_nodes(&node).is_empty());
        assert!(decode_compact_peers(&[1, 1, 1, 1, 0]).is_empty());
    }

    #[test]
    fn sample_response_is_bounded_and_structured() {
        let message = response(
            1,
            [3; 20],
            [
                ("interval", KrpcValue::Int(300)),
                ("num", KrpcValue::Int(2)),
                ("samples", KrpcValue::Bytes([[4; 20], [5; 20]].concat())),
                ("nodes", KrpcValue::Bytes(Vec::new())),
            ],
        );
        let parsed = parse_sample_infohashes_response(&message, 2).unwrap();
        assert_eq!(parsed.samples, vec![[4; 20], [5; 20]]);
        assert!(parse_sample_infohashes_response(&message, 1).is_err());
    }

    #[test]
    fn malformed_sample_lengths_are_rejected() {
        let message = response(
            1,
            [3; 20],
            [
                ("interval", KrpcValue::Int(300)),
                ("num", KrpcValue::Int(1)),
                ("samples", KrpcValue::Bytes(vec![0; 19])),
            ],
        );
        assert!(parse_sample_infohashes_response(&message, 10).is_err());
    }

    #[test]
    fn tokens_are_bound_to_ip_and_recent_bucket() {
        let secret = [7; 16];
        let ip = Ipv4Addr::new(1, 1, 1, 1);
        let token = make_token(&secret, &ip);
        assert!(validate_token(&secret, &ip, &token));
        assert!(!validate_token(&secret, &Ipv4Addr::new(1, 1, 1, 2), &token));
    }

    #[test]
    fn response_shapes_match_original_method() {
        let id = [1; 20];
        assert!(response_has_shape(
            &response(1, id, [("nodes", KrpcValue::Bytes(Vec::new()))]),
            RpcMethod::FindNode
        ));
        assert!(!response_has_shape(
            &response(1, id, std::iter::empty()),
            RpcMethod::GetPeers
        ));
    }

    #[test]
    fn unexpected_and_unsolicited_responses_cannot_consume_pending_state() {
        let mut pending_rpcs = HashMap::new();
        let transaction = transaction_id_bytes(41).to_vec();
        let expected = SocketAddrV4::new(Ipv4Addr::new(8, 8, 8, 8), 6881);
        pending_rpcs.insert(
            transaction.clone(),
            PendingRpc {
                expected_addr: expected,
                expected_node_id: Some([1; 20]),
                method: RpcMethod::FindNode,
                target: [9; 20],
                info_hash: None,
                is_real_lookup: false,
                deadline: Instant::now() + Duration::from_secs(30),
                continuation_budget: 0,
            },
        );
        let message = response(41, [1; 20], [("nodes", KrpcValue::Bytes(Vec::new()))]);
        assert_eq!(
            correlate_response(
                &mut pending_rpcs,
                &message,
                SocketAddrV4::new(Ipv4Addr::new(1, 1, 1, 1), 6881),
            )
            .unwrap_err(),
            CorrelationError::UnexpectedSource
        );
        assert!(pending_rpcs.contains_key(&transaction));
        assert!(correlate_response(&mut pending_rpcs, &message, expected).is_ok());
        assert!(!pending_rpcs.contains_key(&transaction));
        assert_eq!(
            correlate_response(&mut pending_rpcs, &message, expected).unwrap_err(),
            CorrelationError::Unsolicited
        );
    }

    #[test]
    fn returned_nodes_remain_candidates_until_they_answer() {
        let mut routing = RoutingTable::new([9; 20]);
        let source = SocketAddrV4::new(Ipv4Addr::new(8, 8, 4, 4), 6881);
        let returned = NodeContact {
            id: [2; 20],
            addr: SocketAddrV4::new(Ipv4Addr::new(1, 1, 1, 1), 6881),
            last_seen: Instant::now(),
            last_seen_unix: chrono::Utc::now().timestamp(),
        };
        routing.mark_validated([1; 20], source);
        routing.add_candidate(returned);
        assert_eq!(routing.node_count(), 1);
        assert_eq!(routing.candidate_count(), 1);
    }
}
