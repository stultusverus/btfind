use std::net::SocketAddrV4;
use std::time::Instant;

pub const NODE_ID_LEN: usize = 20;
pub const INFO_HASH_LEN: usize = 20;
pub const TRANSACTION_ID_LEN: usize = 2;

pub type NodeId = [u8; NODE_ID_LEN];
pub type InfoHash = [u8; INFO_HASH_LEN];

#[derive(Debug, Clone)]
pub struct NodeContact {
    pub id: NodeId,
    pub addr: SocketAddrV4,
    pub last_seen: Instant,
    pub last_seen_unix: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PeerContact {
    pub addr: SocketAddrV4,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DiscoverySource {
    InboundGetPeers,
    AnnouncePeer,
    Resume,
    SampleInfohashes,
    OutboundGetPeers,
}

impl DiscoverySource {
    pub fn as_str(self) -> &'static str {
        match self {
            DiscoverySource::InboundGetPeers => "inbound_get_peers",
            DiscoverySource::AnnouncePeer => "announce_peer",
            DiscoverySource::Resume => "resume",
            DiscoverySource::SampleInfohashes => "sample_infohashes",
            DiscoverySource::OutboundGetPeers => "outbound_get_peers",
        }
    }
}

#[derive(Debug, Clone)]
pub struct HashDiscovery {
    pub info_hash: InfoHash,
    pub peers: Vec<PeerContact>,
    pub source: DiscoverySource,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetadataFailureReason {
    Connect,
    Timeout,
    Handshake,
    Extension,
    Rejected,
    Protocol,
    HashMismatch,
    Parse,
}

impl MetadataFailureReason {
    pub fn as_str(self) -> &'static str {
        match self {
            MetadataFailureReason::Connect => "connect",
            MetadataFailureReason::Timeout => "timeout",
            MetadataFailureReason::Handshake => "handshake",
            MetadataFailureReason::Extension => "extension",
            MetadataFailureReason::Rejected => "rejected",
            MetadataFailureReason::Protocol => "protocol",
            MetadataFailureReason::HashMismatch => "hash_mismatch",
            MetadataFailureReason::Parse => "parse",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CrawlStatsEvent {
    DhtSnapshot {
        nodes_known: usize,
        queries_sent: usize,
        info_hashes_found: usize,
    },
    DhtNodeSeen {
        id: NodeId,
        addr: SocketAddrV4,
    },
    MetadataFetched,
    MetadataFetchFailed {
        reason: MetadataFailureReason,
    },
    RpcAnswered,
    RpcTimedOut,
    RpcMalformed,
    RpcUnsolicited,
    RpcUnexpectedSource,
    DiscoveryBackpressure,
    SamplingResponse {
        samples: usize,
    },
    SampleObserved {
        info_hash: InfoHash,
        node_id: NodeId,
        addr: SocketAddrV4,
        round_id: u64,
    },
    HashObserved {
        info_hash: InfoHash,
        source: DiscoverySource,
        has_peers: bool,
    },
}

pub fn random_node_id() -> NodeId {
    use rand::Rng;
    let mut rng = rand::thread_rng();
    let mut id = [0u8; NODE_ID_LEN];
    rng.fill(&mut id);
    id
}

#[allow(dead_code)]
pub fn info_hash_from_hex(hex: &str) -> Option<InfoHash> {
    let bytes = hex::decode(hex).ok()?;
    if bytes.len() != INFO_HASH_LEN {
        return None;
    }
    let mut hash = [0u8; INFO_HASH_LEN];
    hash.copy_from_slice(&bytes);
    Some(hash)
}

#[allow(dead_code)]
pub fn info_hash_to_hex(hash: &InfoHash) -> String {
    hex::encode(hash)
}

pub fn node_id_distance(a: &NodeId, b: &NodeId) -> [u8; NODE_ID_LEN] {
    let mut dist = [0u8; NODE_ID_LEN];
    for i in 0..NODE_ID_LEN {
        dist[i] = a[i] ^ b[i];
    }
    dist
}
