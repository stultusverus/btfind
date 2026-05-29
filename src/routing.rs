use crate::types::{node_id_distance, NodeContact, NodeId, NODE_ID_LEN};
use std::collections::BTreeMap;
use std::net::SocketAddrV4;
use std::time::{Duration, Instant};

const BUCKET_COUNT: usize = NODE_ID_LEN * 8;
const MAX_NODES_PER_BUCKET: usize = 64;

pub struct Bucket {
    nodes: BTreeMap<NodeId, NodeContact>,
}

impl Bucket {
    fn new() -> Self {
        Bucket {
            nodes: BTreeMap::new(),
        }
    }

    #[allow(dead_code)]
    fn len(&self) -> usize {
        self.nodes.len()
    }
}

pub struct RoutingTable {
    our_id: NodeId,
    buckets: Vec<Bucket>,
}

impl RoutingTable {
    pub fn new(our_id: NodeId) -> Self {
        let mut buckets = Vec::with_capacity(BUCKET_COUNT);
        for _ in 0..BUCKET_COUNT {
            buckets.push(Bucket::new());
        }
        RoutingTable { our_id, buckets }
    }

    fn bucket_index(&self, node_id: &NodeId) -> usize {
        let dist = node_id_distance(&self.our_id, node_id);
        for (byte_idx, &byte) in dist.iter().enumerate() {
            if byte != 0 {
                let bit = byte.leading_zeros() as usize;
                return byte_idx * 8 + bit;
            }
        }
        BUCKET_COUNT - 1
    }

    pub fn add_node(&mut self, id: NodeId, addr: SocketAddrV4) -> bool {
        if id == self.our_id {
            return false;
        }

        let bucket_idx = self.bucket_index(&id);
        let bucket = &mut self.buckets[bucket_idx];

        if bucket.nodes.contains_key(&id) {
            bucket.nodes.get_mut(&id).unwrap().last_seen = Instant::now();
            return false;
        }

        if bucket.nodes.len() >= MAX_NODES_PER_BUCKET {
            return false;
        }

        bucket.nodes.insert(
            id,
            NodeContact {
                id,
                addr,
                last_seen: Instant::now(),
            },
        );
        true
    }

    pub fn closest_nodes(&self, target: &NodeId, count: usize) -> Vec<NodeContact> {
        let mut all: Vec<&NodeContact> =
            self.buckets.iter().flat_map(|b| b.nodes.values()).collect();

        all.sort_by_key(|node| node_id_distance(&node.id, target));
        all.truncate(count);

        all.into_iter().cloned().collect()
    }

    pub fn remove_stale_nodes(&mut self, max_age: Duration) -> usize {
        let mut removed = 0;
        let now = Instant::now();
        for bucket in &mut self.buckets {
            let stale: Vec<NodeId> = bucket
                .nodes
                .iter()
                .filter(|(_, c)| now.duration_since(c.last_seen) > max_age)
                .map(|(id, _)| *id)
                .collect();
            for id in stale {
                bucket.nodes.remove(&id);
                removed += 1;
            }
        }
        removed
    }

    pub fn node_count(&self) -> usize {
        self.buckets.iter().map(|b| b.nodes.len()).sum()
    }

    #[allow(dead_code)]
    pub fn random_nodes(&self, count: usize) -> Vec<NodeContact> {
        let all: Vec<NodeContact> = self
            .buckets
            .iter()
            .flat_map(|b| b.nodes.values())
            .cloned()
            .collect();

        if all.is_empty() {
            return vec![];
        }

        use rand::seq::SliceRandom;
        let mut rng = rand::thread_rng();
        all.choose_multiple(&mut rng, count.min(all.len()))
            .cloned()
            .collect()
    }

    pub fn update_last_seen(&mut self, node_id: &NodeId) {
        let bucket_idx = self.bucket_index(node_id);
        if let Some(node) = self.buckets[bucket_idx].nodes.get_mut(node_id) {
            node.last_seen = Instant::now();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{Ipv4Addr, SocketAddrV4};

    #[test]
    fn test_empty_table() {
        let table = RoutingTable::new([0u8; 20]);
        assert_eq!(table.node_count(), 0);
    }

    #[test]
    fn test_add_node() {
        let our_id = [0u8; 20];
        let mut table = RoutingTable::new(our_id);

        let node_id = [0x01u8; 20];
        let addr = SocketAddrV4::new(Ipv4Addr::new(1, 2, 3, 4), 6881);
        table.add_node(node_id, addr);

        assert_eq!(table.node_count(), 1);
    }

    #[test]
    fn test_closest_nodes() {
        let our_id = [0u8; 20];
        let mut table = RoutingTable::new(our_id);

        for i in 1u8..=10 {
            let mut id = [0u8; 20];
            id[0] = i;
            let addr = SocketAddrV4::new(Ipv4Addr::new(1, 1, 1, i), 6881);
            table.add_node(id, addr);
        }

        let target = [0x01u8; 20];
        let closest = table.closest_nodes(&target, 5);
        assert_eq!(closest.len(), 5);
    }

    #[test]
    fn test_remove_stale_nodes() {
        let our_id = [0u8; 20];
        let mut table = RoutingTable::new(our_id);

        let addr = SocketAddrV4::new(Ipv4Addr::new(1, 2, 3, 4), 6881);
        table.add_node([0x01u8; 20], addr);
        assert_eq!(table.node_count(), 1);

        table.remove_stale_nodes(std::time::Duration::from_secs(0));
        assert_eq!(table.node_count(), 0);
    }

    #[test]
    fn test_far_bucket_holds_enough_nodes_for_crawling() {
        let our_id = [0u8; 20];
        let mut table = RoutingTable::new(our_id);

        for i in 0u8..32 {
            let mut node_id = [0u8; 20];
            node_id[0] = 0x80;
            node_id[19] = i;
            let addr = SocketAddrV4::new(Ipv4Addr::new(8, 8, 8, i.saturating_add(1)), 6881);
            assert!(table.add_node(node_id, addr));
        }

        assert_eq!(table.node_count(), 32);
    }
}
