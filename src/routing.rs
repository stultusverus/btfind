use crate::types::{node_id_distance, NodeContact, NodeId, NODE_ID_LEN};
use std::collections::{BTreeMap, VecDeque};
use std::net::SocketAddrV4;
use std::time::{Duration, Instant};

const K: usize = 8;
const BAD_AFTER_FAILURES: u8 = 2;

#[allow(dead_code)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ContactState {
    Candidate,
    KnownGood,
    Questionable,
    Bad,
}

#[derive(Clone)]
struct ContactRecord {
    contact: NodeContact,
    unanswered: u8,
}

pub struct Bucket {
    prefix: NodeId,
    prefix_len: usize,
    nodes: BTreeMap<NodeId, ContactRecord>,
    replacements: VecDeque<NodeContact>,
    last_changed: Instant,
}

impl Bucket {
    fn new(prefix: NodeId, prefix_len: usize) -> Self {
        Self {
            prefix,
            prefix_len,
            nodes: BTreeMap::new(),
            replacements: VecDeque::new(),
            last_changed: Instant::now(),
        }
    }

    fn contains(&self, id: &NodeId) -> bool {
        (0..self.prefix_len).all(|bit| bit_value(&self.prefix, bit) == bit_value(id, bit))
    }

    #[allow(dead_code)]
    fn state(record: &ContactRecord, questionable_after: Duration) -> ContactState {
        if record.unanswered >= BAD_AFTER_FAILURES {
            ContactState::Bad
        } else if record.unanswered > 0 || record.contact.last_seen.elapsed() > questionable_after {
            ContactState::Questionable
        } else {
            ContactState::KnownGood
        }
    }
}

pub struct RoutingTable {
    our_id: NodeId,
    buckets: Vec<Bucket>,
    max_candidates: usize,
}

impl RoutingTable {
    #[allow(dead_code)]
    pub fn new(our_id: NodeId) -> Self {
        Self::with_candidate_capacity(our_id, 8192)
    }

    pub fn with_candidate_capacity(our_id: NodeId, max_candidates: usize) -> Self {
        Self {
            our_id,
            buckets: vec![Bucket::new([0; NODE_ID_LEN], 0)],
            max_candidates,
        }
    }

    fn bucket_index(&self, id: &NodeId) -> usize {
        self.buckets
            .iter()
            .position(|bucket| bucket.contains(id))
            .expect("routing buckets cover the entire node-id space")
    }

    fn split_bucket(&mut self, index: usize) {
        let bucket = self.buckets.remove(index);
        let next_len = bucket.prefix_len + 1;
        let left_prefix = bucket.prefix;
        let mut right_prefix = bucket.prefix;
        set_bit(&mut right_prefix, bucket.prefix_len, true);
        let mut left = Bucket::new(left_prefix, next_len);
        let mut right = Bucket::new(right_prefix, next_len);

        for (id, record) in bucket.nodes {
            if right.contains(&id) {
                right.nodes.insert(id, record);
            } else {
                left.nodes.insert(id, record);
            }
        }
        for contact in bucket.replacements {
            let target = if right.contains(&contact.id) {
                &mut right
            } else {
                &mut left
            };
            if target.replacements.len() < K {
                target.replacements.push_back(contact);
            }
        }
        self.buckets.insert(index, right);
        self.buckets.insert(index, left);
    }

    pub fn add_candidate(&mut self, contact: NodeContact) -> bool {
        if contact.id == self.our_id || self.contains(&contact.id) {
            return false;
        }
        let index = self.bucket_index(&contact.id);
        if self.buckets[index]
            .replacements
            .iter()
            .any(|existing| existing.id == contact.id || existing.addr == contact.addr)
        {
            return false;
        }
        while self.candidate_count() >= self.max_candidates {
            if let Some(bucket) = self
                .buckets
                .iter_mut()
                .find(|bucket| !bucket.replacements.is_empty())
            {
                bucket.replacements.pop_front();
            } else {
                break;
            }
        }
        let bucket = &mut self.buckets[index];
        if bucket.replacements.len() >= K {
            bucket.replacements.pop_front();
        }
        bucket.replacements.push_back(contact);
        true
    }

    pub fn add_node(&mut self, id: NodeId, addr: SocketAddrV4) -> bool {
        self.mark_validated(id, addr)
    }

    pub fn mark_validated(&mut self, id: NodeId, addr: SocketAddrV4) -> bool {
        if id == self.our_id {
            return false;
        }
        loop {
            let index = self.bucket_index(&id);
            if let Some(record) = self.buckets[index].nodes.get_mut(&id) {
                record.contact.addr = addr;
                record.contact.last_seen = Instant::now();
                record.contact.last_seen_unix = chrono::Utc::now().timestamp();
                record.unanswered = 0;
                return false;
            }

            if self.buckets[index].nodes.len() < K {
                self.buckets[index]
                    .replacements
                    .retain(|contact| contact.id != id);
                self.buckets[index].nodes.insert(
                    id,
                    ContactRecord {
                        contact: NodeContact {
                            id,
                            addr,
                            last_seen: Instant::now(),
                            last_seen_unix: chrono::Utc::now().timestamp(),
                        },
                        unanswered: 0,
                    },
                );
                self.buckets[index].last_changed = Instant::now();
                return true;
            }

            if self.buckets[index].contains(&self.our_id)
                && self.buckets[index].prefix_len < NODE_ID_LEN * 8
            {
                self.split_bucket(index);
                continue;
            }

            if let Some(bad_id) = self.buckets[index]
                .nodes
                .iter()
                .find(|(_, record)| record.unanswered >= BAD_AFTER_FAILURES)
                .map(|(node_id, _)| *node_id)
            {
                self.buckets[index].nodes.remove(&bad_id);
                continue;
            }

            self.add_candidate(NodeContact {
                id,
                addr,
                last_seen: Instant::now(),
                last_seen_unix: chrono::Utc::now().timestamp(),
            });
            return false;
        }
    }

    pub fn record_failure(&mut self, id: &NodeId) {
        let index = self.bucket_index(id);
        let should_remove = if let Some(record) = self.buckets[index].nodes.get_mut(id) {
            record.unanswered = record.unanswered.saturating_add(1);
            record.unanswered >= BAD_AFTER_FAILURES
        } else {
            false
        };
        if should_remove {
            self.buckets[index].nodes.remove(id);
            self.buckets[index].last_changed = Instant::now();
        }
    }

    #[allow(dead_code)]
    pub fn contact_state(&self, id: &NodeId, questionable_after: Duration) -> Option<ContactState> {
        let index = self.bucket_index(id);
        if let Some(record) = self.buckets[index].nodes.get(id) {
            Some(Bucket::state(record, questionable_after))
        } else if self.buckets[index]
            .replacements
            .iter()
            .any(|contact| &contact.id == id)
        {
            Some(ContactState::Candidate)
        } else {
            None
        }
    }

    pub fn closest_nodes(&self, target: &NodeId, count: usize) -> Vec<NodeContact> {
        let mut all: Vec<&NodeContact> = self
            .buckets
            .iter()
            .flat_map(|bucket| bucket.nodes.values().map(|record| &record.contact))
            .collect();
        all.sort_by_key(|node| node_id_distance(&node.id, target));
        all.truncate(count);
        all.into_iter().cloned().collect()
    }

    pub fn closest_candidates(&self, target: &NodeId, count: usize) -> Vec<NodeContact> {
        let mut all: Vec<&NodeContact> = self
            .buckets
            .iter()
            .flat_map(|bucket| bucket.replacements.iter())
            .collect();
        all.sort_by_key(|node| node_id_distance(&node.id, target));
        all.truncate(count);
        all.into_iter().cloned().collect()
    }

    pub fn node_count(&self) -> usize {
        self.buckets.iter().map(|bucket| bucket.nodes.len()).sum()
    }

    pub fn candidate_count(&self) -> usize {
        self.buckets
            .iter()
            .map(|bucket| bucket.replacements.len())
            .sum()
    }

    #[allow(dead_code)]
    pub fn bucket_count(&self) -> usize {
        self.buckets.len()
    }

    pub fn random_nodes(&self, count: usize) -> Vec<NodeContact> {
        let all: Vec<NodeContact> = self
            .buckets
            .iter()
            .flat_map(|bucket| bucket.nodes.values().map(|record| record.contact.clone()))
            .collect();
        use rand::seq::SliceRandom;
        all.choose_multiple(&mut rand::thread_rng(), count.min(all.len()))
            .cloned()
            .collect()
    }

    pub fn contains(&self, node_id: &NodeId) -> bool {
        let index = self.bucket_index(node_id);
        self.buckets[index].nodes.contains_key(node_id)
    }
}

fn bit_value(id: &NodeId, bit: usize) -> bool {
    id[bit / 8] & (0x80 >> (bit % 8)) != 0
}

fn set_bit(id: &mut NodeId, bit: usize, value: bool) {
    let mask = 0x80 >> (bit % 8);
    if value {
        id[bit / 8] |= mask;
    } else {
        id[bit / 8] &= !mask;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::Ipv4Addr;

    fn addr(last: u8) -> SocketAddrV4 {
        SocketAddrV4::new(Ipv4Addr::new(8, 8, 8, last), 6881)
    }

    #[test]
    fn empty_table() {
        let table = RoutingTable::new([0; 20]);
        assert_eq!(table.node_count(), 0);
        assert_eq!(table.bucket_count(), 1);
    }

    #[test]
    fn validated_nodes_enter_live_table() {
        let mut table = RoutingTable::new([0; 20]);
        assert!(table.mark_validated([1; 20], addr(1)));
        assert_eq!(table.node_count(), 1);
        assert_eq!(
            table.contact_state(&[1; 20], Duration::from_secs(60)),
            Some(ContactState::KnownGood)
        );
    }

    #[test]
    fn candidates_do_not_enter_live_table() {
        let mut table = RoutingTable::new([0; 20]);
        let contact = NodeContact {
            id: [0x80; 20],
            addr: addr(2),
            last_seen: Instant::now(),
            last_seen_unix: chrono::Utc::now().timestamp(),
        };
        assert!(table.add_candidate(contact));
        assert_eq!(table.node_count(), 0);
        assert_eq!(table.candidate_count(), 1);
    }

    #[test]
    fn buckets_split_only_toward_local_id_and_keep_k_live_contacts() {
        let mut table = RoutingTable::new([0; 20]);
        for i in 1..=32u8 {
            let mut id = [0u8; 20];
            id[0] = i;
            table.mark_validated(id, addr(i));
        }
        assert!(table.bucket_count() > 1);
        assert!(table.node_count() <= table.bucket_count() * K);
    }

    #[test]
    fn failures_remove_bad_contacts() {
        let mut table = RoutingTable::new([0; 20]);
        let id = [1; 20];
        table.mark_validated(id, addr(1));
        table.record_failure(&id);
        assert_eq!(
            table.contact_state(&id, Duration::from_secs(60)),
            Some(ContactState::Questionable)
        );
        table.record_failure(&id);
        assert!(!table.contains(&id));
    }

    #[test]
    fn failed_live_node_does_not_promote_unverified_replacement() {
        let mut table = RoutingTable::new([0; 20]);
        let live_id = [1; 20];
        let candidate_id = [2; 20];
        let candidate = NodeContact {
            id: candidate_id,
            addr: addr(2),
            last_seen: Instant::now(),
            last_seen_unix: chrono::Utc::now().timestamp(),
        };
        table.mark_validated(live_id, addr(1));
        table.add_candidate(candidate);

        table.record_failure(&live_id);
        table.record_failure(&live_id);

        assert!(!table.contains(&live_id));
        assert!(!table.contains(&candidate_id));
        assert_eq!(table.node_count(), 0);
        assert_eq!(table.candidate_count(), 1);
        assert_eq!(
            table.contact_state(&candidate_id, Duration::from_secs(60)),
            Some(ContactState::Candidate)
        );

        assert!(table.mark_validated(candidate_id, addr(2)));
        assert!(table.contains(&candidate_id));
        assert_eq!(table.candidate_count(), 0);
    }
}
