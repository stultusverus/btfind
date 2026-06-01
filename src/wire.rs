use std::collections::{BTreeMap, HashMap};

pub const BT_PROTOCOL: &[u8] = b"BitTorrent protocol";
pub const HANDSHAKE_LEN: usize = 68;

pub const MSG_CHOKE: u8 = 0;
pub const MSG_UNCHOKE: u8 = 1;
pub const MSG_INTERESTED: u8 = 2;
pub const MSG_NOT_INTERESTED: u8 = 3;
pub const MSG_HAVE: u8 = 4;
pub const MSG_BITFIELD: u8 = 5;
pub const MSG_REQUEST: u8 = 6;
pub const MSG_PIECE: u8 = 7;
pub const MSG_CANCEL: u8 = 8;
pub const MSG_PORT: u8 = 9;
pub const MSG_EXTENDED: u8 = 20;

#[derive(Debug, Clone)]
pub struct Handshake {
    pub info_hash: [u8; 20],
    pub peer_id: [u8; 20],
    pub reserved: [u8; 8],
}

impl Handshake {
    pub fn new(info_hash: [u8; 20], peer_id: [u8; 20]) -> Self {
        let mut reserved = [0u8; 8];
        reserved[5] |= 0x10; // Extension protocol
        reserved[7] |= 0x01; // Mainline DHT (BEP 5)
        Handshake {
            info_hash,
            peer_id,
            reserved,
        }
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(HANDSHAKE_LEN);
        buf.push(BT_PROTOCOL.len() as u8);
        buf.extend_from_slice(BT_PROTOCOL);
        buf.extend_from_slice(&self.reserved);
        buf.extend_from_slice(&self.info_hash);
        buf.extend_from_slice(&self.peer_id);
        buf
    }

    pub fn from_bytes(data: &[u8]) -> Option<Self> {
        if data.len() < HANDSHAKE_LEN {
            return None;
        }
        let pstr_len = data[0] as usize;
        if pstr_len != 19 || &data[1..20] != BT_PROTOCOL {
            return None;
        }
        let mut reserved = [0u8; 8];
        reserved.copy_from_slice(&data[20..28]);
        let mut info_hash = [0u8; 20];
        info_hash.copy_from_slice(&data[28..48]);
        let mut peer_id = [0u8; 20];
        peer_id.copy_from_slice(&data[48..68]);
        Some(Handshake {
            info_hash,
            peer_id,
            reserved,
        })
    }

    pub fn supports_extensions(&self) -> bool {
        self.reserved[5] & 0x10 != 0
    }
}

#[derive(Debug, Clone)]
pub enum WireMessage {
    KeepAlive,
    Choke,
    Unchoke,
    Interested,
    NotInterested,
    Have(u32),
    Bitfield(Vec<u8>),
    Request {
        index: u32,
        begin: u32,
        length: u32,
    },
    Piece {
        index: u32,
        begin: u32,
        block: Vec<u8>,
    },
    Cancel {
        index: u32,
        begin: u32,
        length: u32,
    },
    Port(u16),
    Extended {
        id: u8,
        payload: Vec<u8>,
    },
}

impl WireMessage {
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut payload = Vec::new();
        match self {
            WireMessage::KeepAlive => return vec![0, 0, 0, 0],
            WireMessage::Choke => payload.push(MSG_CHOKE),
            WireMessage::Unchoke => payload.push(MSG_UNCHOKE),
            WireMessage::Interested => payload.push(MSG_INTERESTED),
            WireMessage::NotInterested => payload.push(MSG_NOT_INTERESTED),
            WireMessage::Have(piece) => {
                payload.push(MSG_HAVE);
                payload.extend_from_slice(&piece.to_be_bytes());
            }
            WireMessage::Bitfield(data) => {
                payload.push(MSG_BITFIELD);
                payload.extend_from_slice(data);
            }
            WireMessage::Request {
                index,
                begin,
                length,
            } => {
                payload.push(MSG_REQUEST);
                payload.extend_from_slice(&index.to_be_bytes());
                payload.extend_from_slice(&begin.to_be_bytes());
                payload.extend_from_slice(&length.to_be_bytes());
            }
            WireMessage::Piece {
                index,
                begin,
                block,
            } => {
                payload.push(MSG_PIECE);
                payload.extend_from_slice(&index.to_be_bytes());
                payload.extend_from_slice(&begin.to_be_bytes());
                payload.extend_from_slice(block);
            }
            WireMessage::Cancel {
                index,
                begin,
                length,
            } => {
                payload.push(MSG_CANCEL);
                payload.extend_from_slice(&index.to_be_bytes());
                payload.extend_from_slice(&begin.to_be_bytes());
                payload.extend_from_slice(&length.to_be_bytes());
            }
            WireMessage::Port(port) => {
                payload.push(MSG_PORT);
                payload.extend_from_slice(&port.to_be_bytes());
            }
            WireMessage::Extended {
                id,
                payload: ext_payload,
            } => {
                payload.push(MSG_EXTENDED);
                payload.push(*id);
                payload.extend_from_slice(ext_payload);
            }
        }
        let mut msg = Vec::with_capacity(4 + payload.len());
        msg.extend_from_slice(&(payload.len() as u32).to_be_bytes());
        msg.extend_from_slice(&payload);
        msg
    }

    #[allow(dead_code)]
    pub fn from_bytes(data: &[u8]) -> Option<Self> {
        if data.len() < 4 {
            return None;
        }
        let mut len_bytes = [0u8; 4];
        len_bytes.copy_from_slice(&data[0..4]);
        let len = u32::from_be_bytes(len_bytes) as usize;
        if len == 0 {
            return Some(WireMessage::KeepAlive);
        }
        if data.len() < 4 + len {
            return None;
        }
        let payload = &data[4..4 + len];
        Self::from_payload(payload)
    }

    pub fn from_bytes_frame(len: u32, body: &[u8]) -> Option<Self> {
        if len == 0 {
            return Some(WireMessage::KeepAlive);
        }
        if body.len() < len as usize {
            return None;
        }
        let payload = &body[..len as usize];
        Self::from_payload(payload)
    }

    fn from_payload(payload: &[u8]) -> Option<Self> {
        if payload.is_empty() {
            return Some(WireMessage::KeepAlive);
        }
        let msg_id = payload[0];
        let body = &payload[1..];
        match msg_id {
            MSG_CHOKE => Some(WireMessage::Choke),
            MSG_UNCHOKE => Some(WireMessage::Unchoke),
            MSG_INTERESTED => Some(WireMessage::Interested),
            MSG_NOT_INTERESTED => Some(WireMessage::NotInterested),
            MSG_HAVE if body.len() >= 4 => {
                let piece = u32::from_be_bytes([body[0], body[1], body[2], body[3]]);
                Some(WireMessage::Have(piece))
            }
            MSG_BITFIELD => Some(WireMessage::Bitfield(body.to_vec())),
            MSG_REQUEST if body.len() >= 12 => {
                let index = u32::from_be_bytes([body[0], body[1], body[2], body[3]]);
                let begin = u32::from_be_bytes([body[4], body[5], body[6], body[7]]);
                let length = u32::from_be_bytes([body[8], body[9], body[10], body[11]]);
                Some(WireMessage::Request {
                    index,
                    begin,
                    length,
                })
            }
            MSG_PIECE if body.len() >= 8 => {
                let index = u32::from_be_bytes([body[0], body[1], body[2], body[3]]);
                let begin = u32::from_be_bytes([body[4], body[5], body[6], body[7]]);
                let block = body[8..].to_vec();
                Some(WireMessage::Piece {
                    index,
                    begin,
                    block,
                })
            }
            MSG_CANCEL if body.len() >= 12 => {
                let index = u32::from_be_bytes([body[0], body[1], body[2], body[3]]);
                let begin = u32::from_be_bytes([body[4], body[5], body[6], body[7]]);
                let length = u32::from_be_bytes([body[8], body[9], body[10], body[11]]);
                Some(WireMessage::Cancel {
                    index,
                    begin,
                    length,
                })
            }
            MSG_PORT if body.len() >= 2 => {
                let port = u16::from_be_bytes([body[0], body[1]]);
                Some(WireMessage::Port(port))
            }
            MSG_EXTENDED if !body.is_empty() => {
                let ext_id = body[0];
                let ext_payload = body[1..].to_vec();
                Some(WireMessage::Extended {
                    id: ext_id,
                    payload: ext_payload,
                })
            }
            _ => None,
        }
    }
}

#[derive(Debug)]
pub struct ExtendedHandshake {
    pub m: BTreeMap<String, u32>,
    pub metadata_size: Option<u32>,
    pub v: Option<String>,
    pub your_ip: Option<String>,
    pub reqq: Option<u32>,
}

impl ExtendedHandshake {
    pub fn to_dict(&self) -> HashMap<Vec<u8>, serde_bencode::value::Value> {
        let mut dict = HashMap::new();
        let mut m_dict = HashMap::new();
        for (k, v) in &self.m {
            m_dict.insert(
                k.as_bytes().to_vec(),
                serde_bencode::value::Value::Int(*v as i64),
            );
        }
        dict.insert(b"m".to_vec(), serde_bencode::value::Value::Dict(m_dict));
        if let Some(size) = self.metadata_size {
            dict.insert(
                b"metadata_size".to_vec(),
                serde_bencode::value::Value::Int(size as i64),
            );
        }
        if let Some(ref v) = self.v {
            dict.insert(
                b"v".to_vec(),
                serde_bencode::value::Value::Bytes(v.as_bytes().to_vec()),
            );
        }
        if let Some(ref ip) = self.your_ip {
            dict.insert(
                b"yourip".to_vec(),
                serde_bencode::value::Value::Bytes(ip.as_bytes().to_vec()),
            );
        }
        if let Some(reqq) = self.reqq {
            dict.insert(
                b"reqq".to_vec(),
                serde_bencode::value::Value::Int(reqq as i64),
            );
        }
        dict
    }

    pub fn from_bytes(data: &[u8]) -> Option<Self> {
        let value: serde_bencode::value::Value = serde_bencode::from_bytes(data).ok()?;
        let dict = match &value {
            serde_bencode::value::Value::Dict(d) => d,
            _ => return None,
        };
        let mut m = BTreeMap::new();
        if let Some(serde_bencode::value::Value::Dict(m_dict)) = dict.get(b"m".as_slice()) {
            for (k, v) in m_dict {
                if let serde_bencode::value::Value::Int(i) = v {
                    if let Ok(key) = std::str::from_utf8(k) {
                        if *i >= 1 && *i <= 255 {
                            m.insert(key.to_string(), *i as u32);
                        }
                    }
                }
            }
        }
        let metadata_size = match dict.get(b"metadata_size".as_slice()) {
            Some(serde_bencode::value::Value::Int(i)) => {
                let val = *i;
                if val <= 0 || val > 64 * 1024 * 1024 {
                    return None;
                }
                Some(val as u32)
            }
            _ => None,
        };
        let v = match dict.get(b"v".as_slice()) {
            Some(serde_bencode::value::Value::Bytes(b)) => {
                Some(String::from_utf8_lossy(b).to_string())
            }
            _ => None,
        };
        let your_ip = match dict.get(b"yourip".as_slice()) {
            Some(serde_bencode::value::Value::Bytes(b)) => {
                Some(String::from_utf8_lossy(b).to_string())
            }
            _ => None,
        };
        let reqq = match dict.get(b"reqq".as_slice()) {
            Some(serde_bencode::value::Value::Int(i)) => {
                if *i >= 1 && *i <= 64 {
                    Some(*i as u32)
                } else {
                    None
                }
            }
            _ => None,
        };
        Some(ExtendedHandshake {
            m,
            metadata_size,
            v,
            your_ip,
            reqq,
        })
    }
}

pub const UT_METADATA_REQUEST: u8 = 0;
pub const UT_METADATA_DATA: u8 = 1;
pub const UT_METADATA_REJECT: u8 = 2;

pub fn build_metadata_request(piece: u32) -> Vec<u8> {
    let mut dict = HashMap::new();
    dict.insert(
        b"msg_type".to_vec(),
        serde_bencode::value::Value::Int(UT_METADATA_REQUEST as i64),
    );
    dict.insert(
        b"piece".to_vec(),
        serde_bencode::value::Value::Int(piece as i64),
    );
    serde_bencode::to_bytes(&serde_bencode::value::Value::Dict(dict))
        .expect("serialization should not fail for constructed bencode values")
}

pub type MetadataResponse = Result<(u32, u32, Vec<u8>), ()>;

pub fn parse_metadata_response(data: &[u8]) -> Option<MetadataResponse> {
    let value: serde_bencode::value::Value = serde_bencode::from_bytes(data).ok()?;
    let dict = match &value {
        serde_bencode::value::Value::Dict(d) => d,
        _ => return None,
    };
    let msg_type = match dict.get(b"msg_type".as_slice()) {
        Some(serde_bencode::value::Value::Int(i)) if *i >= 0 && *i <= 255 => *i as u8,
        _ => return None,
    };
    if msg_type == UT_METADATA_REJECT {
        return Some(Err(()));
    }
    if msg_type != UT_METADATA_DATA {
        return None;
    }
    let piece = match dict.get(b"piece".as_slice()) {
        Some(serde_bencode::value::Value::Int(i)) if *i >= 0 => u32::try_from(*i).ok()?,
        _ => return None,
    };
    let total_size = match dict.get(b"total_size".as_slice()) {
        Some(serde_bencode::value::Value::Int(i)) if *i >= 0 => u32::try_from(*i).ok()?,
        _ => return None,
    };
    let dict_end = find_bencode_end(data)?;
    let meta_data = data[dict_end..].to_vec();
    Some(Ok((piece, total_size, meta_data)))
}

fn find_bencode_end(data: &[u8]) -> Option<usize> {
    let mut pos = 0;
    parse_one(data, &mut pos);
    if pos > 0 && pos <= data.len() {
        Some(pos)
    } else {
        None
    }
}

fn parse_one(data: &[u8], pos: &mut usize) {
    if *pos >= data.len() {
        return;
    }
    match data[*pos] {
        b'd' => {
            *pos += 1;
            while *pos < data.len() && data[*pos] != b'e' {
                parse_one(data, pos);
                if *pos >= data.len() {
                    return;
                }
                parse_one(data, pos);
            }
            if *pos < data.len() {
                *pos += 1;
            }
        }
        b'l' => {
            *pos += 1;
            while *pos < data.len() && data[*pos] != b'e' {
                parse_one(data, pos);
            }
            if *pos < data.len() {
                *pos += 1;
            }
        }
        b'i' => {
            *pos += 1;
            while *pos < data.len() && data[*pos] != b'e' {
                *pos += 1;
            }
            if *pos < data.len() {
                *pos += 1;
            }
        }
        _ => {
            let mut len_end = *pos;
            while len_end < data.len() && data[len_end].is_ascii_digit() {
                len_end += 1;
            }
            if len_end < data.len() && data[len_end] == b':' {
                let len_str = std::str::from_utf8(&data[*pos..len_end]).ok();
                let byte_len: usize = len_str.and_then(|s| s.parse().ok()).unwrap_or(0);
                *pos = len_end + 1 + byte_len;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_handshake_build() {
        let info_hash: [u8; 20] = [0xAA; 20];
        let peer_id: [u8; 20] = [0xBB; 20];
        let hs = Handshake::new(info_hash, peer_id);
        let bytes = hs.to_bytes();
        assert_eq!(bytes[0], 19);
        assert_eq!(&bytes[1..20], b"BitTorrent protocol");
        assert_eq!(&bytes[28..48], &info_hash);
        assert_eq!(&bytes[48..68], &peer_id);
        assert_eq!(bytes.len(), 68);
        assert!(hs.supports_extensions(), "extension bit should be set");
        assert_eq!(hs.reserved[5] & 0x10, 0x10, "extension protocol bit 5");
        assert_eq!(hs.reserved[7] & 0x01, 0x01, "DHT bit 7");
    }

    #[test]
    fn test_handshake_parse() {
        let info_hash: [u8; 20] = [0xAA; 20];
        let peer_id: [u8; 20] = [0xBB; 20];
        let original = Handshake::new(info_hash, peer_id);
        let parsed = Handshake::from_bytes(&original.to_bytes()).unwrap();
        assert_eq!(parsed.info_hash, info_hash);
        assert_eq!(parsed.peer_id, peer_id);
    }

    #[test]
    fn test_build_extended_handshake() {
        let ext = ExtendedHandshake {
            m: {
                let mut m = std::collections::BTreeMap::new();
                m.insert("ut_metadata".to_string(), 1u32);
                m
            },
            metadata_size: Some(1024),
            v: Some("btfind 0.1".to_string()),
            your_ip: None,
            reqq: None,
        };
        let dict = ext.to_dict();
        assert!(!dict.is_empty());
    }

    #[test]
    fn test_parse_metadata_response_data() {
        let mut dict = HashMap::new();
        dict.insert(
            b"msg_type".to_vec(),
            serde_bencode::value::Value::Int(UT_METADATA_DATA as i64),
        );
        dict.insert(b"piece".to_vec(), serde_bencode::value::Value::Int(0));
        dict.insert(
            b"total_size".to_vec(),
            serde_bencode::value::Value::Int(16384),
        );
        let mut payload =
            serde_bencode::to_bytes(&serde_bencode::value::Value::Dict(dict)).unwrap();
        payload.extend_from_slice(b"trailing_piece_data");

        let result = parse_metadata_response(&payload);
        assert!(result.is_some());
        if let Some(Ok((piece, total_size, data))) = result {
            assert_eq!(piece, 0);
            assert_eq!(total_size, 16384);
            assert_eq!(data, b"trailing_piece_data");
        } else {
            panic!("expected Ok data response");
        }
    }

    #[test]
    fn test_parse_metadata_response_reject() {
        let mut dict = HashMap::new();
        dict.insert(
            b"msg_type".to_vec(),
            serde_bencode::value::Value::Int(UT_METADATA_REJECT as i64),
        );
        dict.insert(b"piece".to_vec(), serde_bencode::value::Value::Int(3));
        let payload = serde_bencode::to_bytes(&serde_bencode::value::Value::Dict(dict)).unwrap();

        let result = parse_metadata_response(&payload);
        assert!(result.is_some());
        assert!(result.unwrap().is_err());
    }

    #[test]
    fn test_parse_metadata_response_unknown_type() {
        let mut dict = HashMap::new();
        dict.insert(b"msg_type".to_vec(), serde_bencode::value::Value::Int(99));
        let payload = serde_bencode::to_bytes(&serde_bencode::value::Value::Dict(dict)).unwrap();

        let result = parse_metadata_response(&payload);
        assert!(result.is_none());
    }

    fn metadata_response_payload(msg_type: i64, piece: i64, total_size: i64) -> Vec<u8> {
        let mut dict = HashMap::new();
        dict.insert(
            b"msg_type".to_vec(),
            serde_bencode::value::Value::Int(msg_type),
        );
        dict.insert(b"piece".to_vec(), serde_bencode::value::Value::Int(piece));
        dict.insert(
            b"total_size".to_vec(),
            serde_bencode::value::Value::Int(total_size),
        );
        serde_bencode::to_bytes(&serde_bencode::value::Value::Dict(dict)).unwrap()
    }

    #[test]
    fn test_parse_metadata_response_rejects_invalid_integer_ranges() {
        let cases = [
            (-1, 0, 16384),
            (256, 0, 16384),
            (UT_METADATA_DATA as i64, -1, 16384),
            (UT_METADATA_DATA as i64, u32::MAX as i64 + 1, 16384),
            (UT_METADATA_DATA as i64, 0, -1),
            (UT_METADATA_DATA as i64, 0, u32::MAX as i64 + 1),
        ];

        for (msg_type, piece, total_size) in cases {
            assert!(parse_metadata_response(&metadata_response_payload(
                msg_type, piece, total_size
            ))
            .is_none());
        }
    }

    #[test]
    fn test_parse_metadata_response_rejects_u32_overflow_piece() {
        let mut dict = HashMap::new();
        dict.insert(
            b"msg_type".to_vec(),
            serde_bencode::value::Value::Int(UT_METADATA_DATA as i64),
        );
        dict.insert(
            b"piece".to_vec(),
            serde_bencode::value::Value::Int(u32::MAX as i64 + 1),
        );
        dict.insert(
            b"total_size".to_vec(),
            serde_bencode::value::Value::Int(16384),
        );
        let payload = serde_bencode::to_bytes(&serde_bencode::value::Value::Dict(dict)).unwrap();

        assert!(parse_metadata_response(&payload).is_none());
    }

    #[test]
    fn test_parse_metadata_response_rejects_u32_overflow_total_size() {
        let mut dict = HashMap::new();
        dict.insert(
            b"msg_type".to_vec(),
            serde_bencode::value::Value::Int(UT_METADATA_DATA as i64),
        );
        dict.insert(b"piece".to_vec(), serde_bencode::value::Value::Int(0));
        dict.insert(
            b"total_size".to_vec(),
            serde_bencode::value::Value::Int(u32::MAX as i64 + 1),
        );
        let payload = serde_bencode::to_bytes(&serde_bencode::value::Value::Dict(dict)).unwrap();

        assert!(parse_metadata_response(&payload).is_none());
    }

    #[test]
    fn test_build_metadata_request_valid() {
        let payload = build_metadata_request(5);
        let value: serde_bencode::value::Value = serde_bencode::from_bytes(&payload).unwrap();
        if let serde_bencode::value::Value::Dict(dict) = &value {
            assert_eq!(
                dict.get(&b"msg_type".to_vec()),
                Some(&serde_bencode::value::Value::Int(
                    UT_METADATA_REQUEST as i64
                ))
            );
            assert_eq!(
                dict.get(&b"piece".to_vec()),
                Some(&serde_bencode::value::Value::Int(5))
            );
        } else {
            panic!("expected dict");
        }
    }

    #[test]
    fn test_extended_handshake_zero_metadata_size_rejected() {
        let ext = ExtendedHandshake {
            m: {
                let mut m = std::collections::BTreeMap::new();
                m.insert("ut_metadata".to_string(), 1u32);
                m
            },
            metadata_size: Some(0),
            v: None,
            your_ip: None,
            reqq: None,
        };
        let bytes =
            serde_bencode::to_bytes(&serde_bencode::value::Value::Dict(ext.to_dict())).unwrap();
        let parsed = ExtendedHandshake::from_bytes(&bytes);
        assert!(parsed.is_none(), "zero metadata_size should be rejected");
    }

    #[test]
    fn test_extended_handshake_negative_metadata_size() {
        let mut dict = HashMap::new();
        let mut m_dict = HashMap::new();
        m_dict.insert(
            "ut_metadata".as_bytes().to_vec(),
            serde_bencode::value::Value::Int(1),
        );
        dict.insert(b"m".to_vec(), serde_bencode::value::Value::Dict(m_dict));
        dict.insert(
            b"metadata_size".to_vec(),
            serde_bencode::value::Value::Int(-1),
        );
        let bytes = serde_bencode::to_bytes(&serde_bencode::value::Value::Dict(dict)).unwrap();
        assert!(ExtendedHandshake::from_bytes(&bytes).is_none());
    }

    #[test]
    fn test_extended_handshake_oversized_metadata_size() {
        let mut dict = HashMap::new();
        let mut m_dict = HashMap::new();
        m_dict.insert(
            "ut_metadata".as_bytes().to_vec(),
            serde_bencode::value::Value::Int(1),
        );
        dict.insert(b"m".to_vec(), serde_bencode::value::Value::Dict(m_dict));
        dict.insert(
            b"metadata_size".to_vec(),
            serde_bencode::value::Value::Int(100_000_000),
        );
        let bytes = serde_bencode::to_bytes(&serde_bencode::value::Value::Dict(dict)).unwrap();
        assert!(ExtendedHandshake::from_bytes(&bytes).is_none());
    }

    #[test]
    fn test_extended_handshake_invalid_ut_metadata_ids() {
        let make_with_m_value = |val: i64| -> Vec<u8> {
            let mut dict = HashMap::new();
            let mut m_dict = HashMap::new();
            m_dict.insert(
                "ut_metadata".as_bytes().to_vec(),
                serde_bencode::value::Value::Int(val),
            );
            dict.insert(b"m".to_vec(), serde_bencode::value::Value::Dict(m_dict));
            dict.insert(
                b"metadata_size".to_vec(),
                serde_bencode::value::Value::Int(1024),
            );
            serde_bencode::to_bytes(&serde_bencode::value::Value::Dict(dict)).unwrap()
        };

        // ut_metadata = -1 should be rejected
        let parsed = ExtendedHandshake::from_bytes(&make_with_m_value(-1)).unwrap();
        assert!(!parsed.m.contains_key("ut_metadata"));

        // ut_metadata = 0 should be rejected
        let parsed = ExtendedHandshake::from_bytes(&make_with_m_value(0)).unwrap();
        assert!(!parsed.m.contains_key("ut_metadata"));

        // ut_metadata = 300 should be rejected (>255)
        let parsed = ExtendedHandshake::from_bytes(&make_with_m_value(300)).unwrap();
        assert!(!parsed.m.contains_key("ut_metadata"));

        // ut_metadata = 1 should be accepted
        let parsed = ExtendedHandshake::from_bytes(&make_with_m_value(1)).unwrap();
        assert_eq!(parsed.m.get("ut_metadata"), Some(&1));
    }

    #[test]
    fn test_extended_handshake_invalid_reqq() {
        let make_with_reqq = |val: i64| -> Vec<u8> {
            let mut dict = HashMap::new();
            let mut m_dict = HashMap::new();
            m_dict.insert(
                "ut_metadata".as_bytes().to_vec(),
                serde_bencode::value::Value::Int(1),
            );
            dict.insert(b"m".to_vec(), serde_bencode::value::Value::Dict(m_dict));
            dict.insert(
                b"metadata_size".to_vec(),
                serde_bencode::value::Value::Int(1024),
            );
            dict.insert(b"reqq".to_vec(), serde_bencode::value::Value::Int(val));
            serde_bencode::to_bytes(&serde_bencode::value::Value::Dict(dict)).unwrap()
        };

        // reqq = -1 should be rejected
        assert!(ExtendedHandshake::from_bytes(&make_with_reqq(-1))
            .unwrap()
            .reqq
            .is_none());

        // reqq = 1000000 should be rejected
        assert!(ExtendedHandshake::from_bytes(&make_with_reqq(1000000))
            .unwrap()
            .reqq
            .is_none());

        // reqq = 10 should be accepted
        assert_eq!(
            ExtendedHandshake::from_bytes(&make_with_reqq(10))
                .unwrap()
                .reqq,
            Some(10)
        );
    }
}
