use std::collections::{BTreeMap, HashMap};

/// A value in a bencoded dictionary.
#[derive(Debug, Clone, PartialEq)]
pub enum KrpcValue {
    Bytes(Vec<u8>),
    Int(i64),
    List(Vec<KrpcValue>),
    Dict(BTreeMap<String, KrpcValue>),
}

/// A parsed KRPC message.
#[derive(Debug, Clone)]
pub enum KrpcMessage {
    Query {
        t: Vec<u8>,
        y: String,
        q: String,
        a: BTreeMap<String, KrpcValue>,
    },
    Response {
        t: Vec<u8>,
        y: String,
        r: BTreeMap<String, KrpcValue>,
    },
    Error {
        t: Vec<u8>,
        y: String,
        e: KrpcError,
    },
}

#[derive(Debug, Clone)]
pub struct KrpcError {
    pub code: i64,
    pub description: String,
}

/// Convert our KrpcValue to serde_bencode::value::Value for serialization.
fn krpc_to_bencode(v: &KrpcValue) -> serde_bencode::value::Value {
    match v {
        KrpcValue::Bytes(b) => serde_bencode::value::Value::Bytes(b.clone()),
        KrpcValue::Int(i) => serde_bencode::value::Value::Int(*i),
        KrpcValue::List(l) => {
            serde_bencode::value::Value::List(l.iter().map(krpc_to_bencode).collect())
        }
        KrpcValue::Dict(d) => {
            let mut map = HashMap::new();
            for (k, val) in d {
                map.insert(k.clone().into_bytes(), krpc_to_bencode(val));
            }
            serde_bencode::value::Value::Dict(map)
        }
    }
}

/// Convert serde_bencode::value::Value back to our KrpcValue.
fn bencode_to_krpc(v: &serde_bencode::value::Value) -> KrpcValue {
    match v {
        serde_bencode::value::Value::Bytes(b) => KrpcValue::Bytes(b.clone()),
        serde_bencode::value::Value::Int(i) => KrpcValue::Int(*i),
        serde_bencode::value::Value::List(l) => {
            KrpcValue::List(l.iter().map(bencode_to_krpc).collect())
        }
        serde_bencode::value::Value::Dict(d) => {
            let mut map = BTreeMap::new();
            for (k, val) in d {
                if let Ok(key) = String::from_utf8(k.clone()) {
                    map.insert(key, bencode_to_krpc(val));
                }
            }
            KrpcValue::Dict(map)
        }
    }
}

impl KrpcMessage {
    pub fn to_bytes(&self) -> Vec<u8> {
        match self {
            KrpcMessage::Query { t, y, q, a } => {
                let mut dict = HashMap::new();
                dict.insert(
                    "t".as_bytes().to_vec(),
                    serde_bencode::value::Value::Bytes(t.clone()),
                );
                dict.insert(
                    "y".as_bytes().to_vec(),
                    serde_bencode::value::Value::Bytes(y.as_bytes().to_vec()),
                );
                dict.insert(
                    "q".as_bytes().to_vec(),
                    serde_bencode::value::Value::Bytes(q.as_bytes().to_vec()),
                );
                let mut a_dict = HashMap::new();
                for (k, v) in a {
                    a_dict.insert(k.as_bytes().to_vec(), krpc_to_bencode(v));
                }
                dict.insert(
                    "a".as_bytes().to_vec(),
                    serde_bencode::value::Value::Dict(a_dict),
                );
                serde_bencode::to_bytes(&serde_bencode::value::Value::Dict(dict))
                    .expect("serialization should not fail for constructed bencode values")
            }
            KrpcMessage::Response { t, y, r } => {
                let mut dict = HashMap::new();
                dict.insert(
                    "t".as_bytes().to_vec(),
                    serde_bencode::value::Value::Bytes(t.clone()),
                );
                dict.insert(
                    "y".as_bytes().to_vec(),
                    serde_bencode::value::Value::Bytes(y.as_bytes().to_vec()),
                );
                let mut r_dict = HashMap::new();
                for (k, v) in r {
                    r_dict.insert(k.as_bytes().to_vec(), krpc_to_bencode(v));
                }
                dict.insert(
                    "r".as_bytes().to_vec(),
                    serde_bencode::value::Value::Dict(r_dict),
                );
                serde_bencode::to_bytes(&serde_bencode::value::Value::Dict(dict))
                    .expect("serialization should not fail for constructed bencode values")
            }
            KrpcMessage::Error { t, y, e } => {
                let mut dict = HashMap::new();
                dict.insert(
                    "t".as_bytes().to_vec(),
                    serde_bencode::value::Value::Bytes(t.clone()),
                );
                dict.insert(
                    "y".as_bytes().to_vec(),
                    serde_bencode::value::Value::Bytes(y.as_bytes().to_vec()),
                );
                let e_list = vec![
                    serde_bencode::value::Value::Int(e.code),
                    serde_bencode::value::Value::Bytes(e.description.as_bytes().to_vec()),
                ];
                dict.insert(
                    "e".as_bytes().to_vec(),
                    serde_bencode::value::Value::List(e_list),
                );
                serde_bencode::to_bytes(&serde_bencode::value::Value::Dict(dict))
                    .expect("serialization should not fail for constructed bencode values")
            }
        }
    }

    pub fn from_bytes(data: &[u8]) -> Result<Self, String> {
        let value: serde_bencode::value::Value =
            serde_bencode::from_bytes(data).map_err(|e| format!("bencode decode error: {}", e))?;

        let dict = match &value {
            serde_bencode::value::Value::Dict(d) => d,
            _ => return Err("top-level value is not a dict".into()),
        };

        let t = get_bytes(dict, "t").ok_or("missing t")?;
        let y = get_string(dict, "y").ok_or("missing y")?;

        match y.as_str() {
            "q" => {
                let q = get_string(dict, "q").ok_or("missing q")?;
                let mut a_map = BTreeMap::new();
                if let Some(a) = get_dict(dict, "a") {
                    for (k, v) in a {
                        if let Ok(key) = std::str::from_utf8(k) {
                            a_map.insert(key.to_string(), bencode_to_krpc(v));
                        }
                    }
                }
                Ok(KrpcMessage::Query { t, y, q, a: a_map })
            }
            "r" => {
                let mut r_map = BTreeMap::new();
                if let Some(r) = get_dict(dict, "r") {
                    for (k, v) in r {
                        if let Ok(key) = std::str::from_utf8(k) {
                            r_map.insert(key.to_string(), bencode_to_krpc(v));
                        }
                    }
                }
                Ok(KrpcMessage::Response { t, y, r: r_map })
            }
            "e" => {
                let e_list = get_list(dict, "e").ok_or("missing e")?;
                let code = match e_list.first() {
                    Some(serde_bencode::value::Value::Int(i)) => *i,
                    _ => return Err("invalid error code".into()),
                };
                let description = match e_list.get(1) {
                    Some(serde_bencode::value::Value::Bytes(b)) => {
                        String::from_utf8_lossy(b).to_string()
                    }
                    _ => "unknown error".to_string(),
                };
                Ok(KrpcMessage::Error {
                    t,
                    y,
                    e: KrpcError { code, description },
                })
            }
            _ => Err(format!("unknown message type: {}", y)),
        }
    }

    #[allow(dead_code)]
    pub fn is_query(&self) -> bool {
        matches!(self, KrpcMessage::Query { .. })
    }

    #[allow(dead_code)]
    pub fn is_response(&self) -> bool {
        matches!(self, KrpcMessage::Response { .. })
    }

    #[allow(dead_code)]
    pub fn transaction_id(&self) -> &[u8] {
        match self {
            KrpcMessage::Query { t, .. }
            | KrpcMessage::Response { t, .. }
            | KrpcMessage::Error { t, .. } => t,
        }
    }
}

fn get_bytes(dict: &HashMap<Vec<u8>, serde_bencode::value::Value>, key: &str) -> Option<Vec<u8>> {
    match dict.get(key.as_bytes())? {
        serde_bencode::value::Value::Bytes(b) => Some(b.clone()),
        _ => None,
    }
}

fn get_string(dict: &HashMap<Vec<u8>, serde_bencode::value::Value>, key: &str) -> Option<String> {
    get_bytes(dict, key).map(|b| String::from_utf8_lossy(&b).to_string())
}

fn get_dict<'a>(
    dict: &'a HashMap<Vec<u8>, serde_bencode::value::Value>,
    key: &str,
) -> Option<&'a HashMap<Vec<u8>, serde_bencode::value::Value>> {
    match dict.get(key.as_bytes())? {
        serde_bencode::value::Value::Dict(d) => Some(d),
        _ => None,
    }
}

fn get_list<'a>(
    dict: &'a HashMap<Vec<u8>, serde_bencode::value::Value>,
    key: &str,
) -> Option<&'a [serde_bencode::value::Value]> {
    match dict.get(key.as_bytes())? {
        serde_bencode::value::Value::List(l) => Some(l.as_slice()),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    #[test]
    fn test_serialize_query() {
        let mut a = BTreeMap::new();
        a.insert(
            "id".to_string(),
            KrpcValue::Bytes(b"abcdefghij0123456789".to_vec()),
        );
        a.insert(
            "target".to_string(),
            KrpcValue::Bytes(b"mnopqrstuvwxyz123456".to_vec()),
        );

        let msg = KrpcMessage::Query {
            t: b"aa".to_vec(),
            y: "q".to_string(),
            q: "find_node".to_string(),
            a,
        };

        let encoded = msg.to_bytes();
        assert!(!encoded.is_empty());
    }

    #[test]
    fn test_deserialize_response() {
        // A typical DHT response: {"t":"aa","y":"r","r":{"id":"...","nodes":"..."}}
        let raw = b"d1:rd2:id20:abcdefghij01234567895:nodes0:e1:t2:aa1:y1:re";
        let msg = KrpcMessage::from_bytes(raw).expect("should parse");
        match msg {
            KrpcMessage::Response { t, r, .. } => {
                assert_eq!(t, b"aa");
                assert!(r.contains_key("id"));
            }
            _ => panic!("expected response"),
        }
    }

    #[test]
    fn test_deserialize_query() {
        let raw = b"d1:ad2:id20:abcdefghij01234567896:target20:mnopqrstuvwxyz123456e1:q9:find_node1:t2:aa1:y1:qe";
        let msg = KrpcMessage::from_bytes(raw).expect("should parse");
        match msg {
            KrpcMessage::Query { t, q, .. } => {
                assert_eq!(t, b"aa");
                assert_eq!(q, "find_node");
            }
            _ => panic!("expected query"),
        }
    }
}
