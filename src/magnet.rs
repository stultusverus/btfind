use percent_encoding::{utf8_percent_encode, NON_ALPHANUMERIC};

pub fn magnet_uri(info_hash_hex: &str, display_name: Option<&str>) -> Result<String, String> {
    if info_hash_hex.len() != 40 {
        return Err(format!(
            "info_hash must be exactly 40 hex characters, got {}",
            info_hash_hex.len()
        ));
    }

    let lower = info_hash_hex.to_ascii_lowercase();

    if lower.chars().any(|c| !c.is_ascii_hexdigit()) {
        return Err("info_hash contains non-hex characters".to_string());
    }

    let mut uri = format!("magnet:?xt=urn:btih:{}", lower);

    if let Some(name) = display_name {
        if !name.is_empty() {
            let encoded = utf8_percent_encode(name, NON_ALPHANUMERIC);
            uri.push_str("&dn=");
            uri.push_str(&encoded.to_string());
        }
    }

    Ok(uri)
}

pub fn magnet_uri_from_hash(info_hash: &[u8; 20], display_name: Option<&str>) -> String {
    let hex = hex::encode(info_hash);
    magnet_uri(&hex, display_name).expect("hex-encoded 20-byte hash always produces 40 hex chars")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_valid_hash_without_name() {
        let uri = magnet_uri("0123456789abcdef0123456789abcdef01234567", None).unwrap();
        assert_eq!(
            uri,
            "magnet:?xt=urn:btih:0123456789abcdef0123456789abcdef01234567"
        );
    }

    #[test]
    fn test_uppercase_hash_normalised() {
        let uri = magnet_uri("0123456789ABCDEF0123456789ABCDEF01234567", None).unwrap();
        assert_eq!(
            uri,
            "magnet:?xt=urn:btih:0123456789abcdef0123456789abcdef01234567"
        );
    }

    #[test]
    fn test_valid_hash_with_display_name() {
        let uri = magnet_uri(
            "0123456789abcdef0123456789abcdef01234567",
            Some("Ubuntu ISO"),
        )
        .unwrap();
        assert_eq!(
            uri,
            "magnet:?xt=urn:btih:0123456789abcdef0123456789abcdef01234567&dn=Ubuntu%20ISO"
        );
    }

    #[test]
    fn test_valid_hash_with_display_name_special_chars() {
        let uri = magnet_uri(
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            Some("hello & world! test"),
        )
        .unwrap();
        assert_eq!(uri, "magnet:?xt=urn:btih:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa&dn=hello%20%26%20world%21%20test");
    }

    #[test]
    fn test_invalid_short_hash() {
        let result = magnet_uri("abc123", None);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("40"));
    }

    #[test]
    fn test_invalid_non_hex_hash() {
        let result = magnet_uri("gggggggggggggggggggggggggggggggggggggggg", None);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("non-hex"));
    }

    #[test]
    fn test_magnet_uri_from_hash() {
        let hash = [0xABu8; 20];
        let uri = magnet_uri_from_hash(&hash, Some("Test"));
        assert!(uri.starts_with("magnet:?xt=urn:btih:abababababababababababababababababababab"));
        assert!(uri.contains("&dn=Test"));
    }

    #[test]
    fn test_hash_with_empty_display_name() {
        let uri = magnet_uri("0123456789abcdef0123456789abcdef01234567", Some("")).unwrap();
        assert_eq!(
            uri,
            "magnet:?xt=urn:btih:0123456789abcdef0123456789abcdef01234567"
        );
    }
}
