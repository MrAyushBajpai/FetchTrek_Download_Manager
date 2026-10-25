use std::collections::BTreeMap;

use super::error::TorrentError;

/// Core Bencode data types specified by BEP 0003.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BencodeValue {
    Integer(i64),
    Bytes(Vec<u8>),
    List(Vec<BencodeValue>),
    Dict(BTreeMap<Vec<u8>, BencodeValue>),
}

impl BencodeValue {
    /// Decodes a complete Bencode payload from a byte slice.
    /// Rejects any input with syntax errors, out-of-order keys, or trailing bytes.
    pub fn decode(input: &[u8]) -> Result<Self, TorrentError> {
        let (val, consumed) = parse_value(input, 0)?;
        if consumed != input.len() {
            return Err(TorrentError::BencodeError(format!(
                "Trailing bytes after bencode value: consumed {consumed} of {}",
                input.len()
            )));
        }
        Ok(val)
    }

    /// Encodes this Bencode value into deterministic, canonical byte representation.
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();
        self.encode_to(&mut out);
        out
    }

    /// Serializes canonical Bencode bytes directly into the given vector buffer.
    pub fn encode_to(&self, out: &mut Vec<u8>) {
        match self {
            BencodeValue::Integer(n) => {
                out.push(b'i');
                out.extend_from_slice(n.to_string().as_bytes());
                out.push(b'e');
            }
            BencodeValue::Bytes(b) => {
                out.extend_from_slice(b.len().to_string().as_bytes());
                out.push(b':');
                out.extend_from_slice(b);
            }
            BencodeValue::List(items) => {
                out.push(b'l');
                for item in items {
                    item.encode_to(out);
                }
                out.push(b'e');
            }
            BencodeValue::Dict(map) => {
                out.push(b'd');
                for (k, v) in map {
                    out.extend_from_slice(k.len().to_string().as_bytes());
                    out.push(b':');
                    out.extend_from_slice(k);
                    v.encode_to(out);
                }
                out.push(b'e');
            }
        }
    }

    /// Returns the integer value if this is `BencodeValue::Integer`.
    pub fn as_int(&self) -> Option<i64> {
        match self {
            BencodeValue::Integer(n) => Some(*n),
            _ => None,
        }
    }

    /// Returns the byte slice if this is `BencodeValue::Bytes`.
    pub fn as_bytes(&self) -> Option<&[u8]> {
        match self {
            BencodeValue::Bytes(b) => Some(b),
            _ => None,
        }
    }

    /// Returns the UTF-8 string view if this is `BencodeValue::Bytes` containing valid UTF-8.
    pub fn as_str(&self) -> Option<&str> {
        self.as_bytes().and_then(|b| std::str::from_utf8(b).ok())
    }

    /// Returns the list slice if this is `BencodeValue::List`.
    pub fn as_list(&self) -> Option<&[BencodeValue]> {
        match self {
            BencodeValue::List(l) => Some(l),
            _ => None,
        }
    }

    /// Returns the map view if this is `BencodeValue::Dict`.
    pub fn as_dict(&self) -> Option<&BTreeMap<Vec<u8>, BencodeValue>> {
        match self {
            BencodeValue::Dict(d) => Some(d),
            _ => None,
        }
    }

    /// Looks up a key inside a dictionary value.
    pub fn get(&self, key: &[u8]) -> Option<&BencodeValue> {
        self.as_dict().and_then(|d| d.get(key))
    }
}

/// Recursively parses a single `BencodeValue` starting at `offset`.
/// Returns `(value, new_offset)`.
fn parse_value(input: &[u8], offset: usize) -> Result<(BencodeValue, usize), TorrentError> {
    if offset >= input.len() {
        return Err(TorrentError::BencodeError(
            "Unexpected end of input while expecting value".into(),
        ));
    }

    match input[offset] {
        b'i' => parse_integer(input, offset),
        b'0'..=b'9' => parse_bytes(input, offset),
        b'l' => parse_list(input, offset),
        b'd' => parse_dict(input, offset),
        other => Err(TorrentError::BencodeError(format!(
            "Invalid character at offset {offset}: '{:?}'",
            other as char
        ))),
    }
}

/// Parses integer: `i<ascii_int>e`.
fn parse_integer(input: &[u8], offset: usize) -> Result<(BencodeValue, usize), TorrentError> {
    debug_assert_eq!(input[offset], b'i');
    let start = offset + 1;
    let end = input[start..]
        .iter()
        .position(|&b| b == b'e')
        .map(|idx| start + idx)
        .ok_or_else(|| TorrentError::BencodeError("Unterminated integer: missing 'e'".into()))?;

    let int_slice = &input[start..end];
    if int_slice.is_empty() {
        return Err(TorrentError::BencodeError(
            "Empty integer payload 'ie'".into(),
        ));
    }

    // Check invalid zero formats: "i-0e", "i03e"
    if int_slice == b"-0" {
        return Err(TorrentError::BencodeError(
            "Negative zero 'i-0e' is forbidden in Bencode".into(),
        ));
    }

    if int_slice.starts_with(b"-") {
        if int_slice.len() == 1 {
            return Err(TorrentError::BencodeError(
                "Integer with only negative sign".into(),
            ));
        }
        // Leading zeros after minus: e.g. "-05"
        if int_slice.len() > 2 && int_slice[1] == b'0' {
            return Err(TorrentError::BencodeError(
                "Leading zero in negative integer is forbidden".into(),
            ));
        }
    } else if int_slice.len() > 1 && int_slice[0] == b'0' {
        return Err(TorrentError::BencodeError(
            "Leading zero in integer is forbidden in Bencode".into(),
        ));
    }

    let int_str = std::str::from_utf8(int_slice)
        .map_err(|_| TorrentError::BencodeError("Integer contains non-UTF8 characters".into()))?;

    let value = int_str
        .parse::<i64>()
        .map_err(|e| TorrentError::BencodeError(format!("Integer parse failure: {e}")))?;

    Ok((BencodeValue::Integer(value), end + 1))
}

/// Parses byte string: `<len>:<bytes>`.
fn parse_bytes(input: &[u8], offset: usize) -> Result<(BencodeValue, usize), TorrentError> {
    let colon_pos = input[offset..]
        .iter()
        .position(|&b| b == b':')
        .map(|idx| offset + idx)
        .ok_or_else(|| TorrentError::BencodeError("Byte string missing colon ':'".into()))?;

    let len_slice = &input[offset..colon_pos];
    if len_slice.is_empty() {
        return Err(TorrentError::BencodeError(
            "Byte string missing length digits".into(),
        ));
    }

    // Rejection of leading zero unless single '0'
    if len_slice.len() > 1 && len_slice[0] == b'0' {
        return Err(TorrentError::BencodeError(
            "Byte string length contains forbidden leading zeros".into(),
        ));
    }

    let len_str = std::str::from_utf8(len_slice).map_err(|_| {
        TorrentError::BencodeError("Byte string length contains non-UTF8 digits".into())
    })?;

    let len = len_str
        .parse::<usize>()
        .map_err(|e| TorrentError::BencodeError(format!("Invalid byte string length: {e}")))?;

    let data_start = colon_pos + 1;
    let data_end = data_start
        .checked_add(len)
        .ok_or_else(|| TorrentError::BencodeError("Byte string length overflow".into()))?;

    if data_end > input.len() {
        return Err(TorrentError::BencodeError(format!(
            "Byte string truncated: declared {len} bytes, only {} available",
            input.len().saturating_sub(data_start)
        )));
    }

    let bytes = input[data_start..data_end].to_vec();
    Ok((BencodeValue::Bytes(bytes), data_end))
}

/// Parses list: `l<items>e`.
fn parse_list(input: &[u8], offset: usize) -> Result<(BencodeValue, usize), TorrentError> {
    debug_assert_eq!(input[offset], b'l');
    let mut cur = offset + 1;
    let mut items = Vec::new();

    while cur < input.len() {
        if input[cur] == b'e' {
            return Ok((BencodeValue::List(items), cur + 1));
        }
        let (val, next_cur) = parse_value(input, cur)?;
        items.push(val);
        cur = next_cur;
    }

    Err(TorrentError::BencodeError(
        "Unterminated list: missing closing 'e'".into(),
    ))
}

/// Parses dictionary: `d<key><val>...e`.
/// Enforces BEP 0003 strictly: keys must be byte strings sorted lexicographically.
fn parse_dict(input: &[u8], offset: usize) -> Result<(BencodeValue, usize), TorrentError> {
    debug_assert_eq!(input[offset], b'd');
    let mut cur = offset + 1;
    let mut map = BTreeMap::new();
    let mut last_key: Option<Vec<u8>> = None;

    while cur < input.len() {
        if input[cur] == b'e' {
            return Ok((BencodeValue::Dict(map), cur + 1));
        }

        // Key MUST be a byte string
        if !input[cur].is_ascii_digit() {
            return Err(TorrentError::BencodeError(format!(
                "Dictionary key at offset {cur} must be a byte string, found byte {:?}",
                input[cur] as char
            )));
        }

        let (key_val, next_cur) = parse_bytes(input, cur)?;
        let key_bytes = match key_val {
            BencodeValue::Bytes(b) => b,
            _ => unreachable!(),
        };

        // Validate strictly ascending lexicographical byte order
        if let Some(prev) = &last_key {
            match key_bytes.cmp(prev) {
                std::cmp::Ordering::Less => {
                    return Err(TorrentError::BencodeError(format!(
                        "Dictionary keys out of order: key {:?} appears after {:?}",
                        String::from_utf8_lossy(&key_bytes),
                        String::from_utf8_lossy(prev)
                    )));
                }
                std::cmp::Ordering::Equal => {
                    return Err(TorrentError::BencodeError(format!(
                        "Duplicate dictionary key: {:?}",
                        String::from_utf8_lossy(&key_bytes)
                    )));
                }
                std::cmp::Ordering::Greater => {}
            }
        }

        last_key = Some(key_bytes.clone());
        cur = next_cur;

        // Parse corresponding value
        let (val, after_val) = parse_value(input, cur)?;
        map.insert(key_bytes, val);
        cur = after_val;
    }

    Err(TorrentError::BencodeError(
        "Unterminated dictionary: missing closing 'e'".into(),
    ))
}

/// Extracts the raw byte slice of a specific top-level key's value inside a root dictionary.
/// Used to extract the raw `info` dictionary slice for SHA-1 infohash calculation.
pub fn extract_raw_dict_value<'a>(input: &'a [u8], key: &[u8]) -> Result<&'a [u8], TorrentError> {
    if input.is_empty() || input[0] != b'd' {
        return Err(TorrentError::BencodeError(
            "Root must be a bencoded dictionary".into(),
        ));
    }

    let mut cur = 1;
    while cur < input.len() {
        if input[cur] == b'e' {
            break;
        }

        // Parse key
        let (key_val, next_cur) = parse_bytes(input, cur)?;
        let current_key = match key_val {
            BencodeValue::Bytes(b) => b,
            _ => unreachable!(),
        };
        cur = next_cur;

        let val_start = cur;
        let (_, val_end) = parse_value(input, cur)?;

        if current_key == key {
            return Ok(&input[val_start..val_end]);
        }

        cur = val_end;
    }

    Err(TorrentError::InvalidMetainfo(format!(
        "Key {:?} not found in dictionary",
        String::from_utf8_lossy(key)
    )))
}
