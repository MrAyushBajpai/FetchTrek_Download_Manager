use fetchtrek_types::ByteRange;

use crate::error::PersistenceError;

pub const FORMAT_VERSION: u8 = 1;

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct IntervalSet {
    ranges: Vec<ByteRange>,
}

impl IntervalSet {
    pub fn new() -> Self {
        Self { ranges: Vec::new() }
    }

    pub fn len(&self) -> usize {
        self.ranges.len()
    }

    pub fn is_empty(&self) -> bool {
        self.ranges.is_empty()
    }

    pub fn clear(&mut self) {
        self.ranges.clear();
    }

    pub fn ranges(&self) -> &[ByteRange] {
        &self.ranges
    }

    pub fn total_verified_bytes(&self) -> u64 {
        self.ranges.iter().map(|r| r.len()).sum()
    }

    pub fn is_complete(&self, total_bytes: u64) -> bool {
        if total_bytes == 0 {
            return self.ranges.is_empty();
        }
        self.ranges.len() == 1 && self.ranges[0].start == 0 && self.ranges[0].end == total_bytes - 1
    }

    pub fn contains_byte(&self, offset: u64) -> bool {
        let idx = self.ranges.partition_point(|r| r.end < offset);
        if idx < self.ranges.len() {
            self.ranges[idx].contains(offset)
        } else {
            false
        }
    }

    pub fn contains_range(&self, start: u64, end: u64) -> bool {
        if start > end {
            return false;
        }
        let idx = self.ranges.partition_point(|r| r.end < start);
        if idx < self.ranges.len() {
            self.ranges[idx].start <= start && self.ranges[idx].end >= end
        } else {
            false
        }
    }

    /// Inserts a new closed range `[start, end]`, coalescing overlapping and adjacent intervals.
    /// Insertion maintains strictly sorted order via binary search (`partition_point`).
    pub fn add_range(&mut self, start: u64, end: u64) -> Result<(), PersistenceError> {
        if start > end {
            return Err(PersistenceError::Corruption(format!(
                "Invalid interval bounds: start ({}) > end ({})",
                start, end
            )));
        }

        // Find overlapping or touching range window
        // left: first range whose end + 1 >= start
        let left = self
            .ranges
            .partition_point(|r| r.end.saturating_add(1) < start);

        // right: first range whose start > end + 1
        let right = self
            .ranges
            .partition_point(|r| r.start <= end.saturating_add(1));

        if left == right {
            // No overlapping or adjacent ranges; insert new range at left
            self.ranges.insert(left, ByteRange::new(start, end));
        } else {
            // Coalesce all ranges in left..right with [start, end]
            let merged_start = start.min(self.ranges[left].start);
            let merged_end = end.max(self.ranges[right - 1].end);
            self.ranges
                .splice(left..right, [ByteRange::new(merged_start, merged_end)]);
        }

        Ok(())
    }

    /// Serializes the interval set into a compact binary representation for BLOB storage.
    pub fn to_bytes(&self) -> Vec<u8> {
        let count = self.ranges.len() as u32;
        let mut bytes = Vec::with_capacity(5 + self.ranges.len() * 16);
        bytes.push(FORMAT_VERSION);
        bytes.extend_from_slice(&count.to_le_bytes());
        for r in &self.ranges {
            bytes.extend_from_slice(&r.start.to_le_bytes());
            bytes.extend_from_slice(&r.end.to_le_bytes());
        }
        bytes
    }

    /// Deserializes a compact binary representation into a validated, sorted `IntervalSet`.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, PersistenceError> {
        if bytes.is_empty() {
            return Ok(Self::new());
        }

        if bytes.len() < 5 {
            return Err(PersistenceError::Corruption(format!(
                "Payload too short for interval set: {} bytes",
                bytes.len()
            )));
        }

        let version = bytes[0];
        if version != FORMAT_VERSION {
            return Err(PersistenceError::Corruption(format!(
                "Unsupported interval format version: {}, expected {}",
                version, FORMAT_VERSION
            )));
        }

        let count = u32::from_le_bytes([bytes[1], bytes[2], bytes[3], bytes[4]]) as usize;
        let expected_len = 5 + count * 16;
        if bytes.len() != expected_len {
            return Err(PersistenceError::Corruption(format!(
                "Payload length mismatch: expected {} bytes for {} ranges, got {}",
                expected_len,
                count,
                bytes.len()
            )));
        }

        let mut ranges = Vec::with_capacity(count);
        let mut offset = 5;
        let mut prev_end: Option<u64> = None;

        for _ in 0..count {
            let start = u64::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap());
            let end = u64::from_le_bytes(bytes[offset + 8..offset + 16].try_into().unwrap());
            offset += 16;

            if start > end {
                return Err(PersistenceError::Corruption(format!(
                    "Invalid interval bounds: start ({}) > end ({})",
                    start, end
                )));
            }

            if let Some(prev) = prev_end {
                if start <= prev.saturating_add(1) {
                    return Err(PersistenceError::Corruption(format!(
                        "Unsorted, overlapping, or uncoalesced intervals: previous end {}, current start {}",
                        prev, start
                    )));
                }
            }

            prev_end = Some(end);
            ranges.push(ByteRange::new(start, end));
        }

        Ok(Self { ranges })
    }
}

impl rusqlite::ToSql for IntervalSet {
    fn to_sql(&self) -> rusqlite::Result<rusqlite::types::ToSqlOutput<'_>> {
        Ok(rusqlite::types::ToSqlOutput::from(self.to_bytes()))
    }
}

impl rusqlite::types::FromSql for IntervalSet {
    fn column_result(value: rusqlite::types::ValueRef<'_>) -> rusqlite::types::FromSqlResult<Self> {
        let bytes = value.as_blob()?;
        Self::from_bytes(bytes).map_err(|e| rusqlite::types::FromSqlError::Other(Box::new(e)))
    }
}
