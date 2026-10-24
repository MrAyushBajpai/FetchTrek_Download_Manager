use core::fmt;

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Debug, Default)]
pub struct TaskId(pub u64);

impl TaskId {
    pub const fn new(id: u64) -> Self {
        Self(id)
    }

    pub const fn as_u64(&self) -> u64 {
        self.0
    }
}

impl fmt::Display for TaskId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct ByteRange {
    pub start: u64,
    pub end: u64,
}

impl ByteRange {
    pub const fn new(start: u64, end: u64) -> Self {
        assert!(start <= end, "ByteRange start must be <= end");
        Self { start, end }
    }

    pub const fn len(&self) -> u64 {
        if self.start <= self.end {
            self.end - self.start + 1
        } else {
            0
        }
    }

    pub const fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub const fn contains(&self, offset: u64) -> bool {
        offset >= self.start && offset <= self.end
    }

    pub const fn overlaps(&self, other: &Self) -> bool {
        !self.is_empty() && !other.is_empty() && self.start <= other.end && other.start <= self.end
    }
}

impl fmt::Display for ByteRange {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}-{}", self.start, self.end)
    }
}

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
#[repr(u8)]
pub enum Protocol {
    Http = 0,
    Https = 1,
    BitTorrent = 2,
    Hls = 3,
    Dash = 4,
}

impl Protocol {
    pub const fn as_u8(&self) -> u8 {
        *self as u8
    }
}

impl TryFrom<u8> for Protocol {
    type Error = ();

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::Http),
            1 => Ok(Self::Https),
            2 => Ok(Self::BitTorrent),
            3 => Ok(Self::Hls),
            4 => Ok(Self::Dash),
            _ => Err(()),
        }
    }
}

impl fmt::Display for Protocol {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Http => write!(f, "http"),
            Self::Https => write!(f, "https"),
            Self::BitTorrent => write!(f, "bittorrent"),
            Self::Hls => write!(f, "hls"),
            Self::Dash => write!(f, "dash"),
        }
    }
}

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
#[repr(u8)]
pub enum SegmentStatus {
    Pending = 0,
    Downloading = 1,
    Verifying = 2,
    Completed = 3,
    Failed = 4,
}

impl SegmentStatus {
    pub const fn as_u8(&self) -> u8 {
        *self as u8
    }
}

impl TryFrom<u8> for SegmentStatus {
    type Error = ();

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::Pending),
            1 => Ok(Self::Downloading),
            2 => Ok(Self::Verifying),
            3 => Ok(Self::Completed),
            4 => Ok(Self::Failed),
            _ => Err(()),
        }
    }
}

impl fmt::Display for SegmentStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Pending => write!(f, "pending"),
            Self::Downloading => write!(f, "downloading"),
            Self::Verifying => write!(f, "verifying"),
            Self::Completed => write!(f, "completed"),
            Self::Failed => write!(f, "failed"),
        }
    }
}
