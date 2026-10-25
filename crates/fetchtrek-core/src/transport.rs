use std::pin::Pin;

use async_trait::async_trait;
use fetchtrek_types::{ByteRange, NetworkError};
use tokio::io::AsyncRead;

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ProbeResult {
    pub total_bytes: Option<u64>,
    pub supports_ranges: bool,
    pub etag: Option<String>,
    pub mime_type: Option<String>,
    pub suggested_filename: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct SessionContext {
    pub headers: Vec<(String, String)>,
    pub cookies: Option<String>,
    pub user_agent: Option<String>,
    pub referer: Option<String>,
}

impl SessionContext {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_user_agent(mut self, ua: impl Into<String>) -> Self {
        self.user_agent = Some(ua.into());
        self
    }

    pub fn with_referer(mut self, ref_url: impl Into<String>) -> Self {
        self.referer = Some(ref_url.into());
        self
    }

    pub fn with_cookies(mut self, cookies: impl Into<String>) -> Self {
        self.cookies = Some(cookies.into());
        self
    }

    pub fn with_header(mut self, key: impl Into<String>, val: impl Into<String>) -> Self {
        self.headers.push((key.into(), val.into()));
        self
    }
}

pub trait ByteStream: AsyncRead + Send + Unpin + 'static {}
impl<T: AsyncRead + Send + Unpin + 'static> ByteStream for T {}

impl core::fmt::Debug for dyn ByteStream {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "ByteStream")
    }
}

#[async_trait]
pub trait NetworkTransport: Send + Sync + 'static {
    async fn probe(&self, url: &str, context: &SessionContext)
        -> Result<ProbeResult, NetworkError>;
    async fn open_range_stream(
        &self,
        url: &str,
        range: ByteRange,
        context: &SessionContext,
    ) -> Result<Pin<Box<dyn ByteStream>>, NetworkError>;
    async fn open_stream(
        &self,
        url: &str,
        context: &SessionContext,
    ) -> Result<Pin<Box<dyn ByteStream>>, NetworkError> {
        let probe = self.probe(url, context).await?;
        let end = probe
            .total_bytes
            .map(|t| t.saturating_sub(1))
            .unwrap_or(u64::MAX - 1);
        self.open_range_stream(url, ByteRange::new(0, end), context)
            .await
    }
}
