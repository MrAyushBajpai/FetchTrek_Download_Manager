use std::pin::Pin;

use async_trait::async_trait;
use fetchtrek_core::{ByteStream, NetworkTransport, ProbeResult, SessionContext};
use fetchtrek_types::{ByteRange, NetworkError};
use futures_util::TryStreamExt;
use reqwest::header::{
    HeaderName, HeaderValue, ACCEPT_RANGES, CONTENT_DISPOSITION, CONTENT_LENGTH, CONTENT_RANGE,
    CONTENT_TYPE, COOKIE, ETAG, RANGE, REFERER, USER_AGENT,
};
use reqwest::{Client, RequestBuilder, Response, StatusCode};
use tokio_util::io::StreamReader;

#[derive(Clone, Debug)]
pub struct HttpTransport {
    client: Client,
}

impl Default for HttpTransport {
    fn default() -> Self {
        Self::new()
    }
}

impl HttpTransport {
    pub fn new() -> Self {
        let client = Client::builder().build().unwrap_or_else(|_| Client::new());
        Self { client }
    }

    pub fn with_client(client: Client) -> Self {
        Self { client }
    }
}

#[async_trait]
impl NetworkTransport for HttpTransport {
    async fn probe(
        &self,
        url: &str,
        context: &SessionContext,
    ) -> Result<ProbeResult, NetworkError> {
        let req = self.client.head(url);
        let req = apply_context(req, context);

        let resp = match req.send().await {
            Ok(r) => r,
            Err(e) => return Err(map_reqwest_error(e)),
        };

        // Fallback to GET bytes=0-0 on 405 Method Not Allowed
        let resp = if resp.status() == StatusCode::METHOD_NOT_ALLOWED {
            let get_req = self.client.get(url).header(RANGE, "bytes=0-0");
            let get_req = apply_context(get_req, context);
            match get_req.send().await {
                Ok(r) => r,
                Err(e) => return Err(map_reqwest_error(e)),
            }
        } else {
            resp
        };

        let status = resp.status();
        if !status.is_success() && status != StatusCode::PARTIAL_CONTENT {
            return Err(map_status_error(status));
        }

        let headers = resp.headers();

        let mut supports_ranges = headers
            .get(ACCEPT_RANGES)
            .and_then(|val| val.to_str().ok())
            .map(|s| s.eq_ignore_ascii_case("bytes"))
            .unwrap_or(false);

        if status == StatusCode::PARTIAL_CONTENT {
            supports_ranges = true;
        }

        let mut total_bytes = headers
            .get(CONTENT_LENGTH)
            .and_then(|val| val.to_str().ok())
            .and_then(|s| s.parse::<u64>().ok());

        // In 206 responses, Content-Range provides the true file length (e.g. bytes 0-0/12345)
        if let Some(range_val) = headers.get(CONTENT_RANGE).and_then(|val| val.to_str().ok()) {
            if let Some(parsed_total) = parse_content_range_total(range_val) {
                total_bytes = Some(parsed_total);
                supports_ranges = true;
            }
        }

        let etag = headers
            .get(ETAG)
            .and_then(|val| val.to_str().ok())
            .map(|s| s.trim_matches('"').to_string());

        let mime_type = headers
            .get(CONTENT_TYPE)
            .and_then(|val| val.to_str().ok())
            .map(|s| s.to_string());

        let suggested_filename = extract_suggested_filename(&resp, url);

        Ok(ProbeResult {
            total_bytes,
            supports_ranges,
            etag,
            mime_type,
            suggested_filename,
        })
    }

    async fn open_range_stream(
        &self,
        url: &str,
        range: ByteRange,
        context: &SessionContext,
    ) -> Result<Pin<Box<dyn ByteStream>>, NetworkError> {
        let range_header = format!("bytes={}-{}", range.start, range.end);
        let req = self.client.get(url).header(RANGE, range_header);
        let req = apply_context(req, context);

        let resp = match req.send().await {
            Ok(r) => r,
            Err(e) => return Err(map_reqwest_error(e)),
        };

        let status = resp.status();
        match status {
            StatusCode::PARTIAL_CONTENT => {}
            StatusCode::OK => {
                // If the server answered 200 OK to a range starting beyond 0,
                // it ignored the range header and serves the entire body.
                if range.start > 0 {
                    return Err(NetworkError::NoRangeSupport);
                }
            }
            StatusCode::RANGE_NOT_SATISFIABLE => return Err(NetworkError::NoRangeSupport),
            StatusCode::TOO_MANY_REQUESTS => return Err(NetworkError::RateLimited),
            StatusCode::FORBIDDEN | StatusCode::UNAUTHORIZED => {
                return Err(NetworkError::AccessDenied)
            }
            s if s.is_server_error() => return Err(NetworkError::GatewayTimeout),
            _ => return Err(map_status_error(status)),
        }

        let byte_stream = resp.bytes_stream().map_err(std::io::Error::other);

        let reader = StreamReader::new(byte_stream);
        Ok(Box::pin(reader))
    }

    async fn open_stream(
        &self,
        url: &str,
        context: &SessionContext,
    ) -> Result<Pin<Box<dyn ByteStream>>, NetworkError> {
        let req = self.client.get(url);
        let req = apply_context(req, context);

        let resp = match req.send().await {
            Ok(r) => r,
            Err(e) => return Err(map_reqwest_error(e)),
        };

        let status = resp.status();
        if !status.is_success() {
            return Err(map_status_error(status));
        }

        let byte_stream = resp.bytes_stream().map_err(std::io::Error::other);
        let reader = StreamReader::new(byte_stream);
        Ok(Box::pin(reader))
    }
}

fn apply_context(mut req: RequestBuilder, context: &SessionContext) -> RequestBuilder {
    if let Some(ua) = &context.user_agent {
        req = req.header(USER_AGENT, ua);
    }
    if let Some(ref_url) = &context.referer {
        req = req.header(REFERER, ref_url);
    }
    if let Some(cookies) = &context.cookies {
        req = req.header(COOKIE, cookies);
    }
    for (k, v) in &context.headers {
        if let (Ok(name), Ok(val)) = (
            HeaderName::from_bytes(k.as_bytes()),
            HeaderValue::from_str(v),
        ) {
            req = req.header(name, val);
        }
    }
    req
}

fn parse_content_range_total(value: &str) -> Option<u64> {
    let slash_idx = value.find('/')?;
    let total_str = value[slash_idx + 1..].trim();
    if total_str == "*" {
        None
    } else {
        total_str.parse::<u64>().ok()
    }
}

fn extract_suggested_filename(resp: &Response, url: &str) -> Option<String> {
    if let Some(disp) = resp.headers().get(CONTENT_DISPOSITION) {
        if let Ok(disp_str) = disp.to_str() {
            if let Some(name) = parse_content_disposition(disp_str) {
                return Some(name);
            }
        }
    }
    extract_filename_from_url(url)
}

fn parse_content_disposition(value: &str) -> Option<String> {
    for part in value.split(';') {
        let trimmed = part.trim();
        if let Some(rest) = trimmed.strip_prefix("filename*=") {
            let val = rest.trim_matches('"');
            if let Some(encoded) = val.split("''").nth(1) {
                if let Ok(decoded) = percent_decode(encoded) {
                    return Some(decoded);
                }
            }
        } else if let Some(rest) = trimmed.strip_prefix("filename=") {
            let val = rest.trim_matches('"').trim();
            if !val.is_empty() {
                return Some(val.to_string());
            }
        }
    }
    None
}

fn extract_filename_from_url(url: &str) -> Option<String> {
    let without_fragment = url.split('#').next().unwrap_or(url);
    let without_query = without_fragment
        .split('?')
        .next()
        .unwrap_or(without_fragment);
    let last_segment = without_query.trim_end_matches('/').rsplit('/').next()?;
    if last_segment.is_empty() {
        return None;
    }
    percent_decode(last_segment).ok().filter(|s| !s.is_empty())
}

fn percent_decode(input: &str) -> Result<String, ()> {
    let mut bytes = Vec::with_capacity(input.len());
    let mut chars = input.bytes();
    while let Some(b) = chars.next() {
        if b == b'%' {
            let h1 = chars.next().ok_or(())?;
            let h2 = chars.next().ok_or(())?;
            let hex_str = [h1, h2];
            let hex_text = std::str::from_utf8(&hex_str).map_err(|_| ())?;
            let val = u8::from_str_radix(hex_text, 16).map_err(|_| ())?;
            bytes.push(val);
        } else {
            bytes.push(b);
        }
    }
    String::from_utf8(bytes).map_err(|_| ())
}

fn map_reqwest_error(err: reqwest::Error) -> NetworkError {
    if err.is_timeout() {
        NetworkError::SocketTimeout
    } else if err.is_connect() {
        NetworkError::ConnectionReset
    } else if let Some(status) = err.status() {
        map_status_error(status)
    } else {
        NetworkError::ConnectionReset
    }
}

fn map_status_error(status: StatusCode) -> NetworkError {
    match status.as_u16() {
        429 => NetworkError::RateLimited,
        416 => NetworkError::NoRangeSupport,
        401 | 403 => NetworkError::AccessDenied,
        408 => NetworkError::SocketTimeout,
        500..=504 => NetworkError::GatewayTimeout,
        _ => NetworkError::ConnectionReset,
    }
}
