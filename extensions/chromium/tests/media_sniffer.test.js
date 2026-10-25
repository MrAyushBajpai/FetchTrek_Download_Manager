import { test } from "node:test";
import assert from "node:assert/strict";
import {
  classifyStream,
  extractCanonicalUrl,
  MediaRegistry,
  buildMediaDownloadPayload
} from "../media_sniffer.js";

test("test_hls_url_pattern_detection: correctly detects .m3u8 URLs and tags as HLS", () => {
  const hlsUrls = [
    "https://cdn.example.com/hls/master.m3u8",
    "https://cdn.example.com/hls/live/stream.m3u8?token=secret123",
    "https://cdn.example.com/video/playlist.m3u8#track1",
    "https://media.server.org/streams/index.M3U8"
  ];

  for (const url of hlsUrls) {
    const classification = classifyStream(url);
    assert.equal(classification.isMedia, true, `Failed to classify: ${url}`);
    assert.equal(classification.protocol, "hls", `Expected HLS protocol for: ${url}`);
  }

  const nonMediaUrls = [
    "https://example.com/index.html",
    "https://example.com/api/v1/user.json",
    "https://example.com/image.png"
  ];

  for (const url of nonMediaUrls) {
    const classification = classifyStream(url);
    assert.equal(classification.isMedia, false, `Incorrectly classified as media: ${url}`);
    assert.equal(classification.protocol, null);
  }
});

test("test_dash_url_pattern_detection: correctly detects .mpd URLs and tags as DASH", () => {
  const dashUrls = [
    "https://cdn.example.com/dash/manifest.mpd",
    "https://cdn.example.com/dash/vod/stream.mpd?auth=tok456",
    "https://streaming.example.org/dash/video.MPD#rep1"
  ];

  for (const url of dashUrls) {
    const classification = classifyStream(url);
    assert.equal(classification.isMedia, true, `Failed to classify: ${url}`);
    assert.equal(classification.protocol, "dash", `Expected DASH protocol for: ${url}`);
  }
});

test("test_mime_type_detection: detects HLS and DASH MIME types without URL extensions", () => {
  // Query parameter URLs without file extensions
  const ambiguousUrl = "https://streaming.service.com/manifest?stream_id=98765";

  // HLS MIME types
  const hlsMimes = [
    "application/vnd.apple.mpegurl",
    "application/x-mpegurl",
    "application/vnd.apple.mpegurl; charset=utf-8",
    "video/mp2t"
  ];

  for (const mime of hlsMimes) {
    const classification = classifyStream(ambiguousUrl, mime);
    assert.equal(classification.isMedia, true, `MIME failed: ${mime}`);
    assert.equal(classification.protocol, "hls", `Expected HLS for MIME: ${mime}`);
  }

  // DASH MIME types
  const dashMimes = [
    "application/dash+xml",
    "application/dash+xml; charset=utf-8",
    "video/vnd.mpeg.dash.mpd"
  ];

  for (const mime of dashMimes) {
    const classification = classifyStream(ambiguousUrl, mime);
    assert.equal(classification.isMedia, true, `MIME failed: ${mime}`);
    assert.equal(classification.protocol, "dash", `Expected DASH for MIME: ${mime}`);
  }

  // Standard non-media MIME
  const nonMedia = classifyStream(ambiguousUrl, "application/json");
  assert.equal(nonMedia.isMedia, false);
});

test("test_stream_deduplication: multiple requests for the same manifest produce exactly 1 stream record", () => {
  const registry = new MediaRegistry();
  const tabId = 101;
  const manifestUrl = "https://cdn.example.com/video/master.m3u8?token=xyz123";

  // First request: should be newly registered
  const first = registry.addStream(tabId, {
    url: manifestUrl,
    mimeType: "application/vnd.apple.mpegurl",
    title: "Big Buck Bunny"
  });
  assert.equal(first, true);
  assert.equal(registry.getStreams(tabId).length, 1);

  // Subsequent requests for the same manifest (e.g. player polling or chunk segment requests)
  for (let i = 0; i < 5; i++) {
    const duplicate = registry.addStream(tabId, {
      url: manifestUrl,
      mimeType: "application/vnd.apple.mpegurl"
    });
    assert.equal(duplicate, false, `Duplicate ${i} was not deduplicated`);
  }

  // Same URL with hash fragment: should still deduplicate to canonical URL
  const withHash = registry.addStream(tabId, {
    url: manifestUrl + "#frag",
    mimeType: "application/vnd.apple.mpegurl"
  });
  assert.equal(withHash, false);

  assert.equal(registry.getStreams(tabId).length, 1);
  assert.equal(registry.getStreamCount(tabId), 1);
});

test("test_tab_scoping_and_cleanup: streams are isolated per tab and cleaned up on tab close", () => {
  const registry = new MediaRegistry();
  const tab1 = 1;
  const tab2 = 2;

  registry.addStream(tab1, { url: "https://siteA.com/stream1.m3u8" });
  registry.addStream(tab1, { url: "https://siteA.com/stream2.m3u8" });
  registry.addStream(tab2, { url: "https://siteB.com/movie.mpd" });

  assert.equal(registry.getStreams(tab1).length, 2);
  assert.equal(registry.getStreams(tab2).length, 1);

  // Clean up Tab 1
  registry.clearTab(tab1);

  assert.equal(registry.getStreams(tab1).length, 0);
  assert.equal(registry.getStreamCount(tab1), 0);

  // Tab 2 streams remain intact
  assert.equal(registry.getStreams(tab2).length, 1);
  assert.equal(registry.getStreams(tab2)[0].protocol, "dash");
});

test("test_media_download_payload_generation: validates session context, headers, and cookies are bundled", () => {
  const stream = {
    streamUrl: "https://vod.example.com/ep1/master.m3u8?token=abc",
    protocol: "hls",
    title: "Series Episode 1"
  };

  const session = {
    cookies: "session_id=s98765; auth=xyz",
    headers: [
      ["Authorization", "Bearer tok_123"],
      ["X-Custom-Client", "FetchTrek"]
    ],
    user_agent: "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    referer: "https://player.example.com/watch/ep1"
  };

  const payload = buildMediaDownloadPayload(stream, session);

  assert.equal(payload.action, "download_media_stream");
  assert.equal(payload.url, stream.streamUrl);
  assert.equal(payload.protocol, "hls");
  assert.equal(payload.filename, "Series Episode 1");
  assert.equal(payload.cookies, "session_id=s98765; auth=xyz");
  assert.equal(payload.user_agent, "Mozilla/5.0 (Windows NT 10.0; Win64; x64)");
  assert.equal(payload.referer, "https://player.example.com/watch/ep1");
  assert.equal(payload.headers.length, 2);
  assert.equal(payload.headers[0][0], "Authorization");
  assert.equal(payload.connections, 4);
});
