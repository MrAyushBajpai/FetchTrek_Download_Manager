/**
 * WebExtension Media Stream Sniffer
 * Detects HLS (.m3u8) and MPEG-DASH (.mpd) playlists passively and scopes discovered streams to tabs.
 */

export const MEDIA_SIGNATURES = Object.freeze({
  hlsExtensions: [".m3u8"],
  dashExtensions: [".mpd"],
  hlsMimeTypes: [
    "application/vnd.apple.mpegurl",
    "application/x-mpegurl",
    "audio/x-mpegurl",
    "video/mp2t"
  ],
  dashMimeTypes: [
    "application/dash+xml",
    "video/vnd.mpeg.dash.mpd"
  ]
});

/**
 * Classifies whether a URL or response MIME type represents a media stream manifest.
 *
 * @param {string} url Request URL.
 * @param {string|null} [mimeType] HTTP response Content-Type header.
 * @returns {{ isMedia: boolean, protocol: 'hls'|'dash'|null, mimeType: string|null }}
 */
export function classifyStream(url, mimeType = null) {
  if (!url || typeof url !== "string") {
    return { isMedia: false, protocol: null, mimeType: null };
  }

  const urlLower = url.toLowerCase();
  const urlPath = urlLower.split("?")[0].split("#")[0];
  const mimeLower = (mimeType || "").toLowerCase().trim();

  // Check HLS URL pattern
  for (const ext of MEDIA_SIGNATURES.hlsExtensions) {
    if (urlPath.endsWith(ext) || urlLower.includes(ext + "?") || urlLower.includes(ext + "#")) {
      return { isMedia: true, protocol: "hls", mimeType: mimeType || "application/vnd.apple.mpegurl" };
    }
  }

  // Check DASH URL pattern
  for (const ext of MEDIA_SIGNATURES.dashExtensions) {
    if (urlPath.endsWith(ext) || urlLower.includes(ext + "?") || urlLower.includes(ext + "#")) {
      return { isMedia: true, protocol: "dash", mimeType: mimeType || "application/dash+xml" };
    }
  }

  // Check HLS MIME types
  for (const mime of MEDIA_SIGNATURES.hlsMimeTypes) {
    if (mimeLower.includes(mime)) {
      return { isMedia: true, protocol: "hls", mimeType };
    }
  }

  // Check DASH MIME types
  for (const mime of MEDIA_SIGNATURES.dashMimeTypes) {
    if (mimeLower.includes(mime)) {
      return { isMedia: true, protocol: "dash", mimeType };
    }
  }

  return { isMedia: false, protocol: null, mimeType: null };
}

/**
 * Extracts a canonical, normalized stream URL (preserving query params for auth tokens).
 *
 * @param {string} rawUrl
 * @returns {string}
 */
export function extractCanonicalUrl(rawUrl) {
  if (!rawUrl || typeof rawUrl !== "string") return "";
  try {
    const parsed = new URL(rawUrl);
    parsed.hash = ""; // Strip fragment identifier
    return parsed.toString();
  } catch {
    return rawUrl.split("#")[0];
  }
}

/**
 * Generates a human-readable stream title or label from its URL.
 *
 * @param {string} streamUrl
 * @returns {string}
 */
export function extractLabelFromUrl(streamUrl) {
  try {
    const parsed = new URL(streamUrl);
    const pathname = parsed.pathname;
    const parts = pathname.split("/").filter(Boolean);
    const lastPart = parts[parts.length - 1] || parsed.hostname;
    return decodeURIComponent(lastPart);
  } catch {
    return streamUrl;
  }
}

/**
 * Tab-scoped in-memory registry of detected media streams.
 */
export class MediaRegistry {
  constructor() {
    /** @type {Map<number, Map<string, object>>} */
    this.tabStreams = new Map();
  }

  /**
   * Adds a detected stream to a tab's stream registry.
   * Deduplicates by canonical manifest URL.
   *
   * @param {number} tabId Originating browser tab ID.
   * @param {{ url: string, mimeType?: string|null, title?: string|null, timestamp?: number }} details
   * @returns {boolean} True if a new stream was added; false if already known or not a stream.
   */
  addStream(tabId, details) {
    if (typeof tabId !== "number" || tabId < 0 || !details?.url) {
      return false;
    }

    const canonicalUrl = extractCanonicalUrl(details.url);
    const classification = classifyStream(canonicalUrl, details.mimeType);

    if (!classification.isMedia) {
      return false;
    }

    if (!this.tabStreams.has(tabId)) {
      this.tabStreams.set(tabId, new Map());
    }

    const tabMap = this.tabStreams.get(tabId);
    if (tabMap.has(canonicalUrl)) {
      return false; // Already registered for this tab
    }

    const streamRecord = {
      streamUrl: canonicalUrl,
      protocol: classification.protocol,
      mimeType: details.mimeType || classification.mimeType || "",
      title: details.title || extractLabelFromUrl(canonicalUrl),
      timestamp: details.timestamp || Date.now()
    };

    tabMap.set(canonicalUrl, streamRecord);
    return true;
  }

  /**
   * Retrieves all detected media streams for a specific tab.
   *
   * @param {number} tabId
   * @returns {Array<object>} List of detected stream objects, sorted newest first.
   */
  getStreams(tabId) {
    const tabMap = this.tabStreams.get(tabId);
    if (!tabMap || tabMap.size === 0) {
      return [];
    }
    return Array.from(tabMap.values()).sort((a, b) => b.timestamp - a.timestamp);
  }

  /**
   * Purges all recorded streams for a tab (e.g. upon tab close or top-level navigation).
   *
   * @param {number} tabId
   */
  clearTab(tabId) {
    this.tabStreams.delete(tabId);
  }

  /**
   * Returns the count of streams detected on a tab.
   *
   * @param {number} tabId
   * @returns {number}
   */
  getStreamCount(tabId) {
    return this.tabStreams.get(tabId)?.size || 0;
  }
}

/**
 * Updates extension action badge text and background for a tab.
 *
 * @param {number} tabId
 * @param {number} count
 */
export function updateBadge(tabId, count) {
  if (typeof chrome === "undefined" || !chrome.action?.setBadgeText) {
    return;
  }

  if (count > 0) {
    chrome.action.setBadgeText({ tabId, text: String(count) });
    if (chrome.action.setBadgeBackgroundColor) {
      chrome.action.setBadgeBackgroundColor({ tabId, color: "#2563EB" });
    }
  } else {
    chrome.action.setBadgeText({ tabId, text: "" });
  }
}

/**
 * Builds the native messaging payload for initiating a media stream download.
 *
 * @param {object} stream Stream descriptor from MediaRegistry.
 * @param {object} session Session context object (headers, cookies, user_agent, referer).
 * @returns {object} Payload ready for sendToHost.
 */
export function buildMediaDownloadPayload(stream, session) {
  return {
    action: "download_media_stream",
    url: stream.streamUrl,
    protocol: stream.protocol,
    filename: stream.title || null,
    destination_path: null,
    headers: session?.headers || [],
    cookies: session?.cookies || null,
    user_agent: session?.user_agent || null,
    referer: session?.referer || null,
    connections: 4
  };
}
