/**
 * Evaluates whether an incoming download item should be intercepted by FetchTrek.
 *
 * @param {object} downloadItem - The Chrome download item from chrome.downloads.onCreated.
 * @param {object} settings - The user settings from storage.
 * @returns {boolean} True if the download should be routed to FetchTrek.
 */
export function shouldIntercept(downloadItem, settings) {
  if (!settings || settings.enabled === false) {
    return false;
  }

  if (!downloadItem) {
    return false;
  }

  const rawUrl = downloadItem.url || "";
  const finalUrl = downloadItem.finalUrl || rawUrl;

  // Unfetchable protocol immunity: strictly reject blob: and data:
  if (
    rawUrl.startsWith("blob:") ||
    rawUrl.startsWith("data:") ||
    finalUrl.startsWith("blob:") ||
    finalUrl.startsWith("data:")
  ) {
    return false;
  }

  // Minimum size threshold check (only if totalBytes is known and > 0)
  const minBytes = Number(settings.min_file_size_bytes) || 0;
  if (
    minBytes > 0 &&
    typeof downloadItem.totalBytes === "number" &&
    downloadItem.totalBytes > 0 &&
    downloadItem.totalBytes < minBytes
  ) {
    return false;
  }

  // Domain blacklist evaluation
  let hostname = "";
  try {
    hostname = new URL(finalUrl).hostname.toLowerCase();
  } catch {
    return false;
  }

  const blacklistedDomains = Array.isArray(settings.blacklist_domains)
    ? settings.blacklist_domains
    : [];

  for (const domain of blacklistedDomains) {
    const d = domain.trim().toLowerCase();
    if (d && (hostname === d || hostname.endsWith("." + d))) {
      return false;
    }
  }

  // File extension blacklist evaluation
  const blacklistedExtensions = Array.isArray(settings.blacklist_extensions)
    ? settings.blacklist_extensions
    : [];

  const rawFilename = downloadItem.filename ? downloadItem.filename.toLowerCase() : "";
  const baseFilename = rawFilename.split(/[\\/]/).pop() || "";

  let urlPath = "";
  try {
    urlPath = new URL(finalUrl).pathname.toLowerCase();
  } catch {
    // ignore
  }

  for (const ext of blacklistedExtensions) {
    const e = ext.trim().toLowerCase();
    if (e && (baseFilename.endsWith(e) || urlPath.endsWith(e))) {
      return false;
    }
  }

  return true;
}

/**
 * Extracts and normalizes metadata for native messaging dispatch.
 *
 * @param {object} downloadItem - Chrome download item.
 * @returns {{ url: string, filename: string|null, totalBytes: number|null, referrer: string }}
 */
export function extractDownloadMetadata(downloadItem) {
  if (!downloadItem) {
    return { url: "", filename: null, totalBytes: null, referrer: "" };
  }

  const url = downloadItem.finalUrl || downloadItem.url || "";

  let filename = null;
  if (downloadItem.filename) {
    filename = downloadItem.filename.split(/[\\/]/).pop() || null;
  }

  if (!filename && url) {
    try {
      const pathname = new URL(url).pathname;
      const base = pathname.split("/").pop();
      if (base) {
        filename = decodeURIComponent(base);
      }
    } catch {
      // ignore URL parsing error
    }
  }

  const totalBytes =
    typeof downloadItem.totalBytes === "number" && downloadItem.totalBytes > 0
      ? downloadItem.totalBytes
      : null;

  const referrer = downloadItem.referrer || "";

  return {
    url,
    filename,
    totalBytes,
    referrer
  };
}
