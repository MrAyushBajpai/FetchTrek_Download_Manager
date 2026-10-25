/**
 * Formats an array of cookie objects into an HTTP Cookie header string.
 *
 * @param {Array<{ name: string, value: string }>} cookieList
 * @returns {string|null} Formatted cookie header string or null if empty.
 */
export function formatCookies(cookieList) {
  if (!Array.isArray(cookieList) || cookieList.length === 0) {
    return null;
  }
  const pairs = cookieList
    .filter((c) => c && typeof c.name === "string" && typeof c.value === "string")
    .map((c) => `${c.name}=${c.value}`);
  return pairs.length > 0 ? pairs.join("; ") : null;
}

/**
 * Filters out expired cookies from an array of cookies.
 *
 * @param {Array<{ expirationDate?: number }>} cookieList
 * @param {number} [nowSec] Current unix timestamp in seconds.
 * @returns {Array<object>} Valid unexpired cookies.
 */
export function filterValidCookies(cookieList, nowSec = Date.now() / 1000) {
  if (!Array.isArray(cookieList)) {
    return [];
  }
  return cookieList.filter((cookie) => {
    if (!cookie) return false;
    if (typeof cookie.expirationDate === "number" && cookie.expirationDate > 0) {
      return cookie.expirationDate >= nowSec;
    }
    return true; // Session cookie without expiration
  });
}

/**
 * Scopes and retrieves valid cookies for a target download URL.
 *
 * @param {string} targetUrl Target download URL.
 * @returns {Promise<string|null>} Formatted cookie header string or null.
 */
export async function harvestCookies(targetUrl) {
  if (typeof chrome === "undefined" || !chrome.cookies?.getAll || !targetUrl) {
    return null;
  }

  return new Promise((resolve) => {
    try {
      chrome.cookies.getAll({ url: targetUrl }, (cookies) => {
        if (chrome.runtime?.lastError || !cookies) {
          resolve(null);
          return;
        }
        const valid = filterValidCookies(cookies);
        resolve(formatCookies(valid));
      });
    } catch {
      resolve(null);
    }
  });
}

/**
 * Builds HTTP headers dictionary matching browser environment and session state.
 *
 * @param {object} downloadItem Chrome download item.
 * @param {object} [tab] Initiating browser tab object if available.
 * @returns {Record<string, string>} Key-value map of HTTP headers.
 */
export function harvestHeaders(downloadItem, tab) {
  const headers = {};

  // User-Agent: match browser exactly
  const userAgent =
    typeof navigator !== "undefined" && navigator.userAgent
      ? navigator.userAgent
      : "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36";
  headers["User-Agent"] = userAgent;

  // Referer: check downloadItem first, fall back to tab URL
  const referer =
    (downloadItem && downloadItem.referrer && downloadItem.referrer.trim()) ||
    (tab && tab.url && tab.url.trim()) ||
    null;

  if (referer && !referer.startsWith("blob:") && !referer.startsWith("data:")) {
    headers["Referer"] = referer;
  }

  // Accept and Accept-Language headers
  headers["Accept"] = "*/*";
  const language =
    typeof navigator !== "undefined" && navigator.language
      ? navigator.language
      : "en-US,en;q=0.9";
  headers["Accept-Language"] = language;

  return headers;
}

/**
 * Queries the initiating tab if tabId is provided.
 *
 * @param {number} tabId
 * @returns {Promise<object|null>}
 */
async function queryInitiatingTab(tabId) {
  if (typeof chrome === "undefined" || !chrome.tabs?.get || typeof tabId !== "number" || tabId < 0) {
    return null;
  }

  return new Promise((resolve) => {
    try {
      chrome.tabs.get(tabId, (tab) => {
        if (chrome.runtime?.lastError || !tab) {
          resolve(null);
        } else {
          resolve(tab);
        }
      });
    } catch {
      resolve(null);
    }
  });
}

/**
 * Extracts full session context for an intercepted download.
 *
 * @param {object} downloadItem Chrome download item.
 * @returns {Promise<{
 *   headers: Record<string, string>,
 *   cookies: string|null,
 *   user_agent: string|null,
 *   referer: string|null
 * }>}
 */
export async function buildSessionPayload(downloadItem) {
  const targetUrl = (downloadItem && (downloadItem.finalUrl || downloadItem.url)) || "";
  const tab = await queryInitiatingTab(downloadItem?.tabId);

  const [cookies, headers] = await Promise.all([
    harvestCookies(targetUrl),
    Promise.resolve(harvestHeaders(downloadItem, tab))
  ]);

  return {
    headers,
    cookies,
    user_agent: headers["User-Agent"] || null,
    referer: headers["Referer"] || null
  };
}
