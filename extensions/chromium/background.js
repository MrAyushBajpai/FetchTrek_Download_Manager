import { getSettings } from "./storage.js";
import { shouldIntercept, extractDownloadMetadata } from "./interceptor.js";
import { sendToHost, checkHostHealth } from "./native_client.js";
import { buildSessionPayload, harvestCookies, harvestHeaders } from "./session.js";
import { MediaRegistry, updateBadge, buildMediaDownloadPayload } from "./media_sniffer.js";

const mediaRegistry = new MediaRegistry();

function setBadge(text, color) {
  if (typeof chrome !== "undefined" && chrome.action?.setBadgeText) {
    chrome.action.setBadgeText({ text });
    if (color && chrome.action.setBadgeBackgroundColor) {
      chrome.action.setBadgeBackgroundColor({ color });
    }
  }
}

function notifyUser(title, message) {
  if (typeof chrome !== "undefined" && chrome.notifications?.create) {
    chrome.notifications.create({
      type: "basic",
      iconUrl: "icons/icon48.png",
      title,
      message
    });
  }
}

// Initial host check on startup/install
async function updateHostStatus() {
  const health = await checkHostHealth();
  if (health.online) {
    setBadge("", "#10B981");
  } else {
    setBadge("!", "#EF4444");
  }
}

if (typeof chrome !== "undefined" && chrome.runtime?.onInstalled) {
  chrome.runtime.onInstalled.addListener(() => {
    updateHostStatus();
  });
}

if (typeof chrome !== "undefined" && chrome.runtime?.onStartup) {
  chrome.runtime.onStartup.addListener(() => {
    updateHostStatus();
  });
}

// Passive Media Stream Sniffing via WebRequest
if (typeof chrome !== "undefined" && chrome.webRequest?.onHeadersReceived) {
  chrome.webRequest.onHeadersReceived.addListener(
    (details) => {
      if (!details || details.tabId < 0) return;

      const contentTypeHeader = details.responseHeaders?.find(
        (h) => h.name.toLowerCase() === "content-type"
      )?.value;

      const isNew = mediaRegistry.addStream(details.tabId, {
        url: details.url,
        mimeType: contentTypeHeader
      });

      if (isNew) {
        const streams = mediaRegistry.getStreams(details.tabId);
        updateBadge(details.tabId, streams.length);

        // Forward media_detected to content script in the tab
        chrome.tabs.sendMessage(details.tabId, {
          action: "media_detected",
          streams
        }).catch(() => {
          // Content script may not be attached or page is restricted
        });
      }
    },
    { urls: ["<all_urls>"], types: ["xmlhttprequest", "media", "other"] },
    ["responseHeaders"]
  );
}

// Scoped Tab Lifecycle Cleanup
if (typeof chrome !== "undefined" && chrome.tabs?.onRemoved) {
  chrome.tabs.onRemoved.addListener((tabId) => {
    mediaRegistry.clearTab(tabId);
  });
}

if (typeof chrome !== "undefined" && chrome.tabs?.onUpdated) {
  chrome.tabs.onUpdated.addListener((tabId, changeInfo) => {
    if (changeInfo.status === "loading" && changeInfo.url) {
      mediaRegistry.clearTab(tabId);
      updateBadge(tabId, 0);
    }
  });
}

// Message Dispatcher for Content Scripts and Popup
if (typeof chrome !== "undefined" && chrome.runtime?.onMessage) {
  chrome.runtime.onMessage.addListener((message, sender, sendResponse) => {
    if (!message || typeof message.action !== "string") {
      return false;
    }

    if (message.action === "get_tab_media") {
      const tabId = typeof message.tabId === "number" ? message.tabId : sender.tab?.id;
      if (typeof tabId === "number") {
        sendResponse({ status: "ok", streams: mediaRegistry.getStreams(tabId) });
      } else {
        // Fallback: resolve active tab
        chrome.tabs.query({ active: true, currentWindow: true }, (tabs) => {
          const activeTabId = tabs[0]?.id;
          const streams = typeof activeTabId === "number" ? mediaRegistry.getStreams(activeTabId) : [];
          sendResponse({ status: "ok", streams });
        });
        return true; // asynchronous response
      }
      return false;
    }

    if (message.action === "download_media_stream") {
      (async () => {
        try {
          const streamUrl = message.streamUrl;
          const cookies = await harvestCookies(streamUrl);
          const tabId = typeof message.tabId === "number" ? message.tabId : sender.tab?.id;
          const referer = sender.tab?.url || message.referer || null;
          const headers = harvestHeaders(tabId, referer);

          const payload = buildMediaDownloadPayload(
            {
              streamUrl,
              protocol: message.protocol,
              title: message.filename
            },
            {
              cookies,
              headers: headers.headers,
              user_agent: headers.user_agent,
              referer: headers.referer
            }
          );

          const response = await sendToHost(payload);
          if (response && response.status === "ok") {
            notifyUser("FetchTrek Ingestion", `Stream queued for download: ${message.filename || streamUrl}`);
            sendResponse({ status: "ok", response });
          } else {
            const err = response?.message || "Failed to communicate with native host";
            notifyUser("FetchTrek Error", err);
            sendResponse({ status: "error", message: err });
          }
        } catch (err) {
          notifyUser("FetchTrek Error", err.message || "Failed to route media stream to host");
          sendResponse({ status: "error", message: err.message });
        }
      })();
      return true; // asynchronous response
    }

    return false;
  });
}

// Download interception listener
if (typeof chrome !== "undefined" && chrome.downloads?.onCreated) {
  chrome.downloads.onCreated.addListener(async (item) => {
    try {
      const settings = await getSettings();

      if (!shouldIntercept(item, settings)) {
        return;
      }

      // Race-free cancellation: Cancel and erase immediately to suppress browser shelf
      try {
        await chrome.downloads.cancel(item.id);
        await chrome.downloads.erase({ id: item.id });
      } catch (cancelErr) {
        console.warn("[FetchTrek] Non-fatal cancel/erase error:", cancelErr);
      }

      const meta = extractDownloadMetadata(item);
      const session = await buildSessionPayload(item);

      const payload = {
        action: "intercept_download",
        url: meta.url,
        filename: meta.filename,
        destination_path: null,
        headers: session.headers,
        cookies: session.cookies,
        user_agent: session.user_agent,
        referer: session.referer,
        connections: 4
      };

      const response = await sendToHost(payload);
      if (response && response.status === "ok") {
        console.log("[FetchTrek] Intercepted download routed to host:", meta.filename || meta.url);
        setBadge("✓", "#10B981");
        setTimeout(() => setBadge("", "#10B981"), 3000);
      } else {
        const errorMsg = response?.message || "Unknown error from native host";
        console.error("[FetchTrek] Native host rejected download:", errorMsg);
        setBadge("!", "#EF4444");
        notifyUser("FetchTrek Error", errorMsg);
      }
    } catch (err) {
      console.error("[FetchTrek] Interception failure:", err);
      setBadge("!", "#EF4444");
      notifyUser("FetchTrek Offline", err.message || "Failed to communicate with FetchTrek desktop host.");
    }
  });
}
