import { getSettings } from "./storage.js";
import { shouldIntercept, extractDownloadMetadata } from "./interceptor.js";
import { sendToHost, checkHostHealth } from "./native_client.js";
import { buildSessionPayload } from "./session.js";

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
