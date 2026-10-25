export const NATIVE_HOST_NAME = "com.fetchtrek.downloadmanager";
export const NATIVE_TIMEOUT_MS = 5000;

/**
 * Sends a single-shot native message to the FetchTrek host.
 *
 * @param {object} message - JSON request payload.
 * @returns {Promise<object>} Resolves with the host response.
 */
export function sendToHost(message) {
  return new Promise((resolve, reject) => {
    if (typeof chrome === "undefined" || !chrome.runtime?.sendNativeMessage) {
      reject(new Error("Chrome Native Messaging API unavailable in this context"));
      return;
    }

    let settled = false;
    const timer = setTimeout(() => {
      if (!settled) {
        settled = true;
        reject(new Error("Native messaging request timed out after 5000ms"));
      }
    }, NATIVE_TIMEOUT_MS);

    try {
      chrome.runtime.sendNativeMessage(NATIVE_HOST_NAME, message, (response) => {
        if (settled) return;
        settled = true;
        clearTimeout(timer);

        if (chrome.runtime.lastError) {
          reject(new Error(chrome.runtime.lastError.message || "Failed to communicate with native host"));
          return;
        }

        if (!response) {
          reject(new Error("Native host disconnected without returning a response"));
          return;
        }

        if (response.status === "error") {
          reject(new Error(response.message || "Native host returned an error status"));
          return;
        }

        resolve(response);
      });
    } catch (err) {
      if (!settled) {
        settled = true;
        clearTimeout(timer);
        reject(err);
      }
    }
  });
}

/**
 * Verifies connectivity to the native messaging host.
 *
 * @returns {Promise<{ online: boolean, version?: string, error?: string }>}
 */
export async function checkHostHealth() {
  try {
    const response = await sendToHost({ action: "ping" });
    if (response && response.status === "ok" && response.action === "pong") {
      return { online: true, version: response.version || "0.1.0" };
    }
    return { online: false, error: "Unexpected ping response format" };
  } catch (err) {
    return { online: false, error: err.message || "Host unreachable" };
  }
}
