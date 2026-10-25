export const DEFAULT_SETTINGS = Object.freeze({
  enabled: true,
  floating_grabber_enabled: true,
  min_file_size_bytes: 0,
  blacklist_extensions: [".crx", ".xpi", ".torrent"],
  blacklist_domains: []
});

/**
 * Retrieves the current extension settings, merging with defaults.
 * @returns {Promise<typeof DEFAULT_SETTINGS>}
 */
export async function getSettings() {
  if (typeof chrome !== "undefined" && chrome.storage?.local) {
    return new Promise((resolve) => {
      chrome.storage.local.get(DEFAULT_SETTINGS, (items) => {
        resolve({ ...DEFAULT_SETTINGS, ...items });
      });
    });
  }
  return { ...DEFAULT_SETTINGS };
}

/**
 * Updates stored settings with the provided partial values.
 * @param {Partial<typeof DEFAULT_SETTINGS>} partial
 * @returns {Promise<void>}
 */
export async function updateSettings(partial) {
  if (typeof chrome !== "undefined" && chrome.storage?.local) {
    return new Promise((resolve) => {
      chrome.storage.local.set(partial, () => {
        resolve();
      });
    });
  }
}

/**
 * Registers a callback invoked whenever chrome.storage values change.
 * @param {(changes: Record<string, { oldValue: unknown, newValue: unknown }>) => void} callback
 */
export function onSettingsChanged(callback) {
  if (typeof chrome !== "undefined" && chrome.storage?.onChanged) {
    chrome.storage.onChanged.addListener((changes, areaName) => {
      if (areaName === "local") {
        callback(changes);
      }
    });
  }
}
