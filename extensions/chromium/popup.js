import { getSettings, updateSettings } from "./storage.js";
import { checkHostHealth } from "./native_client.js";

const statusDot = document.getElementById("statusDot");
const statusText = document.getElementById("statusText");
const toggleInterception = document.getElementById("toggleInterception");
const toggleFloatingGrabber = document.getElementById("toggleFloatingGrabber");
const minSizeSelect = document.getElementById("minSizeSelect");
const btnCheckHost = document.getElementById("btnCheckHost");
const streamCounter = document.getElementById("streamCounter");
const streamList = document.getElementById("streamList");
const emptyStreamMsg = document.getElementById("emptyStreamMsg");

async function refreshHostStatus() {
  if (!statusDot || !statusText) return;

  statusDot.className = "status-dot checking";
  statusText.textContent = "Checking host...";

  const health = await checkHostHealth();
  if (health.online) {
    statusDot.className = "status-dot online";
    statusText.textContent = `Native Host Connected (${health.version || "v0.1.0"})`;
  } else {
    statusDot.className = "status-dot offline";
    statusText.textContent = "Native Host Offline";
  }
}

async function refreshTabMedia() {
  if (!streamList) return;

  if (typeof chrome !== "undefined" && chrome.runtime?.sendMessage) {
    chrome.runtime.sendMessage({ action: "get_tab_media" }, (response) => {
      const streams = response?.streams || [];
      if (streamCounter) {
        streamCounter.textContent = String(streams.length);
      }

      streamList.innerHTML = "";

      if (streams.length === 0) {
        const empty = document.createElement("div");
        empty.className = "empty-stream-msg";
        empty.textContent = "No media streams detected on active tab";
        streamList.appendChild(empty);
        return;
      }

      for (const stream of streams) {
        const item = document.createElement("div");
        item.className = "popup-stream-item";

        const info = document.createElement("div");
        info.className = "popup-stream-info";

        const pill = document.createElement("span");
        pill.className = `popup-stream-pill ${stream.protocol || "hls"}`;
        pill.textContent = (stream.protocol || "hls").toUpperCase();

        const title = document.createElement("span");
        title.className = "popup-stream-title";
        title.title = stream.streamUrl;
        title.textContent = stream.title || stream.streamUrl;

        info.appendChild(pill);
        info.appendChild(title);

        const btn = document.createElement("button");
        btn.className = "popup-stream-btn";
        btn.textContent = "Download";

        btn.addEventListener("click", () => {
          btn.disabled = true;
          btn.textContent = "...";
          chrome.runtime.sendMessage(
            {
              action: "download_media_stream",
              streamUrl: stream.streamUrl,
              protocol: stream.protocol,
              filename: stream.title
            },
            (dlResponse) => {
              if (dlResponse?.status === "ok") {
                btn.className = "popup-stream-btn success";
                btn.textContent = "✓ Ingesting";
                setTimeout(() => {
                  btn.className = "popup-stream-btn";
                  btn.textContent = "Download";
                  btn.disabled = false;
                }, 2000);
              } else {
                btn.disabled = false;
                btn.textContent = "Failed";
              }
            }
          );
        });

        item.appendChild(info);
        item.appendChild(btn);
        streamList.appendChild(item);
      }
    });
  }
}

async function initializePopup() {
  const settings = await getSettings();

  if (toggleInterception) {
    toggleInterception.checked = settings.enabled !== false;
    toggleInterception.addEventListener("change", async (e) => {
      await updateSettings({ enabled: e.target.checked });
    });
  }

  if (toggleFloatingGrabber) {
    toggleFloatingGrabber.checked = settings.floating_grabber_enabled !== false;
    toggleFloatingGrabber.addEventListener("change", async (e) => {
      await updateSettings({ floating_grabber_enabled: e.target.checked });
    });
  }

  if (minSizeSelect) {
    minSizeSelect.value = String(settings.min_file_size_bytes || 0);
    minSizeSelect.addEventListener("change", async (e) => {
      await updateSettings({ min_file_size_bytes: Number(e.target.value) });
    });
  }

  if (btnCheckHost) {
    btnCheckHost.addEventListener("click", () => {
      refreshHostStatus();
    });
  }

  await refreshHostStatus();
  await refreshTabMedia();
}

document.addEventListener("DOMContentLoaded", () => {
  initializePopup();
});
