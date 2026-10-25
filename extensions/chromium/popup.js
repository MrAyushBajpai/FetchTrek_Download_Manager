import { getSettings, updateSettings } from "./storage.js";
import { checkHostHealth } from "./native_client.js";

const statusDot = document.getElementById("statusDot");
const statusText = document.getElementById("statusText");
const toggleInterception = document.getElementById("toggleInterception");
const minSizeSelect = document.getElementById("minSizeSelect");
const btnCheckHost = document.getElementById("btnCheckHost");

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

async function initializePopup() {
  const settings = await getSettings();

  if (toggleInterception) {
    toggleInterception.checked = settings.enabled !== false;
    toggleInterception.addEventListener("change", async (e) => {
      await updateSettings({ enabled: e.target.checked });
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
}

document.addEventListener("DOMContentLoaded", () => {
  initializePopup();
});
