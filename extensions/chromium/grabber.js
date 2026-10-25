/**
 * FetchTrek In-Page Floating Video Grabber
 * Injected content script rendering an isolated, unobtrusive floating button
 * to capture and download detected HLS/DASH media streams.
 */

(() => {
  let detectedStreams = [];
  let isMenuOpen = false;
  let isEnabled = true;
  let rootContainer = null;

  // Retrieve user preferences
  if (typeof chrome !== "undefined" && chrome.storage?.local) {
    chrome.storage.local.get({ floating_grabber_enabled: true }, (items) => {
      isEnabled = items.floating_grabber_enabled !== false;
      renderGrabber();
    });

    chrome.storage.onChanged?.addListener((changes, area) => {
      if (area === "local" && changes.floating_grabber_enabled) {
        isEnabled = changes.floating_grabber_enabled.newValue !== false;
        renderGrabber();
      }
    });
  }

  function ensureRootContainer() {
    if (!rootContainer) {
      rootContainer = document.getElementById("fetchtrek-grabber-root");
      if (!rootContainer) {
        rootContainer = document.createElement("div");
        rootContainer.id = "fetchtrek-grabber-root";
        document.body.appendChild(rootContainer);
      }
    }
    return rootContainer;
  }

  function renderGrabber() {
    if (!isEnabled || detectedStreams.length === 0) {
      if (rootContainer) {
        rootContainer.style.display = "none";
      }
      return;
    }

    const root = ensureRootContainer();
    root.style.display = "block";
    root.innerHTML = "";

    // Floating Pill Button
    const pill = document.createElement("div");
    pill.className = "fetchtrek-grabber-pill";
    pill.setAttribute("role", "button");
    pill.setAttribute("aria-label", "FetchTrek Media Grabber");

    const icon = document.createElement("span");
    icon.className = "fetchtrek-grabber-icon";
    icon.textContent = "⚡";

    const label = document.createElement("span");
    label.textContent = "FetchTrek";

    const badge = document.createElement("span");
    badge.className = "fetchtrek-grabber-badge";
    badge.textContent = String(detectedStreams.length);

    pill.appendChild(icon);
    pill.appendChild(label);
    pill.appendChild(badge);

    // Dropdown Stream Menu
    const menu = document.createElement("div");
    menu.className = `fetchtrek-grabber-menu ${isMenuOpen ? "visible" : ""}`;

    const header = document.createElement("div");
    header.className = "fetchtrek-grabber-header";

    const title = document.createElement("span");
    title.className = "fetchtrek-grabber-title";
    title.textContent = `Streams Found (${detectedStreams.length})`;

    header.appendChild(title);
    menu.appendChild(header);

    const list = document.createElement("div");
    list.className = "fetchtrek-grabber-list";

    for (const stream of detectedStreams) {
      const card = document.createElement("div");
      card.className = "fetchtrek-stream-card";

      const streamHeader = document.createElement("div");
      streamHeader.className = "fetchtrek-stream-header";

      const protocolPill = document.createElement("span");
      protocolPill.className = `fetchtrek-protocol-pill ${stream.protocol || "hls"}`;
      protocolPill.textContent = (stream.protocol || "hls").toUpperCase();

      const name = document.createElement("span");
      name.className = "fetchtrek-stream-name";
      name.title = stream.streamUrl;
      name.textContent = stream.title || stream.streamUrl;

      streamHeader.appendChild(protocolPill);
      streamHeader.appendChild(name);

      const downloadBtn = document.createElement("button");
      downloadBtn.className = "fetchtrek-download-btn";
      downloadBtn.textContent = "Download with FetchTrek";

      downloadBtn.addEventListener("click", (e) => {
        e.stopPropagation();
        downloadBtn.disabled = true;
        downloadBtn.textContent = "Sending to Host...";

        if (typeof chrome !== "undefined" && chrome.runtime?.sendMessage) {
          chrome.runtime.sendMessage(
            {
              action: "download_media_stream",
              streamUrl: stream.streamUrl,
              protocol: stream.protocol,
              filename: stream.title
            },
            (response) => {
              if (response && response.status === "ok") {
                downloadBtn.className = "fetchtrek-download-btn success";
                downloadBtn.textContent = "✓ Ingesting...";
                setTimeout(() => {
                  downloadBtn.className = "fetchtrek-download-btn";
                  downloadBtn.textContent = "Download with FetchTrek";
                  downloadBtn.disabled = false;
                }, 2500);
              } else {
                downloadBtn.disabled = false;
                downloadBtn.textContent = "Error: Check Host";
              }
            }
          );
        }
      });

      card.appendChild(streamHeader);
      card.appendChild(downloadBtn);
      list.appendChild(card);
    }

    menu.appendChild(list);

    pill.addEventListener("click", (e) => {
      e.stopPropagation();
      isMenuOpen = !isMenuOpen;
      menu.classList.toggle("visible", isMenuOpen);
    });

    root.appendChild(pill);
    root.appendChild(menu);
  }

  // Close menu on click outside
  document.addEventListener("click", (e) => {
    if (isMenuOpen && rootContainer && !rootContainer.contains(e.target)) {
      isMenuOpen = false;
      const menu = rootContainer.querySelector(".fetchtrek-grabber-menu");
      if (menu) {
        menu.classList.remove("visible");
      }
    }
  });

  // Listen for background notifications
  if (typeof chrome !== "undefined" && chrome.runtime?.onMessage) {
    chrome.runtime.onMessage.addListener((message) => {
      if (message.action === "media_detected" && Array.isArray(message.streams)) {
        detectedStreams = message.streams;
        renderGrabber();
      }
    });

    // Request streams for active tab on script startup
    chrome.runtime.sendMessage({ action: "get_tab_media" }, (response) => {
      if (response && Array.isArray(response.streams)) {
        detectedStreams = response.streams;
        renderGrabber();
      }
    });
  }
})();
