import { test } from "node:test";
import assert from "node:assert/strict";
import {
  formatCookies,
  filterValidCookies,
  harvestHeaders,
  buildSessionPayload
} from "../session.js";

test("formatCookies correctly formats cookie list into HTTP header string", () => {
  const cookies = [
    { name: "session_id", value: "abc123xyz" },
    { name: "cf_clearance", value: "tok_456_turnstile" },
    { name: "theme", value: "dark" }
  ];

  const formatted = formatCookies(cookies);
  assert.equal(formatted, "session_id=abc123xyz; cf_clearance=tok_456_turnstile; theme=dark");

  assert.equal(formatCookies([]), null);
  assert.equal(formatCookies(null), null);
});

test("filterValidCookies removes expired cookies and retains active/session cookies", () => {
  const nowSec = 1700000000;
  const cookies = [
    { name: "active_cookie", value: "1", expirationDate: nowSec + 3600 },
    { name: "expired_cookie", value: "2", expirationDate: nowSec - 60 },
    { name: "session_cookie", value: "3" } // no expiration date = session
  ];

  const valid = filterValidCookies(cookies, nowSec);
  assert.equal(valid.length, 2);
  assert.equal(valid[0].name, "active_cookie");
  assert.equal(valid[1].name, "session_cookie");
});

test("harvestHeaders extracts active user-agent and respects referrer priority", () => {
  // Case A: downloadItem has explicit referrer
  const itemWithReferer = {
    url: "https://example.com/file.tar.gz",
    referrer: "https://example.com/portal/downloads"
  };
  const tab = {
    url: "https://example.com/dashboard"
  };

  const headersA = harvestHeaders(itemWithReferer, tab);
  assert.equal(headersA["Referer"], "https://example.com/portal/downloads");
  assert.ok(headersA["User-Agent"]);
  assert.equal(headersA["Accept"], "*/*");

  // Case B: downloadItem referrer is blank, falls back to tab.url
  const itemWithoutReferer = {
    url: "https://example.com/file.tar.gz",
    referrer: ""
  };
  const headersB = harvestHeaders(itemWithoutReferer, tab);
  assert.equal(headersB["Referer"], "https://example.com/dashboard");

  // Case C: both blank
  const itemNoRefNoTab = {
    url: "https://example.com/file.tar.gz"
  };
  const headersC = harvestHeaders(itemNoRefNoTab, null);
  assert.equal(headersC["Referer"], undefined);
});

test("buildSessionPayload constructs complete payload dictionary", async () => {
  const item = {
    url: "https://example.com/file.zip",
    finalUrl: "https://example.com/releases/file.zip",
    referrer: "https://releases.example.com/"
  };

  const payload = await buildSessionPayload(item);
  assert.ok(payload.headers);
  assert.equal(payload.headers["Referer"], "https://releases.example.com/");
  assert.ok(payload.user_agent);
  assert.equal(payload.referer, "https://releases.example.com/");
});
