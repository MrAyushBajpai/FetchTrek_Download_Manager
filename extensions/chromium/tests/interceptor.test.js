import { test } from "node:test";
import assert from "node:assert/strict";
import { shouldIntercept, extractDownloadMetadata } from "../interceptor.js";
import { DEFAULT_SETTINGS } from "../storage.js";

test("shouldIntercept returns true for standard HTTP/HTTPS URLs", () => {
  const settings = { ...DEFAULT_SETTINGS, enabled: true };
  const item = {
    id: 1,
    url: "https://example.com/releases/archive.zip",
    filename: "archive.zip",
    totalBytes: 50_000_000
  };
  assert.equal(shouldIntercept(item, settings), true);
});

test("shouldIntercept strictly rejects blob: and data: schemes", () => {
  const settings = { ...DEFAULT_SETTINGS, enabled: true };
  const blobItem = {
    id: 2,
    url: "blob:https://example.com/39a67e42-1234-4567-89ab-cdef01234567",
    filename: "export.csv"
  };
  const dataItem = {
    id: 3,
    url: "data:application/pdf;base64,JVBERi0xLjQK...",
    filename: "invoice.pdf"
  };
  assert.equal(shouldIntercept(blobItem, settings), false);
  assert.equal(shouldIntercept(dataItem, settings), false);
});

test("shouldIntercept respects master enabled: false toggle", () => {
  const settings = { ...DEFAULT_SETTINGS, enabled: false };
  const item = {
    id: 4,
    url: "https://example.com/game.iso",
    filename: "game.iso",
    totalBytes: 1_000_000_000
  };
  assert.equal(shouldIntercept(item, settings), false);
});

test("shouldIntercept rejects files below min_file_size_bytes", () => {
  const settings = {
    ...DEFAULT_SETTINGS,
    enabled: true,
    min_file_size_bytes: 10_000_000 // 10 MB
  };
  const smallItem = {
    id: 5,
    url: "https://example.com/small.png",
    filename: "small.png",
    totalBytes: 500_000 // 500 KB < 10 MB
  };
  const largeItem = {
    id: 6,
    url: "https://example.com/large.zip",
    filename: "large.zip",
    totalBytes: 50_000_000 // 50 MB >= 10 MB
  };
  assert.equal(shouldIntercept(smallItem, settings), false);
  assert.equal(shouldIntercept(largeItem, settings), true);
});

test("shouldIntercept rejects blacklisted file extensions", () => {
  const settings = {
    ...DEFAULT_SETTINGS,
    enabled: true,
    blacklist_extensions: [".crx", ".xpi", ".torrent"]
  };
  const crxItem = {
    id: 7,
    url: "https://clients2.google.com/service/update2/crx",
    filename: "extension.crx"
  };
  const torrentItem = {
    id: 8,
    url: "https://tracker.example.com/file.torrent",
    filename: "file.torrent"
  };
  const zipItem = {
    id: 9,
    url: "https://example.com/package.zip",
    filename: "package.zip"
  };
  assert.equal(shouldIntercept(crxItem, settings), false);
  assert.equal(shouldIntercept(torrentItem, settings), false);
  assert.equal(shouldIntercept(zipItem, settings), true);
});

test("shouldIntercept rejects blacklisted domains", () => {
  const settings = {
    ...DEFAULT_SETTINGS,
    enabled: true,
    blacklist_domains: ["internal.corp", "blocked.com"]
  };
  const blockedItem = {
    id: 10,
    url: "https://sub.internal.corp/data.bin",
    filename: "data.bin"
  };
  const allowedItem = {
    id: 11,
    url: "https://cdn.example.org/data.bin",
    filename: "data.bin"
  };
  assert.equal(shouldIntercept(blockedItem, settings), false);
  assert.equal(shouldIntercept(allowedItem, settings), true);
});

test("extractDownloadMetadata correctly extracts filename, url, referrer and size", () => {
  const item = {
    id: 12,
    url: "https://example.com/redirect?id=42",
    finalUrl: "https://example.com/releases/ubuntu-24.04.iso",
    filename: "C:\\Users\\User\\Downloads\\ubuntu-24.04.iso",
    totalBytes: 4_500_000_000,
    referrer: "https://releases.ubuntu.com/"
  };
  const meta = extractDownloadMetadata(item);
  assert.equal(meta.url, "https://example.com/releases/ubuntu-24.04.iso");
  assert.equal(meta.filename, "ubuntu-24.04.iso");
  assert.equal(meta.totalBytes, 4_500_000_000);
  assert.equal(meta.referrer, "https://releases.ubuntu.com/");
});
