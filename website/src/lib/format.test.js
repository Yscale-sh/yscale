import test from "node:test";
import assert from "node:assert/strict";
import { formatBytes } from "./format.js";

test("formatBytes formats safe integer byte counts without losing the exact value", () => {
  assert.equal(formatBytes(0), "0 B (0 bytes)");
  assert.equal(formatBytes(512), "512 bytes");
  assert.equal(formatBytes(1024), "1 KiB (1,024 bytes)");
  assert.equal(formatBytes(1048575), "1 MiB (1,048,575 bytes)");
  assert.equal(formatBytes(1610612736), "1.5 GiB (1,610,612,736 bytes)");
  assert.equal(formatBytes(2147483648), "2 GiB (2,147,483,648 bytes)");
  assert.equal(formatBytes(1024 ** 6), "not reported");
  assert.equal(formatBytes(null), "not reported");
  assert.equal(formatBytes(-100), "not reported");
  assert.equal(formatBytes(1023.4), "not reported");
});
