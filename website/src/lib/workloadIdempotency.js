// A workload submit is a paid operation, so the key that makes it repeatable
// belongs to the form the human filled in — not to the fetch that carries it.
// A double click, a page-level retry, or a proxy timeout must all arrive at
// central under one key; only changed YAML or a freshly mounted form earns a
// new one.

export const IDEMPOTENCY_HEADER = "Idempotency-Key";

// Central's contract is 8..255 printable non-space ASCII. Sixteen bytes of
// CSPRNG output rendered as hex sits well inside that and carries nothing about
// the tenant, the account, or the form.
const KEY_BYTES = 16;
const KEY_PREFIX = "wl_";

export function newIdempotencyKey() {
  const bytes = new Uint8Array(KEY_BYTES);
  globalThis.crypto.getRandomValues(bytes);
  let hex = "";
  for (const byte of bytes) hex += byte.toString(16).padStart(2, "0");
  return KEY_PREFIX + hex;
}

export function isIdempotencyKey(value) {
  if (typeof value !== "string" || value.length < 8 || value.length > 255) return false;
  for (let i = 0; i < value.length; i += 1) {
    const code = value.charCodeAt(i);
    if (code < 0x21 || code > 0x7e) return false;
  }
  return true;
}

// One holder per mounted form. keyFor answers the same key for as long as the
// submission is the same one, and mints a new one the moment it is not — going
// back, editing, and resubmitting is a genuinely different run, while retrying
// the same review screen is not.
//
// A submission is the YAML, where it was sent, *and* what it was composed from.
// Neither the target cluster nor the template selection appears in the YAML —
// both ride as headers — so keying on the document alone would let a reader
// change destination or catalog entry and have central replay the original run
// under the original receipt. JSON encoding keeps the scope collision-safe
// without putting control bytes in this source file.
export function createIdempotencyKeys() {
  let current = "";
  let forScope = null;
  return {
    keyFor(yaml, clusterId = "", template = "") {
      const scope = JSON.stringify([clusterId, template, yaml]);
      if (!current || scope !== forScope) {
        forScope = scope;
        current = newIdempotencyKey();
      }
      return current;
    },
  };
}
