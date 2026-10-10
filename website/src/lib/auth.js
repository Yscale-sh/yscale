// Browser half of the Yscale ID sign-in. Authorization Code + PKCE (S256):
// the SPA never holds a client secret, and the code exchange itself goes
// through our own /api/auth/token because the ID token endpoint is
// cluster-internal.
//
// Everything lives in sessionStorage — never localStorage, never a cookie —
// so the credential dies with the tab and is not readable by any other origin
// or sent along on cross-site requests. There are no refresh tokens: when the
// hour is up you sign in again.
import { ID_URL } from "./identity.js";
import { startDevPreviewSession } from "./devPreview.js";

export const CLIENT_ID = "yscale-platform";
export const ISSUER_URL = ID_URL;

const STATE_KEY = "ys.auth.state";
const VERIFIER_KEY = "ys.auth.verifier";
const NONCE_KEY = "ys.auth.nonce";
const SESSION_KEY = "ys.auth.session";
const RETURN_TO_KEY = "ys.auth.return_to";

// Treat a session as spent slightly before the real deadline so a request
// already in flight does not land after expiry.
const EXPIRY_SKEW_MS = 30_000;

export class AuthError extends Error {
  constructor(message, { code = "auth_failed" } = {}) {
    super(message);
    this.name = "AuthError";
    this.code = code;
  }
}

// sessionStorage throws in some locked-down browser modes; a null store means
// "cannot sign in here", which the UI reports rather than crashing on.
function store() {
  try {
    return window.sessionStorage;
  } catch {
    return null;
  }
}

function read(key) {
  try {
    return store()?.getItem(key) ?? null;
  } catch {
    return null;
  }
}

function write(key, value) {
  try {
    store()?.setItem(key, value);
  } catch {
    /* storage unavailable; beginLogin has already surfaced that */
  }
}

function drop(...keys) {
  const s = store();
  if (!s) return;
  for (const key of keys) {
    try {
      s.removeItem(key);
    } catch {
      /* nothing useful to do */
    }
  }
}

export function redirectURI() {
  return `${window.location.origin}/callback`;
}

function base64url(bytes) {
  let binary = "";
  for (const byte of bytes) binary += String.fromCharCode(byte);
  return btoa(binary).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
}

function randomToken(byteLength = 32) {
  const buf = new Uint8Array(byteLength);
  window.crypto.getRandomValues(buf);
  return base64url(buf);
}

async function challengeFor(verifier) {
  const digest = await window.crypto.subtle.digest("SHA-256", new TextEncoder().encode(verifier));
  return base64url(new Uint8Array(digest));
}

// WebCrypto is only exposed in a secure context, so http:// on a LAN address
// cannot do PKCE. Say so plainly instead of failing at the redirect.
export function canSignIn() {
  return typeof window !== "undefined" && !!window.crypto?.subtle && !!store();
}

function safeReturnTo(value) {
  if (value === "/account" || value === "/workloads" || value?.startsWith("/workloads/")) return value;
  return "/account";
}

export async function beginLogin(returnTo = "/account") {
  if (!canSignIn()) {
    throw new AuthError(
      "This browser cannot start a secure sign-in here. Yscale ID needs an HTTPS origin and session storage.",
      { code: "insecure_context" }
    );
  }
  const verifier = randomToken(32);
  const state = randomToken(16);
  const nonce = randomToken(16);
  const challenge = await challengeFor(verifier);

  write(VERIFIER_KEY, verifier);
  write(STATE_KEY, state);
  write(NONCE_KEY, nonce);
  write(RETURN_TO_KEY, safeReturnTo(returnTo));

  const params = new URLSearchParams({
    response_type: "code",
    client_id: CLIENT_ID,
    redirect_uri: redirectURI(),
    scope: "openid profile email",
    state,
    nonce,
    code_challenge: challenge,
    code_challenge_method: "S256",
  });
  window.location.assign(`${ISSUER_URL}/authorize?${params}`);
}

// completeCallback consumes the one-time material for this redirect. Callers
// pass the query string explicitly so the URL can be scrubbed from history
// before any network call is made.
export async function completeCallback(search) {
  const params = new URLSearchParams(search);
  const expectedState = read(STATE_KEY);
  const verifier = read(VERIFIER_KEY);
  drop(STATE_KEY, VERIFIER_KEY, NONCE_KEY);

  const idError = params.get("error");
  if (idError) {
    throw new AuthError(params.get("error_description") || `Yscale ID refused the sign-in (${idError}).`, {
      code: idError,
    });
  }
  const code = params.get("code");
  const state = params.get("state");
  if (!code || !state) {
    throw new AuthError("This callback is missing its authorization code.", { code: "missing_code" });
  }
  if (!expectedState || state !== expectedState) {
    throw new AuthError(
      "The sign-in state did not match the one this tab started with, so it was discarded. Start again from /account.",
      { code: "state_mismatch" }
    );
  }
  if (!verifier) {
    throw new AuthError("This tab has no PKCE verifier for that code. Start the sign-in again.", {
      code: "missing_verifier",
    });
  }

  let res;
  try {
    res = await fetch("/api/auth/token", {
      method: "POST",
      headers: { "Content-Type": "application/x-www-form-urlencoded", Accept: "application/json" },
      // The dev preview sits behind Cloudflare Access, whose same-origin
      // cookie must reach the edge. The Go seam rebuilds the upstream request
      // and never forwards browser cookies to Yscale ID.
      credentials: "same-origin",
      body: new URLSearchParams({
        grant_type: "authorization_code",
        code,
        redirect_uri: redirectURI(),
        client_id: CLIENT_ID,
        code_verifier: verifier,
      }),
    });
  } catch {
    throw new AuthError("Could not reach yscale.sh to finish the sign-in.", { code: "network" });
  }

  const payload = await res.json().catch(() => null);
  if (!res.ok) {
    throw new AuthError(tokenErrorMessage(res.status, payload), { code: payload?.error || "token_exchange_failed" });
  }
  if (!payload?.access_token) {
    throw new AuthError("Yscale ID returned no access token.", { code: "no_access_token" });
  }

  const expiresIn = Number(payload.expires_in) || 3600;
  const session = {
    accessToken: payload.access_token,
    idToken: payload.id_token || null,
    expiresAt: Date.now() + expiresIn * 1000,
  };
  write(SESSION_KEY, JSON.stringify(session));
  return session;
}

function tokenErrorMessage(status, payload) {
  if (status === 503) return "The identity service is not reachable from this site right now.";
  if (payload?.error_description) return payload.error_description;
  if (payload?.error) return `Yscale ID rejected the exchange (${payload.error}).`;
  return `The token exchange failed (HTTP ${status}).`;
}

export function getSession() {
  const raw = read(SESSION_KEY);
  if (!raw) return null;
  let session;
  try {
    session = JSON.parse(raw);
  } catch {
    drop(SESSION_KEY);
    return null;
  }
  if (!session?.accessToken || !session?.expiresAt || Date.now() >= session.expiresAt - EXPIRY_SKEW_MS) {
    drop(SESSION_KEY);
    return null;
  }
  return session;
}

export function beginDevPreview(mode = "populated") {
  const session = startDevPreviewSession(mode);
  if (!session) return null;
  write(SESSION_KEY, JSON.stringify(session));
  return session;
}

export function signOut() {
  drop(SESSION_KEY, STATE_KEY, VERIFIER_KEY, NONCE_KEY, RETURN_TO_KEY);
}

export function takeLoginReturnTo() {
  const value = safeReturnTo(read(RETURN_TO_KEY));
  drop(RETURN_TO_KEY);
  return value;
}

// Claims are decoded for display only — what this account may actually do is
// decided by the API against the token, never by this function.
export function claimsOf(idToken) {
  if (!idToken) return null;
  const payload = idToken.split(".")[1];
  if (!payload) return null;
  try {
    const b64 = payload.replace(/-/g, "+").replace(/_/g, "/").padEnd(Math.ceil(payload.length / 4) * 4, "=");
    const bytes = Uint8Array.from(atob(b64), (ch) => ch.charCodeAt(0));
    return JSON.parse(new TextDecoder().decode(bytes));
  } catch {
    return null;
  }
}
