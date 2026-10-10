// The one error type every same-origin API client throws. It lives alone so the
// dev-preview fixtures can reject with the same shape the network clients do
// without importing a client that imports the fixtures.

export class ApiError extends Error {
  constructor(status, message, { code = "", payload = null } = {}) {
    super(message);
    this.name = "ApiError";
    this.status = status;
    this.code = code;
    this.payload = payload;
  }
  // Anything the API answered is authoritative; a 0 means we never got there.
  get isOffline() {
    return this.status === 0;
  }
  get isExpired() {
    return this.status === 401;
  }
  get isForbidden() {
    return this.status === 403;
  }
  get isUnavailable() {
    return this.status === 503 || this.status === 502;
  }
}
