// Each installation supplies its own identity provider when it needs sign-in.
export const ID_URL = (import.meta.env?.VITE_YSCALE_ID_URL || "").trim().replace(/\/+$/, "");
export const identityConfigured = Boolean(ID_URL);
export const ACCOUNT_URL = `${ID_URL}/account`;
