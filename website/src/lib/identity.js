const PROD_ID_URL = "https://id.kubagachi.com";
const DEV_ID_URL = "https://id-dev.yscale.sh";

function defaultIDURL() {
  if (typeof window === "undefined") return PROD_ID_URL;

  const host = window.location.hostname.toLowerCase();
  return host === "yscale.sh" || host === "www.yscale.sh"
    ? PROD_ID_URL
    : DEV_ID_URL;
}

// Keep one promotable image: production hosts use production identity, while
// the dev preview, LAN address, and local Vite server use the dev issuer.
export const ID_URL = (import.meta.env.VITE_YSCALE_ID_URL || defaultIDURL()).replace(/\/$/, "");
export const SIGNUP_URL = `${ID_URL}/signup?return_to=${encodeURIComponent("/account")}`;
export const ACCOUNT_URL = `${ID_URL}/account`;
