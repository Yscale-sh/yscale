import { useCallback, useEffect, useState } from "react";
import { handleLinkClick } from "./routerNavigation.js";

// Tiny pathname router. Real paths (/drops, /drops/001, /unsubscribe) work
// because both vite dev and the Go server SPA-fallback unknown paths to
// index.html. Hash fragments still work for in-page anchors (/#access).

export function useRoute() {
  const read = () => (typeof window !== "undefined" ? window.location.pathname : "/");
  const [path, setPath] = useState(read);
  useEffect(() => {
    const onPop = () => setPath(read());
    window.addEventListener("popstate", onPop);
    return () => window.removeEventListener("popstate", onPop);
  }, []);
  // replace: true swaps the current entry instead of stacking one. /callback
  // uses it so the OAuth redirect never becomes a back-button destination.
  const navigate = useCallback((to, { replace = false } = {}) => {
    if (replace) window.history.replaceState(null, "", to);
    else window.history.pushState(null, "", to);
    window.dispatchEvent(new PopStateEvent("popstate"));
    window.scrollTo({ top: 0, behavior: "instant" });
  }, []);
  return [path, navigate];
}

// Link — client-side nav for internal paths; behaves like <a> otherwise
// (modified clicks, external URLs, and #hash anchors fall through).
export function Link({ to, children, onClick, ...rest }) {
  return (
    <a href={to} onClick={(event) => handleLinkClick(event, to, onClick)} {...rest}>
      {children}
    </a>
  );
}
