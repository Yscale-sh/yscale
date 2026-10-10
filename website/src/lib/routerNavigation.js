export function handleLinkClick(event, to, onClick) {
  onClick?.(event);
  if (event.defaultPrevented) return;
  if (event.metaKey || event.ctrlKey || event.shiftKey || event.altKey || event.button !== 0) return;
  if (!to.startsWith("/") || to.startsWith("//")) return;
  if (to.includes("#")) return;

  event.preventDefault();
  window.history.pushState(null, "", to);
  window.dispatchEvent(new PopStateEvent("popstate"));
  window.scrollTo({ top: 0, behavior: "instant" });
}
