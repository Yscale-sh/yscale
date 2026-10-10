// The drops registry — newest first. To publish a release: add a module
// next to this file (copy release-001.jsx as the template) and list it here.
import { release001 } from "./release-001.jsx";

export const RELEASES = [release001];

export function findRelease(slug) {
  return RELEASES.find((r) => r.slug === slug || r.n === slug);
}
