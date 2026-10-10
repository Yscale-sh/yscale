// The console's pure display formatters. They live outside ConsoleKit.jsx so
// the record-derivation modules — and their node:test suites — can render the
// same strings the components do without pulling JSX into a plain node run.

export function formatDate(value, withTime = true) {
  if (!value) return "Not yet";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return "Unknown";
  return date.toLocaleString(undefined, withTime
    ? { dateStyle: "medium", timeStyle: "short" }
    : { dateStyle: "medium" });
}

export function formatUSD(value) {
  const number = Number(value);
  return Number.isFinite(number) ? `$${number.toFixed(2)}` : "—";
}

export function statusLabel(value) {
  return String(value || "unknown").replaceAll("_", " ");
}

export function formatBytes(bytes) {
  if (bytes === null || bytes === undefined) return "not reported";
  const num = Number(bytes);
  if (!Number.isSafeInteger(num) || num < 0) return "not reported";
  const exact = num.toLocaleString("en-US");
  if (num === 0) return `0 B (${exact} bytes)`;
  if (num < 1024) return `${exact} bytes`;
  const units = ["B", "KiB", "MiB", "GiB", "TiB", "PiB"];
  let i = Math.min(Math.floor(Math.log(num) / Math.log(1024)), units.length - 1);
  let val = num / Math.pow(1024, i);
  if (Number(val.toFixed(2)) >= 1024 && i < units.length - 1) {
    i += 1;
    val = num / Math.pow(1024, i);
  }
  const formattedVal = val % 1 === 0 ? val.toString() : val.toFixed(2).replace(/\.?0+$/, "");
  return `${formattedVal} ${units[i]} (${exact} bytes)`;
}
