import { useEffect, useRef, useState } from "react";
import { ApiError } from "../lib/apiError.js";
import { formatDate, formatUSD, formatBytes, statusLabel } from "../lib/format.js";

// The console's shared furniture. Extracted from Workloads.jsx so the section
// pages can be their own module without either one importing the other; the
// markup and class names are unchanged, so the visual system is the same one.

// Console iconography. Inline 24-grid strokes so the shell needs no icon font.
const ICONS = {
  overview: ["M4 4h6v6H4zM14 4h6v6h-6zM4 14h6v6H4zM14 14h6v6h-6z"],
  workloads: ["M12 3l9 5-9 5-9-5 9-5", "M3 12.5l9 5 9-5", "M3 17l9 5 9-5"],
  templates: ["M4 4h7v7H4zM13 4h7v4h-7zM13 11h7v9h-7zM4 14h7v6H4z"],
  clusters: ["M3 5h18v5H3zM3 14h18v5H3z", "M6.5 7.5h.01M6.5 16.5h.01"],
  policies: ["M12 3l7 3v6c0 4.6-3 7.6-7 9-4-1.4-7-4.4-7-9V6z", "M9.5 12l1.8 1.8L15 10"],
  gitops: ["M6.5 7.5a2 2 0 100-4 2 2 0 000 4zM6.5 20.5a2 2 0 100-4 2 2 0 000 4zM17.5 7.5a2 2 0 100-4 2 2 0 000 4z", "M6.5 7.5v9M17.5 7.5v1.5a3.5 3.5 0 01-3.5 3.5H9"],
  usage: ["M5 20V11M12 20V4M19 20v-6", "M3 20h18"],
  team: ["M3 20v-1.2A4.8 4.8 0 017.8 14h3.4A4.8 4.8 0 0116 18.8V20", "M9.5 4.5a3.6 3.6 0 100 7.2 3.6 3.6 0 000-7.2", "M18 20v-1.2a4.8 4.8 0 00-2.6-4.2", "M15.4 5a3.6 3.6 0 010 6.6"],
  audit: ["M8 6.5h10M8 12h10M8 17.5h6", "M4.5 6.5h.01M4.5 12h.01M4.5 17.5h.01"],
  account: ["M12 12.5a4 4 0 100-8 4 4 0 000 8", "M4.8 20a7.4 7.4 0 0114.4 0"],
  search: ["M11 4.5a6.5 6.5 0 100 13 6.5 6.5 0 000-13", "M20 20l-4.4-4.4"],
  menu: ["M4 7h16M4 12h16M4 17h16"],
  close: ["M6.5 6.5l11 11M17.5 6.5l-11 11"],
  plus: ["M12 5.5v13M5.5 12h13"],
  chevron: ["M9.5 6l6 6-6 6"],
  caret: ["M6 9.5l6 6 6-6"],
  external: ["M14 4.5h5.5V10", "M19.5 4.5l-8 8", "M18 14v4.5a1 1 0 01-1 1H5.5a1 1 0 01-1-1V7a1 1 0 011-1H10"],
  refresh: ["M20 12a8 8 0 11-2.4-5.7", "M20.5 4v4.5H16"],
  alert: ["M12 3.5l8.5 16H3.5z", "M12 9.5v4.5M12 17h.01"],
  clock: ["M12 3.5a8.5 8.5 0 100 17 8.5 8.5 0 000-17", "M12 7.5V12l3 1.8"],
  spend: ["M12 3.5v17", "M15.8 7.5H10a2.75 2.75 0 000 5.5h3.6a2.75 2.75 0 010 5.5H8"],
  check: ["M5 12.5l4.5 4.5L19 7.5"],
  lock: ["M6.5 10.5h11v9h-11z", "M9 10.5V7.5a3 3 0 016 0v3"],
  copy: ["M9 9h10v11H9z", "M15 6H5v11"],
};

export function Icon({ name, size = 16 }) {
  const paths = ICONS[name] || [];
  return (
    <svg className="wk-icon" viewBox="0 0 24 24" width={size} height={size} fill="none" stroke="currentColor"
      strokeWidth="1.6" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true" focusable="false">
      {paths.map((d) => <path key={d} d={d} />)}
    </svg>
  );
}

// Re-exported so the pages keep importing their formatters from the kit they
// already import their furniture from.
export { formatDate, formatUSD, formatBytes, statusLabel };

export function apiMessage(error) {
  if (!(error instanceof ApiError)) return error?.message || "The tenant API did not answer.";
  if (error.isOffline) return "This browser could not reach the website API.";
  return error.message;
}

export function ErrorState({ error, onRetry, title = "Workloads are unavailable." }) {
  const detail = error instanceof ApiError && error.isOffline
    ? "This browser could not reach the website API. Check the connection and try again."
    : error?.message || "The workload service did not return a usable response.";
  return (
    <section className="wk-state wk-state-error" role="alert">
      <span className="wk-state-icon" aria-hidden="true"><Icon name="alert" size={18} /></span>
      <div>
        <h2>{title}</h2>
        <p>{detail}</p>
      </div>
      {onRetry && <button type="button" className="wk-btn wk-btn-secondary" onClick={onRetry}><Icon name="refresh" />Try again</button>}
    </section>
  );
}

export function LoadingState({ label = "Loading workloads…", rows = 3 }) {
  return (
    <section className="wk-state wk-state-loading" role="status" aria-live="polite" aria-busy="true">
      <span className="sr-only">{label}</span>
      <div className="wk-skeleton-rows" aria-hidden="true">
        {Array.from({ length: rows }, (_, index) => <i key={index} />)}
      </div>
    </section>
  );
}

// The answer to a role that is not entitled to a surface. Deliberately not an
// error: nothing failed, and a viewer being told "unavailable" would go looking
// for an outage that is not there.
export function ForbiddenState({ title, detail, role }) {
  return (
    <section className="wk-state wk-state-locked" role="status">
      <span className="wk-state-icon" aria-hidden="true"><Icon name="lock" size={18} /></span>
      <div>
        <h2>{title}</h2>
        <p>{detail}</p>
        {role && <span className="wk-chip">your role · {role}</span>}
      </div>
    </section>
  );
}

export function PageHeader({ eyebrow, title, copy, action }) {
  return (
    <header className="wk-page-head">
      <div>
        {eyebrow && <span className="wk-eyebrow">{eyebrow}</span>}
        <h1>{title}</h1>
        {copy && <p>{copy}</p>}
      </div>
      {action}
    </header>
  );
}

export function Panel({ title, id, meta, action, children, className = "" }) {
  return (
    <section className={`wk-panel ${className}`.trim()} aria-labelledby={id}>
      <header className="wk-panel-head">
        <div><h2 id={id}>{title}</h2>{meta && <span>{meta}</span>}</div>
        {action}
      </header>
      {children}
    </section>
  );
}

// A row of headline numbers. `text: true` on a tile switches to the smaller
// type the wrapping values (an id, a plan name) need.
export function StatTiles({ tiles, ready = true, label, columns = 4 }) {
  return (
    <section className={`wk-stats${columns === 3 ? " wk-stats-3" : ""}`} aria-label={label} aria-busy={!ready}>
      {tiles.map((tile) => (
        <article key={tile.key} className={`wk-stat${ready ? "" : " is-pending"}`}>
          <span className="wk-stat-label">{tile.icon && <Icon name={tile.icon} size={14} />}{tile.label}</span>
          <strong className={tile.text ? "wk-stat-text" : undefined}>{ready ? tile.value : "—"}</strong>
          {tile.note && <small>{tile.note}</small>}
        </article>
      ))}
    </section>
  );
}

export function DetailList({ rows }) {
  return (
    <dl className="wk-detail-list">
      {rows.map((row) => <div key={row.label}><dt>{row.label}</dt><dd>{row.value}</dd></div>)}
    </dl>
  );
}

// A description about what this console can and cannot do. Every page
// that shows derived or partial data carries one, so the limit travels with the
// numbers instead of living in a footnote nobody reads.
export function Note({ children, tone = "plain" }) {
  return <p className={`wk-note${tone === "warn" ? " is-warn" : ""}`}>{children}</p>;
}

export function WorkloadStatus({ status }) {
  const value = String(status || "unknown").toLowerCase();
  return <span className={`wk-status wk-status-${value}`}><i aria-hidden="true" />{statusLabel(value)}</span>;
}

// CopyBlock is the console's one clipboard affordance. A copy that the browser
// refuses says so rather than reporting a success that did not happen; the text
// is selectable either way.
export function CopyBlock({ label, language = "yaml", text, name }) {
  const [state, setState] = useState("idle");
  const timer = useRef(0);
  useEffect(() => () => window.clearTimeout(timer.current), []);
  const copy = async () => {
    window.clearTimeout(timer.current);
    try {
      await navigator.clipboard.writeText(text);
      setState("copied");
    } catch {
      setState("failed");
    }
    timer.current = window.setTimeout(() => setState("idle"), 2400);
  };
  return (
    <div className="wk-yaml wk-copyblock">
      <header>
        <b>{label}</b>
        <span className="wk-copyblock-actions">
          {name && <span>{name}</span>}
          <button type="button" className="wk-copy-btn" onClick={copy} aria-label={`Copy ${label}`}>
            <Icon name="copy" size={13} />
            {state === "copied" ? "Copied" : state === "failed" ? "Copy blocked" : "Copy"}
          </button>
        </span>
      </header>
      <pre><code data-language={language}>{text}</code></pre>
      <span className="sr-only" role="status" aria-live="polite">
        {state === "copied" ? `${label} copied to the clipboard.` : state === "failed" ? `${label} could not be copied; select the text instead.` : ""}
      </span>
    </div>
  );
}
