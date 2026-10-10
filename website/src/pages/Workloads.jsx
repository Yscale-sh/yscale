import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import {
  CopyBlock,
  DetailList,
  ErrorState,
  ForbiddenState,
  Icon,
  LoadingState,
  Note,
  PageHeader,
  Panel,
  StatTiles,
  WorkloadStatus,
  apiMessage,
  formatDate,
  formatUSD,
  formatBytes,
  statusLabel,
} from "../components/ConsoleKit.jsx";
import { TenantOnboarding, tenantLabel } from "../components/TenantOnboarding.jsx";
import { ApiError, fetchAccount } from "../lib/accountApi.js";
import { beginDevPreview, beginLogin, canSignIn, getSession, signOut } from "../lib/auth.js";
import { fetchClusters } from "../lib/clusterApi.js";
import {
  CLUSTER_OBSERVATION_NOTE,
  automaticPlacementAvailable,
  clusterChoiceError,
  clusterDetail,
  clusterOptionLabel,
  clusterUnavailableReason,
  findCluster,
  hostedNamespaceError,
  isHostedCluster,
  launchableClusters,
  namespaceForCluster,
} from "../lib/clusters.js";
import { costCellLabel, costDetailRows, costObservation, isTerminal, limitRows, liveDetailRows, liveSpendObservation, mergeWorkloadDetail, shouldPollWorkload, summarizeRuns, workloadNamespaces, workloadReceiptDetails } from "../lib/consoleData.js";
import { cleanupDetailRows, deriveCleanupPresentation, deriveTimelineSteps } from "../lib/workloadLifecycle.js";
import { launchFormPath, parseLaunchClusterId, resolveLaunchClusterId } from "../lib/launchIntent.js";
import { NAV_GROUPS, NAV_LINKS, consoleNavGroups, consoleNavLinks, matchConsoleRoute, navActive, routeInstanceKey } from "../lib/consoleRoutes.js";
import { canUseDevPreview, isDevPreviewToken } from "../lib/devPreview.js";
import {
  GPU_FAILURE_STATES,
  filterGPUWorkloads,
  workloadCluster,
  workloadElapsed,
  workloadFailureState,
  workloadGPU,
  workloadIsTerminal,
  workloadNeedsAttention,
  workloadPhase,
  workloadPlacement,
} from "../lib/gpuConsole.js";
import {
  assignHostedCluster,
  assignmentDraft,
  fetchHostedClusters,
  fetchHostedRequests,
  hostedUninstallCommand,
  operatorCapability,
  revokeHostedCluster,
  rotateHostedClusterCredential,
} from "../lib/operatorApi.js";
import {
  isPlacementReReviewError,
  normalizePreviewPlacement,
  executionRecipeRows,
  launchReviewRows,
  placementReceiptRows,
  placementReviewGuardrail,
  placementReviewShape,
  previewPlacementRows,
} from "../lib/placementReview.js";
import { canManageTenant, canMutateWorkload, canMutateWorkloads } from "../lib/roles.js";
import { deleteRuntimeBinding, fetchRuntimeBindings, putRuntimeBinding, RUNTIME_BINDING_KEY_RE } from "../lib/runtimeBindingApi.js";
import { Link, useRoute } from "../lib/router.jsx";
import {
  TEMPLATE_MODES,
  MAX_TEMPLATE_ENV,
  catalogTemplateById,
  fetchTemplateCatalog,
  managedRuntimeBindingEnv,
  putTemplateCatalog,
  templateDraftChanged,
  templateDraftErrors,
  templateDraftRow,
  templateReceipt,
  templateSelectionError,
  templateSourceLabel,
  versionedTemplateDraft,
} from "../lib/workloadCatalog.js";
import {
  DATA_PROVIDER_OPTIONS,
  GPU_KINDS,
  LAUNCH_RELIABILITY_LABEL,
  SIZE_OPTIONS,
  gpuBackendFor,
  gpuCountsFor,
  gpuKindsFor,
  initialWorkloadForm,
  reconcileWorkloadForm,
  recipeTextFromTokens,
  recipeTokensFromText,
  validateWorkloadForm,
  workloadYAML,
} from "../lib/workloadTemplates.js";
import { cancelWorkload, createWorkload, fetchWorkload, fetchWorkloadLogs, fetchWorkloads, previewPlacement, retryWorkload } from "../lib/workloadApi.js";
import { createIdempotencyKeys } from "../lib/workloadIdempotency.js";
import {
  canInspectRawWorkload,
  canOverrideTemplateRuntime,
  templateCatalogSummary,
  templateOwnedLaunchValues,
} from "../lib/workloadLaunchPolicy.js";
import {
  AuditPage,
  ClustersPage,
  GitOpsPage,
  PoliciesPage,
  TeamPage,
  UsagePage,
} from "./ConsoleSections.jsx";
import { OperatorTenantsPage } from "./OperatorTenants.jsx";

// The workload page before any tenant has been read. "idle" is "nothing has
// been asked for yet", which is only true before an account resolves.
const EMPTY_PAGE = { tenantId: null, status: "idle", workloads: [], error: null };
// The cluster observation moves with its tenant for the same reason the run
// list does: a launch form must never offer tenant A's clusters while tenant B's
// header is on screen.
const EMPTY_CLUSTERS = { tenantId: null, status: "idle", clusters: [], observedAt: "", livePartial: false, policy: null, error: null };
// The template catalog is a tenant's own, so it travels with its tenant for the
// same reason: a launch form must never compose a request from tenant A's
// catalog while tenant B's header is on screen.
const EMPTY_CATALOG = { tenantId: null, status: "idle", record: null, error: null };
const COMMAND_KEY = typeof navigator !== "undefined" && /mac/i.test(navigator.platform || navigator.userAgent) ? "⌘K" : "Ctrl K";

// The record's display name: the spec's metadata name, or the id central minted.
function workloadName(workload) {
  return workload?.spec?.metadata?.name || workload?.id || "Unnamed workload";
}

function SignedOut({ expired, onPreview }) {
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState(null);
  const blocked = !canSignIn();
  const start = async () => {
    setBusy(true);
    setError(null);
    try {
      await beginLogin("/workloads");
    } catch (err) {
      setError(err);
      setBusy(false);
    }
  };
  return (
    <main id="main-content" className="work-auth-page" tabIndex={-1}>
      <div className="wk-auth-card">
        <Link to="/" className="wk-brand wk-auth-brand" aria-label="Yscale home">
          <span className="wk-brand-mark" aria-hidden="true">y.</span><b>yscale</b>
        </Link>
        <h1 id="work-signin-title">Sign in to the console</h1>
        <p>Read tenant state, approved templates, and live workload runs.</p>
        {expired && <p className="wk-inline-note" role="status">Your session expired. Sign in again to continue.</p>}
        {error && <p className="wk-inline-error" role="alert">{error.message}</p>}
        {blocked && !error && <p className="wk-inline-error" role="alert">Secure sign-in needs an HTTPS or localhost origin with session storage.</p>}
        <button type="button" className="wk-btn wk-btn-primary wk-btn-block" onClick={start} disabled={busy || blocked} aria-busy={busy}>
          {busy ? "Opening Yscale ID…" : "Continue with Yscale ID"}
        </button>
        {canUseDevPreview() && (
          <button type="button" className="wk-btn wk-btn-secondary wk-btn-block" onClick={() => onPreview("populated")}>
            Use dummy developer
          </button>
        )}
        {canUseDevPreview() && (
          <button type="button" className="wk-btn wk-btn-secondary wk-btn-block" onClick={() => onPreview("first-workspace")}>
            Use first-workspace preview
          </button>
        )}
        <ul className="wk-auth-steps">
          <li><b>1</b><span>Yscale ID</span><small>PKCE browser sign-in</small></li>
          <li><b>2</b><span>Account</span><small>tenant membership</small></li>
          <li><b>3</b><span>Workloads</span><small>role-scoped access</small></li>
        </ul>
        <p className="wk-auth-foot">Credentials stay in this tab. Only your bearer token reaches the tenant API.</p>
      </div>
    </main>
  );
}

function TenantSwitcher({ tenants, activeId, onChange }) {
  if (tenants.length < 2) {
    const tenant = tenants[0];
    return tenant ? <span className="wk-tenant-static" title={tenant.customer_id}>{tenantLabel(tenant)}</span> : null;
  }
  return (
    <label className="wk-tenant-switcher">
      <span className="sr-only">Active tenant</span>
      <select value={activeId || ""} onChange={(event) => onChange(event.target.value)}>
        {tenants.map((tenant) => (
          <option key={tenant.customer_id} value={tenant.customer_id}>{tenantLabel(tenant)} · {tenant.role}</option>
        ))}
      </select>
      <Icon name="caret" size={14} />
    </label>
  );
}

function SidebarNav({ path, onNavigate, operatorAvailable, tenantAvailable = true }) {
  return (
    <nav className="wk-nav" aria-label="Console sections">
      {consoleNavGroups(operatorAvailable, tenantAvailable).map((group) => (
        <div className="wk-nav-group" key={group.label}>
          <p className="wk-nav-group-label">{group.label}</p>
          {group.items.map((item) => (
            <Link
              key={item.label}
              to={item.to}
              className={`wk-nav-item${navActive(path, item.to) ? " is-active" : ""}`}
              aria-current={navActive(path, item.to) ? "page" : undefined}
              onClick={onNavigate}
            >
              <Icon name={item.icon} /><span>{item.label}</span>
            </Link>
          ))}
        </div>
      ))}
    </nav>
  );
}

function SidebarFoot({ healthy, operatorOnly = false }) {
  return (
    <div className="wk-sidebar-foot">
      <span className="wk-health">
        <i className={healthy ? "wk-dot is-ok" : "wk-dot is-bad"} aria-hidden="true" />
        {healthy ? `${operatorOnly ? "Operator" : "Tenant"} API reachable` : `${operatorOnly ? "Operator" : "Tenant"} API unreachable`}
      </span>
      <Link to="/" className="wk-foot-link">yscale.sh<Icon name="external" size={13} /></Link>
    </div>
  );
}

function AccountMenu({ account, tenant, onSignOut }) {
  const [open, setOpen] = useState(false);
  const rootRef = useRef(null);
  const triggerRef = useRef(null);
  const label = account.name || account.email || account.subject || "Account";
  const initials = label.trim().slice(0, 2).toUpperCase();
  useEffect(() => {
    if (!open) return undefined;
    const onDown = (event) => { if (!rootRef.current?.contains(event.target)) setOpen(false); };
    const onKey = (event) => {
      if (event.key !== "Escape") return;
      setOpen(false);
      triggerRef.current?.focus();
    };
    document.addEventListener("mousedown", onDown);
    document.addEventListener("keydown", onKey);
    return () => {
      document.removeEventListener("mousedown", onDown);
      document.removeEventListener("keydown", onKey);
    };
  }, [open]);
  return (
    <div className="wk-account" ref={rootRef}>
      <button
        type="button"
        ref={triggerRef}
        className="wk-account-trigger"
        aria-label={`Account menu for ${label}, ${tenant?.role || "operator"}`}
        aria-expanded={open}
        aria-haspopup="menu"
        onClick={() => setOpen((value) => !value)}
      >
        <span className="wk-avatar" aria-hidden="true">{initials}</span>
        <span className="wk-account-label"><b>{label}</b><small>{tenant?.role || "operator"}</small></span>
        <Icon name="caret" size={14} />
      </button>
      {/* Clicks bubble from the items, so choosing anything closes the menu. */}
      {open && (
        <div className="wk-menu" role="menu" onClick={() => setOpen(false)}>
          <div className="wk-menu-head">
            <b>{label}</b>
            <small>{account.email || account.account_id}</small>
            <span className="wk-chip">{tenant ? `${tenantLabel(tenant)} · ${tenant.role}` : "platform operator"}</span>
          </div>
          <Link to="/workloads/account" className="wk-menu-item" role="menuitem">Account and role</Link>
          <Link to="/account" className="wk-menu-item" role="menuitem">Access ledger</Link>
          <button type="button" className="wk-menu-item wk-menu-danger" role="menuitem" onClick={onSignOut}>Sign out</button>
        </div>
      )}
    </div>
  );
}

function CommandPalette({ open, onClose, workloads, templates, navigate, onSignOut, operatorAvailable, tenantAvailable = true }) {
  const [query, setQuery] = useState("");
  const [cursor, setCursor] = useState(0);
  const inputRef = useRef(null);
  const paletteRef = useRef(null);

  const items = useMemo(() => {
    const entries = consoleNavLinks(operatorAvailable, tenantAvailable).map((item) => ({
      id: `nav:${item.to}`,
      group: "Go to",
      label: item.label,
      hint: item.hint,
      run: () => navigate(item.to),
    }));
    // Only what this tenant's catalog can actually launch: offering a disabled
    // or removed entry here is a shortcut to a form that refuses to submit.
    for (const template of templates) {
      entries.push({
        id: `tpl:${template.id}`,
        group: "Launch",
        label: template.title,
        hint: template.kind,
        run: () => navigate(`/workloads/new/${template.id}`),
      });
    }
    for (const workload of workloads.slice(0, 25)) {
      entries.push({
        id: `wl:${workload.id}`,
        group: "Workloads",
        label: workloadName(workload),
        hint: `${statusLabel(workload.status)} · ${workload.id}`,
        run: () => navigate(`/workloads/${encodeURIComponent(workload.id)}`),
      });
    }
    entries.push({ id: "act:signout", group: "Session", label: "Sign out", hint: "End this console session", run: onSignOut });
    return entries;
  }, [workloads, templates, navigate, onSignOut, operatorAvailable, tenantAvailable]);

  const results = useMemo(() => {
    const needle = query.trim().toLowerCase();
    if (!needle) return items;
    return items.filter((item) => `${item.label} ${item.hint || ""}`.toLowerCase().includes(needle));
  }, [items, query]);

  useEffect(() => {
    if (!open) return undefined;
    setQuery("");
    setCursor(0);
    const focus = window.requestAnimationFrame(() => inputRef.current?.focus());
    const previous = document.body.style.overflow;
    document.body.style.overflow = "hidden";
    return () => {
      window.cancelAnimationFrame(focus);
      document.body.style.overflow = previous;
    };
  }, [open]);

  if (!open) return null;

  const choose = (item) => {
    onClose();
    item.run();
  };

  const onKeyDown = (event) => {
    if (event.key === "Escape") { event.preventDefault(); onClose(); return; }
    if (!results.length) return;
    if (event.key === "ArrowDown") { event.preventDefault(); setCursor((value) => (value + 1) % results.length); }
    else if (event.key === "ArrowUp") { event.preventDefault(); setCursor((value) => (value - 1 + results.length) % results.length); }
    else if (event.key === "Enter") { event.preventDefault(); choose(results[Math.min(cursor, results.length - 1)]); }
  };

  const onDialogKeyDown = (event) => {
    if (event.key === "Escape") {
      event.preventDefault();
      onClose();
      return;
    }
    if (event.key !== "Tab") return;
    const focusable = [...paletteRef.current.querySelectorAll('input, button:not([disabled]), [href], [tabindex]:not([tabindex="-1"])')];
    if (!focusable.length) return;
    const first = focusable[0];
    const last = focusable[focusable.length - 1];
    if (event.shiftKey && document.activeElement === first) {
      event.preventDefault();
      last.focus();
    } else if (!event.shiftKey && document.activeElement === last) {
      event.preventDefault();
      first.focus();
    }
  };

  const active = results[Math.min(cursor, Math.max(results.length - 1, 0))];
  let lastGroup = null;

  return (
    <div className="wk-palette-layer">
      <div className="wk-palette-scrim" onClick={onClose} aria-hidden="true" />
      <div ref={paletteRef} className="wk-palette" role="dialog" aria-modal="true" aria-label="Command menu" onKeyDown={onDialogKeyDown}>
        <div className="wk-palette-input">
          <Icon name="search" size={18} />
          <input
            ref={inputRef}
            type="text"
            value={query}
            role="combobox"
            aria-expanded="true"
            aria-controls="wk-palette-list"
            aria-activedescendant={active ? `wk-cmd-${active.id}` : undefined}
            aria-label="Search the console"
            placeholder="Search workloads, templates, pages…"
            autoComplete="off"
            spellCheck="false"
            onChange={(event) => { setQuery(event.target.value); setCursor(0); }}
            onKeyDown={onKeyDown}
          />
          <button type="button" className="wk-icon-btn" onClick={onClose} aria-label="Close command menu"><Icon name="close" /></button>
        </div>
        <ul className="wk-palette-list" id="wk-palette-list" role="listbox" aria-label="Results">
          {results.map((item, index) => {
            const header = item.group !== lastGroup ? item.group : null;
            lastGroup = item.group;
            return (
              <li key={item.id} className="wk-palette-entry">
                {header && <p className="wk-palette-group" aria-hidden="true">{header}</p>}
                <div
                  id={`wk-cmd-${item.id}`}
                  role="option"
                  aria-selected={index === cursor}
                  className={`wk-palette-option${index === cursor ? " is-active" : ""}`}
                  onMouseDown={(event) => event.preventDefault()}
                  onMouseEnter={() => setCursor(index)}
                  onClick={() => choose(item)}
                >
                  <span>{item.label}</span>
                  {item.hint && <small>{item.hint}</small>}
                </div>
              </li>
            );
          })}
          {!results.length && <li className="wk-palette-empty">No match for “{query}”.</li>}
        </ul>
        <footer className="wk-palette-foot">
          <span><kbd>↑</kbd><kbd>↓</kbd> move</span><span><kbd>↵</kbd> open</span><span><kbd>esc</kbd> close</span>
        </footer>
      </div>
    </div>
  );
}

function DeveloperShell({ path, account, activeTenant, setActiveId, onSignOut, workloads, workloadStatus, launchTemplates, navigate, operatorAvailable, operatorWarning, children }) {
  const tenants = account?.tenants || [];
  const tenantAvailable = tenants.length > 0;
  const [drawerOpen, setDrawerOpen] = useState(false);
  const [paletteOpen, setPaletteOpen] = useState(false);
  const commandRef = useRef(null);
  const menuButtonRef = useRef(null);
  const drawerRef = useRef(null);
  const drawerCloseRef = useRef(null);
  const running = useMemo(() => workloads.filter((item) => !isTerminal(item.status)).length, [workloads]);

  useEffect(() => { setDrawerOpen(false); }, [path]);

  useEffect(() => {
    const onKey = (event) => {
      if ((event.metaKey || event.ctrlKey) && event.key.toLowerCase() === "k") {
        event.preventDefault();
        setPaletteOpen(true);
      } else if (event.key === "Escape") {
        setDrawerOpen(false);
      }
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, []);

  useEffect(() => {
    if (!drawerOpen) return undefined;
    const previous = document.body.style.overflow;
    document.body.style.overflow = "hidden";
    const frame = window.requestAnimationFrame(() => drawerCloseRef.current?.focus());
    const onKey = (event) => {
      if (event.key === "Escape") {
        event.preventDefault();
        setDrawerOpen(false);
        return;
      }
      if (event.key !== "Tab") return;
      const focusable = [...drawerRef.current.querySelectorAll('button:not([disabled]), [href], select, input, [tabindex]:not([tabindex="-1"])')];
      if (!focusable.length) return;
      const first = focusable[0];
      const last = focusable[focusable.length - 1];
      if (event.shiftKey && document.activeElement === first) {
        event.preventDefault();
        last.focus();
      } else if (!event.shiftKey && document.activeElement === last) {
        event.preventDefault();
        first.focus();
      }
    };
    document.addEventListener("keydown", onKey);
    return () => {
      window.cancelAnimationFrame(frame);
      document.removeEventListener("keydown", onKey);
      document.body.style.overflow = previous;
      menuButtonRef.current?.focus();
    };
  }, [drawerOpen]);

  const closePalette = () => {
    setPaletteOpen(false);
    window.requestAnimationFrame(() => commandRef.current?.focus());
  };

  return (
    <div className="work-app">
      <aside className="wk-sidebar" inert={paletteOpen ? "" : undefined} aria-hidden={paletteOpen ? true : undefined}>
        <Link to="/" className="wk-brand" aria-label="Yscale home">
          <span className="wk-brand-mark" aria-hidden="true">y.</span><b>yscale</b><em>console</em>
        </Link>
        <SidebarNav path={path} operatorAvailable={operatorAvailable} tenantAvailable={tenantAvailable} />
        <SidebarFoot healthy={workloadStatus !== "error"} operatorOnly={!tenantAvailable} />
      </aside>

      {drawerOpen && (
        <div className="wk-drawer-layer">
          <div className="wk-drawer-scrim" onClick={() => setDrawerOpen(false)} aria-hidden="true" />
          <div ref={drawerRef} className="wk-drawer" role="dialog" aria-modal="true" aria-label="Console navigation">
            <div className="wk-drawer-head">
              <Link to="/" className="wk-brand" aria-label="Yscale home">
                <span className="wk-brand-mark" aria-hidden="true">y.</span><b>yscale</b>
              </Link>
              <button ref={drawerCloseRef} type="button" className="wk-icon-btn" onClick={() => setDrawerOpen(false)} aria-label="Close navigation"><Icon name="close" /></button>
            </div>
            <SidebarNav path={path} operatorAvailable={operatorAvailable} tenantAvailable={tenantAvailable} onNavigate={() => setDrawerOpen(false)} />
            <SidebarFoot healthy={workloadStatus !== "error"} operatorOnly={!tenantAvailable} />
          </div>
        </div>
      )}

      <div className="wk-stage" inert={drawerOpen || paletteOpen ? "" : undefined} aria-hidden={drawerOpen || paletteOpen ? true : undefined}>
        <header className="wk-topbar">
          <button ref={menuButtonRef} type="button" className="wk-icon-btn wk-menu-btn" onClick={() => setDrawerOpen(true)} aria-label="Open navigation" aria-expanded={drawerOpen}>
            <Icon name="menu" size={18} />
          </button>
          <Link to="/workloads" className="wk-topbar-brand" aria-label="Console home">
            <span className="wk-brand-mark" aria-hidden="true">y.</span>
          </Link>
          <TenantSwitcher tenants={tenants} activeId={activeTenant?.customer_id} onChange={setActiveId} />
          <button
            type="button"
            ref={commandRef}
            className="wk-command"
            aria-label={`Search workloads, templates, and pages. ${COMMAND_KEY}`}
            aria-haspopup="dialog"
            onClick={() => setPaletteOpen(true)}
          >
            <Icon name="search" />
            <span>Search workloads, templates, pages</span>
            <kbd>{COMMAND_KEY}</kbd>
          </button>
          <span className={`wk-pulse${running ? " is-live" : ""}`} title={`${running} workload${running === 1 ? "" : "s"} running`}>
            <i aria-hidden="true" />
            <span>{running ? `${running} running` : "All idle"}</span>
          </span>
          <AccountMenu account={account} tenant={activeTenant} onSignOut={onSignOut} />
        </header>
        <main id="main-content" className="wk-main" tabIndex={-1}>
          {operatorWarning && <p className="wk-operator-warning" role="status"><Icon name="alert" size={14} />Operator tools could not be verified and remain hidden. {operatorWarning}</p>}
          {children}
        </main>
      </div>

      <CommandPalette
        open={paletteOpen}
        onClose={closePalette}
        workloads={workloads}
        templates={launchTemplates}
        navigate={navigate}
        onSignOut={onSignOut}
        operatorAvailable={operatorAvailable}
        tenantAvailable={tenantAvailable}
      />
    </div>
  );
}

function StatStrip({ stats, ready, attention, gpuRuns }) {
  return (
    <StatTiles
      label="Tenant summary"
      ready={ready}
      tiles={[
        { key: "active", icon: "clock", label: "Active runs", value: stats.active, note: "not yet terminal" },
        { key: "attention", icon: "alert", label: "Needs attention", value: attention, note: attention ? "teardown or placement is open" : "nothing waiting on a human" },
        { key: "gpu", icon: "workloads", label: "GPU runs", value: gpuRuns, note: `of ${stats.total} records` },
        { key: "cost", icon: "spend", label: "Teardown estimates", value: stats.estimateRecords ? formatUSD(stats.estimatedUSD) : "not recorded", note: `frozen on ${stats.estimateRecords} of ${stats.total} records` },
      ]}
    />
  );
}

function LaunchRow({ template, detailed = false, launchClusterId }) {
  const summary = templateCatalogSummary(template);
  return (
    <li className={`wk-launch${template.disabled ? " is-off" : ""}`}>
      <span className="wk-launch-mark" aria-hidden="true">{template.mark}</span>
      <div className="wk-launch-copy">
        <h3>{template.title}</h3>
        <p>{template.description}</p>
        {detailed && <span className="wk-chip">{template.kind} · template v{template.version} · yscale.sh/v1</span>}
      </div>
      <span className="wk-launch-summary"><b>{summary.runtime}</b><small>{summary.compute}</small><small>{summary.references}</small></span>
      {/* A disabled entry stays visible — it is still part of the catalog a
          manager is looking at — but it is not a launch anyone can start. */}
      {template.disabled
        ? <span className="wk-chip wk-chip-warn">disabled</span>
        : <Link to={launchFormPath(template.id, launchClusterId)} className="wk-btn wk-btn-primary">Launch template<Icon name="chevron" size={14} /></Link>}
    </li>
  );
}

// What a record says it was launched from, and how confident that is. A record
// central stamped carries the entry and its version; a record from before the
// contract can only be read back off its own shape, and that guess is labelled
// rather than presented as the same fact.
function TemplateReceipt({ receipt }) {
  return (
    <span className={`wk-receipt${receipt.exact ? "" : " is-inferred"}`}>
      {receipt.title}
      <small>{receipt.exact ? `${receipt.id} v${receipt.version || "?"}` : "inferred from shape"}</small>
    </span>
  );
}


function PlacementDisclosure({ workload }) {
  const placement = workloadPlacement(workload);
  return (
    <details className="wk-placement">
      <summary><span>Provider, region, SKU</span><Icon name="caret" size={12} /></summary>
      <dl>
        <div><dt>Provider</dt><dd>{placement.provider}</dd></div>
        <div><dt>Region</dt><dd>{placement.region}</dd></div>
        <div><dt>SKU</dt><dd>{placement.sku}</dd></div>
      </dl>
    </details>
  );
}

// The one column a reader scans for. A run that needs a human says so with the
// bounded sentence for its state; everything else says nothing rather than
// printing a reassurance central never made.
function AttentionCell({ workload }) {
  const failure = workloadFailureState(workload);
  if (workloadNeedsAttention(workload)) return <span className="wk-chip wk-chip-danger">Needs attention</span>;
  if (failure) return <span className="wk-chip wk-chip-warn">{failure.title}</span>;
  return <span className="wk-quiet">—</span>;
}

// Phase is what the record calls itself; elapsed is measured from the reported
// start (or the request, if a start was never reported) to now for a live run
// and to the reported finish for a settled one. Neither is invented: a record
// missing both timestamps reads "not reported" rather than 0s.
function PhaseCell({ workload }) {
  return (
    <span className="wk-phase">
      <WorkloadStatus status={workloadPhase(workload)} />
      <small>{workloadElapsed(workload)}</small>
    </span>
  );
}

function GPURunTable({ workloads }) {
  return (
    <div className="wk-table-wrap wk-table-wrap-runs">
      <table className="wk-table wk-table-runs">
        <thead>
          <tr>
            <th scope="col">Workload</th>
            <th scope="col">Phase</th>
            <th scope="col">GPU</th>
            <th scope="col">Cluster</th>
            <th scope="col" className="wk-col-cost">Cost</th>
            <th scope="col">Attention</th>
          </tr>
        </thead>
        <tbody>
          {workloads.map((workload) => (
            <tr key={workload.id} className={workloadNeedsAttention(workload) ? "is-attention" : undefined}>
              <th scope="row">
                <Link to={`/workloads/${encodeURIComponent(workload.id)}`}>
                  <span>{workloadName(workload)}<small>{workload.id}</small></span>
                  <Icon name="chevron" size={14} />
                </Link>
              </th>
              <td><PhaseCell workload={workload} /></td>
              <td className="wk-col-gpu">{workloadGPU(workload).label}</td>
              <td className="wk-col-cluster">
                <span className="wk-cluster-value">{workloadCluster(workload)}</span>
                <PlacementDisclosure workload={workload} />
              </td>
              <td className="wk-col-cost">
                <span className="wk-cost-value">{costCellLabel(workload)}</span>
              </td>
              <td><AttentionCell workload={workload} /></td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function GPURunCards({ workloads, catalog }) {
  return (
    <ul className="wk-run-cards">
      {workloads.map((workload) => (
        <li key={workload.id} className={workloadNeedsAttention(workload) ? "is-attention" : undefined}>
          <Link to={`/workloads/${encodeURIComponent(workload.id)}`} className="wk-run-card">
            <div className="wk-run-card-top">
              <h3>{workloadName(workload)}</h3>
              <WorkloadStatus status={workloadPhase(workload)} />
            </div>
            <dl className="wk-run-card-facts">
              <div><dt>Elapsed</dt><dd>{workloadElapsed(workload)}</dd></div>
              <div><dt>GPU</dt><dd>{workloadGPU(workload).label}</dd></div>
              <div><dt>Cluster</dt><dd>{workloadCluster(workload)}</dd></div>
              <div><dt>Cost</dt><dd>{costCellLabel(workload)}</dd></div>
            </dl>
            <p className="wk-run-card-receipt"><TemplateReceipt receipt={templateReceipt(workload, catalog)} /></p>
            <div className="wk-run-card-foot">
              <AttentionCell workload={workload} />
              <Icon name="chevron" size={14} />
            </div>
          </Link>
          <PlacementDisclosure workload={workload} />
        </li>
      ))}
    </ul>
  );
}

function EmptyRuns() {
  return (
    <div className="wk-empty">
      <span className="wk-empty-icon" aria-hidden="true"><Icon name="workloads" size={20} /></span>
      <div>
        <h3>No workloads yet</h3>
        <p>The guided launch walks an approved template through to the first request for this tenant.</p>
      </div>
      <Link to="/workloads/launch" className="wk-btn wk-btn-secondary">Start a launch</Link>
    </div>
  );
}

function RunList({ workloads, workloadStatus, workloadError, retry, limit, catalog }) {
  if (workloadStatus === "loading") return <LoadingState rows={limit ? 4 : 6} />;
  if (workloadStatus === "error") return <ErrorState error={workloadError} onRetry={retry} />;
  if (workloadStatus !== "ready") return null;
  if (!workloads.length) return <EmptyRuns />;
  const rows = limit ? workloads.slice(0, limit) : workloads;
  return (
    <>
      <GPURunTable workloads={rows} />
      <GPURunCards workloads={rows} catalog={catalog} />
    </>
  );
}

const EMPTY_FILTERS = Object.freeze({ phase: "", cluster: "", gpu: "", template: "", date: "", query: "" });

const DATE_WINDOWS = [
  { value: "24h", label: "Last 24 hours" },
  { value: "7d", label: "Last 7 days" },
  { value: "30d", label: "Last 30 days" },
];

// Every option is a value some record in this tenant actually carries. A filter
// that offers a GPU family, a cluster, or a template nobody here has ever run
// is a control whose only outcome is an empty list, so the dimensions are read
// off the records rather than off the platform's catalogs.
function filterDimensions(workloads, catalog) {
  const phases = new Map();
  const clusters = new Set();
  const gpus = new Map();
  const templates = new Map();
  for (const workload of workloads) {
    const phase = workloadPhase(workload);
    if (phase) phases.set(phase.toLowerCase(), phase);
    clusters.add(workloadCluster(workload));
    const gpu = workloadGPU(workload);
    gpus.set(gpu.kind.toLowerCase(), gpu.kind === "cpu" ? "CPU only" : gpu.kind);
    const templateId = workload?.template?.id;
    if (templateId && !templates.has(templateId)) {
      templates.set(templateId, templateReceipt(workload, catalog).title || templateId);
    }
  }
  return {
    phases: [...phases.entries()].sort((a, b) => a[0].localeCompare(b[0])),
    clusters: [...clusters].sort((a, b) => a.localeCompare(b)),
    gpus: [...gpus.entries()].sort((a, b) => a[0].localeCompare(b[0])),
    templates: [...templates.entries()].sort((a, b) => a[1].localeCompare(b[1])),
  };
}

function RunFilters({ workloads, catalog, filters, onChange, resultCount }) {
  const dimensions = useMemo(() => filterDimensions(workloads, catalog), [workloads, catalog]);
  const set = (key) => (event) => onChange({ ...filters, [key]: event.target.value });
  const active = Object.keys(EMPTY_FILTERS).filter((key) => filters[key]);
  return (
    <form className="wk-run-filters" role="search" aria-label="Filter workloads" onSubmit={(event) => event.preventDefault()}>
      <label className="wk-run-filter wk-run-filter-query">
        <span>Search</span>
        <input type="search" value={filters.query} onChange={set("query")} placeholder="name, id, or image" autoComplete="off" />
      </label>
      <label className="wk-run-filter">
        <span>Phase</span>
        <select value={filters.phase} onChange={set("phase")}>
          <option value="">Any phase</option>
          {dimensions.phases.map(([value, label]) => <option key={value} value={value}>{statusLabel(label)}</option>)}
        </select>
      </label>
      <label className="wk-run-filter">
        <span>Cluster</span>
        <select value={filters.cluster} onChange={set("cluster")}>
          <option value="">Any cluster</option>
          {dimensions.clusters.map((value) => <option key={value} value={value}>{value}</option>)}
        </select>
      </label>
      <label className="wk-run-filter">
        <span>GPU</span>
        <select value={filters.gpu} onChange={set("gpu")}>
          <option value="">Any compute</option>
          {dimensions.gpus.map(([value, label]) => <option key={value} value={value}>{label}</option>)}
        </select>
      </label>
      <label className="wk-run-filter">
        <span>Template</span>
        <select value={filters.template} onChange={set("template")}>
          <option value="">Any template</option>
          {dimensions.templates.map(([value, label]) => <option key={value} value={value}>{label}</option>)}
        </select>
      </label>
      <label className="wk-run-filter">
        <span>Requested</span>
        <select value={filters.date} onChange={set("date")}>
          <option value="">Any time</option>
          {DATE_WINDOWS.map((range) => <option key={range.value} value={range.value}>{range.label}</option>)}
        </select>
      </label>
      <div className="wk-run-filters-foot">
        <p aria-live="polite">{resultCount} of {workloads.length} {workloads.length === 1 ? "record" : "records"}{active.length ? ` · ${active.length} filter${active.length === 1 ? "" : "s"}` : ""}</p>
        {active.length > 0 && <button type="button" className="wk-text-link" onClick={() => onChange({ ...EMPTY_FILTERS })}>Clear filters</button>}
      </div>
    </form>
  );
}

// The tenant's primary surface. Active and attention-needing runs sort to the
// top because they are the only ones a reader can still change; everything
// under them is history in the same table rather than a second page.
function WorkloadsHome({ tenant, workloads, workloadStatus, workloadError, retry, catalog }) {
  const [filters, setFilters] = useState(() => ({ ...EMPTY_FILTERS }));
  const stats = useMemo(() => summarizeRuns(workloads), [workloads]);
  const attention = useMemo(() => workloads.filter(workloadNeedsAttention).length, [workloads]);
  const gpuRuns = useMemo(() => workloads.filter((workload) => workloadGPU(workload).count > 0).length, [workloads]);
  const rows = useMemo(() => filterGPUWorkloads(workloads, filters), [workloads, filters]);
  const filtered = workloadStatus === "ready" && workloads.length > 0 && rows.length === 0;
  return (
    <>
      <PageHeader
        eyebrow={tenant.customer_id}
        title="Workloads"
        copy="Active GPU work first, then everything this tenant has recorded. Up to 100 records from the tenant API."
        action={<Link to="/workloads/launch" className="wk-btn wk-btn-primary"><Icon name="plus" size={15} />Launch workload</Link>}
      />
      <StatStrip stats={stats} ready={workloadStatus === "ready"} attention={attention} gpuRuns={gpuRuns} />
      <Panel
        id="runs-title"
        title="Runs"
        meta={workloadStatus === "ready" ? `${stats.active} active · ${attention} needing attention` : null}
        className="wk-panel-flush"
      >
        {workloadStatus === "ready" && workloads.length > 0 && (
          <RunFilters workloads={workloads} catalog={catalog} filters={filters} onChange={setFilters} resultCount={rows.length} />
        )}
        {filtered ? (
          <div className="wk-empty">
            <span className="wk-empty-icon" aria-hidden="true"><Icon name="search" size={20} /></span>
            <div>
              <h3>No record matches these filters</h3>
              <p>Every dimension here is read off this tenant's own records, so a combination with no match means no such run was recorded.</p>
            </div>
            <button type="button" className="wk-btn wk-btn-secondary" onClick={() => setFilters({ ...EMPTY_FILTERS })}>Clear filters</button>
          </div>
        ) : (
          <RunList workloads={rows} workloadStatus={workloadStatus} workloadError={workloadError} retry={retry} catalog={catalog} />
        )}
      </Panel>
      <Note>
        Phase, elapsed, GPU, granted cluster, cost, and placement are read back from each record. A field central never reported reads
        "not reported" rather than a default: this console does not fill gaps in the platform's own account of a run.
      </Note>
    </>
  );
}

// One editable catalog entry. Every field is a control rather than a line of
// JSON: the people who own a tenant's catalog are not the people holding its
// schema, and a text area of objects is a code editor wearing a form's clothes.
function CatalogEntryCard({ index, row, errors, onChange, onRemove, disabled, runtimeBindings = [] }) {
  const set = (key) => (event) => onChange({ ...row, [key]: event.target.value });
  const setDefault = (key) => (event) => onChange({ ...row, defaults: { ...row.defaults, [key]: event.target.value } });
  const setFlag = (key, value) => onChange({
    ...row,
    [key]: value,
    ...(key === "nodeOnly" && value ? { defaults: { ...row.defaults, image: "", command: [], args: [], env: [] } } : {}),
  });
  const updateEnv = (envIndex, next) => onChange({
    ...row,
    defaults: { ...row.defaults, env: row.defaults.env.map((entry, at) => (at === envIndex ? next : entry)) },
  });
  const addEnv = () => onChange({
    ...row,
    defaults: {
      ...row.defaults,
      env: [...row.defaults.env, { name: "", valueFrom: { secretKeyRef: { name: "", key: "" } } }],
    },
  });
  return (
    <li className={`wk-catalog-entry${row.disabled ? " is-off" : ""}`}>
      <header className="wk-catalog-entry-head">
        <div>
          <b>{row.title || "Untitled template"}</b>
          <span>{row.id ? `${row.id} · v${row.version}` : "new entry"}</span>
        </div>
        <div className="wk-catalog-entry-actions">
          {row.disabled && <span className="wk-chip wk-chip-warn">disabled</span>}
          <button type="button" className="wk-btn wk-btn-secondary" onClick={onRemove} disabled={disabled}>Remove</button>
        </div>
      </header>
      <div className="wk-form-grid wk-form-grid-2">
        <Field label="Template id" error={errors.id} hint="Stable across edits; it is the provenance a record keeps.">
          <input value={row.id} onChange={set("id")} autoComplete="off" spellCheck="false" placeholder="pytorch-training" />
        </Field>
        <Field label="Title" error={errors.title}><input value={row.title} onChange={set("title")} autoComplete="off" /></Field>
        <Field label="Kind" error={errors.kind} hint="What the request is, in the reader's words."><input value={row.kind} onChange={set("kind")} autoComplete="off" placeholder="Run-once GPU job" /></Field>
        <Field label="Badge" error={errors.mark} hint="Up to eight characters on the card."><input value={row.mark} onChange={set("mark")} autoComplete="off" placeholder="PT" /></Field>
        <Field label="Description" error={errors.description} className="wk-field-wide">
          <input value={row.description} onChange={set("description")} autoComplete="off" placeholder="Launch a PyTorch image with an explicit GPU shape." />
        </Field>
        <Field label="Default workload name" error={errors.name}><input value={row.defaults.name} onChange={setDefault("name")} autoComplete="off" spellCheck="false" /></Field>
        <Field label="Default OCI image" error={errors.image} hint={row.nodeOnly ? "Node-only capacity runs no image." : "Registry path with an immutable tag."}>
          <input value={row.defaults.image} onChange={setDefault("image")} autoComplete="off" spellCheck="false" disabled={row.nodeOnly} />
        </Field>
        {!row.nodeOnly && (
          <>
            <Field label="Command" error={errors.command} hint="Optional entrypoint override. One token per line. Tenant-visible; never put secrets here." className="wk-field-wide wk-recipe-field">
              <textarea value={recipeTextFromTokens(row.defaults.command)} onChange={(event) => onChange({ ...row, defaults: { ...row.defaults, command: recipeTokensFromText(event.target.value) } })} spellCheck="false" placeholder="python" />
            </Field>
            <Field label="Arguments" error={errors.args} hint="Optional arguments, one token per line. Tenant-visible; use secret references instead of secret values." className="wk-field-wide wk-recipe-field">
              <textarea value={recipeTextFromTokens(row.defaults.args)} onChange={(event) => onChange({ ...row, defaults: { ...row.defaults, args: recipeTokensFromText(event.target.value) } })} spellCheck="false" placeholder={'-c\nprint("hello")'} />
            </Field>
            <fieldset className="wk-env-editor wk-field-wide">
              <legend>Environment references</legend>
              <p>Bind variables to Secret or ConfigMap keys already present in the workload namespace. Values never enter or appear in this form.</p>
              <ol>
                {row.defaults.env.map((entry, envIndex) => {
                  const secretRef = entry.valueFrom.secretKeyRef;
                  const managed = secretRef?.name === "yscale-runtime-bindings";
                  const managedExists = managed && runtimeBindings.some((binding) => binding.key === secretRef.key);
                  const sourceType = managed ? "managedBinding" : secretRef ? "secretKeyRef" : "configMapKeyRef";
                  const source = entry.valueFrom[sourceType];
                  const effectiveSource = managed ? secretRef : source;
                  const envErrors = errors.env?.[envIndex] || {};
                  const setSource = (nextSourceType) => ({
                    ...entry,
                    valueFrom: nextSourceType === "managedBinding"
                      ? { secretKeyRef: { name: "yscale-runtime-bindings", key: runtimeBindings[0]?.key || "" } }
                      : { [nextSourceType]: { name: effectiveSource?.name === "yscale-runtime-bindings" ? "" : effectiveSource?.name || "", key: effectiveSource?.key || "" } },
                  });
                  return (
                    <li key={`${index}:env:${envIndex}`}>
                      <div className="wk-env-row">
                        <Field label="Variable name" error={envErrors.name}>
                          <input value={entry.name} onChange={(event) => updateEnv(envIndex, { ...entry, name: event.target.value })} autoComplete="off" spellCheck="false" placeholder="DATABASE_URL" disabled={disabled} />
                        </Field>
                        <Field label="Source type">
                          <select value={sourceType} onChange={(event) => updateEnv(envIndex, setSource(event.target.value))} disabled={disabled}>
                            <option value="secretKeyRef">Secret</option>
                            <option value="configMapKeyRef">ConfigMap</option>
                            <option value="managedBinding" disabled={!runtimeBindings.length}>Managed runtime binding</option>
                          </select>
                        </Field>
                        {managed ? (
                          <Field label="Managed binding" error={envErrors.key} className="wk-env-managed-field">
                            <select value={secretRef.key} onChange={(event) => updateEnv(envIndex, managedRuntimeBindingEnv(entry.name, event.target.value))} disabled={disabled}>
                              {!managedExists && <option value={secretRef.key} disabled>Missing · {secretRef.key}</option>}
                              {runtimeBindings.map((binding) => <option key={binding.key} value={binding.key}>{binding.name} · {binding.key}</option>)}
                            </select>
                          </Field>
                        ) : (
                          <>
                            <Field label={sourceType === "secretKeyRef" ? "Secret name" : "ConfigMap name"} error={envErrors.objectName}>
                              <input value={source.name} onChange={(event) => updateEnv(envIndex, { ...entry, valueFrom: { [sourceType]: { ...source, name: event.target.value } } })} autoComplete="off" spellCheck="false" placeholder="app-runtime" disabled={disabled} />
                            </Field>
                            <Field label="Key" error={envErrors.key}>
                              <input value={source.key} onChange={(event) => updateEnv(envIndex, { ...entry, valueFrom: { [sourceType]: { ...source, key: event.target.value } } })} autoComplete="off" spellCheck="false" placeholder="database-url" disabled={disabled} />
                            </Field>
                          </>
                        )}
                        <button type="button" className="wk-btn wk-btn-secondary" onClick={() => onChange({ ...row, defaults: { ...row.defaults, env: row.defaults.env.filter((_, at) => at !== envIndex) } })} disabled={disabled} aria-label={`Remove environment reference ${entry.name || envIndex + 1}`}>Remove</button>
                      </div>
                    </li>
                  );
                })}
              </ol>
              {!row.defaults.env.length && <p className="wk-env-empty">No environment references in this template.</p>}
              <button type="button" className="wk-btn wk-btn-secondary" onClick={addEnv} disabled={disabled || row.defaults.env.length >= MAX_TEMPLATE_ENV}><Icon name="plus" size={14} />Add environment reference</button>
            </fieldset>
          </>
        )}
        <Field label="Default size" error={errors.size}>
          <select value={row.defaults.size} onChange={setDefault("size")}>{SIZE_OPTIONS.map((size) => <option key={size}>{size}</option>)}</select>
        </Field>
        <Field label="Default compute" error={errors.mode}>
          <select value={row.defaults.mode} onChange={setDefault("mode")}>{TEMPLATE_MODES.map((mode) => <option key={mode} value={mode}>{mode === "gpu" ? "GPU" : "CPU"}</option>)}</select>
        </Field>
        {row.defaults.mode === "gpu" && (
          <Field label="Default GPU kind" error={errors.gpuKind}>
            <select value={row.defaults.gpuKind} onChange={setDefault("gpuKind")}>{GPU_KINDS.map((kind) => <option key={kind}>{kind}</option>)}</select>
          </Field>
        )}
        <fieldset className="wk-choice-field">
          <legend>Capacity shape</legend>
          <div className="wk-segmented">
            {[[false, "Runs a Job"], [true, "Node only"]].map(([nodeOnly, label]) => (
              <label key={label}>
                <input type="radio" name={`node-only-${index}`} checked={row.nodeOnly === nodeOnly} onChange={() => setFlag("nodeOnly", nodeOnly)} />
                <span>{label}</span>
              </label>
            ))}
          </div>
        </fieldset>
        <fieldset className="wk-choice-field">
          <legend>Availability</legend>
          <div className="wk-segmented">
            {[[false, "Launchable"], [true, "Disabled"]].map(([off, label]) => (
              <label key={label}>
                <input type="radio" name={`availability-${index}`} checked={row.disabled === off} onChange={() => setFlag("disabled", off)} />
                <span>{label}</span>
              </label>
            ))}
          </div>
        </fieldset>
      </div>
    </li>
  );
}

// Rows carry a key of their own rather than being addressed by position: the id
// is editable and the list is reorderable by removal, so keying on either would
// remount the field being typed into and take the caret with it.
function catalogEntries(templates, seed) {
  return templates.map((template, index) => ({ key: `${seed}:${index}`, row: templateDraftRow(template) }));
}

function CatalogManager({ record, token, tenantId, runtimeBindings, onDirtyChange, closing, onKeepEditing, onDiscardEdits, onSaved }) {
  const nextKey = useRef(record.templates.length);
  const [entries, setEntries] = useState(() => catalogEntries(record.templates, "loaded"));
  const [saveState, setSaveState] = useState("idle");
  const [saveError, setSaveError] = useState(null);
  const [showErrors, setShowErrors] = useState(false);
  const [confirmingReset, setConfirmingReset] = useState(false);
  const rows = useMemo(() => entries.map((entry) => entry.row), [entries]);
  const errors = useMemo(() => templateDraftErrors(rows, runtimeBindings), [rows, runtimeBindings]);
  const invalid = errors.some((row) => Object.keys(row).length > 0);
  // Unsaved is measured against the document a save would publish, so a field
  // the reader retyped identically is not something to stop them over — and a
  // removed entry, a new one, or a reorder is.
  const dirty = useMemo(() => templateDraftChanged(rows, record.templates), [rows, record.templates]);
  // The page owns the Close editor control, so it is told what closing would
  // cost. Unmounting reports the editor as holding nothing, because it is.
  useEffect(() => {
    onDirtyChange(dirty);
    return () => onDirtyChange(false);
  }, [dirty, onDirtyChange]);

  const save = async (event) => {
    event.preventDefault();
    setShowErrors(true);
    setSaveError(null);
    if (invalid) return;
    setSaveState("saving");
    try {
      // The catalog is replaced whole, so the versions travel with it: an entry
      // nobody touched keeps the version its records were launched under. The
      // revision it was read at travels too, so a catalog someone else replaced
      // while this form sat open refuses the write instead of losing their work.
      const saved = await putTemplateCatalog({
        token,
        tenantId,
        templates: versionedTemplateDraft(rows, record.templates),
        catalogRevision: record.catalogRevision,
      });
      setEntries(catalogEntries(saved.templates, "saved"));
      setShowErrors(false);
      setSaveState("saved");
      onSaved(saved);
    } catch (error) {
      setSaveError(error);
      setSaveState("idle");
    }
  };

  const applyReset = () => {
    setEntries(catalogEntries(record.templates, "loaded"));
    setShowErrors(false);
    setSaveError(null);
    setSaveState("idle");
    setConfirmingReset(false);
  };

  // Reset throws away every edit in the form at once, so it asks first whenever
  // there is something to throw away.
  const reset = () => {
    if (dirty) setConfirmingReset(true);
    else applyReset();
  };

  const add = () => {
    nextKey.current += 1;
    setEntries((current) => [...current, { key: `new:${nextKey.current}`, row: templateDraftRow() }]);
  };

  return (
    <form className="wk-form wk-catalog-form" onSubmit={save} noValidate>
      {/* Asked where the Close editor button is, since that is the control the
          reader just pressed. */}
      {closing && (
        <div className="wk-confirm" role="alertdialog" aria-labelledby="catalog-close-title">
          <div>
            <b id="catalog-close-title">Close the editor and lose these edits?</b>
            <span>They have not been written to the tenant catalog, and closing the editor throws them away.</span>
          </div>
          <div>
            <button type="button" className="wk-btn wk-btn-secondary" onClick={onKeepEditing}>Keep editing</button>
            <button type="button" className="wk-btn wk-btn-danger" onClick={onDiscardEdits}>Close and discard</button>
          </div>
        </div>
      )}
      <ul className="wk-catalog-list">
        {entries.map((entry, index) => (
          <CatalogEntryCard
            key={entry.key}
            index={entry.key}
            row={entry.row}
            errors={showErrors ? errors[index] || {} : {}}
            disabled={saveState === "saving"}
            runtimeBindings={runtimeBindings}
            onChange={(next) => setEntries((current) => current.map((item) => (item.key === entry.key ? { ...item, row: next } : item)))}
            onRemove={() => setEntries((current) => current.filter((item) => item.key !== entry.key))}
          />
        ))}
      </ul>
      {!entries.length && (
        <p className="wk-form-note"><b>The catalog is empty.</b> Saving this state disables template launches for the tenant until an owner or admin adds an entry.</p>
      )}
      <div className="wk-actions wk-actions-split">
        <button type="button" className="wk-btn wk-btn-secondary" onClick={add} disabled={saveState === "saving"}>
          <Icon name="plus" size={15} />Add template
        </button>
        <div className="wk-actions-right">
          <button type="button" className="wk-btn wk-btn-secondary" onClick={reset} disabled={saveState === "saving"}>Reset</button>
          <button type="submit" className="wk-btn wk-btn-primary" disabled={saveState === "saving"} aria-busy={saveState === "saving"}>
            {saveState === "saving" ? "Saving…" : "Save catalog"}
          </button>
        </div>
      </div>
      {confirmingReset && (
        <div className="wk-confirm" role="alertdialog" aria-labelledby="catalog-reset-title">
          <div>
            <b id="catalog-reset-title">Discard the edits in this form?</b>
            <span>Every field goes back to revision {record.catalogRevision}, the catalog this tenant is being served.</span>
          </div>
          <div>
            <button type="button" className="wk-btn wk-btn-secondary" onClick={() => setConfirmingReset(false)}>Keep editing</button>
            <button type="button" className="wk-btn wk-btn-danger" onClick={applyReset}>Discard edits</button>
          </div>
        </div>
      )}
      {showErrors && invalid && <p className="wk-inline-error" role="alert">Fix the highlighted fields before saving. The whole catalog is written at once.</p>}
      {saveError && <p className="wk-inline-error" role="alert">{saveError.message}</p>}
      {saveState === "saved" && <p className="wk-inline-note" role="status">Saved. This tenant now launches from its own catalog.</p>}
    </form>
  );
}

function RuntimeBindingsPanel({ tenant, token, catalog, record, status, error, onReload, onRecord }) {
  const canManage = canManageTenant(tenant.role);
  const [editing, setEditing] = useState(null);
  const [key, setKey] = useState("");
  const [name, setName] = useState("");
  const [value, setValue] = useState("");
  const [busy, setBusy] = useState("");
  const [actionError, setActionError] = useState(null);
  const [pendingDelete, setPendingDelete] = useState(null);
  const valueRef = useRef(null);
  const bindings = record?.bindings || [];

  const referencedBy = (bindingKey) => (catalog?.templates || []).filter((template) =>
    (template.defaults?.env || []).some((entry) => entry.valueFrom?.secretKeyRef?.name === "yscale-runtime-bindings" && entry.valueFrom.secretKeyRef.key === bindingKey),
  ).map((template) => template.title);

  const closeEditor = () => { setEditing(null); setKey(""); setName(""); setValue(""); setActionError(null); };
  const beginRotate = (binding) => { setPendingDelete(null); setEditing(binding); setKey(binding.key); setName(binding.name); setValue(""); setActionError(null); queueMicrotask(() => valueRef.current?.focus()); };
  const save = async (event) => {
    event.preventDefault();
    setActionError(null);
    const cleanName = name.trim();
    const nameBytes = new TextEncoder().encode(cleanName).length;
    const valueBytes = new TextEncoder().encode(value).length;
    if (!RUNTIME_BINDING_KEY_RE.test(key) || nameBytes < 1 || nameBytes > 100 || valueBytes < 1 || valueBytes > 8192 || value.includes("\0")) {
      setActionError(new Error("Use an uppercase key, a 1–100 byte name, and a 1–8192 byte value with no NUL byte."));
      return;
    }
    setBusy("save");
    try {
      const saved = await putRuntimeBinding({ token, tenantId: tenant.customer_id, key, name: cleanName, value });
      onRecord({ ...record, bindings: [...bindings.filter((binding) => binding.key !== saved.binding.key), saved.binding].sort((a, b) => a.key.localeCompare(b.key)) });
      closeEditor();
    } catch (err) {
      setActionError(err);
    } finally {
      setValue("");
      setBusy("");
    }
  };
  const remove = async () => {
    if (!pendingDelete) return;
    setBusy(`delete:${pendingDelete.key}`); setActionError(null);
    try {
      await deleteRuntimeBinding({ token, tenantId: tenant.customer_id, key: pendingDelete.key });
      onRecord({ ...record, bindings: bindings.filter((binding) => binding.key !== pendingDelete.key) });
      setPendingDelete(null);
    } catch (err) { setActionError(err); } finally { setBusy(""); }
  };

  return (
    <Panel id="runtime-bindings-title" title="Runtime bindings" meta={status === "ready" ? `${bindings.length} managed` : null} className="wk-runtime-bindings">
      <p className="wk-panel-copy">Write values once; this console keeps only safe names, keys, revisions, and sync state. Templates reference the platform-managed Secret <code>yscale-runtime-bindings</code>.</p>
      {status === "loading" && <LoadingState label="Reading runtime binding summaries…" rows={2} />}
      {status === "error" && <ErrorState error={error} onRetry={onReload} title="Runtime bindings could not be read." />}
      {status === "ready" && bindings.length === 0 && <div className="wk-empty"><span className="wk-empty-icon" aria-hidden="true"><Icon name="templates" size={20} /></span><div><h3>No managed bindings</h3><p>{canManage ? "Add a write-only value for templates to reference." : "An owner or admin can add write-only values."}</p></div></div>}
      {status === "ready" && bindings.length > 0 && <ul className="wk-runtime-binding-list">{bindings.map((binding) => (
        <li key={binding.key}><div><b>{binding.name}</b><code>{binding.key}</code></div><dl><div><dt>Revision</dt><dd>{binding.revision}</dd></div><div><dt>Sync</dt><dd>{binding.syncState}</dd></div><div><dt>Updated</dt><dd>{formatDate(binding.updatedAt)}</dd></div></dl>{canManage && <div className="wk-runtime-binding-actions"><button type="button" className="wk-text-link" onClick={() => beginRotate(binding)} disabled={!!busy}>Rotate</button><button type="button" className="wk-text-link wk-text-danger" onClick={() => { closeEditor(); setPendingDelete(binding); }} disabled={!!busy}>Delete</button></div>}</li>
      ))}</ul>}
      {status === "ready" && canManage && !editing && <button type="button" className="wk-btn wk-btn-secondary" onClick={() => { setEditing({}); setKey(""); setName(""); setValue(""); setPendingDelete(null); setActionError(null); }}>Add runtime binding</button>}
      {status === "ready" && canManage && editing && <form className="wk-runtime-binding-form" onSubmit={save} noValidate>
        <Field label="Binding key" hint="Uppercase letters, digits, and underscores."><input value={key} onChange={(event) => setKey(event.target.value.toUpperCase())} maxLength={63} autoComplete="off" spellCheck="false" placeholder="DATABASE_URL" disabled={!!editing.key || !!busy} /></Field>
        <Field label="Display name"><input value={name} onChange={(event) => setName(event.target.value)} maxLength={100} autoComplete="off" disabled={!!busy} /></Field>
        <Field label={editing.key ? "Replacement value" : "Value"} hint="Write-only. It is cleared as soon as this request settles."><input ref={valueRef} type="password" value={value} onChange={(event) => setValue(event.target.value)} autoComplete="new-password" disabled={!!busy} /></Field>
        <div className="wk-actions"><button type="button" className="wk-btn wk-btn-secondary" onClick={closeEditor} disabled={!!busy}>Cancel</button><button type="submit" className="wk-btn wk-btn-primary" disabled={!!busy} aria-busy={busy === "save"}>{busy === "save" ? "Writing…" : editing.key ? "Rotate value" : "Create binding"}</button></div>
      </form>}
      {pendingDelete && <div className="wk-confirm" role="alertdialog" aria-labelledby="runtime-binding-delete-title"><div><b id="runtime-binding-delete-title">Delete {pendingDelete.name}?</b><span>{referencedBy(pendingDelete.key).length ? `Loaded templates still reference this key: ${referencedBy(pendingDelete.key).join(", ")}. Their references will remain and stop resolving.` : "No loaded template references this key. This cannot recover the old value."}</span></div><div><button type="button" className="wk-btn wk-btn-secondary" onClick={() => setPendingDelete(null)} disabled={!!busy}>Keep binding</button><button type="button" className="wk-btn wk-btn-danger" onClick={remove} disabled={!!busy}>{busy ? "Deleting…" : "Delete binding"}</button></div></div>}
      {status === "ready" && !canManage && <Note>This {tenant.role} role reads safe summaries only. Owners and admins create, rotate, and delete values.</Note>}
      {actionError && <p className="wk-inline-error" role="alert">{apiMessage(actionError)}</p>}
    </Panel>
  );
}

function TemplatesPage({ tenant, session, catalog, catalogStatus, catalogError, retryCatalog, setCatalog, launchClusterId }) {
  const canManage = canManageTenant(tenant.role);
  const [managing, setManaging] = useState(false);
  const [savedRevision, setSavedRevision] = useState("");
  // Closing the editor unmounts the form and takes its edits with it, so the
  // page has to know whether there are any before it does that.
  const [unsaved, setUnsaved] = useState(false);
  const [confirmingClose, setConfirmingClose] = useState(false);
  const [bindingPage, setBindingPage] = useState({ status: "loading", record: null, error: null });
  const [bindingReload, setBindingReload] = useState(0);
  useEffect(() => {
    const controller = new AbortController();
    setBindingPage({ status: "loading", record: null, error: null });
    fetchRuntimeBindings({ token: session.accessToken, tenantId: tenant.customer_id, signal: controller.signal })
      .then((record) => setBindingPage({ status: "ready", record, error: null }))
      .catch((error) => { if (error?.name !== "AbortError") setBindingPage({ status: "error", record: null, error }); });
    return () => controller.abort();
  }, [session.accessToken, tenant.customer_id, bindingReload]);
  // An editor the reader put back by hand has nothing left to warn about, so
  // the prompt goes with the edits it was asking about.
  useEffect(() => { if (!unsaved) setConfirmingClose(false); }, [unsaved]);
  const closeEditor = () => {
    setManaging(false);
    setConfirmingClose(false);
  };
  return (
    <>
      <PageHeader
        eyebrow={catalog ? `${templateSourceLabel(catalog.source)} · revision ${catalog.catalogRevision}` : tenant.customer_id}
        title="Templates"
        copy="Approved templates this team may launch. Every launch records the exact entry and version it was composed from."
        action={<Link to="/workloads" className="wk-text-link">Back to overview</Link>}
      />
      {catalogStatus === "loading" || catalogStatus === "idle" ? <LoadingState label="Reading this tenant's template catalog…" rows={4} /> : null}
      {catalogStatus === "error" && <ErrorState error={catalogError} onRetry={retryCatalog} title="The template catalog could not be read." />}
      {catalogStatus === "ready" && catalog && !catalog.templates.length && (
        <Panel id="catalog-empty-title" title="Catalog" meta={templateSourceLabel(catalog.source)} className="wk-panel-flush">
          <div className="wk-empty">
            <span className="wk-empty-icon" aria-hidden="true"><Icon name="templates" size={20} /></span>
            <div>
              <h3>This tenant has no templates</h3>
              <p>{canManage ? "Add the first entry below; until then this tenant cannot launch anything." : "Ask an owner or admin to add one; until then this tenant cannot launch anything."}</p>
            </div>
          </div>
        </Panel>
      )}
      {catalogStatus === "ready" && catalog && catalog.templates.length > 0 && (
        <ul className="wk-launch-list wk-launch-list-page">
          {catalog.templates.map((template) => <LaunchRow key={template.id} template={template} detailed launchClusterId={launchClusterId} />)}
        </ul>
      )}
      <RuntimeBindingsPanel tenant={tenant} token={session.accessToken} catalog={catalog} status={bindingPage.status} record={bindingPage.record} error={bindingPage.error} onReload={() => setBindingReload((value) => value + 1)} onRecord={(record) => setBindingPage({ status: "ready", record, error: null })} />
      {catalogStatus === "ready" && catalog && (
        canManage ? (
          <Panel
            id="catalog-manage-title"
            title="Manage catalog"
            meta={`${catalog.templates.length} entries · replaced whole on save`}
            action={(
              <button
                type="button"
                className="wk-btn wk-btn-secondary"
                onClick={() => {
                  if (!managing) { setManaging(true); return; }
                  if (unsaved) setConfirmingClose(true);
                  else closeEditor();
                }}
              >
                {managing ? "Close editor" : "Edit catalog"}
              </button>
            )}
          >
            {managing ? (
              <CatalogManager
                key={catalog.catalogRevision}
                record={catalog}
                token={session.accessToken}
                tenantId={tenant.customer_id}
                runtimeBindings={bindingPage.record?.bindings || []}
                onDirtyChange={setUnsaved}
                closing={confirmingClose}
                onKeepEditing={() => setConfirmingClose(false)}
                onDiscardEdits={closeEditor}
                onSaved={(saved) => {
                  setCatalog(saved);
                  setSavedRevision(saved.catalogRevision);
                  closeEditor();
                }}
              />
            ) : (
              <p className={savedRevision === catalog.catalogRevision ? "wk-inline-note" : "wk-form-note"} role={savedRevision === catalog.catalogRevision ? "status" : undefined}>
                {savedRevision === catalog.catalogRevision
                  ? `Catalog saved at revision ${catalog.catalogRevision}.`
                  : "Edit approved defaults, retire entries, or publish a new template without writing YAML."}
              </p>
            )}
          </Panel>
        ) : (
          <Note>This role can read the catalog but cannot change it. Owners and admins manage what this tenant may launch.</Note>
        )
      )}
    </>
  );
}

// The six decisions a launch makes, in the order the authoritative form makes
// them. This page is the map, not a second launch path: every step names the
// surface that actually decides it, so nothing here can drift into looking like
// a control that submits work.
const LAUNCH_JOURNEY = [
  {
    key: "workload",
    title: "Workload",
    copy: "Name the run and pick one of the namespaces central authorized for this tenant. The image, command, and arguments come from the approved template.",
    where: "Launch form · step 01",
  },
  {
    key: "compute",
    title: "Compute",
    copy: "Choose a supported size and either CPU or a GPU family and count. Only the counts a provider actually sells a card in are offered, and capacity is on-demand only.",
    where: "Launch form · step 02",
  },
  {
    key: "cluster-data",
    title: "Cluster and data",
    copy: "Pick the connected cluster the tenant placement policy allows, then declare object inputs to pull before the run and outputs to push after it.",
    where: "Launch form · step 03",
  },
  {
    key: "budget",
    title: "Budget",
    copy: "Set the hard maximum total spend, the wall-clock deadline, and any maximum hourly GPU price. Central refuses the launch rather than exceeding them.",
    where: "Launch form · step 04",
  },
  {
    key: "review",
    title: "Review",
    copy: "Read back the placement decision, guardrails, and environment references before anything is sent. The exact workload.yaml stays behind the advanced disclosure.",
    where: "Launch form · review",
  },
  {
    key: "launch",
    title: "Launch",
    copy: "Central admits the request, records the template id and version as provenance, and the run appears in Workloads with its own phase and cost.",
    where: "Tenant API",
  },
];

function LaunchJourney() {
  return (
    <ol className="wk-journey">
      {LAUNCH_JOURNEY.map((step, index) => (
        <li key={step.key}>
          <b aria-hidden="true">{String(index + 1).padStart(2, "0")}</b>
          <div>
            <h3><span className="sr-only">Step {index + 1}: </span>{step.title}</h3>
            <p>{step.copy}</p>
            <span className="wk-chip">{step.where}</span>
          </div>
        </li>
      ))}
    </ol>
  );
}

function LaunchPage({ tenant, catalog, catalogStatus, catalogError, retryCatalog, launchTemplates, launchClusterId }) {
  const canManage = canManageTenant(tenant.role);
  return (
    <>
      <PageHeader
        eyebrow={tenant.customer_id}
        title="Launch"
        copy="Start a GPU workload from an approved template. The template-backed form is the only path that submits to the tenant API."
        action={<Link to="/workloads" className="wk-text-link">Back to workloads</Link>}
      />
      <Panel id="journey-title" title="The guided launch" meta="six steps">
        <LaunchJourney />
      </Panel>
      <Panel
        id="launch-catalog-title"
        title="Launchable templates"
        meta={catalogStatus === "ready" ? `${launchTemplates.length} in this tenant's catalog` : null}
        className="wk-panel-flush"
      >
        {catalogStatus === "loading" || catalogStatus === "idle" ? <LoadingState label="Reading this tenant's template catalog…" rows={3} /> : null}
        {catalogStatus === "error" && <ErrorState error={catalogError} onRetry={retryCatalog} title="The template catalog could not be read." />}
        {catalogStatus === "ready" && !launchTemplates.length && (
          <div className="wk-empty">
            <span className="wk-empty-icon" aria-hidden="true"><Icon name="templates" size={20} /></span>
            <div>
              <h3>No launchable template</h3>
              <p>{canManage ? "This tenant's catalog carries nothing launchable. Add or re-enable an entry before a launch can start." : "This tenant's catalog carries nothing launchable. An owner or admin decides what may be launched."}</p>
            </div>
            <Link to="/workloads/templates" className="wk-btn wk-btn-secondary">Open catalog</Link>
          </div>
        )}
        {catalogStatus === "ready" && launchTemplates.length > 0 && (
          <ul className="wk-launch-list">
            {launchTemplates.map((template) => <LaunchRow key={template.id} template={template} detailed launchClusterId={launchClusterId} />)}
          </ul>
        )}
      </Panel>
      <Note>
        Every launch is composed from a catalog entry and submitted as that entry's exact version, which is what a record reads back as
        provenance. The workload.yaml is shown before submit as an advanced disclosure — it is the document being sent, not a second
        way to write one, and there is no free-form YAML submit in this console.
      </Note>
      <Note tone="warn">
        Provider, region, SKU, and price become an exact placement receipt only when central reports that decision. Until that preview
        contract is available, review shows requested constraints and tenant policy without pretending they are a granted placement.
      </Note>
      {catalog && !canManage && <Note tone="warn">This {tenant.role} role can launch approved entries but cannot change what the catalog approves.</Note>}
    </>
  );
}

// What this console can honestly say about a tenant's data path: the managed
// runtime bindings it can actually read and write, the object-store providers a
// launch may reference, and the transfer receipts central recorded per run.
// Bucket paths, endpoints, and Secret references are deliberately absent — the
// tenant workload response does not carry them, so no panel here invents one.
function DataPage({ tenant, session, catalog, workloads, workloadStatus, workloadError, retry }) {
  const [bindingPage, setBindingPage] = useState({ status: "loading", record: null, error: null });
  const [bindingReload, setBindingReload] = useState(0);
  useEffect(() => {
    const controller = new AbortController();
    setBindingPage({ status: "loading", record: null, error: null });
    fetchRuntimeBindings({ token: session.accessToken, tenantId: tenant.customer_id, signal: controller.signal })
      .then((record) => setBindingPage({ status: "ready", record, error: null }))
      .catch((error) => { if (error?.name !== "AbortError") setBindingPage({ status: "error", record: null, error }); });
    return () => controller.abort();
  }, [session.accessToken, tenant.customer_id, bindingReload]);

  const transfers = useMemo(
    () => workloads
      .filter((workload) => workload?.spec?.spec?.data || workload?.outcome)
      .map((workload) => ({ workload, receipt: workloadReceiptDetails(workload) })),
    [workloads],
  );

  return (
    <>
      <PageHeader
        eyebrow={tenant.customer_id}
        title="Data"
        copy="Inputs a run pulls before compute, artifacts it pushes after, and the write-only values templates reference."
        action={<Link to="/workloads" className="wk-text-link">Back to workloads</Link>}
      />
      <Panel id="data-providers-title" title="Object transfer" meta={`${DATA_PROVIDER_OPTIONS.length} providers`}>
        <DetailList rows={[
          { label: "Providers", value: DATA_PROVIDER_OPTIONS.map((option) => option.label).join(" · ") },
          { label: "Inputs", value: "Pulled before the container starts. A pull that fails stops the run before compute is billed." },
          { label: "Outputs", value: "Pushed after the container exits zero. A failed export is reported as its own outcome, separate from the compute result." },
          { label: "Credentials", value: "Held by central as a Kubernetes Secret reference. This console never reads or displays them." },
        ]} />
        <Note>Bucket paths, endpoints, and Secret names are not in the tenant workload response. This page shows transfer classes, declared capacity, and observed outcome only.</Note>
      </Panel>
      <RuntimeBindingsPanel
        tenant={tenant}
        token={session.accessToken}
        catalog={catalog}
        status={bindingPage.status}
        record={bindingPage.record}
        error={bindingPage.error}
        onReload={() => setBindingReload((value) => value + 1)}
        onRecord={(record) => setBindingPage({ status: "ready", record, error: null })}
      />
      <Panel
        id="data-receipts-title"
        title="Transfer receipts"
        meta={workloadStatus === "ready" ? `${transfers.length} of ${workloads.length} records` : null}
        className="wk-panel-flush"
      >
        {workloadStatus === "loading" && <LoadingState label="Reading workload records…" rows={4} />}
        {workloadStatus === "error" && <ErrorState error={workloadError} onRetry={retry} title="Workload records are unavailable." />}
        {workloadStatus === "ready" && transfers.length === 0 && (
          <div className="wk-empty">
            <span className="wk-empty-icon" aria-hidden="true"><Icon name="gitops" size={20} /></span>
            <div>
              <h3>No transfer receipt recorded</h3>
              <p>No record in this tenant declared object inputs or outputs, and none carries an observed outcome.</p>
            </div>
          </div>
        )}
        {workloadStatus === "ready" && transfers.length > 0 && (
          <>
            <div className="wk-table-wrap wk-table-wrap-data">
              <table className="wk-table">
                <thead>
                  <tr>
                    <th scope="col">Workload</th>
                    <th scope="col">Inputs</th>
                    <th scope="col">Outputs</th>
                    <th scope="col">Compute</th>
                    <th scope="col">Export</th>
                  </tr>
                </thead>
                <tbody>
                  {transfers.map(({ workload, receipt }) => (
                    <tr key={workload.id}>
                      <th scope="row">
                        <Link to={`/workloads/${encodeURIComponent(workload.id)}`}>
                          <span>{workloadName(workload)}<small>{workload.id}</small></span>
                          <Icon name="chevron" size={14} />
                        </Link>
                      </th>
                      <td>{receipt.requestedInputs}<small className="wk-quiet"> · limit {receipt.inputLimit}</small></td>
                      <td>{receipt.requestedOutputs}<small className="wk-quiet"> · limit {receipt.uploadLimit}</small></td>
                      <td><OutcomeChip tone={receipt.computeBadgeTone} label={receipt.computeLabel} /></td>
                      <td><OutcomeChip tone={receipt.artifactBadgeTone} label={receipt.artifactLabel} /></td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
            <ul className="wk-data-cards">
              {transfers.map(({ workload, receipt }) => (
                <li key={workload.id}>
                  <Link to={`/workloads/${encodeURIComponent(workload.id)}`} className="wk-data-card-head">
                    <b>{workloadName(workload)}</b>
                    <Icon name="chevron" size={14} />
                  </Link>
                  <dl>
                    <div><dt>Inputs</dt><dd>{receipt.requestedInputs}</dd></div>
                    <div><dt>Outputs</dt><dd>{receipt.requestedOutputs}</dd></div>
                    <div><dt>Compute</dt><dd><OutcomeChip tone={receipt.computeBadgeTone} label={receipt.computeLabel} /></dd></div>
                    <div><dt>Export</dt><dd><OutcomeChip tone={receipt.artifactBadgeTone} label={receipt.artifactLabel} /></dd></div>
                  </dl>
                </li>
              ))}
            </ul>
          </>
        )}
      </Panel>
    </>
  );
}

// Tenant configuration as it actually exists: what central reported about this
// tenant, what this role may do about it, and where the write surfaces live.
// Nothing here is a setting this console invented a store for.
function SettingsPage({ account, tenant, clusterPolicy, clusterStatus, clusterObservedAt }) {
  const namespaces = workloadNamespaces(tenant);
  const limits = limitRows(tenant.limits);
  const canManage = canManageTenant(tenant.role);
  const policyRows = clusterPolicy
    ? [
      { label: "Automatic placement", value: clusterPolicy.auto === "ordered" ? "Enabled, using the allow list order." : "Disabled; each workload must name an eligible cluster." },
      { label: "Allowed order", value: clusterPolicy.allow?.length ? clusterPolicy.allow.join(" → ") : "Any connected cluster unless denied." },
      { label: "Denied clusters", value: clusterPolicy.deny?.length ? clusterPolicy.deny.join(", ") : "None." },
    ]
    : [{ label: "Placement policy", value: clusterStatus === "loading" ? "reading…" : "not reported on this read" }];
  return (
    <>
      <PageHeader
        eyebrow={tenant.customer_id}
        title="Settings"
        copy="What central reports about this tenant, what this role may change, and the failure states a GPU launch can end in."
        action={<Link to="/workloads" className="wk-text-link">Back to workloads</Link>}
      />
      <div className="wk-detail-grid">
        <Panel id="settings-identity-title" title="Session and tenant">
          <DetailList rows={[
            { label: "Identity", value: account.name || account.email || account.account_id },
            { label: "Tenant", value: tenantLabel(tenant) },
            { label: "Tenant id", value: tenant.customer_id },
            { label: "Plan", value: tenant.plan || "not reported" },
            { label: "Your role", value: `${tenant.role} · ${canMutateWorkloads(tenant.role) ? "can submit and cancel workloads" : "read-only workload access"}` },
          ]} />
        </Panel>
        <Panel id="settings-namespaces-title" title="Authorized namespaces" meta={`${namespaces.length} reported`}>
          <ul className="wk-tag-list">
            {namespaces.map((namespace) => <li key={namespace}><code>{namespace}</code></li>)}
          </ul>
          <Note>A launch may only land in a namespace on this list. It comes from the tenant summary on your account and cannot be changed from this console.</Note>
        </Panel>
        <Panel id="settings-limits-title" title="Tenant limits" meta={limits.length ? `${limits.length} reported` : "none reported"}>
          {limits.length > 0 ? (
            <DetailList rows={limits.map((row) => ({
              label: row.label,
              value: row.unlimited ? "no ceiling" : row.unit === "usd" ? formatUSD(row.value) : String(row.value),
            }))} />
          ) : (
            <p className="wk-panel-copy">The tenant summary reported no limit values for this tenant.</p>
          )}
          <Note>Read-only. Central owns these values; there is no console endpoint that changes them.</Note>
        </Panel>
        <Panel
          id="settings-policy-title"
          title="Cluster placement"
          meta={clusterObservedAt ? `observed ${formatDate(clusterObservedAt)}` : null}
          action={<Link to="/workloads/policies" className="wk-text-link">Open policies<Icon name="chevron" size={13} /></Link>}
        >
          <DetailList rows={policyRows} />
          <Note>{canManage ? "Owners and admins change this on the Policies page." : `This ${tenant.role} role reads the policy but cannot change it.`}</Note>
        </Panel>
      </div>
      <Panel id="settings-writes-title" title="Where changes are made" className="wk-panel-flush">
        <ul className="wk-setting-links">
          <li><Link to="/workloads/team"><b>Team and roles</b><span>Add members, change roles, and see who may submit workloads.</span><Icon name="chevron" size={14} /></Link></li>
          <li><Link to="/workloads/templates"><b>Templates</b><span>What this tenant may launch, and the runtime bindings templates reference.</span><Icon name="chevron" size={14} /></Link></li>
          <li><Link to="/workloads/clusters"><b>Clusters</b><span>Register connectors, review the fleet, and see which clusters are launchable now.</span><Icon name="chevron" size={14} /></Link></li>
          <li><Link to="/account"><b>Account access ledger</b><span>The identity behind this session, outside the tenant console.</span><Icon name="external" size={14} /></Link></li>
        </ul>
      </Panel>
      <Panel id="settings-failures-title" title="GPU failure and recovery states" meta={`${GPU_FAILURE_STATES.length} states`} className="wk-panel-flush">
        <ul className="wk-failure-states">
          {GPU_FAILURE_STATES.map((state) => (
            <li key={state.code}>
              <div>
                <b>{state.title}</b>
                <code>{state.code}</code>
              </div>
              <p>{state.detail}</p>
              <span className="wk-chip">{state.action}</span>
            </li>
          ))}
        </ul>
      </Panel>
      <Note>
        These are every state a GPU launch or teardown can end in. A run in one of them shows the same sentence on its own record, with the
        recovery it names. Cost and cleanup stay open until Yscale has declared the provider resource absent and the run has settled.
      </Note>
    </>
  );
}

function AccountPage({ account, tenant }) {
  const rows = [
    { label: "Identity", value: account.name || account.email || account.subject, note: account.email || account.account_id },
    { label: "Active tenant", value: tenantLabel(tenant), note: tenant.plan || "plan not reported" },
    { label: "Role", value: tenant.role, note: canMutateWorkloads(tenant.role) ? "Can submit and cancel workloads" : "Read-only workload access" },
  ];
  return (
    <>
      <PageHeader
        eyebrow="Signed-in context"
        title="Account"
        copy="The identity and tenant authorizing this console session."
        action={<Link to="/account" className="wk-btn wk-btn-secondary">Access ledger<Icon name="external" size={14} /></Link>}
      />
      <section className="wk-stats wk-stats-3" aria-label="Account context">
        {rows.map((row) => (
          <article key={row.label} className="wk-stat">
            <span className="wk-stat-label">{row.label}</span>
            <strong className="wk-stat-text">{row.value}</strong>
            <small>{row.note}</small>
          </article>
        ))}
      </section>
    </>
  );
}

function Field({ label, error, hint, children, className = "" }) {
  return (
    <label className={`wk-field ${className}${error ? " has-error" : ""}`}>
      <span>{label}</span>{children}{error ? <small role="alert">{error}</small> : hint ? <small>{hint}</small> : null}
    </label>
  );
}

// A form value the request carries but no one chooses. It reads like the
// fields around it, and the term/value pair keeps the label attached to the
// value without a control for a screen reader to land on.
function StaticField({ label, value, error, hint }) {
  return (
    <div className={`wk-field wk-field-static${error ? " has-error" : ""}`}>
      <dl><dt>{label}</dt><dd>{value}</dd></dl>
      {error ? <small role="alert">{error}</small> : hint ? <small>{hint}</small> : null}
    </div>
  );
}

// The launch form's destination. The cluster is not part of the workload
// document — it rides as X-Cluster-ID — so it is chosen here and restated on
// review rather than appearing in the YAML the reader checks. Every state but
// "one or more observed clusters" is a dead end on purpose: a submit with a
// guessed target is a run landing somewhere nobody chose.
function ClusterTargetSection({ step, clusters, status, error, onRetry, value, onChange, fieldError, allowAutomatic }) {
  const eligibleClusters = launchableClusters(clusters);
  const selected = findCluster(clusters, value);
  return (
    <section className="wk-panel wk-form-section">
      <header className="wk-panel-head">
        <div><h2>Cluster and data</h2><span>First choose the connected cluster allowed to run this workload; object transfer follows in the same step.</span></div>
        <b className="wk-step">{step}</b>
      </header>
      {status !== "ready" && status !== "error" && <LoadingState label="Reading this tenant's cluster fleet…" rows={2} />}
      {status === "error" && <ErrorState error={error} onRetry={onRetry} title="The cluster fleet could not be read." />}
      {status === "ready" && !eligibleClusters.length && (
        <div className="wk-empty">
          <span className="wk-empty-icon" aria-hidden="true"><Icon name="clusters" size={20} /></span>
          <div>
            <h3>No connected eligible cluster</h3>
            <p>No registered cluster this tenant's policy allows has a live connection on this replica. Connect or allow one, then check again — a registered cluster without live signal here cannot run a workload.</p>
          </div>
          <button type="button" className="wk-btn wk-btn-secondary" onClick={onRetry}><Icon name="refresh" />Check again</button>
        </div>
      )}
      {status === "ready" && eligibleClusters.length > 0 && (
        <div className="wk-form-grid">
          <Field
            label="Target cluster"
            error={fieldError}
            hint={allowAutomatic && !value ? "Central will choose the first connected eligible tenant-owned cluster in the tenant policy order." : selected ? clusterDetail(selected) : "Choose which connected eligible cluster runs this workload."}
          >
            <select value={value} onChange={(event) => onChange(event.target.value)}>
              {allowAutomatic ? <option value="">Automatic</option> : (!value || eligibleClusters.length > 1) && <option value="">Select a cluster…</option>}
              {/* Registered rows without a live connection stay listed but are
                  never selectable: a target nobody can reach is not a choice. */}
              {clusters.map((cluster) => {
                const reason = clusterUnavailableReason(cluster);
                return (
                  <option key={cluster.id} value={cluster.id} disabled={!!reason}>
                    {clusterOptionLabel(cluster)}{reason ? ` — ${reason}` : ""}
                  </option>
                );
              })}
            </select>
          </Field>
        </div>
      )}
      <p className="wk-form-note">
        <b>Not part of the YAML.</b> Explicit targets are carried as X-Cluster-ID. Automatic placement considers tenant-owned connectors only; hosted capacity must be selected so its reserved namespace can be enforced. {CLUSTER_OBSERVATION_NOTE}
      </p>
    </section>
  );
}

function WorkloadFormPage({
  templateId,
  session,
  tenant,
  navigate,
  reloadWorkloads,
  clusters,
  clusterStatus,
  clusterError,
  clusterPolicy,
  retryClusters,
  catalog,
  catalogStatus,
  catalogError,
  retryCatalog,
  launchClusterId,
}) {
  const template = catalogTemplateById(catalog, templateId);
  const canMutate = canMutateWorkloads(tenant.role);
  const canOverrideRuntime = canOverrideTemplateRuntime(tenant.role);
  const canInspectYAML = canInspectRawWorkload(tenant.role);
  // routeInstanceKey scopes this form to the tenant, so this list is read once
  // per tenant and a switch mounts a new form rather than carrying tenant A's
  // namespace selection into tenant B.
  const namespaces = useMemo(() => workloadNamespaces(tenant), [tenant]);
  const [values, setValues] = useState(null);
  const [errors, setErrors] = useState({});
  const [clusterId, setClusterId] = useState("");
  const [reviewing, setReviewing] = useState(() => !canMutate);
  const [submitting, setSubmitting] = useState(false);
  const [submitError, setSubmitError] = useState(null);
  const [previewStatus, setPreviewStatus] = useState("idle");
  const [previewResult, setPreviewResult] = useState(null);
  const [previewError, setPreviewError] = useState(null);
  const [launchToken, setLaunchToken] = useState(null);
  const previewKeyRef = useRef(null);
  // The version the fields on screen were built from. The catalog is re-read
  // under this form — a manager may be editing it in another tab — so a launch
  // has to be composed from the entry the reader is actually looking at, not
  // from whatever the catalog says by the time they press submit.
  const [composedVersion, setComposedVersion] = useState(null);
  const launchValues = useMemo(
    () => templateOwnedLaunchValues(tenant.role, template, values),
    [tenant.role, template, values],
  );
  const yaml = useMemo(() => template && launchValues ? workloadYAML(template, launchValues) : "", [template, launchValues]);
  const autoMode = clusterPolicy?.auto || "require_pin";
  const eligibleClusters = useMemo(() => launchableClusters(clusters), [clusters]);
  const allowAutomatic = useMemo(() => automaticPlacementAvailable(clusters, autoMode), [clusters, autoMode]);
  // One holder per mounted form: retrying this submission reuses its key, an
  // edited YAML or a changed target earns a new one, and a fresh form is a
  // genuinely new run.
  const keys = useRef(null);
  if (!keys.current) keys.current = createIdempotencyKeys();
  useEffect(() => {
    if (!canMutate) setReviewing(true);
  }, [canMutate]);
  // The catalog arrives after the form mounts, so the defaults are seeded from
  // the entry once it is readable. Seeding happens once per composed version:
  // a later read that leaves the entry alone must not throw away edits.
  const seedForm = useCallback(() => {
    if (!template) return;
    setValues(initialWorkloadForm(template, namespaces));
    setErrors({});
    setComposedVersion(template.version);
  }, [template, namespaces]);
  useEffect(() => {
    if (!template || values) return;
    seedForm();
  }, [template, values, seedForm]);
  // The observation arrives after the form mounts and can change under it. One
  // cluster is an unambiguous target the form may open on; several is a choice
  // the reader has to make, and a selection the latest observation no longer
  // carries is dropped rather than sent to a cluster nobody can see.
  useEffect(() => {
    if (clusterStatus !== "ready") return;
    setClusterId((current) => {
      return resolveLaunchClusterId({
        eligibleClusters,
        requestedClusterId: launchClusterId,
        currentClusterId: current,
        allowAutomatic,
      });
    });
  }, [clusterStatus, eligibleClusters, allowAutomatic, launchClusterId]);
  // A hosted target owns its namespace: while one is selected, its reserved
  // namespace is pinned into the document. Leaving the target keeps that
  // namespace — a live assignment keeps it on the tenant's authorized list —
  // and only a namespace that list does not carry (the observation and the
  // namespace fetch can straddle an assignment change) falls back to the
  // tenant's first authorized one instead of lingering as a refusal.
  // Automatic placement pins nothing: central may choose a different cluster.
  const selectedCluster = findCluster(clusters, clusterId);
  const hostedTarget = isHostedCluster(selectedCluster) ? selectedCluster : null;
  // `values` is a dependency so a reseed — the form opening, or "Reload this
  // template" — re-pins immediately; the updater no-ops once they agree.
  useEffect(() => {
    setValues((current) => {
      if (!current) return current;
      const namespace = namespaceForCluster(selectedCluster, current.namespace, namespaces);
      return namespace === current.namespace ? current : { ...current, namespace };
    });
  }, [selectedCluster, namespaces, values]);

  const invalidatePreview = useCallback(() => {
    previewKeyRef.current = null;
    setPreviewStatus("idle");
    setPreviewResult(null);
    setPreviewError(null);
    setLaunchToken(null);
  }, []);

  const previewInputKey = JSON.stringify([yaml, clusterId, template?.id || "", composedVersion]);
  const requestPreview = useCallback(async () => {
    if (!yaml || previewStatus === "loading") return;
    const key = previewInputKey;
    if (previewKeyRef.current === key && previewStatus === "ready") return;
    previewKeyRef.current = key;
    setPreviewStatus("loading");
    setPreviewError(null);
    setLaunchToken(null);
    try {
      const result = await previewPlacement({
        token: session.accessToken,
        tenantId: tenant.customer_id,
        yaml,
        clusterId,
      });
      if (previewKeyRef.current !== key) return;
      if (result?.status === "ok" && result.launch_token) {
        setPreviewResult(result);
        setLaunchToken(result.launch_token);
        setPreviewStatus("ready");
      } else {
        setPreviewResult(result);
        setPreviewStatus("refused");
        setPreviewError(result?.message || "Central refused this placement.");
      }
    } catch (error) {
      if (previewKeyRef.current !== key) return;
      setPreviewError(error?.message || "Could not reach placement preview.");
      setPreviewStatus("error");
    }
  }, [yaml, clusterId, previewInputKey, previewStatus, session.accessToken, tenant.customer_id]);

  useEffect(() => {
    if (previewKeyRef.current && previewKeyRef.current !== previewInputKey) invalidatePreview();
  }, [previewInputKey, invalidatePreview]);

  if (catalogStatus === "loading" || catalogStatus === "idle") return <LoadingState label="Reading this tenant's template catalog…" rows={5} />;
  if (catalogStatus === "error") return <ErrorState error={catalogError} onRetry={retryCatalog} title="The template catalog could not be read." />;
  // A template the catalog no longer carries is not a form to fill in. The page
  // says which entry is missing and sends the reader back to what this tenant
  // can actually launch, rather than offering fields nothing will accept.
  if (template && !values) return <LoadingState label="Composing this template's defaults…" rows={5} />;
  if (!template) {
    return (
      <>
        <PageHeader
          eyebrow={tenant.customer_id}
          title="Template unavailable"
          copy={`"${templateId}" is not in this tenant's template catalog, so there is nothing to launch from.`}
          action={<Link to="/workloads/launch" className="wk-btn wk-btn-secondary">Browse catalog</Link>}
        />
        <ErrorState
          title="This template is no longer available."
          error={new Error("The catalog may have changed since this link was made. Choose an entry the tenant catalog still carries.")}
          onRetry={retryCatalog}
        />
      </>
    );
  }

  const update = (key) => (event) => {
    const value = event.target.value;
    setValues((current) => reconcileWorkloadForm({ ...current, [key]: value }));
    setErrors((current) => ({ ...current, [key]: undefined }));
    invalidatePreview();
  };

  // The target is validated beside the document it is submitted with: an
  // unreadable observation, an empty one, or an unmade choice all stop the form
  // here rather than becoming a guessed destination.
  const formErrors = () => {
    const nextErrors = validateWorkloadForm(template, launchValues, namespaces);
    const target = clusterStatus === "ready"
      ? clusterChoiceError(clusters, clusterId, { allowAutomatic })
      : "The connected clusters could not be read, so this workload has no target yet.";
    if (target) nextErrors.cluster = target;
    // A hosted target admits only its reserved namespace. The pair is checked
    // here — before review and again before submit — so a mismatch is refused
    // by this form rather than by central's admission decision.
    const reserved = clusterStatus === "ready" ? hostedNamespaceError(selectedCluster, launchValues.namespace) : "";
    if (reserved) nextErrors.namespace = reserved;
    // The entry is checked beside the document it composed: a template removed,
    // disabled, or moved on while the form sat open stops the launch here rather
    // than becoming a run recorded against a catalog entry nobody served.
    const selection = templateSelectionError(catalog, templateId, composedVersion);
    if (selection) nextErrors.template = selection;
    return nextErrors;
  };

  const review = (event) => {
    event.preventDefault();
    const nextErrors = formErrors();
    setErrors(nextErrors);
    if (!Object.keys(nextErrors).length) {
      setReviewing(true);
      requestPreview();
      window.scrollTo({ top: 0, behavior: "instant" });
    }
  };

  const submit = async () => {
    if (!launchToken) return;
    const nextErrors = formErrors();
    if (Object.keys(nextErrors).length) {
      setErrors(nextErrors);
      setReviewing(false);
      return;
    }
    setSubmitting(true);
    setSubmitError(null);
    try {
      const accepted = await createWorkload({
        token: session.accessToken,
        tenantId: tenant.customer_id,
        yaml,
        clusterId,
        template: { id: template.id, version: composedVersion },
        placementToken: launchToken,
        idempotencyKey: keys.current.keyFor(yaml, clusterId || "", `${template.id}@${composedVersion}`),
      });
      if (!accepted?.id) throw new Error("The workload was accepted without an id.");
      await reloadWorkloads().catch(() => {});
      navigate(`/workloads/${encodeURIComponent(accepted.id)}`);
    } catch (error) {
      if (isPlacementReReviewError(error)) {
        invalidatePreview();
        setSubmitError(new Error("The placement changed since your review. Review placement again."));
        setSubmitting(false);
        return;
      }
      setSubmitError(error);
      setSubmitting(false);
    }
  };

  // Fail closed: until the observation is readable and one of the clusters in it
  // is chosen, there is nothing to submit this workload against. An unmade
  // choice still reaches review, where the field says so — a disabled button
  // with nothing to click is not an explanation.
  const targetSelectable = clusterStatus === "ready" && eligibleClusters.length > 0;
  const targetReady = targetSelectable && !clusterChoiceError(clusters, clusterId, { allowAutomatic });
  const selectionError = templateSelectionError(catalog, templateId, composedVersion);
  const reviewShape = reviewing
    ? placementReviewShape({ template, values: launchValues, composedVersion, selectedCluster })
    : null;
  const normalizedPlacement = reviewing
    ? normalizePreviewPlacement(previewResult)
    : null;
  const placementRows = normalizedPlacement ? previewPlacementRows(normalizedPlacement) : [];
  const placementAlternatives = normalizedPlacement?.candidates.length > 0 ? (
    <details className="wk-advanced">
      <summary><span><b>Alternatives</b><small>{normalizedPlacement.candidates.length} other candidate{normalizedPlacement.candidates.length !== 1 ? "s" : ""}</small></span></summary>
      <dl>
        {normalizedPlacement.candidates.map((candidate, index) => (
          <div key={`${candidate.provider}|${candidate.region}|${candidate.sku}|${candidate.reason}|${index}`}>
            <dt>{candidate.provider} / {candidate.region || "any region"}</dt>
            <dd>{candidate.sku || candidate.gpuKind || "candidate"}{candidate.hourlyRate != null ? ` · ${formatUSD(candidate.hourlyRate)}/hr` : ""}{candidate.reason ? ` · ${candidate.reason.replaceAll("_", " ")}` : ""}</dd>
          </div>
        ))}
      </dl>
    </details>
  ) : null;
  const reviewGuardrail = reviewing
    ? placementReviewGuardrail({ values: launchValues, selectedCluster })
    : null;

  return (
    <>
      <PageHeader
        eyebrow={`${template.kind} · v${composedVersion} · catalog rev ${catalog.catalogRevision}`}
        title={template.title}
        copy={reviewing
          ? canMutate
            ? "Check the launch summary below, then submit. Nothing runs until you do."
            : "Inspect the launch summary below. Your viewer role cannot submit it."
          : template.description}
        action={<Link to="/workloads/launch" className="wk-text-link">All templates</Link>}
      />
      {!canMutate && <p className="wk-inline-error" role="alert">Your viewer role is read-only. You can inspect this template but cannot submit it.</p>}
      {/* The catalog moved under an open form. Reloading is offered only where
          it would actually help — a version that moved on — because a button
          that leaves the same refusal on screen is not an escape. */}
      {selectionError && (
        <div className="wk-confirm" role="alert">
          <div><b>This template cannot be launched right now.</b><span>{selectionError}</span></div>
          <div>
            <button type="button" className="wk-btn wk-btn-secondary" onClick={() => navigate("/workloads/launch")}>Browse catalog</button>
            {!template.disabled && template.version !== composedVersion && (
              <button type="button" className="wk-btn wk-btn-primary" onClick={() => { seedForm(); setReviewing(!canMutate); }}>Reload this template</button>
            )}
          </div>
        </div>
      )}
      {reviewing ? (
        <section className="wk-review">
          <section className="wk-review-strip" aria-label="Placement decision preview">
            <article>
              <header><span>Shape</span><b>{reviewShape.compute}</b></header>
              <dl>
                <div><dt>Template</dt><dd>{reviewShape.template}</dd></div>
                <div><dt>Namespace</dt><dd>{reviewShape.namespace}</dd></div>
                <div><dt>Backend</dt><dd>{reviewShape.backend}</dd></div>
              </dl>
            </article>
            <article>
              <header><span>Central placement</span><b>{previewStatus === "loading" ? "Loading…" : previewStatus === "ready" ? normalizedPlacement?.grantedClusterId || "—" : previewStatus === "refused" ? "Refused" : previewStatus === "error" ? "Error" : "Pending"}</b></header>
              {previewStatus === "loading" && <p>Requesting placement preview from Central…</p>}
              {previewStatus === "error" && <p className="wk-inline-error" role="alert">{previewError}</p>}
              {previewStatus === "refused" && (
                <>
                  <p className="wk-inline-error" role="alert">{previewResult?.message || "Central refused this placement."}</p>
                  {previewResult?.code && <small>Code: {previewResult.code}</small>}
                  {placementAlternatives}
                </>
              )}
              {previewStatus === "ready" && normalizedPlacement && (
                <>
                  <dl>
                    {placementRows.map((row) => (
                      <div key={row.label}><dt>{row.label}</dt><dd>{row.value}</dd></div>
                    ))}
                  </dl>
                  {placementAlternatives}
                  <small>Issued {normalizedPlacement.issuedAt ? new Date(normalizedPlacement.issuedAt).toLocaleString() : "—"} · expires {normalizedPlacement.expiresAt ? new Date(normalizedPlacement.expiresAt).toLocaleString() : "—"}</small>
                </>
              )}
              {previewStatus !== "loading" && previewStatus !== "ready" && (
                <button type="button" className="wk-btn wk-btn-secondary" onClick={requestPreview}>Review placement again</button>
              )}
            </article>
            <article>
              <header><span>Guardrail</span><b>{reviewGuardrail.limit}</b></header>
              <p>{reviewGuardrail.note}</p>
              <small>Central remains authoritative at admission and during the run.</small>
            </article>
          </section>
          <div className="wk-panel wk-review-summary">
            <header className="wk-panel-head"><div><h2>{canMutate ? "Review" : "Read-only preview"}</h2><span>Confirm the request, policy, budget, and available placement evidence.</span></div><b className="wk-step">05</b></header>
            <dl>
              {launchReviewRows({ values: launchValues, template, composedVersion, clusterId, allowAutomatic, hostedTarget }).map((row) => (
                <div key={row.label}><dt>{row.label}</dt><dd>{row.value}</dd></div>
              ))}
            </dl>
            {!!launchValues.env.length && (
              <section className="wk-env-review" aria-labelledby="environment-review-title">
                <h3 id="environment-review-title">Environment references</h3>
                <p>These variables resolve from objects already present in namespace <b>{launchValues.namespace}</b>.</p>
                <ul>
                  {launchValues.env.map((entry) => {
                    const sourceType = entry.valueFrom.secretKeyRef ? "secretKeyRef" : "configMapKeyRef";
                    const source = entry.valueFrom[sourceType];
                    return <li key={entry.name}><b>{entry.name}</b><span>{sourceType === "secretKeyRef" ? "Secret" : "ConfigMap"} <code>{source.name}</code> · key <code>{source.key}</code></span></li>;
                  })}
                </ul>
              </section>
            )}
            <Note>
              This submit carries {template.id} v{composedVersion} as X-Template-ID and X-Template-Version. Central records it on the run.
            </Note>
          </div>
          {canInspectYAML && (
            <details className="wk-advanced wk-yaml-advanced">
              <summary><span><b>Advanced · workload.yaml</b><small>The exact technical document this launch submits</small></span><Icon name="caret" /></summary>
              <div className="wk-yaml">
                <header><span>workload.yaml</span><b>text/yaml</b></header>
                <pre>{yaml}</pre>
              </div>
            </details>
          )}
          {submitError && <p className="wk-inline-error" role="alert">{submitError.message}</p>}
          {errors.cluster && <p className="wk-inline-error" role="alert">{errors.cluster}</p>}
          {errors.template && <p className="wk-inline-error" role="alert">{errors.template}</p>}
          {canMutate && (
            <div className="wk-actions">
              <button type="button" className="wk-btn wk-btn-secondary" onClick={() => { setReviewing(false); invalidatePreview(); }} disabled={submitting}>Back to form</button>
              <button type="button" className="wk-btn wk-btn-primary" onClick={submit} disabled={!canMutate || submitting || !launchToken || !!selectionError} aria-busy={submitting}>
                {submitting ? "Submitting…" : "Submit workload"}
              </button>
            </div>
          )}
        </section>
      ) : (
        <form className="wk-form" onSubmit={review} noValidate>
          <section className="wk-panel wk-form-section">
            <header className="wk-panel-head"><div><h2>Workload</h2><span>Name the request and choose its authorized namespace.</span></div><b className="wk-step">01</b></header>
            <div className="wk-form-grid">
              <Field label="Workload name" error={errors.name} hint="Lowercase DNS-style name."><input value={values.name} onChange={update("name")} autoComplete="off" /></Field>
              {/* Only the namespaces central authorized for this tenant, in the
                  order it reported them. There is no free-text escape hatch:
                  anything not on this list is a submission central denies. A
                  hosted target removes the choice entirely — its reserved
                  namespace is the only place central admits the workload. */}
              {hostedTarget ? (
                <StaticField
                  label="Kubernetes namespace"
                  value={values.namespace}
                  error={errors.namespace}
                  hint={`Reserved for this tenant on ${hostedTarget.name}. Hosted workloads run only here; choose another cluster to pick a namespace.`}
                />
              ) : (
                <Field
                  label="Kubernetes namespace"
                  error={errors.namespace}
                  hint={namespaces.length > 1 ? "Namespaces this tenant is authorized to use." : "The only namespace this tenant is authorized to use."}
                >
                  <select value={values.namespace} onChange={update("namespace")}>
                    {namespaces.map((namespace) => <option key={namespace}>{namespace}</option>)}
                  </select>
                </Field>
              )}
              {template.nodeOnly && <p className="wk-form-note"><b>No image needed.</b> This template requests capacity only; your controller owns the pods.</p>}
              {!template.nodeOnly && !canOverrideRuntime && (
                <p className="wk-form-note wk-runtime-locked"><b>Approved runtime.</b> Image, command, and arguments come from {template.title} v{composedVersion} and stay unchanged for this launch.</p>
              )}
            </div>
          </section>
          {!template.nodeOnly && canOverrideRuntime && (
            <details className="wk-advanced wk-runtime-advanced">
              <summary><span><b>Advanced runtime</b><small>Overrides affect this run only; the catalog template stays unchanged</small></span><Icon name="caret" /></summary>
              <div className="wk-form-grid">
                <Field label="OCI image" error={errors.image} hint="Registry path with an immutable tag."><input value={values.image} onChange={update("image")} autoComplete="off" spellCheck="false" /></Field>
                <div className="wk-recipe-fields">
                  <Field label="Command" error={errors.command} hint="Optional entrypoint override. One token per line; spaces stay inside that token. Do not put secrets in commands." className="wk-recipe-field">
                    <textarea value={recipeTextFromTokens(values.command)} onChange={(event) => update("command")({ target: { value: recipeTokensFromText(event.target.value) } })} spellCheck="false" placeholder="Use the image default" />
                  </Field>
                  <Field label="Arguments" error={errors.args} hint="Optional arguments. One line is one exact token. Do not put secrets in arguments." className="wk-recipe-field">
                    <textarea value={recipeTextFromTokens(values.args)} onChange={(event) => update("args")({ target: { value: recipeTokensFromText(event.target.value) } })} spellCheck="false" placeholder="Use the image default" />
                  </Field>
                </div>
              </div>
            </details>
          )}
          <section className="wk-panel wk-form-section">
            <header className="wk-panel-head"><div><h2>Compute</h2><span>Pick one supported size and CPU or GPU capacity.</span></div><b className="wk-step">02</b></header>
            <div className="wk-form-grid">
              <Field label="Size" error={errors.size}><select value={values.size} onChange={update("size")}>{SIZE_OPTIONS.map((size) => <option key={size}>{size}</option>)}</select></Field>
              <fieldset className="wk-choice-field">
                <legend>Compute mode</legend>
                <div className="wk-segmented">
                  {[["cpu", "CPU"], ["gpu", "GPU"]].map(([value, label]) => (
                    <label key={value}><input type="radio" name="mode" value={value} checked={values.mode === value} onChange={update("mode")} /><span>{label}</span></label>
                  ))}
                </div>
              </fieldset>
              {values.mode === "gpu" && (
                <div className="wk-subgrid">
                  <Field label="GPU kind" error={errors.gpuKind} hint={`Placed on ${gpuBackendFor(values.backend, values.gpuKind) || "no GPU backend"}.`}><select value={values.gpuKind} onChange={update("gpuKind")}>{gpuKindsFor(values.backend).map((kind) => <option key={kind}>{kind}</option>)}</select></Field>
                  {/* The counts a card is sold in are fixed per family — an a100 is
                      only placeable as 8 — so this is a choice, not a free number. */}
                  <Field label="GPU count" error={errors.gpuCount}><select value={values.gpuCount} onChange={update("gpuCount")}>{(gpuCountsFor(values.backend, values.gpuKind) || []).map((count) => <option key={count} value={count}>{count}</option>)}</select></Field>
                  {/* Only on-demand capacity is launchable, so this is a fact
                      about the request, not a choice. The error can only show
                      for a state the form itself cannot produce. */}
                  <StaticField label="Reliability" value={LAUNCH_RELIABILITY_LABEL} error={errors.reliability} hint="The only capacity mode supported today." />
                  <Field
                    label="Maximum hourly USD"
                    error={errors.maxHourlyUSD}
                    hint={gpuBackendFor(values.backend, values.gpuKind) === "aws" ? "Unavailable for AWS until region-aware pricing is authoritative." : "Optional per-GPU placement cap."}
                  >
                    <input type="number" min="0" step="0.01" inputMode="decimal" value={values.maxHourlyUSD} onChange={update("maxHourlyUSD")} placeholder="no explicit cap" disabled={gpuBackendFor(values.backend, values.gpuKind) === "aws"} />
                  </Field>
                </div>
              )}
            </div>
          </section>
          <ClusterTargetSection
            step="03"
            clusters={clusters}
            status={clusterStatus}
            error={clusterError}
            onRetry={retryClusters}
            value={clusterId}
            fieldError={errors.cluster}
            allowAutomatic={allowAutomatic}
            onChange={(next) => {
              setClusterId(next);
              // The namespace rule travels with the target, so a refusal
              // earned under the previous target does not outlive the switch.
              setErrors((current) => ({ ...current, cluster: undefined, namespace: undefined }));
            }}
          />
          <section className="wk-panel wk-form-section">
            <header className="wk-panel-head"><div><h2>Cluster and data - object transfer</h2><span>Bring inputs in before compute and send results out before success.</span></div><b className="wk-step">03</b></header>
            {template.nodeOnly ? (
              <p className="wk-form-note"><b>Owned by your controller.</b> Node-only capacity does not create the Job, so its pod and data volumes stay in your manifest.</p>
            ) : (
              <>
                <ol className="wk-data-flow" aria-label="Workload data lifecycle">
                  <li className={values.dataEnabled ? "is-active" : ""}><b>01</b><span>Pull input</span></li>
                  <li className="is-run"><b>02</b><span>Run job</span></li>
                  <li className={values.outputEnabled ? "is-active" : ""}><b>03</b><span>Push output</span></li>
                </ol>
                <fieldset className="wk-choice-field">
                  <legend>Object input</legend>
                  <div className="wk-segmented">
                    {[[false, "No attachment"], [true, "Pull before run"]].map(([enabled, label]) => (
                      <label key={label}><input type="radio" name="dataEnabled" checked={values.dataEnabled === enabled} onChange={() => {
                        setValues((current) => ({ ...current, dataEnabled: enabled }));
                        setErrors((current) => ({ ...current, dataEnabled: undefined }));
                      }} /><span>{label}</span></label>
                    ))}
                  </div>
                </fieldset>
                {errors.dataEnabled && <p className="wk-inline-error" role="alert">{errors.dataEnabled}</p>}
                {values.dataEnabled && (
                  <div className="wk-form-grid wk-data-grid">
                    <Field label="Object store" error={errors.dataProvider} hint="The platform supports Cloudflare R2 and native Amazon S3 on this form.">
                      <select value={values.dataProvider} onChange={update("dataProvider")}>{DATA_PROVIDER_OPTIONS.map((option) => <option key={option.value} value={option.value}>{option.label}</option>)}</select>
                    </Field>
                    <Field label="Attachment name" error={errors.dataName} hint="A safe label for this input."><input value={values.dataName} onChange={update("dataName")} autoComplete="off" /></Field>
                    <Field label="Bucket" error={errors.dataBucket}><input value={values.dataBucket} onChange={update("dataBucket")} autoComplete="off" spellCheck="false" placeholder="training-data" /></Field>
                    <Field label="Object prefix" error={errors.dataPrefix} hint="Optional path within the bucket."><input value={values.dataPrefix} onChange={update("dataPrefix")} autoComplete="off" spellCheck="false" placeholder="datasets/fraud/v4/" /></Field>
                    {values.dataProvider === "r2" ? (
                      <Field label="R2 account endpoint" error={errors.dataEndpoint} hint="HTTPS account endpoint only; no bucket path or credentials."><input type="url" value={values.dataEndpoint} onChange={update("dataEndpoint")} autoComplete="off" spellCheck="false" placeholder="https://ACCOUNT.r2.cloudflarestorage.com" /></Field>
                    ) : (
                      <Field label="AWS region" error={errors.dataRegion}><input value={values.dataRegion} onChange={update("dataRegion")} autoComplete="off" spellCheck="false" placeholder="us-east-1" /></Field>
                    )}
                    <Field label="Credential Secret" error={errors.dataCredentialsSecret} hint={`Bare Secret name in namespace ${values.namespace}. Secret values stay in that cluster.`}><input value={values.dataCredentialsSecret} onChange={update("dataCredentialsSecret")} autoComplete="off" spellCheck="false" /></Field>
                    <Field label="Mount in container" error={errors.dataTarget} hint="Read-only path available after the pull completes."><input value={values.dataTarget} onChange={update("dataTarget")} autoComplete="off" spellCheck="false" /></Field>
                    <Field label="Local size limit (GiB)" error={errors.dataSizeHintGB}><input type="number" min="1" max="65536" step="1" inputMode="numeric" value={values.dataSizeHintGB} onChange={update("dataSizeHintGB")} /></Field>
                    <StaticField label="Lifetime" value="This run only" error={errors.dataRetention} hint="The Job-local copy is deleted with the run; the object bucket remains the source of truth." />
                  </div>
                )}
                <p className="wk-form-note"><b>Pull before run.</b> The connector reads only the named Secret from the authorized workload namespace, signs short-lived object URLs, and the Job waits for the copy before starting. No credential value crosses this form.</p>
                <fieldset className="wk-choice-field wk-output-choice">
                  <legend>Object output</legend>
                  <div className="wk-segmented">
                    {[[false, "No upload"], [true, "Push after run"]].map(([enabled, label]) => (
                      <label key={label}><input type="radio" name="outputEnabled" checked={values.outputEnabled === enabled} onChange={() => {
                        setValues((current) => ({ ...current, outputEnabled: enabled }));
                        setErrors((current) => ({ ...current, outputEnabled: undefined }));
                      }} /><span>{label}</span></label>
                    ))}
                  </div>
                </fieldset>
                {errors.outputEnabled && <p className="wk-inline-error" role="alert">{errors.outputEnabled}</p>}
                {values.outputEnabled && (
                  <div className="wk-form-grid wk-data-grid wk-output-grid">
                    <Field label="Object store" error={errors.outputProvider} hint="Cloudflare R2 or native Amazon S3.">
                      <select value={values.outputProvider} onChange={update("outputProvider")}>{DATA_PROVIDER_OPTIONS.map((option) => <option key={option.value} value={option.value}>{option.label}</option>)}</select>
                    </Field>
                    <Field label="Output name" error={errors.outputName} hint="A safe label for this result set."><input value={values.outputName} onChange={update("outputName")} autoComplete="off" /></Field>
                    <Field label="Bucket" error={errors.outputBucket}><input value={values.outputBucket} onChange={update("outputBucket")} autoComplete="off" spellCheck="false" placeholder="training-results" /></Field>
                    <Field label="Object prefix" error={errors.outputPrefix} hint="Optional path within the bucket."><input value={values.outputPrefix} onChange={update("outputPrefix")} autoComplete="off" spellCheck="false" placeholder="runs/nightly/" /></Field>
                    {values.outputProvider === "r2" ? (
                      <Field label="R2 account endpoint" error={errors.outputEndpoint} hint="HTTPS account endpoint only."><input type="url" value={values.outputEndpoint} onChange={update("outputEndpoint")} autoComplete="off" spellCheck="false" placeholder="https://ACCOUNT.r2.cloudflarestorage.com" /></Field>
                    ) : (
                      <Field label="AWS region" error={errors.outputRegion}><input value={values.outputRegion} onChange={update("outputRegion")} autoComplete="off" spellCheck="false" placeholder="us-east-1" /></Field>
                    )}
                    <Field label="Credential Secret" error={errors.outputCredentialsSecret} hint={`Bare Secret name in namespace ${values.namespace}.`}><input value={values.outputCredentialsSecret} onChange={update("outputCredentialsSecret")} autoComplete="off" spellCheck="false" /></Field>
                    <Field label="Write results here" error={errors.outputTarget} hint="Writable path inside the container."><input value={values.outputTarget} onChange={update("outputTarget")} autoComplete="off" spellCheck="false" /></Field>
                    <Field label="Maximum files" error={errors.outputMaxFiles}><input type="number" min="1" max="1000" step="1" inputMode="numeric" value={values.outputMaxFiles} onChange={update("outputMaxFiles")} /></Field>
                    <Field label="Maximum upload (GiB)" error={errors.outputMaxSizeGB}><input type="number" min="1" max="1024" step="1" inputMode="numeric" value={values.outputMaxSizeGB} onChange={update("outputMaxSizeGB")} /></Field>
                  </div>
                )}
                <p className="wk-form-note"><b>Success includes upload.</b> Yscale reports the workload complete only after every regular file in the output path reaches the bound bucket. Failed or oversized uploads fail the run.</p>
              </>
            )}
          </section>
          <section className="wk-panel wk-form-section">
            <header className="wk-panel-head"><div><h2>Budget</h2><span>Hard spend and wall-clock limits for this request.</span></div><b className="wk-step">04</b></header>
            <div className="wk-form-grid wk-form-grid-2">
              <Field label="Maximum total USD" error={errors.maxUSD}><input type="number" min="0.01" step="0.01" inputMode="decimal" value={values.maxUSD} onChange={update("maxUSD")} /></Field>
              <Field label="Deadline" error={errors.deadline} hint="Duration: 30m, 1h, or 3600s."><input value={values.deadline} onChange={update("deadline")} autoComplete="off" /></Field>
            </div>
          </section>
          <details className="wk-advanced">
            <summary><span><b>Advanced placement</b><small>Optional backend and Linode region request</small></span><Icon name="caret" /></summary>
            <div className="wk-form-grid wk-form-grid-2">
              <Field label="Backend" error={errors.backend}>
                <select value={values.backend} onChange={update("backend")}>
                  <option value="auto">automatic</option><option value="linode">linode</option><option value="aws">aws</option>
                  {values.mode === "cpu" && <><option value="flyio">fly.io</option><option value="gcp">gcp</option><option value="azure">azure</option></>}
                </select>
              </Field>
              <Field label="Region" error={errors.region} hint={values.backend === "linode" ? "Optional Linode region." : "Choose Linode to request a region."}>
                <input value={values.region} onChange={update("region")} disabled={values.backend !== "linode"} placeholder="for example us-east" />
              </Field>
            </div>
          </details>
          <div className="wk-actions">
            <Link to="/workloads/launch" className="wk-btn wk-btn-secondary">Cancel</Link>
            <button type="submit" className="wk-btn wk-btn-primary" disabled={!targetSelectable || !!selectionError}>Review launch</button>
          </div>
        </form>
      )}
    </>
  );
}

// The canonical workload path from #95. Every step remains visible so a missing
// provider/Kubernetes fact is distinguishable from a step that never existed.
// A step is complete only when the workload record carries its own timestamp or
// an explicit receipt/state for that exact event.
const TIMELINE_STEPS = [
  { key: "submitted", label: "Submitted", detail: "Central accepted the tenant request.", times: ["created_at"] },
  { key: "quoted", label: "Quoted / placement selected", detail: "A provider placement or quote was recorded.", times: ["placement.decided_at", "placement.quoted_at", "placement.granted_at", "lifecycle.placed_at"], markers: ["placement.provider"] },
  { key: "capacity-requested", label: "Capacity requested", detail: "The provider capacity request was opened.", times: ["capacity.requested_at", "provider.requested_at", "lifecycle.capacity_requested_at"], markers: ["burst_id"] },
  { key: "provider-created", label: "Provider resource created", detail: "The paid provider resource was created.", times: ["provider.created_at", "provider_resource.created_at", "lifecycle.provider_created_at", "provider_created_at"], markers: ["provider.resource_id", "provider_resource.id"] },
  { key: "node-joined", label: "Node joined cluster", detail: "The provisioned node joined the selected cluster.", times: ["node.joined_at", "lifecycle.node_joined_at", "node_ready_at"] },
  { key: "gpu-healthy", label: "GPU healthy", detail: "The requested GPU became healthy on the node.", times: ["gpu.healthy_at", "lifecycle.gpu_healthy_at", "gpu_ready_at"] },
  { key: "pod-scheduled", label: "Pod scheduled", detail: "Kubernetes placed the workload pod.", times: ["pod.scheduled_at", "lifecycle.pod_scheduled_at", "pod_scheduled_at"] },
  { key: "running", label: "Running", detail: "The workload process started.", times: ["started_at"] },
  { key: "artifacts-uploading", label: "Artifacts uploading", detail: "Post-run artifact export began.", times: ["outcome.artifact_upload_started_at", "artifacts.upload_started_at", "artifact_upload_started_at"] },
  { key: "terminal", label: "Completed / failed / cancelled", detail: "The compute phase reached a terminal outcome.", times: ["finished_at"] },
  { key: "delete-requested", label: "Provider deletion requested", detail: "Teardown asked the provider to delete the resource.", times: ["provider_delete.requested_at", "cleanup.requested_at", "lifecycle.provider_delete_requested_at"] },
  { key: "provider-absent", label: "Provider absent", detail: "Yscale confirmed the provider resource no longer exists.", times: ["provider_delete.confirmed_at", "cleanup.confirmed_at", "lifecycle.cleanup_confirmed_at", "provider_absent_at"] },
  { key: "settled", label: "Settled", detail: "The cloud cost estimate reached its final state.", times: ["settled_at", "lifecycle.settled_at"] },
];

function readPath(source, path) {
  return path.split(".").reduce((value, key) => (value == null ? value : value[key]), source);
}

function reported(value) {
  return value !== undefined && value !== null && value !== "" && value !== false;
}

const DELETE_REQUESTED_STATES = new Set(["deleting", "retrying", "manual_attention", "deleted", "absent", "settled"]);

function deletionWasRequested(workload) {
  const value = workload?.provider_delete?.state || workload?.cleanup?.state || workload?.lifecycle?.cleanup_state;
  const state = typeof value === "string" ? value.trim().toLowerCase().replaceAll("-", "_") : "";
  return DELETE_REQUESTED_STATES.has(state);
}

function timelineSteps(workload) {
  return TIMELINE_STEPS.map((step) => {
    const time = step.times.map((path) => readPath(workload, path)).find((value) => typeof value === "string" && Number.isFinite(Date.parse(value))) || "";
    const marker = step.markers?.some((path) => reported(readPath(workload, path))) || (step.key === "delete-requested" && deletionWasRequested(workload));
    const done = !!time || marker || (step.key === "terminal" && workloadIsTerminal(workload));
    return {
      ...step,
      done,
      time,
      label: step.key === "terminal" && workloadIsTerminal(workload) ? statusLabel(workloadPhase(workload)) : step.label,
    };
  });
}

function Timeline({ workload }) {
  const steps = deriveTimelineSteps(workload);
  return (
    <ol className="wk-timeline">
      {steps.map((step, index) => {
        const className = step.status === "done" ? "is-done"
          : step.status === "current" ? "is-current"
          : step.status === "attention" ? "is-current is-attention"
          : step.status === "unresolved" ? "is-current is-unresolved"
          : "";
        const icon = step.status === "done" ? <Icon name="check" size={12} />
          : step.status === "attention" ? <Icon name="alert" size={12} />
          : index + 1;
        const timeText = step.status === "done" && !step.time ? "Time not recorded" : formatDate(step.time);
        return (
          <li key={`${step.label}-${index}`} className={className}
            {...(step.status === "attention" ? { "aria-label": `${step.label} — requires action` } : {})}
          >
            <i aria-hidden="true">{icon}</i>
            <div><strong>{step.label}</strong><time>{timeText}</time></div>
          </li>
        );
      })}
    </ol>
  );
}

// Where a reader goes next for each failure state. Every destination is a
// surface this console already has — a state whose recovery lives outside the
// console links to the record's own evidence rather than to a page that would
// have to be invented for it.
const FAILURE_RECOVERY = {
  no_connected_cluster: { to: "/workloads/clusters" },
  connector_offline: { to: "/workloads/clusters" },
  no_eligible_placement: { to: "/workloads/launch" },
  price_above_cap: { to: "/workloads/launch" },
  placement_changed: { to: "/workloads/launch" },
  node_join_failed: { to: "#lifecycle-title" },
  gpu_unhealthy: { to: "#logs-title" },
  pod_unschedulable: { to: "#logs-title" },
  provider_delete_retrying: { to: "#cleanup-title" },
  manual_attention: { to: "/workloads/audit" },
};

function FailureRecovery({ workload }) {
  const state = workloadFailureState(workload);
  if (!state) return null;
  // An in-page destination stays an ordinary fragment link: Link falls through
  // to native anchor behaviour for anything that is not an absolute path.
  const recovery = FAILURE_RECOVERY[state.code] || { to: "/workloads/clusters" };
  return (
    <section className={`wk-recovery${workloadNeedsAttention(workload) ? " is-attention" : ""}`} role="status" aria-labelledby="recovery-title">
      <span className="wk-recovery-icon" aria-hidden="true"><Icon name="alert" size={18} /></span>
      <div>
        <h2 id="recovery-title">{state.title}</h2>
        <p>{state.detail}</p>
        <code>{state.code}</code>
      </div>
      <Link to={recovery.to} className="wk-btn wk-btn-secondary">{state.action}<Icon name="chevron" size={14} /></Link>
    </section>
  );
}

function WorkloadLogsPanel({ state, onRefresh }) {
  const streams = state.data?.streams || [];
  return (
    <Panel
      id="logs-title"
      title="Logs"
      className="wk-logs-panel"
      meta={state.data?.observed_at ? `Snapshot ${formatDate(state.data.observed_at)}` : "On demand · no auto-polling"}
      action={(
        <button type="button" className="wk-btn wk-btn-secondary" onClick={onRefresh} disabled={state.status === "loading"} aria-busy={state.status === "loading"}>
          {state.status === "loading" ? "Refreshing…" : "Refresh logs"}
        </button>
      )}
    >
      <div className="wk-log-body" aria-live="polite" aria-busy={state.status === "loading"}>
        {state.status === "idle" && <p className="wk-log-empty">Load a bounded snapshot from the cluster connector when you need it.</p>}
        {state.status === "loading" && !state.data && <p className="wk-log-empty" role="status">Asking the workload's cluster for recent container output…</p>}
        {state.status === "error" && <p className="wk-inline-error" role="alert">{state.error?.message || "The log snapshot is unavailable."}</p>}
        {state.data?.fixture && <p className="wk-log-fixture">Dummy account preview · this output is fixture data, not a live cluster.</p>}
        {state.data && streams.length === 0 && <p className="wk-log-empty">No container output was returned. The Job may not have started, or its logs may no longer be available.</p>}
        {streams.map((stream) => (
          <article className="wk-log-stream" key={`${stream.pod}/${stream.container}`}>
            <header><b>{stream.container}</b><span>{stream.pod}</span></header>
            <pre tabIndex={0} aria-label={`${stream.container} logs from ${stream.pod}`}>{stream.output || "No output."}</pre>
          </article>
        ))}
        {state.data?.truncated && <p className="wk-log-truncated" role="status">Snapshot reached the 256 KiB safety limit. Narrow the output in the workload or use your cluster log system for the full record.</p>}
      </div>
    </Panel>
  );
}

function OutcomeChip({ tone, label }) {
  const toneClass = tone === "ok" ? "wk-chip-ok" : tone === "danger" ? "wk-chip-danger" : tone === "warn" ? "wk-chip-warn" : "";
  return <span className={`wk-chip ${toneClass}`.trim()}>{label}</span>;
}

function WorkloadDetail({ id, session, account, tenant, workloads, reloadWorkloads, navigate, catalog }) {
  const fromList = workloads.find((workload) => workload.id === id) || null;
  const [detail, setDetail] = useState(null);
  const [status, setStatus] = useState("loading");
  const [error, setError] = useState(null);
  const [confirming, setConfirming] = useState(false);
  const [confirmingRetry, setConfirmingRetry] = useState(false);
  const [cancelling, setCancelling] = useState(false);
  const [retrying, setRetrying] = useState(false);
  const [logs, setLogs] = useState({ status: "idle", data: null, error: null });
  const logRequest = useRef(null);
  const retryKeys = useRef(null);
  const retryTriggerRef = useRef(null);
  const retryDismissRef = useRef(null);
  if (!retryKeys.current) retryKeys.current = createIdempotencyKeys();

  const closeRetryConfirmation = useCallback(() => {
    setConfirmingRetry(false);
    window.requestAnimationFrame(() => retryTriggerRef.current?.focus());
  }, []);

  useEffect(() => {
    if (!confirmingRetry) return undefined;
    const focus = window.requestAnimationFrame(() => retryDismissRef.current?.focus());
    const onKeyDown = (event) => {
      if (event.key !== "Escape" || retrying) return;
      event.preventDefault();
      closeRetryConfirmation();
    };
    document.addEventListener("keydown", onKeyDown);
    return () => {
      window.cancelAnimationFrame(focus);
      document.removeEventListener("keydown", onKeyDown);
    };
  }, [closeRetryConfirmation, confirmingRetry, retrying]);

  const load = useCallback(() => {
    const controller = new AbortController();
    setStatus("loading");
    setError(null);
    fetchWorkload({ token: session.accessToken, tenantId: tenant.customer_id, workloadId: id, signal: controller.signal })
      .then((data) => { setDetail(data); setStatus("ready"); })
      .catch((err) => { if (err?.name !== "AbortError") { setError(err); setStatus("error"); } });
    return () => controller.abort();
  }, [id, session.accessToken, tenant.customer_id]);
  useEffect(load, [load]);

  // Keep the 30s poll through teardown and receipt repair, not just compute.
  // Transient errors retain the last snapshot; unmount aborts in-flight work.
  // Log snapshots remain on-demand.
  const pollDetail = shouldPollWorkload(detail);
  useEffect(() => {
    if (status !== "ready" || !pollDetail) return undefined;
    let controller = null;
    const timer = window.setInterval(() => {
      controller?.abort();
      controller = new AbortController();
      fetchWorkload({ token: session.accessToken, tenantId: tenant.customer_id, workloadId: id, signal: controller.signal })
        .then((data) => setDetail(data))
        .catch(() => {});
    }, 30_000);
    return () => {
      window.clearInterval(timer);
      controller?.abort();
    };
  }, [pollDetail, id, session.accessToken, status, tenant.customer_id]);
  useEffect(() => {
    logRequest.current?.abort();
    setLogs({ status: "idle", data: null, error: null });
    return () => logRequest.current?.abort();
  }, [id, tenant.customer_id]);
  const workload = mergeWorkloadDetail(fromList, detail);
  const canMutate = canMutateWorkload(tenant.role, account.account_id, workload);

  const cancel = async () => {
    setCancelling(true);
    setError(null);
    try {
      await cancelWorkload({ token: session.accessToken, tenantId: tenant.customer_id, workloadId: id });
      await reloadWorkloads();
      load();
      setConfirming(false);
      setCancelling(false);
    } catch (err) {
      setError(err);
      setCancelling(false);
    }
  };

  const retry = async () => {
    setRetrying(true);
    setError(null);
    try {
      const accepted = await retryWorkload({
        token: session.accessToken,
        tenantId: tenant.customer_id,
        workloadId: id,
        idempotencyKey: retryKeys.current.keyFor(`retry:${id}`, ""),
      });
      if (!accepted?.id) throw new Error("The retry was accepted without an id.");
      await reloadWorkloads().catch(() => {});
      navigate(`/workloads/${encodeURIComponent(accepted.id)}`);
    } catch (err) {
      setError(err);
      setRetrying(false);
    }
  };

  const refreshLogs = async () => {
    logRequest.current?.abort();
    const controller = new AbortController();
    logRequest.current = controller;
    setLogs((current) => ({ status: "loading", data: current.data, error: null }));
    try {
      const data = await fetchWorkloadLogs({
        token: session.accessToken,
        tenantId: tenant.customer_id,
        workloadId: id,
        tail: 200,
        signal: controller.signal,
      });
      if (logRequest.current === controller) setLogs({ status: "ready", data, error: null });
    } catch (err) {
      if (err?.name !== "AbortError" && logRequest.current === controller) {
        setLogs((current) => ({ status: "error", data: current.data, error: err }));
      }
    }
  };

  if (status === "loading" && !workload) return <LoadingState label="Loading workload detail…" rows={5} />;
  if (status === "error" && !workload) return <ErrorState error={error} onRetry={load} title="Workload detail is unavailable." />;
  if (!workload) return <ErrorState error={new Error("The tenant API returned no workload record.")} />;
  const spec = workload.spec?.spec || {};
  const receipt = templateReceipt(workload, catalog);
  const dataReceipt = workloadReceiptDetails(workload);
  const placement = workloadPlacement(workload);
  const cleanup = deriveCleanupPresentation(workload);
  const terminal = workloadIsTerminal(workload);
  const frozenCost = costObservation(workload);
  const showLiveCost = !frozenCost && !cleanup?.providerAbsentConfirmed && (!terminal || !!liveSpendObservation(workload));
  return (
    <>
      <PageHeader
        eyebrow={`${receipt.exact ? "launched from" : "shape suggests"} ${receipt.id} · ${workload.id}`}
        title={workloadName(workload)}
        copy={receipt.title}
        action={<button type="button" className="wk-text-link" onClick={() => navigate("/workloads")}>Back to workloads</button>}
      />
      <div className="wk-detail-bar">
        <WorkloadStatus status={workloadPhase(workload)} />
        <span>Requested {formatDate(workload.created_at)}</span>
        <span className="wk-detail-elapsed">{terminal ? "Ran" : "Running"} {workloadElapsed(workload)}</span>
        <span className="wk-chip wk-detail-gpu">{workloadGPU(workload).label}</span>
        {!terminal && canMutate && !confirming && (
          <button type="button" className="wk-btn wk-btn-secondary" onClick={() => setConfirming(true)}>Cancel workload</button>
        )}
        {terminal && canMutate && !confirmingRetry && (
          <button ref={retryTriggerRef} type="button" className="wk-btn wk-btn-primary" onClick={() => setConfirmingRetry(true)}>Retry as new run</button>
        )}
        {!canMutate && <span className="wk-chip">viewer · read only</span>}
      </div>
      {confirming && (
        <div className="wk-confirm" role="alertdialog" aria-labelledby="cancel-title">
          <div>
            <b id="cancel-title">Cancel this workload?</b>
            {/* Cancelling starts teardown; it does not end the run's cost. The
                prompt has to say so, because a reader who believes the charge
                stopped at this click will not come back to check. */}
            <span>
              Central stops the workload and begins tearing down its capacity. Teardown stays open until Yscale declares the provider
              resource absent and the run settles — the cloud cost estimate can still change after this.
            </span>
          </div>
          <div>
            <button type="button" className="wk-btn wk-btn-secondary" onClick={() => setConfirming(false)} disabled={cancelling}>Keep running</button>
            <button type="button" className="wk-btn wk-btn-danger" onClick={cancel} disabled={cancelling} aria-busy={cancelling}>{cancelling ? "Cancelling…" : "Confirm cancel"}</button>
          </div>
        </div>
      )}
      {confirmingRetry && (
        <div className="wk-confirm" role="alertdialog" aria-labelledby="retry-title">
          <div><b id="retry-title">Retry this as a new run?</b><span>Central submits a new workload that may incur cloud provider costs under current tenant policy and capacity limits.</span></div>
          <div>
            <button ref={retryDismissRef} type="button" className="wk-btn wk-btn-secondary" onClick={closeRetryConfirmation} disabled={retrying}>Keep record</button>
            <button type="button" className="wk-btn wk-btn-primary" onClick={retry} disabled={retrying} aria-busy={retrying}>{retrying ? "Starting…" : "Start new run"}</button>
          </div>
        </div>
      )}
      {error && <p className="wk-inline-error" role="alert">{error.message}</p>}
      <FailureRecovery workload={workload} />
      <div className="wk-detail-grid">
        <Panel id="lifecycle-title" title="Lifecycle" meta="submitted through settled"><Timeline workload={workload} /></Panel>
        <Panel id="gpu-title" title="GPU and placement" meta="what central granted">
          <DetailList rows={[
            { label: "Compute", value: workloadGPU(workload).label },
            ...placementReceiptRows(workload),
            { label: "Provider", value: placement.provider },
            { label: "Region", value: placement.region },
            { label: "SKU", value: placement.sku },
            { label: "Requested backend", value: spec.backend || "automatic" },
            { label: "Requested region", value: spec.region || spec.machine?.region || "provider default" },
            { label: "Burst receipt", value: workload.burst_id || "not assigned yet" },
          ]} />
          <Note>Provider, region, and SKU are what central recorded for this run. A value it never reported reads "not reported" rather than falling back to what the request asked for.</Note>
        </Panel>
        <Panel id="cleanup-title" title="Cleanup" meta={cleanup?.tone === "danger" ? "needs attention" : cleanup?.tone === "warn" ? "unconfirmed" : null}>
          <DetailList rows={cleanupDetailRows(workload)} />
          {cleanup?.message && <Note tone={cleanup.tone}>{cleanup.message}</Note>}
          <Note>
            A run is only finished for cost when Yscale has declared its provider resource absent and the run has settled. Until then the
            estimate on this record can still change.
          </Note>
        </Panel>
        <Panel id="request-title" title="Request">
          <DetailList rows={[
            { label: "Image", value: spec.nodeOnly ? "not applicable · node only" : spec.image || "not reported" },
            { label: "Size", value: spec.size || [spec.cpu, spec.memory].filter(Boolean).join(" / ") || "not reported" },
            { label: "Compute", value: spec.gpu ? `${spec.gpu.kind || "GPU"} × ${spec.gpu.count || 1} · ${spec.gpu.reliability || "reliable"}` : "CPU" },
            ...executionRecipeRows(spec),
            { label: "Budget", value: spec.budget ? `${formatUSD(spec.budget.maxUSD)} / ${spec.budget.deadline || "no deadline reported"}` : "not reported" },
          ]} />
        </Panel>
        {/* Provenance is what central recorded, not what this console can work
            out. A record without a template block gets the inferred reading and
            says so, so the two are never read as the same claim. */}
        <Panel id="template-title" title="Template" meta={receipt.exact ? "recorded provenance" : "inferred"}>
          <DetailList rows={[
            { label: "Template", value: receipt.title },
            { label: "Catalog id", value: receipt.id },
            { label: "Version", value: receipt.exact ? (receipt.version ? `v${receipt.version}` : "not recorded") : "not recorded" },
            { label: "Catalog revision", value: receipt.exact && receipt.catalogRevision ? receipt.catalogRevision : "not recorded" },
          ]} />
          <Note tone={receipt.exact ? "plain" : "warn"}>{receipt.note}</Note>
        </Panel>
        {showLiveCost && (
          <Panel id="live-title" title={terminal ? "Accrued spend during cleanup" : "Live spend and telemetry"}>
            <DetailList rows={liveDetailRows(workload)} />
            <Note>
              Accrued spend and GPU utilization are what central observed on this
              tenant's live burst, refreshed every 30 seconds. Nothing here is
              used to derive spend from the reader's browser clock; a paused or
              absent telemetry signal reads as "not reported" rather than as $0.00 or 0%.
              Compute completion does not stop provider cost. The estimate stays
              live until provider absence is confirmed, then a frozen teardown
              estimate is reported separately.
            </Note>
          </Panel>
        )}
        {!showLiveCost && (
          <Panel id="cost-title" title="Teardown estimate">
            <DetailList rows={costDetailRows(workload)} />
            <Note>
              {frozenCost
                ? "Central's recorded capacity estimate, with its measurement and freeze basis shown above. It does not move once written."
                : "Central has not recorded a frozen teardown estimate. The cloud cost is still unknown."}
            </Note>
          </Panel>
        )}
        {(spec.data || workload.outcome) && (
          <Panel id="data-title" title="Data receipt">
            <DetailList rows={[
              { label: "Object inputs", value: dataReceipt.requestedInputs },
              { label: "Input limit", value: dataReceipt.inputLimit },
              { label: "Object outputs", value: dataReceipt.requestedOutputs },
              { label: "Upload limit", value: dataReceipt.uploadLimit },
              { label: "Compute result", value: <OutcomeChip tone={dataReceipt.computeBadgeTone} label={dataReceipt.computeLabel} /> },
              { label: "Artifact export", value: <OutcomeChip tone={dataReceipt.artifactBadgeTone} label={dataReceipt.artifactLabel} /> },
              ...(dataReceipt.hasArtifactOutcome ? [
                { label: "Uploaded objects", value: dataReceipt.objectsUploaded !== null ? String(dataReceipt.objectsUploaded) : "not reported" },
                { label: "Uploaded bytes", value: formatBytes(dataReceipt.bytesUploaded) },
              ] : []),
            ]} />
            <Note>Central reports transfer classes, capacity, and observed outcome only. Bucket paths, endpoints, and Kubernetes Secret references stay out of the tenant workload response.</Note>
          </Panel>
        )}
        <Panel id="record-title" title="Record">
          <DetailList rows={[
            { label: "Created", value: formatDate(workload.created_at) },
            { label: "Started", value: formatDate(workload.started_at) },
            { label: "Finished", value: formatDate(workload.finished_at) },
            ...(workload.retry_of ? [{ label: "Retry of", value: <Link to={`/workloads/${encodeURIComponent(workload.retry_of)}`} className="wk-text-link">{workload.retry_of}</Link> }] : []),
            { label: "Id", value: workload.id },
          ]} />
        </Panel>
        <WorkloadLogsPanel state={logs} onRefresh={refreshLogs} />
      </div>
    </>
  );
}

function hostedWait(requestedAt, now = Date.now()) {
  const minutes = Math.max(0, Math.floor((now - Date.parse(requestedAt)) / 60_000));
  if (minutes < 60) return `${minutes}m`;
  const hours = Math.floor(minutes / 60);
  if (hours < 48) return `${hours}h ${minutes % 60}m`;
  return `${Math.floor(hours / 24)}d ${hours % 24}h`;
}

function HostedCredentialReveal({ assignment, onClose, returnFocus }) {
  const dialogRef = useRef(null);
  useEffect(() => {
    const opener = returnFocus instanceof HTMLElement ? returnFocus : document.activeElement;
    const previousOverflow = document.body.style.overflow;
    document.body.style.overflow = "hidden";
    const frame = window.requestAnimationFrame(() => dialogRef.current?.querySelector(".wk-copy-btn")?.focus());
    return () => {
      window.cancelAnimationFrame(frame);
      document.body.style.overflow = previousOverflow;
      if (opener instanceof HTMLElement && document.contains(opener)) opener.focus();
    };
  }, []);
  const onKeyDown = (event) => {
    if (event.key === "Escape") { event.preventDefault(); onClose(); return; }
    if (event.key !== "Tab") return;
    const focusable = [...dialogRef.current.querySelectorAll('button:not([disabled]), [tabindex]:not([tabindex="-1"])')];
    const first = focusable[0];
    const last = focusable.at(-1);
    if (event.shiftKey && document.activeElement === first) { event.preventDefault(); last?.focus(); }
    else if (!event.shiftKey && document.activeElement === last) { event.preventDefault(); first?.focus(); }
  };
  return (
    <div className="wk-reveal-layer">
      <div className="wk-reveal-scrim" aria-hidden="true" />
      <div ref={dialogRef} className="wk-reveal wk-operator-reveal" role="dialog" aria-modal="true" aria-labelledby="hosted-credential-title" onKeyDown={onKeyDown}>
        <header className="wk-reveal-head">
          <div>
            <h2 id="hosted-credential-title">Hosted cluster assigned</h2>
            <p>{assignment.cluster.name} · <span className="wk-mono">{assignment.cluster.clusterId}</span></p>
          </div>
          <button type="button" className="wk-icon-btn" onClick={onClose} aria-label="Close and discard the shown credential"><Icon name="close" /></button>
        </header>
        <p className="wk-reveal-once"><Icon name="alert" size={15} />This handoff is held only in memory and shown once. Copy both values now; closing discards them.</p>
        <dl className="wk-operator-assigned-meta">
          <div><dt>Namespace</dt><dd className="wk-mono">{assignment.cluster.hostedNamespace}</dd></div>
          <div><dt>Helm release</dt><dd className="wk-mono">{assignment.helmRelease}</dd></div>
          <div><dt>RBAC set</dt><dd className="wk-mono">{assignment.connectorRbacSet}</dd></div>
        </dl>
        <CopyBlock label="Connector token" name="shown once" language="text" text={assignment.connectorToken} />
        <CopyBlock label="Helm command" name="run in hosted cluster" language="sh" text={assignment.helmCommand} />
        <footer className="wk-reveal-foot">
          <span>No URL, browser storage, logs, or analytics receive these values.</span>
          <button type="button" className="wk-btn wk-btn-primary" onClick={onClose}>Done, I copied them</button>
        </footer>
      </div>
    </div>
  );
}

function HostedAssignmentReview({ request, session, onCancel, onAssigned, returnFocus }) {
  const [draft, setDraft] = useState(() => assignmentDraft());
  const [posting, setPosting] = useState(false);
  const [error, setError] = useState(null);
  const dialogRef = useRef(null);
  const finalRef = useRef(null);
  const controllerRef = useRef(null);
  useEffect(() => {
    const previousOverflow = document.body.style.overflow;
    document.body.style.overflow = "hidden";
    const frame = window.requestAnimationFrame(() => finalRef.current?.focus());
    return () => {
      window.cancelAnimationFrame(frame);
      controllerRef.current?.abort();
      document.body.style.overflow = previousOverflow;
      if (returnFocus instanceof HTMLElement && document.contains(returnFocus)) returnFocus.focus();
    };
  }, []);
  const close = () => { if (!posting) onCancel(); };
  const onKeyDown = (event) => {
    if (event.key === "Escape") { event.preventDefault(); close(); return; }
    if (event.key !== "Tab") return;
    const focusable = [...dialogRef.current.querySelectorAll('button:not([disabled]), input:not([disabled]), summary, [tabindex]:not([tabindex="-1"])')];
    const first = focusable[0];
    const last = focusable.at(-1);
    if (event.shiftKey && document.activeElement === first) { event.preventDefault(); last?.focus(); }
    else if (!event.shiftKey && document.activeElement === last) { event.preventDefault(); first?.focus(); }
  };
  const submit = async (event) => {
    event.preventDefault();
    if (posting) return;
    setPosting(true);
    setError(null);
    const controller = new AbortController();
    controllerRef.current = controller;
    try {
      const assignment = await assignHostedCluster({ token: session.accessToken, tenantId: request.tenantId, draft, signal: controller.signal });
      onAssigned(request, assignment, returnFocus);
    } catch (caught) {
      if (caught?.name !== "AbortError") setError(caught);
      setPosting(false);
    }
  };
  return (
    <div className="wk-reveal-layer">
      <div className="wk-reveal-scrim" aria-hidden="true" />
      <form ref={dialogRef} className="wk-reveal wk-operator-review" role="dialog" aria-modal="true" aria-labelledby="hosted-review-title" onKeyDown={onKeyDown} onSubmit={submit} noValidate>
        <header className="wk-reveal-head">
          <div><h2 id="hosted-review-title">Review hosted assignment</h2><p>{request.tenantName} · <span className="wk-mono">{request.tenantId}</span></p></div>
          <button type="button" className="wk-icon-btn" onClick={close} disabled={posting} aria-label="Close assignment review"><Icon name="close" /></button>
        </header>
        <dl className="wk-operator-review-facts">
          <div><dt>Plan</dt><dd>{request.plan}</dd></div>
          <div><dt>Waiting</dt><dd>{hostedWait(request.requestedAt)}</dd></div>
          <div><dt>Defaults</dt><dd>Central generates the cluster id, name, and reserved namespace.</dd></div>
        </dl>
        <details className="wk-operator-advanced" open={draft.advanced} onToggle={(event) => setDraft((value) => ({ ...value, advanced: event.currentTarget.open }))}>
          <summary>Advanced identifiers</summary>
          <p>Leave any field blank to let central generate it.</p>
          <div className="wk-form-grid-2">
            <Field label="Cluster id"><input value={draft.clusterId} onChange={(event) => setDraft((value) => ({ ...value, clusterId: event.target.value }))} autoComplete="off" spellCheck="false" /></Field>
            <Field label="Display name"><input value={draft.name} onChange={(event) => setDraft((value) => ({ ...value, name: event.target.value }))} autoComplete="off" /></Field>
            <Field label="Namespace"><input value={draft.namespace} onChange={(event) => setDraft((value) => ({ ...value, namespace: event.target.value }))} autoComplete="off" spellCheck="false" /></Field>
          </div>
        </details>
        {error && <p className="wk-inline-error" role="alert">{error.status === 409 ? "This request is no longer assignable. Refresh the queue to see its current state." : error.message}</p>}
        <footer className="wk-operator-review-actions">
          <button type="button" className="wk-btn wk-btn-secondary" onClick={close} disabled={posting}>Cancel</button>
          <button ref={finalRef} type="submit" className="wk-btn wk-btn-primary" disabled={posting} aria-busy={posting}>{posting ? "Assigning…" : "Assign hosted cluster"}</button>
        </footer>
      </form>
    </div>
  );
}

function HostedFleetConfirmation({ mode, row, session, onCancel, onRotated, onRevoked, returnFocus }) {
  const [posting, setPosting] = useState(false);
  const [error, setError] = useState(null);
  const dialogRef = useRef(null);
  const confirmRef = useRef(null);
  const controllerRef = useRef(null);
  const destructive = mode === "revoke";
  const release = `yscale-agent-${row.cluster.clusterId}`;
  useEffect(() => {
    const previousOverflow = document.body.style.overflow;
    document.body.style.overflow = "hidden";
    const frame = window.requestAnimationFrame(() => confirmRef.current?.focus());
    return () => {
      window.cancelAnimationFrame(frame);
      controllerRef.current?.abort();
      document.body.style.overflow = previousOverflow;
      if (returnFocus instanceof HTMLElement && document.contains(returnFocus)) returnFocus.focus();
    };
  }, []);
  const close = () => { if (!posting) onCancel(); };
  const onKeyDown = (event) => {
    if (event.key === "Escape") { event.preventDefault(); close(); return; }
    if (event.key !== "Tab") return;
    const focusable = [...dialogRef.current.querySelectorAll("button:not([disabled])")];
    const first = focusable[0];
    const last = focusable.at(-1);
    if (event.shiftKey && document.activeElement === first) { event.preventDefault(); last?.focus(); }
    else if (!event.shiftKey && document.activeElement === last) { event.preventDefault(); first?.focus(); }
  };
  const submit = async () => {
    if (posting) return;
    setPosting(true);
    setError(null);
    const controller = new AbortController();
    controllerRef.current = controller;
    try {
      if (destructive) {
        const confirmation = await revokeHostedCluster({ token: session.accessToken, tenantId: row.tenantId, clusterId: row.cluster.clusterId, signal: controller.signal });
        onRevoked(row, confirmation, returnFocus);
      } else {
        const assignment = await rotateHostedClusterCredential({ token: session.accessToken, tenantId: row.tenantId, clusterId: row.cluster.clusterId, signal: controller.signal });
        onRotated(assignment, returnFocus);
      }
    } catch (caught) {
      if (caught?.name !== "AbortError") setError(caught);
      setPosting(false);
    }
  };
  return (
    <div className="wk-reveal-layer">
      <div className="wk-reveal-scrim" aria-hidden="true" />
      <div ref={dialogRef} className="wk-reveal wk-operator-confirm" role="alertdialog" aria-modal="true" aria-labelledby="hosted-fleet-confirm-title" onKeyDown={onKeyDown}>
        <header className="wk-reveal-head">
          <div>
            <h2 id="hosted-fleet-confirm-title">{destructive ? "Revoke hosted assignment?" : "Rotate connector credential?"}</h2>
            <p>{row.tenantName} · <span className="wk-mono">{row.cluster.clusterId}</span></p>
          </div>
          <button type="button" className="wk-icon-btn" onClick={close} disabled={posting} aria-label="Close confirmation"><Icon name="close" /></button>
        </header>
        <dl className="wk-operator-review-facts">
          <div><dt>Tenant</dt><dd>{row.tenantId}</dd></div>
          <div><dt>Namespace</dt><dd>{row.cluster.hostedNamespace}</dd></div>
          <div><dt>{destructive ? "Helm follow-up" : "Effect"}</dt><dd>{destructive ? release : "Current connector credential is replaced."}</dd></div>
        </dl>
        <p className={destructive ? "wk-inline-error" : "wk-inline-note"}>
          {destructive
            ? "Central removes the hosted assignment only. It does not uninstall the Helm release from the hosted cluster."
            : "The replacement token is shown once. The existing assignment and inventory row remain in place."}
        </p>
        {error && <p className="wk-inline-error" role="alert">{error.message}</p>}
        <footer className="wk-operator-review-actions">
          <button type="button" className="wk-btn wk-btn-secondary" onClick={close} disabled={posting}>Cancel</button>
          <button ref={confirmRef} type="button" className={`wk-btn ${destructive ? "wk-btn-danger" : "wk-btn-primary"}`} onClick={submit} disabled={posting} aria-busy={posting}>
            {posting ? (destructive ? "Revoking…" : "Rotating…") : (destructive ? "Revoke assignment" : "Rotate credential")}
          </button>
        </footer>
      </div>
    </div>
  );
}

function HostedFleetTable({ clusters, onAction }) {
  return (
    <>
      <div className="wk-table-wrap wk-table-wrap-hosted-fleet"><table className="wk-table wk-table-hosted-fleet">
        <thead><tr><th scope="col">Tenant</th><th scope="col">Cluster</th><th scope="col">Namespace</th><th scope="col">State</th><th scope="col">Registered</th><th scope="col"><span className="sr-only">Actions</span></th></tr></thead>
        <tbody>{clusters.map((row) => <tr key={`${row.tenantId}:${row.cluster.clusterId}`}>
          <th scope="row"><span>{row.tenantName}<small>{row.tenantId} · {row.plan}</small></span></th>
          <td><span className="wk-receipt">{row.cluster.name}<small>{row.cluster.clusterId}</small></span></td>
          <td className="wk-mono">{row.cluster.hostedNamespace}</td>
          <td><span className="wk-chip">{row.cluster.state}</span></td>
          <td><time dateTime={row.cluster.registeredAt}>{formatDate(row.cluster.registeredAt)}</time></td>
          <td><div className="wk-hosted-fleet-actions"><button type="button" className="wk-text-link" onClick={(event) => onAction("rotate", row, event)}>Rotate credential</button><button type="button" className="wk-text-link wk-text-danger" onClick={(event) => onAction("revoke", row, event)}>Revoke</button></div></td>
        </tr>)}</tbody>
      </table></div>
      <ul className="wk-hosted-fleet-cards">{clusters.map((row) => <li key={`${row.tenantId}:${row.cluster.clusterId}`}>
        <div className="wk-hosted-fleet-card-head"><div><b>{row.cluster.name}</b><small>{row.cluster.clusterId}</small></div><span className="wk-chip">{row.cluster.state}</span></div>
        <p>{row.tenantName} · {row.tenantId} · {row.plan}</p>
        <dl><div><dt>Namespace</dt><dd>{row.cluster.hostedNamespace}</dd></div><div><dt>Registered</dt><dd>{formatDate(row.cluster.registeredAt)}</dd></div></dl>
        <div className="wk-hosted-fleet-actions"><button type="button" className="wk-btn wk-btn-secondary" onClick={(event) => onAction("rotate", row, event)}>Rotate credential</button><button type="button" className="wk-btn wk-btn-danger" onClick={(event) => onAction("revoke", row, event)}>Revoke</button></div>
      </li>)}</ul>
    </>
  );
}

function HostedRequestsPage({ session, operatorQueue, reloadHostedRequests, onAssigned, onRevoked }) {
  const [selected, setSelected] = useState(null);
  const [fleetAction, setFleetAction] = useState(null);
  const [reveal, setReveal] = useState(null);
  const [followUp, setFollowUp] = useState(null);
  const [returnFocus, setReturnFocus] = useState(null);
  const refreshRef = useRef(null);
  const followUpRef = useRef(null);
  useEffect(() => {
    if (!followUp) return undefined;
    const frame = window.requestAnimationFrame(() => followUpRef.current?.focus());
    return () => window.cancelAnimationFrame(frame);
  }, [followUp]);
  const choose = (request, event) => { setReturnFocus(event.currentTarget); setSelected(request); };
  const assigned = (request, assignment, opener) => {
    setSelected(null);
    onAssigned(request, assignment);
    setReturnFocus(opener);
    setReveal(assignment);
  };
  const chooseFleetAction = (mode, row, event) => {
    setReturnFocus(event.currentTarget);
    setFleetAction({ mode, row });
  };
  const rotated = (assignment, opener) => {
    setFleetAction(null);
    setReturnFocus(opener);
    setReveal(assignment);
  };
  const revoked = (row, confirmation, opener) => {
    setFleetAction(null);
    onRevoked(row.tenantId, row.cluster.clusterId);
    setReturnFocus(opener);
    setFollowUp({
      tenantName: row.tenantName,
      clusterId: confirmation.clusterId,
      namespace: confirmation.hostedNamespace || row.cluster.hostedNamespace,
      command: hostedUninstallCommand(confirmation.clusterId),
    });
  };
  return (
    <>
      <PageHeader
        eyebrow="Platform operations"
        title="Hosted capacity"
        copy="Assign pending requests and maintain the platform-managed cluster fleet from one operator-only surface."
        action={<button ref={refreshRef} type="button" className="wk-btn wk-btn-secondary" onClick={() => reloadHostedRequests()} disabled={operatorQueue.status === "loading"}><Icon name="refresh" />Refresh</button>}
      />
      {operatorQueue.status === "loading" && <LoadingState label="Reading hosted capacity requests and active assignments…" rows={6} />}
      {operatorQueue.status === "error" && <ErrorState title="Hosted capacity is unavailable." error={operatorQueue.error} onRetry={() => reloadHostedRequests()} />}
      {operatorQueue.status === "ready" && !operatorQueue.requests.length && (
        <Panel title="Pending requests" meta="0 waiting"><div className="wk-empty"><span className="wk-empty-icon" aria-hidden="true"><Icon name="check" /></span><div><h3>Queue is clear</h3><p>No tenant is waiting for hosted capacity.</p></div></div></Panel>
      )}
      {operatorQueue.status === "ready" && operatorQueue.requests.length > 0 && (
        <Panel title="Pending requests" meta={`${operatorQueue.requests.length} waiting · oldest first`} className="wk-panel-flush">
          <div className="wk-table-wrap wk-table-wrap-hosted"><table className="wk-table wk-table-hosted">
            <thead><tr><th scope="col">Tenant</th><th scope="col">Plan</th><th scope="col">Requested</th><th scope="col">Wait</th><th scope="col"><span className="sr-only">Action</span></th></tr></thead>
            <tbody>{operatorQueue.requests.map((request) => <tr key={request.tenantId}>
              <th scope="row"><span>{request.tenantName}<small>{request.tenantId}</small></span></th>
              <td>{request.plan}</td><td><time dateTime={request.requestedAt}>{formatDate(request.requestedAt)}</time></td><td className="wk-mono">{hostedWait(request.requestedAt)}</td>
              <td><button type="button" className="wk-btn wk-btn-secondary" onClick={(event) => choose(request, event)}>Review</button></td>
            </tr>)}</tbody>
          </table></div>
          <ul className="wk-hosted-cards">{operatorQueue.requests.map((request) => <li key={request.tenantId}>
            <div><b>{request.tenantName}</b><small>{request.tenantId}</small></div><span className="wk-chip">{request.plan}</span>
            <p>Requested {formatDate(request.requestedAt)} · waiting {hostedWait(request.requestedAt)}</p>
            <button type="button" className="wk-btn wk-btn-secondary" onClick={(event) => choose(request, event)}>Review assignment</button>
          </li>)}</ul>
        </Panel>
      )}
      {operatorQueue.status === "ready" && !operatorQueue.clusters.length && (
        <Panel title="Active assignments" meta="0 hosted clusters"><div className="wk-empty"><span className="wk-empty-icon" aria-hidden="true"><Icon name="clusters" /></span><div><h3>No active assignments</h3><p>Assigned hosted clusters appear here with their reserved namespace and connector state.</p></div></div></Panel>
      )}
      {operatorQueue.status === "ready" && operatorQueue.clusters.length > 0 && (
        <Panel title="Active assignments" meta={`${operatorQueue.clusters.length} hosted cluster${operatorQueue.clusters.length === 1 ? "" : "s"}`} className="wk-panel-flush">
          <HostedFleetTable clusters={operatorQueue.clusters} onAction={chooseFleetAction} />
        </Panel>
      )}
      {followUp && (
        <Panel title="Cluster follow-up" meta={`${followUp.tenantName} · ${followUp.clusterId}`}>
          <p className="wk-inline-note" role="status">The hosted assignment was revoked. Central did not uninstall Helm from the shared cluster. Run this exact command there.{followUp.namespace ? ` The released tenant namespace was ${followUp.namespace}.` : ""}</p>
          <CopyBlock label="Helm uninstall command" name="operator follow-up" language="sh" text={followUp.command} />
          <div className="wk-operator-followup-actions"><button ref={followUpRef} type="button" className="wk-btn wk-btn-secondary" onClick={() => { setFollowUp(null); window.requestAnimationFrame(() => refreshRef.current?.focus()); }}>Dismiss follow-up</button></div>
        </Panel>
      )}
      {selected && <HostedAssignmentReview request={selected} session={session} onCancel={() => setSelected(null)} onAssigned={assigned} returnFocus={returnFocus} />}
      {fleetAction && <HostedFleetConfirmation mode={fleetAction.mode} row={fleetAction.row} session={session} onCancel={() => setFleetAction(null)} onRotated={rotated} onRevoked={revoked} returnFocus={returnFocus} />}
      {reveal && <HostedCredentialReveal assignment={reveal} onClose={() => setReveal(null)} returnFocus={returnFocus} />}
    </>
  );
}

function HostedRequestsRoute(context) {
  if (context.operatorAccess.status === "loading" || context.operatorAccess.status === "idle") return <LoadingState label="Checking operator access…" rows={4} />;
  if (context.operatorAccess.status !== "available") {
    return <>
      <ForbiddenState title="Hosted operations unavailable" detail="This signed-in account does not have access to platform operator tools." />
      <Link to="/workloads" className="wk-btn wk-btn-secondary">Return to overview</Link>
    </>;
  }
  return <HostedRequestsPage {...context} />;
}

function OperatorTenantsRoute(context) {
  if (context.operatorAccess.status === "loading" || context.operatorAccess.status === "idle") return <LoadingState label="Checking operator access…" rows={4} />;
  if (context.operatorAccess.status !== "available") {
    return <>
      <ForbiddenState title="Tenant guardrails unavailable" detail="This signed-in account does not have access to platform operator tools." />
      <Link to="/workloads" className="wk-btn wk-btn-secondary">Return to overview</Link>
    </>;
  }
  return <OperatorTenantsPage {...context} />;
}

function OperatorLanding() {
  return <>
    <PageHeader
      eyebrow="Platform operations"
      title="Operator console"
      copy="This account can manage platform-wide hosted capacity without joining a customer tenant."
    />
    <Panel title="Hosted capacity">
      <div className="wk-empty">
        <span className="wk-empty-icon" aria-hidden="true"><Icon name="clusters" /></span>
        <div><h3>Review the assignment queue</h3><p>Assign shared Yscale capacity to tenants that have requested a hosted cluster.</p></div>
        <Link to="/workloads/hosted-requests" className="wk-btn wk-btn-primary">Open hosted requests</Link>
      </div>
    </Panel>
  </>;
}

function routeContent(path, context) {
  const route = matchConsoleRoute(path);
  const key = routeInstanceKey(route, context.tenant?.customer_id);
  switch (route.name) {
    case "overview": return <WorkloadsHome key={key} {...context} />;
    // The pre-#95 history URL still resolves for existing bookmarks. Workloads
    // is the active-first history surface now, so both render the same page.
    case "history": return <WorkloadsHome key={key} {...context} />;
    case "launch": return <LaunchPage key={key} {...context} />;
    case "data": return <DataPage key={key} {...context} />;
    case "settings": return <SettingsPage key={key} {...context} />;
    case "templates": return <TemplatesPage key={key} {...context} />;
    case "clusters": return <ClustersPage key={key} {...context} />;
    case "policies": return <PoliciesPage key={key} {...context} />;
    case "gitops": return <GitOpsPage key={key} {...context} />;
    case "usage": return <UsagePage key={key} {...context} />;
    case "team": return <TeamPage key={key} {...context} />;
    case "audit": return <AuditPage key={key} {...context} />;
    case "account": return <AccountPage key={key} account={context.account} tenant={context.tenant} />;
    case "hosted-requests": return <HostedRequestsRoute key={key} {...context} />;
    case "operator-tenants": return <OperatorTenantsRoute key={key} {...context} />;
    case "new": return <WorkloadFormPage key={key} templateId={route.templateId} {...context} />;
    case "detail": return <WorkloadDetail key={key} id={route.workloadId} {...context} />;
    default:
      return <ErrorState title="Console route not found." error={new Error("Use the console navigation to choose a workload page.")} />;
  }
}

export function Workloads({ path }) {
  const [, navigate] = useRoute();
  const [session, setSession] = useState(() => getSession());
  const [account, setAccount] = useState(null);
  const [accountStatus, setAccountStatus] = useState(() => session ? "loading" : "anonymous");
  const [accountError, setAccountError] = useState(null);
  const [expired, setExpired] = useState(false);
  const [activeId, setActiveId] = useState(null);
  // The records and the tenant they were read for move as one value. Clusters,
  // GitOps, Usage, Overview, History, and detail all render from this, so a
  // switch must not leave the previous tenant's rows under the new tenant's
  // header for even one frame — and no effect has to have run for that to hold.
  const [page, setPage] = useState(EMPTY_PAGE);
  const [clusterPage, setClusterPage] = useState(EMPTY_CLUSTERS);
  const [catalogPage, setCatalogPage] = useState(EMPTY_CATALOG);
  const [operatorAccess, setOperatorAccess] = useState({ status: "idle", warning: "" });
  const [operatorQueue, setOperatorQueue] = useState({ status: "idle", requests: [], clusters: [], error: null });

  useEffect(() => {
    document.body.classList.add("work-console-body");
    return () => document.body.classList.remove("work-console-body");
  }, []);

  const endSession = useCallback((wasExpired = false) => {
    signOut(); setSession(null); setAccount(null); setActiveId(null); setPage(EMPTY_PAGE); setClusterPage(EMPTY_CLUSTERS); setCatalogPage(EMPTY_CATALOG); setOperatorAccess({ status: "idle", warning: "" }); setOperatorQueue({ status: "idle", requests: [], clusters: [], error: null }); setAccountStatus("anonymous"); setExpired(wasExpired);
  }, []);

  useEffect(() => {
    if (!session) return;
    const controller = new AbortController();
    setAccountStatus("loading");
    setAccountError(null);
    fetchAccount({ token: session.accessToken, signal: controller.signal })
      .then((data) => { setAccount(data); setActiveId(data?.tenants?.[0]?.customer_id || null); setAccountStatus("ready"); })
      .catch((error) => {
        if (error?.name === "AbortError") return;
        if (error instanceof ApiError && error.isExpired) { endSession(true); return; }
        setAccountError(error); setAccountStatus("error");
      });
    return () => controller.abort();
  }, [session, endSession]);

  const reloadHostedRequests = useCallback(async (signal, probing = false) => {
    if (!session || accountStatus !== "ready") return;
    if (probing) setOperatorAccess({ status: "loading", warning: "" });
    setOperatorQueue({ status: "loading", requests: [], clusters: [], error: null });
    try {
      const [queue, inventory] = await Promise.all([
        fetchHostedRequests({ token: session.accessToken, signal }),
        fetchHostedClusters({ token: session.accessToken, signal }),
      ]);
      setOperatorAccess({ status: "available", warning: "" });
      setOperatorQueue({ status: "ready", requests: queue.requests, clusters: inventory.clusters, error: null });
    } catch (error) {
      if (error?.name === "AbortError") return;
      const decision = operatorCapability(error);
      if (decision.expired) { endSession(true); return; }
      if (probing || !decision.available && (error.status === 403 || error.status === 404)) {
        setOperatorAccess({ status: "unavailable", warning: decision.warning });
      }
      setOperatorQueue({ status: "error", requests: [], clusters: [], error });
    }
  }, [session, accountStatus, endSession]);

  // A tenant membership role is deliberately insufficient. Only the normal
  // Yscale ID session's operator endpoint can reveal the Operations surface,
  // and it is not probed until account resolution has completed.
  useEffect(() => {
    if (!session || accountStatus !== "ready") return undefined;
    const controller = new AbortController();
    reloadHostedRequests(controller.signal, true);
    return () => controller.abort();
  }, [session, accountStatus, reloadHostedRequests]);

  useEffect(() => {
    if (!session) return;
    const tick = () => { if (!getSession()) endSession(true); };
    const timer = window.setInterval(tick, 20_000);
    return () => window.clearInterval(timer);
  }, [session, endSession]);

  const tenant = account?.tenants?.find((item) => item.customer_id === activeId) || account?.tenants?.[0] || null;
  const tenantId = tenant?.customer_id || null;
  // A page read for another tenant is not this tenant's page, however it got
  // here — a switch mid-flight, or a reload a section started before it
  // unmounted. Reading it back through the active tenant is what makes stale
  // records unrenderable rather than merely unlikely.
  const current = page.tenantId === tenantId ? page : { ...EMPTY_PAGE, tenantId, status: "loading" };
  const currentClusters = clusterPage.tenantId === tenantId ? clusterPage : { ...EMPTY_CLUSTERS, tenantId, status: "loading" };
  const currentCatalog = catalogPage.tenantId === tenantId ? catalogPage : { ...EMPTY_CATALOG, tenantId, status: "loading" };
  // What this tenant can actually start — nothing at all until its catalog has
  // been read. The built-in shapes are this console's own defaults, not this
  // tenant's approved entries, so standing them in while the read is in flight
  // would offer launches the catalog may never carry.
  const launchTemplates = useMemo(
    () => (currentCatalog.record ? currentCatalog.record.templates.filter((template) => !template.disabled) : []),
    [currentCatalog.record],
  );

  const reloadWorkloads = useCallback(async (signal) => {
    if (!session || !tenant) return;
    const readFor = tenant.customer_id;
    setPage({ tenantId: readFor, status: "loading", workloads: [], error: null });
    try {
      const data = await fetchWorkloads({ token: session.accessToken, tenantId: readFor, signal });
      setPage({ tenantId: readFor, status: "ready", workloads: Array.isArray(data?.workloads) ? data.workloads : [], error: null });
    } catch (error) {
      if (error?.name === "AbortError") return;
      if (error instanceof ApiError && error.isExpired) { endSession(true); return; }
      setPage({ tenantId: readFor, status: "error", workloads: [], error });
      throw error;
    }
  }, [session, tenant, endSession]);

  useEffect(() => {
    if (!tenant) return;
    const controller = new AbortController();
    reloadWorkloads(controller.signal).catch(() => {});
    return () => controller.abort();
  }, [tenant?.customer_id, reloadWorkloads]);

  // The observation is read for one tenant and stamped with it, so a switch
  // mid-flight cannot land tenant A's clusters in tenant B's launch form: the
  // abort covers the common case and the tenant stamp covers the rest.
  const reloadClusters = useCallback(async (signal) => {
    if (!session || !tenant) return;
    const readFor = tenant.customer_id;
    setClusterPage({ ...EMPTY_CLUSTERS, tenantId: readFor, status: "loading" });
    try {
      const observation = await fetchClusters({ token: session.accessToken, tenantId: readFor, signal });
      setClusterPage({
        tenantId: readFor,
        status: "ready",
        clusters: observation.clusters,
        observedAt: observation.observedAt,
        livePartial: observation.livePartial,
        policy: observation.policy,
        error: null,
      });
    } catch (error) {
      if (error?.name === "AbortError") return;
      if (error instanceof ApiError && error.isExpired) { endSession(true); return; }
      setClusterPage({ ...EMPTY_CLUSTERS, tenantId: readFor, status: "error", error });
    }
  }, [session, tenant, endSession]);

  useEffect(() => {
    if (!tenant) return;
    const controller = new AbortController();
    reloadClusters(controller.signal);
    return () => controller.abort();
  }, [tenant?.customer_id, reloadClusters]);

  // The catalog is read once per tenant and stamped with it, for the same
  // reason the observation is: a launch form composing from tenant A's entries
  // under tenant B's header is a request nobody in this tenant approved.
  const reloadCatalog = useCallback(async (signal) => {
    if (!session || !tenant) return;
    const readFor = tenant.customer_id;
    setCatalogPage({ ...EMPTY_CATALOG, tenantId: readFor, status: "loading" });
    try {
      const record = await fetchTemplateCatalog({ token: session.accessToken, tenantId: readFor, signal });
      setCatalogPage({ tenantId: readFor, status: "ready", record, error: null });
    } catch (error) {
      if (error?.name === "AbortError") return;
      if (error instanceof ApiError && error.isExpired) { endSession(true); return; }
      setCatalogPage({ ...EMPTY_CATALOG, tenantId: readFor, status: "error", error });
    }
  }, [session, tenant, endSession]);

  useEffect(() => {
    if (!tenant) return;
    const controller = new AbortController();
    reloadCatalog(controller.signal);
    return () => controller.abort();
  }, [tenant?.customer_id, reloadCatalog]);

  const startPreview = (mode) => {
    const preview = beginDevPreview(mode);
    if (preview) {
      setSession(preview);
      setExpired(false);
    }
  };

  const onTenantCreated = (envelope, created) => {
    setAccount(envelope);
    setActiveId(created?.customer_id || envelope?.tenants?.[0]?.customer_id || null);
  };

  const recordHostedAssignment = (request, assignment) => setOperatorQueue((currentQueue) => ({
    ...currentQueue,
    requests: currentQueue.requests.filter((queued) => queued.tenantId !== request.tenantId),
    clusters: [
      ...currentQueue.clusters.filter((row) => row.tenantId !== request.tenantId || row.cluster.clusterId !== assignment.cluster.clusterId),
      { tenantId: request.tenantId, tenantName: request.tenantName, plan: request.plan, cluster: assignment.cluster },
    ].sort((left, right) => left.tenantId.localeCompare(right.tenantId) || left.cluster.clusterId.localeCompare(right.cluster.clusterId)),
  }));

  const recordHostedRevocation = (revokedTenantId, revokedClusterId) => setOperatorQueue((currentQueue) => ({
    ...currentQueue,
    clusters: currentQueue.clusters.filter((row) => row.tenantId !== revokedTenantId || row.cluster.clusterId !== revokedClusterId),
  }));

  if (accountStatus === "anonymous") return <SignedOut expired={expired} onPreview={startPreview} />;
  if (accountStatus === "loading") return <main id="main-content" className="work-auth-page" tabIndex={-1}><div className="wk-auth-card"><LoadingState label="Resolving your account and tenants…" rows={4} /></div></main>;
  if (accountStatus === "error") return <main id="main-content" className="work-auth-page" tabIndex={-1}><div className="wk-auth-card"><ErrorState error={accountError} onRetry={() => setSession(getSession())} title="Account context is unavailable." /></div></main>;
  if (!tenant && (operatorAccess.status === "idle" || operatorAccess.status === "loading")) {
    return <main id="main-content" className="work-auth-page" tabIndex={-1}><div className="wk-auth-card"><LoadingState label="Checking platform access…" rows={4} /></div></main>;
  }
  if (!tenant && operatorAccess.status === "available") {
    const operatorContext = {
      account,
      tenant: null,
      session,
      operatorAccess,
      operatorQueue,
      reloadHostedRequests: () => reloadHostedRequests(),
      onAssigned: recordHostedAssignment,
      onRevoked: recordHostedRevocation,
      onSessionExpired: () => endSession(true),
      onOperatorUnavailable: () => setOperatorAccess({ status: "unavailable", warning: "" }),
    };
    const operatorRoute = matchConsoleRoute(path);
    return (
      <DeveloperShell
        path={path}
        account={account}
        activeTenant={null}
        setActiveId={setActiveId}
        onSignOut={() => endSession(false)}
        workloads={[]}
        workloadStatus="ready"
        launchTemplates={[]}
        navigate={navigate}
        operatorAvailable
        operatorWarning=""
      >
        {operatorRoute.name === "hosted-requests"
          ? <HostedRequestsRoute {...operatorContext} />
          : operatorRoute.name === "operator-tenants"
            ? <OperatorTenantsRoute {...operatorContext} />
            : <OperatorLanding />}
      </DeveloperShell>
    );
  }
  if (!tenant && ["hosted-requests", "operator-tenants"].includes(matchConsoleRoute(path).name)) {
    return (
      <main id="main-content" className="work-auth-page" tabIndex={-1}>
        <div className="wk-auth-card"><ForbiddenState title="Platform operations unavailable" detail="This signed-in account does not have access to platform operator tools." /></div>
      </main>
    );
  }
  if (!tenant) {
    return (
      <main id="main-content" className="work-auth-page" tabIndex={-1}>
        <div className="wk-auth-card wk-onboarding-card">
          <TenantOnboarding account={account} token={session.accessToken} onCreated={onTenantCreated} navigate={navigate} />
        </div>
      </main>
    );
  }

  const context = {
    account,
    tenant,
    session,
    launchClusterId: parseLaunchClusterId(typeof window !== "undefined" ? window.location.search : ""),
    navigate,
    workloads: current.workloads,
    workloadStatus: current.status,
    workloadError: current.error,
    retry: () => reloadWorkloads().catch(() => {}),
    reloadWorkloads,
    clusters: currentClusters.clusters,
    clusterStatus: currentClusters.status,
    clusterError: currentClusters.error,
    clusterObservedAt: currentClusters.observedAt,
    clustersLivePartial: currentClusters.livePartial,
    clusterPolicy: currentClusters.policy,
    retryClusters: () => { reloadClusters(); },
    catalog: currentCatalog.record,
    catalogStatus: currentCatalog.status,
    catalogError: currentCatalog.error,
    retryCatalog: () => { reloadCatalog(); },
    // A save answers with the catalog central now serves, so the console adopts
    // that answer rather than re-reading and briefly showing the old revision.
    setCatalog: (record) => setCatalogPage({ tenantId, status: "ready", record, error: null }),
    launchTemplates,
    operatorAccess,
    operatorQueue,
    reloadHostedRequests: () => reloadHostedRequests(),
    onAssigned: recordHostedAssignment,
    onRevoked: recordHostedRevocation,
    onSessionExpired: () => endSession(true),
    onOperatorUnavailable: () => setOperatorAccess({ status: "unavailable", warning: "" }),
  };
  return (
    <DeveloperShell
      path={path}
      account={account}
      activeTenant={tenant}
      setActiveId={setActiveId}
      onSignOut={() => endSession(false)}
      workloads={current.workloads}
      workloadStatus={current.status}
      launchTemplates={launchTemplates}
      navigate={navigate}
      operatorAvailable={operatorAccess.status === "available"}
      operatorWarning={operatorAccess.warning}
    >
      {routeContent(path, context)}
    </DeveloperShell>
  );
}
