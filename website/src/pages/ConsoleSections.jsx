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
  statusLabel,
} from "../components/ConsoleKit.jsx";
import { addMember, fetchMembers, fetchUsage, removeMember, updateMemberRole } from "../lib/accountApi.js";
import { fetchAudit } from "../lib/auditApi.js";
import { fetchBilling, formatMicroUSD } from "../lib/billingApi.js";
import { deleteCluster, registerCluster, rotateClusterCredential } from "../lib/clusterApi.js";
import { deleteLinodeCloudAccount, fetchLinodeCloudAccount, putLinodeCloudAccount } from "../lib/cloudAccountApi.js";
import { fetchClusterPolicy, normalizeClusterPolicyDraft, putClusterPolicy } from "../lib/clusterPolicy.js";
import {
  catalogPublisherName,
  createCatalogPublisher,
  deleteCatalogPublisher,
  fetchCatalogPublishers,
  rotateCatalogPublisherCredential,
} from "../lib/catalogPublisherApi.js";
import {
  fetchHostedCapacity,
  hostedCapacityCardState,
  requestHostedCapacity,
} from "../lib/hostedCapacity.js";
import {
  CLUSTER_OBSERVATION_NOTE,
  CLUSTER_PARTIAL_NOTE,
  clusterInventorySummary,
  clusterInventoryTotals,
  clusterStateLabel,
  clusterUnavailableReason,
  hostedCluster,
  isConnectedHere,
  isHostedCluster,
  launchableClusters,
  registerDraftError,
} from "../lib/clusters.js";
import {
  CONTROLLER_TRIGGERED_LIMIT,
  burstTargets,
  controllerTriggeredWorkloads,
  lastActivityAt,
  limitRows,
  provenanceSummary,
  summarizeRuns,
  usageByMonth,
  usageByTemplate,
  workloadNamespaces,
} from "../lib/consoleData.js";
import {
  GITOPS_RECONCILERS,
  fetchGitOpsSources,
  gitOpsConflict,
  gitOpsDraftChanged,
  gitOpsDraftErrors,
  gitOpsDraftRow,
  putGitOpsSources,
  reconcilerLabel,
} from "../lib/gitOpsApi.js";
import { canManageTenant, canMutateWorkloads } from "../lib/roles.js";
import { Link } from "../lib/router.jsx";
import { WORKLOAD_TEMPLATES, initialWorkloadForm, workloadYAML } from "../lib/workloadTemplates.js";
import { launchTemplatesPath } from "../lib/launchIntent.js";
import { canInspectRawWorkload } from "../lib/workloadLaunchPolicy.js";

// The account sections the console shows beyond launching and reading workloads.
// None of them invents a capability: where there is no endpoint, the page says
// so and shows what the tenant's own records already prove.

const ROLE_MEANINGS = [
  { role: "owner", can: "Everything an admin can do, and only an owner may remove another owner.", limit: "A tenant's last owner cannot be removed." },
  { role: "admin", can: "Manage the roster and read the audit journal. Submits and cancels workloads.", limit: "Cannot remove an owner." },
  { role: "member", can: "Submit and cancel workloads in the tenant's authorized namespaces.", limit: "No roster or audit access." },
  { role: "viewer", can: "Read workloads, templates, and usage.", limit: "Submissions are refused as read_only." },
];

// A page request — "load more members", "load older events" — outlives the
// click that started it, and the tenant it was asked for can change while it is
// still in flight. The reload effect owns the controller, so a tenant or token
// change aborts that request instead of letting the previous tenant's rows
// land on this one.
function usePageRequest() {
  const controller = useRef(null);
  const begin = useCallback(() => {
    controller.current?.abort();
    controller.current = new AbortController();
    return controller.current.signal;
  }, []);
  const cancel = useCallback(() => {
    controller.current?.abort();
    controller.current = null;
  }, []);
  return [begin, cancel];
}

function limitValue(row) {
  if (row.unlimited) return "No ceiling";
  return row.unit === "usd" ? formatUSD(row.value) : String(row.value);
}

function TenantHeader({ tenant, title, copy, action }) {
  return <PageHeader eyebrow={tenant.customer_id} title={title} copy={copy} action={action} />;
}

export function BillingPage({ tenant, session }) {
  const [state, setState] = useState({ status: "loading", data: null, error: null });
  const load = useCallback(() => {
    const controller = new AbortController();
    setState((current) => ({ ...current, status: "loading", error: null }));
    fetchBilling({ token: session.accessToken, tenantId: tenant.customer_id, signal: controller.signal })
      .then((data) => setState({ status: "ready", data, error: null }))
      .catch((error) => { if (error?.name !== "AbortError") setState({ status: "error", data: null, error }); });
    return () => controller.abort();
  }, [session.accessToken, tenant.customer_id]);
  useEffect(load, [load]);
  const billing = state.data;
  return <>
    <TenantHeader tenant={tenant} title="Billing" copy="Tenant credit available for workloads, including money temporarily reserved by active runs." />
    {state.status === "loading" && <LoadingState label="Reading billing summary…" rows={4} />}
    {state.status === "error" && <ErrorState title="Billing is unavailable." error={state.error} onRetry={load} />}
    {billing && <>
      <StatTiles ready label="Billing summary" tiles={[
        { key: "balance", icon: "spend", label: "Balance", value: formatMicroUSD(billing.balanceMicroUsd), note: "total tenant credit" },
        { key: "spendable", icon: "check", label: "Spendable", value: formatMicroUSD(billing.spendableMicroUsd), note: "available for new holds" },
        { key: "held", icon: "clock", label: "Held", value: formatMicroUSD(billing.heldMicroUsd), note: "reserved by active work" },
        { key: "debt", icon: "policies", label: "Debt", value: formatMicroUSD(billing.debtMicroUsd), note: billing.debtMicroUsd ? "must be resolved before new work" : "none reported" },
      ]} />
      {billing.frozen && <Note tone="warn"><b>Billing is frozen.</b> New billable workloads may be refused. Existing workload records remain readable.</Note>}
      <Panel title="Active holds" meta={`${billing.openHolds.length} open`} className="wk-panel-flush">
        {billing.openHolds.length === 0 ? <div className="wk-empty"><span className="wk-empty-icon" aria-hidden="true"><Icon name="check" /></span><div><h3>No active holds</h3><p>No workload credit is currently reserved.</p></div></div> :
          <ul className="wk-billing-holds">{billing.openHolds.map((hold) => <li key={hold.id}>
            <div><Link className="wk-text-link" to={`/workloads/${encodeURIComponent(hold.workloadId)}`}>{hold.workloadId}</Link><small>Hold {hold.id}</small></div>
            <strong>{formatMicroUSD(hold.amountMicroUsd)}</strong>
            <span>Expires {formatDate(hold.expiresAt)}</span>
          </li>)}</ul>}
      </Panel>
      <Note>{billing.updatedAt ? `Summary updated ${formatDate(billing.updatedAt)}.` : "No summary timestamp was reported."} Holds are temporary reservations, not charges.</Note>
    </>}
  </>;
}

/* ---------------------------------------------------------------- clusters */

const AGENT_VERIFY = `kubectl -n yscale-system get pods
kubectl -n yscale-system logs -l app.kubernetes.io/name=yscale-agent -c agent --tail=20
kubectl get crd workloads.yscale.sh`;

function clusterStateChip(cluster) {
  if (isConnectedHere(cluster)) return "wk-chip wk-chip-ok";
  return cluster.state === "disconnected" ? "wk-chip wk-chip-warn" : "wk-chip";
}

// The one lifecycle fact worth a line under the state label.
function clusterStateDetail(cluster) {
  if (isConnectedHere(cluster)) return cluster.connectedAt ? `since ${formatDate(cluster.connectedAt, false)}` : "";
  if (cluster.state === "disconnected") {
    if (cluster.lastDisconnectedAt) return `since ${formatDate(cluster.lastDisconnectedAt, false)}`;
    return cluster.lastConnectedAt ? `last connected ${formatDate(cluster.lastConnectedAt, false)}` : "";
  }
  return "awaiting its first connection";
}

function clusterLiveSignal(cluster) {
  const parts = [];
  if (isConnectedHere(cluster)) {
    parts.push(cluster.connections === null ? "connections not reported" : `${cluster.connections} connection${cluster.connections === 1 ? "" : "s"}`);
    if (cluster.agentVersion) parts.push(`agent ${cluster.agentVersion}`);
    if (cluster.lastSeen) parts.push(`seen ${formatDate(cluster.lastSeen)}`);
  } else {
    parts.push("none on this replica");
  }
  const inventory = clusterInventorySummary(cluster);
  if (inventory.text) parts.push(inventory.text);
  if (inventory.newestObservedAt) parts.push(`inventory ${formatDate(inventory.newestObservedAt)}`);
  return parts.join(" · ");
}

function clusterMobileSignal(cluster) {
  const state = isConnectedHere(cluster) ? "" : clusterStateDetail(cluster);
  return [state, clusterLiveSignal(cluster)].filter(Boolean).join(" · ");
}

function LinodeCloudAccountCard({ tenant, token }) {
  const tenantId = tenant.customer_id;
  const canManage = canManageTenant(tenant.role);
  const [status, setStatus] = useState("loading");
  const [record, setRecord] = useState(null);
  const [error, setError] = useState(null);
  const [notice, setNotice] = useState("");
  const [busy, setBusy] = useState("");
  const [reload, setReload] = useState(0);
  const [region, setRegion] = useState("us-east");
  const [cpuImage, setCPUImage] = useState("");
  const [gpuImage, setGPUImage] = useState("");
  const [confirmDisconnect, setConfirmDisconnect] = useState(false);
  const tokenInputRef = useRef(null);

  useEffect(() => {
    const controller = new AbortController();
    let current = true;
    setStatus("loading");
    setRecord(null);
    setError(null);
    setNotice("");
    setBusy("");
    setConfirmDisconnect(false);
    fetchLinodeCloudAccount({ token, tenantId, signal: controller.signal })
      .then((next) => {
        if (!current) return;
        setRecord(next);
        setRegion(next.account?.region || "us-east");
        setStatus("ready");
      })
      .catch((nextError) => {
        if (!current || nextError?.name === "AbortError") return;
        setError(nextError);
        setStatus("error");
      });
    return () => { current = false; controller.abort(); };
  }, [token, tenantId, reload]);

  const submit = async (event) => {
    event.preventDefault();
    if (busy || !canManage) return;
    // Read once from the native password control, then erase the DOM value
    // before validation or network work. Provider credentials never enter
    // React state and therefore can never be rendered back into this tree.
    const providerToken = tokenInputRef.current?.value || "";
    if (tokenInputRef.current) tokenInputRef.current.value = "";
    const cleanRegion = region.trim();
    if (!providerToken || !cleanRegion) {
      setError(new Error("A Linode token and region are required. The token field was cleared; paste it again to retry."));
      return;
    }
    setBusy("save");
    setError(null);
    setNotice("");
    try {
      const next = await putLinodeCloudAccount({
        token, tenantId, providerToken, region: cleanRegion,
        cpuImage: cpuImage.trim(), gpuImage: gpuImage.trim(),
      });
      setRecord(next);
      setRegion(next.account?.region || cleanRegion);
      setNotice(record?.account ? "Linode credential rotated. The connected account identity is unchanged." : "Linode account connected. The credential remains write-only.");
      setStatus("ready");
    } catch (nextError) {
      setError(nextError);
    } finally {
      setBusy("");
    }
  };

  const disconnect = async () => {
    if (busy || !canManage || !record?.account) return;
    setBusy("delete");
    setError(null);
    setNotice("");
    try {
      const next = await deleteLinodeCloudAccount({ token, tenantId });
      setRecord(next);
      setConfirmDisconnect(false);
      setNotice("Linode account disconnected. Existing cluster registrations remain unchanged.");
    } catch (nextError) {
      // A 409 deliberately leaves the account row, form, and confirmation in
      // place so the manager can clear the named live work and try again.
      setError(nextError);
    } finally {
      setBusy("");
    }
  };

  const account = record?.account;
  const mode = account ? "Rotate credential" : "Connect Linode";
  return (
    <article className="wk-cloud-account-card">
      <header>
        <span>BYOC cloud account</span>
        <b>Linode</b>
        <small>{status === "loading" ? "checking status" : status === "error" ? "status unavailable" : account ? "connected" : "not connected"}</small>
      </header>
      {status === "loading" && <p role="status">Reading this tenant's Linode connection…</p>}
      {status === "error" && (
        <div className="wk-cloud-error">
          <p className="wk-inline-error" role="alert">{apiMessage(error)}</p>
          <button type="button" className="wk-text-link" onClick={() => setReload((value) => value + 1)}>Retry status</button>
        </div>
      )}
      {status === "ready" && (
        <>
          <dl className="wk-cloud-facts">
            <div><dt>Account</dt><dd>{account ? account.providerAccountId : "Not connected"}</dd></div>
            <div><dt>Region</dt><dd>{account?.region || "Choose on connect"}</dd></div>
            <div><dt>Updated</dt><dd>{account ? formatDate(account.updatedAt) : "—"}</dd></div>
          </dl>
          <div className="wk-cloud-capabilities" aria-label="Linode image capabilities">
            <span className={account?.cpuImageReady ? "is-ready" : ""}><Icon name={account?.cpuImageReady ? "check" : "clock"} size={14} />CPU image {account?.cpuImageReady ? "ready" : "not ready"}</span>
            <span className={account?.gpuImageReady ? "is-ready" : ""}><Icon name={account?.gpuImageReady ? "check" : "clock"} size={14} />GPU image {account?.gpuImageReady ? "ready" : "not ready"}</span>
          </div>
          {!canManage && (
            <p role="note">Your {tenant.role} role can read this status. Ask a tenant owner or admin to {account ? "rotate or disconnect" : "connect"} the Linode account.</p>
          )}
          {canManage && (
            <form className="wk-cloud-form" onSubmit={submit} noValidate>
              <p>{account
                ? "Rotate with a token for this same Linode account identity. A token from another account is refused."
                : "Connect one tenant-wide Linode account. Workloads use it automatically for Linode routes."}</p>
              <label className="wk-field">
                <span>Personal access token</span>
                <input ref={tokenInputRef} type="password" autoComplete="new-password" name="linode-write-only-token" maxLength={4096} disabled={!!busy} aria-describedby="linode-token-note" />
                <small id="linode-token-note">Write-only. Cleared as soon as submission begins and never shown again.</small>
              </label>
              <label className="wk-field">
                <span>Region</span>
                <input value={region} onChange={(event) => setRegion(event.target.value)} autoComplete="off" spellCheck="false" maxLength={128} placeholder="us-east" disabled={!!busy} />
              </label>
              <details className="wk-cloud-advanced">
                <summary>Advanced compatibility</summary>
                <p>CPU can use a public base image. GPU requires an account-accessible or shared Yscale image until automatic image sharing lands.</p>
                <label className="wk-field">
                  <span>CPU image id <small>optional</small></span>
                  <input value={cpuImage} onChange={(event) => setCPUImage(event.target.value)} autoComplete="off" spellCheck="false" maxLength={512} placeholder="linode/ubuntu24.04" disabled={!!busy} />
                </label>
                <label className="wk-field">
                  <span>GPU image id <small>optional</small></span>
                  <input value={gpuImage} onChange={(event) => setGPUImage(event.target.value)} autoComplete="off" spellCheck="false" maxLength={512} placeholder="private/yscale-gpu" disabled={!!busy} />
                </label>
              </details>
              {error && <p className="wk-inline-error" role="alert">{apiMessage(error)}</p>}
              <div className="wk-cloud-actions">
                <button type="submit" className="wk-btn wk-btn-primary" disabled={!!busy} aria-busy={busy === "save"}>{busy === "save" ? "Saving…" : mode}</button>
                {account && <button type="button" className="wk-text-link wk-text-danger" disabled={!!busy} aria-expanded={confirmDisconnect} onClick={() => { setConfirmDisconnect((value) => !value); setError(null); }}>Disconnect</button>}
              </div>
              {confirmDisconnect && account && (
                <div className="wk-cloud-confirm" role="alertdialog" aria-labelledby="disconnect-linode-title">
                  <b id="disconnect-linode-title">Disconnect this tenant's Linode account?</b>
                  <p>New Linode bursts stop. A live burst or lease blocks this action until it is cleared.</p>
                  <div>
                    <button type="button" className="wk-btn wk-btn-secondary" onClick={() => { setConfirmDisconnect(false); setError(null); }} disabled={!!busy}>Keep connected</button>
                    <button type="button" className="wk-btn wk-btn-danger" onClick={disconnect} disabled={!!busy} aria-busy={busy === "delete"}>{busy === "delete" ? "Disconnecting…" : "Confirm disconnect"}</button>
                  </div>
                </div>
              )}
            </form>
          )}
          {notice && <p className="wk-cloud-notice" role="status" aria-live="polite">{notice}</p>}
        </>
      )}
    </article>
  );
}

function fleetInventoryValue(inventory) {
  if (!inventory.totalRows) return "0 registered rows";
  if (!inventory.nodeRows && !inventory.podRows) return "no inventory reported";
  return [
    inventory.nodeRows ? `${inventory.nodes} node${inventory.nodes === 1 ? "" : "s"}` : "nodes not reported",
    inventory.nodeRows ? `${inventory.burstNodes} burst node${inventory.burstNodes === 1 ? "" : "s"}` : "burst nodes not reported",
    inventory.podRows ? `${inventory.pendingPods} pending pod${inventory.pendingPods === 1 ? "" : "s"}` : "pending pods not reported",
  ].join(" · ");
}

function fleetInventoryCoverage(inventory, observed) {
  if (!observed) return "cluster fleet has not been read";
  const parts = [];
  parts.push(`node scope ${inventory.nodeRows}/${inventory.totalRows} rows`);
  parts.push(`pod scope ${inventory.podRows}/${inventory.totalRows} rows`);
  if (inventory.newestObservedAt) parts.push(`newest ${formatDate(inventory.newestObservedAt)}`);
  return parts.join(" · ");
}

// The reveal-once handoff for a freshly minted connector credential. The token
// exists only in the parent's React state and this dialog's DOM — never in
// storage, the URL, or the fleet rows — so dismissal is explicit: Escape or the
// buttons, never a stray scrim click that would discard it by accident.
function CredentialRevealDialog({ credential, onClose, returnFocus }) {
  const dialogRef = useRef(null);
  const closeRef = useRef(null);
  useEffect(() => {
    const opener = returnFocus instanceof HTMLElement ? returnFocus : document.activeElement;
    const previousOverflow = document.body.style.overflow;
    document.body.style.overflow = "hidden";
    // Initial focus lands on the first copy action — the one thing the reader
    // came here to do — and returns to whatever opened the dialog on close.
    const frame = window.requestAnimationFrame(() => {
      (dialogRef.current?.querySelector(".wk-copy-btn") || closeRef.current)?.focus();
    });
    return () => {
      window.cancelAnimationFrame(frame);
      document.body.style.overflow = previousOverflow;
      if (opener instanceof HTMLElement && document.contains(opener)) opener.focus();
    };
  }, []);
  const onKeyDown = (event) => {
    if (event.key === "Escape") {
      event.preventDefault();
      onClose();
      return;
    }
    if (event.key !== "Tab") return;
    const focusable = [...dialogRef.current.querySelectorAll('button:not([disabled]), [href], [tabindex]:not([tabindex="-1"])')];
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
  const rotated = credential.kind === "rotate";
  return (
    <div className="wk-reveal-layer">
      <div className="wk-reveal-scrim" aria-hidden="true" />
      <div ref={dialogRef} className="wk-reveal" role="dialog" aria-modal="true" aria-labelledby="credential-title" onKeyDown={onKeyDown}>
        <header className="wk-reveal-head">
          <div>
            <h2 id="credential-title">{rotated ? "New connector credential" : "Cluster registered"}</h2>
            <p>{credential.cluster.name} · <span className="wk-mono">{credential.cluster.id}</span></p>
          </div>
          <button ref={closeRef} type="button" className="wk-icon-btn" onClick={onClose} aria-label="Close and discard the shown credential"><Icon name="close" /></button>
        </header>
        <p className="wk-reveal-once"><Icon name="alert" size={15} />This token is shown once. Copy it now — closing this dialog discards it, and only rotating mints another.</p>
        {rotated && <p className="wk-reveal-note">The previous credential is revoked and its connector is disconnected. Run the upgrade command so it reconnects with this one.</p>}
        <CopyBlock label="Connector token" name="shown once" language="text" text={credential.connectorToken} />
        <CopyBlock label={rotated ? "Helm upgrade command" : "Helm install command"} name="run in your cluster" language="sh" text={credential.helmCommand} />
        <footer className="wk-reveal-foot">
          <span>This console keeps no copy.</span>
          <button type="button" className="wk-btn wk-btn-primary" onClick={onClose}>Done, I copied it</button>
        </footer>
      </div>
    </div>
  );
}

export function ClustersPage({
  tenant,
  session,
  workloads,
  workloadStatus,
  workloadError,
  retry,
  clusters,
  clusterStatus,
  clusterError,
  clusterObservedAt,
  clustersLivePartial,
  clusterPolicy,
  retryClusters,
}) {
  const canManage = canManageTenant(tenant.role);
  const token = session.accessToken;
  const tenantId = tenant.customer_id;
  const targets = useMemo(() => burstTargets(workloads), [workloads]);
  const ready = workloadStatus === "ready";
  const observed = clusterStatus === "ready";
  const lastAt = useMemo(() => lastActivityAt(workloads), [workloads]);
  const inventory = useMemo(() => clusterInventoryTotals(clusters), [clusters]);
  const connectedCount = clusters.filter(isConnectedHere).length;
  const launchableCount = launchableClusters(clusters).length;
  const hosted = hostedCluster(clusters);
  // Empty string means the hosted row is a launch target right now, in the
  // same words the launch selector would use when it is not.
  const hostedReason = hosted ? clusterUnavailableReason(hosted) : "";

  // The register form. The display name is the whole primary flow; the custom
  // id hides behind the advanced toggle because nobody needs it to succeed.
  const [draftName, setDraftName] = useState("");
  const [draftId, setDraftId] = useState("");
  const [advancedOpen, setAdvancedOpen] = useState(false);
  const [registerError, setRegisterError] = useState(null);
  const registerNameInputRef = useRef(null);
  // One write at a time: every rotate, delete, and register control disables
  // while any of them is pending, so a double click cannot mint two credentials.
  const [busy, setBusy] = useState(null);
  // The one-time credential handoff. Plaintext lives here and nowhere else;
  // closing the dialog or leaving this page is the end of the console's copy.
  const [credential, setCredential] = useState(null);
  const [pendingRotate, setPendingRotate] = useState(null);
  const [pendingDelete, setPendingDelete] = useState(null);
  const [actionError, setActionError] = useState(null);
  const credentialOpenerRef = useRef(null);
  const hostedPostController = useRef(null);
  const [hostedCapacityStatus, setHostedCapacityStatus] = useState("loading");
  const [hostedCapacityRecord, setHostedCapacityRecord] = useState(null);
  const [hostedCapacityError, setHostedCapacityError] = useState(null);
  const [hostedCapacityReload, setHostedCapacityReload] = useState(0);
  const [hostedCapacityPosting, setHostedCapacityPosting] = useState(false);
  const [hostedCapacityNotice, setHostedCapacityNotice] = useState("");

  useEffect(() => {
    const controller = new AbortController();
    let current = true;
    hostedPostController.current?.abort();
    hostedPostController.current = null;
    setHostedCapacityStatus("loading");
    setHostedCapacityRecord(null);
    setHostedCapacityError(null);
    setHostedCapacityPosting(false);
    setHostedCapacityNotice("");
    fetchHostedCapacity({ token, tenantId, signal: controller.signal })
      .then((record) => {
        if (!current) return;
        setHostedCapacityRecord(record);
        setHostedCapacityStatus("ready");
      })
      .catch((error) => {
        if (!current || error?.name === "AbortError") return;
        setHostedCapacityError(error);
        setHostedCapacityStatus("error");
      });
    return () => {
      current = false;
      controller.abort();
      hostedPostController.current?.abort();
      hostedPostController.current = null;
    };
  }, [token, tenantId, hostedCapacityReload]);

  const requestSharedCapacity = async () => {
    if (hostedPostController.current || hostedCapacityPosting || hostedCapacityRecord?.status !== "not_requested" || !canManage) return;
    const controller = new AbortController();
    hostedPostController.current?.abort();
    hostedPostController.current = controller;
    setHostedCapacityPosting(true);
    setHostedCapacityError(null);
    setHostedCapacityNotice("");
    try {
      const record = await requestHostedCapacity({ token, tenantId, signal: controller.signal });
      if (hostedPostController.current !== controller) return;
      setHostedCapacityRecord(record);
      setHostedCapacityStatus("ready");
      setHostedCapacityNotice("Shared capacity request recorded. Only the queue state changed.");
    } catch (error) {
      if (hostedPostController.current !== controller || error?.name === "AbortError") return;
      setHostedCapacityError(error);
      setHostedCapacityNotice("");
    } finally {
      if (hostedPostController.current === controller) {
        hostedPostController.current = null;
        setHostedCapacityPosting(false);
      }
    }
  };

  const focusRegisterFlow = () => {
    const target = document.getElementById("cluster-register-form");
    target?.scrollIntoView({ behavior: "auto", block: "start" });
    window.requestAnimationFrame(() => {
      registerNameInputRef.current?.focus();
    });
  };

  const register = async (event) => {
    event.preventDefault();
    if (busy) return;
    credentialOpenerRef.current = event.nativeEvent?.submitter || document.activeElement;
    const customId = advancedOpen ? draftId.trim() : "";
    const invalid = registerDraftError(draftName, customId);
    if (invalid) {
      setRegisterError(new Error(invalid));
      return;
    }
    setRegisterError(null);
    setBusy("register");
    try {
      const issued = await registerCluster({ token, tenantId, name: draftName.trim(), clusterId: customId });
      setDraftName("");
      setDraftId("");
      setAdvancedOpen(false);
      setCredential({ kind: "register", ...issued });
      retryClusters();
    } catch (error) {
      setRegisterError(error);
    } finally {
      setBusy(null);
    }
  };

  const startRotate = (cluster, opener) => {
    setActionError(null);
    setPendingDelete(null);
    credentialOpenerRef.current = opener;
    setPendingRotate((current) => (current?.id === cluster.id ? null : cluster));
  };

  const startDelete = (cluster) => {
    setActionError(null);
    setPendingRotate(null);
    setPendingDelete((current) => (current?.id === cluster.id ? null : cluster));
  };

  const rotate = async () => {
    if (busy || !pendingRotate) return;
    setActionError(null);
    setBusy("rotate");
    try {
      const issued = await rotateClusterCredential({ token, tenantId, clusterId: pendingRotate.id });
      setPendingRotate(null);
      setCredential({ kind: "rotate", ...issued });
      retryClusters();
    } catch (error) {
      setActionError(error);
    } finally {
      setBusy(null);
    }
  };

  const remove = async () => {
    if (busy || !pendingDelete) return;
    setActionError(null);
    setBusy("delete");
    try {
      await deleteCluster({ token, tenantId, clusterId: pendingDelete.id });
      setPendingDelete(null);
      retryClusters();
    } catch (error) {
      setActionError(error);
    } finally {
      setBusy(null);
    }
  };

  const policySummary = clusterPolicy
    ? `${clusterPolicy.auto === "ordered" ? "automatic in allow order" : "explicit cluster required"}; ${clusterPolicy.allow.length ? `${clusterPolicy.allow.length} allowed` : "all clusters allowed unless denied"}; ${clusterPolicy.deny.length} denied`
    : "policy not reported on this read";
  let connectionSummary = "Not read yet. The fleet is still being read.";
  if (observed) {
    connectionSummary = `${clusters.length} registered; ${connectedCount} connected through the replica that answered${clusterObservedAt ? `, observed ${formatDate(clusterObservedAt)}` : ""}.`;
  } else if (clusterStatus === "error") {
    connectionSummary = "Not read. The cluster registry is not answering this browser right now.";
  }
  const sharedCapacity = hostedCapacityCardState({
    loadState: hostedCapacityStatus,
    capacity: hostedCapacityRecord,
    canManage,
    posting: hostedCapacityPosting,
    fleetObserved: observed,
    hosted,
    hostedReason,
  });
  return (
    <>
      <TenantHeader
        tenant={tenant}
        title="Clusters"
        copy="Every cluster registered to this tenant — connected or not — and the live connector signal this replica observes."
      />
      <section className="wk-review" aria-label="Cluster deployment checkpoint">
        <section className="wk-review-strip">
          <article>
            <header><span>Model</span><b>Existing Kubernetes cluster</b><small>available now</small></header>
            <dl>
              <div><dt>Cloud owner</dt><dd>You</dd></div>
              <div><dt>Credential owner</dt><dd>You</dd></div>
              <div><dt>Provisioning</dt><dd>Your infrastructure, one-time install.</dd></div>
            </dl>
            {canManage ? (
              <button type="button" className="wk-btn wk-btn-primary" onClick={focusRegisterFlow}>Use the existing cluster flow</button>
            ) : (
              <p role="note">Owners and admins can register a cluster in this flow.</p>
            )}
          </article>
          <article>
            <header><span>Model</span><b>Yscale Shared</b><small>{sharedCapacity.label}</small></header>
            <dl>
              <div><dt>Cloud owner</dt><dd>Yscale</dd></div>
              <div><dt>Credential owner</dt><dd>Yscale</dd></div>
              <div><dt>Provisioning</dt><dd>If assigned, Yscale manages capacity and its reserved namespace.</dd></div>
            </dl>
            {sharedCapacity.kind === "loading" && <p role="status">Checking this tenant's shared capacity status…</p>}
            {sharedCapacity.kind === "error" && (
              <>
                <p className="wk-inline-error" role="alert">{apiMessage(hostedCapacityError)}</p>
                <button type="button" className="wk-btn wk-btn-secondary" onClick={() => setHostedCapacityReload((value) => value + 1)}>Retry shared status</button>
              </>
            )}
            {sharedCapacity.kind === "requested" && (
              <p role="status">
                Request pending{sharedCapacity.requestedAt
                  ? <> since <time dateTime={sharedCapacity.requestedAt}>{formatDate(sharedCapacity.requestedAt)}</time></>
                  : "; the original request time was not reported"}. No ready time is promised.
              </p>
            )}
            {sharedCapacity.kind === "not_requested" && (
              <button
                type="button"
                className="wk-btn wk-btn-primary"
                disabled={sharedCapacity.posting}
                aria-busy={sharedCapacity.posting}
                onClick={requestSharedCapacity}
              >
                {sharedCapacity.posting ? "Requesting…" : "Request shared capacity"}
              </button>
            )}
            {sharedCapacity.kind === "not_requested_restricted" && <p role="note">A tenant owner or admin must request shared capacity.</p>}
            {sharedCapacity.kind === "assigned_launchable" && hosted && <p>{hosted.name} · {hosted.id}</p>}
            {sharedCapacity.kind === "assigned_launchable" && hosted ? (
              <Link to={launchTemplatesPath(hosted.id)} className="wk-btn wk-btn-primary">Launch a workload on hosted capacity</Link>
            ) : sharedCapacity.kind === "assigned_unavailable" ? (
              <p role="note">Not a launch target right now: {sharedCapacity.reason}</p>
            ) : sharedCapacity.kind === "assigned_waiting" ? (
              <p role="status">Reading the assigned hosted target from the cluster fleet…</p>
            ) : null}
            {hostedCapacityNotice && <p role="status" aria-live="polite">{hostedCapacityNotice}</p>}
            {hostedCapacityStatus === "ready" && hostedCapacityError && <p className="wk-inline-error" role="alert">{apiMessage(hostedCapacityError)}</p>}
            <p role="note">No provider credentials are collected and no capacity is automatically provisioned here.</p>
          </article>
          <LinodeCloudAccountCard tenant={tenant} token={token} />
        </section>
      </section>

      <StatTiles
        label="Cluster fleet"
        ready={observed}
        tiles={[
          { key: "registered", icon: "clusters", label: "Registered clusters", value: clusters.length, note: "the durable fleet" },
          { key: "connected", icon: "workloads", label: "Connected here", value: connectedCount, note: clustersLivePartial ? "this replica's partial live view" : "live on this replica" },
          {
            key: "inventory",
            icon: "clock",
            label: "Observed inventory",
            value: fleetInventoryValue(inventory),
            note: fleetInventoryCoverage(inventory, observed),
            text: true,
          },
          { key: "launchable", icon: "check", label: "Launchable now", value: observed ? `${launchableCount} / ${clusters.length}` : "not read yet", note: policySummary, text: true },
        ]}
      />

      {canManage && (
        <Panel id="register-title" title="Register a cluster" meta="a name is all it needs">
          <form id="cluster-register-form" className="wk-cluster-register" onSubmit={register} noValidate>
            <label className="wk-field">
              <span>Display name</span>
              <input
                ref={registerNameInputRef}
                value={draftName}
                onChange={(event) => { setDraftName(event.target.value); setRegisterError(null); }}
                autoComplete="off"
                placeholder="Prod US East"
                disabled={!!busy}
              />
              <small>What your team calls this cluster. The id and credential are minted for you.</small>
            </label>
            <button type="submit" className="wk-btn wk-btn-primary" disabled={!!busy} aria-busy={busy === "register"}>
              <Icon name="plus" size={15} />{busy === "register" ? "Registering…" : "Register cluster"}
            </button>
            <div className="wk-cluster-advanced">
              <button type="button" className="wk-text-link" aria-expanded={advancedOpen} onClick={() => setAdvancedOpen((value) => !value)}>
                {advancedOpen ? "Hide advanced" : "Advanced: choose the cluster id"}
              </button>
              {advancedOpen && (
                <label className="wk-field">
                  <span>Custom cluster id</span>
                  <input
                    value={draftId}
                    onChange={(event) => { setDraftId(event.target.value); setRegisterError(null); }}
                    autoComplete="off"
                    spellCheck="false"
                    placeholder="prod-us-east"
                    disabled={!!busy}
                  />
                  <small>Optional. Letters, digits, dots, dashes, and underscores; central mints a unique id when empty.</small>
                </label>
              )}
            </div>
            {registerError && <p className="wk-inline-error" role="alert">{apiMessage(registerError)}</p>}
          </form>
          <Note>Registering reveals the connector token and helm install command exactly once. Nothing runs in your cluster until you run that command there.</Note>
        </Panel>
      )}

      <Panel
        id="fleet-title"
        title="Registered fleet"
        meta={observed ? `${clusters.length} registered` : null}
        action={observed ? <button type="button" className="wk-text-link" onClick={retryClusters}>Read again</button> : null}
        className="wk-panel-flush"
      >
        {clusterStatus !== "ready" && clusterStatus !== "error" && <LoadingState label="Reading this tenant's cluster fleet…" rows={3} />}
        {clusterStatus === "error" && <ErrorState error={clusterError} onRetry={retryClusters} title="The cluster fleet could not be read." />}
        {observed && !clusters.length && (
          <div className="wk-empty">
            <span className="wk-empty-icon" aria-hidden="true"><Icon name="clusters" size={20} /></span>
            <div>
              <h3>No cluster registered</h3>
              <p>{canManage
                ? "Register a cluster above to get its one-time connector credential and install command."
                : "An owner or admin registers clusters; the fleet appears here once one exists."}</p>
            </div>
            <button type="button" className="wk-btn wk-btn-secondary" onClick={retryClusters}><Icon name="refresh" />Check again</button>
          </div>
        )}
        {observed && clusters.length > 0 && (
          <>
            <div className="wk-table-wrap wk-table-wrap-fleet">
              <table className="wk-table">
                <thead>
                  <tr>
                    <th scope="col">Cluster</th>
                    <th scope="col">State</th>
                    <th scope="col">Live signal</th>
                    <th scope="col">Registered</th>
                    <th scope="col">Policy</th>
                    {canManage && <th scope="col" className="wk-cell-actions">Manage</th>}
                  </tr>
                </thead>
                <tbody>
                  {clusters.map((cluster) => {
                    const confirming = pendingRotate?.id === cluster.id || pendingDelete?.id === cluster.id;
                    return (
                      <tr key={cluster.id} className={confirming ? "is-confirming" : undefined}>
                        <th scope="row"><span>{cluster.name}<small>{cluster.id}</small></span></th>
                        <td>
                          <span className={clusterStateChip(cluster)}>{clusterStateLabel(cluster)}</span>
                          {clusterStateDetail(cluster) && <small>{clusterStateDetail(cluster)}</small>}
                        </td>
                        <td>{clusterLiveSignal(cluster)}</td>
                        <td><time dateTime={cluster.registeredAt || undefined}>{cluster.registeredAt ? formatDate(cluster.registeredAt, false) : "not reported"}</time></td>
                        <td>
                          <span className={`wk-chip${cluster.eligible ? "" : " wk-chip-warn"}`}>{cluster.eligible ? "eligible" : "blocked"}</span>
                          {!cluster.eligible && cluster.reason ? <small>{cluster.reason}</small> : null}
                        </td>
                        {canManage && (
                          <td className="wk-cell-actions">
                            {/* A hosted row holds no tenant credential, so there
                                is nothing here to rotate or delete. */}
                            {isHostedCluster(cluster) ? (
                              <span className="wk-chip">Managed by Yscale</span>
                            ) : (
                              <>
                                <button type="button" className="wk-text-link" aria-expanded={pendingRotate?.id === cluster.id} disabled={!!busy} onClick={(event) => startRotate(cluster, event.currentTarget)}>
                                  {pendingRotate?.id === cluster.id ? "Cancel" : "Rotate"}
                                </button>
                                {" "}
                                <button type="button" className="wk-text-link" aria-expanded={pendingDelete?.id === cluster.id} disabled={!!busy} onClick={() => startDelete(cluster)}>
                                  {pendingDelete?.id === cluster.id ? "Cancel" : "Delete"}
                                </button>
                              </>
                            )}
                          </td>
                        )}
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
            <ul className="wk-cluster-cards">
              {clusters.map((cluster) => (
                <li key={cluster.id}>
                  <div className="wk-cluster-card-top">
                    <b>{cluster.name}</b>
                    <span className={clusterStateChip(cluster)}>{clusterStateLabel(cluster)}</span>
                  </div>
                  <span className="wk-cluster-card-id">{cluster.id}</span>
                  <p>{clusterMobileSignal(cluster)}</p>
                  <p>
                    Registered {cluster.registeredAt ? formatDate(cluster.registeredAt, false) : "— date not reported"}
                    {!cluster.eligible && ` · ${cluster.reason || "blocked by policy"}`}
                  </p>
                  {canManage && (
                    <div className="wk-cluster-card-actions">
                      {isHostedCluster(cluster) ? (
                        <span className="wk-chip">Managed by Yscale</span>
                      ) : (
                        <>
                          <button type="button" className="wk-btn wk-btn-secondary" disabled={!!busy} aria-expanded={pendingRotate?.id === cluster.id} onClick={(event) => startRotate(cluster, event.currentTarget)}>
                            {pendingRotate?.id === cluster.id ? "Cancel rotate" : "Rotate credential"}
                          </button>
                          <button type="button" className="wk-btn wk-btn-danger" disabled={!!busy} aria-expanded={pendingDelete?.id === cluster.id} onClick={() => startDelete(cluster)}>
                            {pendingDelete?.id === cluster.id ? "Cancel delete" : "Delete"}
                          </button>
                        </>
                      )}
                    </div>
                  )}
                </li>
              ))}
            </ul>
          </>
        )}
        {pendingRotate && (
          <div className="wk-confirm" role="alertdialog" aria-labelledby="rotate-title">
            <div>
              <b id="rotate-title">Rotate the credential for {pendingRotate.name}?</b>
              <span>The connector holding the current credential is disconnected immediately. The new token and helm upgrade command are shown once.</span>
              {actionError && <span className="wk-confirm-error" role="alert">{apiMessage(actionError)}</span>}
            </div>
            <div>
              <button type="button" className="wk-btn wk-btn-secondary" onClick={() => { setPendingRotate(null); setActionError(null); }} disabled={busy === "rotate"}>Keep current</button>
              <button type="button" className="wk-btn wk-btn-primary" onClick={rotate} disabled={!!busy} aria-busy={busy === "rotate"}>
                {busy === "rotate" ? "Rotating…" : "Rotate and reveal"}
              </button>
            </div>
          </div>
        )}
        {pendingDelete && (
          <div className="wk-confirm" role="alertdialog" aria-labelledby="delete-cluster-title">
            <div>
              <b id="delete-cluster-title">Delete {pendingDelete.name} from this tenant?</b>
              <span>The registration and its credential are removed; a connector still holding it can no longer connect. A policy that names {pendingDelete.id} blocks this until the policy drops it.</span>
              {actionError && <span className="wk-confirm-error" role="alert">{apiMessage(actionError)}</span>}
            </div>
            <div>
              <button type="button" className="wk-btn wk-btn-secondary" onClick={() => { setPendingDelete(null); setActionError(null); }} disabled={busy === "delete"}>Keep cluster</button>
              <button type="button" className="wk-btn wk-btn-danger" onClick={remove} disabled={!!busy} aria-busy={busy === "delete"}>
                {busy === "delete" ? "Deleting…" : "Confirm delete"}
              </button>
            </div>
          </div>
        )}
        {!canManage && observed && (
          <Note>Your {tenant.role} role reads this fleet. Registering, rotating, and deleting clusters needs the owner or admin role.</Note>
        )}
        <Note tone="warn">{CLUSTER_OBSERVATION_NOTE}</Note>
        {clustersLivePartial && <Note>{CLUSTER_PARTIAL_NOTE}</Note>}
      </Panel>

      <Panel id="connector-title" title="Cluster Connector" meta={tenant.customer_id}>
        <DetailList rows={[
          { label: "Model", value: "One connector per registered cluster. The connector runs in your cluster and dials out to central; central never dials in." },
          { label: "Component", value: "yscale-agent, namespace yscale-system, joined to the mesh alongside its gateway." },
          { label: "Credential", value: "Shown exactly once, when a cluster is registered or its credential is rotated. Afterwards it lives only as the Secret in your cluster; this console keeps no copy." },
          { label: "Reported connections", value: connectionSummary },
          { label: "Last accepted request", value: lastAt ? formatDate(lastAt) : "No workload records in this tenant." },
        ]} />
        <Note tone="warn">
          A live connection is not a health check. Whether the agent is reconciling, and whether your nodes are ready, are cluster-side facts this console cannot read — check them with kubectl, below.
        </Note>
      </Panel>

      <Panel
        id="targets-title"
        title="Placements in this tenant's records"
        meta={ready ? `${targets.length} distinct` : null}
        className="wk-panel-flush"
      >
        {workloadStatus === "loading" && <LoadingState label="Loading placements…" rows={3} />}
        {workloadStatus === "error" && <ErrorState error={workloadError} onRetry={retry} title="Placements are unavailable." />}
        {ready && !targets.length && (
          <div className="wk-empty">
            <span className="wk-empty-icon" aria-hidden="true"><Icon name="clusters" size={20} /></span>
            <div>
              <h3>No placements yet</h3>
              <p>Placements appear once this tenant has submitted a workload.</p>
            </div>
            <Link to="/workloads/templates" className="wk-btn wk-btn-secondary">Browse templates</Link>
          </div>
        )}
        {ready && targets.length > 0 && (
          <div className="wk-table-wrap">
            <table className="wk-table">
              <thead>
                <tr>
                  <th scope="col">Backend</th>
                  <th scope="col">Region</th>
                  <th scope="col">Compute</th>
                  <th scope="col">Records</th>
                  <th scope="col">Active</th>
                  <th scope="col">Last requested</th>
                </tr>
              </thead>
              <tbody>
                {targets.map((target) => (
                  <tr key={target.key}>
                    <th scope="row"><span>{target.backend === "auto" ? "automode" : target.backend}<small>{target.bursts} burst receipt{target.bursts === 1 ? "" : "s"}</small></span></th>
                    <td>{target.region || "provider default"}</td>
                    <td>{target.gpu ? "GPU requested" : "CPU"}</td>
                    <td>{target.runs}</td>
                    <td>{target.active}</td>
                    <td><time dateTime={target.lastAt || undefined}>{formatDate(target.lastAt, false)}</time></td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
        <Note>
          Derived from the workload records this console can read, not from a live inventory. A row means this tenant asked for that placement; it does not mean capacity is running there now.
        </Note>
      </Panel>

      <Panel id="connect-title" title="Connect a registered cluster">
        <ol className="wk-steps">
          <li><b>1</b><div><strong>Register it here</strong><span>{canManage ? "Name the cluster above. The connector token and helm install command are shown exactly once." : "An owner or admin names the cluster and gets its one-time connector token and install command."}</span></div></li>
          <li><b>2</b><div><strong>Run the command in your cluster</strong><span>It installs yscale-agent into yscale-system with that credential. The agent dials out and installs the workloads.yscale.sh CRD.</span></div></li>
          <li><b>3</b><div><strong>Watch it turn connected</strong><span>The row above reads “connected here” once this replica sees the connector. Then submit a workload from this console or kubectl.</span></div></li>
        </ol>
        <CopyBlock label="Verify" name="kubectl" language="sh" text={AGENT_VERIFY} />
        <Note tone="warn">
          This console cannot install anything into your cluster, and it cannot show a token again. Lost the credential? Rotate it for a new one.
        </Note>
      </Panel>

      {/* The platform-managed row is still read from the fleet observation.
          The request above changes queue state only, so this panel offers a
          launch route only while central reports an actual usable target. */}
      <Panel id="hosted-title" title="Yscale Hosted" meta={observed && hosted ? "platform-managed" : null}>
        {clusterStatus !== "ready" && clusterStatus !== "error" && <LoadingState label="Reading hosted capacity…" rows={2} />}
        {clusterStatus === "error" && <ErrorState error={clusterError} onRetry={retryClusters} title="Hosted capacity could not be read." />}
        {observed && !hosted && (
          <div className="wk-preview-state">
            <span className="wk-preview-mark" aria-hidden="true"><Icon name="clusters" size={20} /></span>
            <div>
              <h3>Not enabled for this tenant</h3>
              <p>
                No Yscale-operated target is present in the current fleet for {tenant.customer_id}. Until central reports one, every
                workload lands on a cluster this tenant connects above.
              </p>
            </div>
          </div>
        )}
        {observed && hosted && (
          <>
            <div className="wk-hosted-head">
              <div>
                <b>{hosted.name}</b>
                <small>{hosted.id}</small>
              </div>
              <span className={clusterStateChip(hosted)}>{clusterStateLabel(hosted)}</span>
            </div>
            <DetailList rows={[
              { label: "Operated by", value: "Yscale runs this capacity. Registration, connector credential, and upgrades are platform-managed — nothing to install or rotate here." },
              { label: "Reserved namespace", value: hosted.hostedNamespace ? <span className="wk-mono">{hosted.hostedNamespace}</span> : "not reported by central" },
              { label: "Virtual cluster id", value: <span className="wk-mono">{hosted.id}</span> },
              { label: "Live signal", value: clusterLiveSignal(hosted) },
              { label: "Policy", value: hosted.eligible ? "Eligible under this tenant's cluster placement policy." : hosted.reason || "Blocked by this tenant's cluster placement policy." },
            ]} />
            {hostedReason ? (
              <Note>Not a launch target right now: {hostedReason}</Note>
            ) : (
              <p className="wk-hosted-cta">
                <Link to={launchTemplatesPath(hosted.id)} className="wk-btn wk-btn-primary">Launch from a template</Link>
                <span>Pick a template; this hosted cluster stays selected and its reserved namespace pins automatically.</span>
              </p>
            )}
          </>
        )}
      </Panel>

      {credential && (
        <CredentialRevealDialog
          credential={credential}
          returnFocus={credentialOpenerRef.current}
          onClose={() => setCredential(null)}
        />
      )}
    </>
  );
}

/* ---------------------------------------------------------------- policies */

const AUTOMODE_RULES = [
  { when: "backend is auto or unset, and the workload asks for no GPU", then: "Fly.io" },
  { when: "backend is auto or unset, and the GPU kind is any or unset", then: "Linode — the cheapest GPU tier" },
  { when: "backend is auto or unset, and a datacenter GPU is named (l4, l40s, a100, h100, h200)", then: "AWS — the cards Linode does not serve" },
  { when: "backend names a provider", then: "that provider, or the submission is refused if it cannot serve the shape" },
];

const PREFERENCE_RULES = [
  { field: "backend", effect: "Pins the cloud that runs the burst.", omitted: "Automode picks, by the rules below." },
  { field: "region", effect: "Requests a region.", omitted: "The provider's default. Only the Linode backend accepts a region from this console." },
  { field: "gpu.kind / gpu.count", effect: "The accelerator shape to place.", omitted: "CPU-only capacity." },
  { field: "gpu.reliability", effect: "Only reliable — on-demand capacity — is supported today. spot and any are refused.", omitted: "reliable." },
  { field: "gpu.maxHourlyUSD", effect: "Refuses a placement priced above this per hour.", omitted: "No per-hour ceiling on the workload itself — the tenant ceiling still applies." },
  { field: "budget.maxUSD", effect: "Total spend the run may accrue before it is stopped.", omitted: "Required; the console will not submit without it." },
  { field: "budget.deadline", effect: "Wall-clock limit; the run is reaped at it.", omitted: "Required; the console will not submit without it." },
];

export function PoliciesPage({ tenant, session, workloads, retryClusters }) {
  const limits = useMemo(() => limitRows(tenant.limits), [tenant.limits]);
  const provenance = useMemo(() => provenanceSummary(workloads), [workloads]);
  const canManage = canManageTenant(tenant.role);
  const canSubmit = canMutateWorkloads(tenant.role);
  const token = session.accessToken;
  const tenantId = tenant.customer_id;
  const [status, setStatus] = useState("loading");
  const [policyRecord, setPolicyRecord] = useState(null);
  const [draft, setDraft] = useState({ auto: "require_pin", allow: "", deny: "" });
  const [error, setError] = useState(null);
  const [saveState, setSaveState] = useState("idle");
  const [saveError, setSaveError] = useState(null);
  const [reloadKey, setReloadKey] = useState(0);

  useEffect(() => {
    const controller = new AbortController();
    setStatus("loading");
    setError(null);
    setSaveError(null);
    setSaveState("idle");
    fetchClusterPolicy({ token, tenantId, signal: controller.signal })
      .then((record) => {
        setPolicyRecord(record);
        setDraft({
          auto: record.policy.auto,
          allow: record.policy.allow.join("\n"),
          deny: record.policy.deny.join("\n"),
        });
        setStatus("ready");
      })
      .catch((err) => {
        if (err?.name === "AbortError") return;
        setError(err);
        setStatus("error");
      });
    return () => controller.abort();
  }, [token, tenantId, reloadKey]);

  const savePolicy = async (event) => {
    event.preventDefault();
    setSaveError(null);
    setSaveState("saving");
    let next;
    try {
      next = normalizeClusterPolicyDraft(draft);
    } catch (err) {
      setSaveError(err);
      setSaveState("idle");
      return;
    }
    try {
      const saved = await putClusterPolicy({ token, tenantId, policy: next });
      setPolicyRecord(saved);
      setDraft({
        auto: saved.policy.auto,
        allow: saved.policy.allow.join("\n"),
        deny: saved.policy.deny.join("\n"),
      });
      setSaveState("saved");
      retryClusters?.();
    } catch (err) {
      setSaveError(err);
      setSaveState("idle");
    }
  };

  const policy = policyRecord?.policy;
  const policyRows = policy ? [
    { label: "Automatic placement", value: policy.auto === "ordered" ? "Enabled, using the allow list order." : "Disabled; each workload must name an eligible cluster." },
    { label: "Allowed order", value: policy.allow.length ? policy.allow.join(" -> ") : "Any connected cluster unless denied." },
    { label: "Denied clusters", value: policy.deny.length ? policy.deny.join(", ") : "None." },
    { label: "Your access", value: canManage ? "Can read and change policy." : "Read only.", note: policyRecord.role || tenant.role },
  ] : [];
  return (
    <>
      <TenantHeader
        tenant={tenant}
        title="Policies"
        copy="What central enforces for this tenant, and what each workload merely asks for. The two are not the same thing."
      />

      <Panel id="cluster-policy-title" title="Tenant cluster placement" meta="hard policy">
        {status === "loading" && <LoadingState label="Loading cluster policy…" rows={4} />}
        {status === "error" && <ErrorState error={error} onRetry={() => setReloadKey((n) => n + 1)} title="The cluster placement policy could not be read." />}
        {status === "ready" && policy && (
          <>
            <DetailList rows={policyRows} />
            {!canManage && <Note>This role can read the policy but cannot change it.</Note>}
            {canManage && (
              <form className="wk-form" onSubmit={savePolicy}>
                <div className="wk-form-grid">
                  <fieldset className="wk-choice-field">
                    <legend>Automatic cluster placement</legend>
                    <div className="wk-segmented">
                      <label><input type="radio" name="cluster-policy-auto" value="ordered" checked={draft.auto === "ordered"} onChange={(event) => setDraft((current) => ({ ...current, auto: event.target.value }))} /><span>Ordered automatic</span></label>
                      <label><input type="radio" name="cluster-policy-auto" value="require_pin" checked={draft.auto === "require_pin"} onChange={(event) => setDraft((current) => ({ ...current, auto: event.target.value }))} /><span>Require explicit</span></label>
                    </div>
                  </fieldset>
                  <label className="wk-field">
                    <span>Allowed cluster ids, in order</span>
                    <textarea rows={5} value={draft.allow} onChange={(event) => setDraft((current) => ({ ...current, allow: event.target.value }))} spellCheck="false" />
                    <small>One id per line. Disconnected ids are allowed for pre-authorization. Empty means all connected clusters unless denied.</small>
                  </label>
                  <label className="wk-field">
                    <span>Denied cluster ids</span>
                    <textarea rows={5} value={draft.deny} onChange={(event) => setDraft((current) => ({ ...current, deny: event.target.value }))} spellCheck="false" />
                    <small>One id per line. Deny always wins and cannot overlap allow.</small>
                  </label>
                </div>
                {saveError && <p className="wk-inline-error" role="alert">{saveError.message}</p>}
                {saveState === "saved" && <p className="wk-inline-note" role="status">Saved. Cluster observation refresh requested.</p>}
                <div className="wk-actions">
                  <button type="button" className="wk-btn wk-btn-secondary" onClick={() => setReloadKey((n) => n + 1)} disabled={saveState === "saving"}>Reset</button>
                  <button type="submit" className="wk-btn wk-btn-primary" disabled={saveState === "saving"} aria-busy={saveState === "saving"}>{saveState === "saving" ? "Saving…" : "Save policy"}</button>
                </div>
              </form>
            )}
          </>
        )}
        <Note tone="warn">
          This is tenant placement policy. It controls eligible clusters and automatic cluster choice; per-workload backend, region, GPU, and budget fields remain cloud preferences below.
        </Note>
      </Panel>

      <Panel id="hard-title" title="Hard rules" meta="enforced by central">
        <DetailList rows={[
          ...limits.map((row) => ({
            label: row.label,
            value: `${limitValue(row)} — ${row.unlimited ? "central reports 0, which means no ceiling on this tenant." : "reported on your plan; a submission over it is refused."}`,
          })),
          {
            label: "Your role",
            value: `${tenant.role} — ${canSubmit ? "may submit and cancel workloads." : "read-only; submissions from this role are refused as role_read_only."}`,
          },
          {
            label: "Namespace",
            value: "Every submission is decided against the tenant's authorized namespaces. Central records the namespace asked for, the one granted, and the rule that decided it.",
          },
        ]} />
        {!limits.length && (
          <Note tone="warn">
            This tenant's summary reported no numeric limits. That is what the API said, not a claim that nothing is enforced.
          </Note>
        )}
        <Note>
          Plan: {tenant.plan || "not reported"}. These values come from the tenant summary on your account and are read-only here — there is no policy API to change them from the console.
        </Note>
      </Panel>

      <Panel id="prefs-title" title="Per-workload preferences" meta="chosen at launch" className="wk-panel-flush">
        <div className="wk-table-wrap">
          <table className="wk-table">
            <thead>
              <tr>
                <th scope="col">Field</th>
                <th scope="col">What it does</th>
                <th scope="col">If you leave it out</th>
              </tr>
            </thead>
            <tbody>
              {PREFERENCE_RULES.map((rule) => (
                <tr key={rule.field}>
                  <th scope="row"><span className="wk-mono">{rule.field}</span></th>
                  <td>{rule.effect}</td>
                  <td>{rule.omitted}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
        <Note>
          A preference is a request. A hard rule is a refusal. A workload that asks for more than the tenant ceiling is rejected at submission, not trimmed to fit.
        </Note>
      </Panel>

      <Panel id="automode-title" title="What automode does">
        <ul className="wk-rules">
          {AUTOMODE_RULES.map((rule) => (
            <li key={rule.when}>
              <span className="wk-rule-when">{rule.when}</span>
              <Icon name="chevron" size={13} />
              <b>{rule.then}</b>
            </li>
          ))}
        </ul>
        <Note>
          Automode picks the provider; it does not raise a budget, relax a ceiling, or place a shape the named backend cannot serve.
        </Note>
      </Panel>

      <Panel id="decisions-title" title="Decisions on this tenant's records">
        <DetailList rows={[
          { label: "Records", value: `${provenance.total} readable by this console` },
          { label: "With a decision", value: `${provenance.recorded} carry an authorization block; ${provenance.total - provenance.recorded} predate provenance or were not recorded.` },
          {
            label: "Rules applied",
            value: provenance.rules.length
              ? provenance.rules.map((rule) => `${rule.rule}${rule.version ? ` (${rule.version})` : ""} × ${rule.runs}`).join(", ")
              : "No rule was recorded on the records this console can read.",
          },
        ]} />
      </Panel>
    </>
  );
}

/* ------------------------------------------------------------------ gitops */

const KEDA_SNIPPET = `# Pod template of a Deployment that KEDA (or an HPA, or Argo) scales.
# yscale provisions a burst node when a matching pod goes Pending.
# reliability takes only reliable (on-demand); spot and any are refused.
spec:
  template:
    metadata:
      annotations:
        yscale.sh/burst-template: |
          gpu: { kind: rtx4000ada, count: 1, reliability: reliable, maxHourlyUSD: 2.00 }
          budget: { maxUSD: 5, deadline: 1h }
    spec:
      nodeSelector:
        yscale.sh/burst-node: "true"`;

// A form control that carries its own error or hint. The section pages have
// written this by hand until now; the source editor has seven fields per row,
// which is where writing it once starts paying.
function Field({ label, error, hint, children, className = "" }) {
  return (
    <label className={`wk-field ${className}${error ? " has-error" : ""}`}>
      <span>{label}</span>{children}{error ? <small role="alert">{error}</small> : hint ? <small>{hint}</small> : null}
    </label>
  );
}

// The clone URL as a reader recognises it: host and repository, with the
// scheme, the user part, and the .git suffix taken off. It is a label — the
// exact string central holds is printed under the rail, unshortened.
function repoLabel(url) {
  const scp = /^[A-Za-z0-9._-]+@([A-Za-z0-9._+-]+):(.+)$/.exec(url);
  const plain = scp ? `${scp[1]}/${scp[2]}` : url.replace(/^[A-Za-z+]+:\/\//, "").replace(/^[^@/]+@/, "");
  return plain.replace(/\.git$/, "") || url;
}

// What this console can honestly say about the cluster a source names. A fleet
// this page never read and a cluster the fleet does not carry are different
// answers, and neither one is "this tenant has no clusters".
function sourceClusterFact(clusterId, clusters, clusterStatus) {
  if (clusterStatus !== "ready") return { label: clusterId, note: "Cluster fleet not read here", missing: false };
  const match = clusters.find((cluster) => cluster.id === clusterId);
  if (match) return { label: match.name, note: match.id, missing: false };
  return { label: clusterId, note: "Not in the current fleet observation", missing: true };
}

// The configuration path one source describes: repository, then the reconciler
// that reads it, then the cluster it lands on. It is a picture of what was
// registered, not of anything observed running — every value on it came from
// this form.
function SourceRail({ source, cluster }) {
  return (
    <ol className="wk-rail">
      <li className="wk-rail-node">
        <span className="wk-rail-label">Repository</span>
        <b>{repoLabel(source.repoUrl)}</b>
        <small>{source.ref} · {source.path || "repository root"}</small>
      </li>
      <li className="wk-rail-link" aria-hidden="true"><Icon name="chevron" size={13} /></li>
      <li className="wk-rail-node">
        <span className="wk-rail-label">Reconciler</span>
        <b>{reconcilerLabel(source.reconciler)}</b>
        <small>runs in your cluster</small>
      </li>
      <li className="wk-rail-link" aria-hidden="true"><Icon name="chevron" size={13} /></li>
      <li className="wk-rail-node">
        <span className="wk-rail-label">Target cluster</span>
        <b>{cluster.label}</b>
        <small className={cluster.missing ? "wk-rail-missing" : undefined}>{cluster.note}</small>
      </li>
    </ol>
  );
}

function SourceCard({ source, cluster }) {
  return (
    <li className="wk-source">
      <header className="wk-source-head">
        <div>
          <b>{source.name}</b>
          <span className="wk-source-id">{source.id}</span>
        </div>
        <span className="wk-chip">Configured</span>
      </header>
      <SourceRail source={source} cluster={cluster} />
      <p className="wk-source-url">{source.repoUrl}</p>
    </li>
  );
}

// One editable source. Removal asks first: the registry is written whole, so a
// row dropped by a mis-click is a coordinate nobody notices leaving until a
// cluster stops being reconciled from anywhere.
function SourceEntryCard({ index, row, errors, clusters, clusterStatus, removing, onRequestRemove, onCancelRemove, onRemove, onChange, disabled }) {
  const removeButtonRef = useRef(null);
  const keepButtonRef = useRef(null);
  const wasRemovingRef = useRef(false);
  const set = (key) => (event) => onChange({ ...row, [key]: event.target.value });
  const observed = clusterStatus === "ready" ? clusters : [];
  const missing = !!row.clusterId && !observed.some((cluster) => cluster.id === row.clusterId);
  useEffect(() => {
    if (removing) keepButtonRef.current?.focus();
    else if (wasRemovingRef.current) removeButtonRef.current?.focus();
    wasRemovingRef.current = removing;
  }, [removing]);
  return (
    <li className="wk-catalog-entry">
      <header className="wk-catalog-entry-head">
        <div>
          <b>{row.name || "Untitled source"}</b>
          <span>{row.id ? `${row.id} · ${reconcilerLabel(row.reconciler)}` : "new source"}</span>
        </div>
        <div className="wk-catalog-entry-actions">
          <button ref={removeButtonRef} type="button" className="wk-btn wk-btn-secondary" onClick={onRequestRemove} disabled={disabled}>Remove</button>
        </div>
      </header>
      {removing && (
        <div className="wk-confirm" role="alertdialog" aria-labelledby={`gitops-remove-${index}`}>
          <div>
            <b id={`gitops-remove-${index}`}>Remove {row.name || "this source"} from the registry?</b>
            <span>It leaves the registry when you save. Nothing is removed from the repository or the cluster.</span>
          </div>
          <div>
            <button ref={keepButtonRef} type="button" className="wk-btn wk-btn-secondary" onClick={onCancelRemove}>Keep it</button>
            <button type="button" className="wk-btn wk-btn-danger" onClick={onRemove}>Remove source</button>
          </div>
        </div>
      )}
      <div className="wk-form-grid wk-form-grid-2">
        <Field label="Source id" error={errors.id} hint="Stable across edits; it is how central addresses this row.">
          <input value={row.id} onChange={set("id")} autoComplete="off" spellCheck="false" placeholder="prod-infra" disabled={disabled} />
        </Field>
        <Field label="Name" error={errors.name}>
          <input value={row.name} onChange={set("name")} autoComplete="off" disabled={disabled} />
        </Field>
        <Field label="Repository clone URL" error={errors.repoUrl} className="wk-field-wide" hint="A coordinate only. Yscale never clones this repository and holds no credential for it.">
          <input value={row.repoUrl} onChange={set("repoUrl")} autoComplete="off" spellCheck="false" placeholder="https://github.com/acme/infra.git" disabled={disabled} />
        </Field>
        <Field label="Ref" error={errors.ref} hint="Branch, tag, or commit.">
          <input value={row.ref} onChange={set("ref")} autoComplete="off" spellCheck="false" placeholder="main" disabled={disabled} />
        </Field>
        <Field label="Path in repository" error={errors.path} hint="Leave it empty for the repository root.">
          <input value={row.path} onChange={set("path")} autoComplete="off" spellCheck="false" placeholder="clusters/prod" disabled={disabled} />
        </Field>
        <fieldset className="wk-choice-field">
          <legend>Reconciler</legend>
          <div className="wk-segmented">
            {GITOPS_RECONCILERS.map((reconciler) => (
              <label key={reconciler}>
                <input type="radio" name={`reconciler-${index}`} checked={row.reconciler === reconciler} onChange={() => onChange({ ...row, reconciler })} disabled={disabled} />
                <span>{reconcilerLabel(reconciler)}</span>
              </label>
            ))}
          </div>
          {errors.reconciler ? <small className="wk-field-error" role="alert">{errors.reconciler}</small> : null}
        </fieldset>
        {clusterStatus === "ready" ? (
          <Field
            label="Target cluster"
            error={errors.clusterId}
            hint={missing ? "This id is not in the current fleet observation. It stays on the row until you change it." : "Where this source is reconciled."}
          >
            <select value={row.clusterId} onChange={set("clusterId")} disabled={disabled}>
              <option value="">Select a cluster…</option>
              {/* A stored cluster the observation no longer carries stays
                  selectable: dropping it would edit the tenant's registry on
                  the reader's behalf, and this page does not do that. */}
              {missing && <option value={row.clusterId}>{row.clusterId} — not in the current fleet observation</option>}
              {observed.map((cluster) => (
                <option key={cluster.id} value={cluster.id}>{cluster.name === cluster.id ? cluster.id : `${cluster.name} (${cluster.id})`}</option>
              ))}
            </select>
          </Field>
        ) : (
          <Field
            label="Target cluster id"
            error={errors.clusterId}
            hint="The cluster fleet was not read here, so this is the stored id rather than a choice. It is not a report that this tenant has no clusters."
          >
            <input value={row.clusterId} onChange={set("clusterId")} autoComplete="off" spellCheck="false" disabled={disabled} />
          </Field>
        )}
      </div>
    </li>
  );
}

// Rows carry a key of their own rather than being addressed by position: the id
// is editable and the list is reorderable by removal, so keying on either would
// remount the field being typed into and take the caret with it.
function sourceEntries(sources, seed) {
  return sources.map((source, index) => ({ key: `${seed}:${index}`, row: gitOpsDraftRow(source) }));
}

function SourceRegistryEditor({ record, token, tenantId, clusters, clusterStatus, onDirtyChange, closing, onKeepEditing, onDiscardEdits, onSaved, onReviewLatest }) {
  const nextKey = useRef(record.sources.length);
  const addButtonRef = useRef(null);
  const resetButtonRef = useRef(null);
  const resetKeepButtonRef = useRef(null);
  const closeKeepButtonRef = useRef(null);
  const conflictKeepButtonRef = useRef(null);
  const wasConfirmingResetRef = useRef(false);
  const [entries, setEntries] = useState(() => sourceEntries(record.sources, "loaded"));
  const [saveState, setSaveState] = useState("idle");
  const [saveError, setSaveError] = useState(null);
  const [conflict, setConflict] = useState(null);
  const [showErrors, setShowErrors] = useState(false);
  const [removingKey, setRemovingKey] = useState("");
  const [confirmingReset, setConfirmingReset] = useState(false);
  const rows = useMemo(() => entries.map((entry) => entry.row), [entries]);
  const errors = useMemo(() => gitOpsDraftErrors(rows), [rows]);
  const invalid = errors.some((row) => Object.keys(row).length > 0);
  const dirty = useMemo(() => gitOpsDraftChanged(rows, record.sources), [rows, record.sources]);
  const saving = saveState === "saving";
  // The page owns the Close editor control, so it is told what closing would
  // cost. Unmounting reports the editor as holding nothing, because it is.
  useEffect(() => {
    onDirtyChange(dirty);
    return () => onDirtyChange(false);
  }, [dirty, onDirtyChange]);

  useEffect(() => {
    if (closing) closeKeepButtonRef.current?.focus();
  }, [closing]);

  useEffect(() => {
    if (confirmingReset) resetKeepButtonRef.current?.focus();
    else if (wasConfirmingResetRef.current) resetButtonRef.current?.focus();
    wasConfirmingResetRef.current = confirmingReset;
  }, [confirmingReset]);

  useEffect(() => {
    if (conflict) conflictKeepButtonRef.current?.focus();
  }, [conflict]);

  const save = async (event) => {
    event.preventDefault();
    setShowErrors(true);
    setSaveError(null);
    setConflict(null);
    if (invalid) return;
    setSaveState("saving");
    try {
      // The registry is replaced whole, carrying the revision these rows were
      // read at. An empty list is a legitimate document — it is the only way the
      // last source leaves — so it is submitted rather than treated as a mistake.
      const saved = await putGitOpsSources({
        token,
        tenantId,
        sources: rows,
        sourcesRevision: record.sourcesRevision,
      });
      setShowErrors(false);
      setSaveState("saved");
      onSaved(saved);
    } catch (error) {
      // A stale write keeps every edit in this form. The registry central is
      // serving comes back with the refusal, so the reader is offered it rather
      // than having their draft replaced by it.
      const current = gitOpsConflict(error);
      if (current) setConflict(current);
      else setSaveError(error);
      setSaveState("idle");
    }
  };

  const applyReset = () => {
    setEntries(sourceEntries(record.sources, "loaded"));
    setShowErrors(false);
    setSaveError(null);
    setConflict(null);
    setRemovingKey("");
    setSaveState("idle");
    setConfirmingReset(false);
  };

  const reset = () => {
    if (dirty) setConfirmingReset(true);
    else applyReset();
  };

  const add = () => {
    nextKey.current += 1;
    setEntries((current) => [...current, { key: `new:${nextKey.current}`, row: gitOpsDraftRow() }]);
  };

  return (
    <form className="wk-form wk-catalog-form wk-source-form" onSubmit={save} noValidate>
      {closing && (
        <div className="wk-confirm" role="alertdialog" aria-labelledby="gitops-close-title">
          <div>
            <b id="gitops-close-title">Close the editor and lose these edits?</b>
            <span>They have not been written to the registry, and closing the editor throws them away.</span>
          </div>
          <div>
            <button ref={closeKeepButtonRef} type="button" className="wk-btn wk-btn-secondary" onClick={onKeepEditing}>Keep editing</button>
            <button type="button" className="wk-btn wk-btn-danger" onClick={onDiscardEdits}>Close and discard</button>
          </div>
        </div>
      )}
      <ul className="wk-catalog-list">
        {entries.map((entry, index) => (
          <SourceEntryCard
            key={entry.key}
            index={entry.key}
            row={entry.row}
            errors={showErrors ? errors[index] || {} : {}}
            clusters={clusters}
            clusterStatus={clusterStatus}
            disabled={saving}
            removing={removingKey === entry.key}
            onRequestRemove={() => setRemovingKey(entry.key)}
            onCancelRemove={() => setRemovingKey("")}
            onRemove={() => {
              setEntries((current) => current.filter((item) => item.key !== entry.key));
              setRemovingKey("");
              requestAnimationFrame(() => addButtonRef.current?.focus());
            }}
            onChange={(next) => setEntries((current) => current.map((item) => (item.key === entry.key ? { ...item, row: next } : item)))}
          />
        ))}
      </ul>
      {!entries.length && (
        <p className="wk-form-note">
          <b>Saving now empties the registry.</b> Central would hold no source for this tenant. Flux and Argo keep running in your clusters either way — only the record here goes.
        </p>
      )}
      <div className="wk-actions wk-actions-split">
        <button ref={addButtonRef} type="button" className="wk-btn wk-btn-secondary" onClick={add} disabled={saving}>
          <Icon name="plus" size={15} />Add source
        </button>
        <div className="wk-actions-right">
          <button ref={resetButtonRef} type="button" className="wk-btn wk-btn-secondary" onClick={reset} disabled={saving}>Reset</button>
          <button type="submit" className="wk-btn wk-btn-primary" disabled={saving} aria-busy={saving}>
            {saving ? "Saving…" : "Save registry"}
          </button>
        </div>
      </div>
      {confirmingReset && (
        <div className="wk-confirm" role="alertdialog" aria-labelledby="gitops-reset-title">
          <div>
            <b id="gitops-reset-title">Discard the edits in this form?</b>
            <span>Every row goes back to revision {record.sourcesRevision}, the registry central is serving.</span>
          </div>
          <div>
            <button ref={resetKeepButtonRef} type="button" className="wk-btn wk-btn-secondary" onClick={() => setConfirmingReset(false)}>Keep editing</button>
            <button type="button" className="wk-btn wk-btn-danger" onClick={applyReset}>Discard edits</button>
          </div>
        </div>
      )}
      {conflict && (
        <div className="wk-confirm" role="alertdialog" aria-labelledby="gitops-conflict-title">
          <div>
            <b id="gitops-conflict-title">Someone else replaced this registry</b>
            <span>
              Your edits are still in this form; nothing was written. Central is now serving revision {conflict.sourcesRevision} with{" "}
              {conflict.sources.length ? conflict.sources.map((source) => source.name).join(", ") : "no sources"}. Reviewing the latest
              registry replaces this form with it.
            </span>
          </div>
          <div>
            <button ref={conflictKeepButtonRef} type="button" className="wk-btn wk-btn-secondary" onClick={() => setConflict(null)}>Keep my edits</button>
            <button type="button" className="wk-btn wk-btn-danger" onClick={() => onReviewLatest(conflict)}>Review latest</button>
          </div>
        </div>
      )}
      {showErrors && invalid && <p className="wk-inline-error" role="alert">Fix the highlighted fields before saving. The whole registry is written at once.</p>}
      {saveError && <p className="wk-inline-error" role="alert">{saveError.message}</p>}
    </form>
  );
}

function PublisherCredentialDialog({ credential, onClose }) {
  const dialogRef = useRef(null);
  useEffect(() => {
    const opener = document.activeElement;
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
    if (!focusable.length) return;
    const first = focusable[0];
    const last = focusable[focusable.length - 1];
    if (event.shiftKey && document.activeElement === first) { event.preventDefault(); last.focus(); }
    else if (!event.shiftKey && document.activeElement === last) { event.preventDefault(); first.focus(); }
  };
  return (
    <div className="wk-reveal-layer">
      <div className="wk-reveal-scrim" aria-hidden="true" />
      <div ref={dialogRef} className="wk-reveal" role="dialog" aria-modal="true" aria-labelledby="publisher-credential-title" onKeyDown={onKeyDown}>
        <header className="wk-reveal-head"><div><h2 id="publisher-credential-title">{credential.kind === "rotate" ? "New publisher token" : "Catalog publisher created"}</h2><p>{credential.publisher.name} · <span className="wk-mono">{credential.publisher.id}</span></p></div><button type="button" className="wk-icon-btn" onClick={onClose} aria-label="Close and discard the shown publisher token"><Icon name="close" /></button></header>
        <p className="wk-reveal-once"><Icon name="alert" size={15} />This token is shown once. Copy it into your CI secret store now. Closing this dialog permanently removes it from this page.</p>
        {credential.kind === "rotate" && <p className="wk-reveal-note">The previous token has been revoked. Update every publisher job before its next catalog apply.</p>}
        <CopyBlock label="YSCALE_TOKEN" name="shown once" language="text" text={credential.token} />
        <footer className="wk-reveal-foot"><span>No token is placed in a command, URL, or browser storage.</span><button type="button" className="wk-btn wk-btn-primary" onClick={onClose}>Done, I stored it</button></footer>
      </div>
    </div>
  );
}

function CatalogPublishersPanel({ tenant, token }) {
  const tenantId = tenant.customer_id;
  const canManage = canManageTenant(tenant.role);
  const [status, setStatus] = useState("loading");
  const [publishers, setPublishers] = useState([]);
  const [error, setError] = useState(null);
  const [reload, setReload] = useState(0);
  const [name, setName] = useState("");
  const [busy, setBusy] = useState("");
  const [credential, setCredential] = useState(null);
  const [pendingRotate, setPendingRotate] = useState(null);
  const [pendingDelete, setPendingDelete] = useState(null);

  useEffect(() => {
    const controller = new AbortController();
    let current = true;
    setStatus("loading"); setError(null); setCredential(null); setPendingRotate(null); setPendingDelete(null);
    fetchCatalogPublishers({ token, tenantId, signal: controller.signal })
      .then((rows) => { if (current) { setPublishers(rows); setStatus("ready"); } })
      .catch((nextError) => { if (current && nextError?.name !== "AbortError") { setError(nextError); setStatus("error"); } });
    return () => { current = false; controller.abort(); };
  }, [token, tenantId, reload]);

  const create = async (event) => {
    event.preventDefault();
    const cleanName = catalogPublisherName(name);
    if (!cleanName) { setError(new Error("Give the publisher a name of 1–100 UTF-8 bytes on one line.")); return; }
    setBusy("create"); setError(null);
    try {
      const issued = await createCatalogPublisher({ token, tenantId, name: cleanName });
      setPublishers((rows) => [...rows, issued.publisher].sort((a, b) => a.name.localeCompare(b.name)));
      setName(""); setCredential({ kind: "create", ...issued });
    } catch (nextError) { setError(nextError); }
    finally { setBusy(""); }
  };
  const rotate = async () => {
    if (!pendingRotate) return;
    setBusy(`rotate:${pendingRotate.id}`); setError(null);
    try {
      const issued = await rotateCatalogPublisherCredential({ token, tenantId, publisherId: pendingRotate.id });
      setPublishers((rows) => rows.map((row) => row.id === issued.publisher.id ? issued.publisher : row));
      setPendingRotate(null); setCredential({ kind: "rotate", ...issued });
    } catch (nextError) { setError(nextError); }
    finally { setBusy(""); }
  };
  const revoke = async () => {
    if (!pendingDelete) return;
    setBusy(`delete:${pendingDelete.id}`); setError(null);
    try {
      await deleteCatalogPublisher({ token, tenantId, publisherId: pendingDelete.id });
      setPublishers((rows) => rows.filter((row) => row.id !== pendingDelete.id)); setPendingDelete(null);
    } catch (nextError) { setError(nextError); }
    finally { setBusy(""); }
  };

  const commandTenant = /^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$/.test(tenantId) ? tenantId : "TENANT_ID";
  const ciCommand = `# Store the issued value as the masked CI secret YSCALE_TOKEN.\nyscale catalog apply --server https://central.example.com --tenant ${commandTenant} -f catalog.yaml`;
  return (
    <Panel id="catalog-publishers-title" title="Catalog publishers" meta={status === "ready" ? `${publishers.length} active` : null}>
      <p className="wk-panel-copy">Issue narrow automation identities for CI jobs that publish this tenant's approved catalog. Their tokens never appear in this inventory.</p>
      {status === "loading" && <LoadingState label="Reading catalog publishers…" rows={2} />}
      {status === "error" && <ErrorState error={error} onRetry={() => setReload((value) => value + 1)} title="Catalog publishers could not be read." />}
      {status === "ready" && canManage && <form className="wk-publisher-create" onSubmit={create} noValidate><label className="wk-field"><span>Publisher name</span><input value={name} onChange={(event) => setName(event.target.value)} maxLength={100} autoComplete="off" placeholder="Release catalog CI" disabled={!!busy} /></label><button type="submit" className="wk-btn wk-btn-primary" disabled={!!busy} aria-busy={busy === "create"}>{busy === "create" ? "Creating…" : "Create publisher"}</button></form>}
      {status === "ready" && !publishers.length && <div className="wk-empty"><span className="wk-empty-icon" aria-hidden="true"><Icon name="gitops" size={20} /></span><div><h3>No publisher token is active</h3><p>{canManage ? "Create one for a CI job that owns catalog publication." : "An owner or admin can create a catalog publisher for CI."}</p></div></div>}
      {status === "ready" && publishers.length > 0 && <ul className="wk-publisher-list">{publishers.map((publisher) => <li key={publisher.id}><div><b>{publisher.name}</b><small>{publisher.id}</small></div><dl><div><dt>Created</dt><dd>{formatDate(publisher.createdAt)}</dd></div><div><dt>Last rotated</dt><dd>{formatDate(publisher.updatedAt)}</dd></div></dl>{canManage && <div className="wk-publisher-actions"><button type="button" className="wk-text-link" disabled={!!busy} aria-expanded={pendingRotate?.id === publisher.id} onClick={() => { setPendingDelete(null); setPendingRotate((row) => row?.id === publisher.id ? null : publisher); setError(null); }}>Rotate</button><button type="button" className="wk-text-link wk-text-danger" disabled={!!busy} aria-expanded={pendingDelete?.id === publisher.id} onClick={() => { setPendingRotate(null); setPendingDelete((row) => row?.id === publisher.id ? null : publisher); setError(null); }}>Revoke</button></div>}</li>)}</ul>}
      {pendingRotate && <div className="wk-confirm" role="alertdialog" aria-labelledby="publisher-rotate-title"><div><b id="publisher-rotate-title">Rotate {pendingRotate.name}'s token?</b><span>The current token stops working immediately.</span></div><div><button type="button" className="wk-btn wk-btn-secondary" disabled={!!busy} onClick={() => setPendingRotate(null)}>Keep current</button><button type="button" className="wk-btn wk-btn-primary" disabled={!!busy} aria-busy={busy.startsWith("rotate:")} onClick={rotate}>{busy.startsWith("rotate:") ? "Rotating…" : "Rotate and reveal"}</button></div></div>}
      {pendingDelete && <div className="wk-confirm" role="alertdialog" aria-labelledby="publisher-revoke-title"><div><b id="publisher-revoke-title">Revoke {pendingDelete.name}?</b><span>Every CI job using its token loses catalog write access immediately.</span></div><div><button type="button" className="wk-btn wk-btn-secondary" disabled={!!busy} onClick={() => setPendingDelete(null)}>Keep publisher</button><button type="button" className="wk-btn wk-btn-danger" disabled={!!busy} aria-busy={busy.startsWith("delete:")} onClick={revoke}>{busy.startsWith("delete:") ? "Revoking…" : "Revoke publisher"}</button></div></div>}
      {status === "ready" && !canManage && <Note>This {tenant.role} role sees safe publisher summaries only. Owners and admins create, rotate, and revoke publisher tokens.</Note>}
      {status === "ready" && <div className="wk-publisher-ci"><h3>Use from CI</h3><p>Put the one-time value in a masked secret named <code>YSCALE_TOKEN</code>, then run:</p><CopyBlock label="Catalog apply" name="CI command" language="sh" text={ciCommand} /></div>}
      {status === "ready" && error && <p className="wk-inline-error" role="alert">{apiMessage(error)}</p>}
      {credential && <PublisherCredentialDialog credential={credential} onClose={() => setCredential(null)} />}
    </Panel>
  );
}

// The same two facts the workload detail page prints, in one line: which
// cluster central granted, and whether the submission named it or let central
// choose. A record that carries neither says so rather than reading as auto.
function controllerTargetText(workload) {
  const cluster = workload.placement?.granted_cluster_id || workload.cluster_id || "";
  const mode = workload.placement?.mode;
  const modeText = mode === "auto"
    ? "automatic placement"
    : mode === "pinned" ? "explicit target" : "placement not recorded";
  return cluster ? `${cluster} · ${modeText}` : modeText;
}

function controllerReceiptText(workload) {
  return workload.burst_id ? `${workload.id} · ${workload.burst_id}` : `${workload.id} · no burst receipt recorded`;
}

// The one live thing on this page. Every row is a submission central recorded
// with a pending-pod origin, which is the only evidence the console has that
// something in the cluster — rather than a person — asked for capacity. It is
// not a repository read, and the origin does not name the controller.
function ControllerCapacity({ workloads, workloadStatus, workloadError, retry }) {
  const triggered = useMemo(() => controllerTriggeredWorkloads(workloads), [workloads]);
  const ready = workloadStatus === "ready";
  return (
    <Panel
      id="controller-capacity-title"
      title="Controller-triggered capacity"
      meta={ready ? `newest ${triggered.length}` : null}
      action={ready ? <button type="button" className="wk-text-link" onClick={() => retry?.()}>Read again</button> : null}
      className="wk-panel-flush"
    >
      <div className="wk-controller-intro">
        <ol className="wk-data-flow" aria-label="Controller-triggered capacity path">
          <li className="is-active"><b>01</b><span>Your controller</span></li>
          <li className="is-active"><b>02</b><span>Pending pod</span></li>
          <li className="is-run"><b>03</b><span>Yscale capacity</span></li>
        </ol>
        <p className="wk-panel-copy">
          A controller creates a Pending pod, the Connector reports it, and central records a node-only capacity request. Each row below
          is one of those records — the request central accepted, and what has become of it since.
        </p>
      </div>

      {workloadStatus === "loading" && <LoadingState label="Loading controller submissions…" rows={3} />}
      {workloadStatus === "error" && (
        <ErrorState error={workloadError} onRetry={retry} title="Controller-triggered submissions are unavailable." />
      )}
      {ready && !triggered.length && (
        <div className="wk-empty">
          <span className="wk-empty-icon" aria-hidden="true"><Icon name="gitops" size={20} /></span>
          <div>
            <h3>No Pending-pod submissions in this sample</h3>
            <p>
              Nothing in the workload records this console can read carries a pending-pod origin. A record from before central reported
              the field is not counted here either.
            </p>
          </div>
          <Link to="/workloads/history" className="wk-btn wk-btn-secondary">All records</Link>
        </div>
      )}
      {ready && triggered.length > 0 && (
        <ul className="wk-launch-list">
          {triggered.map((workload) => (
            <li key={workload.id} className="wk-launch">
              <span className="wk-launch-mark" aria-hidden="true"><Icon name="gitops" size={16} /></span>
              <div className="wk-launch-copy">
                <h3>{workload.spec?.metadata?.name || workload.id}</h3>
                <p>{controllerTargetText(workload)}</p>
                <span className="wk-launch-meta">
                  <WorkloadStatus status={workload.status} />
                  <time dateTime={workload.created_at || undefined}>{formatDate(workload.created_at)}</time>
                  <span>{controllerReceiptText(workload)}</span>
                </span>
              </div>
              <Link to={`/workloads/${encodeURIComponent(workload.id)}`} className="wk-btn wk-btn-secondary">
                Detail<Icon name="chevron" size={14} />
              </Link>
            </li>
          ))}
        </ul>
      )}

      <Note tone="warn">
        Connector-reported Pending-pod submissions only. KEDA, HPA, Argo, and a hand-applied manifest are indistinguishable at this
        seam, so the controller that created the pod is not recorded. At most {CONTROLLER_TRIGGERED_LIMIT} rows are listed, drawn from
        the up-to-100 newest workload records this console can fetch — a sample, not a total — and never from your repository: no sync
        state, no drift, no reconciliation.
      </Note>
    </Panel>
  );
}

export function GitOpsPage({ tenant, session, workloads, clusters = [], clusterStatus = "idle", workloadStatus, workloadError, retry }) {
  const [templateId, setTemplateId] = useState(WORKLOAD_TEMPLATES[0].id);
  const template = WORKLOAD_TEMPLATES.find((item) => item.id === templateId) || WORKLOAD_TEMPLATES[0];
  // The manifest on this page is meant to be committed as-is, so it carries the
  // namespace central would decide an unqualified submission against for this
  // tenant rather than one this tenant may not be authorized for at all.
  const yaml = useMemo(() => workloadYAML(template, initialWorkloadForm(template, workloadNamespaces(tenant))), [template, tenant]);
  const provenance = useMemo(() => provenanceSummary(workloads), [workloads]);

  const token = session.accessToken;
  const tenantId = tenant.customer_id;
  const canManage = canManageTenant(tenant.role);
  const canInspectRaw = canInspectRawWorkload(tenant.role);
  const [status, setStatus] = useState("loading");
  const [registry, setRegistry] = useState(null);
  const [error, setError] = useState(null);
  const [reloadKey, setReloadKey] = useState(0);
  const [editing, setEditing] = useState(false);
  const [unsaved, setUnsaved] = useState(false);
  const [confirmingClose, setConfirmingClose] = useState(false);
  const [savedRevision, setSavedRevision] = useState("");

  useEffect(() => {
    const controller = new AbortController();
    setStatus("loading");
    setError(null);
    setEditing(false);
    setConfirmingClose(false);
    setSavedRevision("");
    fetchGitOpsSources({ token, tenantId, signal: controller.signal })
      .then((record) => {
        setRegistry(record);
        setStatus("ready");
      })
      .catch((err) => {
        if (err?.name === "AbortError") return;
        setError(err);
        setStatus("error");
      });
    return () => controller.abort();
  }, [token, tenantId, reloadKey]);

  // An editor the reader put back by hand has nothing left to warn about, so
  // the prompt goes with the edits it was asking about.
  useEffect(() => { if (!unsaved) setConfirmingClose(false); }, [unsaved]);

  const closeEditor = () => {
    setEditing(false);
    setConfirmingClose(false);
  };

  const sourceCount = registry?.sources.length || 0;
  const announcement = status === "loading"
    ? "Reading this tenant's GitOps source registry."
    : status === "error"
      ? "The GitOps source registry could not be read."
      : registry
        ? `${sourceCount} configured source${sourceCount === 1 ? "" : "s"} at revision ${registry.sourcesRevision}.`
        : "";

  return (
    <>
      <TenantHeader
        tenant={tenant}
        title="GitOps"
        copy="Which repository, ref, and path each cluster is reconciled from — and the manifests to commit there."
      />

      <Note tone="warn">
        Registering a source records the coordinates with central and nothing else. Yscale does not read your repository, hold a Git
        credential, watch reconciliation, or resolve drift; Flux and Argo still do all of that inside your cluster.
      </Note>

      <ControllerCapacity
        workloads={workloads}
        workloadStatus={workloadStatus}
        workloadError={workloadError}
        retry={retry}
      />

      <Panel
        id="gitops-sources-title"
        title="Source registry"
        meta={status === "ready" && registry ? `revision ${registry.sourcesRevision} · ${sourceCount} configured` : null}
        action={status === "ready" && canManage ? (
          <button
            type="button"
            className="wk-btn wk-btn-secondary"
            onClick={() => {
              if (!editing) { setEditing(true); return; }
              if (unsaved) setConfirmingClose(true);
              else closeEditor();
            }}
          >
            {editing ? "Close editor" : "Edit sources"}
          </button>
        ) : null}
      >
        <p className="sr-only" role="status">{announcement}</p>
        {status === "loading" && <LoadingState label="Reading this tenant's GitOps sources…" rows={3} />}
        {status === "error" && (
          <ErrorState error={error} onRetry={() => setReloadKey((n) => n + 1)} title="The GitOps source registry could not be read." />
        )}
        {status === "ready" && registry && sourceCount === 0 && !editing && (
          <div className="wk-empty">
            <span className="wk-empty-icon" aria-hidden="true"><Icon name="gitops" size={20} /></span>
            <div>
              <h3>No source is registered</h3>
              <p>
                {canManage
                  ? "Record which repository, ref, and path each cluster is reconciled from. Recording it does not start reconciliation — Flux or Argo in that cluster still does."
                  : "No owner or admin has recorded which repository this tenant's clusters are reconciled from."}
              </p>
            </div>
          </div>
        )}
        {status === "ready" && registry && sourceCount > 0 && !editing && (
          <ul className="wk-source-list">
            {registry.sources.map((source) => (
              <SourceCard key={source.id} source={source} cluster={sourceClusterFact(source.clusterId, clusters, clusterStatus)} />
            ))}
          </ul>
        )}
        {status === "ready" && registry && savedRevision === registry.sourcesRevision && !editing && (
          <p className="wk-inline-note" role="status">Saved. Central now serves revision {registry.sourcesRevision} for this tenant.</p>
        )}
        {status === "ready" && registry && editing && (
          <SourceRegistryEditor
            key={registry.sourcesRevision}
            record={registry}
            token={token}
            tenantId={tenantId}
            clusters={clusters}
            clusterStatus={clusterStatus}
            onDirtyChange={setUnsaved}
            closing={confirmingClose}
            onKeepEditing={() => setConfirmingClose(false)}
            onDiscardEdits={closeEditor}
            onSaved={(saved) => {
              setRegistry(saved);
              setSavedRevision(saved.sourcesRevision);
              closeEditor();
            }}
            onReviewLatest={(current) => {
              setRegistry(current);
              setSavedRevision("");
              closeEditor();
            }}
          />
        )}
        {status === "ready" && registry && (
          <Note>
            Configured means these coordinates are on record with central. Sync state, last applied revision, and drift are not observed
            here — read those from Flux or Argo in the cluster.
          </Note>
        )}
        {status === "ready" && registry && !canManage && (
          <Note>This role reads every coordinate and changes none. Owners and admins register sources, and central refuses the write either way.</Note>
        )}
      </Panel>

      <CatalogPublishersPanel tenant={tenant} token={token} />

      {canInspectRaw ? <Panel
        id="manifest-title"
        title="Workload manifest"
        meta="yscale.sh/v1"
        action={
          <label className="wk-inline-select">
            <span className="sr-only">Template to render</span>
            <select value={templateId} onChange={(event) => setTemplateId(event.target.value)}>
              {WORKLOAD_TEMPLATES.map((item) => <option key={item.id} value={item.id}>{item.title}</option>)}
            </select>
            <Icon name="caret" size={14} />
          </label>
        }
      >
        <CopyBlock label={template.title} name={`${template.id}.yaml`} text={yaml} />
        <Note>
          The same document this console POSTs from the launch form, rendered with the template's defaults. Apply it with kubectl, Flux, or Argo — the Connector reconciles the Workload CR either way.
        </Note>
      </Panel> : <Panel id="manifest-title" title="Workload manifest" meta="restricted raw configuration">
        <p className="wk-panel-copy">This tenant can launch only the reviewed template defaults. Owners and admins can inspect and copy the raw Workload YAML used by GitOps tools.</p>
      </Panel>}

      {canInspectRaw ? <Panel id="keda-title" title="Autoscaler bridge" meta="KEDA, HPA, Deployment, Argo">
        <p className="wk-panel-copy">
          Anything that creates pods can burst without submitting a Workload. The Connector watches Pending pods; when one carries both the
          node selector and the burst template, it submits a nodeOnly capacity request and standard Kubernetes scheduling binds your pod to
          the burst once the kubelet joins.
        </p>
        <CopyBlock label="Pod template" name="deployment.yaml" text={KEDA_SNIPPET} />
        <Note>
          Both keys are required — the selector alone does not trigger a burst. The template fragment takes the same gpu, budget, and storage
          fields as a Workload spec.
        </Note>
      </Panel> : <Panel id="keda-title" title="Autoscaler bridge" meta="KEDA, HPA, Deployment, Argo">
        <p className="wk-panel-copy">The Connector can provision capacity for eligible Pending pods created by KEDA, HPA, Deployments, or Argo. Owners and admins can inspect the raw pod-template fragment and its required burst annotations.</p>
      </Panel>}

      <Panel id="provenance-title" title="Provenance rules">
        <p className="wk-panel-copy">
          Central records who submitted each workload and what authorized it, whether the submission came from this console, kubectl, or a
          pipeline. That record is what the audit journal and every workload's detail page read back.
        </p>
        <DetailList rows={[
          { label: "Submitter", value: "The account id of the human, or the cluster credential's principal kind. The identity provider's issuer and subject are never recorded." },
          { label: "Namespace", value: "Requested and granted are both kept, so a submission that was narrowed is visible as narrowed." },
          { label: "Rule", value: "The rule name and version that decided the submission." },
          { label: "On this tenant", value: `${provenance.recorded} of ${provenance.total} readable records carry a decision; ${provenance.submitters} distinct submitter${provenance.submitters === 1 ? "" : "s"}.` },
        ]} />
      </Panel>
    </>
  );
}

/* ------------------------------------------------------------------- usage */

export function UsagePage({ tenant, session, workloads, workloadStatus, workloadError, retry }) {
  const stats = useMemo(() => summarizeRuns(workloads), [workloads]);
  const byTemplate = useMemo(() => usageByTemplate(workloads), [workloads]);
  const byMonth = useMemo(() => usageByMonth(workloads), [workloads]);
  const limits = useMemo(() => limitRows(tenant.limits), [tenant.limits]);
  const ready = workloadStatus === "ready";
  return (
    <>
      <TenantHeader
        tenant={tenant}
        title="Usage"
        copy="A live snapshot from central, plus a separate history sample from the workload records this console can read."
      />
      <LiveUsage tenant={tenant} session={session} />
      <StatTiles
        label="History sample"
        ready={workloadStatus !== "loading"}
        tiles={[
          { key: "total", icon: "workloads", label: "Records", value: stats.total, note: "readable in this tenant" },
          { key: "active", icon: "clock", label: "Active", value: stats.active, note: "not yet terminal" },
          { key: "outcome", icon: "check", label: "Succeeded / failed", value: `${stats.succeeded} / ${stats.failed}`, note: "terminal records" },
          { key: "cost", icon: "spend", label: "Recorded estimates", value: stats.estimateRecords ? formatUSD(stats.estimatedUSD) : "not recorded", note: `frozen on ${stats.estimateRecords} of ${stats.total} records` },
        ]}
      />
      <Note tone="warn">
        History uses only the up-to-100 newest workload records. It is a sample, not a live total. Each cost is an
        estimate central froze at teardown, not billing.
      </Note>
      {workloadStatus === "error" && <ErrorState error={workloadError} onRetry={retry} title="The workload history sample is unavailable." />}

      <Panel id="plan-title" title="Plan and limits" meta={tenant.plan || "plan not reported"}>
        {limits.length ? (
          <DetailList rows={limits.map((row) => ({
            label: row.label,
            value: row.unlimited ? "No ceiling reported (0)" : limitValue(row),
          }))} />
        ) : (
          <Note tone="warn">This tenant's summary reported no numeric limits.</Note>
        )}
        <Note>Reported by the account API for {tenant.customer_id}. See <Link to="/workloads/policies" className="wk-text-link">Policies</Link> for what each ceiling refuses.</Note>
      </Panel>

      <Panel id="by-template-title" title="By template" meta={ready ? `${byTemplate.length} shapes` : null} className="wk-panel-flush">
        {workloadStatus === "loading" && <LoadingState label="Loading usage…" rows={3} />}
        {ready && !byTemplate.length && <p className="wk-panel-copy">No workload records in this tenant yet.</p>}
        {ready && byTemplate.length > 0 && (
          <div className="wk-table-wrap">
            <table className="wk-table">
              <thead>
                <tr>
                  <th scope="col">Template</th>
                  <th scope="col">Records</th>
                  <th scope="col">Active</th>
                  <th scope="col" className="wk-col-cost">Est. cost</th>
                </tr>
              </thead>
              <tbody>
                {byTemplate.map((row) => (
                  <tr key={row.id}>
                    <th scope="row"><span>{row.title}<small>{row.id}</small></span></th>
                    <td>{row.runs}</td>
                    <td>{row.active}</td>
                    <td className="wk-col-cost">{row.estimateRecords ? formatUSD(row.estimatedUSD) : "not recorded"}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </Panel>

      <Panel id="by-month-title" title="By month requested" meta={ready ? `${byMonth.length} month${byMonth.length === 1 ? "" : "s"}` : null} className="wk-panel-flush">
        {workloadStatus === "loading" && <LoadingState label="Loading months…" rows={3} />}
        {ready && !byMonth.length && <p className="wk-panel-copy">No workload records in this tenant yet.</p>}
        {ready && byMonth.length > 0 && (
          <div className="wk-table-wrap">
            <table className="wk-table">
              <thead>
                <tr>
                  <th scope="col">Month (UTC)</th>
                  <th scope="col">Records</th>
                  <th scope="col">Succeeded</th>
                  <th scope="col">Failed or cancelled</th>
                  <th scope="col" className="wk-col-cost">Est. cost</th>
                </tr>
              </thead>
              <tbody>
                {byMonth.map((row) => (
                  <tr key={row.key}>
                    <th scope="row"><span className="wk-mono">{row.key}</span></th>
                    <td>{row.runs}</td>
                    <td>{row.succeeded}</td>
                    <td>{row.failed}</td>
                    <td className="wk-col-cost">{row.estimateRecords ? formatUSD(row.estimatedUSD) : "not recorded"}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
        <Note>Bucketed on the month a workload was requested, not the month it ran or the month it would be billed in.</Note>
      </Panel>
    </>
  );
}

function LiveUsage({ tenant, session }) {
  const token = session.accessToken;
  const tenantId = tenant.customer_id;
  const [status, setStatus] = useState("loading");
  const [usage, setUsage] = useState(null);
  const [error, setError] = useState(null);
  const [reloadKey, setReloadKey] = useState(0);
  const limits = useMemo(() => limitRows(usage?.limits), [usage?.limits]);

  useEffect(() => {
    const controller = new AbortController();
    setStatus("loading");
    setError(null);
    fetchUsage({ token, tenantId, signal: controller.signal })
      .then((data) => {
        setUsage(data || null);
        setStatus("ready");
      })
      .catch((err) => {
        if (err?.name === "AbortError") return;
        setError(err);
        setStatus("error");
      });
    return () => controller.abort();
  }, [token, tenantId, reloadKey]);

  return (
    <Panel id="live-usage-title" title="Live now" meta={usage?.observed_at ? `observed ${formatDate(usage.observed_at, false)}` : "central snapshot"}>
      {status === "loading" && <LoadingState label="Loading live usage…" rows={2} />}
      {status === "error" && <ErrorState error={error} onRetry={() => setReloadKey((n) => n + 1)} title="The live usage snapshot is unavailable." />}
      {status === "ready" && usage && (
        <>
          <DetailList rows={[
            { label: "Running bursts", value: String(usage.running_bursts ?? 0) },
            { label: "Hourly rate", value: formatUSD(usage.hourly_usd || 0) },
            { label: "24-hour projection", value: `${formatUSD(usage.projected_daily_usd || 0)} if this snapshot held for 24 hours` },
            { label: "Observed", value: usage.observed_at ? formatDate(usage.observed_at) : "not reported" },
          ]} />
          {limits.length > 0 && (
            <DetailList rows={limits.map((row) => ({ label: `Limit · ${row.label}`, value: row.unlimited ? "No ceiling reported (0)" : limitValue(row) }))} />
          )}
          <Note>This is a point-in-time central snapshot, not a bill or a historical total.</Note>
        </>
      )}
    </Panel>
  );
}

/* -------------------------------------------------------------------- team */

const ROLE_ORDER = { owner: 0, admin: 1, member: 2, viewer: 3 };
const OWNER_ROLE_OPTIONS = ["owner", "admin", "member", "viewer"];
const ADMIN_ROLE_OPTIONS = ["admin", "member", "viewer"];

function sortMembers(members) {
  return [...members].sort((a, b) => {
    const byRole = (ROLE_ORDER[a.role] ?? 9) - (ROLE_ORDER[b.role] ?? 9);
    if (byRole) return byRole;
    return (a.email || a.account_id).localeCompare(b.email || b.account_id);
  });
}

function roleOptionsForActor(role) {
  if (role === "owner") return OWNER_ROLE_OPTIONS;
  if (role === "admin") return ADMIN_ROLE_OPTIONS;
  return [];
}

function defaultMemberRole(role) {
  const options = roleOptionsForActor(role);
  return options.includes("member") ? "member" : options[0] || "member";
}

function canEditMemberRole(actorRole, member) {
  if (!canManageTenant(actorRole)) return false;
  return !(actorRole === "admin" && member.role === "owner");
}

function mergeMember(prev, member) {
  if (!member?.account_id) return prev;
  const seen = prev.some((item) => item.account_id === member.account_id);
  const next = seen ? prev.map((item) => (item.account_id === member.account_id ? { ...item, ...member } : item)) : [...prev, member];
  return sortMembers(next);
}

function roleOptionsForMember(options, current) {
  if (!current || options.includes(current)) return options;
  return [current, ...options];
}

export function TeamPage({ account, tenant, session }) {
  const token = session.accessToken;
  const tenantId = tenant.customer_id;
  const canManage = canManageTenant(tenant.role);
  const [members, setMembers] = useState([]);
  const [nextAfter, setNextAfter] = useState("");
  const [status, setStatus] = useState("loading");
  const [error, setError] = useState(null);
  const [moreError, setMoreError] = useState(null);
  const [loadingMore, setLoadingMore] = useState(false);
  const [pending, setPending] = useState(null);
  const [busy, setBusy] = useState(false);
  const [removeError, setRemoveError] = useState(null);
  const [addEmail, setAddEmail] = useState("");
  const [addRole, setAddRole] = useState(defaultMemberRole(tenant.role));
  const [addBusy, setAddBusy] = useState(false);
  const [addError, setAddError] = useState(null);
  const [roleBusy, setRoleBusy] = useState({});
  const [roleErrors, setRoleErrors] = useState({});
  const [roleDrafts, setRoleDrafts] = useState({});
  const [announcement, setAnnouncement] = useState("");
  const [reloadKey, setReloadKey] = useState(0);
  const confirmRef = useRef(null);
  const addEmailRef = useRef(null);
  const [beginPageRequest, cancelPageRequest] = usePageRequest();
  const roleOptions = roleOptionsForActor(tenant.role);

  useEffect(() => {
    const controller = new AbortController();
    setStatus("loading");
    setError(null);
    setMembers([]);
    setNextAfter("");
    setPending(null);
    setRemoveError(null);
    setAddError(null);
    setRoleErrors({});
    setRoleBusy({});
    setRoleDrafts({});
    fetchMembers({ token, tenantId, signal: controller.signal })
      .then((data) => {
        setMembers(sortMembers(data?.members || []));
        setNextAfter(data?.next_after || "");
        setStatus("ready");
      })
      .catch((err) => {
        if (err?.name === "AbortError") return;
        setError(err);
        setStatus("error");
      });
    return () => {
      controller.abort();
      cancelPageRequest();
    };
  }, [token, tenantId, reloadKey, cancelPageRequest]);

  useEffect(() => {
    const allowed = roleOptionsForActor(tenant.role);
    setAddRole((current) => (allowed.includes(current) ? current : defaultMemberRole(tenant.role)));
  }, [tenant.role]);

  useEffect(() => {
    if (pending) confirmRef.current?.focus();
  }, [pending]);

  // A failed page keeps the members already on screen and leaves the cursor
  // where it was, so retrying resumes rather than restarts. An aborted one is
  // not a failure: the tenant it belonged to is gone.
  const loadMore = () => {
    const signal = beginPageRequest();
    setLoadingMore(true);
    setMoreError(null);
    fetchMembers({ token, tenantId, after: nextAfter, signal })
      .then((data) => {
        setMembers((prev) => sortMembers([...prev, ...(data?.members || [])]));
        setNextAfter(data?.next_after || "");
      })
      .catch((err) => { if (err?.name !== "AbortError") setMoreError(err); })
      .finally(() => setLoadingMore(false));
  };

  const confirmRemove = async () => {
    const member = pending;
    if (!member) return;
    setBusy(true);
    setRemoveError(null);
    try {
      await removeMember({ token, tenantId, accountId: member.account_id });
      setMembers((prev) => prev.filter((item) => item.account_id !== member.account_id));
      setAnnouncement(`${member.email || member.account_id} was removed from ${tenantId}.`);
      setPending(null);
    } catch (err) {
      // The API is the authority: a refusal here is the real answer, whatever
      // the roster believed about this role.
      setRemoveError(err);
      setAnnouncement(`${member.email || member.account_id} was not removed.`);
    } finally {
      setBusy(false);
    }
  };

  const submitAddMember = async (event) => {
    event.preventDefault();
    const email = addEmail.trim();
    if (!email) {
      setAddError(new Error("Enter the account email."));
      addEmailRef.current?.focus();
      return;
    }
    setAddBusy(true);
    setAddError(null);
    try {
      const member = await addMember({ token, tenantId, email, role: addRole });
      if (!member?.account_id || !member.email) {
        setReloadKey((n) => n + 1);
      } else {
        setMembers((prev) => mergeMember(prev, member));
      }
      setAddEmail("");
      setAnnouncement(`${email} now has ${addRole} access to ${tenantId}.`);
      addEmailRef.current?.focus();
    } catch (err) {
      setAddError(err);
      setAnnouncement(`${email} was not added.`);
      addEmailRef.current?.focus();
    } finally {
      setAddBusy(false);
    }
  };

  const changeRole = async (member, role) => {
    if (!role || role === member.role) return;
    setRoleDrafts((prev) => ({ ...prev, [member.account_id]: role }));
    setRoleBusy((prev) => ({ ...prev, [member.account_id]: true }));
    setRoleErrors((prev) => ({ ...prev, [member.account_id]: null }));
    try {
      const updated = await updateMemberRole({ token, tenantId, accountId: member.account_id, role });
      if (!updated?.account_id) {
        setAnnouncement(`Refreshing ${member.email || member.account_id}'s role from central.`);
        setReloadKey((n) => n + 1);
        return;
      }
      setMembers((prev) => mergeMember(prev, updated));
      setAnnouncement(`${member.email || member.account_id} is now ${updated.role || role}.`);
    } catch (err) {
      setRoleErrors((prev) => ({ ...prev, [member.account_id]: err }));
      setAnnouncement(`${member.email || member.account_id} stayed ${member.role}.`);
    } finally {
      setRoleDrafts((prev) => {
        const next = { ...prev };
        delete next[member.account_id];
        return next;
      });
      setRoleBusy((prev) => ({ ...prev, [member.account_id]: false }));
    }
  };

  return (
    <>
      <TenantHeader
        tenant={tenant}
        title="Team"
        copy="Everyone with access to this tenant, and what their role lets them do."
        action={<Link to="/account" className="wk-btn wk-btn-secondary">Access ledger<Icon name="external" size={14} /></Link>}
      />
      <p className="sr-only" role="status" aria-live="polite">{announcement}</p>

      {canManage && (
        <Panel id="add-member-title" title="Add existing account" meta="signed-in accounts only">
          <form className="wk-team-add" onSubmit={submitAddMember} noValidate>
            <label className={`wk-field${addError ? " has-error" : ""}`}>
              <span>Email</span>
              <input
                ref={addEmailRef}
                type="email"
                value={addEmail}
                onChange={(event) => { setAddEmail(event.target.value); setAddError(null); }}
                placeholder="person@example.com"
                autoComplete="email"
                disabled={addBusy}
              />
              <small>The person must have signed in to Yscale once.</small>
            </label>
            <label className="wk-field">
              <span>Role</span>
              <select value={addRole} onChange={(event) => setAddRole(event.target.value)} disabled={addBusy}>
                {roleOptions.map((role) => <option key={role} value={role}>{role}</option>)}
              </select>
            </label>
            <button type="submit" className="wk-btn wk-btn-primary" disabled={addBusy || !roleOptions.length} aria-busy={addBusy}>
              {addBusy ? "Adding…" : "Add account"}
            </button>
            {addError && <p className="wk-inline-error" role="alert">{apiMessage(addError)}</p>}
          </form>
        </Panel>
      )}

      <Panel
        id="roster-title"
        title="Members"
        meta={status === "ready" ? `${members.length} shown${nextAfter ? " · more available" : ""}` : null}
        className="wk-panel-flush"
      >
        {status === "loading" && <LoadingState label={`Loading the roster for ${tenantId}…`} rows={4} />}
        {status === "error" && <ErrorState error={error} onRetry={() => setReloadKey((n) => n + 1)} title="The roster is unavailable." />}
        {status === "ready" && !members.length && (
          <div className="wk-empty">
            <span className="wk-empty-icon" aria-hidden="true"><Icon name="team" size={20} /></span>
            <div>
              <h3>No members listed</h3>
              <p>The account API returned no roster for this tenant.</p>
            </div>
          </div>
        )}
        {status === "ready" && members.length > 0 && (
          <div className="wk-table-wrap">
            <table className="wk-table wk-table-team">
              <caption className="sr-only">Members of tenant {tenantId}</caption>
              <thead>
                <tr>
                  <th scope="col">Member</th>
                  <th scope="col">Role</th>
                  <th scope="col">Since</th>
                  {canManage && <th scope="col"><span className="sr-only">Actions</span></th>}
                </tr>
              </thead>
              <tbody>
                {members.map((member) => {
                  const self = member.account_id === account.account_id;
                  const confirming = pending?.account_id === member.account_id;
                  const editableRole = canEditMemberRole(tenant.role, member);
                  const memberBusy = !!roleBusy[member.account_id] || (busy && confirming);
                  const memberRoleOptions = roleOptionsForMember(roleOptions, member.role);
                  const displayedRole = roleDrafts[member.account_id] || member.role || "member";
                  return (
                    <tr key={member.account_id} className={confirming ? "is-confirming" : undefined}>
                      <th scope="row">
                        <span>
                          {member.name || member.email || member.account_id}
                          <small>{member.email && member.name ? member.email : member.account_id}{self ? " · you" : ""}</small>
                        </span>
                      </th>
                      <td>
                        {editableRole ? (
                          <label className="wk-inline-select wk-role-select">
                            <span className="sr-only">Role for {member.email || member.account_id}</span>
                            <select
                              value={displayedRole}
                              onChange={(event) => changeRole(member, event.target.value)}
                              disabled={memberBusy}
                              aria-invalid={roleErrors[member.account_id] ? "true" : undefined}
                            >
                              {memberRoleOptions.map((role) => (
                                <option key={role} value={role} disabled={!roleOptions.includes(role)}>
                                  {roleOptions.includes(role) ? role : `${role} (current)`}
                                </option>
                              ))}
                            </select>
                            <Icon name="chevron" size={14} />
                          </label>
                        ) : (
                          <span className="wk-chip">{member.role || "member"}</span>
                        )}
                        {roleErrors[member.account_id] && <small className="wk-row-error" role="alert">{apiMessage(roleErrors[member.account_id])}</small>}
                      </td>
                      <td><time dateTime={member.created_at}>{formatDate(member.created_at, false)}</time></td>
                      {canManage && (
                        <td className="wk-cell-actions">
                          {!self && (
                            <button
                              type="button"
                              className="wk-text-link"
                              aria-expanded={confirming}
                              disabled={memberBusy}
                              onClick={() => { setRemoveError(null); setPending(confirming ? null : member); }}
                            >
                              {confirming ? "Cancel" : "Remove"}
                            </button>
                          )}
                        </td>
                      )}
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        )}

        {pending && (
          <div className="wk-confirm" role="alertdialog" aria-labelledby="remove-title">
            <div>
              <b id="remove-title">Remove {pending.email || pending.account_id}?</b>
              <span>They lose access to {tenantId} immediately. Workloads they already submitted are not cancelled.</span>
              {removeError && <span className="wk-confirm-error" role="alert">{apiMessage(removeError)}</span>}
            </div>
            <div>
              <button type="button" className="wk-btn wk-btn-secondary" onClick={() => setPending(null)} disabled={busy}>Keep access</button>
              <button ref={confirmRef} type="button" className="wk-btn wk-btn-danger" onClick={confirmRemove} disabled={busy} aria-busy={busy}>
                {busy ? "Removing…" : "Confirm removal"}
              </button>
            </div>
          </div>
        )}

        {nextAfter && status === "ready" && (
          <div className="wk-more">
            <button type="button" className="wk-btn wk-btn-secondary" onClick={loadMore} disabled={loadingMore} aria-busy={loadingMore}>
              {loadingMore ? "Loading…" : "Load more members"}
            </button>
            {moreError && <span className="wk-inline-error" role="alert">{apiMessage(moreError)}</span>}
          </div>
        )}
      </Panel>

      <Panel id="roles-title" title="What each role can do" className="wk-panel-flush">
        <div className="wk-table-wrap">
          <table className="wk-table wk-table-roles">
            <thead>
              <tr>
                <th scope="col">Role</th>
                <th scope="col">Can</th>
                <th scope="col">Cannot</th>
              </tr>
            </thead>
            <tbody>
              {ROLE_MEANINGS.map((row) => (
                <tr key={row.role}>
                  <th scope="row"><span className="wk-chip">{row.role}</span></th>
                  <td>{row.can}</td>
                  <td>{row.limit}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
        <Note>
          {canManage
            ? "Central is authoritative for add, role, and removal refusals. This console updates from returned member records and reloads the roster only if central omits profile fields."
            : `Your role in this tenant is ${tenant.role}, so central does not expose roster changes to you.`}
        </Note>
      </Panel>
    </>
  );
}

/* ------------------------------------------------------------------- audit */

const ACTION_LABELS = {
  "workload.submit": "Workload submitted",
  "workload.retry": "Workload retry authorized",
  "workload.logs": "Workload logs read",
  "workload.cancel": "Workload cancelled by request",
  "workload.started": "Workload started",
  "workload.completed": "Workload completed",
  "workload.failed": "Workload failed",
  "workload.cancelled": "Workload cancellation observed",
  "workload.reaped": "Workload reaped",
  "workload.expired": "Workload expired",
  "membership.grant": "Member granted access",
  "membership.role_change": "Member role changed",
  "membership.remove": "Member access removed",
  "tenant.workload_namespaces": "Authorized namespaces set",
};

function auditDetailText(event) {
  const detail = event.detail || {};
  const parts = [];
  if (detail.reason) parts.push(statusLabel(detail.reason));
  if (detail.previous_role && detail.role) parts.push(`${detail.previous_role} → ${detail.role}`);
  else if (detail.role) parts.push(`role ${detail.role}`);
  if (detail.requested_namespace || detail.granted_namespace) {
    const requested = detail.requested_namespace || "unset";
    const granted = detail.granted_namespace || "none";
    parts.push(requested === granted ? `namespace ${granted}` : `namespace ${requested} → ${granted}`);
  }
  if (detail.namespaces?.length) parts.push(`namespaces ${detail.namespaces.join(", ")}`);
  if (detail.rule) parts.push(`${detail.rule}${detail.rule_version ? ` ${detail.rule_version}` : ""}`);
  if (detail.status) parts.push(`status ${detail.status}`);
  if (detail.burst_id) parts.push(detail.burst_id);
  return parts.join(" · ") || "No detail recorded.";
}

function auditActor(event) {
  const actor = event.actor || {};
  if (actor.account_id) return actor.account_id;
  return actor.kind ? `${actor.kind} principal` : "not recorded";
}

export function AuditPage({ tenant, session }) {
  const token = session.accessToken;
  const tenantId = tenant.customer_id;
  const entitled = canManageTenant(tenant.role);
  const [events, setEvents] = useState([]);
  const [nextAfter, setNextAfter] = useState("");
  const [status, setStatus] = useState(entitled ? "loading" : "forbidden");
  const [error, setError] = useState(null);
  const [moreError, setMoreError] = useState(null);
  const [loadingMore, setLoadingMore] = useState(false);
  const [reloadKey, setReloadKey] = useState(0);
  const [beginPageRequest, cancelPageRequest] = usePageRequest();

  useEffect(() => {
    if (!entitled) {
      setStatus("forbidden");
      setEvents([]);
      setNextAfter("");
      return cancelPageRequest;
    }
    const controller = new AbortController();
    setStatus("loading");
    setError(null);
    setEvents([]);
    setNextAfter("");
    fetchAudit({ token, tenantId, signal: controller.signal })
      .then((page) => {
        setEvents(page.events);
        setNextAfter(page.next_after);
        setStatus("ready");
      })
      .catch((err) => {
        if (err?.name === "AbortError") return;
        // Central is the authority on entitlement; a 403 here overrides what
        // the role on the account summary suggested.
        setStatus(err?.status === 403 ? "forbidden" : "error");
        setError(err);
      });
    return () => {
      controller.abort();
      cancelPageRequest();
    };
  }, [token, tenantId, entitled, reloadKey, cancelPageRequest]);

  // The cursor names a row in this tenant's journal and nowhere else, so a page
  // still in flight when the tenant changes is aborted rather than appended.
  const loadOlder = useCallback(() => {
    const signal = beginPageRequest();
    setLoadingMore(true);
    setMoreError(null);
    fetchAudit({ token, tenantId, after: nextAfter, signal })
      .then((page) => {
        setEvents((prev) => [...prev, ...page.events]);
        setNextAfter(page.next_after);
      })
      .catch((err) => { if (err?.name !== "AbortError") setMoreError(err); })
      .finally(() => setLoadingMore(false));
  }, [token, tenantId, nextAfter, beginPageRequest]);

  return (
    <>
      <TenantHeader
        tenant={tenant}
        title="Audit"
        copy="The tenant's governance journal: what was submitted, what was refused, and whose access changed."
      />

      {status === "forbidden" && (
        <ForbiddenState
          title="The journal is a management surface."
          detail={error?.message || `Reading the audit journal for ${tenantId} requires the owner or admin role. Ask an owner of this tenant if you need it.`}
          role={tenant.role}
        />
      )}
      {status === "error" && <ErrorState error={error} onRetry={() => setReloadKey((n) => n + 1)} title="The audit journal is unavailable." />}

      {status !== "forbidden" && status !== "error" && (
        <Panel
          id="journal-title"
          title="Journal"
          meta={status === "ready" ? `${events.length} event${events.length === 1 ? "" : "s"}${nextAfter ? " · older available" : ""}` : null}
          className="wk-panel-flush"
        >
          {status === "loading" && <LoadingState label={`Loading the audit journal for ${tenantId}…`} rows={6} />}
          {status === "ready" && !events.length && (
            <div className="wk-empty">
              <span className="wk-empty-icon" aria-hidden="true"><Icon name="audit" size={20} /></span>
              <div>
                <h3>Nothing recorded yet</h3>
                <p>This tenant has no journal entries. An empty journal is not a missing one.</p>
              </div>
            </div>
          )}
          {status === "ready" && events.length > 0 && (
            <>
              <div className="wk-table-wrap wk-table-wrap-audit">
                <table className="wk-table wk-table-audit">
                  <caption className="sr-only">Audit journal for tenant {tenantId}, newest first</caption>
                  <thead>
                    <tr>
                      <th scope="col">When</th>
                      <th scope="col">Action</th>
                      <th scope="col">Outcome</th>
                      <th scope="col">Actor</th>
                      <th scope="col">Target</th>
                      <th scope="col">Detail</th>
                    </tr>
                  </thead>
                  <tbody>
                    {events.map((event) => (
                      <tr key={event.id}>
                        <th scope="row"><time dateTime={event.at}>{formatDate(event.at)}</time></th>
                        <td>{ACTION_LABELS[event.action] || statusLabel(event.action)}<small className="wk-mono">{event.action}</small></td>
                        <td><span className={`wk-outcome is-${event.outcome}`}>{event.outcome}</span></td>
                        <td className="wk-mono">{auditActor(event)}</td>
                        <td className="wk-mono">{event.target_id || event.target_kind || "—"}</td>
                        <td>{auditDetailText(event)}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
              <ul className="wk-audit-cards">
                {events.map((event) => (
                  <li key={event.id}>
                    <div className="wk-audit-card-top">
                      <b>{ACTION_LABELS[event.action] || statusLabel(event.action)}</b>
                      <span className={`wk-outcome is-${event.outcome}`}>{event.outcome}</span>
                    </div>
                    <p>{auditDetailText(event)}</p>
                    <div className="wk-audit-card-foot">
                      <time dateTime={event.at}>{formatDate(event.at)}</time>
                      <span>{auditActor(event)}</span>
                    </div>
                  </li>
                ))}
              </ul>
            </>
          )}
          {status === "ready" && nextAfter && (
            <div className="wk-more">
              <button type="button" className="wk-btn wk-btn-secondary" onClick={loadOlder} disabled={loadingMore} aria-busy={loadingMore}>
                {loadingMore ? "Loading…" : "Load older events"}
              </button>
              {moreError && <span className="wk-inline-error" role="alert">{apiMessage(moreError)}</span>}
            </div>
          )}
          <Note>
            Newest first, straight from central. Rows carry no token, no spec, no environment value, and no identity-provider subject — the
            journal has no field to put them in.
          </Note>
        </Panel>
      )}
    </>
  );
}
