import { useCallback, useEffect, useRef, useState } from "react";

import { ErrorState, Icon, LoadingState, PageHeader, Panel, formatUSD } from "../components/ConsoleKit.jsx";
import { ApiError } from "../lib/apiError.js";
import { formatMicroUSD } from "../lib/billingApi.js";
import {
  appendOperatorTenantPage,
  fetchOperatorTenants,
  createServiceCreditKey,
  grantServiceCredit,
  operatorMutationAccess,
  replaceOperatorTenant,
  tenantLimitDraft,
  tenantLimitValues,
  usdToMicroUSD,
  updateOperatorTenantLimits,
} from "../lib/operatorApi.js";

const PAGE_SIZE = 50;

function limitLabel(value, currency = false) {
  if (value === 0) return "Unlimited";
  return currency ? `${formatUSD(value)}/hr` : String(value);
}

function TenantGuardrailCards({ tenants, onEdit, onCredit }) {
  return <ul className="wk-tenant-guardrail-cards">{tenants.map((tenant) => (
    <li key={tenant.tenantId}>
      <header><div><b>{tenant.tenantName}</b><small>{tenant.tenantId}</small></div><span className="wk-chip">{tenant.plan}</span></header>
      <dl>
        <div><dt>Running now</dt><dd>{tenant.runningBursts}</dd></div>
        <div><dt>Current rate</dt><dd>{formatUSD(tenant.hourlyUsd)}/hr</dd></div>
        <div><dt>Projected day</dt><dd>{formatUSD(tenant.projectedDailyUsd)}</dd></div>
        <div><dt>Concurrent ceiling</dt><dd>{limitLabel(tenant.limits.maxConcurrentBursts)}</dd></div>
        <div><dt>Hourly ceiling</dt><dd>{limitLabel(tenant.limits.maxHourlyUsd, true)}</dd></div>
      </dl>
      <div className="wk-tenant-actions"><button type="button" className="wk-btn wk-btn-secondary" onClick={(event) => onEdit(tenant, event.currentTarget)}>Edit guardrails</button><button type="button" className="wk-btn wk-btn-secondary" onClick={(event) => onCredit(tenant, event.currentTarget)}>Grant credit</button></div>
    </li>
  ))}</ul>;
}

function TenantGuardrailTable({ tenants, onEdit, onCredit }) {
  return <>
    <div className="wk-table-wrap wk-table-wrap-tenant-guardrails"><table className="wk-table wk-table-tenant-guardrails">
      <thead><tr><th scope="col">Tenant</th><th scope="col">Plan</th><th scope="col">Running</th><th scope="col">Current rate</th><th scope="col">Projected day</th><th scope="col">Concurrent ceiling</th><th scope="col">Hourly ceiling</th><th scope="col"><span className="sr-only">Actions</span></th></tr></thead>
      <tbody>{tenants.map((tenant) => <tr key={tenant.tenantId}>
        <th scope="row"><span>{tenant.tenantName}<small>{tenant.tenantId}</small></span></th>
        <td><span className="wk-chip">{tenant.plan}</span></td>
        <td className="wk-mono">{tenant.runningBursts}</td>
        <td className="wk-mono">{formatUSD(tenant.hourlyUsd)}/hr</td>
        <td className="wk-mono">{formatUSD(tenant.projectedDailyUsd)}</td>
        <td className="wk-guardrail-limit">{limitLabel(tenant.limits.maxConcurrentBursts)}</td>
        <td className="wk-guardrail-limit">{limitLabel(tenant.limits.maxHourlyUsd, true)}</td>
        <td><div className="wk-tenant-actions"><button type="button" className="wk-btn wk-btn-secondary" onClick={(event) => onEdit(tenant, event.currentTarget)}>Edit</button><button type="button" className="wk-btn wk-btn-secondary" onClick={(event) => onCredit(tenant, event.currentTarget)}>Credit</button></div></td>
      </tr>)}</tbody>
    </table></div>
    <TenantGuardrailCards tenants={tenants} onEdit={onEdit} onCredit={onCredit} />
  </>;
}

function ServiceCreditDialog({ tenant, session, returnFocus, onCancel, onGranted, onSessionExpired, onOperatorUnavailable }) {
  const [amount, setAmount] = useState("");
  const [amountMicroUsd, setAmountMicroUsd] = useState(null);
  const [step, setStep] = useState("edit");
  const [error, setError] = useState(null);
  const [posting, setPosting] = useState(false);
  const keyRef = useRef(null);
  const dialogRef = useRef(null);
  const firstRef = useRef(null);
  const confirmRef = useRef(null);
  useEffect(() => {
    const previousOverflow = document.body.style.overflow;
    document.body.style.overflow = "hidden";
    const frame = requestAnimationFrame(() => firstRef.current?.focus());
    return () => { cancelAnimationFrame(frame); document.body.style.overflow = previousOverflow; if (returnFocus instanceof HTMLElement && document.contains(returnFocus)) returnFocus.focus(); };
  }, []);
  useEffect(() => { if (step === "confirm") requestAnimationFrame(() => confirmRef.current?.focus()); }, [step]);
  const review = (event) => {
    event.preventDefault();
    const result = usdToMicroUSD(amount);
    if (result.error) { setError(new Error(result.error)); return; }
    setAmountMicroUsd(result.value); keyRef.current ||= createServiceCreditKey(); setError(null); setStep("confirm");
  };
  const submit = async () => {
    setPosting(true); setError(null);
    try {
      const receipt = await grantServiceCredit({ token: session.accessToken, tenantId: tenant.tenantId, amountMicroUsd, idempotencyKey: keyRef.current });
      onGranted(receipt, tenant, returnFocus);
    } catch (caught) {
      const access = operatorMutationAccess(caught);
      if (access.expired) return onSessionExpired();
      if (access.unavailable) return onOperatorUnavailable();
      setError(caught); setPosting(false);
    }
  };
  const close = () => { if (!posting) onCancel(); };
  const onKeyDown = (event) => {
    if (event.key === "Escape") { event.preventDefault(); close(); return; }
    if (event.key !== "Tab") return;
    const focusable = [...dialogRef.current.querySelectorAll("button:not([disabled]), input:not([disabled])")];
    if (event.shiftKey && document.activeElement === focusable[0]) { event.preventDefault(); focusable.at(-1)?.focus(); }
    else if (!event.shiftKey && document.activeElement === focusable.at(-1)) { event.preventDefault(); focusable[0]?.focus(); }
  };
  return <div className="wk-reveal-layer"><div className="wk-reveal-scrim" aria-hidden="true" />
    <form ref={dialogRef} className="wk-reveal wk-tenant-guardrail-editor" role={step === "confirm" ? "alertdialog" : "dialog"} aria-modal="true" aria-labelledby="service-credit-title" onKeyDown={onKeyDown} onSubmit={step === "edit" ? review : (event) => { event.preventDefault(); submit(); }}>
      <header className="wk-reveal-head"><div><h2 id="service-credit-title">{step === "edit" ? "Grant service credit" : "Confirm service credit"}</h2><p>{tenant.tenantName} · <span className="wk-mono">{tenant.tenantId}</span></p></div><button type="button" className="wk-icon-btn" onClick={close} disabled={posting} aria-label="Close service credit dialog"><Icon name="close" /></button></header>
      {step === "edit" ? <label className={`wk-field${error ? " has-error" : ""}`}><span>Credit amount (USD)</span><input ref={firstRef} type="text" inputMode="decimal" autoComplete="off" maxLength={17} value={amount} aria-describedby="service-credit-help" onChange={(event) => { setAmount(event.target.value); setError(null); keyRef.current = null; }} /><small id="service-credit-help">Positive USD amount, up to six decimal places.</small></label> : <div className="wk-service-credit-confirm"><strong>{formatMicroUSD(amountMicroUsd)}</strong><p>This adds tenant service credit. It does not charge a payment method or create an invoice.</p></div>}
      {error && <p className="wk-inline-error" role="alert">{error.message}</p>}
      <footer className="wk-operator-review-actions">{step === "confirm" && <button type="button" className="wk-btn wk-btn-secondary" disabled={posting} onClick={() => setStep("edit")}>Back</button>}<button type="button" className="wk-btn wk-btn-secondary" disabled={posting} onClick={close}>Cancel</button><button ref={step === "confirm" ? confirmRef : undefined} type="submit" className="wk-btn wk-btn-primary" disabled={posting}>{step === "edit" ? "Review grant" : posting ? "Granting…" : "Confirm grant"}</button></footer>
    </form></div>;
}

function LimitChange({ label, oldValue, newValue, currency = false }) {
  return <div><dt>{label}</dt><dd><span>{limitLabel(oldValue, currency)}</span><span aria-hidden="true">→</span><strong>{limitLabel(newValue, currency)}</strong></dd></div>;
}

function TenantGuardrailEditor({ tenant, session, returnFocus, onCancel, onSaved, onSessionExpired, onOperatorUnavailable }) {
  const [draft, setDraft] = useState(() => tenantLimitDraft(tenant.limits));
  const [errors, setErrors] = useState({});
  const [values, setValues] = useState(null);
  const [step, setStep] = useState("edit");
  const [posting, setPosting] = useState(false);
  const [error, setError] = useState(null);
  const dialogRef = useRef(null);
  const firstRef = useRef(null);
  const confirmRef = useRef(null);
  const controllerRef = useRef(null);

  useEffect(() => {
    const previousOverflow = document.body.style.overflow;
    document.body.style.overflow = "hidden";
    const frame = window.requestAnimationFrame(() => firstRef.current?.focus());
    return () => {
      window.cancelAnimationFrame(frame);
      controllerRef.current?.abort();
      document.body.style.overflow = previousOverflow;
      if (returnFocus instanceof HTMLElement && document.contains(returnFocus)) returnFocus.focus();
    };
  }, []);

  useEffect(() => {
    if (step === "confirm") window.requestAnimationFrame(() => confirmRef.current?.focus());
  }, [step]);

  const close = () => { if (!posting) onCancel(); };
  const onKeyDown = (event) => {
    if (event.key === "Escape") { event.preventDefault(); close(); return; }
    if (event.key !== "Tab") return;
    const focusable = [...dialogRef.current.querySelectorAll("button:not([disabled]), input:not([disabled])")];
    const first = focusable[0];
    const last = focusable.at(-1);
    if (event.shiftKey && document.activeElement === first) { event.preventDefault(); last?.focus(); }
    else if (!event.shiftKey && document.activeElement === last) { event.preventDefault(); first?.focus(); }
  };
  const review = (event) => {
    event.preventDefault();
    const result = tenantLimitValues(draft);
    setErrors(result.errors);
    if (Object.keys(result.errors).length) return;
    setValues(result.values);
    setError(null);
    setStep("confirm");
  };
  const submit = async () => {
    if (posting || !values) return;
    setPosting(true);
    setError(null);
    const controller = new AbortController();
    controllerRef.current = controller;
    try {
      const result = await updateOperatorTenantLimits({ token: session.accessToken, tenantId: tenant.tenantId, limits: values, signal: controller.signal });
      onSaved(result, returnFocus);
    } catch (caught) {
      if (caught?.name === "AbortError") return;
      const access = operatorMutationAccess(caught);
      if (access.expired) { onSessionExpired(); return; }
      if (access.unavailable) { onOperatorUnavailable(); return; }
      setError(caught);
      setPosting(false);
    }
  };

  return <div className="wk-reveal-layer">
    <div className="wk-reveal-scrim" aria-hidden="true" />
    <form ref={dialogRef} className="wk-reveal wk-tenant-guardrail-editor" role={step === "confirm" ? "alertdialog" : "dialog"} aria-modal="true" aria-labelledby="tenant-guardrail-title" onKeyDown={onKeyDown} onSubmit={step === "edit" ? review : (event) => { event.preventDefault(); submit(); }} noValidate>
      <header className="wk-reveal-head">
        <div><h2 id="tenant-guardrail-title">{step === "edit" ? "Edit tenant guardrails" : "Confirm guardrail change"}</h2><p>{tenant.tenantName} · <span className="wk-mono">{tenant.tenantId}</span></p></div>
        <button type="button" className="wk-icon-btn" onClick={close} disabled={posting} aria-label="Close guardrail editor"><Icon name="close" /></button>
      </header>
      {step === "edit" ? <>
        <dl className="wk-operator-review-facts">
          <div><dt>Plan</dt><dd>{tenant.plan}</dd></div>
          <div><dt>Running now</dt><dd>{tenant.runningBursts}</dd></div>
          <div><dt>Current rate</dt><dd>{formatUSD(tenant.hourlyUsd)}/hr</dd></div>
        </dl>
        <p className="wk-form-note"><b>Zero means unlimited.</b> New ceilings govern future admission. Existing runs are not terminated.</p>
        <div className="wk-form-grid wk-form-grid-2">
          <label className={`wk-field${errors.maxConcurrentBursts ? " has-error" : ""}`}>
            <span>Concurrent burst ceiling</span>
            <input ref={firstRef} type="number" min="0" step="1" inputMode="numeric" value={draft.maxConcurrentBursts} aria-invalid={!!errors.maxConcurrentBursts} aria-describedby="concurrent-limit-help" onChange={(event) => setDraft((current) => ({ ...current, maxConcurrentBursts: event.target.value }))} />
            <small id="concurrent-limit-help">{errors.maxConcurrentBursts || `Current: ${limitLabel(tenant.limits.maxConcurrentBursts)}`}</small>
          </label>
          <label className={`wk-field${errors.maxHourlyUsd ? " has-error" : ""}`}>
            <span>Hourly spend ceiling (USD)</span>
            <input type="number" min="0" step="any" inputMode="decimal" value={draft.maxHourlyUsd} aria-invalid={!!errors.maxHourlyUsd} aria-describedby="hourly-limit-help" onChange={(event) => setDraft((current) => ({ ...current, maxHourlyUsd: event.target.value }))} />
            <small id="hourly-limit-help">{errors.maxHourlyUsd || `Current: ${limitLabel(tenant.limits.maxHourlyUsd, true)}`}</small>
          </label>
        </div>
      </> : <>
        <p className="wk-form-note">Review both changes. Saving does not terminate the tenant’s {tenant.runningBursts} running burst{tenant.runningBursts === 1 ? "" : "s"}.</p>
        <dl className="wk-guardrail-change-list">
          <LimitChange label="Concurrent ceiling" oldValue={tenant.limits.maxConcurrentBursts} newValue={values.maxConcurrentBursts} />
          <LimitChange label="Hourly ceiling" oldValue={tenant.limits.maxHourlyUsd} newValue={values.maxHourlyUsd} currency />
        </dl>
      </>}
      {error && <p className="wk-inline-error" role="alert">{error.message}</p>}
      <footer className={`wk-operator-review-actions${step === "confirm" ? " wk-guardrail-confirm-actions" : ""}`}>
        {step === "confirm" && <button type="button" className="wk-btn wk-btn-secondary" onClick={() => { setStep("edit"); setError(null); window.requestAnimationFrame(() => firstRef.current?.focus()); }} disabled={posting}>Back</button>}
        <button type="button" className="wk-btn wk-btn-secondary" onClick={close} disabled={posting}>Cancel</button>
        <button ref={step === "confirm" ? confirmRef : undefined} type="submit" className="wk-btn wk-btn-primary" disabled={posting} aria-busy={posting}>{step === "edit" ? "Review changes" : posting ? "Saving…" : "Save guardrails"}</button>
      </footer>
    </form>
  </div>;
}

export function OperatorTenantsPage({ session, onSessionExpired, onOperatorUnavailable }) {
  const [state, setState] = useState({ status: "loading", tenants: [], nextAfter: "", error: null });
  const [loadingMore, setLoadingMore] = useState(false);
  const [selected, setSelected] = useState(null);
  const [creditTenant, setCreditTenant] = useState(null);
  const [returnFocus, setReturnFocus] = useState(null);
  const [announcement, setAnnouncement] = useState("");
  const controllerRef = useRef(null);

  const load = useCallback(async (after = "", append = false) => {
    controllerRef.current?.abort();
    const controller = new AbortController();
    controllerRef.current = controller;
    if (append) setLoadingMore(true);
    else setState((current) => ({ ...current, status: "loading", error: null }));
    try {
      const page = await fetchOperatorTenants({ token: session.accessToken, after, limit: PAGE_SIZE, signal: controller.signal });
      setState((current) => {
        try {
          return { status: "ready", tenants: append ? appendOperatorTenantPage(current.tenants, page) : page.tenants, nextAfter: page.nextAfter || "", error: null };
        } catch (caught) {
          return { ...current, status: "error", error: caught };
        }
      });
    } catch (caught) {
      if (caught?.name === "AbortError") return;
      if (caught instanceof ApiError && caught.status === 401) { onSessionExpired(); return; }
      if (caught instanceof ApiError && (caught.status === 403 || caught.status === 404)) { onOperatorUnavailable(); return; }
      setState((current) => ({ ...current, status: "error", error: caught }));
    } finally { setLoadingMore(false); }
  }, [session.accessToken, onSessionExpired, onOperatorUnavailable]);

  useEffect(() => {
    load();
    return () => controllerRef.current?.abort();
  }, [load]);

  const saved = (result) => {
    setState((current) => ({ ...current, tenants: replaceOperatorTenant(current.tenants, result.tenant) }));
    setAnnouncement(result.changed ? `Guardrails updated for ${result.tenant.tenantName}.` : `No guardrail changes were needed for ${result.tenant.tenantName}.`);
    setSelected(null);
  };
  const granted = (receipt, tenant) => { setAnnouncement(`${formatMicroUSD(receipt.amountMicroUsd)} service credit granted to ${tenant.tenantName}.`); setCreditTenant(null); };

  return <>
    <PageHeader eyebrow="Platform operations" title="Tenant guardrails" copy="Monitor tenant burst activity and set platform-wide concurrency and hourly spend ceilings." action={<button type="button" className="wk-btn wk-btn-secondary" onClick={() => load()} disabled={state.status === "loading"}><Icon name="refresh" />Refresh</button>} />
    <p className="sr-only" role="status" aria-live="polite">{announcement}</p>
    {state.status === "loading" && <LoadingState label="Reading tenant guardrails…" rows={7} />}
    {state.status === "error" && <ErrorState title="Tenant guardrails are unavailable." error={state.error} onRetry={() => load()} />}
    {state.status === "ready" && state.tenants.length === 0 && <Panel title="Tenant inventory" meta="0 tenants"><div className="wk-empty"><span className="wk-empty-icon" aria-hidden="true"><Icon name="policies" /></span><div><h3>No tenants found</h3><p>Tenant guardrails appear here when central returns operator inventory.</p></div></div></Panel>}
    {state.status === "ready" && state.tenants.length > 0 && <Panel title="Tenant inventory" meta={`${state.tenants.length} tenant${state.tenants.length === 1 ? "" : "s"} loaded`} className="wk-panel-flush">
      <TenantGuardrailTable tenants={state.tenants} onEdit={(tenant, opener) => { setSelected(tenant); setReturnFocus(opener); setAnnouncement(""); }} onCredit={(tenant, opener) => { setCreditTenant(tenant); setReturnFocus(opener); setAnnouncement(""); }} />
      {state.nextAfter && <div className="wk-guardrail-more"><span>More tenants are available after <span className="wk-mono">{state.nextAfter}</span>.</span><button type="button" className="wk-btn wk-btn-secondary" onClick={() => load(state.nextAfter, true)} disabled={loadingMore} aria-busy={loadingMore}>{loadingMore ? "Loading…" : "Load more"}</button></div>}
    </Panel>}
    {selected && <TenantGuardrailEditor tenant={selected} session={session} returnFocus={returnFocus} onCancel={() => setSelected(null)} onSaved={saved} onSessionExpired={onSessionExpired} onOperatorUnavailable={onOperatorUnavailable} />}
    {creditTenant && <ServiceCreditDialog tenant={creditTenant} session={session} returnFocus={returnFocus} onCancel={() => setCreditTenant(null)} onGranted={granted} onSessionExpired={onSessionExpired} onOperatorUnavailable={onOperatorUnavailable} />}
  </>;
}
