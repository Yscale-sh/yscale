import { useState } from "react";
import { ApiError, createTenant } from "../lib/accountApi.js";

function createMessage(error) {
  if (!(error instanceof ApiError)) return error?.message || "The account service did not answer. Try again.";
  if (error.status === 400) return "Use a workspace name from 1 to 64 characters with no control characters.";
  if (error.status === 403) return "Verify your email, then create the workspace again.";
  if (error.status === 409) return "This account already belongs to a workspace. Refresh your account.";
  if (error.status === 404) return "Trial workspace creation is not enabled here.";
  if (error.status === 503 || error.status === 0) return "Workspace creation is unavailable right now. Try again in a moment.";
  return error.message || `Workspace creation failed (HTTP ${error.status}).`;
}

function tenantName(tenant) {
  return tenant?.display_name || tenant?.displayName || tenant?.name || "";
}

export function tenantLabel(tenant) {
  const id = tenant?.customer_id || tenant?.tenant_id || "";
  const name = tenantName(tenant);
  if (name && id && name !== id) return `${name} (${id})`;
  return name || id || "Workspace";
}

function createdTenant(envelope, previousIds, name) {
  const tenants = Array.isArray(envelope?.tenants) ? envelope.tenants : [];
  return tenants.find((tenant) => !previousIds.has(tenant.customer_id)) ||
    tenants.find((tenant) => tenantName(tenant).trim().toLowerCase() === name.trim().toLowerCase()) ||
    tenants[0] ||
    null;
}

export function TenantOnboarding({ account, token, onCreated, navigate, className = "" }) {
  const [name, setName] = useState("");
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState(null);
  const [successTenant, setSuccessTenant] = useState(null);
  const verified = account?.email_verified === true;
  const trimmed = name.trim();

  const submit = async (event) => {
    event.preventDefault();
    if (busy) return;
    setError(null);
    if (!trimmed) {
      setError(new Error("Enter a workspace name."));
      return;
    }
    const characters = [...trimmed];
    if (characters.length > 64 || characters.some((character) => /[\u0000-\u001f\u007f-\u009f]/u.test(character))) {
      setError(new Error("Use 1 to 64 characters with no control characters."));
      return;
    }
    setBusy(true);
    try {
      const previousIds = new Set((account?.tenants || []).map((tenant) => tenant.customer_id));
      const envelope = await createTenant({ token, name: trimmed });
      const tenant = createdTenant(envelope, previousIds, trimmed);
      setSuccessTenant(tenant);
      const reduced = typeof window !== "undefined" && window.matchMedia?.("(prefers-reduced-motion: reduce)").matches;
      window.setTimeout(() => {
        onCreated?.(envelope, tenant);
        navigate?.("/workloads/clusters");
      }, reduced ? 0 : 520);
    } catch (err) {
      setError(err);
      setBusy(false);
    }
  };

  if (!verified) {
    return (
      <section className={`tenant-onboarding ${className}`} aria-labelledby="tenant-onboarding-title">
        <span className="tenant-onboarding-kicker">Workspace setup</span>
        <h2 id="tenant-onboarding-title">Verify your email first.</h2>
        <p>Workspace creation is available after this account has a verified email address.</p>
      </section>
    );
  }

  return (
    <section className={`tenant-onboarding ${successTenant ? "is-complete" : ""} ${className}`} aria-labelledby="tenant-onboarding-title">
      <div className="tenant-onboarding-copy">
        <span className="tenant-onboarding-kicker">Workspace setup</span>
        <h2 id="tenant-onboarding-title">{successTenant ? "Workspace ready." : "Create your trial workspace."}</h2>
        <p>{successTenant ? `${tenantLabel(successTenant)} is ready.` : "Name the workspace. You can invite the team after it opens."}</p>
      </div>

      <div className="tenant-guardrails" aria-label="Initial guardrails">
        <span>1 concurrent burst</span>
        <span>USD 1/hour cap</span>
      </div>

      <form className="tenant-onboarding-form" onSubmit={submit}>
        <label>
          <span>Workspace name</span>
          <input
            value={name}
            onChange={(event) => setName(event.target.value)}
            disabled={busy || !!successTenant}
            autoComplete="organization"
            placeholder="Acme research"
          />
        </label>
        {error && <p className="tenant-onboarding-error" role="alert">{createMessage(error)}</p>}
        <button type="submit" disabled={busy || !!successTenant} aria-busy={busy}>
          {successTenant ? "opening console…" : busy ? "creating…" : "create workspace"}
        </button>
      </form>
      {successTenant && <div className="tenant-complete-mark" aria-hidden="true" />}
    </section>
  );
}
