import { Fragment, useCallback, useEffect, useMemo, useRef, useState } from "react";
import { motion, useAnimationControls, useReducedMotion } from "motion/react";
import { TenantOnboarding, tenantLabel } from "../components/TenantOnboarding.jsx";
import { Link, useRoute } from "../lib/router.jsx";
import { ApiError, fetchAccount, fetchMembers, removeMember } from "../lib/accountApi.js";
import { ISSUER_URL, beginDevPreview, beginLogin, canSignIn, claimsOf, getSession, signOut } from "../lib/auth.js";
import { canUseDevPreview } from "../lib/devPreview.js";
import { canManageTenant } from "../lib/roles.js";

// The Access Ledger. Same paper-and-ink accounting language as the Burst
// Ledger on the home page, pointed at a different fact: not what a workload
// cost, but who you are, which tenants you are on, and what each of them lets
// you do. Every number on this page came from the API — there is no demo data
// here, and an empty account says so plainly.

const EASE_OUT = [0.16, 1, 0.3, 1];

const ROLE_ORDER = { owner: 0, admin: 1, member: 2 };
function formatDate(value) {
  if (!value) return "—";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return "—";
  return date.toLocaleDateString(undefined, { year: "numeric", month: "short", day: "2-digit" });
}

function formatUSD(value) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) return "—";
  return `$${Number(value).toFixed(2)}`;
}

function formatCount(value) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) return "—";
  return String(value);
}

function hostOf(url) {
  try {
    return new URL(url).host;
  } catch {
    return url || "—";
  }
}

function minutesLeft(expiresAt) {
  return Math.max(0, Math.floor((expiresAt - Date.now()) / 60000));
}

function CopyButton({ value, label = "copy", idle = "copy" }) {
  const [state, setState] = useState("idle");
  const timer = useRef(0);
  useEffect(() => () => clearTimeout(timer.current), []);

  const copy = async () => {
    let ok = false;
    try {
      await navigator.clipboard.writeText(value);
      ok = true;
    } catch {
      ok = false;
    }
    setState(ok ? "copied" : "failed");
    clearTimeout(timer.current);
    timer.current = setTimeout(() => setState("idle"), 2600);
  };

  return (
    <>
      <button type="button" className="copy-btn" onClick={copy} aria-label={`${label}: ${value}`}>
        {state === "copied" ? "copied" : state === "failed" ? "select it" : idle}
      </button>
      <span className="sr-only" role="status">
        {state === "copied" ? `${label} copied to clipboard` : ""}
        {state === "failed" ? `${label} could not be copied; select the text instead` : ""}
      </span>
    </>
  );
}

function Field({ label, children, mono = false }) {
  return (
    <div className="acct-field">
      <span>{label}</span>
      <strong className={mono ? "num acct-mono" : undefined}>{children}</strong>
    </div>
  );
}

// ---------------------------------------------------------------- signed out

function SignedOut({ onSignIn, onPreview, busy, error, expired }) {
  const blocked = !canSignIn();
  return (
    <section className="acct-gate" aria-labelledby="gate-title">
      <div className="acct-gate-grid">
        <div>
          <span className="acct-kicker">ACCESS LEDGER · SIGN IN REQUIRED</span>
          <h1 id="gate-title" className="acct-title display">
            Your account,<br /><span>on the record.</span>
          </h1>
          <p className="acct-lede">
            Sign in to view tenant roles, burst limits, spend caps, and member access.
          </p>
          <p className="acct-note">
            Sessions stay in this tab and expire after one hour.
          </p>

          {expired && (
            <p className="acct-alert" role="status">Your previous session expired. Sign in again to continue.</p>
          )}
          {error && (
            <p className="acct-alert acct-alert-bad" role="alert">{error.message}</p>
          )}
          {blocked && !error && (
            <p className="acct-alert acct-alert-bad" role="alert">
              This origin cannot start a secure sign-in. Yscale ID needs an HTTPS
              address and session storage available.
            </p>
          )}

          <div className="acct-actions">
            <button type="button" className="btn-solid" onClick={onSignIn} disabled={busy || blocked} aria-busy={busy}>
              {busy ? "opening yscale id…" : "sign in with Yscale ID"} <span aria-hidden="true">↗</span>
            </button>
            <Link to="/open-source" className="btn-ghost">what runs where <span aria-hidden="true">→</span></Link>
            {canUseDevPreview() && (
              <button type="button" className="btn-ghost" onClick={() => onPreview("first-workspace")}>
                first-workspace preview
              </button>
            )}
            {canUseDevPreview() && (
              <button type="button" className="btn-ghost" onClick={() => onPreview("populated")}>
                populated preview
              </button>
            )}
          </div>
        </div>

        <aside className="acct-gate-card" aria-label="What this page shows once you sign in">
          <div className="gate-head">
            <span className="acct-kicker">WHAT LANDS HERE</span>
            <span className="gate-state">SIGNED OUT</span>
          </div>
          <ol className="gate-list">
            <li><span>01</span><div><strong>Identity</strong><small>issuer, subject, email</small></div></li>
            <li><span>02</span><div><strong>Account</strong><small>id and creation date</small></div></li>
            <li><span>03</span><div><strong>Tenants</strong><small>linked tenant accounts</small></div></li>
            <li><span>04</span><div><strong>Role + limits</strong><small>plan, bursts, spend</small></div></li>
            <li><span>05</span><div><strong>Roster</strong><small>members and roles</small></div></li>
          </ol>
          <p className="gate-foot">
            Issuer <b className="acct-mono">{hostOf(ISSUER_URL)}</b>
          </p>
        </aside>
      </div>
    </section>
  );
}

// ------------------------------------------------------------------- the rail

function AccessRail({ account, activeTenant, onSelect, reduced }) {
  const tenants = account.tenants || [];
  const identity = account.name || account.email || account.subject || "your account";
  const press = useAnimationControls();
  const marker = reduced ? { duration: 0 } : { type: "spring", stiffness: 420, damping: 34 };
  const swap = reduced
    ? { initial: false, animate: { opacity: 1, y: 0 }, transition: { duration: 0 } }
    : { initial: { opacity: 0, y: 8 }, animate: { opacity: 1, y: 0 }, transition: { duration: 0.28, ease: EASE_OUT } };

  const drawRule = (index) =>
    reduced
      ? {}
      : {
          initial: { scaleX: 0 },
          animate: { scaleX: 1 },
          transition: { duration: 0.4, ease: EASE_OUT, delay: 0.08 * index },
        };

  const pressRail = (event) => {
    if (reduced || event.pointerType !== "mouse") return;
    const bounds = event.currentTarget.getBoundingClientRect();
    const direction = (event.clientX - bounds.left) / bounds.width - 0.5;
    press.start({
      scaleX: 0.988,
      scaleY: 1.014,
      rotate: direction * 1.6,
      transition: { duration: 0.16, ease: "easeOut" },
    });
  };

  const releaseRail = (event) => {
    if (reduced || event.pointerType !== "mouse") return;
    press.start({
      scaleX: 1,
      scaleY: 1,
      rotate: 0,
      transition: { type: "spring", stiffness: 220, damping: 12, mass: 0.58 },
    });
  };

  return (
    <section className="rail-section" aria-labelledby="rail-title">
      <div className="acct-heading">
        <div>
          <span className="acct-kicker">ACCESS RAIL</span>
          <h2 id="rail-title">Identity, account, tenant, role.</h2>
        </div>
        <span className="rail-count num">
          {tenants.length} {tenants.length === 1 ? "tenant" : "tenants"}
        </span>
      </div>

      <motion.div
        className="rail-spring-body"
        animate={press}
        initial={false}
        onPointerDown={pressRail}
        onPointerUp={releaseRail}
        onPointerCancel={releaseRail}
        onPointerLeave={releaseRail}
      >
      <ol className="access-rail">
        <li className="rail-station">
          <motion.i className="rail-rule" aria-hidden="true" {...drawRule(0)} />
          <span className="rail-index num">01</span>
          <span className="rail-label">IDENTITY</span>
          <div className="rail-body">
            <strong>{identity}</strong>
            <small className="acct-mono">{hostOf(account.issuer || ISSUER_URL)}</small>
          </div>
        </li>

        <li className="rail-station">
          <motion.i className="rail-rule" aria-hidden="true" {...drawRule(1)} />
          <span className="rail-index num">02</span>
          <span className="rail-label">ACCOUNT</span>
          <div className="rail-body">
            <strong className="acct-mono rail-id">{account.account_id}</strong>
            <small>since {formatDate(account.created_at)}</small>
          </div>
        </li>

        <li className="rail-station">
          <motion.i className="rail-rule" aria-hidden="true" {...drawRule(2)} />
          <span className="rail-index num">03</span>
          <span className="rail-label">TENANT</span>
          {tenants.length === 0 ? (
            <div className="rail-body"><strong>none yet</strong><small>no tenant attached</small></div>
          ) : (
            <div className="rail-tenants" role="group" aria-label="Choose a tenant">
              {tenants.map((tenant) => {
                const selected = tenant.customer_id === activeTenant?.customer_id;
                return (
                  <button
                    key={tenant.customer_id}
                    type="button"
                    className={`rail-tenant${selected ? " is-active" : ""}`}
                    aria-pressed={selected}
                    onClick={() => onSelect(tenant.customer_id)}
                  >
                    {selected && (
                      <motion.i layoutId="rail-marker" className="rail-marker" aria-hidden="true" transition={marker} />
                    )}
                    <span>{tenantLabel(tenant)}</span>
                    <small>{tenant.role}</small>
                  </button>
                );
              })}
            </div>
          )}
        </li>

        <li className="rail-station rail-station-end">
          <span className="rail-index num">04</span>
          <span className="rail-label">ROLE + LIMITS</span>
          {activeTenant ? (
            <motion.div key={activeTenant.customer_id} className="rail-body" {...swap}>
              <strong className={`role-chip role-${activeTenant.role || "member"}`}>{activeTenant.role || "member"}</strong>
              <dl className="rail-limits">
                <div><dt>plan</dt><dd>{activeTenant.plan || "—"}</dd></div>
                <div><dt>concurrent bursts</dt><dd className="num">{formatCount(activeTenant.limits?.max_concurrent_bursts)}</dd></div>
                <div><dt>hourly ceiling</dt><dd className="num">{formatUSD(activeTenant.limits?.max_hourly_usd)}</dd></div>
              </dl>
            </motion.div>
          ) : (
            <div className="rail-body"><strong>—</strong><small>no limits yet</small></div>
          )}
        </li>
      </ol>
      </motion.div>
    </section>
  );
}

// ----------------------------------------------------------------- the roster

function apiMessage(error) {
  if (!(error instanceof ApiError)) return error?.message || "The account service did not answer.";
  if (error.isForbidden) return "Your role in this tenant cannot do that.";
  if (error.isUnavailable) return "The account service is not answering right now.";
  if (error.isOffline) return "This browser could not reach the account service.";
  return error.message;
}

function Roster({ token, tenant, selfAccountId, reduced }) {
  const [members, setMembers] = useState([]);
  const [nextAfter, setNextAfter] = useState(null);
  const [status, setStatus] = useState("loading"); // loading | ready | error
  const [error, setError] = useState(null);
  const [loadingMore, setLoadingMore] = useState(false);
  const [pending, setPending] = useState(null); // member awaiting confirmation
  const [busy, setBusy] = useState(false);
  const [removeError, setRemoveError] = useState(null);
  const [moreError, setMoreError] = useState(null);
  const [announcement, setAnnouncement] = useState("");
  const [reloadKey, setReloadKey] = useState(0);
  const confirmRef = useRef(null);

  const tenantId = tenant.customer_id;
  const canManage = canManageTenant(tenant.role);

  useEffect(() => {
    const ac = new AbortController();
    setStatus("loading");
    setError(null);
    setMembers([]);
    setNextAfter(null);
    setPending(null);
    setRemoveError(null);
    fetchMembers({ token, tenantId, signal: ac.signal })
      .then((data) => {
        setMembers(sortMembers(data?.members || []));
        setNextAfter(data?.next_after || null);
        setStatus("ready");
      })
      .catch((err) => {
        if (err?.name === "AbortError") return;
        setError(err);
        setStatus("error");
      });
    return () => ac.abort();
  }, [token, tenantId, reloadKey]);

  useEffect(() => {
    if (pending) confirmRef.current?.focus();
  }, [pending]);

  // A failed page does not discard the members already on screen; it says so
  // next to the button and leaves the cursor where it was.
  const loadMore = () => {
    setLoadingMore(true);
    setMoreError(null);
    fetchMembers({ token, tenantId, after: nextAfter })
      .then((data) => {
        setMembers((prev) => sortMembers([...prev, ...(data?.members || [])]));
        setNextAfter(data?.next_after || null);
      })
      .catch((err) => setMoreError(err))
      .finally(() => setLoadingMore(false));
  };

  const confirmRemove = async () => {
    const member = pending;
    if (!member) return;
    setBusy(true);
    setRemoveError(null);
    try {
      await removeMember({ token, tenantId, accountId: member.account_id });
      setMembers((prev) => prev.filter((m) => m.account_id !== member.account_id));
      setAnnouncement(`${member.email || member.account_id} was removed from ${tenantId}.`);
      setPending(null);
    } catch (err) {
      // The API is the authority: a refusal here is the real answer, whatever
      // the UI believed about this role.
      setRemoveError(err);
      setAnnouncement(`${member.email || member.account_id} was not removed.`);
    } finally {
      setBusy(false);
    }
  };

  const rows = reduced ? {} : { initial: { opacity: 0 }, animate: { opacity: 1 }, transition: { duration: 0.24 } };

  return (
    <section className="roster-section" aria-labelledby="roster-title">
      <div className="acct-heading">
        <div>
          <span className="acct-kicker">TENANT ROSTER</span>
          <h2 id="roster-title">Members with access to <b className="acct-mono">{tenantId}</b>.</h2>
        </div>
        <span className="roster-count num">
          {status === "ready" ? `${members.length} shown${nextAfter ? " · more available" : ""}` : "—"}
        </span>
      </div>

      <p className="sr-only" role="status" aria-live="polite">{announcement}</p>

      {status === "loading" && (
        <div className="roster-skeleton" role="status" aria-live="polite" aria-busy="true">
          <span className="sr-only">Loading the roster for {tenantId}.</span>
          <i /><i /><i />
        </div>
      )}

      {status === "error" && (
        <div className="roster-notice roster-notice-bad" role="alert">
          <p>{apiMessage(error)}</p>
          <button type="button" className="btn-ghost" onClick={() => setReloadKey((n) => n + 1)}>try again</button>
        </div>
      )}

      {status === "ready" && members.length === 0 && (
        <div className="roster-notice">
          <p>No members are listed yet.</p>
          <small>Accepted invites appear here.</small>
        </div>
      )}

      {status === "ready" && members.length > 0 && (
        <motion.div className="roster-table-wrap" {...rows}>
          <table className="roster-table">
            <caption className="sr-only">Members of tenant {tenantId}</caption>
            <thead>
              <tr>
                <th scope="col">Member</th>
                <th scope="col">Role</th>
                <th scope="col">Account</th>
                <th scope="col">Joined</th>
                <th scope="col"><span className="sr-only">Actions</span></th>
              </tr>
            </thead>
            <tbody>
              {members.map((member) => {
                const isSelf = member.account_id === selfAccountId;
                const isPending = pending?.account_id === member.account_id;
                return (
                  <Fragment key={member.account_id}>
                    <tr className={isPending ? "is-pending" : undefined}>
                      <th scope="row">
                        <strong>{member.name || member.email || member.account_id}</strong>
                        {member.name && member.email && <small>{member.email}</small>}
                      </th>
                      <td data-label="Role">
                        <span className={`role-chip role-${member.role || "member"}`}>{member.role || "member"}</span>
                      </td>
                      <td data-label="Account" className="acct-mono roster-id">{member.account_id}</td>
                      <td data-label="Joined" className="num">{formatDate(member.created_at)}</td>
                      <td className="roster-actions">
                        {isSelf ? (
                          <span className="roster-you">you</span>
                        ) : canManage ? (
                          <button
                            type="button"
                            className="roster-remove"
                            onClick={() => { setRemoveError(null); setPending(member); }}
                            disabled={busy}
                            aria-expanded={isPending}
                            aria-controls={`confirm-${member.account_id}`}
                          >
                            remove
                          </button>
                        ) : null}
                      </td>
                    </tr>
                    {isPending && (
                      <tr className="roster-confirm-row" id={`confirm-${member.account_id}`}>
                        <td colSpan={5}>
                          <div className="roster-confirm">
                            <p>
                              Remove <strong>{member.email || member.account_id}</strong> from{" "}
                              <b className="acct-mono">{tenantId}</b>? Their access to this tenant ends immediately.
                            </p>
                            {removeError && <p className="roster-error" role="alert">{apiMessage(removeError)}</p>}
                            <div className="roster-confirm-actions">
                              <button
                                ref={confirmRef}
                                type="button"
                                className="btn-solid roster-danger"
                                onClick={confirmRemove}
                                disabled={busy}
                                aria-busy={busy}
                              >
                                {busy ? "removing…" : "yes, remove"}
                              </button>
                              <button
                                type="button"
                                className="btn-ghost"
                                onClick={() => { setPending(null); setRemoveError(null); }}
                                disabled={busy}
                              >
                                keep access
                              </button>
                            </div>
                          </div>
                        </td>
                      </tr>
                    )}
                  </Fragment>
                );
              })}
            </tbody>
          </table>
        </motion.div>
      )}

      {status === "ready" && nextAfter && (
        <div className="roster-more">
          <button type="button" className="btn-ghost" onClick={loadMore} disabled={loadingMore} aria-busy={loadingMore}>
            {loadingMore ? "loading…" : "load more members"}
          </button>
          {moreError && <p className="roster-error" role="alert">{apiMessage(moreError)}</p>}
        </div>
      )}

      {!canManage && status === "ready" && members.length > 0 && (
        <p className="acct-note">
          Only owners and admins can remove members. Your role is <b>{tenant.role || "member"}</b>.
        </p>
      )}
    </section>
  );
}

function sortMembers(list) {
  return [...list].sort((a, b) => {
    const byRole = (ROLE_ORDER[a.role] ?? 9) - (ROLE_ORDER[b.role] ?? 9);
    if (byRole !== 0) return byRole;
    return (a.email || a.account_id || "").localeCompare(b.email || b.account_id || "");
  });
}

// ------------------------------------------------------------- no tenants yet

function NoTenants({ account, token, onCreated, navigate }) {
  return <TenantOnboarding account={account} token={token} onCreated={onCreated} navigate={navigate} className="acct-empty" />;
}

// ------------------------------------------------------------------ the page

export function Account() {
  const [, navigate] = useRoute();
  const reduced = useReducedMotion();
  const [session, setSession] = useState(() => getSession());
  const [account, setAccount] = useState(null);
  const [status, setStatus] = useState(() => (getSession() ? "loading" : "anonymous"));
  const [error, setError] = useState(null);
  const [expired, setExpired] = useState(false);
  const [activeId, setActiveId] = useState(null);
  const [signingIn, setSigningIn] = useState(false);
  const [signInError, setSignInError] = useState(null);
  const [remaining, setRemaining] = useState(() => {
    const current = getSession();
    return current ? minutesLeft(current.expiresAt) : 0;
  });

  // ID-token claims are for the label above only. What this account may do is
  // answered by the API against the access token, never decoded here.
  const claims = useMemo(() => claimsOf(session?.idToken), [session]);

  const endSession = useCallback((wasExpiry) => {
    signOut();
    setSession(null);
    setAccount(null);
    setActiveId(null);
    setStatus("anonymous");
    setExpired(!!wasExpiry);
  }, []);

  useEffect(() => {
    if (!session) return;
    // getSession is the single authority on whether the session is still
    // usable, skew included; the countdown just reports what it decides.
    const tick = () => {
      if (!getSession()) {
        endSession(true);
        return;
      }
      setRemaining(minutesLeft(session.expiresAt));
    };
    tick();
    const id = setInterval(tick, 20_000);
    return () => clearInterval(id);
  }, [session, endSession]);

  useEffect(() => {
    if (!session) return;
    const ac = new AbortController();
    setStatus("loading");
    setError(null);
    fetchAccount({ token: session.accessToken, signal: ac.signal })
      .then((data) => {
        setAccount(data);
        setActiveId(data?.tenants?.[0]?.customer_id ?? null);
        setStatus("ready");
      })
      .catch((err) => {
        if (err?.name === "AbortError") return;
        if (err instanceof ApiError && err.isExpired) {
          endSession(true);
          return;
        }
        setError(err);
        setStatus("error");
      });
    return () => ac.abort();
  }, [session, endSession]);

  const startSignIn = async () => {
    setSigningIn(true);
    setSignInError(null);
    setExpired(false);
    try {
      await beginLogin(); // navigates to Yscale ID on success
    } catch (err) {
      setSignInError(err);
      setSigningIn(false);
    }
  };

  const startPreview = (mode) => {
    const preview = beginDevPreview(mode);
    if (preview) {
      setSession(preview);
      setExpired(false);
      setStatus("loading");
    }
  };

  // Retrying re-reads storage: if the hour ran out while the error was on
  // screen, that is a sign-in problem now, not an account-service one.
  const retry = () => {
    const current = getSession();
    if (current) setSession(current);
    else endSession(true);
  };

  const tenants = account?.tenants || [];
  const activeTenant = tenants.find((t) => t.customer_id === activeId) || tenants[0] || null;
  const onTenantCreated = (envelope, tenant) => {
    setAccount(envelope);
    setActiveId(tenant?.customer_id ?? envelope?.tenants?.[0]?.customer_id ?? null);
  };

  return (
    <div className="acct-page">
      <div className="acct-atmosphere" aria-hidden="true" />
      <div className="wrap">
        <header className="acct-top">
          <div className="acct-crumbs">
            <Link to="/">yscale.sh</Link><span aria-hidden="true">/</span><strong>access ledger</strong>
          </div>
          {session ? (
            <div className="acct-session" aria-label="Session status">
              <span className="acct-live"><i aria-hidden="true" /> SIGNED IN</span>
              <span className="num">expires in {remaining}m</span>
              <button type="button" className="acct-signout" onClick={() => endSession(false)}>sign out</button>
            </div>
          ) : (
            <span className="acct-session acct-session-out">SIGNED OUT</span>
          )}
        </header>

        {status === "anonymous" && (
          <SignedOut onSignIn={startSignIn} onPreview={startPreview} busy={signingIn} error={signInError} expired={expired} />
        )}

        {status === "loading" && (
          <section className="acct-loading" role="status" aria-live="polite" aria-busy="true">
            <span className="acct-kicker">READING THE LEDGER</span>
            <h1 className="acct-title display">Loading your account…</h1>
            <p className="acct-lede">
              Signed in as <b>{claims?.email || claims?.name || "your Yscale ID"}</b>. Fetching tenants, roles, and limits.
            </p>
            <div className="acct-skeleton" aria-hidden="true"><i /><i /><i /></div>
          </section>
        )}

        {status === "error" && (
          <section className="acct-gate" aria-labelledby="acct-error-title">
            <span className="acct-kicker">ACCOUNT UNAVAILABLE</span>
            <h1 id="acct-error-title" className="acct-title display">Account data is unavailable.</h1>
            <p className="acct-lede" role="alert">{apiMessage(error)}</p>
            <p className="acct-note">Your sign-in is still valid.</p>
            <div className="acct-actions">
              <button type="button" className="btn-solid" onClick={retry}>try again</button>
              <button type="button" className="btn-ghost" onClick={() => endSession(false)}>sign out</button>
            </div>
          </section>
        )}

        {status === "ready" && account && (
          <>
            <section className="acct-identity" aria-labelledby="acct-title">
              <span className="acct-kicker">ACCESS LEDGER</span>
              <h1 id="acct-title" className="acct-title display">
                {account.name || account.email || "Your account"}
              </h1>
              <div className="acct-fields">
                <Field label="account id" mono>
                  {account.account_id} <CopyButton value={account.account_id} label="account id" />
                </Field>
                <Field label="email">
                  {account.email || "—"}{" "}
                  <span className={`verify-chip${account.email_verified ? " is-verified" : ""}`}>
                    {account.email_verified ? "verified" : "unverified"}
                  </span>
                </Field>
                <Field label="issuer" mono>{hostOf(account.issuer || ISSUER_URL)}</Field>
                <Field label="subject" mono>{account.subject || "—"}</Field>
              </div>
              {!account.email_verified && (
                <p className="acct-alert" role="status">
                  Verify this email to unlock tenant actions.
                </p>
              )}
            </section>

            <AccessRail account={account} activeTenant={activeTenant} onSelect={setActiveId} reduced={reduced} />

            {activeTenant ? (
              <Roster
                key={activeTenant.customer_id}
                token={session.accessToken}
                tenant={activeTenant}
                selfAccountId={account.account_id}
                reduced={reduced}
              />
            ) : (
              <NoTenants account={account} token={session.accessToken} onCreated={onTenantCreated} navigate={navigate} />
            )}
          </>
        )}
      </div>
    </div>
  );
}
