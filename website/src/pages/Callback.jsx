import { useEffect, useRef, useState } from "react";
import { Link, useRoute } from "../lib/router.jsx";
import { completeCallback, takeLoginReturnTo } from "../lib/auth.js";

// The landing strip for the Yscale ID redirect. It has one job and shows what
// it is doing while it does it: read the one-time code, scrub it out of the
// address bar and this history entry, exchange it, then replace this entry
// with /account so the back button never returns to a spent code.
export function Callback() {
  const [, navigate] = useRoute();
  const [error, setError] = useState(null);
  const started = useRef(false);

  useEffect(() => {
    if (started.current) return; // StrictMode double-invokes; a code is single-use
    started.current = true;

    const search = window.location.search;
    window.history.replaceState(null, "", "/callback");

    let alive = true;
    completeCallback(search)
      .then(() => {
        if (alive) navigate(takeLoginReturnTo(), { replace: true });
      })
      .catch((err) => {
        if (alive) setError(err);
      });
    return () => {
      alive = false;
    };
  }, [navigate]);

  return (
    <section className="acct-page" aria-labelledby="callback-title">
      <div className="wrap">
        <header className="acct-top">
          <div className="acct-crumbs">
            <Link to="/">yscale.sh</Link><span aria-hidden="true">/</span><strong>sign-in</strong>
          </div>
          <span className="acct-session acct-session-out">{error ? "NOT SIGNED IN" : "EXCHANGING"}</span>
        </header>

        <div className={`callback-card${error ? " callback-failed" : ""}`}>
          <span className="acct-kicker">YSCALE ID · AUTHORIZATION CODE</span>
          {error ? (
            <>
              <h1 id="callback-title" className="acct-title display">Sign-in failed.</h1>
              <p className="acct-lede" role="alert">{error.message}</p>
              <p className="acct-note">
                The one-time code was discarded. No session was saved.
              </p>
              <div className="acct-actions">
                <Link to="/account" className="btn-solid">try sign-in again <span aria-hidden="true">→</span></Link>
              </div>
            </>
          ) : (
            <>
              <h1 id="callback-title" className="acct-title display">Finishing sign-in…</h1>
              <p className="acct-lede" role="status" aria-live="polite">
                Creating this tab's one-hour session.
              </p>
              <div className="callback-progress" aria-hidden="true"><i /></div>
            </>
          )}
        </div>
      </div>
    </section>
  );
}
