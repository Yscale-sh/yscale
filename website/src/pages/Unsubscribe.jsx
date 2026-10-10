import { useState } from "react";

// /unsubscribe — one field, no guilt trip, no dark patterns. Appends a
// tombstone server-side; the export endpoint folds it out of the list.
export function Unsubscribe({ isMobile }) {
  const [email, setEmail] = useState("");
  const [website, setWebsite] = useState(""); // honeypot
  const [pending, setPending] = useState(false);
  const [done, setDone] = useState(false);
  const [error, setError] = useState("");

  async function onSubmit(e) {
    e.preventDefault();
    if (pending) return;
    setError("");
    setPending(true);
    try {
      const res = await fetch("/api/unsubscribe", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ email, website }),
      });
      const data = await res.json();
      if (res.ok && data.ok) setDone(true);
      else setError("that didn't go through — try again in a minute");
    } catch {
      setError("that didn't go through — try again in a minute");
    } finally {
      setPending(false);
    }
  }

  return (
    <section style={{ padding: isMobile ? "64px 0 96px" : "96px 0 128px" }}>
      <div className="wrap" style={{ maxWidth: 560 }}>
        <div className="eyebrow">MAILING LIST</div>
        <h1 className="display" style={{ fontSize: isMobile ? 40 : 56, margin: "14px 0 0" }}>
          Off the list.
        </h1>
        <p style={{ marginTop: 18, color: "var(--ink-60)", fontSize: 16 }}>
          Enter your email and you're out — waitlist, drops, kritters, all of
          it. No confirmation maze, no "are you sure". You can rejoin any time
          from any signup form.
        </p>

        {done ? (
          <p style={{ marginTop: 28, fontFamily: "var(--font-mono)", fontSize: 14 }}>
            <span style={{ color: "var(--accent-deep)" }}>✓</span> done — {email} is
            off the list.
          </p>
        ) : (
          <form onSubmit={onSubmit} style={{ position: "relative", display: "grid", gap: 10, marginTop: 28 }}>
            <input
              className="lane-input"
              type="email"
              required
              value={email}
              onChange={(e) => setEmail(e.target.value)}
              placeholder="you@company.com"
              aria-label="email"
              style={{
                display: "block",
                width: "100%",
                boxSizing: "border-box",
                border: "1px solid var(--ink)",
                borderRadius: 2,
                background: "var(--paper)",
                color: "var(--ink)",
                fontFamily: "var(--font-mono)",
                fontSize: 14,
                padding: 12,
              }}
            />
            <input
              type="text"
              name="website"
              value={website}
              onChange={(e) => setWebsite(e.target.value)}
              tabIndex={-1}
              autoComplete="off"
              aria-hidden="true"
              style={{ position: "absolute", left: "-9999px", width: 1, height: 1, opacity: 0 }}
            />
            <button
              type="submit"
              className="btn-ghost"
              disabled={pending}
              aria-busy={pending}
              style={{ width: "100%", cursor: "pointer" }}
            >
              {pending ? "…" : "UNSUBSCRIBE"}
            </button>
            {error && (
              <p role="alert" style={{ margin: 0, fontFamily: "var(--font-mono)", fontSize: 12, color: "var(--ink-60)" }}>
                {error}
              </p>
            )}
          </form>
        )}

        <p style={{ marginTop: 24, fontFamily: "var(--font-mono)", fontSize: 12, color: "var(--ink-60)" }}>
          stuck? email{" "}
          <a href="mailto:yscale@unbelievablesite.com" style={{ textDecoration: "underline" }}>
            yscale@unbelievablesite.com
          </a>{" "}
          and a human will remove you.
        </p>
      </div>
    </section>
  );
}
