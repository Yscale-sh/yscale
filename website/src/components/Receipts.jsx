// Receipts.jsx — thin pre-launch honesty strip.
// Placement: between Serverless and PickYourLane (the access / conversion section).
// No CTA. No logo bar. Just what actually exists.

export function Receipts() {
  return (
    <section
      style={{
        padding: "36px 0 44px",
        borderTop: "1px solid var(--border)",
      }}
    >
      <div className="wrap">
        <div className="eyebrow reveal" style={{ marginBottom: 14 }}>
          RECEIPTS
        </div>
        <p
          className="reveal"
          style={{
            maxWidth: 720,
            fontFamily: "var(--font-mono)",
            fontSize: 13,
            lineHeight: 1.75,
            color: "var(--fg-muted)",
          }}
        >
          Pre-launch means no customer logos. Here is what exists instead. The
          core engine is open source at{" "}
          <a
            href="https://github.com/Yscale-sh"
            target="_blank"
            rel="noopener noreferrer"
            style={{ color: "var(--fg)", textDecoration: "underline" }}
          >
            github.com/Yscale-sh
          </a>
          . Real bursts run end to end on a live cluster: actual Fly and Linode
          machines joining, running pods, getting reaped. The lifecycle animation
          at the top of this page is that loop, beat for beat. And this site
          runs on the cluster it advertises, deployed by a git commit.
        </p>
      </div>
    </section>
  );
}
