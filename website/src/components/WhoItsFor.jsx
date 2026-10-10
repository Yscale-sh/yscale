// WHO IT'S FOR — the three real audiences, each sold their own outcome.
// Platform engineers buy relief from capacity planning; AI/ML teams buy
// "train tonight"; homelabbers buy datacenter muscles on a hobby budget.
import { SIGNUP_URL } from "../lib/identity.js";

const AUDIENCES = [
  {
    eyebrow: "PLATFORM ENGINEERS",
    title: "The queue clears itself.",
    body:
      "Stop being the human between a quota ticket and a training run. Bursts land in the cluster you already operate — kubectl, Argo, KEDA, and your dashboards untouched — and every node carries a budget, so nothing can outlive its cost cap.",
    points: [
      "composes with Karpenter & KEDA — replaces neither",
      "cloud credentials never touch your cluster",
      "budget + deadline reaper on every burst",
    ],
    cta: { label: "create account →", href: SIGNUP_URL },
  },
  {
    eyebrow: "AI / ML TEAMS",
    title: "Train tonight.",
    body:
      "No GPU waitlist, no one-hour minimums, no reserved instances gathering dust. Say what the job needs and an A100 exists for exactly as long as the epochs take — checkpoints stream to your bucket, the node evaporates, the meter stops.",
    points: [
      "a 17-min A100 fine-tune: $0.59",
      "$ per epoch — not $ per month",
      "kubectl logs & exec work mid-run",
    ],
    cta: { label: "create account →", href: SIGNUP_URL },
  },
  {
    eyebrow: "HOMELABBERS",
    title: "Give the lab real muscles.",
    body:
      "Your three-node k3s box stays cozy — and when a job needs more than it has, a real cloud node joins your cluster for cents, does the work, and leaves. Self-host the whole engine free on your own accounts.",
    points: [
      "self-host free — your clouds, your keys, your bill",
      "cloud cost estimates for each experiment",
      "one Helm chart; nothing else to run in-cluster",
    ],
    cta: { label: "create account →", href: SIGNUP_URL },
  },
];

export function WhoItsFor({ isMobile }) {
  return (
    <section id="who" className="rule" style={{ padding: isMobile ? "56px 0" : "80px 0" }}>
      <div className="wrap">
        <div className="eyebrow reveal">WHO IT'S FOR</div>
        <h2 className="display reveal" style={{ fontSize: isMobile ? "clamp(23px, 7.2vw, 34px)" : 52, margin: "14px 0 0", maxWidth: 900 }}>
          Three very different jobs.<br />
          <span className="mark">One burst engine.</span>
        </h2>

        <div
          className="reveal"
          style={{
            display: "grid",
            gridTemplateColumns: isMobile ? "1fr" : "repeat(3, 1fr)",
            border: "1px solid var(--line)",
            borderRadius: 12,
            marginTop: 44,
            background: "var(--paper)",
          }}
        >
          {AUDIENCES.map((a, i) => (
            <div
              key={a.eyebrow}
              style={{
                display: "flex",
                flexDirection: "column",
                padding: isMobile ? "26px 22px" : "32px 28px",
                borderLeft: !isMobile && i > 0 ? "1px solid var(--line)" : "none",
                borderTop: isMobile && i > 0 ? "1px solid var(--line)" : "none",
              }}
            >
              <div className="eyebrow" style={{ color: "var(--accent-deep)" }}>{a.eyebrow}</div>
              <h3 className="display" style={{ fontSize: isMobile ? 22 : 24, marginTop: 12 }}>{a.title}</h3>
              <p style={{ marginTop: 12, fontSize: 14.5, color: "var(--ink-60)", flexGrow: 1 }}>{a.body}</p>
              <ul style={{ listStyle: "none", margin: "16px 0 0", padding: 0, display: "grid", gap: 8, fontFamily: "var(--font-body)", fontSize: 13.5, color: "var(--ink)" }}>
                {a.points.map((p) => (
                  <li key={p} className="num" style={{ display: "flex", gap: 8 }}>
                    <span style={{ color: "var(--accent-deep)", flexShrink: 0 }}>→</span>
                    <span>{p}</span>
                  </li>
                ))}
              </ul>
              <a
                href={a.cta.href}
                className="eyebrow"
                style={{
                  display: "inline-flex",
                  alignItems: "center",
                  minHeight: 44,
                  marginTop: 20,
                  color: "var(--ink)",
                  textDecoration: "underline",
                  textUnderlineOffset: 3,
                }}
              >
                {a.cta.label}
              </a>
            </div>
          ))}
        </div>
        <div className="eyebrow reveal num" style={{ marginTop: 12, textAlign: "right" }}>
          fig. 03 — same Workload YAML for all three
        </div>
      </div>
    </section>
  );
}
