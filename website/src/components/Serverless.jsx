// Serverless.jsx — "The Fair Question" section.
// Addresses the "why not just go serverless?" objection before the conversion ask.
// Placement: between UseCases and Receipts/PickYourLane.

const LEFT_ITEMS = [
  "webhooks & event glue",
  "spiky HTTP endpoints",
  "code you never want to operate",
];

const RIGHT_ITEMS = [
  "training runs & fine-tunes",
  "GPU inference",
  "custom containers & multi-GB images",
  "jobs with volumes, sidecars, or six-hour runtimes",
  "anything your ML team already ships as a pod",
];

export function Serverless({ isMobile }) {
  return (
    <section className="rule" style={{ padding: isMobile ? "64px 0" : "88px 0" }}>
      <div className="wrap">
        <div className="eyebrow reveal">THE FAIR QUESTION</div>
        <h2
          className="display reveal"
          style={{
            fontSize: isMobile ? "clamp(38px, 10vw, 56px)" : 68,
            margin: "12px 0 0",
            maxWidth: 820,
            color: "var(--fg)",
            lineHeight: 1.0,
          }}
        >
          Serverless economics.{" "}
          <span className="mark">Your Kubernetes.</span>
        </h2>

        <div
          style={{
            maxWidth: 680,
            marginTop: 36,
            display: "grid",
            gap: 20,
            fontSize: 16,
            lineHeight: 1.68,
            color: "var(--fg-muted)",
          }}
        >
          <p className="reveal">
            "Why not just go serverless?" Sometimes you should. For webhooks,
            event glue, and spiky request traffic, a function you never have to
            operate is the right call. No argument here.
          </p>
          <p className="reveal">
            But if you run a platform team, you already own a cluster. There's
            RBAC in it. Observability you trust. GitOps that deploys it, secrets
            management that guards it, GPU drivers somebody fought a whole
            afternoon for. A serverless platform replaces none of that. It sits
            beside it: a second platform with its own IAM, its own limits, its
            own bill.
          </p>
          <p className="reveal">
            And the teams that need elastic compute most can't ship there anyway.
            ML and data science teams ship custom containers with CUDA baked in.
            Training jobs that run for hours. Images measured in gigabytes. Pods
            that need a GPU, a volume, and each other. That's not a function.
            That's a pod, and a pod already has a home.
          </p>
          <p className="reveal">
            yscale takes the part of serverless worth stealing: ephemeral
            nodes, zero idle, scale from zero. And it delivers that into the
            cluster you already operate. Same kubectl. Same pipelines. Same RBAC.
            When the job ends, the node is gone, and so is the bill.
          </p>
        </div>

        {/* Ledger — two columns, mono, respectful to both sides */}
        <div
          className="reveal card"
          style={{
            marginTop: 44,
            padding: isMobile ? "28px 24px" : "32px 40px",
            overflow: "hidden",
          }}
        >
          <div
            style={{
              display: "grid",
              gridTemplateColumns: isMobile ? "1fr" : "1fr 1fr",
              gap: isMobile ? 28 : 0,
            }}
          >
            {/* Left column */}
            <div
              style={{
                paddingRight: isMobile ? 0 : 32,
                borderRight: isMobile ? "none" : "1px solid var(--border)",
                paddingBottom: isMobile ? 28 : 0,
                borderBottom: isMobile ? "1px solid var(--border)" : "none",
              }}
            >
              <div
                className="eyebrow"
                style={{ marginBottom: 18, color: "var(--fg-faint)", letterSpacing: "0.1em" }}
              >
                KEEP SERVERLESS FOR
              </div>
              <ul
                style={{
                  listStyle: "none",
                  margin: 0,
                  padding: 0,
                  display: "grid",
                  gap: 10,
                  fontFamily: "var(--font-mono)",
                  fontSize: isMobile ? 12.5 : 13,
                  lineHeight: 1.55,
                  color: "var(--fg-muted)",
                }}
              >
                {LEFT_ITEMS.map((item) => (
                  <li key={item} style={{ display: "flex", gap: 10 }}>
                    <span style={{ color: "var(--fg-faint)", flexShrink: 0 }}>·</span>
                    <span>{item}</span>
                  </li>
                ))}
              </ul>
            </div>

            {/* Right column */}
            <div style={{ paddingLeft: isMobile ? 0 : 32 }}>
              <div
                className="eyebrow"
                style={{ marginBottom: 18, color: "var(--fg-faint)", letterSpacing: "0.1em" }}
              >
                BURST YOUR CLUSTER FOR
              </div>
              <ul
                style={{
                  listStyle: "none",
                  margin: 0,
                  padding: 0,
                  display: "grid",
                  gap: 10,
                  fontFamily: "var(--font-mono)",
                  fontSize: isMobile ? 12.5 : 13,
                  lineHeight: 1.55,
                  color: "var(--fg-muted)",
                }}
              >
                {RIGHT_ITEMS.map((item) => (
                  <li key={item} style={{ display: "flex", gap: 10 }}>
                    <span style={{ color: "var(--fg-faint)", flexShrink: 0 }}>·</span>
                    <span>{item}</span>
                  </li>
                ))}
              </ul>
            </div>
          </div>

          {/* Kicker */}
          <p
            style={{
              marginTop: 28,
              paddingTop: 20,
              borderTop: "1px solid var(--border)",
              fontFamily: "var(--font-mono)",
              fontSize: 12,
              color: "var(--fg-faint)",
              letterSpacing: "0.02em",
            }}
          >
            Serverless for the glue. Your cluster for the work.
          </p>
        </div>

        <div
          className="eyebrow reveal num"
          style={{ marginTop: 8, textAlign: "right", color: "var(--fg-faint)" }}
        >
          fig. 04 — same economics, no second platform
        </div>
      </div>
    </section>
  );
}
