import { useEffect, useRef, useState } from "react";

// SAME OLD CLUSTER. ANY COMPUTE. — the use-case cycler. Four workload
// vignettes (train / transcode / edge / batch), each a tiny animated
// mono-art scene in the dark panel. Tabs auto-advance; clicking pins.
// Same derived-clock pattern as BurstLifecycle: one interval, everything
// a pure function of t, paused offscreen, reduced-motion = final frame.

const CYCLE = 5200; // ms per vignette when auto-advancing

const CASES = [
  {
    key: "train",
    tab: "GPU TRAINING",
    line: "An A100 appears, your fine-tune runs, the node vanishes.",
    sub: "$ per epoch — not $ per month.",
    node: "gpu-a100 · 80 GB",
    render: (f) => {
      const pct = Math.min(1, f / 0.85);
      const bars = Math.round(pct * 22);
      const loss = (2.31 - 1.89 * pct).toFixed(2);
      return [
        ["dim", "$ yscale: workload train-llama → 1× gpu-a100"],
        ["fg", `epoch ${Math.min(12, 1 + Math.floor(pct * 12))}/12  ${"▰".repeat(bars)}${"▱".repeat(22 - bars)}`],
        ["lime", `loss ${loss}  ·  $${(pct * 0.98).toFixed(2)} so far`],
        pct >= 1 ? ["green", "✓ checkpoints synced — node reaped"] : ["dim", "checkpoint every 500 steps → your bucket"],
      ];
    },
  },
  {
    key: "transcode",
    tab: "TRANSCODE",
    line: "The 4K queue chews itself through burst CPUs overnight.",
    sub: "The farm costs nothing by breakfast.",
    node: "cpu-large ×4",
    render: (f) => {
      const done = Math.min(24, Math.floor(f * 26));
      return [
        ["dim", "$ yscale: queue night-batch → 4× cpu-large"],
        ["fg", `ep${String(done).padStart(2, "0")}.mov → av1  ${done < 24 ? "⣾ encoding" : "done"}`],
        ["lime", `${done}/24 files  ·  212 fps aggregate`],
        done >= 24 ? ["green", "✓ queue empty — 4 nodes reaped"] : ["dim", "queue drains → nodes reap one by one"],
      ];
    },
  },
  {
    key: "edge",
    tab: "EDGE / CDN",
    line: "Fan out close to users when traffic spikes.",
    sub: "Fold back to zero when it fades.",
    node: "cpu-nano ×3 regions",
    render: (f) => {
      const regions = [
        ["ord", 0.15, "9 ms"],
        ["fra", 0.4, "11 ms"],
        ["sin", 0.65, "14 ms"],
      ];
      const up = regions.filter(([, at]) => f >= at);
      return [
        ["dim", "$ yscale: spike detected → fan out 3 regions"],
        ...regions.map(([name, at, ms]) =>
          f >= at ? ["green", `✓ ${name}  edge node up · ${ms} to users`] : ["dim", `· ${name}  waiting…`]
        ),
        up.length === 3 && f > 0.9 ? ["lime", "traffic fading → folding back to 0"] : ["lime", `${up.length}/3 regions live`],
      ];
    },
  },
  {
    key: "batch",
    tab: "BATCH & CI",
    line: "Nightly jobs get nightly nodes.",
    sub: "Your devs' laptops stay laptops.",
    node: "cpu-medium ×2",
    render: (f) => {
      const jobs = ["migrate-db", "rebuild-index", "e2e-suite", "report-gen"];
      const done = Math.min(4, Math.floor(f * 4.6));
      return [
        ["dim", "$ cron 02:00 → 4 jobs queued"],
        ...jobs.slice(0, Math.max(1, done + 1)).map((j, i) =>
          i < done ? ["green", `✓ ${j}`] : ["fg", `⣾ ${j} running…`]
        ),
        done >= 4 ? ["lime", "all green · $0.31 total · nodes gone"] : ["dim", ""],
      ].filter((l) => l[1] !== "");
    },
  },
];

const COLOR = {
  dim: "var(--term-dim)",
  fg: "var(--term-fg)",
  green: "var(--term-green)",
  lime: "var(--accent)",
};

// Burst-block shapes per use case, on a 6×3 grid. Cols 1–3 are the steady
// cluster (always-on dim blocks); burst blocks (lime) snap into cols 4–6 as
// the vignette plays — one fat GPU block, four transcode workers, three
// scattered edge nodes, two batch runners — then reap out at the end.
const BASE_BLOCKS = [
  { c: 1, r: 1 }, { c: 2, r: 1 }, { c: 3, r: 1 },
  { c: 1, r: 2 }, { c: 2, r: 2 }, { c: 3, r: 2 },
  { c: 1, r: 3 }, { c: 2, r: 3 },
];

const BURST_BLOCKS = {
  train: [{ at: 0.18, c: 4, r: 1, w: 2, h: 2, label: "GPU" }],
  transcode: [
    { at: 0.12, c: 4, r: 1 }, { at: 0.24, c: 5, r: 1 },
    { at: 0.36, c: 4, r: 2 }, { at: 0.48, c: 5, r: 2 },
  ],
  edge: [
    { at: 0.15, c: 4, r: 1 }, { at: 0.4, c: 6, r: 2 }, { at: 0.65, c: 5, r: 3 },
  ],
  batch: [
    { at: 0.15, c: 4, r: 2 }, { at: 0.3, c: 5, r: 2 },
  ],
};

// The cluster-blocks visual: dim steady blocks + lime burst blocks that pop
// in (scale/opacity transition) as frac passes each threshold and reap out
// near the end of the cycle. Pure CSS transitions — the clock just flips
// booleans, so tab switches re-play naturally.
function BlockGrid({ caseKey, frac, reduced }) {
  const bursts = BURST_BLOCKS[caseKey] || [];
  const reaping = !reduced && frac > 0.94;
  return (
    <div
      aria-hidden="true"
      style={{
        display: "grid",
        gridTemplateColumns: "repeat(6, 1fr)",
        gridTemplateRows: "repeat(3, 1fr)",
        gap: 5,
        height: 96,
        marginTop: 16,
      }}
    >
      {BASE_BLOCKS.map((b, i) => (
        <div
          key={`b${i}`}
          style={{
            gridColumn: b.c,
            gridRow: b.r,
            border: "1px solid var(--term-line)",
            borderRadius: 2,
            background: "rgba(233,237,242,0.05)",
          }}
        />
      ))}
      {bursts.map((b, i) => {
        const on = reduced || frac >= b.at;
        return (
          <div
            key={`${caseKey}-${i}`}
            className="num"
            style={{
              gridColumn: `${b.c} / span ${b.w || 1}`,
              gridRow: `${b.r} / span ${b.h || 1}`,
              borderRadius: 2,
              background: "var(--accent)",
              boxShadow: on && !reaping ? "0 0 14px rgba(255,210,63,0.35)" : "none",
              opacity: on && !reaping ? 1 : 0,
              transform: on && !reaping ? "scale(1)" : reaping ? "scale(0.7)" : "scale(0.55)",
              transition: "opacity 320ms ease, transform 320ms cubic-bezier(0.2, 0.9, 0.3, 1.2), box-shadow 320ms ease",
              display: "grid",
              placeItems: "center",
              fontFamily: "var(--font-mono)",
              fontSize: 10,
              letterSpacing: "0.08em",
              color: "var(--ink)",
              fontWeight: 500,
            }}
          >
            {b.label || ""}
          </div>
        );
      })}
    </div>
  );
}

function usePrefersReducedMotion() {
  const [reduced, setReduced] = useState(
    () => typeof window !== "undefined" && window.matchMedia("(prefers-reduced-motion: reduce)").matches
  );
  useEffect(() => {
    const mq = window.matchMedia("(prefers-reduced-motion: reduce)");
    const on = () => setReduced(mq.matches);
    mq.addEventListener("change", on);
    return () => mq.removeEventListener("change", on);
  }, []);
  return reduced;
}

export function UseCases({ isMobile }) {
  const reduced = usePrefersReducedMotion();
  const [active, setActive] = useState(0);
  const [pinned, setPinned] = useState(false);
  const [t, setT] = useState(reduced ? CYCLE : 0);
  const rootRef = useRef(null);
  const tabRefs = useRef([]);
  const visibleRef = useRef(true);

  useEffect(() => {
    if (reduced) return;
    const el = rootRef.current;
    const io = new IntersectionObserver(([e]) => { visibleRef.current = e.isIntersecting; }, { threshold: 0.2 });
    if (el) io.observe(el);
    const iv = setInterval(() => {
      if (!visibleRef.current || document.hidden) return;
      setT((prev) => {
        const next = prev + 80;
        if (next >= CYCLE) {
          if (!pinned) setActive((a) => (a + 1) % CASES.length);
          return 0;
        }
        return next;
      });
    }, 80);
    return () => { clearInterval(iv); io.disconnect(); };
  }, [reduced, pinned]);

  const c = CASES[active];
  const frac = reduced ? 1 : Math.min(1, t / (CYCLE * 0.82));
  const lines = c.render(frac);

  function selectCase(index, moveFocus = false) {
    setActive(index);
    setPinned(true);
    setT(0);
    if (moveFocus) tabRefs.current[index]?.focus();
  }

  function onTabKeyDown(event, index) {
    let next = index;
    if (event.key === "ArrowRight" || event.key === "ArrowDown") next = (index + 1) % CASES.length;
    else if (event.key === "ArrowLeft" || event.key === "ArrowUp") next = (index - 1 + CASES.length) % CASES.length;
    else if (event.key === "Home") next = 0;
    else if (event.key === "End") next = CASES.length - 1;
    else return;
    event.preventDefault();
    selectCase(next, true);
  }

  return (
    <section ref={rootRef} className="rule" style={{ padding: isMobile ? "56px 0" : "80px 0" }}>
      <div className="wrap">
        <div className="eyebrow reveal">WHATEVER YOU'RE BURSTING FOR</div>
        <h2 className="display reveal" style={{ fontSize: isMobile ? "clamp(23px, 7.2vw, 34px)" : 52, margin: "14px 0 0", maxWidth: 900 }}>
          Same old cluster.<br /><span className="mark">Any compute.</span>
        </h2>
        <p className="reveal" style={{ maxWidth: 560, marginTop: 18, color: "var(--ink-60)", fontSize: 16 }}>
          Training runs, transcode queues, edge fan-outs, nightly batch — point
          the workload at the cluster you already run and it bursts to exactly
          the compute it needs. No new platform. No YAML rewrite.{" "}
          <strong style={{ color: "var(--ink)", fontWeight: 500 }}>It just works.</strong>
        </p>

        {/* tab bar */}
        <div
          className="reveal"
          role="tablist"
          aria-label="burst use cases"
          style={{ display: "flex", flexWrap: "wrap", gap: 8, marginTop: 32 }}
        >
          {CASES.map((uc, i) => (
            <button
              key={uc.key}
              ref={(el) => { tabRefs.current[i] = el; }}
              id={`usecase-tab-${uc.key}`}
              role="tab"
              aria-selected={i === active}
              aria-controls={`usecase-panel-${uc.key}`}
              tabIndex={i === active ? 0 : -1}
              onClick={() => selectCase(i)}
              onKeyDown={(event) => onTabKeyDown(event, i)}
              style={{
                fontFamily: "var(--font-body)",
                fontWeight: 600,
                fontSize: 13,
                letterSpacing: "0.01em",
                padding: "10px 14px",
                borderRadius: 2,
                border: "1px solid " + (i === active ? "var(--ink)" : "var(--line)"),
                background: i === active ? "var(--accent)" : "transparent",
                color: "var(--ink)",
                cursor: "pointer",
                position: "relative",
                overflow: "hidden",
              }}
            >
              {uc.tab}
              {/* auto-advance progress sliver on the active tab */}
              {i === active && !pinned && !reduced && (
                <span
                  aria-hidden="true"
                  style={{
                    position: "absolute", left: 0, bottom: 0, height: 2,
                    width: `${(t / CYCLE) * 100}%`, background: "var(--ink)", opacity: 0.5,
                  }}
                />
              )}
            </button>
          ))}
        </div>

        {/* the vignette */}
        <div
          className="reveal bifrost-edge"
          id={`usecase-panel-${c.key}`}
          role="tabpanel"
          aria-labelledby={`usecase-tab-${c.key}`}
          tabIndex={0}
          style={{
            marginTop: 16,
            display: "grid",
            gridTemplateColumns: isMobile ? "1fr" : "7fr 5fr",
            background: "var(--term-bg)",
            border: "1px solid rgba(22,24,29,0.08)",
            borderRadius: 14,
            boxShadow: "0 12px 32px rgba(16,24,40,0.12)",
            overflow: "hidden",
          }}
        >
          <div style={{ padding: isMobile ? "18px 16px" : "24px 26px", minHeight: isMobile ? 170 : 190, borderRight: isMobile ? "none" : "1px solid var(--term-line)", borderBottom: isMobile ? "1px solid var(--term-line)" : "none" }}>
            <div style={{ display: "flex", justifyContent: "space-between", marginBottom: 14 }}>
              <span style={{ fontFamily: "var(--font-mono)", fontSize: 11, letterSpacing: "0.14em", color: "var(--term-dim)" }}>
                {c.tab}
              </span>
              <span className="num" style={{ fontFamily: "var(--font-mono)", fontSize: 11, color: "var(--accent)" }}>
                burst: {c.node}
              </span>
            </div>
            <div className="num" style={{ fontFamily: "var(--font-mono)", fontSize: isMobile ? 12.5 : 13.5, lineHeight: 1.9 }}>
              {lines.map(([color, text], i) => (
                <div key={i} style={{ color: COLOR[color], whiteSpace: "pre-wrap" }}>{text}</div>
              ))}
            </div>
          </div>
          <div style={{ padding: isMobile ? "18px 16px" : "24px 26px", display: "flex", flexDirection: "column", justifyContent: "center" }}>
            <p style={{ fontSize: isMobile ? 16 : 18, lineHeight: 1.45, color: "var(--term-fg)", fontWeight: 500 }}>
              {c.line}
            </p>
            <p style={{ marginTop: 8, fontFamily: "var(--font-mono)", fontSize: 13, color: "var(--accent)" }}>
              {c.sub}
            </p>
            <BlockGrid caseKey={c.key} frac={frac} reduced={reduced} />
            <div style={{ display: "flex", justifyContent: "space-between", marginTop: 8, fontFamily: "var(--font-mono)", fontSize: 10, letterSpacing: "0.1em", color: "var(--term-dim)" }}>
              <span>YOUR NODES</span>
              <span style={{ color: "var(--accent)" }}>BURST</span>
            </div>
          </div>
        </div>
        <div className="eyebrow reveal num" style={{ marginTop: 12, textAlign: "right" }}>
          fig. 03 — one Workload YAML, any shape of compute
        </div>
      </div>
    </section>
  );
}
