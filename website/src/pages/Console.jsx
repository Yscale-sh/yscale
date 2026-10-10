import { useState } from "react";
import { SIGNUP_URL } from "../lib/identity.js";
import { Link } from "../lib/router.jsx";

const RANGES = {
  "24h": {
    ready: "68s",
    percentiles: ["36s", "68s", "101s"],
    success: "99.4%",
    route: "79ms",
    budget: 91,
    burn: "0.42×",
    points: [136, 121, 128, 104, 112, 87, 95, 73, 88, 66, 76, 62],
  },
  "7d": {
    ready: "73s",
    percentiles: ["41s", "73s", "118s"],
    success: "99.2%",
    route: "84ms",
    budget: 86,
    burn: "0.71×",
    points: [145, 128, 132, 111, 121, 91, 104, 82, 96, 71, 84, 68],
  },
  "30d": {
    ready: "81s",
    percentiles: ["44s", "81s", "136s"],
    success: "98.9%",
    route: "91ms",
    budget: 72,
    burn: "1.08×",
    points: [154, 142, 147, 129, 136, 118, 123, 105, 112, 94, 102, 86],
  },
};

const TRACE = [
  ["01", "Pod pending", "train-llama-0 requests 1× A100", "done"],
  ["02", "Provider selected", "Linode · us-east · quota verified", "done"],
  ["03", "Node joining", "ys-burst-7c4a91 · tunnel established", "done"],
  ["04", "Kubelet ready", "CSR approved · route probe 82ms", "done"],
  ["05", "Workload running", "kubectl logs + exec available", "live"],
  ["06", "Reap scheduled", "after completion · hard stop in 47m", "next"],
];

const WORKLOADS = [
  {
    name: "train-llama-0",
    namespace: "ml-train",
    phase: "Running",
    target: "Linode · us-east",
    node: "ys-burst-7c4a91",
    age: "13m 42s",
    cost: "$0.38",
    guardrail: "$2.50 · 47m",
    access: "logs · exec",
  },
  {
    name: "transcode-4fx2",
    namespace: "media",
    phase: "Starting",
    target: "Fly · ord",
    node: "ys-burst-91af20",
    age: "48s",
    cost: "$0.02",
    guardrail: "$0.80 · 19m",
    access: "logs",
  },
  {
    name: "nightly-index-287",
    namespace: "search",
    phase: "Queued",
    target: "policy matching",
    node: "—",
    age: "11s",
    cost: "$0.00",
    guardrail: "$1.20 · 30m",
    access: "pending",
  },
];

const CAPACITY = [
  { provider: "Fly.io", region: "ord", state: "Cold start only", detail: "warm resume not enabled", cost: "teardown destroys VM", tone: "watch" },
  { provider: "Linode", region: "us-east", state: "Running", detail: "1× A100 allocated", cost: "$0.00058/sec", tone: "live" },
  { provider: "AWS", region: "us-east-2", state: "On demand", detail: "quota checked 2m ago", cost: "provider costs after boot", tone: "ready" },
];

const AUDIT = [
  ["03:31:42", "route", "kubelet exact-GET probe recovered", "healthy"],
  ["03:29:08", "budget", "train-llama crossed 50% of ceiling", "watch"],
  ["03:18:47", "reap", "ys-burst-2d108e deleted at completion", "healthy"],
  ["03:17:12", "connector", "heartbeat resumed after 8s backoff", "healthy"],
];

function LatencyChart({ points, label }) {
  const width = 640;
  const height = 180;
  const max = 180;
  const min = 40;
  const plotted = points.map((value, index) => {
    const x = (index / (points.length - 1)) * width;
    const y = height - ((value - min) / (max - min)) * height;
    return `${x.toFixed(1)},${Math.max(8, Math.min(height - 8, y)).toFixed(1)}`;
  }).join(" ");

  return (
    <div className="latency-chart">
      <svg viewBox={`0 0 ${width} ${height}`} role="img" aria-label={label}>
        <g className="chart-grid" aria-hidden="true">
          <line x1="0" y1="30" x2={width} y2="30" />
          <line x1="0" y1="90" x2={width} y2="90" />
          <line x1="0" y1="150" x2={width} y2="150" />
        </g>
        <line className="chart-slo" x1="0" y1="70" x2={width} y2="70" />
        <polyline className="chart-line" points={plotted} />
      </svg>
      <div className="chart-axis" aria-hidden="true"><span>oldest</span><span>SLO target 90s</span><span>now</span></div>
    </div>
  );
}

function Metric({ label, value, detail, tone = "neutral" }) {
  return (
    <article className={`console-metric metric-${tone}`}>
      <span>{label}</span>
      <strong className="num">{value}</strong>
      <small>{detail}</small>
    </article>
  );
}

export function Console() {
  const [range, setRange] = useState("7d");
  const data = RANGES[range];

  return (
    <div className="console-page">
      <div className="console-atmosphere" aria-hidden="true" />
      <header className="console-context wrap">
        <div className="console-crumbs">
          <Link to="/">yscale.sh</Link><span>/</span><strong>control room</strong>
        </div>
        <div className="demo-notice"><i aria-hidden="true" /> DEMO DATA · PRODUCT PREVIEW</div>
        <span className="console-clock">updated just now · UTC</span>
      </header>

      <section className="console-intro wrap" aria-labelledby="console-title">
        <div>
          <p className="console-kicker">PROD-US · HOMELAB-01 · SRE CONTROL ROOM</p>
          <h1 id="console-title">Your cluster has<br /><span>room to breathe.</span></h1>
          <p className="console-lede">Capacity, reliability, cost, and the exact Kubernetes path—one operational view from pending Pod to reaped node.</p>
        </div>
        <div className="console-health" aria-label="Cluster connection health">
          <div><span className="health-dot" aria-hidden="true" /><small>overall</small><strong>Operational</strong></div>
          <div><small>Connector heartbeat</small><strong className="num">8s ago</strong></div>
          <div><small>connected routes</small><strong className="num">6 / 6</strong></div>
        </div>
      </section>

      <div className="console-action wrap" role="status">
        <span><b>WATCH</b> 30-day error-budget burn is above baseline.</span>
        <span>No page required · review capacity policy this week</span>
      </div>

      <section className="console-metrics wrap" aria-label="Service-level indicators">
        <Metric label="Pending → Ready p95" value={data.ready} detail={`${range} · target < 90s`} tone="healthy" />
        <Metric label="Burst success" value={data.success} detail={`${range} · target 99.0%`} tone="healthy" />
        <Metric label="Error budget left" value={`${data.budget}%`} detail={`99.9% SLO · burn ${data.burn}`} tone={data.budget < 80 ? "watch" : "healthy"} />
        <Metric label="Kubelet route p95" value={data.route} detail={`${range} · exact GET probe`} tone="healthy" />
      </section>

      <section className="console-observe wrap" aria-label="Latency and reliability">
        <article className="console-panel latency-panel">
          <div className="panel-head">
            <div><span className="panel-kicker">LATENCY SLI</span><h2>Pending Pod → Ready node</h2></div>
            <div className="range-switch" aria-label="Metric time range">
              {Object.keys(RANGES).map((key) => (
                <button key={key} type="button" aria-pressed={range === key} onClick={() => setRange(key)}>{key}</button>
              ))}
            </div>
          </div>
          <div className="percentile-row" aria-label={`${range} latency percentiles`}>
            {data.percentiles.map((value, index) => <div key={value}><span>p{[50, 95, 99][index]}</span><strong className="num">{value}</strong></div>)}
          </div>
          <LatencyChart points={data.points} label={`${range} pending-to-ready latency trend. p95 is ${data.ready}, below the 90 second target.`} />
        </article>

        <article className="console-panel budget-panel">
          <div className="panel-head"><div><span className="panel-kicker">RELIABILITY</span><h2>30-day error budget</h2></div><strong className="budget-number num">{data.budget}%</strong></div>
          <div className="budget-track" role="img" aria-label={`${data.budget} percent of error budget remaining`}><i style={{ width: `${data.budget}%` }} /></div>
          <div className="budget-legend"><span>remaining</span><span>burn rate <b className="num">{data.burn}</b></span></div>
          <dl className="reliability-list">
            <div><dt>Availability SLO</dt><dd className="num">99.9%</dd></div>
            <div><dt>Fast burn · 1h</dt><dd>clear</dd></div>
            <div><dt>Slow burn · 6h</dt><dd>clear</dd></div>
            <div><dt>Pages this window</dt><dd className="num">0</dd></div>
          </dl>
          <p className="panel-note">Burn combines failed provisions, nodes missing the Ready deadline, and kubelet-route probe failures.</p>
        </article>
      </section>

      <section className="console-route wrap" aria-labelledby="route-title">
        <div className="section-heading"><div><span className="panel-kicker">LIVE BURST TRACE · DEMO</span><h2 id="route-title">One workload. Every handoff.</h2></div><span className="route-elapsed num">elapsed 01:13</span></div>
        <ol className="route-line">
          {TRACE.map(([code, title, detail, state]) => (
            <li key={code} className={`route-${state}`}>
              <span className="route-code">{code}</span>
              <i aria-hidden="true" />
              <div><strong>{title}</strong><small>{detail}</small></div>
            </li>
          ))}
        </ol>
      </section>

      <section className="console-workloads wrap" aria-labelledby="workloads-title">
        <div className="section-heading"><div><span className="panel-kicker">WORKLOADS</span><h2 id="workloads-title">What is running—and what can run away.</h2></div><span className="table-count">3 active · 2 providers</span></div>
        <div className="workload-table-wrap">
          <table className="workload-table">
            <thead><tr><th scope="col">Workload</th><th scope="col">Phase</th><th scope="col">Placement</th><th scope="col">Age</th><th scope="col">Live cost</th><th scope="col">Ceiling · deadline</th><th scope="col">kubectl</th></tr></thead>
            <tbody>
              {WORKLOADS.map((workload) => (
                <tr key={workload.name}>
                  <th scope="row"><strong>{workload.name}</strong><small>{workload.namespace}</small></th>
                  <td data-label="Phase"><span className={`phase phase-${workload.phase.toLowerCase()}`}>{workload.phase}</span></td>
                  <td data-label="Placement"><strong>{workload.target}</strong><small>{workload.node}</small></td>
                  <td data-label="Age" className="num">{workload.age}</td>
                  <td data-label="Live cost" className="num">{workload.cost}</td>
                  <td data-label="Ceiling · deadline" className="num">{workload.guardrail}</td>
                  <td data-label="kubectl">{workload.access}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </section>

      <section className="console-lower wrap">
        <article className="capacity-panel" aria-labelledby="capacity-title">
          <div className="section-heading"><div><span className="panel-kicker">CAPACITY INVENTORY</span><h2 id="capacity-title">Warm is a state, not a promise.</h2></div></div>
          <div className="capacity-list">
            {CAPACITY.map((item) => (
              <div key={`${item.provider}-${item.region}`} className={`capacity-row capacity-${item.tone}`}>
                <i aria-hidden="true" /><div><strong>{item.provider}</strong><small>{item.region}</small></div><div><span>{item.state}</span><small>{item.detail}</small></div><b>{item.cost}</b>
              </div>
            ))}
          </div>
          <p className="panel-note">Fly suspend can preserve machine memory, but Yscale currently creates auto-destroy burst Machines. Warm inventory stays disabled until the suspend/resume lifecycle test passes.</p>
        </article>

        <article className="audit-panel" aria-labelledby="audit-title">
          <div className="section-heading"><div><span className="panel-kicker">INCIDENTS + AUDIT</span><h2 id="audit-title">The trail explains itself.</h2></div></div>
          <ol className="audit-list">
            {AUDIT.map(([time, kind, message, tone]) => <li key={`${time}-${kind}`} className={`audit-${tone}`}><time>{time}</time><b>{kind}</b><span>{message}</span></li>)}
          </ol>
          <div className="console-next">
            <div><span>NEXT ACTION</span><strong>Verify your first cluster route.</strong></div>
            <a href={SIGNUP_URL}>connect a cluster <span aria-hidden="true">↗</span></a>
          </div>
        </article>
      </section>
    </div>
  );
}
