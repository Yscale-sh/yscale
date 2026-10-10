// Cluster field: a small, deterministic particle simulation for the hero.
//
// It tells the product story in motion. Resident nodes orbit the cluster core.
// When demand rises, burst nodes stream in from the edges ("any cloud"), are
// pulled into orbit by damped springs, run for a while, then get reaped and
// fade out. Nearby nodes are linked like a mesh. The pointer repels nodes and a
// click sends a shockwave plus a few new bursts.
//
// Pure functions over plain objects: no DOM, no timers. The component owns the
// canvas and the single animation clock; tests drive stepField directly.

const TAU = Math.PI * 2;
export const STEP = 1 / 60;
const MAX_SPEED = 900;

export function mulberry32(seed) {
  let a = seed >>> 0;
  return function next() {
    a = (a + 0x6d2b79f5) >>> 0;
    let t = a;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

export function fieldConfig(width, height) {
  const compact = width < 700;
  return {
    compact,
    residents: compact ? 9 : 16,
    minBursts: compact ? 2 : 5,
    maxBursts: compact ? 12 : 28,
    linkDist: compact ? 78 : 118,
    pointerRadius: compact ? 0 : 140,
    core: { x: width * (compact ? 0.8 : 0.72), y: height * (compact ? 0.12 : 0.28) },
    rings: compact ? [30, 52, 74] : [48, 84, 120, 158],
    squash: compact ? 0.6 : 0.5,
    demandPeriod: 14,
  };
}

function ringTarget(state, p) {
  const r = state.config.rings[p.ring];
  return {
    x: state.config.core.x + Math.cos(p.angle) * r,
    y: state.config.core.y + Math.sin(p.angle) * r * state.config.squash,
  };
}

function orbitSpeed(radius, dir) {
  return (dir * 9) / Math.sqrt(radius);
}

function makeResident(state, i) {
  const { rings } = state.config;
  const ring = i % 2;
  const p = {
    kind: "resident", state: "joined", ring,
    angle: state.rand() * TAU,
    omega: orbitSpeed(rings[ring], 1),
    x: 0, y: 0, vx: 0, vy: 0, alpha: 1, age: 0, ttl: Infinity, delay: 0,
    r: 2.4 + state.rand() * 0.8,
  };
  const t = ringTarget(state, p);
  p.x = t.x;
  p.y = t.y;
  return p;
}

function makeBurst(state) {
  return {
    kind: "burst", state: "idle", ring: 1, angle: 0, omega: 0,
    x: -100, y: -100, vx: 0, vy: 0, alpha: 0, age: 0, ttl: 0,
    delay: state.rand() * 2, r: 3.2,
  };
}

export function createField(width, height, seed = 7) {
  const state = {
    width, height, time: 0, rand: mulberry32(seed),
    config: fieldConfig(width, height),
    particles: [], waves: [], quiet: [],
  };
  for (let i = 0; i < state.config.residents; i++) state.particles.push(makeResident(state, i));
  for (let i = 0; i < state.config.maxBursts; i++) state.particles.push(makeBurst(state));
  return state;
}

// Keep particles proportionally in place when the hero resizes.
export function resizeField(state, width, height) {
  if (width <= 0 || height <= 0) return;
  const sx = width / state.width;
  const sy = height / state.height;
  state.width = width;
  state.height = height;
  const next = fieldConfig(width, height);
  next.residents = state.config.residents;
  next.maxBursts = state.config.maxBursts;
  next.rings = next.rings.length === state.config.rings.length ? next.rings : state.config.rings;
  state.config = next;
  for (const p of state.particles) {
    p.x *= sx;
    p.y *= sy;
    p.ring = Math.min(p.ring, next.rings.length - 1);
  }
}

export function demandAt(state) {
  const { demandPeriod } = state.config;
  return 0.5 - 0.5 * Math.cos((TAU * state.time) / demandPeriod);
}

function targetBursts(state) {
  const { minBursts, maxBursts } = state.config;
  return minBursts + Math.round(demandAt(state) * (maxBursts - minBursts));
}

function spawnFromEdge(state, p) {
  const { width: w, height: h, config } = state;
  const edge = Math.floor(state.rand() * 4);
  const along = state.rand();
  if (edge === 0) { p.x = -12; p.y = along * h; }
  else if (edge === 1) { p.x = w + 12; p.y = along * h; }
  else if (edge === 2) { p.x = along * w; p.y = -12; }
  else { p.x = along * w; p.y = h + 12; }
  launch(state, p, config.core.x - p.x, config.core.y - p.y, 150 + state.rand() * 110);
}

function launch(state, p, dx, dy, speed) {
  const d = Math.hypot(dx, dy) || 1;
  const swirl = (state.rand() - 0.5) * 160;
  p.vx = (dx / d) * speed - (dy / d) * swirl;
  p.vy = (dy / d) * speed + (dx / d) * swirl;
  const { rings } = state.config;
  p.ring = 1 + Math.floor(state.rand() * (rings.length - 1));
  p.angle = state.rand() * TAU;
  p.omega = orbitSpeed(rings[p.ring], state.rand() < 0.15 ? -1 : 1);
  p.state = "inbound";
  p.alpha = 0;
  p.age = 0;
  p.ttl = 6 + state.rand() * 8;
}

function activeBursts(state) {
  let n = 0;
  for (const p of state.particles) if (p.kind === "burst" && (p.state === "inbound" || p.state === "joined")) n++;
  return n;
}

// Text boxes (canvas coordinates) where nodes and links are dimmed so the
// copy above the field stays crisp. The component measures them from the DOM.
export function setQuietZones(state, rects) {
  state.quiet = rects.map((r) => ({ x0: r.x - 6, y0: r.y - 6, x1: r.x + r.width + 6, y1: r.y + r.height + 6 }));
}

function quietness(state, x, y) {
  for (const q of state.quiet) if (x >= q.x0 && x <= q.x1 && y >= q.y0 && y <= q.y1) return 0.28;
  return 1;
}

// A click: shockwave plus up to three bursts launched from the point.
export function burstAt(state, x, y) {
  state.waves.push({ x, y, age: 0 });
  for (const p of state.particles) {
    const dx = p.x - x;
    const dy = p.y - y;
    const d = Math.hypot(dx, dy);
    if (d > 0 && d < 240) {
      const k = 520 * (1 - d / 240);
      p.vx += (dx / d) * k;
      p.vy += (dy / d) * k;
    }
  }
  let launched = 0;
  for (const p of state.particles) {
    if (launched === 3) break;
    if (p.kind !== "burst" || p.state !== "idle") continue;
    p.x = x;
    p.y = y;
    const a = state.rand() * TAU;
    launch(state, p, Math.cos(a), Math.sin(a), 260);
    launched++;
  }
}

export function stepField(state, dt, pointer) {
  state.time += dt;
  const { config } = state;
  const { core } = config;
  let wanted = targetBursts(state) - activeBursts(state);

  for (const p of state.particles) {
    p.age += dt;
    let ax = 0;
    let ay = 0;

    if (p.state === "idle") {
      p.delay -= dt;
      if (p.delay <= 0 && wanted > 0) {
        spawnFromEdge(state, p);
        wanted--;
      }
      continue;
    }

    if (p.state === "inbound" || p.state === "joined") {
      p.angle += p.omega * dt;
      const t = ringTarget(state, p);
      const joined = p.state === "joined";
      const k = joined ? 22 : 7;
      const c = joined ? 6.5 : 1.6;
      ax += k * (t.x - p.x) - c * p.vx;
      ay += k * (t.y - p.y) - c * p.vy;
      p.alpha = Math.min(1, p.alpha + dt / 0.6);
      if (!joined && (Math.hypot(t.x - p.x, t.y - p.y) < 10 || p.age > 7)) {
        p.state = "joined";
        p.age = 0;
      }
      if (joined && p.kind === "burst" && p.age > p.ttl) {
        p.state = "reaped";
        p.age = 0;
      }
    } else if (p.state === "reaped") {
      const dx = p.x - core.x;
      const dy = p.y - core.y;
      const d = Math.hypot(dx, dy) || 1;
      ax += (dx / d) * 90 - 0.8 * p.vx;
      ay += (dy / d) * 90 - 0.8 * p.vy;
      p.alpha -= dt / 1.4;
      if (p.alpha <= 0) {
        p.alpha = 0;
        p.state = "idle";
        p.delay = 0.4 + state.rand() * 1.6;
        continue;
      }
    }

    if (pointer && pointer.active && config.pointerRadius > 0) {
      const dx = p.x - pointer.x;
      const dy = p.y - pointer.y;
      const d = Math.hypot(dx, dy);
      if (d > 0 && d < config.pointerRadius) {
        const f = 2600 * (1 - d / config.pointerRadius) ** 2;
        ax += (dx / d) * f;
        ay += (dy / d) * f;
      }
    }

    p.vx += ax * dt;
    p.vy += ay * dt;
    const speed = Math.hypot(p.vx, p.vy);
    if (speed > MAX_SPEED) {
      p.vx *= MAX_SPEED / speed;
      p.vy *= MAX_SPEED / speed;
    }
    p.x += p.vx * dt;
    p.y += p.vy * dt;
  }

  for (const w of state.waves) w.age += dt;
  state.waves = state.waves.filter((w) => w.age < 0.9);
}

// Run the simulation offscreen so the first frame is already populated.
export function settleField(state, seconds) {
  for (let t = 0; t < seconds; t += STEP) stepField(state, STEP, null);
}

function visible(p) {
  return p.state !== "idle" && p.alpha > 0.02;
}

export function drawField(ctx, state, colors) {
  const { width: w, height: h, config } = state;
  const { core, rings, squash, linkDist } = config;
  ctx.clearRect(0, 0, w, h);

  // Orbits of the cluster.
  ctx.lineWidth = 1;
  ctx.strokeStyle = colors.ink;
  rings.forEach((r, i) => {
    ctx.globalAlpha = 0.09 - i * 0.012;
    ctx.setLineDash(i === 0 ? [] : [3, 6]);
    ctx.beginPath();
    ctx.ellipse(core.x, core.y, r, r * squash, 0, 0, TAU);
    ctx.stroke();
  });
  ctx.setLineDash([]);

  // Mesh links between nearby nodes (spatial grid, each pair once).
  const live = state.particles.filter(visible);
  for (const p of live) p.q = quietness(state, p.x, p.y);
  const grid = new Map();
  for (let i = 0; i < live.length; i++) {
    const key = `${Math.floor(live[i].x / linkDist)},${Math.floor(live[i].y / linkDist)}`;
    if (!grid.has(key)) grid.set(key, []);
    grid.get(key).push(i);
  }
  ctx.strokeStyle = colors.ink;
  for (let i = 0; i < live.length; i++) {
    const a = live[i];
    const cx = Math.floor(a.x / linkDist);
    const cy = Math.floor(a.y / linkDist);
    for (let gx = cx - 1; gx <= cx + 1; gx++) {
      for (let gy = cy - 1; gy <= cy + 1; gy++) {
        const cell = grid.get(`${gx},${gy}`);
        if (!cell) continue;
        for (const j of cell) {
          if (j <= i) continue;
          const b = live[j];
          const d = Math.hypot(a.x - b.x, a.y - b.y);
          if (d >= linkDist) continue;
          ctx.globalAlpha = 0.22 * (1 - d / linkDist) ** 1.5 * Math.min(a.alpha * a.q, b.alpha * b.q);
          ctx.beginPath();
          ctx.moveTo(a.x, a.y);
          ctx.lineTo(b.x, b.y);
          ctx.stroke();
        }
      }
    }
  }

  // Joined bursts tether to the core.
  ctx.strokeStyle = colors.accentDeep;
  for (const p of live) {
    if (p.kind !== "burst" || p.state !== "joined") continue;
    ctx.globalAlpha = 0.16 * p.alpha * p.q;
    ctx.beginPath();
    ctx.moveTo(core.x, core.y);
    ctx.lineTo(p.x, p.y);
    ctx.stroke();
  }

  // Click shockwaves.
  ctx.strokeStyle = colors.accentDeep;
  for (const wv of state.waves) {
    ctx.globalAlpha = 0.5 * (1 - wv.age / 0.9);
    ctx.lineWidth = 2;
    ctx.beginPath();
    ctx.arc(wv.x, wv.y, 18 + wv.age * 260, 0, TAU);
    ctx.stroke();
  }
  ctx.lineWidth = 1;

  // Nodes: residents in ink, bursts in accent with a short motion trail.
  for (const p of live) {
    if (p.kind === "burst" && p.state === "inbound") {
      ctx.globalAlpha = 0.45 * p.alpha * p.q;
      ctx.strokeStyle = colors.accentDeep;
      ctx.beginPath();
      ctx.moveTo(p.x - p.vx * 0.07, p.y - p.vy * 0.07);
      ctx.lineTo(p.x, p.y);
      ctx.stroke();
    }
    ctx.globalAlpha = (p.kind === "resident" ? 0.62 : 0.95) * p.alpha * p.q;
    ctx.beginPath();
    ctx.arc(p.x, p.y, p.r, 0, TAU);
    if (p.kind === "resident") {
      ctx.fillStyle = colors.ink;
      ctx.fill();
    } else if (p.state === "reaped") {
      ctx.strokeStyle = colors.ink;
      ctx.stroke();
    } else {
      ctx.fillStyle = colors.accent;
      ctx.fill();
      ctx.strokeStyle = colors.ink;
      ctx.stroke();
    }
  }

  // The core pulses with demand.
  const demand = demandAt(state);
  ctx.globalAlpha = 0.25 + 0.35 * demand;
  ctx.strokeStyle = colors.accentDeep;
  ctx.beginPath();
  ctx.arc(core.x, core.y, 10 + 10 * demand, 0, TAU);
  ctx.stroke();
  ctx.globalAlpha = 1;
  ctx.fillStyle = colors.accent;
  ctx.strokeStyle = colors.ink;
  ctx.beginPath();
  ctx.arc(core.x, core.y, 6, 0, TAU);
  ctx.fill();
  ctx.stroke();
  ctx.globalAlpha = 1;
}
