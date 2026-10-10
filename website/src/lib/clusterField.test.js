import test from "node:test";
import assert from "node:assert/strict";
import { createField, stepField, settleField, burstAt, resizeField, demandAt, setQuietZones, STEP } from "./clusterField.js";

const run = (state, seconds, pointer = null) => {
  for (let t = 0; t < seconds; t += STEP) stepField(state, STEP, pointer);
};
const bursts = (state, s) => state.particles.filter((p) => p.kind === "burst" && p.state === s).length;

test("simulation is deterministic and stays finite", () => {
  const a = createField(1200, 700, 42);
  const b = createField(1200, 700, 42);
  run(a, 30);
  run(b, 30);
  assert.deepEqual(a.particles.map((p) => [p.x, p.y]), b.particles.map((p) => [p.x, p.y]));
  for (const p of a.particles) assert(Number.isFinite(p.x) && Number.isFinite(p.y) && Number.isFinite(p.vx), "finite");
});

test("bursts follow demand: they join the cluster and are later reaped", () => {
  const s = createField(1200, 700, 3);
  let joined = 0;
  let reaped = 0;
  for (let t = 0; t < 40; t += STEP) {
    stepField(s, STEP, null);
    joined = Math.max(joined, bursts(s, "joined"));
    reaped = Math.max(reaped, bursts(s, "reaped"));
  }
  assert(joined >= 8, `peak joined bursts ${joined}`);
  assert(reaped >= 1, "bursts are reaped");
  assert(demandAt(s) >= 0 && demandAt(s) <= 1);
});

test("joined nodes settle near their orbit, not off-screen", () => {
  const s = createField(1200, 700, 9);
  settleField(s, 20);
  for (const p of s.particles) {
    if (p.state !== "joined") continue;
    const d = Math.hypot(p.x - s.config.core.x, (p.y - s.config.core.y) / s.config.squash);
    assert(d < 230, `orbiting node drifted ${d.toFixed(0)}px`);
  }
});

test("pointer repels nearby nodes and a click launches a shockwave with bursts", () => {
  const s = createField(1200, 700, 11);
  settleField(s, 8);
  const resident = s.particles.find((p) => p.kind === "resident");
  const pointer = { x: resident.x + 4, y: resident.y, active: true };
  const before = Math.hypot(resident.x - pointer.x, resident.y - pointer.y);
  run(s, 0.25, pointer);
  assert(Math.hypot(resident.x - pointer.x, resident.y - pointer.y) > before, "pointer pushes away");

  const idleBefore = bursts(s, "idle");
  burstAt(s, 300, 300);
  assert.equal(s.waves.length, 1);
  assert(bursts(s, "idle") <= idleBefore, "click launches idle bursts");
  run(s, 1);
  assert.equal(s.waves.length, 0, "shockwave expires");
});

test("resize keeps nodes proportionally placed and switches to the compact layout", () => {
  const s = createField(1200, 700, 5);
  settleField(s, 5);
  const p = s.particles[0];
  const [x, y] = [p.x, p.y];
  resizeField(s, 600, 700);
  assert(Math.abs(p.x - x / 2) < 1e-9 && Math.abs(p.y - y) < 1e-9);
  assert.equal(s.config.compact, true);
  run(s, 5);
  for (const q of s.particles) assert(Number.isFinite(q.x));
});

test("quiet zones dim nodes behind the copy without moving them", () => {
  const s = createField(1200, 700, 13);
  settleField(s, 6);
  const before = s.particles.map((p) => [p.x, p.y]);
  setQuietZones(s, [{ x: 0, y: 0, width: 1200, height: 700 }]);
  assert.equal(s.quiet.length, 1);
  assert.deepEqual(s.particles.map((p) => [p.x, p.y]), before);
});
