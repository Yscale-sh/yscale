import { useEffect, useRef } from "react";
import { STEP, burstAt, createField, drawField, resizeField, setQuietZones, settleField, stepField } from "../lib/clusterField.js";

// Decorative particle field behind the hero. One requestAnimationFrame loop with
// a fixed physics step; paused when the hero is offscreen or the tab is hidden;
// a single settled frame (no animation) for prefers-reduced-motion.
const INTERACTIVE = "a, button, summary, input, textarea, select, [role='button']";

function readColors() {
  const css = getComputedStyle(document.documentElement);
  const v = (name, fallback) => css.getPropertyValue(name).trim() || fallback;
  return { ink: v("--ink", "#171714"), accent: v("--accent", "#f4c900"), accentDeep: v("--accent-deep", "#6e5b00") };
}

export function ClusterField() {
  const canvasRef = useRef(null);

  useEffect(() => {
    const canvas = canvasRef.current;
    const host = canvas?.parentElement;
    const ctx = canvas?.getContext("2d");
    if (!canvas || !host || !ctx) return undefined;

    const reduce = window.matchMedia("(prefers-reduced-motion: reduce)");
    const colors = readColors();
    const pointer = { x: 0, y: 0, active: false };
    let state = null;
    let raf = 0;
    let last = 0;
    let acc = 0;
    let onScreen = true;
    let running = false;

    const resize = () => {
      const rect = canvas.getBoundingClientRect();
      if (rect.width < 1 || rect.height < 1) return;
      const dpr = Math.min(window.devicePixelRatio || 1, rect.width < 700 ? 1.5 : 2);
      canvas.width = Math.round(rect.width * dpr);
      canvas.height = Math.round(rect.height * dpr);
      ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
      if (!state) {
        state = createField(rect.width, rect.height);
        settleField(state, 4);
      } else {
        resizeField(state, rect.width, rect.height);
      }
      // Dim the field behind the headline and intro copy.
      const quiet = [...host.querySelectorAll("h1 .hero-line, .source-intro > div > p")].flatMap((el) =>
        [...el.getClientRects()].map((r) => ({ x: r.left - rect.left, y: r.top - rect.top, width: r.width, height: r.height })));
      setQuietZones(state, quiet);
      if (!running) drawField(ctx, state, colors);
    };

    const frame = (now) => {
      raf = 0;
      if (!running) return;
      acc += Math.min(0.05, (now - last) / 1000);
      last = now;
      while (acc >= STEP) {
        stepField(state, STEP, pointer);
        acc -= STEP;
      }
      drawField(ctx, state, colors);
      raf = requestAnimationFrame(frame);
    };
    const start = () => {
      if (running || !state || reduce.matches || !onScreen || document.hidden) return;
      running = true;
      last = performance.now();
      acc = 0;
      raf = requestAnimationFrame(frame);
    };
    const stop = () => {
      running = false;
      if (raf) cancelAnimationFrame(raf);
      raf = 0;
    };

    const local = (event) => {
      const rect = canvas.getBoundingClientRect();
      return { x: event.clientX - rect.left, y: event.clientY - rect.top };
    };
    const onMove = (event) => {
      if (event.pointerType === "touch") return;
      Object.assign(pointer, local(event), { active: true });
    };
    const onLeave = () => { pointer.active = false; };
    const onDown = (event) => {
      if (!state || reduce.matches || event.target.closest(INTERACTIVE)) return;
      const p = local(event);
      burstAt(state, p.x, p.y);
      start();
    };
    const onVisibility = () => (document.hidden ? stop() : start());
    const onMotionPreference = () => {
      if (reduce.matches) {
        stop();
        if (state) drawField(ctx, state, colors);
      } else {
        start();
      }
    };

    const resizeObserver = new ResizeObserver(resize);
    resizeObserver.observe(canvas);
    const intersection = new IntersectionObserver(([entry]) => {
      onScreen = entry.isIntersecting;
      if (onScreen) start();
      else stop();
    });
    intersection.observe(host);
    host.addEventListener("pointermove", onMove);
    host.addEventListener("pointerleave", onLeave);
    host.addEventListener("pointerdown", onDown);
    document.addEventListener("visibilitychange", onVisibility);
    reduce.addEventListener("change", onMotionPreference);
    resize();
    start();
    // Re-measure the quiet zones once the headline has settled and fonts load.
    const settleTimer = window.setTimeout(resize, 1500);
    document.fonts?.ready.then(resize).catch(() => {});

    return () => {
      window.clearTimeout(settleTimer);
      stop();
      resizeObserver.disconnect();
      intersection.disconnect();
      host.removeEventListener("pointermove", onMove);
      host.removeEventListener("pointerleave", onLeave);
      host.removeEventListener("pointerdown", onDown);
      document.removeEventListener("visibilitychange", onVisibility);
      reduce.removeEventListener("change", onMotionPreference);
    };
  }, []);

  return <canvas ref={canvasRef} className="cluster-field" aria-hidden="true" />;
}
