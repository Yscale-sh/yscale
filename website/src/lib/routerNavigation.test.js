import assert from "node:assert/strict";
import test from "node:test";

import { handleLinkClick } from "./routerNavigation.js";

function clickEvent(overrides = {}) {
  return {
    metaKey: false,
    ctrlKey: false,
    shiftKey: false,
    altKey: false,
    button: 0,
    defaultPrevented: false,
    preventDefault() {
      this.defaultPrevented = true;
    },
    ...overrides,
  };
}

function withBrowser(run) {
  const previousWindow = globalThis.window;
  const previousPopStateEvent = globalThis.PopStateEvent;
  const calls = [];
  globalThis.window = {
    history: { pushState: (...args) => calls.push(["pushState", ...args]) },
    dispatchEvent: (event) => calls.push(["dispatchEvent", event.type]),
    scrollTo: (options) => calls.push(["scrollTo", options]),
  };
  globalThis.PopStateEvent = class PopStateEvent {
    constructor(type) { this.type = type; }
  };
  try {
    run(calls);
  } finally {
    globalThis.window = previousWindow;
    globalThis.PopStateEvent = previousPopStateEvent;
  }
}

test("an internal link composes caller behavior before client-side navigation", () => {
  withBrowser((calls) => {
    const event = clickEvent({
      preventDefault() {
        calls.push(["preventDefault"]);
        this.defaultPrevented = true;
      },
    });

    handleLinkClick(event, "/workloads/audit", () => calls.push(["caller"]));

    assert.deepEqual(calls, [
      ["caller"],
      ["preventDefault"],
      ["pushState", null, "", "/workloads/audit"],
      ["dispatchEvent", "popstate"],
      ["scrollTo", { top: 0, behavior: "instant" }],
    ]);
  });
});

test("a caller can deliberately cancel internal navigation", () => {
  withBrowser((calls) => {
    const event = clickEvent();
    handleLinkClick(event, "/workloads/team", (click) => {
      calls.push(["caller"]);
      click.preventDefault();
    });

    assert.equal(event.defaultPrevented, true);
    assert.deepEqual(calls, [["caller"]]);
  });
});

test("browser-owned links and modified clicks still fall through", () => {
  const cases = [
    ["https://example.test", {}],
    ["//example.test/path", {}],
    ["/workloads#main-content", {}],
    ["/workloads", { metaKey: true }],
    ["/workloads", { ctrlKey: true }],
    ["/workloads", { shiftKey: true }],
    ["/workloads", { altKey: true }],
    ["/workloads", { button: 1 }],
  ];

  for (const [to, overrides] of cases) {
    withBrowser((calls) => {
      const event = clickEvent(overrides);
      handleLinkClick(event, to, () => calls.push(["caller"]));
      assert.equal(event.defaultPrevented, false, `${to} should remain browser-owned`);
      assert.deepEqual(calls, [["caller"]], `${to} unexpectedly navigated in the SPA`);
    });
  }
});
