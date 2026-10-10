// RELEASE 001 — kubagachi, the first ecosystem drop.
//
// HOW TO WRITE A RELEASE: each drop is a module exporting one object.
// `sections` is an ordered list of { heading, body } — body is plain JSX.
// Add sections as the article grows; the page renders whatever is here.
// Register new releases in src/drops/index.js (newest first).

export const release001 = {
  n: "001",
  slug: "001",
  date: "2026-07",
  title: "KUBAGACHI — YOUR CLUSTER, ALIVE",
  dek: "kubagachi open-sources a Kubernetes cockpit where every pod is a tamagotchi critter: k9s meets Freelens, with Flux first-class and Nori inside.",
  kritter: {
    name: "NORI",
    tag: "the habitat cat",
    // The kubagachi mascot: Nori, the gray-and-white fleet cat. Served in full
    // color from the site's server at /kritters/nori.png.
    img: "/kritters/nori.png",
    sheet: "/kritters/nori-sheet.png",
    frames: 8,
    // The card links out to the kubagachi project.
    href: "https://yscale.sh/kubagachi",
    hrefLabel: "Open kubagachi ↗",
    // ASCII fallback if the mascot art can't load.
    ascii: [
      " /\\_/\\",
      "( o.o )",
      " > ^ <",
      "running",
    ],
  },
  sections: [
    {
      heading: "What this is",
      body: (
        <>
          <p>
            kubagachi is a Kubernetes cockpit where your cluster is rendered
            as a living habitat: every pod is a pixel-art critter, and a
            pod's health <em>is</em> its critter's mood. A content cat for
            Running. A sleepy critter for BackOff. A tombstone for
            CrashLoopBackOff. A fading ghost for Terminating. Restarts make
            them sick — and you care for them with real operations: logs,
            shells, deletes, Flux reconciles.
          </p>
          <p>
            Two faces, one binary. A k9s-style terminal UI with a{" "}
            <code>:</code> command palette, and a browser cockpit — the same
            live cluster as a clickable dashboard with the full keyboard
            layer, resource drawers, a navigable resource tree, and an
            embedded terminal running real <code>kubectl exec</code> over a
            websocket.
          </p>
        </>
      ),
    },
    {
      heading: "Flux, first-class",
      body: (
        <>
          <p>
            GitOps state isn't an afterthought: <code>:flux</code> opens
            Kustomizations, HelmReleases and sources with readiness, source
            chain and revision — with one-key reconcile and suspend, and a
            toggle between table and dependency graph. If you run your
            cluster the git-is-the-only-writer way, this view is home.
          </p>
          <p>
            There's also a ranch view — press <code>v</code> and each node
            becomes a grassy platform with its pods scattered across it like
            a calm little Pokémon ranch. It is completely unnecessary and it
            sparks more joy than any dashboard I've ever shipped.
          </p>
        </>
      ),
    },
    {
      heading: "Where yscale fits",
      body: (
        <>
          <p>
            kubagachi is also a viewer for yscale: an optional burst-fleet
            tab shows your live bursts and spend, proxied server-side so the
            token never reaches the browser. Watch a burst node ignite into
            the habitat, run its pod, and get reaped — same lifecycle as the
            demo on the <a href="/">home page</a>, but with your real
            cluster and a critter's face on it.
          </p>
        </>
      ),
    },
  ],
};
