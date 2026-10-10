# Website and console source

This directory contains the website and console implementation previously kept
in a separate repository. It is included directly in the full Yscale source
release; no private submodule access is required.

The landing page and consumer docs present **Yscale — a flexible Kubernetes
burst tool** as self-hosted and source-available. The complete console, account
and billing implementation is included too; it is not an offer of managed
hosting or a production signup or payment flow.

The root [Yscale Community License 1.0](../LICENSE) is effective: personal
and internal business self-hosting are free at any company size. Resale or
hosting Yscale for third parties requires paid written permission. There is
no Yscale-run hosted service or pricing. Third-party components retain their
own licenses.

```sh
npm ci
npm test
npm run build
```

Use Node.js 20.19+ on the 20.x line or Node.js 22.12+ with the pinned Vite 8
toolchain. Keep development servers private. Run `npm audit` alongside tests
when preparing a release; a passing static build is not a security review.

The Go API proxy under `server/` has its own module and tests:

```sh
cd server
go test ./...
```

Configure your own API, identity, mail, storage and payment services before
running a complete console. Private deployment bindings are not shipped. No
website or hosted service is deployed by this source release.
