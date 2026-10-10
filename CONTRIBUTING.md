# Contributing to yscale

Thanks for considering a contribution. This page keeps it short and honest.

## Ground rules

- **Bugs / questions:** open an issue with the failing command, the log lines,
  and your environment (cluster flavor, cloud backend, networking tier — full is the only supported tier).
- **Small fixes** (docs, error messages, obvious bugs): just send the PR.
- **Anything larger:** open an issue first so we can agree on the seam before
  you invest time. The codebase has deliberate boundaries — `pkg/` is the only
  cross-service surface, `agent/internal` and `central/internal` do not import
  each other — and PRs that respect them merge much faster.

## Dev loop

Install `curl` and `jq` alongside Go. The cache-init regression tests execute the
generated shell command against local HTTP(S) servers using those real tools;
they do not contact cloud services or install packages on the test host.

```sh
go build ./...        # everything compiles
go vet ./...          # vet is CI-enforced
gofmt -l .            # must print nothing (CI-enforced)
go test ./...         # the unit suite; no cluster or cloud account needed
```

CI includes these checks on every PR. For the default self-hosted deployment,
see `docs/self-hosted-mesh.md`. Live cloud tests require separately approved
provider scope and cost; the historical Tailscale-only export is not the default.

## Licensing of contributions

The full-source release uses the **source-available** Yscale Community
License: free personal and internal organization use regardless of revenue,
with a separate paid license required for resale or third-party hosting.
See `LICENSE` and `COMMERCIAL.md`.

The contribution terms are:

1. You have the right to submit it (it's your work, or you're authorized).
2. Your contribution is licensed to the project under the repository's
   license, and you grant the maintainers the right to also distribute it
   under the project's commercial license (the dual-licensing that funds the
   project).

A formal CLA flow may replace this note.
