# Supported configurations — source-preview-unqualified

<!-- GENERATED FILE: do not edit by hand. Edit the manifest and regenerate. -->

Generated from `docs/release/manifest.json` for release candidate commit
`0000000000000000000000000000000000000000`
(tested head `0000000000000000000000000000000000000000`,
tree `0000000000000000000000000000000000000000`).

This is a machine-generated PRE-RELEASE evidence record: it restates the
manifest's recorded gate and cell status and makes no claim beyond what that
manifest proves. It is not a deployment, general-availability, or
customer-readiness statement.

Qualified-this-release configurations recorded in the manifest: none.

## Exact missing launch proof

The ONE command that lists exactly what proof is still missing for launch:

```
go run ./cmd/yscale-launch-readiness readiness -manifest docs/release/manifest.json
```

## Qualified this release

None. The manifest records zero qualified-this-release configurations.

## Historically tested (NOT qualified for this release)

None recorded.

## Unsupported

| Provider | Target | Compute | Networking | Lifecycle | Region | Instance | Status | Evidence | Notes |
|---|---|---|---|---|---|---|---|---|---|
| aws-ec2 | eks | cpu | full | — | — | — | unsupported | — | Not qualified for this source snapshot. Source inclusion is not operational qualification. |
| aws-ec2 | eks | gpu | full | — | — | — | unsupported | — | Not qualified for this source snapshot. Source inclusion is not operational qualification. |
| aws-ec2 | gke | cpu | full | — | — | — | unsupported | — | Not qualified for this source snapshot. Source inclusion is not operational qualification. |
| aws-ec2 | k3s | cpu | full | — | — | — | unsupported | — | Not qualified for this source snapshot. Source inclusion is not operational qualification. |
| aws-ec2 | k3s | gpu | full | — | — | — | unsupported | — | Not qualified for this source snapshot. Source inclusion is not operational qualification. |
| azure | k3s | cpu | full | — | — | — | unsupported | — | Not qualified for this source snapshot. Source inclusion is not operational qualification. |
| azure | k3s | gpu | full | — | — | — | unsupported | — | Not qualified for this source snapshot. Source inclusion is not operational qualification. |
| flyio | k3s | cpu | full | — | — | — | unsupported | — | Not qualified for this source snapshot. Source inclusion is not operational qualification. |
| flyio | k3s | gpu | full | — | — | — | unsupported | — | Not qualified for this source snapshot. Source inclusion is not operational qualification. |
| gcp | gke | cpu | full | — | — | — | unsupported | — | Not qualified for this source snapshot. Source inclusion is not operational qualification. |
| gcp | gke | gpu | full | — | — | — | unsupported | — | Not qualified for this source snapshot. Source inclusion is not operational qualification. |
| gcp | k3s | cpu | full | — | — | — | unsupported | — | Not qualified for this source snapshot. Source inclusion is not operational qualification. |
| linode | eks | cpu | full | — | — | — | unsupported | — | Not qualified for this source snapshot. Source inclusion is not operational qualification. |
| linode | k3s | gpu | full | — | — | — | unsupported | — | Not qualified for this source snapshot. Source inclusion is not operational qualification. |
| linode | lke | cpu | full | — | — | — | unsupported | — | Not qualified for this source snapshot. Source inclusion is not operational qualification. |
| linode | lke | gpu | full | — | — | — | unsupported | — | Not qualified for this source snapshot. Source inclusion is not operational qualification. |

## Notes

- All-zero Git IDs are unset placeholders, not a published commit or a verified build.
- No private deployment identities, historic receipts or operator notes are carried into this manifest.
- Azure and GCP GPU configurations are outside launch scope.
- Operational qualification and recovery: docs/operations.md. Consumer setup: docs/self-hosted-mesh.md.
- The operational readiness validator retains its complete commercial-service checks. Source publication does not require operating a Yscale-hosted service.

Regenerate: `go run ./cmd/yscale-launch-readiness matrix -manifest docs/release/manifest.json`
Verify a checked-in copy: `go run ./cmd/yscale-launch-readiness matrix -manifest docs/release/manifest.json -verify -doc <path-to-this-file>`
