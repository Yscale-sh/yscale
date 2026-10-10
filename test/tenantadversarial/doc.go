// Package tenantadversarial holds the deterministic retained-evidence slice
// of the two-tenant control-plane denial suite.
//
// It orchestrates the authoritative negative tests that already live next to
// each seam — it never duplicates their logic or opens a parallel auth seam —
// by shelling out to `go test -json` with anchored test names, and refuses to
// pass unless every named test both exists and passes. A source-contract
// posture check pins central telemetry (metrics/logs) as operator-only. The
// suite finally rebuilds the checked-in evidence artifact in memory and
// byte-diffs it against testdata/two-tenant-denial.json so any drift
// fails the ordinary `go test ./...` gate.
package tenantadversarial
