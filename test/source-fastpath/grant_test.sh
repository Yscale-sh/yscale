#!/usr/bin/env bash
#
# Tests for the source-fastpath agent reconcile scripts (scripts/source-grant.sh,
# source-revoke.sh). Uses `--dry-run` to assert the rendered Caddyfile + manifests
# WITHOUT a cluster, so it runs anywhere (CI). Run: bash test/source-fastpath/grant_test.sh
set -uo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
GRANT="$ROOT/scripts/source-grant.sh"
REVOKE="$ROOT/scripts/source-revoke.sh"
PASS=0 FAIL=0
ok()   { PASS=$((PASS+1)); printf '  ok   %s\n' "$1"; }
bad()  { FAIL=$((FAIL+1)); printf '  FAIL %s\n' "$1"; }
have() { [[ "$1" == *"$2"* ]]; }

# A canonical grant rendered once, reused by most assertions.
OUT="$(bash "$GRANT" --dry-run \
  --ns srcfp --name gw --burst-ip 203.0.113.9 \
  --nfs-server 10.0.0.236 --nfs-path "/vol/.data" \
  --mount movies=Lib/Movies --mount tv=Lib/TV \
  --node optiplex --lb-ip 10.0.0.220 --lb-pool pool-high --token tok_FIXED 2>&1)"

echo "== Caddyfile security model =="
have "$OUT" 'respond "403 denied" 403'        && ok "deny-by-default catch-all 403"            || bad "missing deny-by-default 403"
have "$OUT" 'remote_ip 203.0.113.9/32'        && ok "IP-allowlist scoped to burst /32"         || bad "missing/incorrect remote_ip allowlist"
have "$OUT" 'Authorization "Bearer tok_FIXED"' && ok "bearer-token required (honors --token)"   || bad "missing bearer-token matcher"
have "$OUT" 'method GET HEAD'                  && ok "read-only (GET/HEAD only)"                || bad "method restriction missing"
have "$OUT" 'root * /srv'                      && ok "serves jailed root /srv"                  || bad "root directive missing"
have "$OUT" 'admin off'                        && ok "caddy admin API disabled"                || bad "admin not disabled"

echo "== jail: only the named mounts, read-only =="
have "$OUT" '/srv/movies, subPath: "Lib/Movies", readOnly: true' && ok "movies mount RO subPath" || bad "movies mount wrong"
have "$OUT" '/srv/tv, subPath: "Lib/TV", readOnly: true'         && ok "tv mount RO subPath"     || bad "tv mount wrong"
have "$OUT" 'nfs: {server: "10.0.0.236", path: "/vol/.data", readOnly: true}' && ok "source NFS volume RO" || bad "nfs volume not RO/wrong"
# nothing outside the named subpaths is mounted
[ "$(grep -c 'subPath:' <<< "$OUT")" = 2 ] && ok "exactly 2 jailed subPaths (no extra exposure)" || bad "unexpected number of subPath mounts"

echo "== LoadBalancer exposure (source-IP preserving) =="
have "$OUT" 'type: LoadBalancer'                 && ok "exposed via Service type=LoadBalancer"  || bad "not a LoadBalancer"
have "$OUT" 'externalTrafficPolicy: Local'       && ok "externalTrafficPolicy: Local (preserve src IP)" || bad "missing Local policy (allowlist would see SNAT)"
have "$OUT" 'loadBalancerIPs: "10.0.0.220"'      && ok "requests the given LB IP (MetalLB)"     || bad "LB IP annotation missing"
have "$OUT" 'kubernetes.io/hostname: optiplex'   && ok "honors --node placement"                || bad "nodeSelector missing"
have "$OUT" 'ENDPOINT=https://10.0.0.220:8443'   && ok "emits ENDPOINT"                          || bad "no ENDPOINT emitted"
have "$OUT" 'TOKEN=tok_FIXED'                     && ok "emits TOKEN"                             || bad "no TOKEN emitted"

echo "== token auto-generation =="
A_OUT="$(bash "$GRANT" --dry-run --ns n --name g --burst-ip 1.2.3.4 --pvc data --mount d=. 2>&1)"
A="$(grep '^TOKEN=' <<< "$A_OUT")"
grep -qE '^TOKEN=brst_[0-9a-f]{32}$' <<< "$A" && ok "auto-generates a brst_ token when --token omitted" || bad "token auto-gen wrong: $A"
have "$A" 'tok_FIXED' && bad "leaked fixed token" || ok "fresh token per grant"

echo "== PVC source path =="
P="$(bash "$GRANT" --dry-run --ns n --name g --burst-ip 1.2.3.4 --pvc mydata --mount d=sub 2>&1)"
have "$P" 'persistentVolumeClaim: {claimName: mydata, readOnly: true}' && ok "supports --pvc source (RO)" || bad "pvc source wrong"

echo "== required-arg validation (must fail, exit!=0) =="
bash "$GRANT" --dry-run --name g --burst-ip 1.2.3.4 --pvc d --mount d=. >/dev/null 2>&1 && bad "accepted missing --ns" || ok "rejects missing --ns"
bash "$GRANT" --dry-run --ns n --name g --pvc d --mount d=. >/dev/null 2>&1            && bad "accepted missing --burst-ip" || ok "rejects missing --burst-ip"
bash "$GRANT" --dry-run --ns n --name g --burst-ip 1.2.3.4 --pvc d >/dev/null 2>&1     && bad "accepted no --mount" || ok "rejects missing --mount"
bash "$GRANT" --dry-run --ns n --name g --burst-ip 1.2.3.4 --mount d=. >/dev/null 2>&1 && bad "accepted no source" || ok "rejects missing source (nfs/pvc)"
bash "$GRANT" --dry-run --ns n --name g --burst-ip 1.2.3.4 --pvc d --mount d=. --bogus >/dev/null 2>&1 && bad "accepted unknown arg" || ok "rejects unknown arg"

echo "== revoke arg validation =="
bash "$REVOKE" --name g >/dev/null 2>&1 && bad "revoke accepted missing --ns" || ok "revoke rejects missing --ns"
bash "$REVOKE" --ns n >/dev/null 2>&1   && bad "revoke accepted missing --name" || ok "revoke rejects missing --name"

echo "== shellcheck (if available) =="
if command -v shellcheck >/dev/null 2>&1; then
  shellcheck -S warning "$GRANT" "$REVOKE" "${BASH_SOURCE[0]}" >/dev/null 2>&1 && ok "shellcheck clean (warning+)" || bad "shellcheck findings (run: shellcheck $GRANT $REVOKE)"
else
  echo "  skip shellcheck (not installed)"
fi

echo
echo "RESULT: $PASS passed, $FAIL failed"
[ "$FAIL" = 0 ]
