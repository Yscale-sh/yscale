#!/usr/bin/env bash
# Bootstraps unmanaged smoke-test Secrets from local inputs. Supplied, non-empty
# keys are merged; omitted keys and unrelated metadata/credentials are preserved.
# GitOps/controller-managed Secrets must be changed through their secret store.
# Requires python3 and kubectl. Values travel only over stdin, never argv or logs.
set +x
set -euo pipefail

NS="${NAMESPACE:-yscale}"
NAME="${SECRET_NAME:-yscale-cloud-secrets}"
ENV_FILE="${ENV_FILE:-.env}"
GCP_NAME="${GCP_SECRET_NAME:-yscale-gcp-sa}"
GCP_KEY_FILE="${GCP_KEY_FILE:-.gcp-sa.json}"

# Keep parsed credentials in memory and serialize them into the client pipe.
# Validate BOTH inputs and targets before making any write.
exec python3 - "$ENV_FILE" "$NS" "$NAME" "$GCP_NAME" "$GCP_KEY_FILE" <<'PY'
import base64
import json
import pathlib
import subprocess
import sys

env_file, namespace, cloud_name, gcp_name, gcp_file = sys.argv[1:]
allowed = {
    "FLYIO_TOKEN", "FLY_ORG", "FLY_REGION", "TS_OAUTH_CLIENT_ID",
    "TS_OAUTH_CLIENT_SECRET", "TS_TAILNET", "YSCALE_PLACEMENT_TOKEN_KEY",
}


def fail(message):
    print("error: " + message, file=sys.stderr)
    sys.exit(1)


def encode(value):
    return base64.b64encode(value).decode("ascii")


def kubectl(name, args, payload=None):
    try:
        result = subprocess.run(
            ["kubectl", "-n", namespace] + args,
            input=None if payload is None else json.dumps(payload).encode(),
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=False,
        )
    except OSError:
        fail("could not execute kubectl")
    if result.returncode:
        # Client/server errors can contain Secret data; never relay their output.
        fail(f"kubectl {args[0]} failed for Secret {namespace}/{name}; no Secret was deleted")
    return result.stdout


try:
    lines = pathlib.Path(env_file).read_text(encoding="utf-8").splitlines()
except (OSError, UnicodeError):
    fail("cannot read ENV_FILE")
data, seen = {}, set()
for number, line in enumerate(lines, 1):
    key, separator, value = line.partition("=")
    key = key.strip()
    if not separator or key not in allowed:
        continue
    if key in seen:
        fail(f"duplicate recognized key on ENV_FILE line {number}")
    seen.add(key)
    value = value.strip()
    if value.startswith(('"', "'")):
        if len(value) < 2 or value[-1] != value[0]:
            fail(f"unclosed quote on ENV_FILE line {number}")
        value = value[1:-1]
    if value:
        data[key] = encode(value.encode())
if not data:
    fail("ENV_FILE contains no non-empty recognized keys")

targets = [(cloud_name, data)]
gcp_path = pathlib.Path(gcp_file)
if gcp_path.exists():
    try:
        gcp = gcp_path.read_bytes()
        json.loads(gcp)
    except (OSError, ValueError, UnicodeError):
        fail("GCP_KEY_FILE cannot be read as valid JSON")
    if gcp_name == cloud_name:
        fail("SECRET_NAME and GCP_SECRET_NAME must be different")
    targets.append((gcp_name, {"key.json": encode(gcp)}))

plans = []
for name, incoming in targets:
    raw = kubectl(name, ["get", "secret", name, "--ignore-not-found", "-o", "json"])
    if not raw.strip():
        plans.append((name, "create", {
            "apiVersion": "v1", "kind": "Secret", "type": "Opaque",
            "metadata": {"name": name, "namespace": namespace}, "data": incoming,
        }))
        continue
    try:
        current = json.loads(raw)
    except (ValueError, UnicodeError):
        fail(f"could not decode Secret {namespace}/{name}")
    metadata = current.get("metadata") or {}
    labels, annotations = metadata.get("labels") or {}, metadata.get("annotations") or {}
    if (any(owner.get("controller") for owner in metadata.get("ownerReferences", []))
            or "kustomize.toolkit.fluxcd.io/name" in labels
            or labels.get("app.kubernetes.io/managed-by") == "Helm"
            or "meta.helm.sh/release-name" in annotations):
        fail(f"Secret {namespace}/{name} is controller-managed; update its source instead")
    if current.get("type", "Opaque") != "Opaque":
        fail(f"Secret {namespace}/{name} is not an Opaque bootstrap Secret")
    if all((current.get("data") or {}).get(key) == value for key, value in incoming.items()):
        plans.append((name, "unchanged", {}))
        continue
    if current.get("immutable"):
        fail(f"Secret {namespace}/{name} is immutable; no Secret was deleted")
    version = metadata.get("resourceVersion")
    if not version:
        fail(f"Secret {namespace}/{name} has no resourceVersion")
    plans.append((name, "patch", {"metadata": {"resourceVersion": version}, "data": incoming}))

for name, operation, payload in plans:
    if operation == "create":
        kubectl(name, ["create", "-f", "-", "-o", "name"], payload)
    elif operation == "patch":
        # Preserve unrelated keys; refuse concurrent changes using resourceVersion.
        kubectl(name, ["patch", "secret", name, "--type=merge", "--patch-file", "/dev/stdin", "-o", "name"], payload)
    print(f"{operation}: Secret {namespace}/{name}")
if len(targets) == 1:
    print(f"skipped: Secret {namespace}/{gcp_name} (GCP_KEY_FILE absent)")
PY
