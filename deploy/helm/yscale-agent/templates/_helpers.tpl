{{/*
Common labels applied to every resource the chart creates.
*/}}
{{- define "yscale-agent.labels" -}}
app.kubernetes.io/name: yscale-agent
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" }}
{{- end -}}

{{/*
Selector labels for the Pod / Service. Kept smaller than the full
label set so they don't drift on chart upgrades (selectors are
immutable on Service / Deployment).
*/}}
{{- define "yscale-agent.selectorLabels" -}}
app.kubernetes.io/name: yscale-agent
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{/*
Service account name — generated unless explicitly overridden.
*/}}
{{- define "yscale-agent.serviceAccountName" -}}
{{- if .Values.serviceAccount.name -}}
{{ .Values.serviceAccount.name }}
{{- else -}}
{{ printf "%s-yscale-agent" .Release.Name | trunc 63 | trimSuffix "-" }}
{{- end -}}
{{- end -}}

{{/*
Resolve the Cluster Connector image under the immutable-image contract.
A non-empty agent.image.digest pins the image by content digest
(repository@sha256:<64 lowercase hex>) and takes precedence over the tag.
A non-empty digest that is malformed always fails — even when
agent.image.requireDigest is false — so a bad pin can never silently fall
back to a tag. When agent.image.requireDigest is true, rendering fails unless
a valid digest is supplied (the production immutability switch); otherwise the
existing tag reference is used, falling back to .Chart.AppVersion when tag is
blank.
*/}}
{{- define "yscale-agent.agentImage" -}}
{{- $repo := .Values.agent.image.repository -}}
{{- $digest := .Values.agent.image.digest | default "" -}}
{{- if $digest -}}
{{- if not (regexMatch "^sha256:[0-9a-f]{64}$" $digest) -}}
{{- fail (printf "agent.image.digest %q is malformed: expected sha256:<64 lowercase hex>" $digest) -}}
{{- end -}}
{{- printf "%s@%s" $repo $digest -}}
{{- else if .Values.agent.image.requireDigest -}}
{{- fail "agent.image.requireDigest is true but agent.image.digest is empty: set agent.image.digest to sha256:<64 lowercase hex> to satisfy the production immutability contract" -}}
{{- else -}}
{{- $tag := .Values.agent.image.tag | default .Chart.AppVersion -}}
{{- printf "%s:%s" $repo $tag -}}
{{- end -}}
{{- end -}}


{{/*
Validate the rbac.scope and allowedNamespaces combo at install time.
Fails fast with a clear error rather than silently producing a chart
that doesn't grant the right permissions.
*/}}
{{- define "yscale-agent.validateRBAC" -}}
{{- if and (eq .Values.rbac.scope "namespaced") (not .Values.rbac.allowedNamespaces) -}}
{{ fail "rbac.scope=namespaced requires at least one entry in rbac.allowedNamespaces" }}
{{- end -}}
{{- if and (eq .Values.rbac.scope "namespaced") .Values.workloadNamespace (not (has .Values.workloadNamespace .Values.rbac.allowedNamespaces)) -}}
{{ fail (printf "workloadNamespace=%q must be listed in rbac.allowedNamespaces when rbac.scope=namespaced" .Values.workloadNamespace) }}
{{- end -}}
{{- end -}}

{{/*
Validate cloudProvider against the known-good list. Catches typos at
install time rather than running with surprising defaults.
*/}}
{{- define "yscale-agent.validateCloudProvider" -}}
{{- $valid := list "" "linode" "aws" "gcp" "azure" "k3s" "self-managed" -}}
{{- if not (has .Values.cloudProvider $valid) -}}
{{ fail (printf "cloudProvider=%q is not one of: %s" .Values.cloudProvider (join ", " $valid)) }}
{{- end -}}
{{- end -}}

{{/*
Resolve a stable clusterID. Prefers an explicitly-set .Values.clusterID
(treat as cluster-wide identity); falls back to a deterministic value
derived from the Helm release metadata so the tailscale sidecar's
TS_HOSTNAME never becomes a literal sentinel like REPLACE-AT-RUNTIME
(the Bug 8 footgun). Lowercased + DNS-1123-truncated so it survives as
both a tailscale hostname and a CLI flag value.
*/}}
{{- define "yscale-agent.clusterID" -}}
{{- if .Values.clusterID -}}
{{- .Values.clusterID -}}
{{- else -}}
{{- printf "%s-%s" .Release.Namespace .Release.Name | lower | trunc 40 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}

{{/*
Resolve cluster.dnsIP from cloudProvider when caller hasn't set one
explicitly. Defaults match each cloud's default kube-dns / coredns
ClusterIP — pod DNS resolution on bursts depends on getting this
right. Used by deployment.yaml to stamp CLUSTER_DNS on the agent's
environment so it can forward to central for burst provisioning.
*/}}
{{- define "yscale-agent.clusterDNS" -}}
{{- if .Values.cluster.dnsIP -}}
{{- .Values.cluster.dnsIP -}}
{{- else if eq .Values.cloudProvider "linode" -}}10.96.0.10
{{- else if eq .Values.cloudProvider "self-managed" -}}10.96.0.10
{{- else if eq .Values.cloudProvider "aws" -}}172.20.0.10
{{- else if eq .Values.cloudProvider "gcp" -}}10.0.0.10
{{- else if eq .Values.cloudProvider "azure" -}}10.0.0.10
{{- else if eq .Values.cloudProvider "k3s" -}}10.43.0.10
{{- else -}}10.43.0.10
{{- end -}}
{{- end -}}

{{/*
Resolve burst.providerIDOverride.format from cloudProvider when
caller hasn't set one explicitly. The literal token "{BURST_ID}" is
substituted by central / burst entrypoint at provision time — the
helm chart does not see real burst IDs. Empty string means "do not
set --provider-id on burst kubelet" (correct for k3s and other
CCM-less clusters).
*/}}
{{- define "yscale-agent.burstProviderIDFormat" -}}
{{- if .Values.burst.providerIDOverride.format -}}
{{- .Values.burst.providerIDOverride.format -}}
{{- else if eq .Values.cloudProvider "linode" -}}linode://yscale-burst-{BURST_ID}
{{- else if eq .Values.cloudProvider "aws" -}}aws://yscale-burst-{BURST_ID}
{{- else if eq .Values.cloudProvider "gcp" -}}
{{- else if eq .Values.cloudProvider "azure" -}}azure://yscale-burst-{BURST_ID}
{{- else if eq .Values.cloudProvider "k3s" -}}
{{- else if eq .Values.cloudProvider "self-managed" -}}
{{- else -}}linode://yscale-burst-{BURST_ID}
{{- end -}}
{{- end -}}

{{/*
Namespace the gateway Deployment's hostAliases lookup finds the
in-cluster `yscale-cloud` central Service in. Empty
gateway.centralServiceNamespace (the default) resolves to the release
namespace — the single-release install where connector and central share
one namespace. The split-namespace shape (release in yscale-system,
central Service in yscale) sets the value so the lookup still finds the
Service; the gateway pod runs dnsPolicy None, so without the /etc/hosts
pin that lookup feeds, its init container can never resolve central.
*/}}
{{- define "yscale-agent.gatewayCentralServiceNamespace" -}}
{{- .Values.gateway.centralServiceNamespace | default .Release.Namespace -}}
{{- end -}}

{{/*
Bind address for the connector's bootstrap-token + storage-signing
endpoint. The agent is a Pod, so a wildcard bind publishes burst-token
minting and bucket-URL signing on the customer's pod network, where the
tailnet ACL cannot reach — the chart therefore never emits one unless
the operator asks for it by name via listenMode=custom.

  loopback  127.0.0.1:<port>. Correct for this chart: its tailscale
            sidecar runs userspace (TS_USERSPACE=true), holds no
            interface address, and `tailscale serve` forwards inbound
            tailnet TCP to loopback inside the pod.
  tailnet   `tailnet:<port>`; the agent resolves tailscaled's own
            100.64.0.0/10 address and binds only that. Needs a
            KERNEL-mode tailscaled in this pod's network namespace —
            the agent fails closed when no such address exists.
  custom    bootstrap.listenAddress, verbatim.
*/}}
{{- define "yscale-agent.bootstrapListen" -}}
{{- $mode := default "loopback" .Values.bootstrap.listenMode -}}
{{- if eq $mode "loopback" -}}
{{- printf "127.0.0.1:%v" .Values.bootstrap.port -}}
{{- else if eq $mode "tailnet" -}}
{{- printf "tailnet:%v" .Values.bootstrap.port -}}
{{- else if eq $mode "custom" -}}
{{- if not .Values.bootstrap.listenAddress -}}
{{ fail "bootstrap.listenMode=custom requires bootstrap.listenAddress (host:port)" }}
{{- end -}}
{{- .Values.bootstrap.listenAddress -}}
{{- else -}}
{{ fail (printf "bootstrap.listenMode=%q is not one of: loopback, tailnet, custom" $mode) }}
{{- end -}}
{{- end -}}

{{/*
In-pod address `tailscale serve` forwards inbound tailnet TCP to. Derived
from the SAME helper that renders -bootstrap-listen, so the forward target
and the bind address cannot drift apart on a port change.

Empty in tailnet mode: there tailscaled owns a real interface address and the
agent binds it directly, so a serve forward to loopback would point at nothing.
*/}}
{{- define "yscale-agent.bootstrapServeForward" -}}
{{- $mode := default "loopback" .Values.bootstrap.listenMode -}}
{{- if ne $mode "tailnet" -}}
{{- $addr := include "yscale-agent.bootstrapListen" . -}}
{{- if or (hasPrefix "0.0.0.0:" $addr) (hasPrefix "[::]:" $addr) -}}
{{- printf "127.0.0.1:%s" (last (splitList ":" $addr)) -}}
{{- else -}}
{{- $addr -}}
{{- end -}}
{{- end -}}
{{- end -}}
