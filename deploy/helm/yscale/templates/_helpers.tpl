{{/*
Expand the name of the chart.
*/}}
{{- define "yscale.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
Truncated to 63 chars to honor the DNS subdomain limit.
*/}}
{{- define "yscale.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{/*
Chart name and version label.
*/}}
{{- define "yscale.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels.
*/}}
{{- define "yscale.labels" -}}
helm.sh/chart: {{ include "yscale.chart" . }}
{{ include "yscale.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Resolve the controller image reference under the immutable-image contract.
A non-empty image.digest pins the image by content digest
(repository@sha256:<64 lowercase hex>) and takes precedence over image.tag.
A non-empty digest that is malformed always fails — even when
image.requireDigest is false — so a bad pin can never silently fall back to a
tag. When image.requireDigest is true, rendering fails unless a valid digest
is supplied (the production immutability switch); otherwise the existing
repository:tag reference is preserved for local/dev.
*/}}
{{- define "yscale.image" -}}
{{- $repo := .Values.image.repository -}}
{{- $digest := .Values.image.digest | default "" -}}
{{- if $digest -}}
{{- if not (regexMatch "^sha256:[0-9a-f]{64}$" $digest) -}}
{{- fail (printf "image.digest %q is malformed: expected sha256:<64 lowercase hex>" $digest) -}}
{{- end -}}
{{- printf "%s@%s" $repo $digest -}}
{{- else if .Values.image.requireDigest -}}
{{- fail "image.requireDigest is true but image.digest is empty: set image.digest to sha256:<64 lowercase hex> to satisfy the production immutability contract" -}}
{{- else -}}
{{- printf "%s:%s" $repo .Values.image.tag -}}
{{- end -}}
{{- end }}

{{/*
Selector labels.
*/}}
{{- define "yscale.selectorLabels" -}}
app.kubernetes.io/name: {{ include "yscale.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Service account name.
*/}}
{{- define "yscale.serviceAccountName" -}}
{{ include "yscale.fullname" . }}
{{- end }}

{{/*
Resolve the secret name to mount, preferring an existing one if provided.
*/}}
{{- define "yscale.secretName" -}}
{{- if .Values.secrets.existingSecret }}
{{- .Values.secrets.existingSecret }}
{{- else }}
{{- printf "%s-secrets" (include "yscale.fullname" .) }}
{{- end }}
{{- end }}

{{/*
ConfigMap name.
*/}}
{{- define "yscale.configMapName" -}}
{{- printf "%s-config" (include "yscale.fullname" .) }}
{{- end }}
