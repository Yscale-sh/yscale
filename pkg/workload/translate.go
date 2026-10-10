package workload

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"math"
	"path"
	"strings"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/yscale-sh/yscale/pkg/backends"
	"github.com/yscale-sh/yscale/pkg/cache"
)

// Annotation keys used to ride workload-level metadata through to the
// Job/pod so the controller and (future) SaaS layer can read it without
// re-parsing the YAML.
const (
	AnnotationGPUKind   = "yscale.sh/gpu-kind"
	AnnotationGPUCount  = "yscale.sh/gpu-count"
	AnnotationBudgetUSD = "yscale.sh/budget-usd"
	AnnotationDeadline  = "yscale.sh/deadline"
	AnnotationStorageR2 = "yscale.sh/storage-r2"
	AnnotationArtifacts = "yscale.sh/artifacts"
	AnnotationOwner     = "yscale.sh/owner"
	AnnotationProject   = "yscale.sh/project"
	AnnotationModelVol  = "yscale.sh/model-volume"
)

const (
	// modelCacheHostPath is where the burst's bootstrap mounts the
	// pre-seeded model volume (see linode bootstrap-baked.sh). It is
	// bind-mounted into the workload pod via hostPath at modelCachePodPath,
	// with HF_HOME pointed at its hf/ subdir so HuggingFace loads from the
	// pre-downloaded cache instead of fetching over the network.
	modelCacheHostPath = "/mnt/model-cache"
	modelCachePodPath  = "/models"

	// AnnotationCacheBurstID and AnnotationCacheBootstrapEndpoint are
	// populated by the planner after it allocates the burst. Cache init
	// containers read them through the downward API to request short-lived
	// object URLs from the existing agent-side storage signer.
	AnnotationCacheBurstID           = "yscale.sh/cache-burst-id"
	AnnotationCacheBootstrapEndpoint = "yscale.sh/cache-bootstrap-endpoint"
)

const (
	cacheInitImage       = "alpine:3.23@sha256:fd791d74b68913cbb027c6546007b3f0d3bc45125f797758156952bc2d6daf40"
	artifactPrepareImage = "busybox:1.36.1@sha256:73aaf090f3d85aa34ee199857f03fa3a95c8ede2ffd4cc2cdb5b94e566b11662"
	cacheInitCommand     = `set -eu
umask 022
fail() { printf 'cache init failed: %s\n' "$1" >&2; exit 1; }
apk add --no-cache curl jq >/dev/null 2>&1 || fail 'install download tools'
cache_tmp="$(mktemp -d)"
cache_partial=''
cleanup() {
  if [ -n "$cache_partial" ]; then rm -f -- "$cache_partial"; fi
  rm -f "$cache_tmp/request.json" "$cache_tmp/urls.json" "$cache_tmp/objects.jsonl"
  rmdir "$cache_tmp"
}
trap cleanup EXIT
trap 'exit 1' HUP INT TERM
mkdir -p "$CACHE_TARGET"
jq -cn --arg burst_id "$BURST_ID" --argjson source "$CACHE_SOURCE" \
  '{burst_id: $burst_id, mode: "read", source: $source}' > "$cache_tmp/request.json" 2>/dev/null || fail 'invalid cache source'
status="$(curl --disable --globoff --proto '=http,https' --fail --silent --retry 3 \
  --connect-timeout 10 --max-time 60 -H 'Content-Type: application/json' \
  --data-binary "@$cache_tmp/request.json" --output "$cache_tmp/urls.json" \
  --write-out '%{http_code}' --url "$BOOTSTRAP_ENDPOINT/storage/sign-urls" 2>/dev/null)" || fail 'storage signing request'
[ "$status" = 200 ] || fail 'storage signing response'
# Validate the entire response before touching cache files. A jq | while pipeline
# hides jq failure in POSIX sh, allowing the main workload to start without data.
jq -se '
  def valid_key:
    type == "string" and length > 0 and utf8bytelength <= 1024 and
    (explode | all(. >= 32 and . != 127 and . != 92)) and
    (split("/") | all(. != "" and . != "." and . != ".."));
  length == 1 and (.[0] |
    type == "object" and (.urls | type == "array") and
    all(.urls[]; type == "object" and (.key | valid_key) and
      (.url | type == "string" and test("^https?://[^/[:space:]]+") and
        (explode | all(. > 32 and . != 127)))) and
    ([.urls[].key] | length == (unique | length)))
' "$cache_tmp/urls.json" >/dev/null 2>&1 || fail 'invalid signer response'
jq -c '.urls[]' "$cache_tmp/urls.json" > "$cache_tmp/objects.jsonl" 2>/dev/null || fail 'invalid signer response'
while IFS= read -r object; do
  url="$(printf '%s' "$object" | jq -r '.url')"
  key="$(printf '%s' "$object" | jq -r '.key')"
  destination="$CACHE_TARGET/$key"
  [ ! -d "$destination" ] || fail 'cache path conflict'
  mkdir -p "$(dirname "$destination")"
  cache_partial="$(mktemp "$(dirname "$destination")/.yscale-cache.XXXXXX")"
  # Do not follow redirects, expand URL globs, or log bearer URLs/upstream bodies.
  # Only a complete 200 response replaces the destination, on the same filesystem.
  status="$(curl --disable --globoff --proto '=http,https' --fail --silent --retry 3 \
    --connect-timeout 10 --speed-limit 1 --speed-time 60 \
    --output "$cache_partial" --write-out '%{http_code}' --url "$url" 2>/dev/null)" || fail 'object download'
  [ "$status" = 200 ] || fail 'object download response'
  chmod 644 "$cache_partial"
  mv -f -- "$cache_partial" "$destination"
  cache_partial=''
done < "$cache_tmp/objects.jsonl"`
)

// ToJob translates a Workload into a Kubernetes Job manifest the
// controller will see as Pending and provision a burst node for.
//
// The pod template is configured to:
//   - tolerate the burst-node taint (so the scheduler can place it on
//     a yscale-provisioned node)
//   - request the burst-node label via nodeSelector (so the scheduler
//     *only* places it on yscale nodes — no accidents on cluster nodes)
//   - request CPU/memory either from Size preset or explicit values
//   - request nvidia.com/gpu when GPU is set
func ToJob(w *Workload) (*batchv1.Job, error) {
	if err := Validate(w); err != nil {
		return nil, err
	}

	resources, err := buildResources(&w.Spec)
	if err != nil {
		return nil, err
	}

	annotations := map[string]string{}
	if w.Spec.GPU != nil {
		gpuCount := max(1, w.Spec.GPU.Count)
		annotations[AnnotationGPUKind] = w.Spec.GPU.Kind
		annotations[AnnotationGPUCount] = fmt.Sprintf("%d", gpuCount)
	}
	if w.Spec.Budget != nil {
		if w.Spec.Budget.MaxUSD > 0 {
			annotations[AnnotationBudgetUSD] = fmt.Sprintf("%.4f", w.Spec.Budget.MaxUSD)
		}
		if w.Spec.Budget.Deadline > 0 {
			annotations[AnnotationDeadline] = w.Spec.Budget.Deadline.String()
		}
	}
	if w.Spec.Storage != nil && w.Spec.Storage.R2 != nil {
		// Stored as JSON for round-tripping; consumers parse on demand.
		if encoded, err := json.Marshal(w.Spec.Storage.R2); err == nil {
			annotations[AnnotationStorageR2] = string(encoded)
		}
	}
	if w.Spec.Storage != nil && len(w.Spec.Storage.Artifacts) > 0 {
		encoded, err := json.Marshal(w.Spec.Storage.Artifacts)
		if err != nil {
			return nil, fmt.Errorf("marshal artifact outputs: %w", err)
		}
		annotations[AnnotationArtifacts] = string(encoded)
	}
	if owner, ok := w.Metadata.Tags["owner"]; ok {
		annotations[AnnotationOwner] = owner
	}
	if project, ok := w.Metadata.Tags["project"]; ok {
		annotations[AnnotationProject] = project
	}

	labels := map[string]string{
		"app.kubernetes.io/managed-by": "yscale",
		"yscale.sh/workload":           w.Metadata.Name,
	}
	for k, v := range w.Metadata.Labels {
		labels[k] = v
	}

	container := corev1.Container{
		Name:      "workload",
		Image:     w.Spec.Image,
		Command:   w.Spec.Command,
		Args:      w.Spec.Args,
		Env:       toCoreEnv(w.Spec.Env),
		Resources: resources,
		// Defense-in-depth against a workload escaping to the burst host (which
		// holds the mesh key + kubelet cert). Conservative subset that won't
		// break arbitrary batch/compute images: block setuid privilege
		// escalation and apply the runtime's default seccomp profile.
		// Deliberately NOT runAsNonRoot / readOnlyRootFilesystem / drop-all-caps
		// — those break common ML/compute images that run as root or need caps.
		SecurityContext: &corev1.SecurityContext{
			AllowPrivilegeEscalation: boolPtr(false),
			SeccompProfile:           &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault},
		},
	}

	// When the workload requests a pre-seeded model volume, the burst's
	// bootstrap mounts it on the host at modelCacheHostPath. Bind it into
	// the pod via hostPath (read-only) and point HF_HOME at its hf/ subdir
	// so HuggingFace loads weights from the pre-downloaded cache instead of
	// fetching them — turning a multi-GB download into a mount. The volume
	// attach itself is the backend's job (see linode CreateNode); this only
	// makes the already-mounted host path visible to the pod.
	var podVolumes []corev1.Volume
	if w.Spec.ModelVolume != "" {
		annotations[AnnotationModelVol] = w.Spec.ModelVolume
		hostPathDir := corev1.HostPathDirectory
		podVolumes = append(podVolumes, corev1.Volume{
			Name: "model-cache",
			VolumeSource: corev1.VolumeSource{
				HostPath: &corev1.HostPathVolumeSource{
					Path: modelCacheHostPath,
					Type: &hostPathDir,
				},
			},
		})
		container.VolumeMounts = append(container.VolumeMounts, corev1.VolumeMount{
			Name:      "model-cache",
			MountPath: modelCachePodPath,
			ReadOnly:  true,
		})
		// Appended after the user's env so these win on any key collision.
		container.Env = append(container.Env,
			corev1.EnvVar{Name: "HF_HOME", Value: modelCachePodPath + "/hf"},
			corev1.EnvVar{Name: "HF_HUB_OFFLINE", Value: "1"},
		)
	}

	var initContainers []corev1.Container
	if w.Spec.Storage != nil {
		for i, c := range w.Spec.Storage.Cache {
			volumeName := fmt.Sprintf("cache-%d", i)
			source, err := json.Marshal(map[string]string{
				"bucket":             c.Source.Bucket,
				"prefix":             c.Source.Prefix,
				"endpoint":           c.Source.Endpoint,
				"region":             c.Source.Region,
				"credentials_secret": c.Source.CredentialsSecret,
			})
			if err != nil {
				return nil, fmt.Errorf("marshal cache source %q: %w", c.Name, err)
			}

			podVolumes = append(podVolumes, corev1.Volume{
				Name: volumeName,
				VolumeSource: corev1.VolumeSource{
					EmptyDir: &corev1.EmptyDirVolumeSource{
						SizeLimit: sizeLimitFromGB(c.SizeHintGB),
					},
				},
			})
			mainMount := corev1.VolumeMount{Name: volumeName, MountPath: c.Target, ReadOnly: true}
			container.VolumeMounts = append(container.VolumeMounts, mainMount)
			initContainers = append(initContainers, corev1.Container{
				Name:    fmt.Sprintf("cache-sync-%d", i),
				Image:   cacheInitImage,
				Command: []string{"/bin/sh", "-c", cacheInitCommand},
				Env: []corev1.EnvVar{
					{Name: "CACHE_TARGET", Value: c.Target},
					{Name: "CACHE_SOURCE", Value: string(source)},
					{Name: "BURST_ID", ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.annotations['" + AnnotationCacheBurstID + "']"}}},
					{Name: "BOOTSTRAP_ENDPOINT", ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.annotations['" + AnnotationCacheBootstrapEndpoint + "']"}}},
				},
				VolumeMounts: []corev1.VolumeMount{{Name: volumeName, MountPath: c.Target}},
			})
		}
		if len(w.Spec.Storage.Artifacts) > 0 {
			prepare := corev1.Container{
				Name:    "artifact-prepare",
				Image:   artifactPrepareImage,
				Command: []string{"/bin/sh", "-c"},
				Args: []string{`set -eu
for dir in "$@"; do
  rm -rf "$dir"/* "$dir"/.[!.]* "$dir"/..?*
  chmod 0777 "$dir"
done`, "artifact-prepare"},
				SecurityContext: &corev1.SecurityContext{
					AllowPrivilegeEscalation: boolPtr(false),
					ReadOnlyRootFilesystem:   boolPtr(true),
					RunAsUser:                int64Ptr(0),
					SeccompProfile:           &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault},
					Capabilities:             &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}},
				},
			}
			for i, artifact := range w.Spec.Storage.Artifacts {
				volumeName := fmt.Sprintf("artifact-%d", i)
				preparePath := fmt.Sprintf("/yscale-artifacts/%d", i)
				hostPathType := corev1.HostPathDirectoryOrCreate
				podVolumes = append(podVolumes, corev1.Volume{
					Name: volumeName,
					VolumeSource: corev1.VolumeSource{HostPath: &corev1.HostPathVolumeSource{
						Path: artifactHostPath(w, artifact.Name),
						Type: &hostPathType,
					}},
				})
				container.VolumeMounts = append(container.VolumeMounts, corev1.VolumeMount{Name: volumeName, MountPath: artifact.Target})
				prepare.VolumeMounts = append(prepare.VolumeMounts, corev1.VolumeMount{Name: volumeName, MountPath: preparePath})
				prepare.Args = append(prepare.Args, preparePath)
			}
			initContainers = append(initContainers, prepare)
		}
	}

	job := &batchv1.Job{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "batch/v1",
			Kind:       "Job",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:        w.Metadata.Name,
			Namespace:   namespaceOrDefault(w.Metadata.Namespace),
			Labels:      labels,
			Annotations: annotations,
		},
		Spec: batchv1.JobSpec{
			BackoffLimit: int32Ptr(w.Spec.Retries),
			// k8s garbage-collects the finished Job and its pods 10 min
			// after completion, so completed Jobs don't pile up in the
			// customer's cluster.
			TTLSecondsAfterFinished: int32Ptr(600),
			Parallelism:             jobReplicas(w),
			Completions:             jobReplicas(w),
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels:      labels,
					Annotations: annotations,
				},
				Spec: corev1.PodSpec{
					InitContainers: initContainers,
					Containers:     []corev1.Container{container},
					RestartPolicy:  corev1.RestartPolicyOnFailure,
					Volumes:        podVolumes,
					NodeSelector: map[string]string{
						"yscale.sh/burst-node": "true",
					},
					Tolerations: burstTolerations(w),
				},
			},
		},
	}

	return job, nil
}

// Validate returns an error if the Workload can't be translated. Run
// before any consumer of the spec to fail fast on user errors.
func Validate(w *Workload) error {
	if w == nil {
		return fmt.Errorf("workload is nil")
	}
	if err := validateEnvelope(w); err != nil {
		return err
	}
	if err := validateResources(&w.Spec); err != nil {
		return err
	}
	if err := validateBackendCompat(&w.Spec); err != nil {
		return err
	}
	if err := validateNetworking(w.Spec.Networking); err != nil {
		return err
	}
	if err := validateGPU(w.Spec.GPU); err != nil {
		return err
	}
	if err := ValidateEnv(w.Spec.Env); err != nil {
		return err
	}
	if w.Spec.Retries < 0 {
		return fmt.Errorf("spec.retries must be >= 0")
	}
	return validateStorage(&w.Spec)
}

func validateEnvelope(w *Workload) error {
	if w.APIVersion != "" && w.APIVersion != APIVersion {
		return fmt.Errorf("unsupported apiVersion %q (want %q)", w.APIVersion, APIVersion)
	}
	if w.Kind != "" && w.Kind != Kind {
		return fmt.Errorf("unsupported kind %q (want %q)", w.Kind, Kind)
	}
	if w.Metadata.Name == "" {
		return fmt.Errorf("metadata.name is required")
	}
	return nil
}

// validateResources checks the image+sizing rules. nodeOnly workloads
// provision a burst without running anything, so they're allowed to
// omit image.
func validateResources(s *Spec) error {
	if s.Image == "" && !s.NodeOnly {
		return fmt.Errorf("spec.image is required (unless spec.nodeOnly=true)")
	}
	if s.Size != "" && (s.CPU != "" || s.Memory != "") {
		return fmt.Errorf("spec.size and spec.cpu/spec.memory are mutually exclusive")
	}
	if (s.CPU == "") != (s.Memory == "") {
		return fmt.Errorf("spec.cpu and spec.memory must be set together")
	}
	hasMachine := s.Machine != nil && s.Machine.FlyType != ""
	if !hasMachine && s.Size == "" && s.CPU == "" && s.Memory == "" {
		return fmt.Errorf("spec must set either size, cpu+memory, or machine")
	}
	if hasMachine && (s.Size != "" || s.CPU != "" || s.Memory != "") {
		return fmt.Errorf("spec.machine is mutually exclusive with size and cpu/memory")
	}
	if s.Size != "" {
		if _, ok := LookupSize(s.Size); !ok {
			return fmt.Errorf("unknown size %q (valid: %s)", s.Size, strings.Join(SizeNames(), ", "))
		}
	}
	return nil
}

// validateBackendCompat checks the backend enum and incompatibilities
// between backend choice and other spec fields. Fly.io GPU was
// deprecated upstream (Aug 2025); GPU workloads must route to Linode.
func validateBackendCompat(s *Spec) error {
	switch s.Backend {
	case "", backends.BackendAuto, backends.TypeFlyIO, backends.TypeLinode, backends.TypeAWS, backends.TypeGCP, backends.TypeAzure:
	default:
		return fmt.Errorf("unknown spec.backend %q (valid: %s, %s, %s, %s, %s, %s)", s.Backend, backends.BackendAuto, backends.TypeFlyIO, backends.TypeLinode, backends.TypeAWS, backends.TypeGCP, backends.TypeAzure)
	}
	if s.Backend == backends.TypeFlyIO && s.GPU != nil {
		return fmt.Errorf("spec.backend=flyio does not support GPU workloads (Fly GPU deprecated); use backend=linode or auto")
	}
	if s.Backend == backends.TypeGCP && s.GPU != nil {
		return fmt.Errorf("spec.backend=gcp does not support GPU workloads yet; use backend=linode, backend=aws, or auto")
	}
	if s.Backend == backends.TypeAzure && s.GPU != nil {
		return fmt.Errorf("spec.backend=azure does not support GPU workloads yet; use backend=linode, backend=aws, or auto")
	}
	if s.Region != "" && s.Backend != backends.TypeLinode {
		// Region is honored only by the Linode backend today. "" (auto) can route
		// a CPU workload to Fly or a datacenter GPU to AWS, both of which ignore
		// the pin — so require an explicit backend=linode rather than silently
		// violating the region a customer set.
		return fmt.Errorf("spec.region is honored only by backend=linode; set backend: linode explicitly (got backend=%q)", s.Backend)
	}
	if s.Machine != nil && s.Machine.FlyType != "" && (s.Backend == backends.TypeLinode || s.Backend == backends.TypeAWS || s.Backend == backends.TypeGCP || s.Backend == backends.TypeAzure) {
		return fmt.Errorf("spec.machine.flyType is Fly-specific and incompatible with backend=%s", s.Backend)
	}
	return nil
}

func validateNetworking(n *NetworkingSpec) error {
	if n == nil || n.Tier == "" {
		return nil
	}
	switch n.Tier {
	case NetworkingTierFull:
		return nil
	case NetworkingTierLite:
		// The lite tier is permanently unsupported. Refuse it before any
		// provider, mesh, billing or PodCIDR side effect so a legacy request
		// cannot silently be run and billed as the full tier it never asked
		// for.
		return fmt.Errorf("spec.networking.tier %q is not supported: the lite tier has been removed; omit spec.networking.tier or set it to %q (empty defaults to full)", n.Tier, NetworkingTierFull)
	default:
		return fmt.Errorf("unknown spec.networking.tier %q (valid: %s)", n.Tier, NetworkingTierFull)
	}
}

func validateGPU(g *GPURequest) error {
	if g == nil {
		return nil
	}
	if g.Count < 0 {
		return fmt.Errorf("spec.gpu.count must be >= 0")
	}
	if math.IsNaN(g.MaxHourlyUSD) || math.IsInf(g.MaxHourlyUSD, 0) {
		return fmt.Errorf("spec.gpu.maxHourlyUSD must be finite")
	}
	if g.MaxHourlyUSD < 0 {
		return fmt.Errorf("spec.gpu.maxHourlyUSD must be >= 0")
	}
	switch g.Reliability {
	case "", ReliabilityReliable:
		// Both mean on-demand, non-preemptible capacity — what every live
		// provider launch actually asks for.
	case ReliabilitySpot, ReliabilityAny:
		// Nothing downstream reads this field: the decider and every provider
		// launch on-demand regardless. Accepting it would sell an interruptible
		// tier and run — and bill — reliable capacity, so refuse before any
		// side effect rather than silently honoring the wrong contract.
		return fmt.Errorf("spec.gpu.reliability %q is not supported: the live launch path provisions on-demand capacity only, so the request would run and bill as %s; set reliability: %s or omit it",
			g.Reliability, ReliabilityReliable, ReliabilityReliable)
	default:
		return fmt.Errorf("unknown spec.gpu.reliability %q (valid: %s)",
			g.Reliability, ReliabilityReliable)
	}
	if g.Kind == "" && g.SKU == "" {
		return fmt.Errorf("spec.gpu requires either kind or sku")
	}
	return nil
}

// validateStorage checks shape-only constraints on storage entries.
// Cache and Persistent names must be unique within their list,
// retention strings must parse, and BucketRef must have a bucket +
// credentials reference.
func validateStorage(s *Spec) error {
	if s.ModelVolume != "" && s.Backend != backends.TypeLinode {
		// ModelVolume is a Linode block-storage attach; every other backend
		// (incl. "" / auto, which can route to Fly or AWS) silently ignores it.
		// Require an explicit backend=linode so it never becomes a no-op.
		return fmt.Errorf("spec.modelVolume is supported only by backend=linode; set backend: linode explicitly (got %q)", s.Backend)
	}
	if s.Storage == nil {
		return nil
	}
	seenCache := map[string]bool{}
	seenCacheTarget := map[string]bool{}
	for i, c := range s.Storage.Cache {
		if c.Name == "" {
			return fmt.Errorf("spec.storage.cache[%d].name is required", i)
		}
		if seenCache[c.Name] {
			return fmt.Errorf("spec.storage.cache[%d].name %q is duplicated", i, c.Name)
		}
		seenCache[c.Name] = true
		if c.Target == "" {
			return fmt.Errorf("spec.storage.cache[%q].target is required", c.Name)
		}
		if !strings.HasPrefix(c.Target, "/") {
			return fmt.Errorf("spec.storage.cache[%q].target must be an absolute path, got %q", c.Name, c.Target)
		}
		if seenCacheTarget[c.Target] {
			return fmt.Errorf("spec.storage.cache[%q].target %q collides with another cache mount path", c.Name, c.Target)
		}
		seenCacheTarget[c.Target] = true
		if c.SizeHintGB < 0 || c.SizeHintGB > maxCacheSizeHintGB {
			return fmt.Errorf("spec.storage.cache[%q].sizeHintGB must be between 0 and %d GiB, got %d", c.Name, maxCacheSizeHintGB, c.SizeHintGB)
		}
		if err := validateBucketRef(&c.Source, fmt.Sprintf("spec.storage.cache[%q].source", c.Name)); err != nil {
			return err
		}
		if retention := strings.TrimSpace(c.Retention); retention != "" && retention != "ephemeral" {
			// Cache data currently lives in a pod-local emptyDir. Accepting a
			// durable policy would claim data survives the workload even though
			// no backend cache volume is provisioned or reattached. Refuse that
			// contract before central can reserve or create paid capacity.
			if err := validateRetention(retention, fmt.Sprintf("spec.storage.cache[%q].retention", c.Name)); err != nil {
				return err
			}
			return fmt.Errorf("spec.storage.cache[%q].retention %q is not supported: cache storage is pod-local and ephemeral; use retention: ephemeral or omit it", c.Name, c.Retention)
		}
	}
	seenPersist := map[string]bool{}
	for i, p := range s.Storage.Persistent {
		if p.Name == "" {
			return fmt.Errorf("spec.storage.persistent[%d].name is required", i)
		}
		if seenPersist[p.Name] {
			return fmt.Errorf("spec.storage.persistent[%d].name %q is duplicated", i, p.Name)
		}
		seenPersist[p.Name] = true
		if p.Target == "" {
			return fmt.Errorf("spec.storage.persistent[%q].target is required", p.Name)
		}
		if !strings.HasPrefix(p.Target, "/") {
			return fmt.Errorf("spec.storage.persistent[%q].target must be an absolute path, got %q", p.Name, p.Target)
		}
		if p.SizeGB <= 0 {
			return fmt.Errorf("spec.storage.persistent[%q].sizeGB must be > 0", p.Name)
		}
		if err := validateRetention(p.Retention, fmt.Sprintf("spec.storage.persistent[%q].retention", p.Name)); err != nil {
			return err
		}
		if p.Snapshot != nil {
			if err := validateBucketRef(&p.Snapshot.To, fmt.Sprintf("spec.storage.persistent[%q].snapshot.to", p.Name)); err != nil {
				return err
			}
			if p.Snapshot.Interval < 0 {
				return fmt.Errorf("spec.storage.persistent[%q].snapshot.interval must be >= 0", p.Name)
			}
		}
	}
	if len(s.Storage.Artifacts) > 0 {
		if s.NodeOnly {
			return fmt.Errorf("spec.storage.artifacts requires a Yscale-managed Job; nodeOnly workloads own their own output lifecycle")
		}
		if s.Replicas > 1 {
			return fmt.Errorf("spec.storage.artifacts currently supports exactly one workload replica")
		}
		if len(s.Storage.Artifacts) > maxArtifactOutputs {
			return fmt.Errorf("spec.storage.artifacts supports at most %d outputs", maxArtifactOutputs)
		}
		encoded, err := json.Marshal(s.Storage.Artifacts)
		if err != nil {
			return fmt.Errorf("spec.storage.artifacts cannot be encoded: %w", err)
		}
		if len(encoded) > maxArtifactConfigBytes {
			return fmt.Errorf("spec.storage.artifacts exceeds the %d-byte configuration limit", maxArtifactConfigBytes)
		}
	}
	seenArtifacts := map[string]bool{}
	occupiedTargets := make([]string, 0, len(s.Storage.Cache)+len(s.Storage.Persistent)+len(s.Storage.Artifacts))
	for _, c := range s.Storage.Cache {
		occupiedTargets = append(occupiedTargets, c.Target)
	}
	for _, p := range s.Storage.Persistent {
		occupiedTargets = append(occupiedTargets, p.Target)
	}
	for i, artifact := range s.Storage.Artifacts {
		path := fmt.Sprintf("spec.storage.artifacts[%d]", i)
		if artifact.Name == "" {
			return fmt.Errorf("%s.name is required", path)
		}
		if seenArtifacts[artifact.Name] {
			return fmt.Errorf("%s.name %q is duplicated", path, artifact.Name)
		}
		seenArtifacts[artifact.Name] = true
		if !safeArtifactTarget(artifact.Target) {
			return fmt.Errorf("%s.target must be a canonical absolute directory without '.', '..', or repeated separators, got %q", path, artifact.Target)
		}
		for _, existing := range occupiedTargets {
			if mountPathsOverlap(existing, artifact.Target) {
				return fmt.Errorf("%s.target %q overlaps storage mount %q", path, artifact.Target, existing)
			}
		}
		occupiedTargets = append(occupiedTargets, artifact.Target)
		if artifact.MaxFiles < 1 || artifact.MaxFiles > maxArtifactFiles {
			return fmt.Errorf("%s.maxFiles must be between 1 and %d", path, maxArtifactFiles)
		}
		if artifact.MaxSizeGB < 1 || artifact.MaxSizeGB > maxArtifactSizeGB {
			return fmt.Errorf("%s.maxSizeGB must be between 1 and %d", path, maxArtifactSizeGB)
		}
		if err := validateBucketRef(&artifact.To, path+".to"); err != nil {
			return err
		}
	}
	return nil
}

// maxCacheSizeHintGB bounds CacheSpec.SizeHintGB so sizeLimitFromGB's
// GiB->bytes multiply cannot overflow int64 into a negative Quantity. 64 TiB is
// far above any real model cache.
const (
	maxCacheSizeHintGB     = 65536
	maxArtifactOutputs     = 4
	maxArtifactFiles       = 1000
	maxArtifactSizeGB      = 1024
	maxArtifactConfigBytes = 64 << 10
)

func safeArtifactTarget(target string) bool {
	if target == "" || target == "/" || !strings.HasPrefix(target, "/") || strings.ContainsAny(target, "\r\n\x00") || path.Clean(target) != target {
		return false
	}
	for _, segment := range strings.Split(target, "/") {
		if segment == "." || segment == ".." {
			return false
		}
	}
	return true
}

func mountPathsOverlap(a, b string) bool {
	a = path.Clean(a)
	b = path.Clean(b)
	return a == b || strings.HasPrefix(a, b+"/") || strings.HasPrefix(b, a+"/")
}

func artifactHostPath(w *Workload, name string) string {
	identity := namespaceOrDefault(w.Metadata.Namespace) + "/" + w.Metadata.Name + "\x00" + name
	sum := sha256.Sum256([]byte(identity))
	return fmt.Sprintf("/var/lib/yscale/artifacts/%x", sum[:16])
}

func sizeLimitFromGB(sizeGB int) *resource.Quantity {
	if sizeGB <= 0 {
		return nil
	}
	return resource.NewQuantity(int64(sizeGB)*1024*1024*1024, resource.BinarySI)
}

func validateBucketRef(b *BucketRef, path string) error {
	if b.Bucket == "" {
		return fmt.Errorf("%s.bucket is required", path)
	}
	if b.CredentialsSecret == "" {
		return fmt.Errorf("%s.credentialsSecret is required (K8s Secret with AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY)", path)
	}
	// The agent reads this Secret only from the workload's own
	// namespace, so a "namespace/name" reference can never resolve.
	// Rejecting it here beats a burst that boots and then fails its
	// cache init with a 400 from the storage signer.
	if strings.Contains(b.CredentialsSecret, "/") {
		return fmt.Errorf("%s.credentialsSecret must be a bare Secret name in the workload's namespace, not %q", path, b.CredentialsSecret)
	}
	return nil
}

// validateRetention is the workload-spec front gate. The actual
// parsing logic lives in pkg/cache.ParseRetention so the rules
// can't drift between submit-time validation and runtime use.
func validateRetention(r, path string) error {
	if _, err := cache.ParseRetention(r); err != nil {
		return fmt.Errorf("%s: %w", path, err)
	}
	return nil
}

func buildResources(s *Spec) (corev1.ResourceRequirements, error) {
	cpu, mem, err := cpuAndMemory(s)
	if err != nil {
		return corev1.ResourceRequirements{}, err
	}

	requests := corev1.ResourceList{
		corev1.ResourceCPU:    cpu,
		corev1.ResourceMemory: mem,
	}
	limits := corev1.ResourceList{
		corev1.ResourceCPU:    cpu,
		corev1.ResourceMemory: mem,
	}

	if s.GPU != nil {
		// nvidia.com/gpu is the canonical extended resource for NVIDIA
		// GPUs; backends surface their GPUs this way. Count zero is the
		// public default-one spelling and must match backend provisioning.
		gpuQty := resource.NewQuantity(int64(max(1, s.GPU.Count)), resource.DecimalSI)
		limits[corev1.ResourceName("nvidia.com/gpu")] = *gpuQty
	}

	return corev1.ResourceRequirements{
		Requests: requests,
		Limits:   limits,
	}, nil
}

func cpuAndMemory(s *Spec) (cpu, mem resource.Quantity, err error) {
	if s.Size != "" {
		size, _ := LookupSize(s.Size) // existence checked in Validate
		cpu = *resource.NewMilliQuantity(size.CPUMillis, resource.DecimalSI)
		mem = *resource.NewQuantity(size.MemoryMB*1024*1024, resource.BinarySI)
		return cpu, mem, nil
	}
	cpu, parseErr := resource.ParseQuantity(s.CPU)
	if parseErr != nil {
		return cpu, mem, fmt.Errorf("parsing spec.cpu %q: %w", s.CPU, parseErr)
	}
	mem, parseErr = resource.ParseQuantity(s.Memory)
	if parseErr != nil {
		return cpu, mem, fmt.Errorf("parsing spec.memory %q: %w", s.Memory, parseErr)
	}
	return cpu, mem, nil
}

func toCoreEnv(in []EnvVar) []corev1.EnvVar {
	out := make([]corev1.EnvVar, 0, len(in))
	for _, e := range in {
		ce := corev1.EnvVar{Name: e.Name, Value: e.Value}
		if e.ValueFrom != nil {
			ce.ValueFrom = &corev1.EnvVarSource{}
			if e.ValueFrom.SecretKeyRef != nil {
				ce.ValueFrom.SecretKeyRef = &corev1.SecretKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{Name: e.ValueFrom.SecretKeyRef.Name},
					Key:                  e.ValueFrom.SecretKeyRef.Key,
				}
			}
			if e.ValueFrom.ConfigMapKeyRef != nil {
				ce.ValueFrom.ConfigMapKeyRef = &corev1.ConfigMapKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{Name: e.ValueFrom.ConfigMapKeyRef.Name},
					Key:                  e.ValueFrom.ConfigMapKeyRef.Key,
				}
			}
		}
		out = append(out, ce)
	}
	return out
}

func namespaceOrDefault(ns string) string {
	if ns == "" {
		return "default"
	}
	return ns
}

func int32Ptr(v int32) *int32 { return &v }

func int64Ptr(v int64) *int64 { return &v }

func boolPtr(v bool) *bool { return &v }

// burstTolerations returns the tolerations the workload pod needs to land
// on a yscale burst node. Every burst carries yscale.sh/burst-node:NoSchedule
// (so the pod must tolerate it). The full tier — now the only supported one
// — runs host-mode Cilium on the burst and registers the agent-not-ready
// taint until Cilium is healthy (kube-proxy-replacement + ClusterIP routing
// ready). The pod must NOT tolerate it: otherwise it schedules immediately,
// its ClusterIP / pod-to-pod traffic fails (Cilium isn't up), and with
// backoffLimit 0 the whole Job fails. Leaving the taint un-tolerated makes
// the scheduler hold the pod Pending until Cilium readies and removes the
// taint, then the pod lands on a fully networked node.
func burstTolerations(w *Workload) []corev1.Toleration {
	tols := []corev1.Toleration{
		{Key: "yscale.sh/burst-node", Operator: corev1.TolerationOpExists, Effect: corev1.TaintEffectNoSchedule},
	}
	// GPU device-plugin readiness gate (issue #39). nvidia.com/gpu-not-ready is
	// applied at kubelet registration and cleared by the controller only once the
	// NVIDIA device plugin has registered nvidia.com/gpu as allocatable. A GPU
	// workload must NOT tolerate it — otherwise it schedules before the resource
	// exists, hits FailedScheduling: Insufficient nvidia.com/gpu and burns its
	// backoffLimit. Non-GPU workloads tolerate it as a harmless no-op (they never
	// request nvidia.com/gpu).
	if w.Spec.GPU == nil {
		tols = append(tols, corev1.Toleration{
			Key:      "nvidia.com/gpu-not-ready",
			Operator: corev1.TolerationOpExists,
			Effect:   corev1.TaintEffectNoSchedule,
		})
	}
	return tols
}

// ResolveNetworkingTier returns the effective workload networking tier.
// Full is the only supported tier: Validate refuses "lite" (and every other
// non-empty non-"full" value) before side effects, so downstream code always
// sees full. This function preserves that invariant defensively — anything a
// caller manages to pass in still resolves to full so a disabled path cannot
// be re-opened by a spec that bypassed validation.
func ResolveNetworkingTier(w *Workload) string {
	return NetworkingTierFull
}

// jobReplicas returns the Pod parallelism for this Workload. Default
// 1; clamps to 1 if the spec sets a non-positive number.
func jobReplicas(w *Workload) *int32 {
	n := w.Spec.Replicas
	if n <= 0 {
		n = 1
	}
	return &n
}

// EffectiveReplicas is the same as jobReplicas but returns the raw
// int (no pointer). Decider uses this to size the burst.
func EffectiveReplicas(w *Workload) int {
	if w.Spec.Replicas <= 0 {
		return 1
	}
	return int(w.Spec.Replicas)
}

// Compile-time check that the package's annotation keys agree with the
// runtime label namespace used by the controller. If the controller's
// LabelBurstNode constant ever drifts, the linker won't catch it but a
// reader of this file will.
var _ = backends.NodeNamePrefix
