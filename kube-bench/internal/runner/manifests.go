package runner

import (
	"crypto/sha256"
	"fmt"
	"regexp"
	"strings"

	"github.com/JakeNesler/yscale-kube-bench/internal/config"
)

var invalidDNSLabel = regexp.MustCompile(`[^a-z0-9-]+`)

type jobRequest struct {
	Name            string
	NodeName        string
	Args            []string
	Environment     map[string]string
	Volumes         []map[string]any
	VolumeMounts    []map[string]any
	Resources       map[string]any
	Privileged      bool
	Image           string
	ImagePullPolicy string
	TimeoutSeconds  int
}

func (r *Runner) jobManifest(request jobRequest) map[string]any {
	labels := r.resourceLabels(map[string]string{"yscale.dev/bench-role": "worker"})
	container := map[string]any{
		"name":            "bench",
		"image":           choose(request.Image, r.config.Spec.Image),
		"imagePullPolicy": choose(request.ImagePullPolicy, r.config.Spec.ImagePullPolicy),
		"args":            request.Args,
		"resources":       defaultResources(request.Resources),
	}
	if len(request.Environment) > 0 {
		container["env"] = environment(request.Environment)
	}
	if len(request.VolumeMounts) > 0 {
		container["volumeMounts"] = request.VolumeMounts
	}
	if request.Privileged {
		container["securityContext"] = map[string]any{"privileged": true, "allowPrivilegeEscalation": true}
	}
	podSpec := map[string]any{
		"restartPolicy":                 "Never",
		"terminationGracePeriodSeconds": 0,
		"tolerations":                   []map[string]any{{"operator": "Exists"}},
		"containers":                    []map[string]any{container},
	}
	if request.NodeName != "" {
		podSpec["nodeName"] = request.NodeName
	}
	if len(request.Volumes) > 0 {
		podSpec["volumes"] = request.Volumes
	}
	return map[string]any{
		"apiVersion": "batch/v1",
		"kind":       "Job",
		"metadata": map[string]any{
			"name":   sanitizeName(request.Name),
			"labels": labels,
		},
		"spec": map[string]any{
			"backoffLimit": 0,
			"template": map[string]any{
				"metadata": map[string]any{"labels": labels},
				"spec":     podSpec,
			},
		},
	}
}

func (r *Runner) serverPodManifest(name, node string) map[string]any {
	labels := r.resourceLabels(map[string]string{
		"yscale.dev/bench-role": "network-server",
		"yscale.dev/node":       sanitizeName(node),
	})
	return map[string]any{
		"apiVersion": "v1",
		"kind":       "Pod",
		"metadata": map[string]any{
			"name":   sanitizeName(name),
			"labels": labels,
		},
		"spec": map[string]any{
			"nodeName":                      node,
			"restartPolicy":                 "Never",
			"terminationGracePeriodSeconds": 0,
			"tolerations":                   []map[string]any{{"operator": "Exists"}},
			"containers": []map[string]any{
				{
					"name":            "server",
					"image":           r.config.Spec.Image,
					"imagePullPolicy": r.config.Spec.ImagePullPolicy,
					"args":            []string{"worker", "net-server", "--tcp-listen=:9090", "--udp-listen=:9091"},
					"ports": []map[string]any{
						{"name": "tcp", "containerPort": 9090, "protocol": "TCP"},
						{"name": "udp", "containerPort": 9091, "protocol": "UDP"},
					},
					"readinessProbe": map[string]any{
						"tcpSocket":           map[string]any{"port": 9090},
						"initialDelaySeconds": 0,
						"periodSeconds":       1,
						"timeoutSeconds":      1,
						"failureThreshold":    60,
					},
					"resources": defaultResources(nil),
				},
			},
		},
	}
}

func (r *Runner) reactionPodManifest(name, node, image, pullPolicy string) map[string]any {
	labels := r.resourceLabels(map[string]string{"yscale.dev/bench-role": "reaction"})
	return map[string]any{
		"apiVersion": "v1",
		"kind":       "Pod",
		"metadata": map[string]any{
			"name":   sanitizeName(name),
			"labels": labels,
		},
		"spec": map[string]any{
			"nodeSelector":                  map[string]string{"kubernetes.io/hostname": node},
			"restartPolicy":                 "Never",
			"terminationGracePeriodSeconds": 0,
			"tolerations":                   []map[string]any{{"operator": "Exists"}},
			"containers": []map[string]any{
				{
					"name":            "reaction",
					"image":           image,
					"imagePullPolicy": pullPolicy,
					"args":            []string{"worker", "ready-server", "--listen=:8080"},
					"ports":           []map[string]any{{"name": "ready", "containerPort": 8080}},
					"readinessProbe": map[string]any{
						"httpGet":             map[string]any{"path": "/ready", "port": 8080},
						"initialDelaySeconds": 0,
						"periodSeconds":       1,
						"timeoutSeconds":      1,
						"failureThreshold":    120,
					},
					"resources": map[string]any{
						"requests": map[string]string{"cpu": "5m", "memory": "8Mi"},
					},
				},
			},
		},
	}
}

func (r *Runner) networkServiceManifest(name string) map[string]any {
	return map[string]any{
		"apiVersion": "v1",
		"kind":       "Service",
		"metadata": map[string]any{
			"name":   sanitizeName(name),
			"labels": r.resourceLabels(nil),
		},
		"spec": map[string]any{
			"selector": map[string]string{
				"app.kubernetes.io/name": "yscale-kube-bench",
				"yscale.dev/bench-run":   r.runID,
				"yscale.dev/bench-role":  "network-server",
			},
			"ports": []map[string]any{
				{"name": "tcp", "port": 9090, "targetPort": 9090, "protocol": "TCP"},
				{"name": "udp", "port": 9091, "targetPort": 9091, "protocol": "UDP"},
			},
		},
	}
}

func storageVolume(target config.StorageTarget) ([]map[string]any, []map[string]any, string) {
	name := sanitizeName("storage-" + target.Name)
	mountPath := target.MountPath
	if mountPath == "" {
		mountPath = "/bench/" + sanitizeName(target.Name)
	}
	var source map[string]any
	switch target.Kind {
	case "hostPath":
		source = map[string]any{"hostPath": map[string]any{"path": target.HostPath, "type": "DirectoryOrCreate"}}
	case "pvc":
		source = map[string]any{"persistentVolumeClaim": map[string]any{"claimName": target.ClaimName, "readOnly": false}}
	default:
		source = map[string]any{"emptyDir": map[string]any{}}
	}
	volume := map[string]any{"name": name}
	for key, value := range source {
		volume[key] = value
	}
	mount := map[string]any{"name": name, "mountPath": mountPath, "readOnly": false}
	return []map[string]any{volume}, []map[string]any{mount}, mountPath
}

func hostPathVolumes(paths []config.HostPathMount) ([]map[string]any, []map[string]any) {
	volumes := make([]map[string]any, 0, len(paths))
	mounts := make([]map[string]any, 0, len(paths))
	for index, path := range paths {
		name := path.Name
		if name == "" {
			name = fmt.Sprintf("host-%d", index)
		}
		name = sanitizeName(name)
		volumes = append(volumes, map[string]any{
			"name":     name,
			"hostPath": map[string]any{"path": path.HostPath},
		})
		mounts = append(mounts, map[string]any{
			"name": name, "mountPath": path.MountPath, "readOnly": path.ReadOnly,
		})
	}
	return volumes, mounts
}

func inventoryHostVolumes() ([]map[string]any, []map[string]any) {
	paths := []struct{ name, host, mount string }{
		{"host-proc", "/proc", "/host/proc"},
		{"host-sys", "/sys", "/host/sys"},
		{"host-etc", "/etc", "/host/etc"},
		{"host-dev", "/dev", "/host/dev"},
	}
	var volumes []map[string]any
	var mounts []map[string]any
	for _, path := range paths {
		volumes = append(volumes, map[string]any{"name": path.name, "hostPath": map[string]any{"path": path.host}})
		mounts = append(mounts, map[string]any{"name": path.name, "mountPath": path.mount, "readOnly": true})
	}
	return volumes, mounts
}

func (r *Runner) resourceLabels(extra map[string]string) map[string]string {
	result := make(map[string]string, len(r.labels)+len(extra))
	for key, value := range r.labels {
		result[key] = value
	}
	for key, value := range extra {
		result[key] = value
	}
	return result
}

func defaultResources(custom map[string]any) map[string]any {
	if custom != nil {
		return custom
	}
	return map[string]any{"requests": map[string]string{"cpu": "10m", "memory": "32Mi"}}
}

func environment(values map[string]string) []map[string]string {
	result := make([]map[string]string, 0, len(values))
	for key, value := range values {
		result = append(result, map[string]string{"name": key, "value": value})
	}
	return result
}

func sanitizeName(value string) string {
	value = strings.ToLower(value)
	value = invalidDNSLabel.ReplaceAllString(value, "-")
	value = strings.Trim(value, "-")
	if value == "" {
		value = "bench"
	}
	if len(value) > 63 {
		digest := fmt.Sprintf("%x", sha256.Sum256([]byte(value)))[:8]
		prefix := strings.Trim(value[:54], "-")
		value = prefix + "-" + digest
	}
	return value
}

func choose(value, fallback string) string {
	if strings.TrimSpace(value) != "" {
		return value
	}
	return fallback
}
