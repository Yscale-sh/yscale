package flyio

import "time"

// Fly Machines API types.
// Reference: https://fly.io/docs/machines/api/machines-resource/

type CreateMachineRequest struct {
	Name   string        `json:"name"`
	Region string        `json:"region,omitempty"`
	Config MachineConfig `json:"config"`
}

type MachineConfig struct {
	Image    string            `json:"image"`
	Env      map[string]string `json:"env,omitempty"`
	Guest    GuestConfig       `json:"guest,omitempty"`
	Init     InitConfig        `json:"init,omitempty"`
	Restart  RestartPolicy     `json:"restart,omitempty"`
	Mounts   []MachineMount    `json:"mounts,omitempty"`
	Metadata map[string]string `json:"metadata,omitempty"`
	// AutoDestroy destroys the machine when it exits.
	AutoDestroy bool `json:"auto_destroy,omitempty"`
}

type MachineMount struct {
	Volume string `json:"volume"`
	Path   string `json:"path"`
}

type GuestConfig struct {
	CPUs     int    `json:"cpus"`
	MemoryMB int    `json:"memory_mb"`
	CPUKind  string `json:"cpu_kind"` // "shared" or "performance"
}

type InitConfig struct {
	Cmd        []string `json:"cmd,omitempty"`
	Entrypoint []string `json:"entrypoint,omitempty"`
}

type RestartPolicy struct {
	Policy string `json:"policy"` // "no", "on-failure", "always"
}

type Machine struct {
	ID         string        `json:"id"`
	Name       string        `json:"name"`
	State      string        `json:"state"`
	Region     string        `json:"region"`
	InstanceID string        `json:"instance_id"`
	PrivateIP  string        `json:"private_ip"`
	Config     MachineConfig `json:"config"`
	CreatedAt  time.Time     `json:"created_at"`
	UpdatedAt  time.Time     `json:"updated_at"`
}

type CreateAppRequest struct {
	AppName string `json:"app_name"`
	OrgSlug string `json:"org_slug"`
}

type App struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

type APIError struct {
	Error   string `json:"error"`
	Status  int    `json:"status"`
	Message string `json:"message"`
}
