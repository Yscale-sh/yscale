package cache

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// ManifestFilename is what burst-init writes after a successful cache
// fetch. Subsequent boots check it to decide whether to skip the fetch.
const ManifestFilename = "manifest.json"

// ManifestVersion is bumped on breaking format changes. Burst-init
// must reject a manifest with a higher version than it understands.
const ManifestVersion = 1

// Manifest is what we write at <cache_dir>/manifest.json after a
// successful fetch. ETags pin source freshness — on next boot, the
// burst can ask "have any of these objects changed?" before deciding
// to refetch.
type Manifest struct {
	Version     int               `json:"version"`
	Key         Key               `json:"key"`
	SourceURI   string            `json:"source_uri"`
	SizeBytes   int64             `json:"size_bytes"`
	FileCount   int               `json:"file_count"`
	PopulatedAt time.Time         `json:"populated_at"`
	SourceETags map[string]string `json:"source_etags,omitempty"`
}

// Read returns the manifest at dir/manifest.json. (nil, nil) when the
// file is absent — the caller treats that as "needs fetch."
func Read(dir string) (*Manifest, error) {
	data, err := os.ReadFile(filepath.Join(dir, ManifestFilename))
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var m Manifest
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, fmt.Errorf("parse manifest: %w", err)
	}
	if m.Version > ManifestVersion {
		return nil, fmt.Errorf("manifest version %d unsupported (max %d) — refetch required", m.Version, ManifestVersion)
	}
	return &m, nil
}

// Write atomically writes the manifest. Atomic so a crashed write
// doesn't leave a half-written file that next boot misinterprets.
func Write(dir string, m *Manifest) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	m.Version = ManifestVersion
	data, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return err
	}
	tmp := filepath.Join(dir, ManifestFilename+".tmp")
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, filepath.Join(dir, ManifestFilename))
}
