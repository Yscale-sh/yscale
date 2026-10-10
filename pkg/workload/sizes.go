package workload

// Size maps to a (cpu, memory) preset. Keep the list short and
// opinionated so users don't have to think about it; they can still
// override by setting Spec.CPU/Memory explicitly.
type Size struct {
	CPUMillis int64
	MemoryMB  int64
}

var sizes = map[string]Size{
	"nano":    {250, 512},
	"small":   {1000, 2048},
	"medium":  {2000, 4096},
	"large":   {4000, 16384},
	"xlarge":  {8000, 32768},
	"2xlarge": {16000, 65536},
}

// LookupSize returns the preset for a size name. The bool is false for
// unknown names so the caller can produce a helpful error.
func LookupSize(name string) (Size, bool) {
	s, ok := sizes[name]
	return s, ok
}

// SizeNames returns the supported preset names, useful for error
// messages and CLI help text.
func SizeNames() []string {
	out := make([]string, 0, len(sizes))
	for k := range sizes {
		out = append(out, k)
	}
	return out
}
