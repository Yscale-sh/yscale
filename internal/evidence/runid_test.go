package evidence

import (
	"regexp"
	"testing"
	"time"
)

var runIDPattern = regexp.MustCompile(`^yt-\d+-[0-9a-f]{16}$`)

func TestGenerateRunID_Format(t *testing.T) {
	id, err := GenerateRunID(time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if !runIDPattern.MatchString(id) {
		t.Fatalf("run ID %q does not match expected pattern", id)
	}
}

func TestGenerateRunID_Unique(t *testing.T) {
	now := time.Now()
	a, err := GenerateRunID(now)
	if err != nil {
		t.Fatal(err)
	}
	b, err := GenerateRunID(now)
	if err != nil {
		t.Fatal(err)
	}
	if a == b {
		t.Fatalf("two run IDs with the same timestamp must differ: %q", a)
	}
}

func TestGenerateRunID_Bounded(t *testing.T) {
	id, err := GenerateRunID(time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if len(id) > 64 {
		t.Fatalf("run ID length %d exceeds 64", len(id))
	}
}
