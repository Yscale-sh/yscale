package evidence

import (
	"crypto/rand"
	"fmt"
	"time"
)

// GenerateRunID returns a collision-resistant, bounded run ID using
// wall-clock time and 8 bytes of cryptographic randomness. Format:
// yt-<unix_seconds>-<16 hex chars>. Fails closed if the system CSPRNG
// is unavailable.
func GenerateRunID(now time.Time) (string, error) {
	var buf [8]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return "", fmt.Errorf("generate run ID: CSPRNG unavailable: %w", err)
	}
	return fmt.Sprintf("yt-%d-%x", now.Unix(), buf), nil
}
