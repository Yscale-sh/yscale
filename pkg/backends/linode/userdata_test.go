package linode

import (
	"bytes"
	"compress/gzip"
	cryptorand "crypto/rand"
	"encoding/base64"
	"io"
	"strings"
	"testing"
)

// encodeUserData must gzip-then-base64 so cloud-init (which auto-gunzips) gets
// the original script, while staying under Linode's 16 KB user_data cap.
func TestEncodeUserData_RoundTripAndGzip(t *testing.T) {
	// A realistic, repetitive bootstrap body (compresses well).
	raw := buildUserData(map[string]string{
		"YSCALE_ROLE":           "transcode",
		"YSCALE_TRANSCODE_MODE": "sharded",
		"DATABASE_URL":          "postgres://user:pass@10.43.0.5:5432/yscale",
	}, strings.Repeat("echo bootstrapping the burst node\n", 400))

	enc, err := encodeUserData(raw)
	if err != nil {
		t.Fatalf("encodeUserData: %v", err)
	}

	gzipped, err := base64.StdEncoding.DecodeString(enc)
	if err != nil {
		t.Fatalf("base64 decode: %v", err)
	}
	// cloud-init detects gzip by magic bytes 0x1f 0x8b.
	if len(gzipped) < 2 || gzipped[0] != 0x1f || gzipped[1] != 0x8b {
		t.Fatalf("payload is not gzip (magic=%x)", gzipped[:min(2, len(gzipped))])
	}
	// The decoded (gzipped) bytes are what counts against Linode's limit.
	if len(gzipped) > maxLinodeUserData {
		t.Fatalf("gzipped user_data %d exceeds limit %d", len(gzipped), maxLinodeUserData)
	}
	if len(gzipped) >= len(raw) {
		t.Fatalf("gzip did not shrink a repetitive script: raw=%d gzipped=%d", len(raw), len(gzipped))
	}

	gr, err := gzip.NewReader(bytes.NewReader(gzipped))
	if err != nil {
		t.Fatalf("gzip reader: %v", err)
	}
	got, err := io.ReadAll(gr)
	if err != nil {
		t.Fatalf("gunzip: %v", err)
	}
	if !bytes.Equal(got, raw) {
		t.Fatalf("round-trip mismatch: cloud-init would not see the original script")
	}
}

// Incompressible input larger than the cap must error clearly, not be sent to
// Linode (which would 400).
func TestEncodeUserData_OverLimitErrors(t *testing.T) {
	big := make([]byte, maxLinodeUserData+8192)
	if _, err := cryptorand.Read(big); err != nil {
		t.Fatalf("rand: %v", err)
	}
	if _, err := encodeUserData(big); err == nil {
		t.Fatal("expected an over-limit error for incompressible oversized user_data, got nil")
	}
}
