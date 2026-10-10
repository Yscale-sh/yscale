// yscale:proprietary

package billingcanary

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func newTightControlDir(t *testing.T) string {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "control")
	if err := os.Mkdir(dir, 0o700); err != nil {
		t.Fatalf("Mkdir: %v", err)
	}
	return dir
}

func TestBrowserHandoffRejectsUnsafeControlDir(t *testing.T) {
	if _, err := NewBrowserHandoff("relative"); !errors.Is(err, ErrBrowserHandoff) {
		t.Fatalf("relative dir err = %v", err)
	}
	dir := filepath.Join(t.TempDir(), "loose")
	if err := os.Mkdir(dir, 0o755); err != nil {
		t.Fatalf("Mkdir: %v", err)
	}
	if _, err := NewBrowserHandoff(dir); !errors.Is(err, ErrBrowserHandoff) {
		t.Fatalf("loose perms err = %v", err)
	}
	file := filepath.Join(t.TempDir(), "not-a-dir")
	if err := os.WriteFile(file, []byte("x"), 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	if _, err := NewBrowserHandoff(file); !errors.Is(err, ErrBrowserHandoff) {
		t.Fatalf("file (not dir) err = %v", err)
	}
}

func TestBrowserHandoffPublishesAndReturnsSuccess(t *testing.T) {
	dir := newTightControlDir(t)
	handoff, err := NewBrowserHandoff(dir)
	if err != nil {
		t.Fatalf("NewBrowserHandoff err = %v", err)
	}
	if err := handoff.PublishCheckoutURL("https://checkout.stripe.com/pay/session"); err != nil {
		t.Fatalf("PublishCheckoutURL err = %v", err)
	}
	tokenBody, err := os.ReadFile(filepath.Join(dir, paymentTokenFile))
	if err != nil {
		t.Fatalf("read token: %v", err)
	}
	token := strings.TrimSpace(string(tokenBody))
	if token == "" {
		t.Fatal("token file empty")
	}
	if err := os.WriteFile(filepath.Join(dir, paymentDoneFile), []byte(token+"\n"+doneOK+"\n"), 0o600); err != nil {
		t.Fatalf("write done: %v", err)
	}
	ok, err := handoff.WaitForCompletion(context.Background(), 2*time.Second)
	if err != nil {
		t.Fatalf("WaitForCompletion err = %v", err)
	}
	if !ok {
		t.Fatalf("WaitForCompletion returned cancelled, want ok")
	}
	handoff.Cleanup()
	if _, err := os.Stat(filepath.Join(dir, paymentDoneFile)); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("done file survived Cleanup: %v", err)
	}
}

func TestBrowserHandoffRejectsStaleReply(t *testing.T) {
	dir := newTightControlDir(t)
	handoff, err := NewBrowserHandoff(dir)
	if err != nil {
		t.Fatalf("NewBrowserHandoff err = %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, paymentDoneFile), []byte("stale-token\n"+doneOK+"\n"), 0o600); err != nil {
		t.Fatalf("write done: %v", err)
	}
	if _, err := handoff.WaitForCompletion(context.Background(), 500*time.Millisecond); !errors.Is(err, ErrBrowserHandoff) {
		t.Fatalf("stale-token err = %v", err)
	}
}

func TestBrowserHandoffTimesOut(t *testing.T) {
	dir := newTightControlDir(t)
	handoff, err := NewBrowserHandoff(dir)
	if err != nil {
		t.Fatalf("NewBrowserHandoff err = %v", err)
	}
	ok, err := handoff.WaitForCompletion(context.Background(), 300*time.Millisecond)
	if !errors.Is(err, ErrBrowserHandoff) {
		t.Fatalf("timeout err = %v", err)
	}
	if ok {
		t.Fatalf("WaitForCompletion returned ok on timeout")
	}
}
