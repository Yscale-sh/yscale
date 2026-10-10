// yscale:proprietary

package billingcanary

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// The browser handoff is a private on-disk protocol between this test process
// and an out-of-process agent-browser driver invoked by
// scripts/billing-canary.sh. No chromedp/rod dependency is added; the two
// participants coordinate through three fixed filenames in a caller-supplied
// control directory:
//
//	checkout-url    - the ephemeral hosted-checkout URL; written by the test,
//	                  read by the wrapper, never printed.
//	payment-token   - the opaque handshake token the wrapper echoes when it
//	                  confirms the payment attempt completed; the test picks
//	                  the value so a stale reply cannot be replayed.
//	payment-done    - the wrapper's terminal signal (contents "ok" or
//	                  "cancelled"); presence-only, never enumerated.
//
// The control directory MUST be caller-created with permissions 0700 inside a
// temp dir. This package enforces it and refuses to write anywhere else.
const (
	checkoutURLFile  = "checkout-url"
	paymentTokenFile = "payment-token"
	paymentDoneFile  = "payment-done"
	doneOK           = "ok"
	doneCancelled    = "cancelled"
)

// ErrBrowserHandoff wraps every browser-coordination failure so the canary can
// surface a single-line, redacted reason instead of a raw file-system error.
var ErrBrowserHandoff = errors.New("billingcanary: browser handoff rejected")

// BrowserHandoff coordinates a single Stripe hosted-checkout completion with
// the out-of-process wrapper. The zero value is not usable; construct with
// NewBrowserHandoff so the control directory is verified before any file is
// written.
type BrowserHandoff struct {
	dir   string
	token string
}

// NewBrowserHandoff verifies the caller-supplied control directory and mints
// a fresh handshake token. It refuses a symlink, a world-readable directory,
// or a directory the current process does not own.
func NewBrowserHandoff(dir string) (*BrowserHandoff, error) {
	if !filepath.IsAbs(dir) {
		return nil, fmt.Errorf("%w: control dir must be absolute", ErrBrowserHandoff)
	}
	info, err := os.Lstat(dir)
	if err != nil {
		return nil, fmt.Errorf("%w: control dir stat: %v", ErrBrowserHandoff, err)
	}
	if info.Mode()&os.ModeSymlink != 0 {
		return nil, fmt.Errorf("%w: control dir must not be a symlink", ErrBrowserHandoff)
	}
	if !info.IsDir() {
		return nil, fmt.Errorf("%w: control dir must be a directory", ErrBrowserHandoff)
	}
	if perm := info.Mode().Perm(); perm&0o077 != 0 {
		return nil, fmt.Errorf("%w: control dir must be mode 0700", ErrBrowserHandoff)
	}
	token, err := newHandshakeToken()
	if err != nil {
		return nil, fmt.Errorf("%w: mint handshake token: %v", ErrBrowserHandoff, err)
	}
	return &BrowserHandoff{dir: dir, token: token}, nil
}

// PublishCheckoutURL hands the ephemeral URL to the wrapper. The URL itself
// never appears in stdout or evidence; a wrapper that copies it into a log is
// out of scope for this package but violates the runbook.
//
// A stale payment-done file from a prior partial run in the same control
// directory would otherwise short-circuit WaitForCompletion before the wrapper
// has even opened the page; clearing it here is the defensive step that
// guarantees only this handoff's own signal is ever observed.
func (h *BrowserHandoff) PublishCheckoutURL(url string) error {
	if h == nil {
		return fmt.Errorf("%w: nil handoff", ErrBrowserHandoff)
	}
	if !strings.HasPrefix(url, "https://") {
		return fmt.Errorf("%w: URL must be https", ErrBrowserHandoff)
	}
	if err := os.Remove(filepath.Join(h.dir, paymentDoneFile)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("%w: clear stale completion: %v", ErrBrowserHandoff, err)
	}
	if err := writePrivateFile(filepath.Join(h.dir, checkoutURLFile), []byte(url+"\n")); err != nil {
		return fmt.Errorf("%w: publish url: %v", ErrBrowserHandoff, err)
	}
	if err := writePrivateFile(filepath.Join(h.dir, paymentTokenFile), []byte(h.token+"\n")); err != nil {
		return fmt.Errorf("%w: publish token: %v", ErrBrowserHandoff, err)
	}
	return nil
}

// WaitForCompletion polls for the wrapper's done sentinel and returns whether
// it declared success. The wrapper's file must contain the handshake token on
// its first line so a stale response from a previous run cannot satisfy the
// wait.
func (h *BrowserHandoff) WaitForCompletion(ctx context.Context, timeout time.Duration) (bool, error) {
	if h == nil {
		return false, fmt.Errorf("%w: nil handoff", ErrBrowserHandoff)
	}
	if timeout <= 0 {
		return false, fmt.Errorf("%w: timeout must be positive", ErrBrowserHandoff)
	}
	deadline := time.Now().Add(timeout)
	tick := time.NewTicker(500 * time.Millisecond)
	defer tick.Stop()
	for {
		ok, done, err := h.readCompletion()
		if err != nil {
			return false, err
		}
		if done {
			return ok, nil
		}
		if time.Now().After(deadline) {
			return false, fmt.Errorf("%w: browser did not complete within %s", ErrBrowserHandoff, timeout)
		}
		select {
		case <-ctx.Done():
			return false, ctx.Err()
		case <-tick.C:
		}
	}
}

// Cleanup removes every file this handoff wrote. The directory itself is the
// caller's to remove.
func (h *BrowserHandoff) Cleanup() {
	if h == nil {
		return
	}
	for _, name := range []string{checkoutURLFile, paymentTokenFile, paymentDoneFile} {
		_ = os.Remove(filepath.Join(h.dir, name))
	}
}

func (h *BrowserHandoff) readCompletion() (ok, done bool, err error) {
	body, readErr := os.ReadFile(filepath.Join(h.dir, paymentDoneFile))
	if readErr != nil {
		if errors.Is(readErr, os.ErrNotExist) {
			return false, false, nil
		}
		return false, false, fmt.Errorf("%w: read completion: %v", ErrBrowserHandoff, readErr)
	}
	lines := strings.SplitN(strings.TrimSpace(string(body)), "\n", 2)
	if len(lines) < 2 || lines[0] != h.token {
		return false, false, fmt.Errorf("%w: completion file did not echo handshake token", ErrBrowserHandoff)
	}
	switch strings.TrimSpace(lines[1]) {
	case doneOK:
		return true, true, nil
	case doneCancelled:
		return false, true, nil
	default:
		return false, false, fmt.Errorf("%w: completion status must be %q or %q", ErrBrowserHandoff, doneOK, doneCancelled)
	}
}

func writePrivateFile(path string, body []byte) error {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	defer f.Close()
	if _, err := f.Write(body); err != nil {
		return err
	}
	return nil
}

func newHandshakeToken() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}
