// yscale:proprietary

package billingcanary

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"time"
)

// stripeSignature produces the Stripe-Signature header value the gateway's
// VerifyWebhook accepts: t=<unix>,v1=<hex hmac_sha256(secret, "<t>.<body>")>.
// The canary uses this to feed the real HTTP intake with a valid signed body
// for the deterministic scenarios that must not call Stripe.
func stripeSignature(payload []byte, secret string, at time.Time) string {
	ts := at.UTC().Unix()
	mac := hmac.New(sha256.New, []byte(secret))
	fmt.Fprintf(mac, "%d.%s", ts, payload)
	return fmt.Sprintf("t=%d,v1=%s", ts, hex.EncodeToString(mac.Sum(nil)))
}
