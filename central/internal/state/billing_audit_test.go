// yscale:proprietary

package state

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestServiceCreditAuditShapeIsClosedAndSafe(t *testing.T) {
	s, spy, ids := manageStore(t, "cust_bill", map[string]string{"operator": RoleOwner})
	ev := NewAuditEvent(AuditEvent{
		CustomerID: "cust_bill", Actor: HumanActor(ids["operator"], "cust_bill"),
		Action: ActionBillingServiceCreditGrant, Outcome: OutcomeAccepted,
		TargetKind: TargetTenant, TargetID: "cust_bill",
		Detail: AuditDetail{Reason: ReasonBillingServiceCreditGranted, AmountMicroUSD: 2500000, Currency: "USD"},
	})
	if err := s.AppendAudit(ev); err != nil {
		t.Fatal(err)
	}
	events := spy.eventsWith(ActionBillingServiceCreditGrant)
	if len(events) != 1 || events[0].Actor.Kind != ActorHuman || events[0].Actor.AccountID != ids["operator"] || events[0].Detail.AmountMicroUSD != 2500000 {
		t.Fatalf("service credit audit = %+v", events)
	}
	raw, _ := json.Marshal(events[0])
	for _, forbidden := range []string{"payment_intent", "card", "token", "provider_secret"} {
		if strings.Contains(string(raw), forbidden) {
			t.Fatalf("audit leaked %q: %s", forbidden, raw)
		}
	}
}
