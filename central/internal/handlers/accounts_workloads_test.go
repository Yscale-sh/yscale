// yscale:proprietary

package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/yscale-sh/yscale/central/internal/lifecycle"
	"github.com/yscale-sh/yscale/central/internal/state"
	"github.com/yscale-sh/yscale/pkg/protocol"
)

const tenantWorkloadYAML = `apiVersion: yscale.sh/v1
kind: Workload
metadata:
  name: console-job
  namespace: jobs
  labels:
    private-label: label-secret
  tags:
    owner: tag-secret
spec:
  image: busybox:latest
  size: small
  budget:
    maxUSD: 2.5
    deadline: 5m
  command: ["sh", "-c"]
  args: ["echo arg-secret"]
  env:
    - name: API_KEY
      value: env-secret
    - name: SECRET_REF
      valueFrom:
        secretKeyRef:
          name: private-env-secret-ref
          key: private-env-secret-key
    - name: CONFIG_REF
      valueFrom:
        configMapKeyRef:
          name: private-env-config-ref
          key: private-env-config-key
  storage:
    cache:
      - name: private-data-secret
        source:
          bucket: private-bucket-secret
          prefix: private-prefix-secret/
          endpoint: https://account-secret.r2.cloudflarestorage.com
          region: auto
          credentialsSecret: private-credentials-secret
        target: /private-target-secret
        retention: ephemeral
        sizeHintGB: 32
    persistent:
      - name: private-output-secret
        sizeGB: 100
        target: /private-output-target-secret
        retention: keep
        snapshot:
          to:
            bucket: private-artifact-bucket-secret
            credentialsSecret: private-artifact-credentials-secret
          interval: 5m
    artifacts:
      - name: private-artifact-name-secret
        target: /private-artifact-target-secret
        to:
          bucket: private-artifact-output-bucket-secret
          credentialsSecret: private-artifact-output-credentials-secret
        maxFiles: 250
        maxSizeGB: 12
`

func tenantWorkloadMux(a *Accounts) *http.ServeMux {
	mux := http.NewServeMux()
	mux.Handle("GET /v1/tenants/{tenant_id}/workloads", http.HandlerFunc(a.HandleListWorkloads))
	mux.Handle("POST /v1/tenants/{tenant_id}/workloads", http.HandlerFunc(a.HandleCreateWorkload))
	mux.Handle("GET /v1/tenants/{tenant_id}/workloads/{id}", http.HandlerFunc(a.HandleGetWorkload))
	mux.Handle("GET /v1/tenants/{tenant_id}/workloads/{id}/logs", http.HandlerFunc(a.HandleGetWorkloadLogs))
	mux.Handle("POST /v1/tenants/{tenant_id}/workloads/{id}/retry", http.HandlerFunc(a.HandleRetryWorkload))
	mux.Handle("DELETE /v1/tenants/{tenant_id}/workloads/{id}", http.HandlerFunc(a.HandleCancelWorkload))
	return mux
}

// tenantWorkloadKeys hands each human call its own Idempotency-Key. The human
// submit path requires one, and a FRESH one per call is what keeps these tests
// meaning what they meant before it existed: every call here is a distinct
// submission, not a retry of the last. Idempotent behaviour under a REUSED key
// is its own suite (workloads_idempotency_test.go).
var tenantWorkloadKeys atomic.Int64

func callTenantWorkload(a *Accounts, method, path, token, body string) *httptest.ResponseRecorder {
	return callTenantWorkloadWithKey(a, method, path, token, body, fmt.Sprintf("test-submission-%d", tenantWorkloadKeys.Add(1)), "")
}

func callTenantWorkloadWithKey(a *Accounts, method, path, token, body, key, clusterID string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	if key != "" {
		req.Header.Set("Idempotency-Key", key)
	}
	if clusterID != "" {
		req.Header.Set("X-Cluster-ID", clusterID)
	}
	rec := httptest.NewRecorder()
	tenantWorkloadMux(a).ServeHTTP(rec, req)
	return rec
}

func tenantWorkloadFixture(t *testing.T, roles map[string]string) (*state.Store, *Accounts, *fakeDecider, *fakeReaper) {
	t.Helper()
	store := state.New()
	// tenantWorkloadYAML submits into "jobs", so the tenant has to be
	// authorized for it — central pins metadata.namespace to this set.
	store.AddCustomer(&state.Customer{ID: "cust_console", Token: "ysk_cluster_secret", Plan: "pro",
		MaxConcurrentBursts: 20, MaxHourlyUSD: 100, WorkloadNamespaces: []string{"jobs"}})
	store.AddAgent(&state.Agent{
		ID: "agent_console", CustomerID: "cust_console", ClusterID: "cluster_console",
		Send: make(chan protocol.Envelope, 256),
	})
	accounts, _ := rosterFixture(t, store, "cust_console", roles)
	decider, reaper := &fakeDecider{}, &fakeReaper{}
	accounts.Workloads = &Workloads{Store: store, Decider: decider, Reaper: reaper, Log: quietLog()}
	return store, accounts, decider, reaper
}

func putTenantWorkload(store *state.Store, id, customerID, burstID string, created time.Time) {
	store.PutWorkload(&state.Workload{
		ID: id, CustomerID: customerID, ClusterID: "cluster_console", BurstID: burstID, Status: "running",
		CreatedAt: created, SpecYAML: []byte(tenantWorkloadYAML),
	})
	store.PutBurst(&state.Burst{ID: burstID, CustomerID: customerID, CreatedAt: created})
}

func putTerminalTenantWorkload(store *state.Store, id string, submittedBy *state.Actor, placement *state.WorkloadPlacement) {
	finished := time.Now().UTC().Add(-time.Minute)
	store.PutWorkload(&state.Workload{
		ID:          id,
		CustomerID:  "cust_console",
		ClusterID:   "cluster_console",
		BurstID:     "burst_" + id,
		Status:      "succeeded",
		CreatedAt:   finished.Add(-time.Hour),
		FinishedAt:  &finished,
		SpecYAML:    []byte(tenantWorkloadYAML),
		SubmittedBy: submittedBy,
		Placement:   placement,
	})
}

func decodeTenantWorkloads(t *testing.T, rec *httptest.ResponseRecorder) TenantWorkloadsResponse {
	t.Helper()
	var response TenantWorkloadsResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode tenant workloads: %v (body %q)", err, rec.Body.String())
	}
	return response
}

func TestTenantWorkloadRolePermissionsAndLifecycleReuse(t *testing.T) {
	roles := []struct {
		role     string
		mutateOK bool
		// cancelSeedOK is separate from mutateOK because cancelling is the one
		// verb where a member is narrower than an owner: the seeded workload
		// carries no recorded submitter, so nothing proves it was theirs and a
		// member is refused it. Owner and admin may cancel any of the tenant's.
		cancelSeedOK bool
	}{
		{state.RoleOwner, true, true},
		{state.RoleAdmin, true, true},
		{state.RoleMember, true, false},
		{state.RoleViewer, false, false},
	}
	for _, tc := range roles {
		t.Run(tc.role, func(t *testing.T) {
			store, accounts, decider, reaper := tenantWorkloadFixture(t, map[string]string{tc.role: tc.role})
			seedID, burstID := "wl_seed_"+tc.role, "burst_seed_"+tc.role
			putTenantWorkload(store, seedID, "cust_console", burstID, time.Now().UTC())
			token := "human_" + tc.role

			if rec := callTenantWorkload(accounts, http.MethodGet, "/v1/tenants/cust_console/workloads", token, ""); rec.Code != http.StatusOK {
				t.Fatalf("list status = %d, want 200 (body %q)", rec.Code, rec.Body.String())
			}
			if rec := callTenantWorkload(accounts, http.MethodGet, "/v1/tenants/cust_console/workloads/"+seedID, token, ""); rec.Code != http.StatusOK {
				t.Fatalf("get status = %d, want 200 (body %q)", rec.Code, rec.Body.String())
			}

			created := callTenantWorkload(accounts, http.MethodPost, "/v1/tenants/cust_console/workloads", token, tenantWorkloadYAML)
			cancelled := callTenantWorkload(accounts, http.MethodDelete, "/v1/tenants/cust_console/workloads/"+seedID, token, "")
			if !tc.mutateOK {
				if created.Code != http.StatusForbidden || cancelled.Code != http.StatusForbidden {
					t.Fatalf("viewer create/delete = %d/%d, want 403/403", created.Code, cancelled.Code)
				}
				if decider.calls != 0 || reaper.teardownCount(burstID) != 0 {
					t.Fatal("viewer reached the create or cancel lifecycle")
				}
				return
			}

			if created.Code != http.StatusAccepted {
				t.Fatalf("create status = %d, want 202 (body %q)", created.Code, created.Body.String())
			}
			var createResponse CreateWorkloadResponse
			if err := json.Unmarshal(created.Body.Bytes(), &createResponse); err != nil {
				t.Fatal(err)
			}
			createdWorkload, err := store.GetWorkload(createResponse.ID)
			if err != nil || createdWorkload.CustomerID != "cust_console" || decider.calls != 1 {
				t.Fatalf("existing create path not used: workload=%+v err=%v calls=%d", createdWorkload, err, decider.calls)
			}
			if tc.cancelSeedOK {
				if cancelled.Code != http.StatusOK || reaper.teardownCount(burstID) != 1 {
					t.Fatalf("existing cancel path not used: status=%d teardown count=%d", cancelled.Code, reaper.teardownCount(burstID))
				}
				return
			}
			// A refused cancel must leave the workload and its burst exactly as
			// they were: a 403 that tore something down would be worse than one
			// that allowed the cancel.
			if cancelled.Code != http.StatusForbidden || reaper.teardownCount(burstID) != 0 {
				t.Fatalf("member cancel of a workload they did not submit = %d, teardown count=%d; want 403 and no teardown",
					cancelled.Code, reaper.teardownCount(burstID))
			}
			seed, err := store.GetWorkload(seedID)
			if err != nil || seed.FinishedAt != nil || seed.Status != "running" {
				t.Fatalf("refused cancel changed the workload: %+v err=%v", seed, err)
			}
		})
	}
}

func TestTenantWorkloadLogsRoleScopeRoutingAndAudit(t *testing.T) {
	for _, role := range []string{state.RoleOwner, state.RoleAdmin, state.RoleMember, state.RoleViewer} {
		t.Run(role, func(t *testing.T) {
			store, accounts, _, _ := tenantWorkloadFixture(t, map[string]string{role: role})
			putTenantWorkload(store, "wl_logs", "cust_console", "burst_logs", time.Now().UTC())
			recorder := &recordingAuditStore{accountStore: store}
			accounts.Store = recorder
			agent, err := store.AgentForCluster("cust_console", "cluster_console")
			if err != nil {
				t.Fatal(err)
			}
			response := make(chan *httptest.ResponseRecorder, 1)
			go func() {
				response <- callTenantWorkload(accounts, http.MethodGet, "/v1/tenants/cust_console/workloads/wl_logs/logs?tail=25", "human_"+role, "")
			}()

			var env protocol.Envelope
			select {
			case env = <-agent.Send:
			case <-time.After(time.Second):
				t.Fatal("log command was not sent to the workload cluster")
			}
			if env.Type != protocol.TypeFetchWorkloadLogs || env.ID == "" {
				t.Fatalf("command = %+v", env)
			}
			var command protocol.FetchWorkloadLogs
			if err := json.Unmarshal(env.Body, &command); err != nil {
				t.Fatal(err)
			}
			if command.WorkloadID != "wl_logs" || command.Namespace != "jobs" || command.TailLines != 25 || command.MaxBytes != maxTenantLogBytes {
				t.Fatalf("command body = %+v", command)
			}
			result, _ := json.Marshal(protocol.WorkloadLogs{
				ObservedAt: time.Now().UTC(),
				Streams:    []protocol.WorkloadLogStream{{Pod: "console-job-abc", Container: "main", Output: "ready\n"}},
			})
			if !agent.DeliverCommandAck(protocol.CommandAck{CommandID: env.ID, Success: true, Result: result}) {
				t.Fatal("log acknowledgement was not delivered")
			}
			rec := <-response
			if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"output":"ready\n"`) {
				t.Fatalf("response = %d %q", rec.Code, rec.Body.String())
			}
			events := recorder.recorded(state.ActionWorkloadLogs)
			if len(events) != 1 || events[0].Outcome != state.OutcomeAccepted || events[0].Detail.Role != role {
				t.Fatalf("audit events = %+v", events)
			}
		})
	}
}

func TestTenantWorkloadLogsRejectsUntrustedSelectorsAndBadTail(t *testing.T) {
	store, accounts, _, _ := tenantWorkloadFixture(t, map[string]string{"alice": state.RoleOwner})
	putTenantWorkload(store, "wl_logs", "cust_console", "burst_logs", time.Now().UTC())
	agent, _ := store.AgentForCluster("cust_console", "cluster_console")

	for _, path := range []string{
		"/v1/tenants/cust_console/workloads/wl_logs/logs?tail=0",
		"/v1/tenants/cust_console/workloads/wl_logs/logs?tail=1001",
		"/v1/tenants/cust_console/workloads/wl_logs/logs?tail=20&container=main",
		"/v1/tenants/cust_console/workloads/wl_logs/logs?tail=20&tail=21",
	} {
		if rec := callTenantWorkload(accounts, http.MethodGet, path, "human_alice", ""); rec.Code != http.StatusBadRequest {
			t.Fatalf("%s = %d, want 400", path, rec.Code)
		}
	}
	if rec := callTenantWorkloadWithKey(accounts, http.MethodGet, "/v1/tenants/cust_console/workloads/wl_logs/logs", "human_alice", "", "", "cluster_console"); rec.Code != http.StatusBadRequest {
		t.Fatalf("cluster selector = %d, want 400", rec.Code)
	}
	if rec := callTenantWorkload(accounts, http.MethodGet, "/v1/tenants/cust_console/workloads/wl_logs/logs", "human_alice", "x"); rec.Code != http.StatusBadRequest {
		t.Fatalf("body = %d, want 400", rec.Code)
	}
	select {
	case command := <-agent.Send:
		t.Fatalf("invalid request reached connector: %+v", command)
	default:
	}
}

func TestTenantWorkloadLogsOfflineTimeoutAgentFailureAndAuditFailure(t *testing.T) {
	t.Run("offline", func(t *testing.T) {
		store, accounts, _, _ := tenantWorkloadFixture(t, map[string]string{"alice": state.RoleOwner})
		putTenantWorkload(store, "wl_logs", "cust_console", "burst_logs", time.Now().UTC())
		store.RemoveAgent("agent_console")
		rec := callTenantWorkload(accounts, http.MethodGet, "/v1/tenants/cust_console/workloads/wl_logs/logs", "human_alice", "")
		if rec.Code != http.StatusServiceUnavailable || !strings.Contains(rec.Body.String(), "connector is offline") {
			t.Fatalf("offline = %d %q", rec.Code, rec.Body.String())
		}
	})

	t.Run("timeout", func(t *testing.T) {
		store, accounts, _, _ := tenantWorkloadFixture(t, map[string]string{"alice": state.RoleOwner})
		putTenantWorkload(store, "wl_logs", "cust_console", "burst_logs", time.Now().UTC())
		accounts.WorkloadLogTimeout = 5 * time.Millisecond
		rec := callTenantWorkload(accounts, http.MethodGet, "/v1/tenants/cust_console/workloads/wl_logs/logs", "human_alice", "")
		if rec.Code != http.StatusGatewayTimeout {
			t.Fatalf("timeout = %d %q", rec.Code, rec.Body.String())
		}
	})

	t.Run("older agent failure is sanitized", func(t *testing.T) {
		store, accounts, _, _ := tenantWorkloadFixture(t, map[string]string{"alice": state.RoleOwner})
		putTenantWorkload(store, "wl_logs", "cust_console", "burst_logs", time.Now().UTC())
		agent, _ := store.AgentForCluster("cust_console", "cluster_console")
		response := make(chan *httptest.ResponseRecorder, 1)
		go func() {
			response <- callTenantWorkload(accounts, http.MethodGet, "/v1/tenants/cust_console/workloads/wl_logs/logs", "human_alice", "")
		}()
		env := <-agent.Send
		agent.DeliverCommandAck(protocol.CommandAck{CommandID: env.ID, Error: "unknown command with internal details"})
		rec := <-response
		if rec.Code != http.StatusBadGateway || strings.Contains(rec.Body.String(), "internal details") {
			t.Fatalf("agent failure = %d %q", rec.Code, rec.Body.String())
		}
	})

	t.Run("audit failure is closed before command", func(t *testing.T) {
		store, accounts, _, _ := tenantWorkloadFixture(t, map[string]string{"alice": state.RoleOwner})
		putTenantWorkload(store, "wl_logs", "cust_console", "burst_logs", time.Now().UTC())
		agent, _ := store.AgentForCluster("cust_console", "cluster_console")
		accounts.Store = &auditFailStore{Store: store, err: fmt.Errorf("audit down")}
		rec := callTenantWorkload(accounts, http.MethodGet, "/v1/tenants/cust_console/workloads/wl_logs/logs", "human_alice", "")
		if rec.Code != http.StatusServiceUnavailable {
			t.Fatalf("audit failure = %d %q", rec.Code, rec.Body.String())
		}
		select {
		case command := <-agent.Send:
			t.Fatalf("unaudited request reached connector: %+v", command)
		default:
		}
	})
}

func TestTenantWorkloadCreateCarriesClusterPolicyThroughSyntheticCustomer(t *testing.T) {
	store, accounts, _, _ := tenantWorkloadFixture(t, map[string]string{"alice": state.RoleOwner})
	alice, err := store.AccountByIdentity(testIssuer, "alice")
	if err != nil {
		t.Fatalf("AccountByIdentity: %v", err)
	}
	store.AddAgent(&state.Agent{
		ID: "agent_console_west", CustomerID: "cust_console", ClusterID: "cluster_west",
		ConnectedAt: time.Now().UTC().Add(time.Minute), Send: make(chan protocol.Envelope, 256),
	})
	if _, _, _, err := store.SetTenantClusterPolicy("cust_console", alice.ID, state.ClusterPolicy{
		Allow: []string{"cluster_west", "cluster_console"},
		Auto:  state.ClusterPolicyAutoOrdered,
	}, state.HumanActor(alice.ID, "cust_console")); err != nil {
		t.Fatalf("SetTenantClusterPolicy: %v", err)
	}

	rec := callTenantWorkload(accounts, http.MethodPost, "/v1/tenants/cust_console/workloads", "human_alice", tenantWorkloadYAML)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("create = %d, want 202: %s", rec.Code, rec.Body)
	}
	var response CreateWorkloadResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if response.Placement == nil || response.Placement.GrantedClusterID != "cluster_west" || response.Placement.Mode != state.ClusterPlacementModeAuto {
		t.Fatalf("response placement = %+v", response.Placement)
	}
	wl, err := store.GetWorkload(response.ID)
	if err != nil {
		t.Fatalf("GetWorkload: %v", err)
	}
	if wl.ClusterID != "cluster_west" || wl.Placement == nil || wl.Placement.GrantedClusterID != "cluster_west" {
		t.Fatalf("human submission ignored cluster policy: cluster=%q placement=%+v", wl.ClusterID, wl.Placement)
	}
}

func TestTenantWorkloadHostedPlacementIsExplicitAndNamespacePinned(t *testing.T) {
	addHosted := func(t *testing.T, store *state.Store) {
		t.Helper()
		if _, _, err := store.AssignHostedCluster("cust_console", "hosted-console", "Yscale hosted", "ys-console", state.OperatorActor()); err != nil {
			t.Fatalf("AssignHostedCluster: %v", err)
		}
		store.AddAgent(&state.Agent{
			ID: "agent_hosted_console", CustomerID: "cust_console", ClusterID: "hosted-console",
			ConnectedAt: time.Now().UTC().Add(2 * time.Minute), Send: make(chan protocol.Envelope, 256),
		})
	}

	t.Run("explicit target refuses another authorized namespace before planning", func(t *testing.T) {
		store, accounts, decider, _ := tenantWorkloadFixture(t, map[string]string{"alice": state.RoleOwner})
		addHosted(t, store)

		rec := callTenantWorkloadWithKey(accounts, http.MethodPost,
			"/v1/tenants/cust_console/workloads", "human_alice", tenantWorkloadYAML,
			"hosted-wrong-namespace", "hosted-console")
		if rec.Code != http.StatusConflict || !strings.Contains(rec.Body.String(), "reserved namespace ys-console") {
			t.Fatalf("hosted namespace refusal = %d %q", rec.Code, rec.Body.String())
		}
		if decider.calls != 0 {
			t.Fatalf("Plan calls = %d, want 0", decider.calls)
		}
	})

	t.Run("explicit target accepts its reserved namespace", func(t *testing.T) {
		store, accounts, decider, _ := tenantWorkloadFixture(t, map[string]string{"alice": state.RoleOwner})
		addHosted(t, store)
		body := strings.Replace(tenantWorkloadYAML, "namespace: jobs", "namespace: ys-console", 1)

		rec := callTenantWorkloadWithKey(accounts, http.MethodPost,
			"/v1/tenants/cust_console/workloads", "human_alice", body,
			"hosted-right-namespace", "hosted-console")
		if rec.Code != http.StatusAccepted {
			t.Fatalf("hosted create = %d, want 202: %s", rec.Code, rec.Body.String())
		}
		if decider.calls != 1 {
			t.Fatalf("Plan calls = %d, want 1", decider.calls)
		}
		var response CreateWorkloadResponse
		if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
			t.Fatal(err)
		}
		wl, err := store.GetWorkload(response.ID)
		if err != nil {
			t.Fatal(err)
		}
		if wl.ClusterID != "hosted-console" || wl.Placement == nil || wl.Placement.Mode != state.ClusterPlacementModePinned {
			t.Fatalf("hosted placement = cluster %q, placement %+v", wl.ClusterID, wl.Placement)
		}
	})

	t.Run("ordered automatic routing skips hosted capacity", func(t *testing.T) {
		store, accounts, _, _ := tenantWorkloadFixture(t, map[string]string{"alice": state.RoleOwner})
		addHosted(t, store)
		alice, err := store.AccountByIdentity(testIssuer, "alice")
		if err != nil {
			t.Fatal(err)
		}
		if _, _, _, err := store.SetTenantClusterPolicy("cust_console", alice.ID, state.ClusterPolicy{
			Allow: []string{"hosted-console", "cluster_console"},
			Auto:  state.ClusterPolicyAutoOrdered,
		}, state.HumanActor(alice.ID, "cust_console")); err != nil {
			t.Fatalf("SetTenantClusterPolicy: %v", err)
		}

		rec := callTenantWorkload(accounts, http.MethodPost, "/v1/tenants/cust_console/workloads", "human_alice", tenantWorkloadYAML)
		if rec.Code != http.StatusAccepted {
			t.Fatalf("automatic create = %d, want 202: %s", rec.Code, rec.Body.String())
		}
		var response CreateWorkloadResponse
		if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
			t.Fatal(err)
		}
		if response.Placement == nil || response.Placement.GrantedClusterID != "cluster_console" {
			t.Fatalf("automatic placement = %+v, want tenant-owned cluster", response.Placement)
		}
	})

	t.Run("hosted-only automatic policy requires an explicit target", func(t *testing.T) {
		store, accounts, decider, _ := tenantWorkloadFixture(t, map[string]string{"alice": state.RoleOwner})
		addHosted(t, store)
		alice, err := store.AccountByIdentity(testIssuer, "alice")
		if err != nil {
			t.Fatal(err)
		}
		if _, _, _, err := store.SetTenantClusterPolicy("cust_console", alice.ID, state.ClusterPolicy{
			Allow: []string{"hosted-console"},
			Auto:  state.ClusterPolicyAutoOrdered,
		}, state.HumanActor(alice.ID, "cust_console")); err != nil {
			t.Fatalf("SetTenantClusterPolicy: %v", err)
		}

		rec := callTenantWorkload(accounts, http.MethodPost, "/v1/tenants/cust_console/workloads", "human_alice", tenantWorkloadYAML)
		if rec.Code != http.StatusConflict || !strings.Contains(rec.Body.String(), "require X-Cluster-ID") {
			t.Fatalf("hosted automatic refusal = %d %q", rec.Code, rec.Body.String())
		}
		if decider.calls != 0 {
			t.Fatalf("Plan calls = %d, want 0", decider.calls)
		}
	})

	t.Run("no policy and a lone hosted connector still requires an explicit target", func(t *testing.T) {
		store, accounts, decider, _ := tenantWorkloadFixture(t, map[string]string{"alice": state.RoleOwner})
		addHosted(t, store)
		// A fresh hosted-only enterprise tenant: no cluster policy, and the
		// hosted release is the ONE connected agent — so routing reaches the
		// unambiguous single-agent branch, not the policy filter, and that
		// branch has to refuse hosted on its own.
		store.RemoveAgent("agent_console")

		rec := callTenantWorkload(accounts, http.MethodPost, "/v1/tenants/cust_console/workloads", "human_alice", tenantWorkloadYAML)
		if rec.Code != http.StatusConflict || !strings.Contains(rec.Body.String(), "require X-Cluster-ID") {
			t.Fatalf("no-policy hosted refusal = %d %q", rec.Code, rec.Body.String())
		}
		if decider.calls != 0 {
			t.Fatalf("Plan calls = %d, want 0", decider.calls)
		}
	})
}

func TestTenantRetryAuthorizationTerminalProvenanceAndAudit(t *testing.T) {
	store, accounts, decider, _ := tenantWorkloadFixture(t, map[string]string{
		"alice": state.RoleOwner, "admin": state.RoleAdmin, "mem": state.RoleMember, "other": state.RoleMember, "view": state.RoleViewer,
	})
	mem, err := store.AccountByIdentity(testIssuer, "mem")
	if err != nil {
		t.Fatal(err)
	}
	memActor := state.HumanActor(mem.ID, "cust_console")
	putTerminalTenantWorkload(store, "wl_retry_source", &memActor, &state.WorkloadPlacement{
		RequestedClusterID: "cluster_console",
		GrantedClusterID:   "cluster_console",
		Mode:               state.ClusterPlacementModePinned,
	})
	retryAudit := &recordingAuditStore{accountStore: store}
	submitAudit := &failingJournal{Store: store}
	accounts.Store = retryAudit
	accounts.Workloads.Journal = submitAudit

	for _, token := range []string{"human_alice", "human_admin", "human_mem"} {
		rec := callTenantWorkload(accounts, http.MethodPost, "/v1/tenants/cust_console/workloads/wl_retry_source/retry", token, "")
		if rec.Code != http.StatusAccepted {
			t.Fatalf("%s retry = %d, want 202: %s", token, rec.Code, rec.Body.String())
		}
		var response CreateWorkloadResponse
		if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
			t.Fatal(err)
		}
		wl, err := store.GetWorkload(response.ID)
		if err != nil {
			t.Fatalf("GetWorkload(%s): %v", response.ID, err)
		}
		if wl.RetryOfWorkloadID != "wl_retry_source" || wl.SubmittedBy == nil || wl.SubmittedBy.Kind != state.ActorHuman {
			t.Fatalf("retry provenance = retryOf %q submittedBy %+v", wl.RetryOfWorkloadID, wl.SubmittedBy)
		}
	}
	for _, token := range []string{"human_other", "human_view"} {
		rec := callTenantWorkload(accounts, http.MethodPost, "/v1/tenants/cust_console/workloads/wl_retry_source/retry", token, "")
		if rec.Code != http.StatusForbidden {
			t.Fatalf("%s retry = %d, want 403: %s", token, rec.Code, rec.Body.String())
		}
	}
	if decider.calls != 3 {
		t.Fatalf("Plan calls = %d, want only the three authorized retries", decider.calls)
	}
	if got := len(retryAudit.recorded(state.ActionWorkloadRetry)); got != 5 {
		t.Fatalf("workload.retry audit rows = %d, want accepted+denied rows", got)
	}
	if got := len(submitAudit.recorded(state.ActionWorkloadSubmit)); got != 3 {
		t.Fatalf("workload.submit audit rows = %d, want one per new retry run", got)
	}
	list := decodeTenantWorkloads(t, callTenantWorkload(accounts, http.MethodGet, "/v1/tenants/cust_console/workloads", "human_alice", ""))
	found := false
	for _, workload := range list.Workloads {
		if workload.RetryOf == "wl_retry_source" {
			found = true
		}
	}
	if !found {
		t.Fatalf("tenant workload list did not expose retry_of: %+v", list.Workloads)
	}
}

func TestTenantRetryRequiresTerminalSourceAndCollapsesMisses(t *testing.T) {
	store, accounts, decider, _ := tenantWorkloadFixture(t, map[string]string{"alice": state.RoleOwner})
	retryAudit := &recordingAuditStore{accountStore: store}
	accounts.Store = retryAudit
	putTenantWorkload(store, "wl_running", "cust_console", "burst_running", time.Now().UTC())
	if rec := callTenantWorkload(accounts, http.MethodPost, "/v1/tenants/cust_console/workloads/wl_running/retry", "human_alice", ""); rec.Code != http.StatusConflict {
		t.Fatalf("running retry = %d, want 409: %s", rec.Code, rec.Body.String())
	}
	denied := retryAudit.recorded(state.ActionWorkloadRetry)
	if len(denied) != 1 || denied[0].Outcome != state.OutcomeDenied || denied[0].Detail.Reason != state.ReasonWorkloadNotTerminal {
		t.Fatalf("non-terminal retry audit = %+v, want one workload_not_terminal denial", denied)
	}
	store.AddCustomer(&state.Customer{ID: "cust_other", Token: "ysk_other", Plan: "pro"})
	putTenantWorkload(store, "wl_other", "cust_other", "burst_other", time.Now().UTC())
	for _, path := range []string{
		"/v1/tenants/cust_console/workloads/wl_missing/retry",
		"/v1/tenants/cust_console/workloads/wl_other/retry",
		"/v1/tenants/cust_missing/workloads/wl_running/retry",
	} {
		if rec := callTenantWorkload(accounts, http.MethodPost, path, "human_alice", ""); rec.Code != http.StatusNotFound {
			t.Fatalf("%s = %d, want 404: %s", path, rec.Code, rec.Body.String())
		}
	}
	if decider.calls != 0 {
		t.Fatalf("Plan calls = %d, want 0", decider.calls)
	}
}

func TestTenantRetryFailsClosedWhenAuditWriteFails(t *testing.T) {
	store, accounts, decider, _ := tenantWorkloadFixture(t, map[string]string{"alice": state.RoleOwner})
	putTerminalTenantWorkload(store, "wl_retry_fc", nil, nil)
	accounts.Store = &auditFailStore{
		Store: store,
		err:   fmt.Errorf("%w: append audit: down", state.ErrPersistence),
	}

	rec := callTenantWorkload(accounts, http.MethodPost, "/v1/tenants/cust_console/workloads/wl_retry_fc/retry", "human_alice", "")
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("retry with dead audit = %d, want 503: %s", rec.Code, rec.Body.String())
	}
	if decider.calls != 0 {
		t.Fatalf("Plan calls = %d, want 0 before audited authorization", decider.calls)
	}
}

func TestTenantRetryRejectsBodyClusterHeaderAndInvalidIdempotency(t *testing.T) {
	store, accounts, decider, _ := tenantWorkloadFixture(t, map[string]string{"alice": state.RoleOwner})
	putTerminalTenantWorkload(store, "wl_retry_headers", nil, nil)
	path := "/v1/tenants/cust_console/workloads/wl_retry_headers/retry"
	cases := []struct {
		name      string
		body      string
		key       string
		clusterID string
	}{
		{name: "body", body: "{}", key: "retry-key-headers-1"},
		{name: "missing key"},
		{name: "bad key", key: "bad key"},
		{name: "cluster header", key: "retry-key-headers-2", clusterID: "cluster_console"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := callTenantWorkloadWithKey(accounts, http.MethodPost, path, "human_alice", tc.body, tc.key, tc.clusterID)
			if rec.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400: %s", rec.Code, rec.Body.String())
			}
		})
	}
	if decider.calls != 0 {
		t.Fatalf("Plan calls = %d, want 0", decider.calls)
	}
}

func TestTenantRetryReevaluatesCurrentPlacementPolicyAndPreservesPins(t *testing.T) {
	store, accounts, _, _ := tenantWorkloadFixture(t, map[string]string{"alice": state.RoleOwner})
	alice, err := store.AccountByIdentity(testIssuer, "alice")
	if err != nil {
		t.Fatal(err)
	}
	store.AddAgent(&state.Agent{
		ID: "agent_console_west", CustomerID: "cust_console", ClusterID: "cluster_west",
		ConnectedAt: time.Now().UTC().Add(time.Minute), Send: make(chan protocol.Envelope, 256),
	})
	if _, _, _, err := store.SetTenantClusterPolicy("cust_console", alice.ID, state.ClusterPolicy{
		Allow: []string{"cluster_west", "cluster_console"},
		Auto:  state.ClusterPolicyAutoOrdered,
	}, state.HumanActor(alice.ID, "cust_console")); err != nil {
		t.Fatalf("SetTenantClusterPolicy: %v", err)
	}
	putTerminalTenantWorkload(store, "wl_auto_source", nil, &state.WorkloadPlacement{
		GrantedClusterID: "cluster_console",
		Mode:             state.ClusterPlacementModeAuto,
	})
	putTerminalTenantWorkload(store, "wl_pinned_source", nil, &state.WorkloadPlacement{
		RequestedClusterID: "cluster_console",
		GrantedClusterID:   "cluster_console",
		Mode:               state.ClusterPlacementModePinned,
	})

	auto := callTenantWorkload(accounts, http.MethodPost, "/v1/tenants/cust_console/workloads/wl_auto_source/retry", "human_alice", "")
	pinned := callTenantWorkload(accounts, http.MethodPost, "/v1/tenants/cust_console/workloads/wl_pinned_source/retry", "human_alice", "")
	for name, rec := range map[string]*httptest.ResponseRecorder{"auto": auto, "pinned": pinned} {
		if rec.Code != http.StatusAccepted {
			t.Fatalf("%s retry = %d, want 202: %s", name, rec.Code, rec.Body.String())
		}
	}
	var autoResp, pinnedResp CreateWorkloadResponse
	if err := json.Unmarshal(auto.Body.Bytes(), &autoResp); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(pinned.Body.Bytes(), &pinnedResp); err != nil {
		t.Fatal(err)
	}
	autoWL, _ := store.GetWorkload(autoResp.ID)
	pinnedWL, _ := store.GetWorkload(pinnedResp.ID)
	if autoWL.ClusterID != "cluster_west" || pinnedWL.ClusterID != "cluster_console" {
		t.Fatalf("retry placement auto=%q pinned=%q, want west/console", autoWL.ClusterID, pinnedWL.ClusterID)
	}
}

func TestTenantRetryExplainsWhenCurrentPolicyNeedsANewPinnedRun(t *testing.T) {
	store, accounts, decider, _ := tenantWorkloadFixture(t, map[string]string{"alice": state.RoleOwner})
	alice, err := store.AccountByIdentity(testIssuer, "alice")
	if err != nil {
		t.Fatal(err)
	}
	store.AddAgent(&state.Agent{
		ID: "agent_console_west", CustomerID: "cust_console", ClusterID: "cluster_west",
		ConnectedAt: time.Now().UTC().Add(time.Minute), Send: make(chan protocol.Envelope, 256),
	})
	putTerminalTenantWorkload(store, "wl_auto_ambiguous", nil, &state.WorkloadPlacement{
		GrantedClusterID: "cluster_console",
		Mode:             state.ClusterPlacementModeAuto,
	})
	path := "/v1/tenants/cust_console/workloads/wl_auto_ambiguous/retry"
	key := "retry-policy-change-01"
	blocked := callTenantWorkloadWithKey(accounts, http.MethodPost, path, "human_alice", "", key, "")
	if blocked.Code != http.StatusConflict {
		t.Fatalf("ambiguous retry = %d, want 409: %s", blocked.Code, blocked.Body.String())
	}
	var refusal CreateWorkloadResponse
	if err := json.Unmarshal(blocked.Body.Bytes(), &refusal); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(refusal.Message, "X-Cluster-ID") || !strings.Contains(refusal.Message, "start a new workload") {
		t.Fatalf("retry refusal = %q, want an actionable message that does not request a forbidden header", refusal.Message)
	}
	if decider.calls != 0 {
		t.Fatalf("Plan calls = %d, want 0 while placement is ambiguous", decider.calls)
	}
	if _, _, _, err := store.SetTenantClusterPolicy("cust_console", alice.ID, state.ClusterPolicy{
		Allow: []string{"cluster_west", "cluster_console"},
		Auto:  state.ClusterPolicyAutoOrdered,
	}, state.HumanActor(alice.ID, "cust_console")); err != nil {
		t.Fatalf("SetTenantClusterPolicy: %v", err)
	}
	accepted := callTenantWorkloadWithKey(accounts, http.MethodPost, path, "human_alice", "", key, "")
	if accepted.Code != http.StatusAccepted {
		t.Fatalf("same-key retry after policy fix = %d, want 202: %s", accepted.Code, accepted.Body.String())
	}
	if decider.calls != 1 {
		t.Fatalf("Plan calls = %d, want 1 after policy became automatic", decider.calls)
	}
}

func TestTenantRetryIdempotencyScopesRequestHashBySourceWorkload(t *testing.T) {
	store, accounts, decider, _ := tenantWorkloadFixture(t, map[string]string{"alice": state.RoleOwner})
	putTerminalTenantWorkload(store, "wl_source_one", nil, nil)
	putTerminalTenantWorkload(store, "wl_source_two", nil, nil)

	first := callTenantWorkloadWithKey(accounts, http.MethodPost, "/v1/tenants/cust_console/workloads/wl_source_one/retry", "human_alice", "", "same-retry-key-01", "")
	if first.Code != http.StatusAccepted {
		t.Fatalf("first retry = %d, want 202: %s", first.Code, first.Body.String())
	}
	replay := callTenantWorkloadWithKey(accounts, http.MethodPost, "/v1/tenants/cust_console/workloads/wl_source_one/retry", "human_alice", "", "same-retry-key-01", "")
	if replay.Code != http.StatusAccepted || replay.Header().Get("Idempotency-Replayed") != "true" {
		t.Fatalf("same source replay = %d replay=%q body=%s", replay.Code, replay.Header().Get("Idempotency-Replayed"), replay.Body.String())
	}
	conflict := callTenantWorkloadWithKey(accounts, http.MethodPost, "/v1/tenants/cust_console/workloads/wl_source_two/retry", "human_alice", "", "same-retry-key-01", "")
	if conflict.Code != http.StatusConflict {
		t.Fatalf("different source same key = %d, want 409: %s", conflict.Code, conflict.Body.String())
	}
	if decider.calls != 1 {
		t.Fatalf("Plan calls = %d, want 1", decider.calls)
	}
}

func TestTenantWorkloadRoutesHideUnknownRevokedAndNonMemberIdentically(t *testing.T) {
	store, accounts, _, _ := tenantWorkloadFixture(t, map[string]string{"alice": state.RoleOwner})
	store.AddCustomer(&state.Customer{ID: "cust_revoked", Token: "ysk_revoked", Plan: "pro"})
	store.AddCustomer(&state.Customer{ID: "cust_other", Token: "ysk_other", Plan: "pro"})
	alice, err := store.AccountByIdentity(testIssuer, "alice")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.AddTenantMembership(alice.ID, "cust_revoked", state.RoleOwner); err != nil {
		t.Fatal(err)
	}
	if err := store.RevokeCustomer("cust_revoked"); err != nil {
		t.Fatal(err)
	}

	targets := []string{"cust_unknown", "cust_revoked", "cust_other"}
	requests := []struct {
		method string
		suffix string
		body   string
	}{
		{http.MethodGet, "/workloads", ""},
		{http.MethodPost, "/workloads", tenantWorkloadYAML},
		{http.MethodGet, "/workloads/wl_x", ""},
		{http.MethodGet, "/workloads/wl_x/logs", ""},
		{http.MethodDelete, "/workloads/wl_x", ""},
	}
	var wantBody string
	for _, tenantID := range targets {
		for _, request := range requests {
			rec := callTenantWorkload(accounts, request.method, "/v1/tenants/"+tenantID+request.suffix, "human_alice", request.body)
			if rec.Code != http.StatusNotFound {
				t.Fatalf("%s %s = %d, want 404 (body %q)", request.method, tenantID, rec.Code, rec.Body.String())
			}
			if wantBody == "" {
				wantBody = rec.Body.String()
			} else if rec.Body.String() != wantBody {
				t.Fatalf("%s %s body %q differs from uniform 404 %q", request.method, tenantID, rec.Body.String(), wantBody)
			}
		}
	}

	newcomer := &Accounts{
		Store: store, Resolver: idProvider(t, map[string]string{"human_new": "new"}), Issuer: testIssuer,
		Log: quietLog(), Workloads: accounts.Workloads,
	}
	rec := callTenantWorkload(newcomer, http.MethodGet, "/v1/tenants/cust_console/workloads", "human_new", "")
	if rec.Code != http.StatusNotFound || rec.Body.String() != wantBody {
		t.Fatalf("unknown account = %d %q, want uniform 404 %q", rec.Code, rec.Body.String(), wantBody)
	}
	if _, err := store.AccountByIdentity(testIssuer, "new"); err == nil {
		t.Fatal("tenant workload management minted an account")
	}
}

func TestTenantWorkloadListIsIsolatedStableAndSafe(t *testing.T) {
	store, accounts, _, _ := tenantWorkloadFixture(t, map[string]string{"alice": state.RoleOwner})
	store.AddCustomer(&state.Customer{ID: "cust_other", Token: "ysk_other_secret", Plan: "pro"})
	base := time.Date(2026, 8, 12, 10, 0, 0, 0, time.UTC)
	putTenantWorkload(store, "wl_b", "cust_console", "burst_b", base)
	putTenantWorkload(store, "wl_a", "cust_console", "burst_a", base)
	putTenantWorkload(store, "wl_new", "cust_console", "burst_new", base.Add(time.Minute))
	putTenantWorkload(store, "wl_other", "cust_other", "burst_other", base.Add(time.Hour))

	rec := callTenantWorkload(accounts, http.MethodGet, "/v1/tenants/cust_console/workloads", "human_alice", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("list status = %d, want 200 (body %q)", rec.Code, rec.Body.String())
	}
	response := decodeTenantWorkloads(t, rec)
	if len(response.Workloads) != 3 {
		t.Fatalf("workloads = %+v, want three tenant records", response.Workloads)
	}
	if response.Workloads[0].ID != "wl_new" || response.Workloads[1].ID != "wl_a" || response.Workloads[2].ID != "wl_b" {
		t.Fatalf("order = [%s %s %s], want newest then ID", response.Workloads[0].ID, response.Workloads[1].ID, response.Workloads[2].ID)
	}
	if response.Workloads[0].ClusterID != "cluster_console" {
		t.Fatalf("cluster_id = %q, want cluster_console", response.Workloads[0].ClusterID)
	}
	if response.Workloads[0].Spec.Metadata.Name != "console-job" || response.Workloads[0].Spec.Spec.Image != "busybox:latest" {
		t.Fatalf("safe spec summary missing fields: %+v", response.Workloads[0].Spec)
	}
	if got := response.Workloads[0].Spec.Spec.Execution; got == nil || got.Mode != "custom" || got.CommandTokens != 2 || got.ArgumentTokens != 1 {
		t.Fatalf("safe execution summary = %+v, want custom 2/1 token counts", got)
	}
	if got := response.Workloads[0].Spec.Spec.Budget; got == nil || got.Deadline != "5m0s" || got.MaxUSD != 2.5 {
		t.Fatalf("safe budget summary = %+v, want maxUSD 2.5 and deadline 5m0s", got)
	}
	if got := response.Workloads[0].Spec.Spec.Data; got == nil || got.PullBeforeRunInputs != 1 || got.InputSizeHintGB != 32 || got.PushAfterRunOutputs != 1 || got.OutputMaxUploadGB != 12 {
		t.Fatalf("safe data summary = %+v", got)
	}
	for _, forbidden := range []string{
		"wl_other", "ysk_cluster_secret", "ysk_other_secret", "label-secret", "tag-secret", "arg-secret", "env-secret",
		"private-env-secret-ref", "private-env-secret-key", "private-env-config-ref", "private-env-config-key",
		"private-data-secret", "private-bucket-secret", "private-prefix-secret", "account-secret", "private-credentials-secret",
		"private-target-secret", "private-output-secret", "private-output-target-secret", "private-artifact-bucket-secret", "private-artifact-credentials-secret",
		"private-artifact-name-secret", "private-artifact-target-secret", "private-artifact-output-bucket-secret", "private-artifact-output-credentials-secret",
	} {
		if strings.Contains(rec.Body.String(), forbidden) {
			t.Fatalf("list leaked %q: %s", forbidden, rec.Body.String())
		}
	}

	detail := callTenantWorkload(accounts, http.MethodGet, "/v1/tenants/cust_console/workloads/wl_b", "human_alice", "")
	if detail.Code != http.StatusOK {
		t.Fatalf("get status = %d, want 200 (body %q)", detail.Code, detail.Body.String())
	}
	var read struct {
		Spec TenantWorkloadSpec `json:"spec"`
	}
	if err := json.Unmarshal(detail.Body.Bytes(), &read); err != nil {
		t.Fatalf("decode tenant workload: %v", err)
	}
	if got := read.Spec.Spec.Execution; got == nil || got.Mode != "custom" || got.CommandTokens != 2 || got.ArgumentTokens != 1 {
		t.Fatalf("single read execution summary = %+v, want custom 2/1 token counts", got)
	}
	for _, forbidden := range []string{
		"env-secret", "private-env-secret-ref", "private-env-secret-key", "private-env-config-ref", "private-env-config-key",
	} {
		if strings.Contains(detail.Body.String(), forbidden) {
			t.Fatalf("get leaked environment detail %q: %s", forbidden, detail.Body.String())
		}
	}

	emptyStore, emptyAccounts, _, _ := tenantWorkloadFixture(t, map[string]string{"alice": state.RoleOwner})
	_ = emptyStore
	empty := callTenantWorkload(emptyAccounts, http.MethodGet, "/v1/tenants/cust_console/workloads", "human_alice", "")
	if empty.Code != http.StatusOK || strings.TrimSpace(empty.Body.String()) != `{"workloads":[]}` {
		t.Fatalf("empty list = %d %q, want 200 with workloads array", empty.Code, empty.Body.String())
	}
}

// A stored receipt has to reach the people who read the history: the list and
// the single-record read both render it, both keep the compute and export
// results separate, and a run with no receipt shows none rather than an empty
// object claiming an observation nobody made.
func TestTenantWorkloadListRendersStoredOutcome(t *testing.T) {
	store, accounts, _, _ := tenantWorkloadFixture(t, map[string]string{"alice": state.RoleOwner})
	base := time.Date(2026, 8, 12, 10, 0, 0, 0, time.UTC)
	putTenantWorkload(store, "wl_receipt", "cust_console", "burst_receipt", base.Add(time.Minute))
	putTenantWorkload(store, "wl_plain", "cust_console", "burst_plain", base)

	uploaded := int64(3)
	if !store.FinishWorkloadWithOutcome("wl_receipt", "failed", base.Add(time.Hour), false, &state.WorkloadOutcome{
		Compute: state.WorkloadComputeOutcome{Result: state.WorkloadResultSucceeded},
		Artifacts: &state.WorkloadArtifactOutcome{
			Result: state.WorkloadResultFailed, Reason: "destination rejected the upload", ObjectsUploaded: &uploaded,
		},
	}) {
		t.Fatal("FinishWorkloadWithOutcome should apply")
	}

	rec := callTenantWorkload(accounts, http.MethodGet, "/v1/tenants/cust_console/workloads", "human_alice", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("list status = %d, want 200 (body %q)", rec.Code, rec.Body.String())
	}
	response := decodeTenantWorkloads(t, rec)
	if len(response.Workloads) != 2 {
		t.Fatalf("workloads = %+v, want two records", response.Workloads)
	}
	listed := response.Workloads[0]
	if listed.ID != "wl_receipt" {
		t.Fatalf("first record = %q, want wl_receipt", listed.ID)
	}
	if listed.Outcome == nil || listed.Outcome.Compute.Result != state.WorkloadResultSucceeded {
		t.Fatalf("outcome = %+v, want the succeeded compute", listed.Outcome)
	}
	if listed.Outcome.Artifacts == nil || listed.Outcome.Artifacts.Result != state.WorkloadResultFailed ||
		listed.Outcome.Artifacts.ObjectsUploaded == nil || *listed.Outcome.Artifacts.ObjectsUploaded != 3 {
		t.Fatalf("artifacts = %+v, want the failed export with its count", listed.Outcome.Artifacts)
	}
	if listed.Outcome.Artifacts.BytesUploaded != nil {
		t.Errorf("bytes = %v, want nil rather than a fabricated zero", listed.Outcome.Artifacts.BytesUploaded)
	}
	if response.Workloads[1].Outcome != nil {
		t.Errorf("record with no receipt rendered %+v, want nothing", response.Workloads[1].Outcome)
	}

	got := callTenantWorkload(accounts, http.MethodGet, "/v1/tenants/cust_console/workloads/wl_receipt", "human_alice", "")
	if got.Code != http.StatusOK {
		t.Fatalf("get status = %d, want 200 (body %q)", got.Code, got.Body.String())
	}
	var single struct {
		Outcome *OutcomeResponse `json:"outcome"`
	}
	if err := json.Unmarshal(got.Body.Bytes(), &single); err != nil {
		t.Fatalf("decode single workload: %v (body %q)", err, got.Body.String())
	}
	if single.Outcome == nil || single.Outcome.Artifacts == nil ||
		single.Outcome.Artifacts.Result != state.WorkloadResultFailed {
		t.Fatalf("single read outcome = %+v, want the stored receipt", single.Outcome)
	}

	plain := callTenantWorkload(accounts, http.MethodGet, "/v1/tenants/cust_console/workloads/wl_plain", "human_alice", "")
	if strings.Contains(plain.Body.String(), "outcome") {
		t.Errorf("read of a record with no receipt carried one: %s", plain.Body.String())
	}
}

func TestTenantWorkloadListSkipsUnreadableHistoricalRecord(t *testing.T) {
	store, accounts, _, _ := tenantWorkloadFixture(t, map[string]string{"alice": state.RoleOwner})
	base := time.Date(2026, 8, 12, 10, 0, 0, 0, time.UTC)
	putTenantWorkload(store, "wl_valid", "cust_console", "burst_valid", base)
	store.PutWorkload(&state.Workload{
		ID: "wl_unreadable", CustomerID: "cust_console", Status: "failed",
		CreatedAt: base.Add(time.Minute), SpecYAML: []byte("not: [valid"),
	})

	rec := callTenantWorkload(accounts, http.MethodGet, "/v1/tenants/cust_console/workloads", "human_alice", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("list status = %d, want 200 (body %q)", rec.Code, rec.Body.String())
	}
	response := decodeTenantWorkloads(t, rec)
	if len(response.Workloads) != 1 || response.Workloads[0].ID != "wl_valid" {
		t.Fatalf("workloads = %+v, want only readable historical record", response.Workloads)
	}
}

func TestTenantWorkloadGetAndCancelPreserveOwnershipCheck(t *testing.T) {
	store, accounts, _, reaper := tenantWorkloadFixture(t, map[string]string{"alice": state.RoleOwner})
	store.AddCustomer(&state.Customer{ID: "cust_other", Token: "ysk_other", Plan: "pro"})
	putTenantWorkload(store, "wl_other", "cust_other", "burst_other", time.Now().UTC())

	get := callTenantWorkload(accounts, http.MethodGet, "/v1/tenants/cust_console/workloads/wl_other", "human_alice", "")
	cancel := callTenantWorkload(accounts, http.MethodDelete, "/v1/tenants/cust_console/workloads/wl_other", "human_alice", "")
	if get.Code != http.StatusNotFound || cancel.Code != http.StatusNotFound {
		t.Fatalf("cross-tenant get/cancel = %d/%d, want 404/404", get.Code, cancel.Code)
	}
	if reaper.teardownCount("burst_other") != 0 {
		t.Fatal("cross-tenant cancel reached teardown")
	}
	if _, err := store.GetBurst("burst_other"); err != nil {
		t.Fatal("cross-tenant cancel removed the burst")
	}
}

func TestTenantWorkloadCredentialTypesNeverCross(t *testing.T) {
	store, accounts, _, _ := tenantWorkloadFixture(t, map[string]string{"alice": state.RoleOwner})
	clusterToken := "ysk_cluster_secret"
	if rec := callTenantWorkload(accounts, http.MethodGet, "/v1/tenants/cust_console/workloads", clusterToken, ""); rec.Code != http.StatusUnauthorized {
		t.Fatalf("cluster token on human route = %d, want 401", rec.Code)
	}

	req := httptest.NewRequest(http.MethodGet, "/v1/workloads/wl_x", nil)
	req.SetPathValue("id", "wl_x")
	req.Header.Set("Authorization", "Bearer human_alice")
	rec := httptest.NewRecorder()
	Auth(store, http.HandlerFunc(accounts.Workloads.Get)).ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("human token on cluster route = %d, want 401", rec.Code)
	}
}

// The human surface reaches the preview through the same tenant scoping every
// other route on it uses, and a read-only seat may ask what a run would cost.
// This test belongs with the managed account surface: keeping it out of the
// public placement suite prevents the OSS export from depending on private
// account fixtures that are deliberately stripped.
func TestTenantPlacementPreviewRouteIsTenantScoped(t *testing.T) {
	store, accounts, _, _ := tenantWorkloadFixture(t, map[string]string{
		"owner": state.RoleOwner, "viewer": state.RoleViewer, "outsider": state.RoleOwner,
	})
	accounts.Workloads.Decider = &receiptDecider{}
	signer, err := state.NewPlacementTokenSigner(previewSigningKey)
	if err != nil {
		t.Fatalf("NewPlacementTokenSigner: %v", err)
	}
	accounts.Workloads.PlacementTokens = signer
	store.AddCustomer(&state.Customer{ID: "cust_elsewhere", Token: "ysk_elsewhere"})

	mux := http.NewServeMux()
	mux.Handle("POST /v1/tenants/{tenant_id}/placement-preview", http.HandlerFunc(accounts.HandlePreviewPlacement))
	call := func(token, tenant string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodPost, "/v1/tenants/"+tenant+"/placement-preview",
			strings.NewReader(previewSpec))
		req.Header.Set("Authorization", "Bearer "+token)
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, req)
		return rec
	}

	owner := call("human_owner", "cust_console")
	if owner.Code != http.StatusOK {
		t.Fatalf("owner preview = %d: %s", owner.Code, owner.Body)
	}
	issued := decodePreview(t, owner)
	claims, err := signer.Verify(issued.LaunchToken)
	if err != nil {
		t.Fatalf("the tenant route issued a token that does not verify: %v", err)
	}
	if claims.Tenant != "cust_console" {
		t.Fatalf("token tenant = %q, want the scoped tenant", claims.Tenant)
	}
	if rec := call("human_viewer", "cust_console"); rec.Code != http.StatusOK {
		t.Fatalf("viewer preview = %d, want 200 — a preview decides and spends nothing: %s", rec.Code, rec.Body)
	}
	if rec := call("human_owner", "cust_elsewhere"); rec.Code != http.StatusNotFound {
		t.Fatalf("preview of another tenant = %d, want 404: %s", rec.Code, rec.Body)
	}
	if rec := call("", "cust_console"); rec.Code != http.StatusUnauthorized {
		t.Fatalf("unauthenticated preview = %d, want 401", rec.Code)
	}
	if got := len(store.BurstsForCustomer("cust_console")); got != 0 {
		t.Fatalf("previews left %d burst(s)", got)
	}
}

// The tenant workload list surfaces node_observation when the workload carries
// one, and omits it on records that predate the feature or whose burst has not
// registered. Both surfaces (list and single GET) use the same converter so the
// snapshot is consistent.
// stubCleanupSummaryDeletes is a fake ProviderDeletes that also satisfies the
// CleanupSummaryGetter interface. The real ProviderDeletes methods panic because
// the cleanup-summary tests never reach them; what matters is the type assertion
// in HandleListWorkloads.
type stubCleanupSummaryDeletes struct {
	summaries map[string]map[lifecycle.ProviderDeleteSummaryRef]lifecycle.ProviderDeleteSummary
	err       error
	calls     int
	tenantID  string
	refs      []lifecycle.ProviderDeleteSummaryRef
}

func (s *stubCleanupSummaryDeletes) GetProviderDeleteSummaries(_ context.Context, customerID string, refs []lifecycle.ProviderDeleteSummaryRef) (map[lifecycle.ProviderDeleteSummaryRef]lifecycle.ProviderDeleteSummary, error) {
	s.calls++
	s.tenantID = customerID
	s.refs = append([]lifecycle.ProviderDeleteSummaryRef(nil), refs...)
	if s.err != nil {
		return nil, s.err
	}
	tenant, ok := s.summaries[customerID]
	if !ok {
		return nil, nil
	}
	out := make(map[lifecycle.ProviderDeleteSummaryRef]lifecycle.ProviderDeleteSummary)
	for _, ref := range refs {
		if v, found := tenant[ref]; found {
			out[ref] = v
		}
	}
	return out, nil
}

func (s *stubCleanupSummaryDeletes) RequestProviderDeleteForBooking(context.Context, lifecycle.ProviderDeleteBooking) (lifecycle.ProviderDeleteResponse, error) {
	panic("not used in cleanup summary tests")
}
func (s *stubCleanupSummaryDeletes) ClaimProviderDelete(context.Context, time.Duration) (lifecycle.ProviderDeleteRecord, bool, error) {
	panic("not used in cleanup summary tests")
}
func (s *stubCleanupSummaryDeletes) MarkProviderDeleteSucceeded(context.Context, int64, string) (time.Time, error) {
	panic("not used in cleanup summary tests")
}
func (s *stubCleanupSummaryDeletes) MarkProviderDeleteFailed(context.Context, int64, string, string, time.Time, int) error {
	panic("not used in cleanup summary tests")
}
func (s *stubCleanupSummaryDeletes) ClaimProviderDeleteCleanup(context.Context, time.Duration) (lifecycle.OutboxEvent, bool, error) {
	panic("not used in cleanup summary tests")
}
func (s *stubCleanupSummaryDeletes) AcknowledgeOutboxEvent(context.Context, int64, string) error {
	panic("not used in cleanup summary tests")
}
func (s *stubCleanupSummaryDeletes) MarkOutboxFailed(context.Context, int64, string, string, time.Time, int) error {
	panic("not used in cleanup summary tests")
}

func TestTenantWorkloadListRendersCleanupSummary(t *testing.T) {
	store, accounts, _, _ := tenantWorkloadFixture(t, map[string]string{"alice": state.RoleOwner})
	base := time.Date(2026, 8, 22, 10, 0, 0, 0, time.UTC)
	putTenantWorkload(store, "wl_manual", "cust_console", "burst_manual", base.Add(2*time.Minute))
	putTenantWorkload(store, "wl_terminated", "cust_console", "burst_terminated", base.Add(time.Minute))
	putTenantWorkload(store, "wl_no_cleanup", "cust_console", "burst_no_cleanup", base)

	deletedAt := base.Add(90 * time.Second)
	stub := &stubCleanupSummaryDeletes{
		summaries: map[string]map[lifecycle.ProviderDeleteSummaryRef]lifecycle.ProviderDeleteSummary{
			"cust_console": {
				{ClusterID: "cluster_console", BurstID: "burst_manual"}:     {State: lifecycle.ProviderDeleteManualAttention},
				{ClusterID: "cluster_console", BurstID: "burst_terminated"}: {State: lifecycle.ProviderDeleteTerminated, DeletedAt: &deletedAt},
			},
		},
	}
	accounts.Workloads.Deletes = stub

	rec := callTenantWorkload(accounts, http.MethodGet, "/v1/tenants/cust_console/workloads", "human_alice", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("list status = %d (body %q)", rec.Code, rec.Body.String())
	}
	response := decodeTenantWorkloads(t, rec)
	if len(response.Workloads) != 3 {
		t.Fatalf("workloads = %d, want 3", len(response.Workloads))
	}
	if stub.calls != 1 || stub.tenantID != "cust_console" {
		t.Fatalf("batch calls = %d tenant = %q, want one call for cust_console", stub.calls, stub.tenantID)
	}
	wantRefs := map[lifecycle.ProviderDeleteSummaryRef]bool{
		{ClusterID: "cluster_console", BurstID: "burst_manual"}:     true,
		{ClusterID: "cluster_console", BurstID: "burst_terminated"}: true,
		{ClusterID: "cluster_console", BurstID: "burst_no_cleanup"}: true,
	}
	if len(stub.refs) != len(wantRefs) {
		t.Fatalf("batch refs = %+v, want exactly current list refs", stub.refs)
	}
	for _, ref := range stub.refs {
		if !wantRefs[ref] {
			t.Fatalf("unexpected batch ref %+v", ref)
		}
	}

	var manual, terminated, noCleanup *TenantWorkload
	for i := range response.Workloads {
		switch response.Workloads[i].ID {
		case "wl_manual":
			manual = &response.Workloads[i]
		case "wl_terminated":
			terminated = &response.Workloads[i]
		case "wl_no_cleanup":
			noCleanup = &response.Workloads[i]
		}
	}
	if manual == nil || terminated == nil || noCleanup == nil {
		t.Fatalf("response = %+v, want all three workloads", response.Workloads)
	}
	if manual.Cleanup == nil || manual.Cleanup.State != lifecycle.ProviderDeleteManualAttention {
		t.Fatalf("manual cleanup = %+v, want manual_attention", manual.Cleanup)
	}
	if manual.Cleanup.DeletedAt != nil {
		t.Fatalf("manual deleted_at = %v, want nil", manual.Cleanup.DeletedAt)
	}
	if terminated.Cleanup == nil || terminated.Cleanup.State != lifecycle.ProviderDeleteTerminated {
		t.Fatalf("terminated cleanup = %+v, want terminated", terminated.Cleanup)
	}
	if terminated.Cleanup.DeletedAt == nil || !terminated.Cleanup.DeletedAt.Equal(deletedAt) {
		t.Fatalf("terminated deleted_at = %v, want %v", terminated.Cleanup.DeletedAt, deletedAt)
	}
	if noCleanup.Cleanup != nil {
		t.Fatalf("no_cleanup cleanup = %+v, want nil (omitted)", noCleanup.Cleanup)
	}

	// Verify no forbidden internal fields leak through the cleanup summary.
	for _, forbidden := range []string{
		"cloud_account_id", "provider_resource_id", "payload", "lease_token",
		"last_error", "generation", "attempts", "reason", "provider", "region",
		"sku", "requested_at", "updated_at",
	} {
		// Check that the cleanup objects do not contain these fields.
		// The body may contain "provider" or "region" in other contexts (spec),
		// so we check the JSON cleanup objects specifically.
		for _, wl := range response.Workloads {
			if wl.Cleanup != nil {
				raw, _ := json.Marshal(wl.Cleanup)
				if strings.Contains(string(raw), `"`+forbidden+`"`) {
					t.Errorf("cleanup summary leaked %q: %s", forbidden, string(raw))
				}
			}
		}
	}
}

func TestTenantWorkloadListCleanupOmittedWhenReaderAbsent(t *testing.T) {
	store, accounts, _, _ := tenantWorkloadFixture(t, map[string]string{"alice": state.RoleOwner})
	putTenantWorkload(store, "wl_1", "cust_console", "burst_1", time.Now().UTC())

	// Workloads.Deletes is nil (default in fixture) — no CleanupSummaryGetter.
	rec := callTenantWorkload(accounts, http.MethodGet, "/v1/tenants/cust_console/workloads", "human_alice", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d (body %q)", rec.Code, rec.Body.String())
	}
	response := decodeTenantWorkloads(t, rec)
	if len(response.Workloads) != 1 {
		t.Fatalf("workloads = %d, want 1", len(response.Workloads))
	}
	if response.Workloads[0].Cleanup != nil {
		t.Fatalf("cleanup should be omitted when reader is absent: %+v", response.Workloads[0].Cleanup)
	}
	if strings.Contains(rec.Body.String(), "cleanup") {
		t.Fatalf("cleanup key should not appear in JSON: %s", rec.Body.String())
	}
}

func TestTenantWorkloadListCleanupOmittedOnReaderError(t *testing.T) {
	store, accounts, _, _ := tenantWorkloadFixture(t, map[string]string{"alice": state.RoleOwner})
	putTenantWorkload(store, "wl_1", "cust_console", "burst_1", time.Now().UTC())

	stub := &stubCleanupSummaryDeletes{err: fmt.Errorf("database unavailable")}
	accounts.Workloads.Deletes = stub

	rec := callTenantWorkload(accounts, http.MethodGet, "/v1/tenants/cust_console/workloads", "human_alice", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("list should still succeed on reader error: status = %d (body %q)", rec.Code, rec.Body.String())
	}
	response := decodeTenantWorkloads(t, rec)
	if len(response.Workloads) != 1 || response.Workloads[0].Cleanup != nil {
		t.Fatalf("cleanup should be omitted on error: %+v", response.Workloads)
	}
}

func TestTenantWorkloadListCleanupPreservesOrderAndCount(t *testing.T) {
	store, accounts, _, _ := tenantWorkloadFixture(t, map[string]string{"alice": state.RoleOwner})
	base := time.Date(2026, 8, 22, 10, 0, 0, 0, time.UTC)
	putTenantWorkload(store, "wl_c", "cust_console", "burst_c", base)
	putTenantWorkload(store, "wl_b", "cust_console", "burst_b", base)
	putTenantWorkload(store, "wl_a", "cust_console", "burst_a", base.Add(time.Minute))

	stub := &stubCleanupSummaryDeletes{
		summaries: map[string]map[lifecycle.ProviderDeleteSummaryRef]lifecycle.ProviderDeleteSummary{
			"cust_console": {
				{ClusterID: "cluster_console", BurstID: "burst_a"}: {State: lifecycle.ProviderDeleteTerminated},
			},
		},
	}
	accounts.Workloads.Deletes = stub

	rec := callTenantWorkload(accounts, http.MethodGet, "/v1/tenants/cust_console/workloads", "human_alice", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}
	response := decodeTenantWorkloads(t, rec)
	if len(response.Workloads) != 3 {
		t.Fatalf("count = %d, want 3", len(response.Workloads))
	}
	if response.Workloads[0].ID != "wl_a" {
		t.Fatalf("order not preserved: first = %q, want wl_a", response.Workloads[0].ID)
	}
	if response.Workloads[0].Cleanup == nil || response.Workloads[0].Cleanup.State != lifecycle.ProviderDeleteTerminated {
		t.Fatalf("wl_a cleanup = %+v, want terminated", response.Workloads[0].Cleanup)
	}
	if response.Workloads[1].Cleanup != nil || response.Workloads[2].Cleanup != nil {
		t.Fatalf("wl_b/wl_c should have no cleanup")
	}
}

func TestTenantWorkloadListRendersNodeObservation(t *testing.T) {
	store, accounts, _, _ := tenantWorkloadFixture(t, map[string]string{"alice": state.RoleOwner})
	base := time.Date(2026, 8, 20, 12, 0, 0, 0, time.UTC)
	putTenantWorkload(store, "wl_observed", "cust_console", "burst_obs", base.Add(time.Minute))
	putTenantWorkload(store, "wl_legacy", "cust_console", "burst_leg", base)

	store.PutWorkload(&state.Workload{
		ID: "wl_observed", CustomerID: "cust_console", ClusterID: "cluster_console",
		BurstID: "burst_obs", Status: "running", CreatedAt: base.Add(time.Minute),
		SpecYAML: []byte(tenantWorkloadYAML),
		NodeObservation: &state.NodeObservation{
			NodeName: "ys-burst-obs", Phase: "Ready", Reason: "KubeletReady",
			ObservedAt: base.Add(2 * time.Minute),
		},
	})

	rec := callTenantWorkload(accounts, http.MethodGet, "/v1/tenants/cust_console/workloads", "human_alice", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("list status = %d (body %q)", rec.Code, rec.Body.String())
	}
	response := decodeTenantWorkloads(t, rec)
	var observed, legacy *TenantWorkload
	for i := range response.Workloads {
		switch response.Workloads[i].ID {
		case "wl_observed":
			observed = &response.Workloads[i]
		case "wl_legacy":
			legacy = &response.Workloads[i]
		}
	}
	if observed == nil || legacy == nil {
		t.Fatalf("response = %+v, want both workloads", response.Workloads)
	}
	if observed.NodeObservation == nil || observed.NodeObservation.NodeName != "ys-burst-obs" || observed.NodeObservation.Phase != "Ready" {
		t.Fatalf("observed node_observation = %+v, want the stored snapshot", observed.NodeObservation)
	}
	if legacy.NodeObservation != nil {
		t.Fatalf("legacy node_observation = %+v, want omitted", legacy.NodeObservation)
	}

	got := callTenantWorkload(accounts, http.MethodGet, "/v1/tenants/cust_console/workloads/wl_observed", "human_alice", "")
	if got.Code != http.StatusOK {
		t.Fatalf("get status = %d (body %q)", got.Code, got.Body.String())
	}
	var single map[string]any
	if err := json.Unmarshal(got.Body.Bytes(), &single); err != nil {
		t.Fatalf("decode: %v", err)
	}
	obs, ok := single["node_observation"].(map[string]any)
	if !ok || obs["node_name"] != "ys-burst-obs" || obs["phase"] != "Ready" {
		t.Fatalf("single GET node_observation = %v", single["node_observation"])
	}
}
