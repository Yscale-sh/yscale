package handlers

import (
	"context"
	"crypto/subtle"
	"errors"
	"net/http"
	"strings"

	"github.com/yscale-sh/yscale/central/internal/state"
)

type ctxKey int

const (
	ctxCustomer ctxKey = 1
	// ctxSubmitter carries the authenticated principal behind a mutating
	// workload request. Separate from ctxCustomer because they answer different
	// questions: the customer is WHICH TENANT the request acts on, and is set by
	// both credential types, while the submitter is WHO asked and is only ever
	// set by the human surface. Its absence is meaningful — see submitterOrCluster.
	ctxSubmitter ctxKey = 2
	// ctxAgentCluster carries the cluster id a cluster-scoped connector
	// credential is bound to. Set ONLY by ConnectorAuth and only for scoped
	// credentials; absent means the caller presented the legacy tenant token,
	// which is not bound to any one cluster. Its presence is what lets the
	// agent routes hold Hello and X-Cluster-ID to the credential's cluster.
	ctxAgentCluster ctxKey = 3
	// ctxOperatorAccount carries the central account id of the signed-in human
	// a browser-operator route acted for. Set ONLY by OperatorAuth, and only
	// after the subject cleared the operator allowlist AND resolved to an
	// existing account; absent means the request did not come through that
	// middleware, which is how the shared hosted handlers keep the static
	// AdminAuth route attributed to state.OperatorActor().
	ctxOperatorAccount       ctxKey = 4
	ctxCatalogPublisher      ctxKey = 5
	ctxConnectorWorkloadRead ctxKey = 6
	ctxWorkloadCredential    ctxKey = 7
)

// connectorWorkloadRead binds the front-half scope check to one observation.
// GET projects that snapshot; mutations also recheck authority and the observed
// identity in their state transaction. It is never a mutation-cache grant.
type connectorWorkloadRead struct {
	store     *state.Store
	clusterID string
	snapshot  state.WorkloadSnapshot
}

type catalogPublisherPrincipal struct {
	TenantID    string
	PublisherID string
}

// submitter is the principal a workload submission is attributed to, plus the
// authority they held. Role is a membership role for a human and empty for a
// cluster credential, which holds none: a cluster token authenticates a cluster,
// and a cluster has no place in the roster.
type submitter struct {
	Actor state.Actor
	Role  string
}

func withSubmitter(ctx context.Context, s submitter) context.Context {
	return context.WithValue(ctx, ctxSubmitter, s)
}

// submitterOrCluster resolves the principal behind a request. No submitter in
// the context means the cluster-token path — the OSS and connector route, where
// the credential IS the tenant and no human is involved — so the default is the
// truthful one rather than an error: the caller was authenticated, just not as a
// person.
func submitterOrCluster(ctx context.Context, customerID string) submitter {
	if s, ok := ctx.Value(ctxSubmitter).(submitter); ok {
		return s
	}
	return submitter{Actor: state.ClusterActor(customerID)}
}

// Auth wraps a handler in bearer-token auth.
func Auth(store *state.Store, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		header := r.Header.Get("Authorization")
		const prefix = "Bearer "
		if !strings.HasPrefix(header, prefix) {
			http.Error(w, "unauthorized: missing Bearer token", http.StatusUnauthorized)
			return
		}
		token := strings.TrimPrefix(header, prefix)
		c, err := store.AuthCustomerContext(r.Context(), token)
		if err != nil {
			writeCredentialError(w, err)
			return
		}
		ctx := context.WithValue(r.Context(), ctxCustomer, c)
		ctx = context.WithValue(ctx, ctxWorkloadCredential, state.WorkloadCancelPrincipal{CredentialHash: state.HashCustomerToken(token)})
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// CatalogPublisherAuth accepts only catalog-publisher credentials and pins the
// path tenant before the automation handler runs. It deliberately has no
// fallback to tenant or connector credentials.
func CatalogPublisherAuth(store *state.Store, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		header := r.Header.Get("Authorization")
		const prefix = "Bearer "
		if !strings.HasPrefix(header, prefix) {
			http.Error(w, "unauthorized: missing Bearer token", http.StatusUnauthorized)
			return
		}
		customer, publisherID, err := store.AuthCatalogPublisherContext(r.Context(), strings.TrimPrefix(header, prefix))
		if err != nil {
			writeCredentialError(w, err)
			return
		}
		if tenantID := r.PathValue("tenant_id"); tenantID == "" || tenantID != customer.ID {
			http.Error(w, "forbidden: credential is scoped to another tenant", http.StatusForbidden)
			return
		}
		principal := catalogPublisherPrincipal{TenantID: customer.ID, PublisherID: publisherID}
		ctx := context.WithValue(r.Context(), ctxCatalogPublisher, principal)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func catalogPublisherFromContext(ctx context.Context) (catalogPublisherPrincipal, bool) {
	p, ok := ctx.Value(ctxCatalogPublisher).(catalogPublisherPrincipal)
	return p, ok && p.TenantID != "" && p.PublisherID != ""
}

// ConnectorAuth is Auth for the two agent routes (GET /v1/agent/stream and
// POST /v1/agent/ts-auth-key), and the credential resolver the two
// connector-workload middlewares below build on. It accepts either credential a
// connector can hold: a cluster-scoped connector credential — resolved by
// SHA-256 digest, never stored or compared in plaintext — or the legacy tenant
// token, which keeps every already-deployed connector working unchanged.
//
// It is a SEPARATE middleware rather than a mode on Auth so the two credential
// types cannot bleed into each other's surfaces: Auth resolves only tenant
// tokens, so a connector credential presented to the price, spend, burst,
// workload-read, account or admin endpoints authenticates nothing — a
// credential that lives inside a customer's cluster reaches only the work of
// its own cluster, and never a tenant's money or roster. A scoped credential's
// requests are pinned to its bound cluster here: an X-Cluster-ID naming any
// other cluster is refused before a handler runs, and the agent stream holds
// Hello to the same binding.
func ConnectorAuth(store *state.Store, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		header := r.Header.Get("Authorization")
		const prefix = "Bearer "
		if !strings.HasPrefix(header, prefix) {
			http.Error(w, "unauthorized: missing Bearer token", http.StatusUnauthorized)
			return
		}
		token := strings.TrimPrefix(header, prefix)
		if c, clusterID, err := store.AuthClusterCredentialContext(r.Context(), token); err == nil {
			if hdr := r.Header.Get(clusterIDHeader); hdr != "" && hdr != clusterID {
				http.Error(w, "X-Cluster-ID does not match this connector credential's cluster", http.StatusForbidden)
				return
			}
			ctx := context.WithValue(r.Context(), ctxCustomer, c)
			ctx = context.WithValue(ctx, ctxAgentCluster, clusterID)
			ctx = context.WithValue(ctx, ctxWorkloadCredential, state.WorkloadCancelPrincipal{ClusterID: clusterID, CredentialHash: state.HashClusterCredential(token)})
			next.ServeHTTP(w, r.WithContext(ctx))
			return
		} else if !errors.Is(err, state.ErrNotFound) {
			writeCredentialError(w, err)
			return
		}
		c, err := store.AuthCustomerContext(r.Context(), token)
		if err != nil {
			writeCredentialError(w, err)
			return
		}
		ctx := context.WithValue(r.Context(), ctxCustomer, c)
		ctx = context.WithValue(ctx, ctxWorkloadCredential, state.WorkloadCancelPrincipal{CredentialHash: state.HashCustomerToken(token)})
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func writeCredentialError(w http.ResponseWriter, err error) {
	if errors.Is(err, state.ErrNotFound) {
		http.Error(w, "unauthorized: unknown token", http.StatusUnauthorized)
		return
	}
	writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "credential store unavailable"})
}

// ConnectorSubmitAuth gates POST /v1/workloads, where the connector submits the
// work its own cluster asked for. It resolves the same two credentials
// ConnectorAuth does and then, for a SCOPED credential, makes the binding the
// routing answer: the bound cluster id is written onto the request as
// X-Cluster-ID, so Create resolves the connector that presented the credential
// and nothing else.
//
// Injecting rather than requiring the header is what closes the gap. A
// connector that sends no header would otherwise fall into the tenant-wide
// routing below it — automode, ordered policy, the single-connector fallback —
// and those answer "which cluster does this TENANT want", which on a
// multi-cluster tenant is a sibling. A header naming another cluster never gets
// this far: ConnectorAuth refuses it 403 before the decider or any provider is
// reached.
//
// The legacy tenant token is left exactly as it was — bound to no cluster,
// routed by header and tenant policy — so every deployed connector and every
// customer script submitting with it behaves identically.
func ConnectorSubmitAuth(store *state.Store, next http.Handler) http.Handler {
	return ConnectorAuth(store, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		clusterID := AgentClusterFromContext(r.Context())
		if clusterID == "" {
			next.ServeHTTP(w, r)
			return
		}
		// Clone, because r's header map is the one the server handed the mux and
		// writing an authorization decision into a caller-owned map is not this
		// middleware's to do. Set rather than add: it replaces every value under
		// the key, so a repeated X-Cluster-ID whose FIRST value matched the
		// binding cannot leave a second one behind for Create to read.
		pinned := r.Clone(r.Context())
		pinned.Header.Set(clusterIDHeader, clusterID)
		next.ServeHTTP(w, pinned)
	}))
}

// ConnectorWorkloadAuth gates the workload routes a connector acts on an
// EXISTING workload through — GET /v1/workloads/{id}, POST
// /v1/workloads/{id}/started, POST /v1/workloads/{id}/complete and DELETE
// /v1/workloads/{id}. It resolves the
// same two credentials ConnectorAuth does, then narrows a SCOPED credential to
// the workloads its own cluster is actually running: the durable record must
// name both the credential's tenant and its bound cluster.
//
// The narrowing is why this is not just ConnectorAuth on these routes. A
// scoped credential lives inside one customer cluster, so tenant ownership
// alone would let it finish — and bill the teardown of — a sibling cluster's
// workload. A legacy record that names no cluster may match only when the
// tenant has exactly one registered cluster and it is this binding. That
// bounded migration case lets an in-flight legacy burst finish after
// credential rotation without letting one connector choose among siblings;
// an empty record on a multi-cluster tenant still matches nothing.
//
// The legacy tenant token passes through unnarrowed: it is bound to no cluster,
// so there is no binding to hold it to, and Get, Started, Complete and Cancel
// already refuse another tenant's workload. Every route outside these and
// submit — spend, price, burst, account and admin — stays on Auth, where a
// connector credential authenticates nothing at all.
func ConnectorWorkloadAuth(store *state.Store, next http.Handler) http.Handler {
	return ConnectorAuth(store, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		clusterID := AgentClusterFromContext(r.Context())
		if clusterID == "" {
			next.ServeHTTP(w, r)
			return
		}
		cust, err := CustomerFromContext(r.Context())
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		snapshot, err := store.WorkloadSnapshotForCustomer(r.Context(), cust.ID, r.PathValue("id"))
		if err != nil {
			if errors.Is(err, state.ErrNotFound) {
				http.Error(w, "not found", http.StatusNotFound)
			} else {
				writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "workload store unavailable"})
			}
			return
		}
		if snapshot.Workload.CustomerID != cust.ID || !connectorOwnsWorkload(cust, clusterID, snapshot.Workload.ClusterID) {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		approved := connectorWorkloadRead{store: store, clusterID: clusterID, snapshot: snapshot}
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), ctxConnectorWorkloadRead, approved)))
		return
	}))
}

func connectorOwnsWorkload(cust *state.Customer, boundClusterID, workloadClusterID string) bool {
	if workloadClusterID != "" {
		return workloadClusterID == boundClusterID
	}
	return len(cust.RegisteredClusters) == 1 &&
		cust.RegisteredClusters[0] != nil &&
		cust.RegisteredClusters[0].ClusterID == boundClusterID
}

// AgentClusterFromContext reports the cluster a scoped connector credential is
// bound to; "" for the legacy tenant token, whose reach ConnectorAuth did not
// narrow.
func AgentClusterFromContext(ctx context.Context) string {
	clusterID, _ := ctx.Value(ctxAgentCluster).(string)
	return clusterID
}

// AdminAuth gates operator-only endpoints (tenant provision/offboard) on a
// single admin bearer token, compared in constant time. When adminToken is
// empty the wrapped handler is DISABLED (404) so admin endpoints are never
// accidentally open on a deployment that didn't set one.
func AdminAuth(adminToken string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if adminToken == "" {
			http.NotFound(w, r)
			return
		}
		const prefix = "Bearer "
		header := r.Header.Get("Authorization")
		token := strings.TrimPrefix(header, prefix)
		if !strings.HasPrefix(header, prefix) ||
			subtle.ConstantTimeCompare([]byte(token), []byte(adminToken)) != 1 {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// CustomerFromContext extracts the authenticated customer.
func CustomerFromContext(ctx context.Context) (*state.Customer, error) {
	c, ok := ctx.Value(ctxCustomer).(*state.Customer)
	if !ok {
		return nil, errors.New("no customer in context")
	}
	return c, nil
}
