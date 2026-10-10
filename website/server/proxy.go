// The account seam. The SPA at /account may not talk to Yscale ID's token
// endpoint or to the central account API directly — both are cluster-internal
// — so this file exposes a small allowlist of same-origin handlers that relay
// to them. Nothing here is a general reverse proxy: every request is
// rebuilt field by field, the only credential that crosses is the caller's own
// Authorization header, and cookies never leave the browser's origin.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"math"
	"mime"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"
)

const (
	maxTokenReqBytes            = 4 << 10   // /api/auth/token request body cap
	maxFormFieldLen             = 2 << 10   // per-field cap inside that body
	maxBearerLen                = 8 << 10   // Authorization credential cap
	maxWorkloadBytes            = 1 << 20   // matches central's workload YAML request cap
	maxMemberJSONBytes          = 4 << 10   // roster mutation JSON cap
	maxTenantJSONBytes          = 4 << 10   // first-workspace creation JSON cap
	maxClusterJSONBytes         = 4 << 10   // cluster registration JSON cap
	maxHostedClusterJSONBytes   = 4 << 10   // operator hosted-cluster assignment JSON cap
	maxOperatorLimitsJSONBytes  = 4 << 10   // operator tenant guardrail update JSON cap
	maxPolicyJSONBytes          = 16 << 10  // tenant cluster placement policy JSON cap
	maxCatalogJSONBytes         = 128 << 10 // matches central's template catalog JSON cap
	maxGitOpsJSONBytes          = 64 << 10  // matches central's GitOps source registry JSON cap
	maxPublisherJSONBytes       = 4 << 10   // one bounded catalog publisher display name
	maxCloudAccountJSONBytes    = 8 << 10   // write-only Linode credential plus bounded image ids
	maxRuntimeBindingJSONBytes  = 64 << 10  // largest value plus worst-case JSON escaping, still bounded
	maxRuntimeBindingsRespBytes = 64 << 10  // at most 32 summary rows, never secret material
	maxServiceCreditJSONBytes   = 1 << 10   // two small scalar fields only
	maxBillingRespBytes         = 128 << 10 // bounded summary plus at most 100 active holds
	maxUpstreamBytes            = 256 << 10 // upstream response cap
	// Log output is capped to 256 KiB before JSON encoding. Escaping control
	// bytes can expand the wire body, so this route earns a larger relay cap.
	maxWorkloadLogsUpstreamBytes = 2 << 20
	maxMemberLimit               = 100 // central's roster page ceiling
	maxAuditLimit                = 200 // central's audit page ceiling (state.MaxAuditLimit)
	maxOperatorTenantLimit       = 100 // central's operator tenant page ceiling
	upstreamTimeout              = 10 * time.Second
	// A workload create waits on central admitting the run against a provider,
	// which legitimately outlasts a read. Two minutes is long enough for that
	// and still short enough that a wedged upstream cannot pin a browser
	// request open; every other route keeps the 10-second deadline.
	workloadCreateTimeout = 2 * time.Minute
	workloadLogTimeout    = 15 * time.Second
	minIdempotencyKeyLen  = 8   // central's floor
	maxIdempotencyKeyLen  = 255 // central's ceiling
	maxUpstreamHeaderLen  = 256

	idempotencyKeyHeader      = "Idempotency-Key"
	clusterIDHeader           = "X-Cluster-ID"
	templateIDHeader          = "X-Template-ID"
	templateVersionHeader     = "X-Template-Version"
	placementTokenHeader      = "X-Yscale-Placement-Token"
	maxPlacementTokenLen      = 4096
	idempotencyReplayedHeader = "Idempotency-Replayed"
	retryAfterHeader          = "Retry-After"
	proxyUserAgent            = "yscale-website/account-proxy"
	callbackPath              = "/callback"
	oidcTokenPath             = "/token"
	accountAPIVersion         = "/v1"
	accountClientID           = "yscale-platform"
)

var callbackOrigins = map[string]bool{
	"http://localhost:8081":        true,
	"http://127.0.0.1:8081":        true,
	"https://yscale-dev.yscale.sh": true,
	"https://yscale.sh":            true,
	"https://www.yscale.sh":        true,
}

// Path segments we are willing to interpolate into an upstream URL. Tenant and
// account ids are opaque to this server, so the rule is shape-only: no slashes,
// no dots-dots, no encoded anything.
var pathSegmentRe = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$`)

// Opaque pagination cursor from the account API, echoed back verbatim.
var cursorRe = regexp.MustCompile(`^[A-Za-z0-9._~:@=+-]{1,256}$`)

// Template versions are positive integers bounded more tightly by central.
// The proxy only checks their transport shape before forwarding them.
var templateVersionRe = regexp.MustCompile(`^[1-9][0-9]{0,8}$`)

var (
	runtimeBindingKeyRe      = regexp.MustCompile(`^[A-Z_][A-Z0-9_]{0,62}$`)
	runtimeBindingRevisionRe = regexp.MustCompile(`^(0|[1-9][0-9]{0,18})$`)
	serviceCreditKeyRe       = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]{7,127}$`)
)

var memberRoles = map[string]bool{
	"owner":  true,
	"admin":  true,
	"member": true,
	"viewer": true,
}

type addMemberReq struct {
	Email string `json:"email"`
	Role  string `json:"role"`
}

type updateMemberRoleReq struct {
	Role string `json:"role"`
}

type linodeCloudAccountReq struct {
	Token    string `json:"token"`
	Region   string `json:"region"`
	CPUImage string `json:"cpu_image,omitempty"`
	GPUImage string `json:"gpu_image,omitempty"`
}

type runtimeBindingReq struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

type serviceCreditReq struct {
	AmountMicroUSD int64  `json:"amount_micro_usd"`
	IdempotencyKey string `json:"idempotency_key"`
}

func (o *serviceCreditReq) UnmarshalJSON(data []byte) error {
	fields, err := decodeExactJSONObject(data)
	if err != nil || len(fields) != 2 || fields["amount_micro_usd"] == nil || fields["idempotency_key"] == nil {
		return errors.New("service credit body must contain only amount_micro_usd and idempotency_key")
	}
	if json.Unmarshal(fields["amount_micro_usd"], &o.AmountMicroUSD) != nil || json.Unmarshal(fields["idempotency_key"], &o.IdempotencyKey) != nil {
		return errors.New("invalid service credit fields")
	}
	return nil
}

func (o *runtimeBindingReq) UnmarshalJSON(data []byte) error {
	fields, err := decodeExactJSONObject(data)
	if err != nil || len(fields) != 2 || fields["name"] == nil || fields["value"] == nil {
		return errors.New("runtime binding body must contain only name and value")
	}
	if json.Unmarshal(fields["name"], &o.Name) != nil || json.Unmarshal(fields["value"], &o.Value) != nil {
		return errors.New("runtime binding fields must be strings")
	}
	return nil
}

type createTenantReq struct {
	Name string `json:"name"`
}

type createCatalogPublisherReq struct {
	Name string `json:"name"`
}

func (o *createCatalogPublisherReq) UnmarshalJSON(data []byte) error {
	fields, err := decodeExactJSONObject(data)
	if err != nil || len(fields) != 1 || fields["name"] == nil {
		return errors.New("publisher body must contain only name")
	}
	return json.Unmarshal(fields["name"], &o.Name)
}

type automationCatalogReq struct {
	CatalogRevision json.RawMessage `json:"catalog_revision"`
	Templates       json.RawMessage `json:"templates"`
}

type createHostedClusterReq struct {
	ClusterID json.RawMessage `json:"cluster_id"`
	Name      json.RawMessage `json:"name"`
	Namespace json.RawMessage `json:"namespace"`
}

type operatorTenantLimitsReq struct {
	MaxConcurrentBursts json.RawMessage `json:"max_concurrent_bursts"`
	MaxHourlyUSD        json.RawMessage `json:"max_hourly_usd"`
}

func (o *operatorTenantLimitsReq) UnmarshalJSON(data []byte) error {
	dec := json.NewDecoder(bytes.NewReader(data))
	opening, err := dec.Token()
	if err != nil || opening != json.Delim('{') {
		return errors.New("tenant guardrail body must be an object")
	}
	seen := map[string]bool{}
	for dec.More() {
		key, err := dec.Token()
		if err != nil {
			return err
		}
		name, ok := key.(string)
		if !ok || seen[name] {
			return errors.New("tenant guardrail body has a duplicate field")
		}
		seen[name] = true
		switch name {
		case "max_concurrent_bursts":
			err = dec.Decode(&o.MaxConcurrentBursts)
		case "max_hourly_usd":
			err = dec.Decode(&o.MaxHourlyUSD)
		default:
			return errors.New("tenant guardrail body has an unknown field")
		}
		if err != nil {
			return err
		}
	}
	closing, err := dec.Token()
	if err != nil || closing != json.Delim('}') {
		return errors.New("tenant guardrail body is not closed")
	}
	return nil
}

func (o *automationCatalogReq) UnmarshalJSON(data []byte) error {
	fields, err := decodeExactJSONObject(data)
	if err != nil {
		return err
	}
	if len(fields) != 2 || fields["catalog_revision"] == nil || fields["templates"] == nil {
		return errors.New("catalog body must contain catalog_revision and templates")
	}
	o.CatalogRevision = fields["catalog_revision"]
	o.Templates = fields["templates"]
	return nil
}

type operatorTenantLimitsOut struct {
	MaxConcurrentBursts int     `json:"max_concurrent_bursts"`
	MaxHourlyUSD        float64 `json:"max_hourly_usd"`
}

// accountProxy holds the two cluster-internal bases and the client used to
// reach them. Both are injected rather than read from the environment at call
// time so tests can point them at httptest servers.
type accountProxy struct {
	idURL    string // Yscale ID, e.g. http://yscale-id...:8080
	cloudURL string // central account API, e.g. http://yscale-cloud...:8443
	client   *http.Client
	// Deadlines are fields rather than constants at the call site so a test can
	// prove the create route outlives the generic one without waiting out a
	// real ten seconds. timeout covers auth, reads, and roster mutations.
	timeout       time.Duration
	createTimeout time.Duration
}

func newAccountProxy(idURL, cloudURL string, client *http.Client) *accountProxy {
	if client == nil {
		client = &http.Client{
			// The client-wide ceiling is the longest route's, not the shortest:
			// each request carries its own context deadline, and a client
			// Timeout of 10s would silently cap the create route at 10s too.
			Timeout: workloadCreateTimeout,
		}
	} else {
		// Do not mutate a caller-owned client just to enforce this seam's
		// redirect policy.
		copy := *client
		client = &copy
	}
	// A redirect from either upstream is a misconfiguration, not something to
	// chase with the caller's bearer token attached. This applies to injected
	// clients too; tests and production wiring must have the same security seam.
	client.CheckRedirect = func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	}
	return &accountProxy{
		idURL:         strings.TrimSuffix(idURL, "/"),
		cloudURL:      strings.TrimSuffix(cloudURL, "/"),
		client:        client,
		timeout:       upstreamTimeout,
		createTimeout: workloadCreateTimeout,
	}
}

// handleAuthToken completes the browser's PKCE flow. The SPA holds the code and
// verifier; only this server can reach Yscale ID's token endpoint. Exactly five
// form fields are forwarded — anything else the caller sends is dropped.
func (a *app) handleAuthToken(w http.ResponseWriter, r *http.Request) {
	if a.proxy == nil || a.proxy.idURL == "" {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "identity service is not configured"})
		return
	}
	if !a.allowAccountRequest(w, r) {
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, maxTokenReqBytes)
	if err := r.ParseForm(); err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			writeJSON(w, http.StatusRequestEntityTooLarge, map[string]string{"error": "request too large"})
			return
		}
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid form body"})
		return
	}
	// r.PostForm only — a code smuggled in the query string is not a code.
	form, ok := tokenExchangeForm(r.PostForm)
	if !ok {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "unsupported token request"})
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), a.proxy.deadline(0))
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, a.proxy.idURL+oidcTokenPath, strings.NewReader(form.Encode()))
	if err != nil {
		writeJSON(w, http.StatusBadGateway, map[string]string{"error": "upstream unavailable"})
		return
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", proxyUserAgent)
	a.proxy.relay(w, req, maxUpstreamBytes, nil, nil)
}

// tokenExchangeForm rebuilds the token request from scratch. Only the
// authorization_code grant is proxied, and the callback must be the SPA's own
// /callback path — this endpoint is not a general OIDC relay.
func tokenExchangeForm(in url.Values) (url.Values, bool) {
	if in.Get("grant_type") != "authorization_code" {
		return nil, false
	}
	out := url.Values{"grant_type": {"authorization_code"}}
	for _, field := range []string{"code", "redirect_uri", "client_id", "code_verifier"} {
		v := in.Get(field)
		if v == "" || len(v) > maxFormFieldLen {
			return nil, false
		}
		out.Set(field, v)
	}
	if out.Get("client_id") != accountClientID {
		return nil, false
	}
	if !validCallbackURI(out.Get("redirect_uri")) {
		return nil, false
	}
	return out, true
}

func validCallbackURI(raw string) bool {
	u, err := url.Parse(raw)
	if err != nil {
		return false
	}
	return (u.Scheme == "https" || u.Scheme == "http") &&
		u.Host != "" && u.User == nil &&
		callbackOrigins[u.Scheme+"://"+u.Host] &&
		u.Path == callbackPath && u.RawQuery == "" && u.Fragment == ""
}

func (a *app) allowAccountRequest(w http.ResponseWriter, r *http.Request) bool {
	if a.proxyLimiter != nil && !a.proxyLimiter.allow("account:"+clientIP(r)) {
		w.Header().Set("Retry-After", "60")
		writeJSON(w, http.StatusTooManyRequests, map[string]string{"error": "slow down"})
		return false
	}
	return true
}

// handleAccount proxies GET /v1/account for the bearer in the request.
func (a *app) handleAccount(w http.ResponseWriter, r *http.Request) {
	a.relayAccountAPI(w, r, http.MethodGet, accountAPIVersion+"/account", nil)
}

// handleCreateTenant proxies first-workspace creation. The only browser-sent
// field is a display name; central mints ids, roles, limits, and the returned
// account envelope.
func (a *app) handleCreateTenant(w http.ResponseWriter, r *http.Request) {
	if r.URL.RawQuery != "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "query parameters are not supported"})
		return
	}
	mediaType, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || mediaType != "application/json" {
		writeJSON(w, http.StatusUnsupportedMediaType, map[string]string{"error": "Content-Type must be application/json"})
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, maxTenantJSONBytes)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	var body createTenantReq
	if err := dec.Decode(&body); err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			writeJSON(w, http.StatusRequestEntityTooLarge, map[string]string{"error": "tenant JSON is too large"})
			return
		}
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid tenant JSON"})
		return
	}
	if err := dec.Decode(&struct{}{}); err != io.EOF {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid tenant JSON"})
		return
	}
	body.Name = strings.TrimSpace(body.Name)
	if !validTenantName(body.Name) {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "workspace name must be 1 to 64 characters with no control characters"})
		return
	}
	out, err := json.Marshal(body)
	if err != nil {
		writeJSON(w, http.StatusBadGateway, map[string]string{"error": "could not rebuild tenant request"})
		return
	}
	a.relayAccountAPIWithBody(w, r, http.MethodPost, accountAPIVersion+"/account/tenants", nil, bytes.NewReader(out), "application/json", maxUpstreamBytes, relayExtras{})
}

func validTenantName(name string) bool {
	if name == "" || !utf8.ValidString(name) {
		return false
	}
	n := utf8.RuneCountInString(name)
	if n < 1 || n > 64 {
		return false
	}
	for _, r := range name {
		if unicode.IsControl(r) {
			return false
		}
	}
	return true
}

// handleListMembers proxies the tenant roster, preserving only limit and after.
func (a *app) handleListMembers(w http.ResponseWriter, r *http.Request) {
	tenant := r.PathValue("tenant_id")
	if !pathSegmentRe.MatchString(tenant) {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid tenant id"})
		return
	}
	query, ok := pageQuery(r.URL.Query(), maxMemberLimit)
	if !ok {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid pagination parameters"})
		return
	}
	a.relayAccountAPI(w, r, http.MethodGet, accountAPIVersion+"/tenants/"+tenant+"/members", query)
}

// handleAddMember proxies exactly the roster add body central accepts. The
// person must already be a known account; central decides whether the caller
// may grant the requested role and whether the account exists.
func (a *app) handleAddMember(w http.ResponseWriter, r *http.Request) {
	tenant := r.PathValue("tenant_id")
	if !pathSegmentRe.MatchString(tenant) {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid tenant id"})
		return
	}
	if r.URL.RawQuery != "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "query parameters are not supported"})
		return
	}
	var body addMemberReq
	if !decodeMemberJSON(w, r, &body) {
		return
	}
	body.Email = strings.TrimSpace(body.Email)
	if body.Email == "" || len(body.Email) > maxEmailLen || !emailRe.MatchString(body.Email) || !memberRoles[body.Role] {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid member request"})
		return
	}
	out, err := json.Marshal(body)
	if err != nil {
		writeJSON(w, http.StatusBadGateway, map[string]string{"error": "could not rebuild member request"})
		return
	}
	a.relayAccountAPIWithBody(w, r, http.MethodPost, accountAPIVersion+"/tenants/"+tenant+"/members", nil, bytes.NewReader(out), "application/json", maxUpstreamBytes, relayExtras{})
}

// A full journal page is maxAuditLimit rows, each carrying an actor, a target,
// and a free-form detail object — more than the 256 KiB the rest of the seam
// needs. Advertising a limit and then refusing the answer to it as "too large"
// is this server contradicting itself, so the journal gets its own ceiling. It
// is the only route that does; everything else stays on maxUpstreamBytes.
const maxAuditRespBytes = 1 << 20

// handleListAudit proxies the tenant's governance journal, preserving only
// limit and after. Whether the caller may read it is central's call — the
// journal is scoped there to a tenant's own owners and admins — so this server
// checks the shape, forwards the bearer, and relays the answer, including a
// 403, unchanged.
func (a *app) handleListAudit(w http.ResponseWriter, r *http.Request) {
	tenant := r.PathValue("tenant_id")
	if !pathSegmentRe.MatchString(tenant) {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid tenant id"})
		return
	}
	query, ok := pageQuery(r.URL.Query(), maxAuditLimit)
	if !ok {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid pagination parameters"})
		return
	}
	a.relayAccountAPIWithBody(w, r, http.MethodGet, accountAPIVersion+"/tenants/"+tenant+"/audit", query, nil, "", maxAuditRespBytes, relayExtras{})
}

// handleRemoveMember proxies the member removal. Whether the caller may do it
// is the account API's call — this server only checks the shape.
func (a *app) handleRemoveMember(w http.ResponseWriter, r *http.Request) {
	tenant := r.PathValue("tenant_id")
	account := r.PathValue("account_id")
	if !pathSegmentRe.MatchString(tenant) || !pathSegmentRe.MatchString(account) {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid tenant or account id"})
		return
	}
	a.relayAccountAPI(w, r, http.MethodDelete, accountAPIVersion+"/tenants/"+tenant+"/members/"+account, nil)
}

func (a *app) handleUpdateMemberRole(w http.ResponseWriter, r *http.Request) {
	tenant := r.PathValue("tenant_id")
	account := r.PathValue("account_id")
	if !pathSegmentRe.MatchString(tenant) || !pathSegmentRe.MatchString(account) {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid tenant or account id"})
		return
	}
	if r.URL.RawQuery != "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "query parameters are not supported"})
		return
	}
	var body updateMemberRoleReq
	if !decodeMemberJSON(w, r, &body) {
		return
	}
	if !memberRoles[body.Role] {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid role"})
		return
	}
	out, err := json.Marshal(body)
	if err != nil {
		writeJSON(w, http.StatusBadGateway, map[string]string{"error": "could not rebuild member request"})
		return
	}
	a.relayAccountAPIWithBody(w, r, http.MethodPatch, accountAPIVersion+"/tenants/"+tenant+"/members/"+account, nil, bytes.NewReader(out), "application/json", maxUpstreamBytes, relayExtras{})
}

func (a *app) handleTenantUsage(w http.ResponseWriter, r *http.Request) {
	tenant := r.PathValue("tenant_id")
	if !pathSegmentRe.MatchString(tenant) {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid tenant id"})
		return
	}
	if r.URL.RawQuery != "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "query parameters are not supported"})
		return
	}
	a.relayAccountAPI(w, r, http.MethodGet, accountAPIVersion+"/tenants/"+tenant+"/usage", nil)
}

// Hosted capacity is an explicit tenant-scoped read/write seam. The POST only
// joins the tenant to central's queue; it accepts no body or parameters, and
// central remains the authority on both role checks and the returned state.
func (a *app) handleGetHostedCapacity(w http.ResponseWriter, r *http.Request) {
	a.handleHostedCapacity(w, r, http.MethodGet)
}

func (a *app) handleRequestHostedCapacity(w http.ResponseWriter, r *http.Request) {
	a.handleHostedCapacity(w, r, http.MethodPost)
}

func (a *app) handleHostedCapacity(w http.ResponseWriter, r *http.Request, method string) {
	tenant := r.PathValue("tenant_id")
	if !pathSegmentRe.MatchString(tenant) {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid tenant id"})
		return
	}
	if r.URL.RawQuery != "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "query parameters are not supported"})
		return
	}
	if method == http.MethodPost {
		r.Body = http.MaxBytesReader(w, r.Body, 1)
		body, err := io.ReadAll(r.Body)
		if err != nil {
			var tooLarge *http.MaxBytesError
			if errors.As(err, &tooLarge) {
				writeJSON(w, http.StatusBadRequest, map[string]string{"error": "request body is not accepted"})
				return
			}
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "could not read request body"})
			return
		}
		if len(body) != 0 {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "request body is not accepted"})
			return
		}
	}
	a.relayAccountAPIWithBody(w, r, method, accountAPIVersion+"/tenants/"+tenant+"/hosted-capacity", nil, http.NoBody, "", maxUpstreamBytes, relayExtras{})
}

// Operator hosted-capacity routes are a separate, exact allowlist. Central
// remains the authority on operator roles and assignment state; this seam only
// validates transport shape and forwards the caller's bearer credential.
func (a *app) handleOperatorHostedCapacityRequests(w http.ResponseWriter, r *http.Request) {
	if !operatorNoQueryOrBody(w, r) {
		return
	}
	a.relayAccountAPI(w, r, http.MethodGet, accountAPIVersion+"/operator/hosted-capacity/requests", nil)
}

func (a *app) handleOperatorHostedClusters(w http.ResponseWriter, r *http.Request) {
	if !operatorNoQueryOrBody(w, r) {
		return
	}
	a.relayAccountAPI(w, r, http.MethodGet, accountAPIVersion+"/operator/hosted-clusters", nil)
}

func (a *app) handleOperatorTenants(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 1)
	body, err := io.ReadAll(r.Body)
	if err != nil || len(body) != 0 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "request body is not accepted"})
		return
	}
	parsed, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid tenant inventory query"})
		return
	}
	query, ok := operatorTenantPageQuery(parsed)
	if !ok {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid tenant inventory query"})
		return
	}
	a.relayAccountAPI(w, r, http.MethodGet, accountAPIVersion+"/operator/tenants", query)
}

func operatorTenantPageQuery(in url.Values) (url.Values, bool) {
	out := url.Values{}
	for key, values := range in {
		if (key != "after" && key != "limit") || len(values) != 1 {
			return nil, false
		}
	}
	if values, exists := in["after"]; exists {
		if len(values) != 1 || !pathSegmentRe.MatchString(values[0]) {
			return nil, false
		}
		out.Set("after", values[0])
	}
	if values, exists := in["limit"]; exists {
		if len(values) != 1 {
			return nil, false
		}
		n, err := strconv.Atoi(values[0])
		if err != nil || n < 1 || n > maxOperatorTenantLimit || strconv.Itoa(n) != values[0] {
			return nil, false
		}
		out.Set("limit", values[0])
	}
	return out, true
}

func (a *app) handlePatchOperatorTenantLimits(w http.ResponseWriter, r *http.Request) {
	tenant := r.PathValue("tenant_id")
	if !pathSegmentRe.MatchString(tenant) {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid tenant id"})
		return
	}
	if r.URL.RawQuery != "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "query parameters are not supported"})
		return
	}
	if len(r.Header.Values("Content-Type")) != 1 || r.Header.Get("Content-Type") != "application/json" {
		writeJSON(w, http.StatusUnsupportedMediaType, map[string]string{"error": "Content-Type must be application/json"})
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, maxOperatorLimitsJSONBytes)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	var body *operatorTenantLimitsReq
	if err := dec.Decode(&body); err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			writeJSON(w, http.StatusRequestEntityTooLarge, map[string]string{"error": "tenant guardrail JSON is too large"})
			return
		}
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid tenant guardrail JSON"})
		return
	}
	if body == nil || dec.Decode(&struct{}{}) != io.EOF {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid tenant guardrail JSON"})
		return
	}
	concurrent, ok := nonNegativeJSONInt(body.MaxConcurrentBursts)
	if !ok {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "max_concurrent_bursts must be a non-negative integer"})
		return
	}
	hourly, ok := nonNegativeJSONNumber(body.MaxHourlyUSD)
	if !ok {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "max_hourly_usd must be a finite non-negative number"})
		return
	}
	out, err := json.Marshal(operatorTenantLimitsOut{MaxConcurrentBursts: concurrent, MaxHourlyUSD: hourly})
	if err != nil {
		writeJSON(w, http.StatusBadGateway, map[string]string{"error": "could not rebuild tenant guardrail request"})
		return
	}
	a.relayAccountAPIWithBody(w, r, http.MethodPatch, accountAPIVersion+"/operator/tenants/"+tenant+"/limits", nil, bytes.NewReader(out), "application/json", maxUpstreamBytes, relayExtras{})
}

func nonNegativeJSONNumber(raw json.RawMessage) (float64, bool) {
	if raw == nil {
		return 0, false
	}
	var value any
	if err := json.Unmarshal(raw, &value); err != nil {
		return 0, false
	}
	number, ok := value.(float64)
	return number, ok && !math.IsNaN(number) && !math.IsInf(number, 0) && number >= 0
}

func nonNegativeJSONInt(raw json.RawMessage) (int, bool) {
	if raw == nil || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return 0, false
	}
	var value int
	if err := json.Unmarshal(raw, &value); err != nil {
		return 0, false
	}
	return value, value >= 0
}

func operatorHostedClusterSegments(w http.ResponseWriter, r *http.Request) (string, string, bool) {
	tenant := r.PathValue("tenant_id")
	cluster := r.PathValue("cluster_id")
	if !pathSegmentRe.MatchString(tenant) {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid tenant id"})
		return "", "", false
	}
	if !pathSegmentRe.MatchString(cluster) {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid cluster id"})
		return "", "", false
	}
	return tenant, cluster, true
}

func operatorNoQueryOrBody(w http.ResponseWriter, r *http.Request) bool {
	if r.URL.RawQuery != "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "query parameters are not supported"})
		return false
	}
	r.Body = http.MaxBytesReader(w, r.Body, 1)
	body, err := io.ReadAll(r.Body)
	if err != nil || len(body) != 0 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "request body is not accepted"})
		return false
	}
	return true
}

func (a *app) handleRotateOperatorHostedClusterCredential(w http.ResponseWriter, r *http.Request) {
	tenant, cluster, ok := operatorHostedClusterSegments(w, r)
	if !ok {
		return
	}
	if r.Header.Get("Content-Type") != "" {
		writeJSON(w, http.StatusUnsupportedMediaType, map[string]string{"error": "Content-Type is not accepted"})
		return
	}
	if !operatorNoQueryOrBody(w, r) {
		return
	}
	a.relayAccountAPIWithBody(w, r, http.MethodPost, accountAPIVersion+"/operator/tenants/"+tenant+"/hosted-clusters/"+cluster+"/credential", nil, http.NoBody, "", maxUpstreamBytes, relayExtras{})
}

func (a *app) handleDeleteOperatorHostedCluster(w http.ResponseWriter, r *http.Request) {
	tenant, cluster, ok := operatorHostedClusterSegments(w, r)
	if !ok || !operatorNoQueryOrBody(w, r) {
		return
	}
	a.relayAccountAPIWithBody(w, r, http.MethodDelete, accountAPIVersion+"/operator/tenants/"+tenant+"/hosted-clusters/"+cluster, nil, http.NoBody, "", maxUpstreamBytes, relayExtras{})
}

func (a *app) handleCreateOperatorHostedCluster(w http.ResponseWriter, r *http.Request) {
	tenant := r.PathValue("tenant_id")
	if !pathSegmentRe.MatchString(tenant) {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid tenant id"})
		return
	}
	if r.URL.RawQuery != "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "query parameters are not supported"})
		return
	}
	mediaType, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || mediaType != "application/json" {
		writeJSON(w, http.StatusUnsupportedMediaType, map[string]string{"error": "Content-Type must be application/json"})
		return
	}

	r.Body = http.MaxBytesReader(w, r.Body, maxHostedClusterJSONBytes)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	var body *createHostedClusterReq
	if err := dec.Decode(&body); err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			writeJSON(w, http.StatusRequestEntityTooLarge, map[string]string{"error": "hosted cluster JSON is too large"})
			return
		}
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid hosted cluster JSON"})
		return
	}
	if body == nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid hosted cluster JSON"})
		return
	}
	if err := dec.Decode(&struct{}{}); err != io.EOF {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid hosted cluster JSON"})
		return
	}

	outFields := map[string]string{}
	for name, raw := range map[string]json.RawMessage{
		"cluster_id": body.ClusterID,
		"name":       body.Name,
		"namespace":  body.Namespace,
	} {
		if raw == nil {
			continue
		}
		var value any
		if err := json.Unmarshal(raw, &value); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid hosted cluster JSON"})
			return
		}
		text, ok := value.(string)
		if !ok {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": name + " must be a string"})
			return
		}
		outFields[name] = text
	}
	out, err := json.Marshal(outFields)
	if err != nil {
		writeJSON(w, http.StatusBadGateway, map[string]string{"error": "could not rebuild hosted cluster request"})
		return
	}
	a.relayAccountAPIWithBody(w, r, http.MethodPost, accountAPIVersion+"/operator/tenants/"+tenant+"/hosted-clusters", nil, bytes.NewReader(out), "application/json", maxUpstreamBytes, relayExtras{})
}

// handleListWorkloads and handleCreateWorkload are deliberately separate
// allowlisted routes. Neither accepts query parameters, and the create path
// forwards only a bounded text/yaml document.
func (a *app) handleListWorkloads(w http.ResponseWriter, r *http.Request) {
	tenant, ok := workloadTenantSegment(w, r)
	if !ok {
		return
	}
	a.relayAccountAPI(w, r, http.MethodGet, accountAPIVersion+"/tenants/"+tenant+"/workloads", nil)
}

// handleListClusters proxies the tenant's cluster observation. It is a plain
// read with no pagination and no parameters: central answers with what the
// replica handling the request currently observes, and this server neither
// filters those rows nor decides who may see them.
func (a *app) handleListClusters(w http.ResponseWriter, r *http.Request) {
	tenant := r.PathValue("tenant_id")
	if !pathSegmentRe.MatchString(tenant) {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid tenant id"})
		return
	}
	if r.URL.RawQuery != "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "query parameters are not supported"})
		return
	}
	a.relayAccountAPI(w, r, http.MethodGet, accountAPIVersion+"/tenants/"+tenant+"/clusters", nil)
}

// The durable cluster registry writes. Central owns names, ids, credentials,
// and who may write them; this server checks only the transport shape — path
// segments, JSON, a bounded body — forwards the bearer, and relays the answer
// as it came back. The one-time connector credential in that answer passes
// through unread and unlogged.
func (a *app) handleRegisterCluster(w http.ResponseWriter, r *http.Request) {
	tenant := r.PathValue("tenant_id")
	if !pathSegmentRe.MatchString(tenant) {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid tenant id"})
		return
	}
	if r.URL.RawQuery != "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "query parameters are not supported"})
		return
	}
	mediaType, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || mediaType != "application/json" {
		writeJSON(w, http.StatusUnsupportedMediaType, map[string]string{"error": "Content-Type must be application/json"})
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, maxClusterJSONBytes)
	body, err := io.ReadAll(r.Body)
	if err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			writeJSON(w, http.StatusRequestEntityTooLarge, map[string]string{"error": "cluster registration JSON is too large"})
			return
		}
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "could not read cluster registration JSON"})
		return
	}
	if !json.Valid(bytes.TrimSpace(body)) {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid cluster registration JSON"})
		return
	}
	a.relayAccountAPIWithBody(w, r, http.MethodPost, accountAPIVersion+"/tenants/"+tenant+"/clusters", nil, bytes.NewReader(body), "application/json", maxUpstreamBytes, relayExtras{})
}

func clusterPathSegments(w http.ResponseWriter, r *http.Request) (string, string, bool) {
	tenant := r.PathValue("tenant_id")
	if !pathSegmentRe.MatchString(tenant) {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid tenant id"})
		return "", "", false
	}
	cluster := r.PathValue("cluster_id")
	if !pathSegmentRe.MatchString(cluster) {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid cluster id"})
		return "", "", false
	}
	if r.URL.RawQuery != "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "query parameters are not supported"})
		return "", "", false
	}
	return tenant, cluster, true
}

func (a *app) handleRotateClusterCredential(w http.ResponseWriter, r *http.Request) {
	tenant, cluster, ok := clusterPathSegments(w, r)
	if !ok {
		return
	}
	a.relayAccountAPI(w, r, http.MethodPost, accountAPIVersion+"/tenants/"+tenant+"/clusters/"+cluster+"/credential", nil)
}

func (a *app) handleDeleteCluster(w http.ResponseWriter, r *http.Request) {
	tenant, cluster, ok := clusterPathSegments(w, r)
	if !ok {
		return
	}
	a.relayAccountAPI(w, r, http.MethodDelete, accountAPIVersion+"/tenants/"+tenant+"/clusters/"+cluster, nil)
}

func (a *app) handleGetClusterPolicy(w http.ResponseWriter, r *http.Request) {
	tenant := r.PathValue("tenant_id")
	if !pathSegmentRe.MatchString(tenant) {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid tenant id"})
		return
	}
	if r.URL.RawQuery != "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "query parameters are not supported"})
		return
	}
	a.relayAccountAPI(w, r, http.MethodGet, accountAPIVersion+"/tenants/"+tenant+"/cluster-policy", nil)
}

func (a *app) handlePutClusterPolicy(w http.ResponseWriter, r *http.Request) {
	tenant := r.PathValue("tenant_id")
	if !pathSegmentRe.MatchString(tenant) {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid tenant id"})
		return
	}
	if r.URL.RawQuery != "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "query parameters are not supported"})
		return
	}
	mediaType, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || mediaType != "application/json" {
		writeJSON(w, http.StatusUnsupportedMediaType, map[string]string{"error": "Content-Type must be application/json"})
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, maxPolicyJSONBytes)
	body, err := io.ReadAll(r.Body)
	if err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			writeJSON(w, http.StatusRequestEntityTooLarge, map[string]string{"error": "cluster policy JSON is too large"})
			return
		}
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "could not read cluster policy JSON"})
		return
	}
	if !json.Valid(bytes.TrimSpace(body)) {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid cluster policy JSON"})
		return
	}
	a.relayAccountAPIWithBody(w, r, http.MethodPut, accountAPIVersion+"/tenants/"+tenant+"/cluster-policy", nil, bytes.NewReader(body), "application/json", maxUpstreamBytes, relayExtras{})
}

// The tenant's workload template catalog. Central owns what a tenant may launch
// and who may change it; this server checks the shape of the tenant segment and
// the JSON, forwards the bearer, and relays the answer — including a 403 — as
// it came back.
func (a *app) handleGetTenantTemplates(w http.ResponseWriter, r *http.Request) {
	tenant := r.PathValue("tenant_id")
	if !pathSegmentRe.MatchString(tenant) {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid tenant id"})
		return
	}
	if r.URL.RawQuery != "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "query parameters are not supported"})
		return
	}
	a.relayAccountAPI(w, r, http.MethodGet, accountAPIVersion+"/tenants/"+tenant+"/templates", nil)
}

// The catalog is replaced whole rather than patched entry by entry, so the
// document that crosses is the one the manager reviewed. Its contents stay
// opaque here: central decides which templates are legal and which roles may
// write them.
func (a *app) handlePutTenantTemplates(w http.ResponseWriter, r *http.Request) {
	tenant := r.PathValue("tenant_id")
	if !pathSegmentRe.MatchString(tenant) {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid tenant id"})
		return
	}
	if r.URL.RawQuery != "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "query parameters are not supported"})
		return
	}
	mediaType, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || mediaType != "application/json" {
		writeJSON(w, http.StatusUnsupportedMediaType, map[string]string{"error": "Content-Type must be application/json"})
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, maxCatalogJSONBytes)
	body, err := io.ReadAll(r.Body)
	if err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			writeJSON(w, http.StatusRequestEntityTooLarge, map[string]string{"error": "template catalog JSON is too large"})
			return
		}
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "could not read template catalog JSON"})
		return
	}
	if !json.Valid(bytes.TrimSpace(body)) {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid template catalog JSON"})
		return
	}
	a.relayAccountAPIWithBody(w, r, http.MethodPut, accountAPIVersion+"/tenants/"+tenant+"/templates", nil, bytes.NewReader(body), "application/json", maxUpstreamBytes, relayExtras{})
}

// The tenant's GitOps source registry — the repository, ref, path, reconciler,
// and cluster a tenant has told central about. These are configuration
// coordinates and nothing else: no credential, no repository content, and no
// reconciliation state crosses this seam in either direction.
func (a *app) handleGetTenantGitOpsSources(w http.ResponseWriter, r *http.Request) {
	tenant := r.PathValue("tenant_id")
	if !pathSegmentRe.MatchString(tenant) {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid tenant id"})
		return
	}
	if r.URL.RawQuery != "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "query parameters are not supported"})
		return
	}
	a.relayAccountAPI(w, r, http.MethodGet, accountAPIVersion+"/tenants/"+tenant+"/gitops/sources", nil)
}

// The registry is replaced whole, carrying the revision it was read at, so
// central can refuse a write composed against a registry someone else has
// already replaced. That refusal is a 409 whose body is the registry as it
// stands now, and the relay hands it back unchanged — the browser needs it to
// show the reader what moved without discarding their edits.
func (a *app) handlePutTenantGitOpsSources(w http.ResponseWriter, r *http.Request) {
	tenant := r.PathValue("tenant_id")
	if !pathSegmentRe.MatchString(tenant) {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid tenant id"})
		return
	}
	if r.URL.RawQuery != "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "query parameters are not supported"})
		return
	}
	mediaType, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || mediaType != "application/json" {
		writeJSON(w, http.StatusUnsupportedMediaType, map[string]string{"error": "Content-Type must be application/json"})
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, maxGitOpsJSONBytes)
	body, err := io.ReadAll(r.Body)
	if err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			writeJSON(w, http.StatusRequestEntityTooLarge, map[string]string{"error": "GitOps source registry JSON is too large"})
			return
		}
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "could not read GitOps source registry JSON"})
		return
	}
	if !json.Valid(bytes.TrimSpace(body)) {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid GitOps source registry JSON"})
		return
	}
	a.relayAccountAPIWithBody(w, r, http.MethodPut, accountAPIVersion+"/tenants/"+tenant+"/gitops/sources", nil, bytes.NewReader(body), "application/json", maxUpstreamBytes, relayExtras{})
}

func (a *app) handlePlacementPreview(w http.ResponseWriter, r *http.Request) {
	tenant, ok := workloadTenantSegment(w, r)
	if !ok {
		return
	}
	mediaType, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || mediaType != "text/yaml" {
		writeJSON(w, http.StatusUnsupportedMediaType, map[string]string{"error": "Content-Type must be text/yaml"})
		return
	}
	cluster, ok := clusterIDValue(r)
	if !ok {
		writeJSON(w, http.StatusBadRequest, map[string]string{
			"error":   "invalid_cluster_id",
			"message": "X-Cluster-ID must be a single cluster id.",
		})
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, maxWorkloadBytes)
	body, err := io.ReadAll(r.Body)
	if err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			writeJSON(w, http.StatusRequestEntityTooLarge, map[string]string{"error": "workload YAML is too large"})
			return
		}
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "could not read workload YAML"})
		return
	}
	reqHeaders := map[string]string{}
	if cluster != "" {
		reqHeaders[clusterIDHeader] = cluster
	}
	a.relayAccountAPIWithBody(w, r, http.MethodPost, accountAPIVersion+"/tenants/"+tenant+"/placement-preview", nil, bytes.NewReader(body), "text/yaml", maxUpstreamBytes, relayExtras{
		reqHeaders: reqHeaders,
	})
}

func catalogPublisherSegments(w http.ResponseWriter, r *http.Request, withPublisher bool) (string, string, bool) {
	tenant := r.PathValue("tenant_id")
	if !pathSegmentRe.MatchString(tenant) {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid tenant id"})
		return "", "", false
	}
	publisher := ""
	if withPublisher {
		publisher = r.PathValue("publisher_id")
		if !pathSegmentRe.MatchString(publisher) {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid publisher id"})
			return "", "", false
		}
	}
	if r.URL.RawQuery != "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "query parameters are not supported"})
		return "", "", false
	}
	return tenant, publisher, true
}

func (a *app) handleListCatalogPublishers(w http.ResponseWriter, r *http.Request) {
	tenant, _, ok := catalogPublisherSegments(w, r, false)
	if !ok {
		return
	}
	a.relayAccountAPIWithBody(w, r, http.MethodGet, accountAPIVersion+"/tenants/"+tenant+"/catalog-publishers", nil, nil, "", maxUpstreamBytes, relayExtras{validateResp: catalogPublisherListResponseValidator})
}

func (a *app) handleCreateCatalogPublisher(w http.ResponseWriter, r *http.Request) {
	tenant, _, ok := catalogPublisherSegments(w, r, false)
	if !ok {
		return
	}
	if len(r.Header.Values("Content-Type")) != 1 || r.Header.Get("Content-Type") != "application/json" {
		writeJSON(w, http.StatusUnsupportedMediaType, map[string]string{"error": "Content-Type must be application/json"})
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, maxPublisherJSONBytes)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	var body *createCatalogPublisherReq
	if err := dec.Decode(&body); err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			writeJSON(w, http.StatusRequestEntityTooLarge, map[string]string{"error": "catalog publisher JSON is too large"})
			return
		}
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid catalog publisher JSON"})
		return
	}
	if body == nil || dec.Decode(&struct{}{}) != io.EOF {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid catalog publisher JSON"})
		return
	}
	body.Name = strings.TrimSpace(body.Name)
	if body.Name == "" || len(body.Name) > 100 || !utf8.ValidString(body.Name) || strings.IndexFunc(body.Name, unicode.IsControl) >= 0 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "publisher name must be 1 to 100 UTF-8 bytes with no control characters"})
		return
	}
	out, _ := json.Marshal(body)
	a.relayAccountAPIWithBody(w, r, http.MethodPost, accountAPIVersion+"/tenants/"+tenant+"/catalog-publishers", nil, bytes.NewReader(out), "application/json", maxUpstreamBytes, relayExtras{validateResp: catalogPublisherCredentialResponseValidator})
}

func (a *app) handleRotateCatalogPublisherCredential(w http.ResponseWriter, r *http.Request) {
	tenant, publisher, ok := catalogPublisherSegments(w, r, true)
	if !ok {
		return
	}
	a.relayAccountAPIWithBody(w, r, http.MethodPost, accountAPIVersion+"/tenants/"+tenant+"/catalog-publishers/"+publisher+"/credential", nil, nil, "", maxUpstreamBytes, relayExtras{validateResp: catalogPublisherCredentialResponseValidator})
}

func (a *app) handleDeleteCatalogPublisher(w http.ResponseWriter, r *http.Request) {
	tenant, publisher, ok := catalogPublisherSegments(w, r, true)
	if !ok {
		return
	}
	a.relayAccountAPIWithBody(w, r, http.MethodDelete, accountAPIVersion+"/tenants/"+tenant+"/catalog-publishers/"+publisher, nil, nil, "", maxUpstreamBytes, relayExtras{validateResp: catalogPublisherDeleteResponseValidator})
}

const catalogPublisherCommand = "yscale catalog apply --server https://central.example.com --tenant TENANT_ID -f catalog.yaml"

func decodeExactJSONObject(data []byte) (map[string]json.RawMessage, error) {
	dec := json.NewDecoder(bytes.NewReader(data))
	opening, err := dec.Token()
	if err != nil || opening != json.Delim('{') {
		return nil, errors.New("expected object")
	}
	fields := map[string]json.RawMessage{}
	for dec.More() {
		key, err := dec.Token()
		if err != nil {
			return nil, err
		}
		name, ok := key.(string)
		if !ok || fields[name] != nil {
			return nil, errors.New("duplicate field")
		}
		var raw json.RawMessage
		if err := dec.Decode(&raw); err != nil {
			return nil, err
		}
		fields[name] = raw
	}
	closing, err := dec.Token()
	if err != nil || closing != json.Delim('}') {
		return nil, errors.New("unclosed object")
	}
	if dec.Decode(&struct{}{}) != io.EOF {
		return nil, errors.New("trailing JSON")
	}
	return fields, nil
}

func safePublisherRecord(raw json.RawMessage) bool {
	fields, err := decodeExactJSONObject(raw)
	if err != nil || len(fields) != 4 {
		return false
	}
	for _, key := range []string{"id", "name", "created_at", "updated_at"} {
		if fields[key] == nil {
			return false
		}
	}
	var id, name, created, updated string
	if json.Unmarshal(fields["id"], &id) != nil || json.Unmarshal(fields["name"], &name) != nil || json.Unmarshal(fields["created_at"], &created) != nil || json.Unmarshal(fields["updated_at"], &updated) != nil {
		return false
	}
	if !pathSegmentRe.MatchString(id) || name == "" || len(name) > 100 || !utf8.ValidString(name) || strings.IndexFunc(name, unicode.IsControl) >= 0 {
		return false
	}
	_, createErr := time.Parse(time.RFC3339, created)
	_, updateErr := time.Parse(time.RFC3339, updated)
	return createErr == nil && updateErr == nil
}

func safePublisherError(data []byte) bool {
	var value any
	return json.Unmarshal(data, &value) == nil && !containsSecretField(value)
}

func catalogPublisherListResponseValidator(data []byte, status int) bool {
	if status < 200 || status >= 300 {
		return safePublisherError(data)
	}
	fields, err := decodeExactJSONObject(data)
	if err != nil || len(fields) != 1 || fields["publishers"] == nil {
		return false
	}
	var publishers []json.RawMessage
	if json.Unmarshal(fields["publishers"], &publishers) != nil || len(publishers) > 100 {
		return false
	}
	seen := map[string]bool{}
	for _, raw := range publishers {
		if !safePublisherRecord(raw) {
			return false
		}
		var record struct {
			ID string `json:"id"`
		}
		_ = json.Unmarshal(raw, &record)
		if seen[record.ID] {
			return false
		}
		seen[record.ID] = true
	}
	return true
}

func catalogPublisherCredentialResponseValidator(data []byte, status int) bool {
	if status < 200 || status >= 300 {
		return safePublisherError(data)
	}
	fields, err := decodeExactJSONObject(data)
	if err != nil || (len(fields) != 2 && len(fields) != 3) || fields["publisher"] == nil || fields["token"] == nil {
		return false
	}
	if len(fields) == 3 && fields["command"] == nil {
		return false
	}
	if !safePublisherRecord(fields["publisher"]) {
		return false
	}
	var token string
	if json.Unmarshal(fields["token"], &token) != nil || token == "" || len(token) > maxBearerLen || !printableASCII(token) {
		return false
	}
	if fields["command"] != nil {
		var command string
		if json.Unmarshal(fields["command"], &command) != nil || command != catalogPublisherCommand {
			return false
		}
	}
	return true
}

func catalogPublisherDeleteResponseValidator(data []byte, status int) bool {
	if status < 200 || status >= 300 {
		return safePublisherError(data)
	}
	fields, err := decodeExactJSONObject(data)
	if err != nil || len(fields) != 2 || fields["publisher_id"] == nil || fields["deleted"] == nil {
		return false
	}
	var publisherID string
	var deleted bool
	return json.Unmarshal(fields["publisher_id"], &publisherID) == nil && pathSegmentRe.MatchString(publisherID) &&
		json.Unmarshal(fields["deleted"], &deleted) == nil && deleted
}

func automationTenant(w http.ResponseWriter, r *http.Request) (string, bool) {
	tenant := r.PathValue("tenant_id")
	if !pathSegmentRe.MatchString(tenant) {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid tenant id"})
		return "", false
	}
	if r.URL.RawQuery != "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "query parameters are not supported"})
		return "", false
	}
	return tenant, true
}

func (a *app) handleGetAutomationTenantTemplates(w http.ResponseWriter, r *http.Request) {
	tenant, ok := automationTenant(w, r)
	if !ok {
		return
	}
	a.relayAccountAPI(w, r, http.MethodGet, accountAPIVersion+"/automation/tenants/"+tenant+"/templates", nil)
}

func (a *app) handlePutAutomationTenantTemplates(w http.ResponseWriter, r *http.Request) {
	tenant, ok := automationTenant(w, r)
	if !ok {
		return
	}
	if len(r.Header.Values("Content-Type")) != 1 || r.Header.Get("Content-Type") != "application/json" {
		writeJSON(w, http.StatusUnsupportedMediaType, map[string]string{"error": "Content-Type must be application/json"})
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, maxCatalogJSONBytes)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	var body *automationCatalogReq
	if err := dec.Decode(&body); err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			writeJSON(w, http.StatusRequestEntityTooLarge, map[string]string{"error": "template catalog JSON is too large"})
			return
		}
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid template catalog JSON"})
		return
	}
	if body == nil || dec.Decode(&struct{}{}) != io.EOF {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid template catalog JSON"})
		return
	}
	var revision any
	var templates []any
	if json.Unmarshal(body.CatalogRevision, &revision) != nil || json.Unmarshal(body.Templates, &templates) != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid template catalog JSON"})
		return
	}
	out, _ := json.Marshal(map[string]any{"catalog_revision": revision, "templates": templates})
	a.relayAccountAPIWithBody(w, r, http.MethodPut, accountAPIVersion+"/automation/tenants/"+tenant+"/templates", nil, bytes.NewReader(out), "application/json", maxUpstreamBytes, relayExtras{})
}

// The Linode credential is write-only. This route validates the exact request
// document and the safe response envelope on both sides of the account seam;
// an upstream regression therefore cannot reflect a token or ciphertext into
// the browser even if central accidentally includes one.
func (a *app) handleLinodeCloudAccount(w http.ResponseWriter, r *http.Request) {
	tenant := r.PathValue("tenant_id")
	if !pathSegmentRe.MatchString(tenant) {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid tenant id"})
		return
	}
	if r.URL.RawQuery != "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "query parameters are not supported"})
		return
	}
	path := accountAPIVersion + "/tenants/" + tenant + "/cloud-accounts/linode"
	extra := relayExtras{validateResp: linodeCloudAccountResponseValidator(tenant)}
	switch r.Method {
	case http.MethodGet:
		a.relayAccountAPIWithBody(w, r, http.MethodGet, path, nil, http.NoBody, "", maxUpstreamBytes, extra)
	case http.MethodDelete:
		body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 1))
		if err != nil || len(body) != 0 {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "disconnect request body must be empty"})
			return
		}
		a.relayAccountAPIWithBody(w, r, http.MethodDelete, path, nil, http.NoBody, "", maxUpstreamBytes, extra)
	case http.MethodPut:
		mediaType, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
		if err != nil || mediaType != "application/json" {
			writeJSON(w, http.StatusUnsupportedMediaType, map[string]string{"error": "Content-Type must be application/json"})
			return
		}
		r.Body = http.MaxBytesReader(w, r.Body, maxCloudAccountJSONBytes)
		dec := json.NewDecoder(r.Body)
		dec.DisallowUnknownFields()
		var in linodeCloudAccountReq
		if err := dec.Decode(&in); err != nil {
			var tooLarge *http.MaxBytesError
			if errors.As(err, &tooLarge) {
				writeJSON(w, http.StatusRequestEntityTooLarge, map[string]string{"error": "cloud account JSON is too large"})
			} else {
				writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid cloud account JSON"})
			}
			return
		}
		if err := dec.Decode(&struct{}{}); err != io.EOF {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid cloud account JSON"})
			return
		}
		if !boundedCloudAccountValue(in.Token, 1, 4096) || !boundedCloudAccountValue(in.Region, 1, 128) ||
			!boundedCloudAccountValue(in.CPUImage, 0, 512) || !boundedCloudAccountValue(in.GPUImage, 0, 512) {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid cloud account fields"})
			return
		}
		body, err := json.Marshal(in)
		if err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid cloud account JSON"})
			return
		}
		a.relayAccountAPIWithBody(w, r, http.MethodPut, path, nil, bytes.NewReader(body), "application/json", maxUpstreamBytes, extra)
	default:
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
	}
}

func boundedCloudAccountValue(value string, min, max int) bool {
	if len(value) < min || len(value) > max || strings.TrimSpace(value) != value {
		return false
	}
	return utf8.ValidString(value) && !strings.ContainsAny(value, "\r\n\x00")
}

// Runtime binding values cross this process once, from the caller to central.
// Only summary-shaped responses may cross back; the validator rejects secret-
// like fields recursively on success and error paths.
func (a *app) handleRuntimeBindings(w http.ResponseWriter, r *http.Request) {
	tenant := r.PathValue("tenant_id")
	if !pathSegmentRe.MatchString(tenant) {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid tenant id"})
		return
	}
	if r.URL.RawQuery != "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "query parameters are not supported"})
		return
	}
	path := accountAPIVersion + "/tenants/" + tenant + "/runtime-bindings"
	switch r.Method {
	case http.MethodGet:
		body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 1))
		if err != nil || len(body) != 0 {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "list request body must be empty"})
			return
		}
		a.relayAccountAPIWithBody(w, r, http.MethodGet, path, nil, http.NoBody, "", maxRuntimeBindingsRespBytes, relayExtras{validateResp: runtimeBindingListResponseValidator(tenant)})
	case http.MethodPut:
		key := r.PathValue("binding_key")
		if !runtimeBindingKeyRe.MatchString(key) {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid runtime binding key"})
			return
		}
		if len(r.Header.Values("Content-Type")) != 1 || r.Header.Get("Content-Type") != "application/json" {
			writeJSON(w, http.StatusUnsupportedMediaType, map[string]string{"error": "Content-Type must be application/json"})
			return
		}
		r.Body = http.MaxBytesReader(w, r.Body, maxRuntimeBindingJSONBytes)
		dec := json.NewDecoder(r.Body)
		var in *runtimeBindingReq
		if err := dec.Decode(&in); err != nil {
			var tooLarge *http.MaxBytesError
			if errors.As(err, &tooLarge) {
				writeJSON(w, http.StatusRequestEntityTooLarge, map[string]string{"error": "runtime binding JSON is too large"})
			} else {
				writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid runtime binding JSON"})
			}
			return
		}
		if in == nil || dec.Decode(&struct{}{}) != io.EOF || !validRuntimeBindingName(in.Name) || !validRuntimeBindingValue(in.Value) {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid runtime binding fields"})
			return
		}
		out, _ := json.Marshal(in)
		a.relayAccountAPIWithBody(w, r, http.MethodPut, path+"/"+key, nil, bytes.NewReader(out), "application/json", maxRuntimeBindingsRespBytes, relayExtras{validateResp: runtimeBindingItemResponseValidator(tenant)})
	case http.MethodDelete:
		key := r.PathValue("binding_key")
		if !runtimeBindingKeyRe.MatchString(key) {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid runtime binding key"})
			return
		}
		body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 1))
		if err != nil || len(body) != 0 {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "delete request body must be empty"})
			return
		}
		a.relayAccountAPIWithBody(w, r, http.MethodDelete, path+"/"+key, nil, http.NoBody, "", maxRuntimeBindingsRespBytes, relayExtras{validateResp: runtimeBindingDeleteResponseValidator(key)})
	default:
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
	}
}

func validRuntimeBindingName(value string) bool {
	return value != "" && len(value) <= 100 && utf8.ValidString(value) && strings.TrimSpace(value) == value && strings.IndexFunc(value, unicode.IsControl) < 0
}

func validRuntimeBindingValue(value string) bool {
	return len(value) >= 1 && len(value) <= 8192 && utf8.ValidString(value) && !strings.ContainsRune(value, '\x00')
}

func containsRuntimeSecretField(value any) bool {
	switch typed := value.(type) {
	case map[string]any:
		for key, child := range typed {
			name := strings.ToLower(key)
			for _, part := range []string{"value", "cipher", "nonce", "hash", "secret", "credential", "token"} {
				if strings.Contains(name, part) {
					return true
				}
			}
			if containsRuntimeSecretField(child) {
				return true
			}
		}
	case []any:
		for _, child := range typed {
			if containsRuntimeSecretField(child) {
				return true
			}
		}
	}
	return false
}

func safeRuntimeBindingError(raw any) bool {
	obj, ok := raw.(map[string]any)
	if !ok || len(obj) == 0 || !exactJSONKeys(obj, "error", "message") {
		return false
	}
	for _, value := range obj {
		if _, ok := value.(string); !ok {
			return false
		}
	}
	return true
}

func safeRuntimeBinding(raw any) bool {
	obj, ok := raw.(map[string]any)
	if !ok || len(obj) != 7 || !exactJSONKeys(obj, "id", "key", "name", "revision", "sync_state", "created_at", "updated_at") {
		return false
	}
	id, idOK := obj["id"].(string)
	key, keyOK := obj["key"].(string)
	name, nameOK := obj["name"].(string)
	revision, revisionOK := obj["revision"].(string)
	syncState, syncOK := obj["sync_state"].(string)
	created, createdOK := obj["created_at"].(string)
	updated, updatedOK := obj["updated_at"].(string)
	if !idOK || !keyOK || !nameOK || !revisionOK || !syncOK || !createdOK || !updatedOK || !pathSegmentRe.MatchString(id) || !runtimeBindingKeyRe.MatchString(key) || !validRuntimeBindingName(name) || !runtimeBindingRevisionRe.MatchString(revision) {
		return false
	}
	if syncState != "pending" && syncState != "synced" && syncState != "error" {
		return false
	}
	_, createErr := time.Parse(time.RFC3339, created)
	_, updateErr := time.Parse(time.RFC3339, updated)
	return createErr == nil && updateErr == nil
}

func runtimeBindingListResponseValidator(tenant string) func([]byte, int) bool {
	return func(body []byte, status int) bool {
		var raw any
		if json.Unmarshal(body, &raw) != nil || containsRuntimeSecretField(raw) {
			return false
		}
		if status < 200 || status >= 300 {
			return safeRuntimeBindingError(raw)
		}
		obj, ok := raw.(map[string]any)
		if !ok || len(obj) != 3 || !exactJSONKeys(obj, "tenant_id", "role", "bindings") || obj["tenant_id"] != tenant {
			return false
		}
		role, roleOK := obj["role"].(string)
		rows, rowsOK := obj["bindings"].([]any)
		if !roleOK || !memberRoles[role] || !rowsOK || len(rows) > 32 {
			return false
		}
		seen := map[string]bool{}
		for _, row := range rows {
			if !safeRuntimeBinding(row) {
				return false
			}
			key := row.(map[string]any)["key"].(string)
			if seen[key] {
				return false
			}
			seen[key] = true
		}
		return true
	}
}

func runtimeBindingItemResponseValidator(tenant string) func([]byte, int) bool {
	return func(body []byte, status int) bool {
		var raw any
		if json.Unmarshal(body, &raw) != nil || containsRuntimeSecretField(raw) {
			return false
		}
		if status < 200 || status >= 300 {
			return safeRuntimeBindingError(raw)
		}
		obj, ok := raw.(map[string]any)
		if !ok || len(obj) != 3 || !exactJSONKeys(obj, "tenant_id", "role", "binding") || obj["tenant_id"] != tenant {
			return false
		}
		role, roleOK := obj["role"].(string)
		return roleOK && memberRoles[role] && safeRuntimeBinding(obj["binding"])
	}
}

func runtimeBindingDeleteResponseValidator(key string) func([]byte, int) bool {
	return func(body []byte, status int) bool {
		var raw any
		if json.Unmarshal(body, &raw) != nil || containsRuntimeSecretField(raw) {
			return false
		}
		if status < 200 || status >= 300 {
			return safeRuntimeBindingError(raw)
		}
		obj, ok := raw.(map[string]any)
		if !ok || len(obj) != 2 || !exactJSONKeys(obj, "key", "deleted") {
			return false
		}
		deleted, deletedOK := obj["deleted"].(bool)
		return obj["key"] == key && deletedOK && deleted
	}
}

func emptyProxyRequest(w http.ResponseWriter, r *http.Request) bool {
	if r.URL.RawQuery != "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "query parameters are not supported"})
		return false
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 1))
	if err != nil || len(body) != 0 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "request body must be empty"})
		return false
	}
	return true
}

func safeMicroUSD(raw any) bool {
	value, ok := raw.(float64)
	return ok && value >= 0 && value <= 9007199254740991 && math.Trunc(value) == value
}

func safePositiveJSONInt(raw any) bool {
	value, ok := raw.(float64)
	return ok && value > 0 && value <= 9007199254740991 && math.Trunc(value) == value
}

func safeBillingHold(raw any) bool {
	obj, ok := raw.(map[string]any)
	if !ok || len(obj) != 5 || !exactJSONKeys(obj, "id", "workload_id", "amount_micro_usd", "expires_at", "created_at") {
		return false
	}
	workload, workloadOK := obj["workload_id"].(string)
	expires, expiresOK := obj["expires_at"].(string)
	created, createdOK := obj["created_at"].(string)
	if !safePositiveJSONInt(obj["id"]) || !workloadOK || !expiresOK || !createdOK || !pathSegmentRe.MatchString(workload) || !safeMicroUSD(obj["amount_micro_usd"]) {
		return false
	}
	_, expiresErr := time.Parse(time.RFC3339, expires)
	_, createdErr := time.Parse(time.RFC3339, created)
	return expiresErr == nil && createdErr == nil
}

func containsUnsafeBillingField(raw any) bool {
	unsafe := []string{"payment", "provider", "card", "invoice", "customer", "cipher", "nonce", "secret", "credential", "token"}
	switch value := raw.(type) {
	case map[string]any:
		for key, child := range value {
			lower := strings.ToLower(key)
			for _, fragment := range unsafe {
				if strings.Contains(lower, fragment) {
					return true
				}
			}
			if containsUnsafeBillingField(child) {
				return true
			}
		}
	case []any:
		for _, child := range value {
			if containsUnsafeBillingField(child) {
				return true
			}
		}
	}
	return false
}

func safeBillingError(raw any) bool {
	obj, ok := raw.(map[string]any)
	if !ok || len(obj) == 0 || !exactJSONKeys(obj, "error", "message") || containsUnsafeBillingField(raw) {
		return false
	}
	for _, value := range obj {
		if _, ok := value.(string); !ok {
			return false
		}
	}
	return true
}

func tenantBillingResponseValidator(tenant string) func([]byte, int) bool {
	return func(body []byte, status int) bool {
		var raw any
		if json.Unmarshal(body, &raw) != nil || containsUnsafeBillingField(raw) {
			return false
		}
		if status < 200 || status >= 300 {
			return safeBillingError(raw)
		}
		obj, ok := raw.(map[string]any)
		if !ok || (len(obj) != 8 && len(obj) != 9) || !exactJSONKeys(obj, "tenant_id", "currency", "balance_micro_usd", "held_micro_usd", "spendable_micro_usd", "debt_micro_usd", "frozen", "updated_at", "open_holds") || obj["tenant_id"] != tenant || obj["currency"] != "USD" {
			return false
		}
		for _, key := range []string{"balance_micro_usd", "held_micro_usd", "spendable_micro_usd", "debt_micro_usd"} {
			if !safeMicroUSD(obj[key]) {
				return false
			}
		}
		if _, ok := obj["frozen"].(bool); !ok {
			return false
		}
		if updated, exists := obj["updated_at"]; exists {
			stamp, ok := updated.(string)
			if !ok || func() bool { _, err := time.Parse(time.RFC3339, stamp); return err == nil }() == false {
				return false
			}
		}
		holds, ok := obj["open_holds"].([]any)
		if !ok || len(holds) > 100 {
			return false
		}
		seen := map[float64]bool{}
		for _, hold := range holds {
			if !safeBillingHold(hold) {
				return false
			}
			id := hold.(map[string]any)["id"].(float64)
			if seen[id] {
				return false
			}
			seen[id] = true
		}
		return true
	}
}

func serviceCreditResponseValidator(tenant string, amount int64, key string) func([]byte, int) bool {
	return func(body []byte, status int) bool {
		var raw any
		if json.Unmarshal(body, &raw) != nil || containsUnsafeBillingField(raw) {
			return false
		}
		if status < 200 || status >= 300 {
			return safeBillingError(raw)
		}
		obj, ok := raw.(map[string]any)
		granted, grantedOK := obj["granted"].(bool)
		return ok && len(obj) == 5 && exactJSONKeys(obj, "tenant_id", "amount_micro_usd", "currency", "idempotency_key", "granted") && obj["tenant_id"] == tenant && obj["currency"] == "USD" && obj["idempotency_key"] == key && safeMicroUSD(obj["amount_micro_usd"]) && obj["amount_micro_usd"] == float64(amount) && grantedOK && granted
	}
}

func (a *app) handleTenantBilling(w http.ResponseWriter, r *http.Request) {
	tenant := r.PathValue("tenant_id")
	if !pathSegmentRe.MatchString(tenant) {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid tenant id"})
		return
	}
	if !emptyProxyRequest(w, r) {
		return
	}
	a.relayAccountAPIWithBody(w, r, http.MethodGet, accountAPIVersion+"/tenants/"+tenant+"/billing", nil, http.NoBody, "", maxBillingRespBytes, relayExtras{validateResp: tenantBillingResponseValidator(tenant)})
}

func (a *app) handleOperatorServiceCredit(w http.ResponseWriter, r *http.Request) {
	tenant := r.PathValue("tenant_id")
	if !pathSegmentRe.MatchString(tenant) || r.URL.RawQuery != "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid service credit request"})
		return
	}
	if len(r.Header.Values("Content-Type")) != 1 || r.Header.Get("Content-Type") != "application/json" {
		writeJSON(w, http.StatusUnsupportedMediaType, map[string]string{"error": "Content-Type must be application/json"})
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, maxServiceCreditJSONBytes)
	dec := json.NewDecoder(r.Body)
	var in *serviceCreditReq
	if err := dec.Decode(&in); err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			writeJSON(w, http.StatusRequestEntityTooLarge, map[string]string{"error": "service credit JSON is too large"})
		} else {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid service credit JSON"})
		}
		return
	}
	if in == nil || dec.Decode(&struct{}{}) != io.EOF || in.AmountMicroUSD <= 0 || in.AmountMicroUSD > 9007199254740991 || !serviceCreditKeyRe.MatchString(in.IdempotencyKey) {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid service credit fields"})
		return
	}
	body, _ := json.Marshal(in)
	path := accountAPIVersion + "/operator/tenants/" + tenant + "/billing/service-credits"
	a.relayAccountAPIWithBody(w, r, http.MethodPost, path, nil, bytes.NewReader(body), "application/json", maxBillingRespBytes, relayExtras{validateResp: serviceCreditResponseValidator(tenant, in.AmountMicroUSD, in.IdempotencyKey)})
}

func linodeCloudAccountResponseValidator(tenant string) func([]byte, int) bool {
	return func(body []byte, status int) bool {
		var raw any
		if json.Unmarshal(body, &raw) != nil || containsSecretField(raw) {
			return false
		}
		if status < 200 || status >= 300 {
			obj, ok := raw.(map[string]any)
			if !ok {
				return false
			}
			for key, value := range obj {
				if key != "error" && key != "message" {
					return false
				}
				if _, ok := value.(string); !ok {
					return false
				}
			}
			return len(obj) > 0
		}
		obj, ok := raw.(map[string]any)
		_, hasTenant := obj["tenant_id"]
		_, hasRole := obj["role"]
		_, hasAccount := obj["account"]
		if !ok || (len(obj) != 3 && len(obj) != 4) || !hasTenant || !hasRole || !hasAccount ||
			!exactJSONKeys(obj, "tenant_id", "role", "account", "changed") || obj["tenant_id"] != tenant {
			return false
		}
		role, roleOK := obj["role"].(string)
		if !roleOK || !memberRoles[role] {
			return false
		}
		if changed, exists := obj["changed"]; exists {
			if _, ok := changed.(bool); !ok {
				return false
			}
		}
		if obj["account"] == nil {
			return true
		}
		account, ok := obj["account"].(map[string]any)
		if !ok || len(account) != 7 || !exactJSONKeys(account, "id", "provider", "provider_account_id", "region", "cpu_image_ready", "gpu_image_ready", "updated_at") {
			return false
		}
		id, idOK := account["id"].(string)
		providerAccountID, providerAccountIDOK := account["provider_account_id"].(string)
		region, regionOK := account["region"].(string)
		updatedAt, updatedAtOK := account["updated_at"].(string)
		if !idOK || !providerAccountIDOK || !regionOK || !updatedAtOK ||
			!boundedCloudAccountValue(id, 1, 512) || !boundedCloudAccountValue(providerAccountID, 1, 512) ||
			!boundedCloudAccountValue(region, 1, 128) || !boundedCloudAccountValue(updatedAt, 1, 64) {
			return false
		}
		if _, err := time.Parse(time.RFC3339, updatedAt); err != nil {
			return false
		}
		_, cpuOK := account["cpu_image_ready"].(bool)
		_, gpuOK := account["gpu_image_ready"].(bool)
		return account["provider"] == "linode" && cpuOK && gpuOK
	}
}

func exactJSONKeys(obj map[string]any, allowed ...string) bool {
	set := make(map[string]bool, len(allowed))
	for _, key := range allowed {
		set[key] = true
	}
	for key := range obj {
		if !set[key] {
			return false
		}
	}
	return true
}

func containsSecretField(value any) bool {
	switch typed := value.(type) {
	case map[string]any:
		for key, child := range typed {
			name := strings.ToLower(key)
			if strings.Contains(name, "token") || strings.Contains(name, "cipher") || strings.Contains(name, "secret") || strings.Contains(name, "credential") {
				return true
			}
			if containsSecretField(child) {
				return true
			}
		}
	case []any:
		for _, child := range typed {
			if containsSecretField(child) {
				return true
			}
		}
	}
	return false
}

func (a *app) handleCreateWorkload(w http.ResponseWriter, r *http.Request) {
	tenant, ok := workloadTenantSegment(w, r)
	if !ok {
		return
	}
	mediaType, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || mediaType != "text/yaml" {
		writeJSON(w, http.StatusUnsupportedMediaType, map[string]string{"error": "Content-Type must be text/yaml"})
		return
	}
	// A create without a usable key is refused here rather than upstream: a
	// submit that central cannot deduplicate is a paid operation waiting to run
	// twice. The rejection never repeats the key back.
	key, ok := idempotencyKey(r)
	if !ok {
		writeJSON(w, http.StatusBadRequest, map[string]string{
			"error":   "invalid_idempotency_key",
			"message": "Idempotency-Key must be 8 to 255 printable ASCII characters without spaces.",
		})
		return
	}
	// The target cluster is optional on this route and opaque to this server —
	// central decides whether the tenant may launch there. A value that is not
	// the id shape central mints is refused here rather than forwarded, for the
	// same reason a tenant id is: it is about to be handed to another service.
	cluster, ok := clusterIDValue(r)
	if !ok {
		writeJSON(w, http.StatusBadRequest, map[string]string{
			"error":   "invalid_cluster_id",
			"message": "X-Cluster-ID must be a single cluster id.",
		})
		return
	}
	// The template a run was launched from is provenance central records on the
	// record, so it travels as a pair or not at all: half a receipt would be
	// stored as an id with no version behind it.
	templateID, templateVersion, ok := templateSelection(r)
	if !ok {
		writeJSON(w, http.StatusBadRequest, map[string]string{
			"error":   "invalid_template_selection",
			"message": "X-Template-ID and X-Template-Version must be sent together, as one id and one positive version.",
		})
		return
	}
	placementToken, ok := placementTokenValue(r)
	if !ok {
		writeJSON(w, http.StatusBadRequest, map[string]string{
			"error":   "invalid_placement_token",
			"message": "X-Yscale-Placement-Token must be exactly one nonblank, printable, bounded header.",
		})
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, maxWorkloadBytes)
	body, err := io.ReadAll(r.Body)
	if err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			writeJSON(w, http.StatusRequestEntityTooLarge, map[string]string{"error": "workload YAML is too large"})
			return
		}
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "could not read workload YAML"})
		return
	}
	reqHeaders := map[string]string{idempotencyKeyHeader: key}
	if cluster != "" {
		reqHeaders[clusterIDHeader] = cluster
	}
	if templateID != "" {
		reqHeaders[templateIDHeader] = templateID
		reqHeaders[templateVersionHeader] = templateVersion
	}
	if placementToken != "" {
		reqHeaders[placementTokenHeader] = placementToken
	}
	a.relayAccountAPIWithBody(w, r, http.MethodPost, accountAPIVersion+"/tenants/"+tenant+"/workloads", nil, bytes.NewReader(body), "text/yaml", maxUpstreamBytes, relayExtras{
		reqHeaders:  reqHeaders,
		respHeaders: []string{idempotencyReplayedHeader, retryAfterHeader},
		timeout:     a.proxy.createDeadline(),
	})
}

// clusterIDValue reads the optional target cluster. Absent is allowed and means
// "central decides"; present means exactly one header carrying one id in the
// same opaque shape every other interpolated segment must satisfy. Two headers
// is not a choice this server gets to make, so it is refused rather than
// resolved.
func clusterIDValue(r *http.Request) (string, bool) {
	values := r.Header.Values(clusterIDHeader)
	if len(values) == 0 {
		return "", true
	}
	if len(values) != 1 || !pathSegmentRe.MatchString(values[0]) {
		return "", false
	}
	return values[0], true
}

// templateSelection reads the optional template receipt. Absent on both headers
// is allowed and means the submission names no catalog entry; present means
// exactly one id in the opaque segment shape and exactly one positive version.
// One without the other is refused rather than half-forwarded.
func templateSelection(r *http.Request) (string, string, bool) {
	ids := r.Header.Values(templateIDHeader)
	versions := r.Header.Values(templateVersionHeader)
	if len(ids) == 0 && len(versions) == 0 {
		return "", "", true
	}
	if len(ids) != 1 || len(versions) != 1 {
		return "", "", false
	}
	if !pathSegmentRe.MatchString(ids[0]) || !templateVersionRe.MatchString(versions[0]) {
		return "", "", false
	}
	return ids[0], versions[0], true
}

// placementTokenValue reads the optional placement token. Absent is allowed;
// present means exactly one header, nonblank, printable ASCII, bounded. The
// value is opaque — this server never interprets, stores, or logs it.
func placementTokenValue(r *http.Request) (string, bool) {
	values := r.Header.Values(placementTokenHeader)
	if len(values) == 0 {
		return "", true
	}
	if len(values) != 1 {
		return "", false
	}
	tok := values[0]
	if tok == "" || len(tok) > maxPlacementTokenLen {
		return "", false
	}
	for i := 0; i < len(tok); i++ {
		if tok[i] < 0x21 || tok[i] > 0x7e {
			return "", false
		}
	}
	return tok, true
}

// idempotencyKey enforces central's contract before the request is worth
// building: exactly one header, 8..255 bytes, printable ASCII with no space.
// The value stays opaque — this server never interprets, stores, or logs it.
func idempotencyKey(r *http.Request) (string, bool) {
	values := r.Header.Values(idempotencyKeyHeader)
	if len(values) != 1 {
		return "", false
	}
	key := values[0]
	if len(key) < minIdempotencyKeyLen || len(key) > maxIdempotencyKeyLen {
		return "", false
	}
	for i := 0; i < len(key); i++ {
		if key[i] < 0x21 || key[i] > 0x7e {
			return "", false
		}
	}
	return key, true
}

func (a *app) handleGetWorkload(w http.ResponseWriter, r *http.Request) {
	tenant, id, ok := workloadPathSegments(w, r)
	if !ok {
		return
	}
	a.relayAccountAPI(w, r, http.MethodGet, accountAPIVersion+"/tenants/"+tenant+"/workloads/"+id, nil)
}

func (a *app) handleGetWorkloadLogs(w http.ResponseWriter, r *http.Request) {
	tenant, id, ok := workloadPathSegments(w, r)
	if !ok {
		return
	}
	for _, header := range []string{clusterIDHeader, "X-Namespace", "X-Container"} {
		if len(r.Header.Values(header)) != 0 {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "workload log selectors are not accepted"})
			return
		}
	}
	r.Body = http.MaxBytesReader(w, r.Body, 1)
	body, err := io.ReadAll(r.Body)
	if err != nil || len(body) != 0 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "request body is not accepted"})
		return
	}
	query := r.URL.Query()
	for key := range query {
		if key != "tail" {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "only the tail query parameter is supported"})
			return
		}
	}
	forward := url.Values{}
	if values, exists := query["tail"]; exists {
		if len(values) != 1 {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "tail must be one integer from 1 to 1000"})
			return
		}
		tail, err := strconv.ParseInt(values[0], 10, 64)
		if err != nil || tail < 1 || tail > 1000 {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "tail must be one integer from 1 to 1000"})
			return
		}
		forward.Set("tail", strconv.FormatInt(tail, 10))
	}
	a.relayAccountAPIWithBody(w, r, http.MethodGet,
		accountAPIVersion+"/tenants/"+tenant+"/workloads/"+id+"/logs",
		forward, nil, "", maxWorkloadLogsUpstreamBytes, relayExtras{timeout: workloadLogTimeout})
}

func (a *app) handleRetryWorkload(w http.ResponseWriter, r *http.Request) {
	tenant, id, ok := workloadPathSegments(w, r)
	if !ok {
		return
	}
	if r.URL.RawQuery != "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "query parameters are not supported"})
		return
	}
	if len(r.Header.Values(clusterIDHeader)) != 0 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "X-Cluster-ID is not accepted on workload retry"})
		return
	}
	// A retry carries the source record's own provenance forward, so naming a
	// template here would relabel a run this console did not compose.
	if len(r.Header.Values(templateIDHeader)) != 0 || len(r.Header.Values(templateVersionHeader)) != 0 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "template selection headers are not accepted on workload retry"})
		return
	}
	key, ok := idempotencyKey(r)
	if !ok {
		writeJSON(w, http.StatusBadRequest, map[string]string{
			"error":   "invalid_idempotency_key",
			"message": "Idempotency-Key must be 8 to 255 printable ASCII characters without spaces.",
		})
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, 1)
	body, err := io.ReadAll(r.Body)
	if err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "request body is not accepted"})
			return
		}
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "could not read request body"})
		return
	}
	if len(body) != 0 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "request body is not accepted"})
		return
	}
	a.relayAccountAPIWithBody(w, r, http.MethodPost, accountAPIVersion+"/tenants/"+tenant+"/workloads/"+id+"/retry", nil, http.NoBody, "", maxUpstreamBytes, relayExtras{
		reqHeaders:  map[string]string{idempotencyKeyHeader: key},
		respHeaders: []string{idempotencyReplayedHeader, retryAfterHeader},
		timeout:     a.proxy.createDeadline(),
	})
}

func (a *app) handleCancelWorkload(w http.ResponseWriter, r *http.Request) {
	tenant, id, ok := workloadPathSegments(w, r)
	if !ok {
		return
	}
	a.relayAccountAPI(w, r, http.MethodDelete, accountAPIVersion+"/tenants/"+tenant+"/workloads/"+id, nil)
}

func workloadTenantSegment(w http.ResponseWriter, r *http.Request) (string, bool) {
	tenant := r.PathValue("tenant_id")
	if !pathSegmentRe.MatchString(tenant) {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid tenant id"})
		return "", false
	}
	return tenant, true
}

func workloadPathSegments(w http.ResponseWriter, r *http.Request) (string, string, bool) {
	tenant, ok := workloadTenantSegment(w, r)
	if !ok {
		return "", "", false
	}
	id := r.PathValue("workload_id")
	if !pathSegmentRe.MatchString(id) {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid workload id"})
		return "", "", false
	}
	return tenant, id, true
}

// pageQuery keeps limit and after and discards everything else. An out-of-range
// limit or a cursor that is not cursor-shaped is a client bug, so it is
// rejected rather than silently dropped. The cursor itself is opaque here and
// travels verbatim: rewriting it would hand back a page nobody asked for.
//
// maxLimit is the ceiling of the route being proxied, not one number for the
// seam — refusing a limit the upstream would have answered is this server
// inventing a rule.
func pageQuery(in url.Values, maxLimit int) (url.Values, bool) {
	out := url.Values{}
	if v := in.Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 || n > maxLimit {
			return nil, false
		}
		out.Set("limit", strconv.Itoa(n))
	}
	if v := in.Get("after"); v != "" {
		if !cursorRe.MatchString(v) {
			return nil, false
		}
		out.Set("after", v)
	}
	return out, true
}

func decodeMemberJSON(w http.ResponseWriter, r *http.Request, out any) bool {
	mediaType, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || mediaType != "application/json" {
		writeJSON(w, http.StatusUnsupportedMediaType, map[string]string{"error": "Content-Type must be application/json"})
		return false
	}
	r.Body = http.MaxBytesReader(w, r.Body, maxMemberJSONBytes)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(out); err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			writeJSON(w, http.StatusRequestEntityTooLarge, map[string]string{"error": "member JSON is too large"})
			return false
		}
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid member JSON"})
		return false
	}
	if err := dec.Decode(&struct{}{}); err != io.EOF {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid member JSON"})
		return false
	}
	return true
}

// relayAccountAPI forwards one account-API call. The caller's bearer is the
// only thing carried over; cookies, referers, forwarded-for hops, and every
// other inbound header stay on this side of the seam.
func (a *app) relayAccountAPI(w http.ResponseWriter, r *http.Request, method, path string, query url.Values) {
	a.relayAccountAPIWithBody(w, r, method, path, query, nil, "", maxUpstreamBytes, relayExtras{})
}

// relayExtras carries the ways one route may differ from the seam's defaults:
// the caller headers it is allowed to forward, the upstream headers it is
// allowed to hand back, and a deadline where the route has earned a longer one.
// The zero value is the default seam, which is what every route but the
// workload create uses. Both allowlists are explicit — nothing reaches central
// or the browser because a caller or an upstream simply asked.
type relayExtras struct {
	reqHeaders   map[string]string
	respHeaders  []string
	timeout      time.Duration
	validateResp func([]byte, int) bool
}

// deadline picks the route's own bound where it has one and the seam's
// otherwise. A zero on both sides would mean "already expired", so the constant
// is the floor.
func (p *accountProxy) deadline(routeTimeout time.Duration) time.Duration {
	if routeTimeout > 0 {
		return routeTimeout
	}
	if p != nil && p.timeout > 0 {
		return p.timeout
	}
	return upstreamTimeout
}

// createDeadline is nil-safe on purpose: an unconfigured proxy answers 503 in
// the relay, and reading the route's deadline must not be what panics first.
func (p *accountProxy) createDeadline() time.Duration {
	if p == nil || p.createTimeout <= 0 {
		return workloadCreateTimeout
	}
	return p.createTimeout
}

// maxRespBytes is the ceiling for this route's answer. It is a parameter rather
// than one constant because a route's honest largest page is a property of the
// route, not of the seam.
func (a *app) relayAccountAPIWithBody(w http.ResponseWriter, r *http.Request, method, path string, query url.Values, body io.Reader, contentType string, maxRespBytes int, extra relayExtras) {
	if a.proxy == nil || a.proxy.cloudURL == "" {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "account service is not configured"})
		return
	}
	if !a.allowAccountRequest(w, r) {
		return
	}
	bearer, ok := bearerCredential(r)
	if !ok {
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "missing bearer token"})
		return
	}

	target := a.proxy.cloudURL + path
	if len(query) > 0 {
		target += "?" + query.Encode()
	}
	ctx, cancel := context.WithTimeout(r.Context(), a.proxy.deadline(extra.timeout))
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, method, target, body)
	if err != nil {
		writeJSON(w, http.StatusBadGateway, map[string]string{"error": "upstream unavailable"})
		return
	}
	// The route validated these before we got here. They go on first so the
	// seam's own headers below cannot be shadowed by a route's extras.
	for name, value := range extra.reqHeaders {
		req.Header.Set(name, value)
	}
	req.Header.Set("Authorization", "Bearer "+bearer)
	req.Header.Set("Accept", "application/json")
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	req.Header.Set("User-Agent", proxyUserAgent)
	a.proxy.relay(w, req, maxRespBytes, extra.respHeaders, extra.validateResp)
}

// bearerCredential extracts the caller's credential without inspecting it —
// the account API is the authority on whether the token is any good. The only
// checks here are the ones that keep a malformed value out of an outbound
// header: printable ASCII, non-empty, bounded.
func bearerCredential(r *http.Request) (string, bool) {
	tok, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
	if !ok {
		return "", false
	}
	tok = strings.TrimSpace(tok)
	if tok == "" || len(tok) > maxBearerLen {
		return "", false
	}
	for i := 0; i < len(tok); i++ {
		if tok[i] < 0x21 || tok[i] > 0x7e {
			return "", false
		}
	}
	return tok, true
}

// relay performs the upstream call and copies back a bounded, JSON-only
// response. Nothing about the token is logged, and no upstream header — cookie
// or otherwise — reaches the browser unless the route named it in respHeaders.
func (p *accountProxy) relay(w http.ResponseWriter, req *http.Request, maxRespBytes int, respHeaders []string, validateResp func([]byte, int) bool) {
	resp, err := p.client.Do(req)
	if err != nil {
		slog.Error("account seam upstream failed", "host", req.URL.Host, "path", req.URL.Path, "err", err)
		writeJSON(w, http.StatusBadGateway, map[string]string{"error": "upstream unavailable"})
		return
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, int64(maxRespBytes)+1))
	if err != nil {
		writeJSON(w, http.StatusBadGateway, map[string]string{"error": "upstream read failed"})
		return
	}
	if len(body) > maxRespBytes {
		slog.Error("account seam upstream response too large", "path", req.URL.Path)
		writeJSON(w, http.StatusBadGateway, map[string]string{"error": "upstream response too large"})
		return
	}
	if resp.StatusCode >= 300 && resp.StatusCode < 400 {
		slog.Error("account seam upstream redirected", "path", req.URL.Path, "status", resp.StatusCode)
		writeJSON(w, http.StatusBadGateway, map[string]string{"error": "upstream unavailable"})
		return
	}

	w.Header().Set("Cache-Control", "no-store")
	copyUpstreamHeaders(w, resp, respHeaders)
	if len(bytes.TrimSpace(body)) == 0 {
		if validateResp != nil {
			writeJSON(w, http.StatusBadGateway, map[string]string{"error": "upstream returned an unsafe cloud account response"})
			return
		}
		w.WriteHeader(resp.StatusCode)
		return
	}
	if !json.Valid(body) {
		mediaType, _, _ := mime.ParseMediaType(resp.Header.Get("Content-Type"))
		if mediaType == "text/plain" && resp.StatusCode >= 400 {
			message := strings.TrimSpace(string(body))
			if message == "" {
				message = "upstream request failed"
			}
			writeJSON(w, passthroughStatus(resp.StatusCode), map[string]string{"error": message})
			return
		}
		// An HTML error page from an ingress in front of the upstream must not
		// reach the browser as-is; keep the status, drop the payload.
		writeJSON(w, passthroughStatus(resp.StatusCode), map[string]string{"error": "upstream returned a non-JSON response"})
		return
	}
	if validateResp != nil && !validateResp(body, resp.StatusCode) {
		writeJSON(w, http.StatusBadGateway, map[string]string{"error": "upstream returned an unsafe cloud account response"})
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(resp.StatusCode)
	w.Write(body) //nolint:errcheck // client went away; nothing to do
}

// copyUpstreamHeaders hands back only the headers the route asked for, and only
// when their value is a single bounded printable token. The browser needs to
// tell a replayed create from a fresh one and to honour central's retry pacing;
// it does not need anything else the upstream happens to send.
func copyUpstreamHeaders(w http.ResponseWriter, resp *http.Response, allowed []string) {
	for _, name := range allowed {
		values := resp.Header.Values(name)
		if len(values) != 1 {
			continue
		}
		value := strings.TrimSpace(values[0])
		if value == "" || len(value) > maxUpstreamHeaderLen || !printableASCII(value) {
			continue
		}
		w.Header().Set(name, value)
	}
}

// printableASCII is the header-safe alphabet: no controls, no CR or LF, and
// nothing above ASCII. Spaces are allowed because a header value may carry one.
func printableASCII(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] < 0x20 || s[i] > 0x7e {
			return false
		}
	}
	return true
}

// passthroughStatus keeps a real upstream error status (the SPA distinguishes
// 401 from 403 from 404) and turns anything else into a plain bad gateway.
func passthroughStatus(code int) int {
	if code >= 400 && code <= 599 {
		return code
	}
	return http.StatusBadGateway
}
