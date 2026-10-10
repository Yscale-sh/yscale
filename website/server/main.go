// Command website-server serves the built yscale.sh marketing site and
// captures waitlist signups (POST /api/signup) as JSON lines. It replaces
// the previous nginx runtime so the site can take signups without a
// third-party form service.
//
// The signup file is append-only: unsubscribes (POST /api/unsubscribe) are
// tombstone records (action="unsubscribe") rather than deletions. The export
// endpoint folds them: GET /api/signups?effective=1 returns only addresses
// whose most recent record is a subscribe.
package main

import (
	"crypto/subtle"
	"encoding/json"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"
	"unicode/utf8"
)

const (
	maxBodyBytes   = 16 << 10 // request body cap
	maxEmailLen    = 254
	maxCompanyLen  = 200
	rateLimit      = 5   // signup POSTs per IP per window
	proxyRateLimit = 120 // account seam requests per IP per window
	rateWindow     = time.Minute
)

var emailRe = regexp.MustCompile(`^[^\s@]+@[^\s@]+\.[^\s@]+$`)

// vanityRedirects are short yscale.sh/* URLs that 302 to a canonical home —
// e.g. the drop card links to yscale.sh/kubagachi, which lands on the
// open-sourced repo. Kept server-side so we can repoint one without rebuilding
// the SPA, and so direct hits and crawlers get a real redirect.
var vanityRedirects = map[string]string{
	"/kubagachi": "https://github.com/Yscale-sh/Kubagachi",
}

// identityRedirects are resolved per request so one image can be promoted
// unchanged: preview/LAN hosts use dev identity, while the production website
// uses the production issuer. /account is deliberately absent — the SPA owns
// that path and starts its own PKCE flow against the same issuer.
var identityRedirects = map[string]string{
	"/access":   "/signup?return_to=%2Faccount",
	"/login":    "/login?return_to=%2Faccount",
	"/signup":   "/signup?return_to=%2Faccount",
	"/waitlist": "/signup?return_to=%2Faccount",
}

func identityBaseURL(r *http.Request) string {
	host := strings.ToLower(r.Host)
	if parsedHost, _, err := net.SplitHostPort(host); err == nil {
		host = parsedHost
	}
	if host == "yscale.sh" || host == "www.yscale.sh" {
		return "https://id.kubagachi.com"
	}
	return "https://id-dev.yscale.sh"
}

// rateLimiter is the in-memory limiter implementation: a per-IP
// sliding-window counter. Old entries are pruned opportunistically so the
// map does not grow without bound.
type rateLimiter struct {
	mu        sync.Mutex
	hits      map[string][]time.Time
	lastPrune time.Time
	limit     int
}

func newRateLimiter() *rateLimiter {
	return newWindowRateLimiter(rateLimit)
}

func newWindowRateLimiter(limit int) *rateLimiter {
	return &rateLimiter{hits: map[string][]time.Time{}, lastPrune: time.Now(), limit: limit}
}

// allow records a hit for ip and reports whether it is within the limit.
func (rl *rateLimiter) allow(ip string) bool {
	rl.mu.Lock()
	defer rl.mu.Unlock()
	now := time.Now()
	cutoff := now.Add(-rateWindow)
	if now.Sub(rl.lastPrune) > rateWindow {
		for k, ts := range rl.hits {
			if len(ts) == 0 || ts[len(ts)-1].Before(cutoff) {
				delete(rl.hits, k)
			}
		}
		rl.lastPrune = now
	}
	ts := rl.hits[ip]
	for len(ts) > 0 && ts[0].Before(cutoff) {
		ts = ts[1:]
	}
	if len(ts) >= rl.limit {
		rl.hits[ip] = ts
		return false
	}
	rl.hits[ip] = append(ts, now)
	return true
}

type app struct {
	staticDir    string
	exportToken  string
	limiter      limiter
	store        store
	kritters     map[string][]byte // path -> PNG bytes; finale is grayscaled here
	proxy        *accountProxy     // account seam; nil-safe, unconfigured = 503
	proxyLimiter limiter           // separate budget; dashboard reads must not consume signup slots
}

func newApp(staticDir, exportToken string, st store, lim limiter, px *accountProxy) *app {
	return &app{
		staticDir:    staticDir,
		exportToken:  exportToken,
		limiter:      lim,
		store:        st,
		kritters:     loadKritterArt(),
		proxy:        px,
		proxyLimiter: newWindowRateLimiter(proxyRateLimit),
	}
}

func (a *app) routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/signup", a.handleSignup)
	mux.HandleFunc("POST /api/unsubscribe", a.handleUnsubscribe)
	mux.HandleFunc("GET /api/signups", a.handleExport)
	mux.HandleFunc("POST /api/auth/token", a.handleAuthToken)
	mux.HandleFunc("GET /api/account", a.handleAccount)
	mux.HandleFunc("POST /api/account/tenants", a.handleCreateTenant)
	mux.HandleFunc("GET /api/tenants/{tenant_id}/members", a.handleListMembers)
	mux.HandleFunc("POST /api/tenants/{tenant_id}/members", a.handleAddMember)
	mux.HandleFunc("PATCH /api/tenants/{tenant_id}/members/{account_id}", a.handleUpdateMemberRole)
	mux.HandleFunc("DELETE /api/tenants/{tenant_id}/members/{account_id}", a.handleRemoveMember)
	mux.HandleFunc("GET /api/tenants/{tenant_id}/usage", a.handleTenantUsage)
	mux.HandleFunc("GET /api/tenants/{tenant_id}/hosted-capacity", a.handleGetHostedCapacity)
	mux.HandleFunc("POST /api/tenants/{tenant_id}/hosted-capacity", a.handleRequestHostedCapacity)
	mux.HandleFunc("GET /api/tenants/{tenant_id}/cloud-accounts/linode", a.handleLinodeCloudAccount)
	mux.HandleFunc("PUT /api/tenants/{tenant_id}/cloud-accounts/linode", a.handleLinodeCloudAccount)
	mux.HandleFunc("DELETE /api/tenants/{tenant_id}/cloud-accounts/linode", a.handleLinodeCloudAccount)
	mux.HandleFunc("GET /api/tenants/{tenant_id}/runtime-bindings", a.handleRuntimeBindings)
	mux.HandleFunc("PUT /api/tenants/{tenant_id}/runtime-bindings/{binding_key}", a.handleRuntimeBindings)
	mux.HandleFunc("DELETE /api/tenants/{tenant_id}/runtime-bindings/{binding_key}", a.handleRuntimeBindings)
	mux.HandleFunc("GET /api/tenants/{tenant_id}/billing", a.handleTenantBilling)
	mux.HandleFunc("GET /api/operator/hosted-capacity/requests", a.handleOperatorHostedCapacityRequests)
	mux.HandleFunc("GET /api/operator/hosted-clusters", a.handleOperatorHostedClusters)
	mux.HandleFunc("GET /api/operator/tenants", a.handleOperatorTenants)
	mux.HandleFunc("PATCH /api/operator/tenants/{tenant_id}/limits", a.handlePatchOperatorTenantLimits)
	mux.HandleFunc("POST /api/operator/tenants/{tenant_id}/billing/service-credits", a.handleOperatorServiceCredit)
	mux.HandleFunc("POST /api/operator/tenants/{tenant_id}/hosted-clusters", a.handleCreateOperatorHostedCluster)
	mux.HandleFunc("POST /api/operator/tenants/{tenant_id}/hosted-clusters/{cluster_id}/credential", a.handleRotateOperatorHostedClusterCredential)
	mux.HandleFunc("DELETE /api/operator/tenants/{tenant_id}/hosted-clusters/{cluster_id}", a.handleDeleteOperatorHostedCluster)
	mux.HandleFunc("GET /api/tenants/{tenant_id}/audit", a.handleListAudit)
	mux.HandleFunc("GET /api/tenants/{tenant_id}/clusters", a.handleListClusters)
	mux.HandleFunc("POST /api/tenants/{tenant_id}/clusters", a.handleRegisterCluster)
	mux.HandleFunc("POST /api/tenants/{tenant_id}/clusters/{cluster_id}/credential", a.handleRotateClusterCredential)
	mux.HandleFunc("DELETE /api/tenants/{tenant_id}/clusters/{cluster_id}", a.handleDeleteCluster)
	mux.HandleFunc("GET /api/tenants/{tenant_id}/cluster-policy", a.handleGetClusterPolicy)
	mux.HandleFunc("PUT /api/tenants/{tenant_id}/cluster-policy", a.handlePutClusterPolicy)
	mux.HandleFunc("GET /api/tenants/{tenant_id}/templates", a.handleGetTenantTemplates)
	mux.HandleFunc("PUT /api/tenants/{tenant_id}/templates", a.handlePutTenantTemplates)
	mux.HandleFunc("GET /api/tenants/{tenant_id}/gitops/sources", a.handleGetTenantGitOpsSources)
	mux.HandleFunc("PUT /api/tenants/{tenant_id}/gitops/sources", a.handlePutTenantGitOpsSources)
	mux.HandleFunc("POST /api/tenants/{tenant_id}/placement-preview", a.handlePlacementPreview)
	mux.HandleFunc("GET /api/tenants/{tenant_id}/catalog-publishers", a.handleListCatalogPublishers)
	mux.HandleFunc("POST /api/tenants/{tenant_id}/catalog-publishers", a.handleCreateCatalogPublisher)
	mux.HandleFunc("POST /api/tenants/{tenant_id}/catalog-publishers/{publisher_id}/credential", a.handleRotateCatalogPublisherCredential)
	mux.HandleFunc("DELETE /api/tenants/{tenant_id}/catalog-publishers/{publisher_id}", a.handleDeleteCatalogPublisher)
	mux.HandleFunc("GET /v1/automation/tenants/{tenant_id}/templates", a.handleGetAutomationTenantTemplates)
	mux.HandleFunc("PUT /v1/automation/tenants/{tenant_id}/templates", a.handlePutAutomationTenantTemplates)
	mux.HandleFunc("GET /api/tenants/{tenant_id}/workloads", a.handleListWorkloads)
	mux.HandleFunc("POST /api/tenants/{tenant_id}/workloads", a.handleCreateWorkload)
	mux.HandleFunc("GET /api/tenants/{tenant_id}/workloads/{workload_id}", a.handleGetWorkload)
	mux.HandleFunc("GET /api/tenants/{tenant_id}/workloads/{workload_id}/logs", a.handleGetWorkloadLogs)
	mux.HandleFunc("POST /api/tenants/{tenant_id}/workloads/{workload_id}/retry", a.handleRetryWorkload)
	mux.HandleFunc("DELETE /api/tenants/{tenant_id}/workloads/{workload_id}", a.handleCancelWorkload)
	mux.HandleFunc("GET /healthz", a.handleHealthz)
	mux.HandleFunc("GET /kritters/{name}", a.handleKritter)
	for from, to := range vanityRedirects {
		dest := to // capture per iteration for the closure
		mux.HandleFunc("GET "+from, func(w http.ResponseWriter, r *http.Request) {
			http.Redirect(w, r, dest, http.StatusFound)
		})
	}
	for from, to := range identityRedirects {
		dest := to // capture per iteration for the closure
		mux.HandleFunc("GET "+from, func(w http.ResponseWriter, r *http.Request) {
			http.Redirect(w, r, identityBaseURL(r)+dest, http.StatusFound)
		})
	}
	mux.HandleFunc("/", a.handleStatic)
	return mux
}

type signupReq struct {
	Email   string `json:"email"`
	Track   string `json:"track"`
	Company string `json:"company"`
	Website string `json:"website"` // honeypot; humans never fill it
}

type signupRecord struct {
	Ts      string `json:"ts"`
	Email   string `json:"email"`
	Track   string `json:"track,omitempty"`
	Company string `json:"company,omitempty"`
	Action  string `json:"action,omitempty"` // "" = subscribe; "unsubscribe" = tombstone
	IP      string `json:"ip"`
	UA      string `json:"ua"`
}

func (a *app) handleSignup(w http.ResponseWriter, r *http.Request) {
	ip := clientIP(r)
	if !a.limiter.allow(ip) {
		writeJSON(w, http.StatusTooManyRequests, map[string]string{"error": "slow down"})
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, maxBodyBytes)
	var req signupReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid json"})
		return
	}
	if req.Website != "" {
		// Honeypot tripped: pretend success, store and log nothing.
		writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
		return
	}
	email := strings.TrimSpace(req.Email)
	if email == "" || len(email) > maxEmailLen || !emailRe.MatchString(email) {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid email"})
		return
	}
	track := req.Track
	if track == "" {
		track = "oss"
	}
	if track != "oss" && track != "cloud" && track != "drops" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid track"})
		return
	}
	company := strings.TrimSpace(req.Company)
	if utf8.RuneCountInString(company) > maxCompanyLen {
		company = string([]rune(company)[:maxCompanyLen])
	}
	rec := signupRecord{
		Ts:      time.Now().UTC().Format(time.RFC3339),
		Email:   email,
		Track:   track,
		Company: company,
		IP:      ip,
		UA:      r.UserAgent(),
	}
	if err := a.store.append(rec); err != nil {
		slog.Error("signup append failed", "err", err)
	}
	// The platform log pipeline is the durable backup store.
	slog.Info("signup", "track", track, "email", email, "company", company)
	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}

// handleUnsubscribe appends an unsubscribe tombstone for the address. Same
// defenses as signup (honeypot, rate limit, validation); always responds
// {"ok":true} for a valid email, whether or not it was ever subscribed —
// enumeration of the list is not possible through this endpoint.
func (a *app) handleUnsubscribe(w http.ResponseWriter, r *http.Request) {
	ip := clientIP(r)
	if !a.limiter.allow(ip) {
		writeJSON(w, http.StatusTooManyRequests, map[string]string{"error": "slow down"})
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, maxBodyBytes)
	var req signupReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid json"})
		return
	}
	if req.Website != "" {
		writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
		return
	}
	email := strings.TrimSpace(req.Email)
	if email == "" || len(email) > maxEmailLen || !emailRe.MatchString(email) {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid email"})
		return
	}
	rec := signupRecord{
		Ts:     time.Now().UTC().Format(time.RFC3339),
		Email:  email,
		Action: "unsubscribe",
		IP:     ip,
		UA:     r.UserAgent(),
	}
	if err := a.store.append(rec); err != nil {
		slog.Error("unsubscribe append failed", "err", err)
	}
	slog.Info("unsubscribe", "email", email)
	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}

func (a *app) handleExport(w http.ResponseWriter, r *http.Request) {
	if a.exportToken == "" {
		http.NotFound(w, r)
		return
	}
	tok, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
	if !ok || subtle.ConstantTimeCompare([]byte(tok), []byte(a.exportToken)) != 1 {
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "unauthorized"})
		return
	}
	recs, err := a.store.readAll()
	if err != nil {
		http.Error(w, "read failed", http.StatusInternalServerError)
		return
	}
	if r.URL.Query().Get("effective") == "1" {
		// Fold the append-only log — the LAST record per email wins, and an
		// unsubscribe tombstone removes the address from the output.
		recs = foldEffective(recs)
	}
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	for _, rec := range recs {
		line, err := json.Marshal(rec)
		if err != nil {
			continue
		}
		w.Write(append(line, '\n')) //nolint:errcheck // client went away; nothing to do
	}
}

func (a *app) handleHealthz(w http.ResponseWriter, _ *http.Request) {
	w.Write([]byte("ok"))
}

// handleStatic serves files from staticDir with an SPA fallback: unknown
// non-/api paths get index.html so client-side routes deep-link cleanly.
func (a *app) handleStatic(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	p := path.Clean("/" + r.URL.Path) // rooted; cannot escape staticDir
	if strings.HasPrefix(p, "/api/") || p == "/api" {
		http.NotFound(w, r)
		return
	}
	if p == "/" {
		p = "/index.html"
	}
	fp := filepath.Join(a.staticDir, filepath.FromSlash(p))
	// Release docs are generated static pages, not client-side app routes.
	// Serve their directory index and fail closed for missing doc URLs rather
	// than silently showing the unrelated account/marketing application.
	isDoc := p == "/docs" || strings.HasPrefix(p, "/docs/")
	if p == "/docs" {
		fp = filepath.Join(a.staticDir, "docs", "index.html")
	}
	if isDoc {
		w.Header().Set("Cache-Control", "no-store, must-revalidate")
		w.Header().Set("CDN-Cache-Control", "no-store")
	}
	if st, err := os.Stat(fp); err == nil && !st.IsDir() {
		if strings.HasPrefix(p, "/assets/") {
			// Vite emits content-hashed filenames under /assets.
			w.Header().Set("Cache-Control", "public, max-age=31536000, immutable")
		} else if p == "/index.html" {
			w.Header().Set("Cache-Control", "no-store, must-revalidate")
			w.Header().Set("CDN-Cache-Control", "no-store")
		}
		http.ServeFile(w, r, fp)
		return
	}
	if isDoc {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Cache-Control", "no-store, must-revalidate")
	w.Header().Set("CDN-Cache-Control", "no-store")
	http.ServeFile(w, r, filepath.Join(a.staticDir, "index.html"))
}

// clientIP prefers the first X-Forwarded-For hop (we sit behind the platform
// proxy), falling back to the connection's remote host.
func clientIP(r *http.Request) string {
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		if i := strings.IndexByte(xff, ','); i >= 0 {
			xff = xff[:i]
		}
		return strings.TrimSpace(xff)
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(v) //nolint:errcheck
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func openAppend(p string) (*os.File, error) {
	return os.OpenFile(p, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
}

func main() {
	port := envOr("PORT", "8080")
	staticDir := envOr("STATIC_DIR", "dist")
	signupPath := envOr("SIGNUP_FILE", "/data/signups.jsonl")

	// DATABASE_URL / REDIS_URL are injected by the platform for the
	// deploy.yaml db/cache blocks. Either being absent or broken falls back
	// to the old behavior — never crash-loop the marketing site over a dep.
	var st store
	if dbURL := os.Getenv("DATABASE_URL"); dbURL != "" {
		if ps, err := newPGStore(dbURL); err != nil {
			slog.Error("postgres unavailable, falling back to signup file", "err", err)
		} else {
			slog.Info("signups: postgres backend")
			st = ps
		}
	}
	if st == nil {
		file, err := openAppend(signupPath)
		if err != nil {
			fallback := "./signups.jsonl"
			slog.Warn("signup file not writable, falling back", "path", signupPath, "fallback", fallback, "err", err)
			signupPath = fallback
			if file, err = openAppend(signupPath); err != nil {
				slog.Error("fallback signup file not writable; signups persist via logs only", "err", err)
				file = nil
			}
		}
		st = &fileStore{path: signupPath, file: file}
		slog.Info("signups: file backend", "path", signupPath)
	}

	var lim limiter = newRateLimiter()
	if redisURL := os.Getenv("REDIS_URL"); redisURL != "" {
		if rl, err := newRedisLimiter(redisURL); err != nil {
			slog.Error("redis unavailable, rate limiting in-memory", "err", err)
		} else {
			slog.Info("rate limit: redis backend")
			lim = rl
		}
	}

	// Both are cluster-internal service URLs injected by deploy.yaml. Absent,
	// the /account dashboard still renders and every account call answers 503.
	px := newAccountProxy(os.Getenv("YSCALE_ID_INTERNAL_URL"), os.Getenv("YSCALE_CLOUD_INTERNAL_URL"), nil)
	if px.idURL == "" || px.cloudURL == "" {
		slog.Warn("account seam partially configured", "id_configured", px.idURL != "", "cloud_configured", px.cloudURL != "")
	}

	a := newApp(staticDir, os.Getenv("EXPORT_TOKEN"), st, lim, px)
	srv := &http.Server{
		Addr:              ":" + port,
		Handler:           a.routes(),
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	slog.Info("listening", "addr", srv.Addr, "static", staticDir)
	if err := srv.ListenAndServe(); err != nil {
		slog.Error("server exited", "err", err)
		os.Exit(1)
	}
}
