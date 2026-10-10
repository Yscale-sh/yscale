package boxes

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"log"
	"net/url"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/yscale-sh/yscale/factory/internal/hsclient"
	"github.com/yscale-sh/yscale/factory/internal/store"
)

const (
	headscaleTag       = "yscale-headscale"
	headscaleFWLabel   = "yscale-headscale-fw"
	defaultCloudInit   = "deploy/headscale/cloud-init.sh"
	defaultACLTemplate = "deploy/headscale/acls.hujson"
)

// linodeAPI is the complete cloud boundary for a Headscale box. Its production
// implementation belongs outside this worker; tests provide a fake.
type linodeAPI interface {
	EnsureFirewall(ctx context.Context, label string, tags []string) (firewallID string, err error)
	CreateInstance(ctx context.Context, spec InstanceSpec) (Instance, error)
	GetInstance(ctx context.Context, id string) (Instance, error)
	ListInstancesByTag(ctx context.Context, tag string) ([]Instance, error)
	DeleteInstance(ctx context.Context, id string) error
}

// Instance is the subset of a Linode instance used by the lifecycle worker.
type Instance struct {
	ID     string
	Status string
	IPv4   []string
	Tags   []string
	Label  string
}

// InstanceSpec is the create contract ported from provision.sh.
type InstanceSpec struct {
	Label       string
	Region      string
	Type        string
	Image       string
	RootPass    string
	UserDataB64 string
	FirewallID  string
	Tags        []string
}

// Option configures a LinodeWorker without coupling it to process globals.
type Option func(*LinodeWorker)

func WithRegion(region string) Option      { return func(w *LinodeWorker) { w.region = region } }
func WithType(typ string) Option           { return func(w *LinodeWorker) { w.typ = typ } }
func WithPodCIDR(cidr string) Option       { return func(w *LinodeWorker) { w.podCIDR = cidr } }
func WithServiceCIDR(cidr string) Option   { return func(w *LinodeWorker) { w.svcCIDR = cidr } }
func WithNodeCIDR(cidr string) Option      { return func(w *LinodeWorker) { w.nodeCIDR = cidr } }
func WithCloudInitPath(path string) Option { return func(w *LinodeWorker) { w.cloudInitPath = path } }
func WithACLTemplatePath(path string) Option {
	return func(w *LinodeWorker) { w.aclTemplatePath = path }
}
func WithPollAttempts(n int) Option { return func(w *LinodeWorker) { w.maxAttempts = n } }

// WithOpsHandoff configures the private ops-tailnet channel used by a box to
// hand its generated Headscale admin key back to the factory.
func WithOpsHandoff(authKey, factoryOpsURL string) Option {
	return func(w *LinodeWorker) {
		w.opsAuthKey = authKey
		w.opsFactoryURL = factoryOpsURL
	}
}

// WithOpsLoginServer selects the operator-owned coordinator for the separate
// ops mesh. There is deliberately no hosted Tailscale default.
func WithOpsLoginServer(loginServer string) Option {
	return func(w *LinodeWorker) { w.opsLoginServer = loginServer }
}

// ValidateOpsLoginServer rejects missing/insecure coordination before any
// credentials or billable resources are used. Do not echo supplied URLs: they
// may contain mistakenly pasted credentials.
func ValidateOpsLoginServer(loginServer string) error {
	u, err := url.Parse(loginServer)
	if err != nil || u.Scheme != "https" || u.Hostname() == "" || u.User != nil ||
		u.RawQuery != "" || u.ForceQuery || u.Fragment != "" ||
		(u.Path != "" && u.Path != "/") {
		return errors.New("FACTORY_OPS_LOGIN_SERVER must be an explicit HTTPS coordinator origin without credentials, path, query or fragment")
	}
	return nil
}
func withSleep(sleep func(context.Context) error) Option {
	return func(w *LinodeWorker) { w.sleep = sleep }
}
func withHeadscaleRegistration(register func(context.Context, hsclient.BoxInfo) (string, string, string, string, error)) Option {
	return func(w *LinodeWorker) { w.registerHeadscale = register }
}

// LinodeWorker provisions and retires dedicated Headscale Linodes.
type LinodeWorker struct {
	store store.Store
	api   linodeAPI
	reg   Registrar

	region, typ                    string
	podCIDR, svcCIDR, nodeCIDR     string
	cloudInitPath, aclTemplatePath string
	opsAuthKey, opsFactoryURL      string
	opsLoginServer                 string
	maxAttempts                    int
	sleep                          func(context.Context) error
	registerHeadscale              func(context.Context, hsclient.BoxInfo) (string, string, string, string, error)
}

// NewLinodeWorker constructs the production lifecycle worker. A nil registrar
// deliberately fails before cloud creation until an ops registrar is wired.
func NewLinodeWorker(s store.Store, api linodeAPI, reg Registrar, opts ...Option) *LinodeWorker {
	if reg == nil {
		reg = unavailableRegistrar{}
	}
	w := &LinodeWorker{
		store: s, api: api, reg: reg,
		region: "us-ord", typ: "g6-nanode-1",
		podCIDR: "10.42.0.0/16", svcCIDR: "10.43.0.0/16", nodeCIDR: "10.0.0.0/24",
		cloudInitPath: defaultCloudInit, aclTemplatePath: defaultACLTemplate,
		maxAttempts: 30, sleep: sleepPoll, registerHeadscale: hsclient.RegisterHeadscale,
	}
	for _, opt := range opts {
		opt(w)
	}
	return w
}

func (w *LinodeWorker) Provision(ctx context.Context, tenantID, idempotencyKey string) (store.Job, error) {
	job, box, err := w.store.EnsureFabric(tenantID, idempotencyKey)
	if err != nil {
		return store.Job{}, err
	}
	if box.Status != store.StatusProvisioning {
		return job, nil
	}
	// EnsureFabric stages the box under a temporary "…factory.invalid" login
	// server; capture it so the validated box can be re-keyed to its real
	// endpoint below without disturbing any other tenant's staging record.
	stagingLoginServer := box.LoginServer
	if isUnavailableRegistrar(w.reg) {
		if _, err := w.reg.NewRegistration(ctx, tenantID); err != nil {
			return job, err
		}
		return job, unavailableRegistrarError()
	}
	if w.api == nil {
		return job, errors.New("linode worker: API is required")
	}
	// A real registrar REQUIRES ops-handoff config: without it the box has no
	// channel to hand its key back, so we'd create a billable box that can only
	// block on AwaitKey until timeout and then degrade. Refuse BEFORE any VM.
	if !w.hasOpsHandoff() {
		return job, errors.New("linode worker: registrar configured without ops handoff (WithOpsHandoff) — refusing to create a box that cannot return its admin key")
	}
	if err := ValidateOpsLoginServer(w.opsLoginServer); err != nil {
		return job, err
	}
	token, err := w.reg.NewRegistration(ctx, tenantID)
	if err != nil {
		return job, fmt.Errorf("linode worker: mint ops handoff: %w", err)
	}

	aclTemplate, err := os.ReadFile(w.aclTemplatePath)
	if err != nil {
		return w.degradeProvision(job, box, fmt.Errorf("linode worker: read ACL template: %w", err))
	}
	acl, err := renderACL(aclTemplate, tenantID, w.podCIDR, w.svcCIDR, w.nodeCIDR)
	if err != nil {
		return w.degradeProvision(job, box, err)
	}
	cloudInit, err := os.ReadFile(w.cloudInitPath)
	if err != nil {
		return w.degradeProvision(job, box, fmt.Errorf("linode worker: read cloud-init: %w", err))
	}
	userData, err := cloudInitUserData(tenantID, acl, token, w.opsAuthKey, w.opsFactoryURL, w.opsLoginServer, cloudInit)
	if err != nil {
		return w.degradeProvision(job, box, err)
	}

	firewallID, err := w.api.EnsureFirewall(ctx, headscaleFWLabel, []string{headscaleTag})
	if err != nil {
		log.Printf("linode worker: ensure firewall: %v", err)
		firewallID = ""
	}
	rootPass, err := randomPassword(32)
	if err != nil {
		return w.degradeProvision(job, box, fmt.Errorf("linode worker: generate root password: %w", err))
	}
	inst, err := w.api.CreateInstance(ctx, InstanceSpec{
		Label: "headscale-" + tenantID + "-" + job.ID[:12], Region: w.region, Type: w.typ,
		Image: "linode/debian12", RootPass: rootPass, UserDataB64: base64.StdEncoding.EncodeToString(userData),
		FirewallID: firewallID, Tags: []string{headscaleTag},
	})
	if err != nil {
		return w.degradeProvision(job, box, fmt.Errorf("linode worker: create instance: %w", err))
	}
	inst, err = w.waitForRunning(ctx, inst.ID)
	if err != nil {
		return w.degradeProvision(job, box, err)
	}
	if len(inst.IPv4) == 0 || inst.IPv4[0] == "" {
		return w.degradeProvision(job, box, errors.New("linode worker: running instance has no IPv4 address"))
	}
	hostname := strings.ReplaceAll(inst.IPv4[0], ".", "-") + ".ip.linodeusercontent.com"
	loginServer := "https://" + hostname
	apiKey, err := w.reg.AwaitKey(ctx, token, loginServer, tenantID)
	if err != nil {
		return w.degradeProvision(job, box, fmt.Errorf("linode worker: acquire box API key: %w", err))
	}
	login, key, user, _, err := w.registerHeadscale(ctx, hsclient.BoxInfo{Hostname: hostname, APIKey: apiKey, User: tenantID, BackendID: inst.ID})
	if err != nil {
		return w.degradeProvision(job, box, fmt.Errorf("linode worker: validate box: %w", err))
	}
	box.LoginServer = login
	box.Hostname = hostname
	box.Backend = "linode"
	box.BackendID = inst.ID
	box.HSUser = user
	if err := w.store.PromoteFabric(stagingLoginServer, box, key); err != nil {
		return store.Job{}, fmt.Errorf("linode worker: persist box: %w", err)
	}
	if err := w.store.SetBoxStatus(login, store.StatusReady); err != nil {
		return store.Job{}, fmt.Errorf("linode worker: mark box ready: %w", err)
	}
	job.LoginServer = login
	return job, nil
}

func (w *LinodeWorker) Deprovision(ctx context.Context, tenantID, loginServer string) (store.Job, error) {
	if err := ctx.Err(); err != nil {
		return store.Job{}, err
	}
	box, started, err := w.store.BeginDeprovision(tenantID, loginServer)
	if err != nil {
		return store.Job{}, err
	}
	if !started {
		return store.Job{}, nil
	}
	if w.api == nil {
		return store.Job{}, errors.New("linode worker: API is required")
	}
	inst, err := w.api.GetInstance(ctx, box.BackendID)
	if err != nil {
		return store.Job{}, fmt.Errorf("linode worker: inspect instance for delete: %w", err)
	}
	if !hasExactTag(inst.Tags, headscaleTag) {
		return store.Job{}, errors.New("linode worker: refusing to delete instance without yscale-headscale tag")
	}
	if err := w.api.DeleteInstance(ctx, box.BackendID); err != nil {
		return store.Job{}, fmt.Errorf("linode worker: delete instance: %w", err)
	}
	if err := w.store.SetBoxStatus(loginServer, store.StatusDead); err != nil {
		return store.Job{}, fmt.Errorf("linode worker: mark box dead: %w", err)
	}
	job, err := w.store.EnqueueJob(store.Job{TenantID: tenantID, LoginServer: loginServer, Kind: "deprovision"})
	if err != nil {
		return store.Job{}, err
	}
	return job, nil
}

func (w *LinodeWorker) waitForRunning(ctx context.Context, instanceID string) (Instance, error) {
	for attempt := 0; attempt < w.maxAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return Instance{}, err
		}
		inst, err := w.api.GetInstance(ctx, instanceID)
		if err != nil {
			return Instance{}, fmt.Errorf("linode worker: poll instance: %w", err)
		}
		if inst.Status == "running" {
			return inst, nil
		}
		if attempt+1 < w.maxAttempts {
			if err := w.sleep(ctx); err != nil {
				return Instance{}, err
			}
		}
	}
	return Instance{}, errors.New("linode worker: instance did not reach running state")
}

func (w *LinodeWorker) degradeProvision(job store.Job, box store.Box, err error) (store.Job, error) {
	if statusErr := w.store.SetBoxStatus(box.LoginServer, store.StatusDegraded); statusErr != nil {
		return store.Job{}, fmt.Errorf("%w; mark box degraded: %v", err, statusErr)
	}
	return job, err
}

func unavailableRegistrarError() error {
	return errors.New("ops key handoff not wired: set an ops registrar")
}

func isUnavailableRegistrar(reg Registrar) bool {
	switch reg.(type) {
	case unavailableRegistrar, *unavailableRegistrar:
		return true
	default:
		return false
	}
}

func sleepPoll(ctx context.Context) error {
	timer := time.NewTimer(10 * time.Second)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func randomPassword(length int) (string, error) {
	const alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
	buf := make([]byte, length)
	for i := range buf {
		for {
			var b [1]byte
			if _, err := rand.Read(b[:]); err != nil {
				return "", err
			}
			if int(b[0]) < 256-(256%len(alphabet)) {
				buf[i] = alphabet[int(b[0])%len(alphabet)]
				break
			}
		}
	}
	return string(buf), nil
}

func (w *LinodeWorker) hasOpsHandoff() bool {
	return w.opsAuthKey != "" && w.opsFactoryURL != ""
}

func cloudInitUserData(user, acl, opsToken, opsAuthKey, opsFactoryURL, opsLoginServer string, cloudInit []byte) ([]byte, error) {
	body := string(cloudInit)
	firstNewline := strings.IndexByte(body, '\n')
	if firstNewline < 0 {
		return nil, errors.New("linode worker: cloud-init has no shebang line")
	}
	prefix := "#!/bin/bash\n" +
		"export HS_USER=" + shellQuote(user) + "\n" +
		"export HS_SELFDESTRUCT_MIN=''\n" +
		"export HS_ACL_B64=" + shellQuote(acl) + "\n" +
		"export HS_OPS_TOKEN=" + shellQuote(opsToken) + "\n" +
		"export HS_OPS_AUTHKEY=" + shellQuote(opsAuthKey) + "\n" +
		"export HS_OPS_LOGIN_SERVER=" + shellQuote(opsLoginServer) + "\n" +
		"export HS_OPS_FACTORY_URL=" + shellQuote(opsFactoryURL) + "\n"
	return []byte(prefix + body[firstNewline+1:]), nil
}

func shellQuote(value string) string { return "'" + strings.ReplaceAll(value, "'", "'\"'\"'") + "'" }

var placeholderRe = regexp.MustCompile(`<[^>]+>`)

// renderACL ports provision.sh's strict render, minify, and base64 contract.
func renderACL(templateBytes []byte, customer, podCIDR, svcCIDR, nodeCIDR string) (string, error) {
	rendered := string(templateBytes)
	for token, value := range map[string]string{
		"<CUSTOMER>": customer, "<POD_CIDR>": podCIDR,
		"<SVC_CIDR>": svcCIDR, "<NODE_CIDR>": nodeCIDR,
	} {
		rendered = strings.ReplaceAll(rendered, token, value)
	}
	if leftover := placeholderRe.FindString(rendered); leftover != "" {
		return "", fmt.Errorf("linode worker: ACL has unrendered placeholder %s", leftover)
	}
	lines := strings.Split(rendered, "\n")
	minified := make([]string, 0, len(lines))
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" || strings.HasPrefix(trimmed, "//") {
			continue
		}
		minified = append(minified, line)
	}
	return base64.StdEncoding.EncodeToString([]byte(strings.Join(minified, "\n"))), nil
}

func hasExactTag(tags []string, wanted string) bool {
	for _, tag := range tags {
		if tag == wanted {
			return true
		}
	}
	return false
}

var _ Worker = (*LinodeWorker)(nil)
