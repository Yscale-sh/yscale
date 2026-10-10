package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"net/netip"
	"regexp"
	"strings"
	"time"

	"github.com/yscale-sh/yscale/factory/internal/hsclient"
	"github.com/yscale-sh/yscale/factory/internal/store"
)

// Mesh operations on a tenant's coordination box (central never holds the box's admin key).
// Central holds no box admin key: it asks the factory to mint join keys,
// find/delete devices, approve gateway routes and replace the tenant policy.
// The box key is decrypted per request and never leaves this process.
//
//	POST   /v1/tenants/{id}/mesh/keys                 {"tags":[...],"expiry_seconds":N,"ephemeral":bool}
//	GET    /v1/tenants/{id}/mesh/devices?hostname=H   -> {"id":"..."} ("" when absent)
//	DELETE /v1/tenants/{id}/mesh/devices/{node}
//	POST   /v1/tenants/{id}/mesh/devices/{node}/routes {"routes":[...]}
//	PUT    /v1/tenants/{id}/mesh/policy               {"tag_owners":{...},"route_approvers":{...},"extra_acls":[...]}
//
// Every call names the box it expects in the X-Yscale-Login-Server header and
// is resolved against that exact tenant/login-server pair, so a device ID or
// policy can never land on a different (for example replacement) box. Mint,
// routes and policy need a ready box; find and delete also reach a draining
// box so cleanup of devices minted there still works.

const meshLoginServerHeader = "X-Yscale-Login-Server"

// meshBox is the subset of the Headscale client the mesh API drives.
type meshBox interface {
	MintAuthKeyEphemeral(ctx context.Context, tags []string, expiry time.Duration, ephemeral bool) (string, error)
	FindDeviceByHostname(ctx context.Context, hostname string) (string, error)
	DeleteDevice(ctx context.Context, deviceID string) error
	ApproveNodeRoutes(ctx context.Context, nodeID string, routes []string) error
	EnsurePolicy(ctx context.Context, tagOwners, routeApprovers map[string][]string, extraACLs []hsclient.PolicyACL) error
}

type meshBoxFactory func(loginServer, apiKey, user string) meshBox

func defaultMeshBox(loginServer, apiKey, user string) meshBox {
	return hsclient.NewHeadscale(loginServer, apiKey, user)
}

const (
	meshBodyLimit = 64 << 10
	minKeyExpiry  = time.Minute
	maxKeyExpiry  = 24 * time.Hour
	maxKeyTags    = 16
	maxRoutes     = 64
)

var (
	meshTagRe    = regexp.MustCompile(`^tag:[A-Za-z0-9][A-Za-z0-9._-]{0,62}$`)
	meshNodeIDRe = regexp.MustCompile(`^[0-9]{1,20}$`)
)

// meshPath splits /v1/tenants/{id}/mesh/{rest}.
func meshPath(path string) (tenantID, rest string, ok bool) {
	const prefix = "/v1/tenants/"
	if !strings.HasPrefix(path, prefix) {
		return "", "", false
	}
	tenantID, rest, found := strings.Cut(strings.TrimPrefix(path, prefix), "/mesh/")
	if !found || tenantID == "" || strings.Contains(tenantID, "/") || rest == "" {
		return "", "", false
	}
	return tenantID, rest, true
}

func serveMesh(w http.ResponseWriter, r *http.Request, s store.Store, newBox meshBoxFactory, tenantID, rest string) {
	r.Body = http.MaxBytesReader(w, r.Body, meshBodyLimit)
	segments := strings.Split(rest, "/")
	cleanup := segments[0] == "devices" && (r.Method == http.MethodGet || r.Method == http.MethodDelete)
	box, ok := resolveBox(w, s, newBox, tenantID, r.Header.Get(meshLoginServerHeader), cleanup)
	if !ok {
		return
	}
	switch {
	case len(segments) == 1 && segments[0] == "keys" && r.Method == http.MethodPost:
		mintMeshKey(w, r, box, tenantID)
	case len(segments) == 1 && segments[0] == "devices" && r.Method == http.MethodGet:
		hostname := r.URL.Query().Get("hostname")
		if hostname == "" || len(hostname) > 253 {
			writeJSON(w, http.StatusBadRequest, map[string]string{"status": "invalid_hostname"})
			return
		}
		id, err := box.hs.FindDeviceByHostname(r.Context(), hostname)
		if err != nil {
			boxError(w, tenantID, "find device", err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"id": id})
	case len(segments) == 2 && segments[0] == "devices" && r.Method == http.MethodDelete:
		if !meshNodeIDRe.MatchString(segments[1]) {
			writeJSON(w, http.StatusBadRequest, map[string]string{"status": "invalid_device"})
			return
		}
		if err := box.hs.DeleteDevice(r.Context(), segments[1]); err != nil {
			boxError(w, tenantID, "delete device", err)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	case len(segments) == 3 && segments[0] == "devices" && segments[2] == "routes" && r.Method == http.MethodPost:
		approveMeshRoutes(w, r, box, tenantID, segments[1])
	case len(segments) == 1 && segments[0] == "policy" && r.Method == http.MethodPut:
		ensureMeshPolicy(w, r, box, tenantID)
	default:
		writeJSON(w, http.StatusNotFound, map[string]string{"status": "not_found"})
	}
}

type resolvedBox struct {
	hs          meshBox
	loginServer string
}

// resolveBox resolves exactly the tenant's box named by expectedLogin and its
// decrypted admin key. A box that is not ready answers 409 (central maps it to
// its typed, retryable onboarding error); cleanup calls also accept a draining
// box.
func resolveBox(w http.ResponseWriter, s store.Store, newBox meshBoxFactory, tenantID, expectedLogin string, cleanup bool) (resolvedBox, bool) {
	if !strings.HasPrefix(expectedLogin, "https://") {
		writeJSON(w, http.StatusBadRequest, map[string]string{"status": "missing_login_server"})
		return resolvedBox{}, false
	}
	box, err := s.GetBoxForTenant(tenantID, expectedLogin)
	if err != nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"status": "absent"})
		return resolvedBox{}, false
	}
	usable := box.Status == store.StatusReady || (cleanup && box.Status == store.StatusDecommissioning)
	if !usable {
		writeJSON(w, http.StatusConflict, map[string]string{"status": box.Status})
		return resolvedBox{}, false
	}
	key, err := s.DecryptAPIKey(box.LoginServer)
	if err != nil {
		log.Printf("factory: mesh: tenant %s: box credential unavailable: %v", tenantID, err)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"status": "credential_unavailable"})
		return resolvedBox{}, false
	}
	return resolvedBox{hs: newBox(box.LoginServer, key, box.HSUser), loginServer: box.LoginServer}, true
}

func mintMeshKey(w http.ResponseWriter, r *http.Request, box resolvedBox, tenantID string) {
	var req struct {
		Tags          []string `json:"tags"`
		ExpirySeconds int64    `json:"expiry_seconds"`
		Ephemeral     bool     `json:"ephemeral"`
	}
	if !decodeMeshBody(w, r, &req) {
		return
	}
	expiry := time.Duration(req.ExpirySeconds) * time.Second
	if len(req.Tags) == 0 || len(req.Tags) > maxKeyTags || expiry < minKeyExpiry || expiry > maxKeyExpiry {
		writeJSON(w, http.StatusBadRequest, map[string]string{"status": "invalid_key_request"})
		return
	}
	for _, tag := range req.Tags {
		if !meshTagRe.MatchString(tag) {
			writeJSON(w, http.StatusBadRequest, map[string]string{"status": "invalid_tag"})
			return
		}
	}
	key, err := box.hs.MintAuthKeyEphemeral(r.Context(), req.Tags, expiry, req.Ephemeral)
	if err != nil {
		boxError(w, tenantID, "mint key", err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"auth_key": key, "login_server": box.loginServer})
}

func approveMeshRoutes(w http.ResponseWriter, r *http.Request, box resolvedBox, tenantID, nodeID string) {
	if !meshNodeIDRe.MatchString(nodeID) {
		writeJSON(w, http.StatusBadRequest, map[string]string{"status": "invalid_device"})
		return
	}
	var req struct {
		Routes []string `json:"routes"`
	}
	if !decodeMeshBody(w, r, &req) {
		return
	}
	if len(req.Routes) > maxRoutes {
		writeJSON(w, http.StatusBadRequest, map[string]string{"status": "invalid_routes"})
		return
	}
	for _, route := range req.Routes {
		if _, err := netip.ParsePrefix(route); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"status": "invalid_routes"})
			return
		}
	}
	if err := box.hs.ApproveNodeRoutes(r.Context(), nodeID, req.Routes); err != nil {
		boxError(w, tenantID, "approve routes", err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func ensureMeshPolicy(w http.ResponseWriter, r *http.Request, box resolvedBox, tenantID string) {
	var req struct {
		TagOwners      map[string][]string  `json:"tag_owners"`
		RouteApprovers map[string][]string  `json:"route_approvers"`
		ExtraACLs      []hsclient.PolicyACL `json:"extra_acls"`
	}
	if !decodeMeshBody(w, r, &req) {
		return
	}
	if err := box.hs.EnsurePolicy(r.Context(), req.TagOwners, req.RouteApprovers, req.ExtraACLs); err != nil {
		boxError(w, tenantID, "ensure policy", err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func decodeMeshBody(w http.ResponseWriter, r *http.Request, out any) bool {
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(out); err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			writeJSON(w, http.StatusRequestEntityTooLarge, map[string]string{"status": "too_large"})
			return false
		}
		writeJSON(w, http.StatusBadRequest, map[string]string{"status": "invalid_json"})
		return false
	}
	return true
}

// boxError keeps the response generic; the operator log carries the cause.
func boxError(w http.ResponseWriter, tenantID, op string, err error) {
	log.Printf("factory: mesh: tenant %s: %s: %v", tenantID, op, err)
	writeJSON(w, http.StatusBadGateway, map[string]string{"status": "box_error"})
}
