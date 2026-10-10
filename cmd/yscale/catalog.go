package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"sigs.k8s.io/yaml"
)

const (
	defaultCatalogTokenEnv = "YSCALE_TOKEN"
	maxCatalogHTTPBytes    = 128 << 10
)

type catalogCLIOptions struct {
	server   string
	tenant   string
	tokenEnv string
}

type catalogGETResponse struct {
	CatalogRevision string          `json:"catalog_revision"`
	Templates       json.RawMessage `json:"templates"`
}

func runCatalog(args []string) error {
	if len(args) == 0 {
		return errors.New("expected get or apply")
	}
	switch args[0] {
	case "get":
		return runCatalogGet(args[1:])
	case "apply":
		return runCatalogApply(args[1:])
	default:
		return fmt.Errorf("unknown command %q (expected get or apply)", args[0])
	}
}

func runCatalogGet(args []string) error {
	fs := flag.NewFlagSet("catalog get", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	var opts catalogCLIOptions
	var output string
	fs.StringVar(&opts.server, "server", "", "Yscale server URL")
	fs.StringVar(&opts.tenant, "tenant", "", "tenant ID")
	fs.StringVar(&opts.tokenEnv, "token-env", defaultCatalogTokenEnv, "environment variable containing the catalog publisher token")
	fs.StringVar(&output, "o", "-", "output file, or - for stdout")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if opts.server == "" || opts.tenant == "" {
		return errors.New("--server and --tenant are required")
	}
	if opts.tokenEnv == "" || fs.NArg() != 0 {
		return errors.New("--token-env must not be empty and positional arguments are not accepted")
	}
	body, err := catalogRequest(opts, http.MethodGet, nil)
	if err != nil {
		return err
	}
	body = append(body, '\n')
	if output == "" || output == "-" {
		_, err = os.Stdout.Write(body)
		return err
	}
	if err := os.WriteFile(output, body, 0o644); err != nil {
		return fmt.Errorf("write %s: %w", output, err)
	}
	return nil
}

func runCatalogApply(args []string) error {
	var opts catalogCLIOptions
	var file string
	fs := flag.NewFlagSet("catalog apply", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	fs.StringVar(&opts.server, "server", "", "Yscale server URL")
	fs.StringVar(&opts.tenant, "tenant", "", "tenant ID")
	fs.StringVar(&opts.tokenEnv, "token-env", defaultCatalogTokenEnv, "environment variable containing the catalog publisher token")
	fs.StringVar(&file, "f", "", "catalog YAML or JSON file")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if opts.server == "" || opts.tenant == "" || file == "" {
		return errors.New("--server, --tenant, and -f are required")
	}
	if opts.tokenEnv == "" || fs.NArg() != 0 {
		return errors.New("--token-env must not be empty and positional arguments are not accepted")
	}
	desired, err := os.ReadFile(file)
	if err != nil {
		return fmt.Errorf("read %s: %w", file, err)
	}
	desiredJSON, err := yaml.YAMLToJSON(desired)
	if err != nil {
		return fmt.Errorf("parse catalog YAML or JSON: %w", err)
	}
	var doc struct {
		Templates json.RawMessage `json:"templates"`
	}
	dec := json.NewDecoder(bytes.NewReader(desiredJSON))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&doc); err != nil || len(doc.Templates) == 0 {
		return errors.New("catalog file must be an object containing only a templates array")
	}
	if trimmed := bytes.TrimSpace(doc.Templates); len(trimmed) == 0 || trimmed[0] != '[' {
		return errors.New("catalog file templates must be an array")
	}
	var entries []json.RawMessage
	if err := json.Unmarshal(doc.Templates, &entries); err != nil {
		return errors.New("catalog file templates must be an array")
	}
	currentBody, err := catalogRequest(opts, http.MethodGet, nil)
	if err != nil {
		return fmt.Errorf("read current catalog: %w", err)
	}
	var current catalogGETResponse
	if err := json.Unmarshal(currentBody, &current); err != nil || current.CatalogRevision == "" {
		return errors.New("server returned an invalid catalog response")
	}
	requestBody, err := json.Marshal(struct {
		CatalogRevision string          `json:"catalog_revision"`
		Templates       json.RawMessage `json:"templates"`
	}{CatalogRevision: current.CatalogRevision, Templates: doc.Templates})
	if err != nil {
		return fmt.Errorf("encode catalog: %w", err)
	}
	_, err = catalogRequest(opts, http.MethodPut, requestBody)
	return err
}

func catalogRequest(opts catalogCLIOptions, method string, body []byte) ([]byte, error) {
	endpoint, err := catalogEndpoint(opts.server, opts.tenant)
	if err != nil {
		return nil, err
	}
	token := os.Getenv(opts.tokenEnv)
	if token == "" {
		return nil, fmt.Errorf("environment variable %s is empty", opts.tokenEnv)
	}
	req, err := http.NewRequest(method, endpoint, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	client := &http.Client{
		Timeout: 20 * time.Second,
		// The Authorization header must never follow a server-controlled
		// redirect to another origin (or even another path). Operators provide
		// the canonical control-plane URL explicitly.
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("%s catalog: %w", strings.ToLower(method), err)
	}
	defer resp.Body.Close()
	data, readErr := io.ReadAll(io.LimitReader(resp.Body, maxCatalogHTTPBytes+1))
	if readErr != nil {
		return nil, fmt.Errorf("read response: %w", readErr)
	}
	if len(data) > maxCatalogHTTPBytes {
		return nil, errors.New("server response exceeds 128 KiB")
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		message := strings.TrimSpace(string(data))
		if len(message) > 512 {
			message = message[:512]
		}
		if resp.StatusCode == http.StatusConflict {
			return nil, fmt.Errorf("catalog changed since it was read (HTTP 409); apply did not retry or overwrite: %s", message)
		}
		return nil, fmt.Errorf("server returned HTTP %d: %s", resp.StatusCode, message)
	}
	return data, nil
}

func catalogEndpoint(server, tenant string) (string, error) {
	u, err := url.Parse(strings.TrimSpace(server))
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.RawQuery != "" || u.Fragment != "" {
		return "", errors.New("--server must be an http(s) URL without query or fragment")
	}
	base := strings.TrimRight(u.String(), "/")
	return base + "/v1/automation/tenants/" + url.PathEscape(tenant) + "/templates", nil
}
