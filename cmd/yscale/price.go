package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"text/tabwriter"
	"time"
)

// runPrice implements `yscale price gpu` — fetches the GPU price index
// from central and pretty-prints it. Prices are all-in customer rates
// (upstream cost + yscale margin); provider names are intentionally
// absent — yscale brands the index, not the upstream neoclouds.
func runPrice(args []string) error {
	fs := flag.NewFlagSet("price", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	var (
		endpoint string
		token    string
		outJSON  bool
	)
	fs.StringVar(&endpoint, "endpoint", "", "your central server URL (default $YSCALE_ENDPOINT, then http://127.0.0.1:8443 for local development)")
	fs.StringVar(&token, "token", "", "customer API token (default $YSCALE_TOKEN)")
	fs.BoolVar(&outJSON, "json", false, "raw JSON output")

	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}
	if len(fs.Args()) == 0 || fs.Arg(0) != "gpu" {
		fmt.Fprintln(os.Stderr, "usage: yscale price gpu")
		return errors.New("missing subcommand")
	}

	if endpoint == "" {
		endpoint = os.Getenv("YSCALE_ENDPOINT")
	}
	if endpoint == "" {
		endpoint = "http://127.0.0.1:8443"
	}
	if token == "" {
		token = os.Getenv("YSCALE_TOKEN")
	}
	if token == "" {
		return errors.New("YSCALE_TOKEN env var (or -token) required")
	}

	req, err := http.NewRequest("GET", endpoint+"/v1/price/gpu", nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	cli := &http.Client{Timeout: 10 * time.Second}
	resp, err := cli.Do(req)
	if err != nil {
		return fmt.Errorf("fetch gpu prices: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("status %d: %s", resp.StatusCode, string(body))
	}

	if outJSON {
		_, err := io.Copy(os.Stdout, resp.Body)
		return err
	}

	var listing struct {
		GPUs []struct {
			Kind         string  `json:"kind"`
			VRAMGiB      int     `json:"vram_gib"`
			Reliability  string  `json:"reliability"`
			USDPerHour   float64 `json:"usd_per_hour"`
			USDPerMinute float64 `json:"usd_per_minute"`
			Note         string  `json:"note,omitempty"`
		} `json:"gpus"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&listing); err != nil {
		return err
	}

	// Stable order: by kind, then by reliability (reliable before spot).
	sort.SliceStable(listing.GPUs, func(i, j int) bool {
		if listing.GPUs[i].Kind != listing.GPUs[j].Kind {
			return listing.GPUs[i].Kind < listing.GPUs[j].Kind
		}
		return listing.GPUs[i].Reliability < listing.GPUs[j].Reliability
	})

	tw := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "KIND\tVRAM\tRELIABILITY\t$/hr\t¢/min\tNOTE")
	for _, g := range listing.GPUs {
		fmt.Fprintf(tw, "%s\t%dGB\t%s\t$%.2f\t%.2f¢\t%s\n",
			g.Kind, g.VRAMGiB, g.Reliability, g.USDPerHour, g.USDPerMinute*100, g.Note)
	}
	return tw.Flush()
}
