package main

import (
	"fmt"
	"os"
	"strings"
	"text/tabwriter"
	"time"
)

// printReport writes a tab-aligned summary of all case results to
// stdout. Format:
//
//	CASE                  BACKEND   PHASE       WALL    REAPED  ERROR
//	flyio-cpu-nano        flyio     Succeeded   2m31s   yes
//	linode-cpu-small      linode    Succeeded   7m12s   yes
//	linode-gpu-rtx6000    linode    Timeout     12m     no      timed out (last phase="Provisioning")
func printReport(results []CaseResult) {
	fmt.Println()
	fmt.Println(strings.Repeat("─", 80))
	fmt.Println("yscaletest results")
	fmt.Println(strings.Repeat("─", 80))

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "CASE\tBACKEND\tPHASE\tWALL\tREAP\tBURST\tERROR")
	pass, fail := 0, 0
	for _, r := range results {
		ok := casePassed(r)
		if ok {
			pass++
		} else {
			fail++
		}
		reapStr := "no"
		if r.Reaped {
			reapStr = fmt.Sprintf("yes(%s)", shortDuration(r.ReapTime))
		}
		burst := r.BurstID
		if burst == "" {
			burst = "-"
		}
		err := r.errorText()
		if len(err) > 60 {
			err = err[:57] + "..."
		}
		fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
			r.Case.Name,
			or(r.Backend, "-"),
			or(r.Phase, "-"),
			shortDuration(r.WallTime),
			reapStr,
			burst,
			err,
		)
	}
	w.Flush()
	fmt.Println(strings.Repeat("─", 80))

	// On any failure, dump the captured diagnostic — pod logs, events,
	// container exit info. Saves the operator from running kubectl on
	// resources that may already be GC'd.
	for _, r := range results {
		if r.Diagnostic == "" {
			continue
		}
		fmt.Printf("\n══ diagnostic: %s (%s) ══", r.Case.Name, r.Phase)
		fmt.Print(r.Diagnostic)
		fmt.Println(strings.Repeat("─", 80))
	}

	fmt.Printf("PASS=%d  FAIL=%d  TOTAL=%d\n", pass, fail, len(results))
}

// errorText merges the workload error and the reap error into the single
// ERROR cell the report has always had, so the column set is unchanged
// while a fail-closed reap failure stops being invisible. Previously a
// case could print REAP=no with a blank ERROR and leave the operator to
// guess whether the node lingered, the audit failed, or nobody looked.
func (r CaseResult) errorText() string {
	parts := r.Err
	if r.ReapErr != "" {
		if parts != "" {
			parts += " | "
		}
		parts += "reap: " + r.ReapErr
	}
	if r.ArtifactResult != "" && r.ArtifactResult != "passed" {
		if parts != "" {
			parts += " | "
		}
		parts += "evidence: failed"
	}
	return parts
}

func casePassed(r CaseResult) bool {
	if r.Phase != "Succeeded" || !r.Reaped || r.Err != "" || r.ReapErr != "" {
		return false
	}
	if r.ArtifactResult != "" && r.ArtifactResult != "passed" {
		return false
	}
	return true
}

func anyFailed(results []CaseResult) bool {
	for _, r := range results {
		if !casePassed(r) {
			return true
		}
	}
	return false
}

func or(s, fallback string) string {
	if s == "" {
		return fallback
	}
	return s
}

// shortDuration keeps sub-second reap evidence honest while removing noisy
// precision from longer wall times.
func shortDuration(d time.Duration) string {
	if d < time.Second {
		return d.Truncate(time.Millisecond).String()
	}
	return d.Truncate(time.Second).String()
}
