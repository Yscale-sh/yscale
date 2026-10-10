// Command yscale-launch-readiness is the deterministic local validator for
// the launch release-evidence manifest.
//
//	yscale-launch-readiness schema    -manifest <path>
//	yscale-launch-readiness readiness -manifest <path> [-root <dir>] [-allow-synthetic]
//	yscale-launch-readiness matrix    -manifest <path> [-verify -doc <path>] [-out <path>]
//
// schema checks shape only and exits 0 even when every gate is pending;
// schema validity is explicitly NOT readiness. readiness is fail-closed: it
// exits nonzero unless every required gate is proven with verifiable
// evidence. matrix generates (or verifies) the supported-configuration
// document from the manifest.
package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/yscale-sh/yscale/internal/launch"
)

func main() {
	os.Exit(run(os.Args[1:]))
}

func run(args []string) int {
	if len(args) < 1 {
		usage()
		return 2
	}
	switch args[0] {
	case "schema":
		return runSchema(args[1:])
	case "readiness":
		return runReadiness(args[1:])
	case "matrix":
		return runMatrix(args[1:])
	case "-h", "-help", "--help", "help":
		usage()
		return 0
	default:
		fmt.Fprintf(os.Stderr, "unknown subcommand %q\n", args[0])
		usage()
		return 2
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: yscale-launch-readiness <schema|readiness|matrix> -manifest <path> [flags]")
	fmt.Fprintln(os.Stderr, "  schema    -manifest <path>                       schema-only validation (NOT readiness)")
	fmt.Fprintln(os.Stderr, "  readiness -manifest <path> [-root <dir>] [-allow-synthetic]")
	fmt.Fprintln(os.Stderr, "                                                   fail-closed launch readiness")
	fmt.Fprintln(os.Stderr, "  matrix    -manifest <path> [-verify -doc <path>] [-out <path>]")
	fmt.Fprintln(os.Stderr, "                                                   generate or verify the supported-configuration matrix")
}

func loadManifest(fs *flag.FlagSet, manifestPath string) (*launch.Manifest, int) {
	if manifestPath == "" {
		fmt.Fprintln(os.Stderr, "-manifest is required")
		fs.Usage()
		return nil, 2
	}
	manifest, err := launch.LoadManifest(manifestPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		return nil, 1
	}
	return manifest, 0
}

func runSchema(args []string) int {
	fs := flag.NewFlagSet("schema", flag.ExitOnError)
	manifestPath := fs.String("manifest", "", "path to the release-evidence manifest")
	fs.Parse(args)
	manifest, code := loadManifest(fs, *manifestPath)
	if manifest == nil {
		return code
	}
	if errs := launch.ValidateSchema(manifest); len(errs) > 0 {
		fmt.Fprintf(os.Stderr, "schema: INVALID — %d error(s)\n", len(errs))
		for _, err := range errs {
			fmt.Fprintf(os.Stderr, "  %s\n", err)
		}
		return 1
	}
	fmt.Println("schema: VALID (schema validity is NOT readiness; run the readiness subcommand for the fail-closed gate check)")
	return 0
}

func runReadiness(args []string) int {
	fs := flag.NewFlagSet("readiness", flag.ExitOnError)
	manifestPath := fs.String("manifest", "", "path to the release-evidence manifest")
	root := fs.String("root", ".", "repository root for resolving local-file evidence")
	allowSynthetic := fs.Bool("allow-synthetic", false, "test-only: allow a synthetic manifest to pass (result stays marked SYNTHETIC)")
	fs.Parse(args)
	manifest, code := loadManifest(fs, *manifestPath)
	if manifest == nil {
		return code
	}
	report := launch.EvaluateReadiness(manifest, launch.ReadinessOptions{
		Root:           *root,
		AllowSynthetic: *allowSynthetic,
	})
	fmt.Print(report.Render())
	if !report.Ready {
		return 1
	}
	return 0
}

func runMatrix(args []string) int {
	fs := flag.NewFlagSet("matrix", flag.ExitOnError)
	manifestPath := fs.String("manifest", "", "path to the release-evidence manifest")
	verify := fs.Bool("verify", false, "verify a checked-in matrix document instead of printing")
	docPath := fs.String("doc", "", "path to the checked-in matrix document (with -verify)")
	outPath := fs.String("out", "", "write the generated matrix to this path instead of stdout")
	fs.Parse(args)
	manifest, code := loadManifest(fs, *manifestPath)
	if manifest == nil {
		return code
	}
	if *verify {
		if *docPath == "" {
			fmt.Fprintln(os.Stderr, "-doc is required with -verify")
			fs.Usage()
			return 2
		}
		doc, err := os.ReadFile(*docPath)
		if err != nil {
			fmt.Fprintf(os.Stderr, "error: read doc: %v\n", err)
			return 1
		}
		if err := launch.VerifyMatrix(manifest, *manifestPath, doc); err != nil {
			fmt.Fprintf(os.Stderr, "matrix: MISMATCH — %v\n", err)
			return 1
		}
		fmt.Println("matrix: OK — document matches the manifest")
		return 0
	}
	generated, err := launch.GenerateMatrix(manifest, *manifestPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		return 1
	}
	if *outPath != "" {
		if err := os.WriteFile(*outPath, []byte(generated), 0o644); err != nil {
			fmt.Fprintf(os.Stderr, "error: write matrix: %v\n", err)
			return 1
		}
		fmt.Fprintf(os.Stderr, "wrote %s\n", *outPath)
		return 0
	}
	fmt.Print(generated)
	return 0
}
