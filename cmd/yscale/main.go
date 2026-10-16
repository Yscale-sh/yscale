package main

import (
	"fmt"
	"os"
)

func main() {
	// Subcommand routing: yscale <subcommand> [flags].
	if len(os.Args) > 1 {
		switch os.Args[1] {
		case "apply":
			if err := runApply(os.Args[2:]); err != nil {
				fmt.Fprintln(os.Stderr, "yscale apply:", err)
				os.Exit(1)
			}
			return
		case "price":
			if err := runPrice(os.Args[2:]); err != nil {
				fmt.Fprintln(os.Stderr, "yscale price:", err)
				os.Exit(1)
			}
			return
		case "catalog":
			if err := runCatalog(os.Args[2:]); err != nil {
				fmt.Fprintln(os.Stderr, "yscale catalog:", err)
				os.Exit(1)
			}
			return
		case "inject":
			if err := runInject(os.Args[2:]); err != nil {
				fmt.Fprintln(os.Stderr, "yscale inject:", err)
				os.Exit(1)
			}
			return
		case "help", "-h", "--help":
			printUsage()
			return
		}
	}
	printUsage()
	os.Exit(2)
}

func printUsage() {
	fmt.Fprint(os.Stderr, `yscale — Kubernetes burst autoscaler CLI

Usage:
  yscale apply [-f FILE]            submit a Workload YAML
  yscale price gpu                  show the yscale GPU price index
  yscale catalog get [flags]        fetch a tenant template catalog
  yscale catalog apply [flags]      replace a tenant template catalog
  yscale inject -f FILE             inject burst nodeSelector/toleration into
                                    annotated Pod templates (never applies)
  yscale help                       show this message

For the Yscale Cluster Connector, see the separate yscale-agent binary
(yscale-agent remains the compatibility artifact name).
For the Yscale Control Plane API, see yscale-cloud.

Run 'yscale apply -h', 'yscale inject -h', or 'yscale -h' for flags.
`)
}
