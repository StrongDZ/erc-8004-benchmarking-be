package main

// sensitivity — Master CLI for snapshot management and parameter sensitivity benches.
//
// Subcommands:
//   snapshot create --label <label> [--chain-id <id>] [--include-spam]
//   snapshot list
//   snapshot inspect --id <snapshot_id>
//   snapshot drop --id <snapshot_id>
//   bench --cluster=<A|B|C|D> --method=<oat|tornado|sobol|grid> --snapshot=<snapshot_id>
//
// Bench is implemented incrementally by Plan-A through Plan-D.

import (
	"fmt"
	"os"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(1)
	}
	switch os.Args[1] {
	case "snapshot":
		runSnapshot(os.Args[2:])
	case "bench":
		runBench(os.Args[2:])
	case "-h", "--help", "help":
		usage()
	default:
		fmt.Fprintf(os.Stderr, "unknown subcommand %q\n", os.Args[1])
		usage()
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, `sensitivity — snapshot + bench tool

Usage:
  sensitivity snapshot create --label <label> [--chain-id <id>] [--include-spam]
  sensitivity snapshot list
  sensitivity snapshot inspect --id <snapshot_id>
  sensitivity snapshot drop --id <snapshot_id>
  sensitivity bench --cluster=<A|B|C|D> --method=<oat|tornado|sobol|grid> --snapshot=<snapshot_id>`)
}

func runSnapshot(args []string) {
	if len(args) < 1 {
		fmt.Fprintln(os.Stderr, "snapshot needs a subcommand: create | list | inspect | drop")
		os.Exit(1)
	}
	switch args[0] {
	case "create":
		snapshotCreate(args[1:])
	case "list":
		snapshotList(args[1:])
	case "inspect":
		snapshotInspect(args[1:])
	case "drop":
		snapshotDrop(args[1:])
	default:
		fmt.Fprintf(os.Stderr, "unknown snapshot subcommand %q\n", args[0])
		os.Exit(1)
	}
}

// runBench dispatches the parameter sensitivity bench (see bench.go).
func runBench(args []string) {
	benchDispatch(args)
}
