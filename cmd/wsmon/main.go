// Command wsmon is the WebSocket synthetic connection monitor.
//
//	wsmon server       run the echo server
//	wsmon client       run one probe: one connection, one mode
//	wsmon healthcheck  exit 0 if the local /healthz returns 200 (either role)
//	wsmon version      print the version
//
// Both roles are configured only through WSMON_* environment variables;
// `wsmon server -h` and `wsmon client -h` list them.
package main

import (
	"fmt"
	"io"
	"os"
	"time"

	"github.com/asappinc-opensource/wsmon/internal/config"
	"github.com/asappinc-opensource/wsmon/internal/telemetry"
)

// version is set at build time with -ldflags "-X main.version=...". Both
// roles report it: the server in every frame's server.version, the client in
// client.version.
var version = "dev"

const usage = `usage: wsmon <command>

commands:
  server       run the WebSocket echo server
  client       run the synthetic-monitoring probe
  healthcheck  exit 0 if http://127.0.0.1:$WSMON_METRICS_ADDR/healthz is 200
  version      print the version

Configuration comes only from WSMON_* environment variables.
Run "wsmon server -h" or "wsmon client -h" to list them.
`

func main() { os.Exit(run(os.Args[1:], os.Stdout, os.Stderr)) }

func run(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		fmt.Fprint(stderr, usage)
		return 2
	}
	cmd, rest := args[0], args[1:]
	if len(rest) == 1 && isHelp(rest[0]) {
		switch cmd {
		case "server":
			fmt.Fprintf(stdout, "usage: wsmon server\n\nenvironment:\n%s", config.Usage(config.ServerVars))
			return 0
		case "client":
			fmt.Fprintf(stdout, "usage: wsmon client\n\nenvironment:\n%s", config.Usage(config.ClientVars))
			return 0
		}
	}
	if len(rest) > 0 {
		fmt.Fprint(stderr, usage)
		return 2
	}
	switch cmd {
	case "server":
		return runServer(stdout)
	case "client":
		return runClient(stdout)
	case "healthcheck":
		return healthcheck(stderr)
	case "version":
		fmt.Fprintln(stdout, version)
		return 0
	case "help", "-h", "-help", "--help":
		fmt.Fprint(stdout, usage)
		return 0
	default:
		fmt.Fprint(stderr, usage)
		return 2
	}
}

func isHelp(s string) bool { return s == "-h" || s == "-help" || s == "--help" || s == "help" }

// healthcheck reads only WSMON_METRICS_ADDR, so it works for either role
// and even when the rest of the configuration is missing.
func healthcheck(stderr io.Writer) int {
	port, ok := config.MetricsPort(os.Getenv)
	if !ok {
		fmt.Fprintf(stderr, "healthcheck: invalid %s\n", config.EnvMetricsAddr)
		return 1
	}
	if err := telemetry.Healthcheck(port, 2*time.Second); err != nil {
		fmt.Fprintf(stderr, "healthcheck failed: %v\n", err)
		return 1
	}
	return 0
}
