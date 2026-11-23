# wsmon — persistent WebSocket synthetic monitoring

`wsmon` keeps persistent WebSocket tunnels open between its own client and its
own server, across your network path, and reports every time a tunnel is
disrupted or degraded.

## Why

HTTP synthetic checks hide short network problems. Each request is
independent, so when a hop between client and server (CDN, load balancer,
proxy, ingress) has a short failure, the request is queued or retried
and usually still succeeds. The check stays green.

A WebSocket is one long-lived connection through every hop. If any hop
restarts, fails over, drops idle connections or degrades, the tunnel is
cut and does not come back on its own. Users of real-time features notice.
HTTP checks don't.

`wsmon` measures this directly: it holds tunnels open, sends messages through
them, and records every connect failure, closed tunnel, lost message and
latency change.

## How it works

You run two things:

- **The server** (`wsmon server`): a small WebSocket echo server. Deploy it
  behind the network path you want to monitor (the same CDN, load balancers,
  proxies and ingress your real WebSocket traffic goes through). The server is
  only an endpoint for the tunnels.
- **The clients, or probes** (`wsmon client`): each probe opens one
  persistent WebSocket tunnel to the server, sends test messages through it,
  checks the echoes, reconnects when the tunnel is cut, and reports metrics
  and traces.

Put probes in different regions, clouds or networks. Each probe shows how a
persistent WebSocket connection from that location behaves: whether it can
connect, how long the tunnel lasts, why it was closed, round-trip latency,
lost messages, and how long an idle tunnel survives before a hop in the path
closes it.

The first argument picks the role:

```
wsmon server       run the echo server
wsmon client       run one probe
wsmon healthcheck  exit 0 if the local /healthz returns 200 (either role)
wsmon version      print the version
```

## Quick start

Build and run both on your machine (the build runs in Docker):

```sh
make run-server                  # terminal 1: server on :8080, metrics on :9090
make run-client MODE=pingpong    # terminal 2: probe, metrics on :9091
curl -s localhost:9091/metrics | grep wsmon_client_rtt
```

Or with the binary directly:

```sh
make build-local

./wsmon server

WSMON_URL=ws://localhost:8080/__synthetic/ws \
WSMON_METRICS_ADDR=:9091 \
WSMON_LABELS=target=local,vantage=laptop \
./wsmon client
```

With the container image, pass the role as the argument:

```sh
docker run -p 8080:8080 -p 9090:9090 wsmon:local server
docker run -e WSMON_URL=wss://relay.example.com/__synthetic/ws wsmon:local client
```

In Kubernetes, use `args: ["server"]` or `args: ["client"]`.

## Configuration

Everything is set with `WSMON_*` environment variables. There are no config
files or flags. To list every variable with its default:

```sh
wsmon server -h
wsmon client -h
```

The probe checks its whole configuration at startup and exits with a list of
every problem it found. It also refuses settings the server would reject,
such as a message rate above the server's cap.

## The server

The server accepts WebSocket connections on `WSMON_WS_PATH` (default
`/__synthetic/ws`), answers each probe message with an echo, and records
how long it spent handling each one. It knows nothing about targets, regions
or probes. You can run 2–3 replicas behind your normal ingress.

Each reply names the replica that sent it, so probes can tell which replica
answered. The server assumes nothing about where it runs (Kubernetes, a VM,
bare metal): give it a name, and put anything else you want to group by
(zone, node, region) in `WSMON_LABELS`.

| Variable | Purpose |
|---|---|
| `WSMON_NAME` | Replica name, sent in every reply. Defaults to the host name, which on Kubernetes is the pod name. |
| `WSMON_LABELS` | `name=value,...` labels put on every server metric and span, e.g. `zone=east-1a,node=ip-10-1-2-3`. Not sent to probes. |
| `WSMON_LISTEN_ADDR` | WebSocket listener (default `:8080`) |
| `WSMON_METRICS_ADDR` | Metrics and health listener (default `:9090`) |
| `WSMON_AUTH_TOKEN` | If set, connections must send this bearer token |
| `WSMON_MAX_CONNECTIONS` | Connections above this get HTTP 503 (default `2000`) |

On SIGTERM the server fails `/readyz`, waits `WSMON_DRAIN_DELAY` (default `5s`)
so the load balancer stops sending it traffic, then closes connections with
"going away". Probes reconnect to another replica.

## The client (probe)

One probe process makes one WebSocket connection and runs one mode. To run
several modes, or test several targets, run several probes.

The probe only needs `WSMON_URL`:

| Variable | Purpose |
|---|---|
| `WSMON_URL` | **Required.** `ws://` or `wss://` URL of the server, through the path you want to test |
| `WSMON_MODE` | `pingpong` (default), `burst` or `idle` |
| `WSMON_NAME` | This probe's name, sent to the server and shown on `wsmon_client_build_info` and in traces. Defaults to the host name. |
| `WSMON_LABELS` | `name=value,...` labels put on every metric and span. See [Labels](#labels-one-target-many-vantages). |
| `WSMON_AUTH_TOKEN` | Bearer token sent on connect |
| `WSMON_METRICS_ADDR` | Metrics and health listener (default `:9090`) |

### Modes

**`pingpong`** checks steady latency and message loss. It sends one message,
waits for the echo, pauses `WSMON_PINGPONG_INTERVAL` (default `1s`), and
repeats. Use this mode for your main latency and availability alerts.

**`burst`** checks behaviour under a short spike of traffic. It sends
`WSMON_BURST_SIZE` messages (default `20`) back to back, waits for all the
echoes, then pauses `WSMON_BURST_INTERVAL` (default `10s`). It reports the
time from the first send to the last echo.

**`idle`** checks how long a quiet connection survives. It sends nothing for
each step of `WSMON_IDLE_LADDER` (default `30s,60s,90s,120s,300s,600s`), then
sends one message to see if the connection is still alive. This finds proxies
and load balancers that drop idle connections. The result is reported for
each step, so you can see that 120s connections survive and 300s ones do not.

**Server push** (pingpong only): set `WSMON_PUSH_INTERVAL_MS` (100–3600000)
and the server also sends messages to the probe on that schedule. The probe
reports gaps and missing pushes, which tests the server-to-client direction
separately.

Other useful settings:

| Variable | Default | Purpose |
|---|---|---|
| `WSMON_PAYLOADS` | `small,large` | Rotate between small messages and large ones (`WSMON_LARGE_PAYLOAD_BYTES`, default 32 KiB) |
| `WSMON_MESSAGE_TIMEOUT` | `10s` | Echo deadline. After this a message counts as lost. |
| `WSMON_CONNECT_TIMEOUT` | `15s` | Time limit for the whole connect |
| `WSMON_BACKOFF_BASE` / `WSMON_BACKOFF_CAP` | `1s` / `30s` | Reconnect backoff, with jitter |
| `WSMON_ENABLE_PERMESSAGE_DEFLATE` | `false` | Offer WebSocket compression |

### Testing a specific path

To test one hop of a chain on its own, for example the CDN, the public load
balancer and an internal ingress, point probes at each hop. Use the same URL
and change where it connects:

| Variable | Purpose |
|---|---|
| `WSMON_RESOLVE_OVERRIDE` | `host:ip`: connect to this IP instead of resolving the host (like `curl --resolve`) |
| `WSMON_HOST_HEADER` | Override the `Host` header |
| `WSMON_SNI_HOST` | Override the TLS server name |
| `WSMON_HEADERS` | Extra request headers, e.g. `X-Synthetic-Probe=wsmon` |
| `WSMON_UPGRADE_TRACEPARENT` | Send a `traceparent` header on the upgrade request (default `true`; see [Traces](#traces)) |
| `WSMON_URL_PARAMS` | Extra query parameters |
| `WSMON_INSECURE_SKIP_VERIFY` | Skip TLS verification (`wss://` only) |

## Telemetry

### Metrics

Both roles serve Prometheus metrics at `/metrics` on `WSMON_METRICS_ADDR`.
Histograms are exposed as classic buckets and as native histograms.

The main **probe** metrics:

| What | Metric | Labels |
|---|---|---|
| Is the connection up | `wsmon_client_connection_up` | `mode` |
| Connect attempts / failures | `wsmon_client_connect_attempts_total`, `wsmon_client_connect_failures_total` | `mode`, `reason` (`dns`, `tcp`, `tls`, `auth`, `http_status`, `upgrade_rejected`, `timeout`, …) |
| Connect time per step | `wsmon_client_connect_duration_seconds` | `mode`, `phase` (`dns`, `tcp`, `tls`, `upgrade`, `hello`) |
| Connection closes and lifetime | `wsmon_client_closes_total`, `wsmon_client_connection_lifetime_seconds` | `mode`, `close_code`, `initiator` |
| Round-trip latency | `wsmon_client_rtt_seconds` | `mode`, `payload`, `server_name` |
| Latency without server time | `wsmon_client_rtt_net_seconds` | `mode`, `payload` |
| Messages sent / echoed / lost / late | `wsmon_client_messages_{sent,echoed,lost,late}_total` | `mode`, `payload` |
| Corrupted or misordered echoes | `wsmon_client_messages_{mismatched,duplicate,out_of_order,unexpected}_total` | `mode` |
| Burst completion time | `wsmon_client_burst_completion_seconds` | `payload` |
| Idle survival | `wsmon_client_idle_probe_total` | `idle_bucket`, `result` (`survived`, `dropped`) |
| Who closed an idle connection | `wsmon_client_idle_probe_close_total` | `idle_bucket`, `close_code` |
| Server push | `wsmon_client_push_{received,lost,out_of_order}_total`, `wsmon_client_push_gap_seconds` | |
| Error frames from the server | `wsmon_client_server_errors_received_total` | `code` |
| Probe health | `wsmon_client_loop_lag_seconds`, `wsmon_client_clock_offset_seconds`, `wsmon_client_build_info`, `wsmon_client_config_hash` | |

Every probe metric also carries all of your `WSMON_LABELS`.

The **server** metrics (`wsmon_server_*`) cover open connections, messages by
type, handling time, errors and closes. They carry no probe labels.

### Pushing metrics instead of scraping

Probes in other clouds or behind NAT are often not reachable by your
Prometheus. Set `WSMON_REMOTE_WRITE_URL` and the probe pushes its metrics with
Prometheus remote-write every `WSMON_REMOTE_WRITE_INTERVAL` (default `15s`).
The series are the same as on `/metrics`, plus `job="wsmon-probe"`. Prometheus
(with `--web.enable-remote-write-receiver`), Mimir, Thanos Receive and Grafana
Cloud all accept it. Use `WSMON_REMOTE_WRITE_BEARER_TOKEN`, or
`WSMON_REMOTE_WRITE_USERNAME` and `WSMON_REMOTE_WRITE_PASSWORD`, for auth.

Scrape a probe or have it push. Don't do both into the same Prometheus, or
every series is stored twice.

### Traces

Set `WSMON_OTLP_ENDPOINT` (`host:port` for plaintext, or an `https://` URL) to
export OpenTelemetry traces. The probe records `ws.connect` (with every
connect step and any CDN or load-balancer request IDs from the response) and
`ws.roundtrip`. The server records `ws.echo` as a child span, so one trace
covers both ends. A share of round trips is sampled
(`WSMON_TRACE_SAMPLE_RATIO`, default `0.02`). Round trips slower than
`WSMON_TRACE_SLOW_THRESHOLD` (default `500ms`) are always traced. Latency
histograms link to example traces (exemplars).

Each upgrade request carries a W3C `traceparent` header with the trace and
span ID of that attempt's `ws.connect`, so a CDN, load balancer or server
that logs or traces `traceparent` can be joined to the probe's trace. This is
the only link for a failed handshake. Set `WSMON_UPGRADE_TRACEPARENT=false`
to stop sending it, for example to put a static `Traceparent` in
`WSMON_HEADERS` instead (both at once is a config error).

Service names: `wsmon-probe` and `wsmon-server`.

### Health

Both roles serve `/healthz` and `/readyz` on the metrics port. The image has
no shell or `curl`, so for Docker or other container-runtime health checks use:

```
HEALTHCHECK CMD ["/wsmon", "healthcheck"]
```

## Distributed monitoring: one server, many probes

```
           vantage: nimbus-east1    vantage: stratus-west2   vantage: vps-lima
           ┌──────────────┐         ┌──────────────┐         ┌──────────────┐
           │ probe  ping  │         │ probe  ping  │         │ probe  ping  │
           │ probe  burst │         │ probe  burst │         │ probe  idle  │
           │ probe  idle  │         │ probe  idle  │         └──────┬───────┘
           └──────┬───────┘         └──────┬───────┘                │
                  └────────────────┬───────┴────────────────────────┘
                                   ▼
                     CDN → load balancer → ingress
                                   ▼
                          wsmon server (target)
```

1. **Deploy the server once per target**: a target is one wsmon server
   endpoint, placed behind the network path you want to monitor (for example,
   inside a production cluster, behind the same CDN, load balancer and ingress
   as your real WebSocket traffic). Two or three replicas are enough.
2. **Deploy probes at each vantage point**: each region, cloud or network you
   want to test that path from. Include at least one outside your own cloud,
   or you only learn how the path behaves from inside your own network.
3. **Run one probe per mode** at each vantage, usually one each of
   `pingpong`, `burst` and `idle`.
4. **Keep the probes of one vantage together** (one Pod, one container task or one
   host). They then share a network path, so when one entry point is slow and
   another is not, the cause is that entry point and not the probe's network.
5. **Get the metrics to one Prometheus**, by scraping or by remote-write.

Probes are independent. One that crashes or loses its network affects only
its own series.

## Labels: one target, many vantages

The probe does not know where it runs or what it is testing. You give it
that with `WSMON_LABELS`. Every metric and span from that probe carries
those labels, so you can group and compare probes in Prometheus and Grafana.

A good label set:

| Label | Meaning | Example |
|---|---|---|
| `target` | The wsmon server endpoint (and the network path in front of it); the same on every probe that connects to it | `relay-prod` |
| `vantage` | Where the probe runs; the same on every probe in one place | `nimbus-east1`, `stratus-west2`, `vps-lima` |
| `entrypoint` | Which path into the target, if you test several | `skyedge`, `edge-lb`, `ingress` |
| `env` | Environment | `prod` |

You can add others, such as `provider` or `region`. `mode` is reserved, because
the probe sets it.

Example: the same target from two places.

```sh
# Lima VPS
WSMON_URL=wss://relay.example.com/__synthetic/ws
WSMON_MODE=pingpong
WSMON_LABELS=target=relay-prod,vantage=vps-lima,entrypoint=skyedge,env=prod

# Nimbus east-1
WSMON_URL=wss://relay.example.com/__synthetic/ws
WSMON_MODE=pingpong
WSMON_LABELS=target=relay-prod,vantage=nimbus-east1,entrypoint=skyedge,env=prod
```

Rules:

- **Every probe must have a unique label set plus `mode`.** Two probes with the
  same labels and mode produce the same series and overwrite each other.
  Usually `vantage` is what makes them unique.
- **Keep labels low-cardinality.** Use names of places and paths only. Never
  use hostnames, pod IDs, IPs or URLs, because each value creates new series.
  Those details are in traces and logs.

### Example queries

p95 round-trip latency for one target, one line per vantage:

```promql
histogram_quantile(0.95,
  sum by (vantage, le) (
    rate(wsmon_client_rtt_seconds_bucket{target="relay-prod", mode="pingpong"}[5m])))
```

Message loss ratio per vantage:

```promql
  sum by (vantage) (rate(wsmon_client_messages_lost_total{target="relay-prod"}[5m]))
/
  sum by (vantage) (rate(wsmon_client_messages_sent_total{target="relay-prod"}[5m]))
```

Vantages that cannot hold a connection:

```promql
min by (vantage, mode) (wsmon_client_connection_up{target="relay-prod"}) == 0
```

Connect failures by reason, to see whether failures are in DNS, TLS or the
upgrade:

```promql
sum by (vantage, reason) (rate(wsmon_client_connect_failures_total{target="relay-prod"}[15m]))
```

Idle connections dropped, per vantage and idle duration:

```promql
sum by (vantage, idle_bucket) (increase(wsmon_client_idle_probe_total{target="relay-prod", result="dropped"}[6h]))
```

When tunnels from every vantage are cut or slow at the same time, the problem
is probably in the shared path near the target (ingress, load balancer,
cluster). When only one vantage is affected, it is probably that vantage's
network or the edge nearest to it. When one `entrypoint` is slow and the one behind it is not, the problem is
the layer between them.

## Development

All Go commands run in a `golang` container, so `make` needs only Docker.

```sh
make test          # unit, integration and client/server contract tests
make lint          # go vet and gofmt
make image-local   # build the container image as wsmon:local
make image IMAGE=ghcr.io/<owner>/wsmon PUSH=1   # multi-arch build and push
make helm-test     # helm lint, then render every helm/wsmon/ci fixture (needs helm)
make check-version # Makefile VERSION == Chart version == Chart appVersion
```

## Releasing

The Makefile `VERSION`, the chart `version` and the chart `appVersion` are
always the same. To release `X.Y.Z`, set all three to `X.Y.Z` on `main`, then
push the tag `vX.Y.Z`. The release workflow checks the versions, runs the tests,
and publishes:

| Artifact | Reference |
|---|---|
| Image (linux/amd64, linux/arm64) | `ghcr.io/asappinc-opensource/wsmon:X.Y.Z` |
| Helm chart | `oci://ghcr.io/asappinc-opensource/wsmon-helm/wsmon --version X.Y.Z` |

```sh
helm install wsmon oci://ghcr.io/asappinc-opensource/wsmon-helm/wsmon --version X.Y.Z \
  --set image.repository=ghcr.io/asappinc-opensource/wsmon --set server.enabled=true
```

Both artifacts are signed with [cosign](https://github.com/sigstore/cosign)
keyless signing; the image also carries SBOM and provenance attestations.
To verify, check that the signature comes from this repository's release
workflow running for that exact tag:

```sh
VERSION=X.Y.Z
IDENTITY=https://github.com/asappinc-opensource/wsmon/.github/workflows/release.yaml@refs/tags/v$VERSION
ISSUER=https://token.actions.githubusercontent.com

cosign verify ghcr.io/asappinc-opensource/wsmon:$VERSION \
  --certificate-identity "$IDENTITY" --certificate-oidc-issuer "$ISSUER"
cosign verify ghcr.io/asappinc-opensource/wsmon-helm/wsmon:$VERSION \
  --certificate-identity "$IDENTITY" --certificate-oidc-issuer "$ISSUER"
```

Prefer `--certificate-identity` (an exact match) over
`--certificate-identity-regexp`: an unanchored or loosely escaped pattern can
accept signatures from other workflows or refs.

## License

MIT. See [LICENSE](LICENSE).
