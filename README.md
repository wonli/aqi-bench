# AQI Benchmark

Reproducible load tests for [AQI](https://github.com/wonli/aqi), focused on WebSocket connection lifecycle, request/response throughput, latency, churn, and profiling.

## WebSocket baseline

The current baseline uses a real AQI application generated with `aqi new` and a Go load generator based on `github.com/gobwas/ws`.

Each logical client:

1. Connects to `/ws` with a unique `appId` and `clientId`.
2. Sends the `bench.echo` action at a configurable interval.
3. Validates the response `code`, `action`, request `id`, and echoed payload.
4. Records connection failures, runtime failures, sent/received counts, reconnects, throughput, and RTT percentiles.

The first message from each client is randomly spread across one full interval to avoid creating an artificial synchronized burst after connection establishment.

## Run the server

```bash
go run . api
```

The generated AQI application listens on port `2015` by default, so the WebSocket endpoint is:

```text
ws://127.0.0.1:2015/ws
```

The benchmark app also exposes Go pprof endpoints for CPU, heap, and goroutine profiling.

## Run the load generator

Example baseline:

```bash
go run ./cmd/loadtest \
  -url ws://127.0.0.1:2015/ws \
  -connections 15000 \
  -duration 5m \
  -interval 2s
```

Available flags:

| Flag | Default | Description |
| --- | ---: | --- |
| `-url` | `ws://127.0.0.1:2015/ws` | AQI WebSocket endpoint |
| `-connections` | `1000` | Concurrent logical WebSocket clients |
| `-duration` | `10m` | Test duration |
| `-interval` | `2s` | Echo interval per connection |
| `-churn` | `0` | Random reconnect interval; `0` disables churn |

For connection churn testing:

```bash
go run ./cmd/loadtest \
  -connections 1000 \
  -duration 5m \
  -interval 2s \
  -churn 30s
```

## Metrics

The summary reports:

- connection attempts and connection errors;
- runtime errors after a connection has been established;
- messages sent and received;
- successful reconnects;
- received messages per second;
- RTT P50, P95, and P99;
- top error types when failures occur.

For a clean run, the most important invariants are:

```text
Connect errors = 0
Runtime errors = 0
Messages sent == Messages recv
```

## Current verified result

Benchmark machine:

```text
CPU              Apple M1
Memory           16 GB
OS               macOS 15.7.7 (24G720)
Topology         load generator and AQI server on the same machine
Target           ws://127.0.0.1:2015/ws
```

Result:

```text
AQI WebSocket Baseline
────────────────────────────
Target           ws://127.0.0.1:2015/ws
Connections      15000
Duration         5m0.199s
Connect attempts 15000
Connect errors   0
Runtime errors   0
Messages sent    2226297
Messages recv    2226297
Reconnects       0
Throughput       7416.1 msg/s
RTT P50          890.292µs
RTT P95          14.759584ms
RTT P99          34.762167ms
```

This result was collected with AQI's WebSocket file ledger enabled. It is a reproducible project baseline, not a universal capacity claim: results depend on hardware, operating system, network topology, logging configuration, Go version, and workload.

## Localhost limits

Very high connection counts can hit the load generator machine before they hit AQI. In particular, a client and server running on the same macOS host and using a single `127.0.0.1:port` destination can run out of usable ephemeral source ports.

If failures look like source-port exhaustion or connection timeouts while pushing beyond the local port range, use multiple load-generator hosts/source IPs instead of treating that result as the AQI server limit.

## Profiling

During a run, capture profiles from the benchmark server and inspect them with standard Go tooling. The goal is to optimize only observed hot paths rather than changing queue sizes, locks, or goroutine structure speculatively.

Typical workflow:

```text
baseline -> load test -> inspect errors/latency -> pprof/-race -> isolate one variable -> repeat
```

The benchmark repository is intentionally kept separate from AQI so framework code and benchmark scenarios can evolve independently.
