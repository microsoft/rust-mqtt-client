<!--
Copyright (c) Microsoft Corporation.
Licensed under the MIT License.
-->

# iso_bench — isolation benchmarks for `ms-mqtt-client`

A small tooling workspace for detecting **software performance regressions** in the client's
transport, by comparing runs of one build against another (e.g. `main` vs. a refactor branch).

> **This is a regression detector, not a real-world benchmark.** It deliberately runs everything on
> **one machine over loopback** so the network, other hardware, and broker behavior are *not*
> confounds. The numbers are meaningful **relative to another run on the same box**, not as absolute
> real-world figures.

## How it works

To measure the client in isolation we do **not** use a real broker (a broker's own latency and
throughput would confound the result). Instead the client under test talks to a trivial, controlled
peer that stands in for "the other end of the connection":

| Component | What it is |
|---|---|
| **`bench_client`** | The instrumented MQTT client (uses `ms-mqtt-client`). Runs a workload and prints a machine-readable `RESULT` line. |
| **`bench_peer`** | An independent MQTT 5 peer that hand-rolls the minimal wire bytes and does **not** depend on `ms-mqtt-client`, so its behavior is invariant across client builds. It either firehoses messages at the client or drains/acks the client's messages. |

Because the peer is trivial and build-invariant, any difference between two runs is attributable to
the **client**.

## Recommended target for running it

- **Azure VM: `Standard_F8s_v2`** (8 vCPU, compute-optimized, **non-burstable**). `F4s_v2` (4 vCPU)
  is the workable minimum. Avoid **B-series** (burstable — credit throttling ruins measurements).
- **A single VM, loopback only.** Keeping both processes on one host removes the network as a
  variable and holds the hardware constant across A/B runs — exactly what a regression detector
  wants. Containers (bridge networking adds a confound) and multi-VM (network becomes a variable)
  are intentionally avoided.
- **Core budget (why 8 vCPU):** the client wants ~2 cores (its connection reader and workload
  consumer run concurrently), the peer ~1–2 (more under TLS, which encrypts live), and the OS needs
  headroom. Loopback has no hardware NIC IRQ to steer, so the main job is keeping the client and
  peer on **separate physical cores**.
- The tooling runs anywhere Linux + Rust is available; the VM above is only the *recommended* target
  for trustworthy numbers.

> **Azure guest caveat:** you generally cannot set the CPU governor / turbo / C-states from inside
> the guest. Determinism therefore comes from **core pinning + repeated, alternating runs + reading
> tails (p99/p99.9)**, not from power settings.

## Prerequisites

- A Rust toolchain (the repo pins one via `rust-toolchain.toml`).
- `taskset` (from `util-linux`) — for core pinning.
- GNU `time` at `/usr/bin/time` — for CPU-per-message (optional; the wrapper degrades gracefully).
- `openssl` CLI — only for generating TLS test certs.
- `tc` (from `iproute2`) and root — only if you use `NETEM_DELAY` to inject a controlled RTT.

## Quick start (recommended: `bench.sh`)

[`bench.sh`](bench.sh) is the main entry point: it runs the **full curated
suite** — latency, throughput, and inbound, each over TCP and TLS — `REPS` reps per config, and
prints a per-config summary plus (for A/B) a per-config comparison. One command covers the whole
regression surface.

```bash
LABEL=main ./bench.sh          # full suite (~10 reps per config)
```

Under the suite sit two lower-level entry points you can use directly:

- [`bench-workload.sh`](bench-workload.sh) — **one config**, `REPS` reps, aggregated (median/mean/min/max/CV%):
  ```bash
  LABEL=main REPS=8 MODE=latency QOS=1 COUNT=100000 ./bench-workload.sh
  LABEL=main REPS=8 MODE=inbound PAYLOAD_BYTES=16384 COUNT=300000 ./bench-workload.sh
  ```
- [`bench-once.sh`](bench-once.sh) — **one config, one rep** (a single `RESULT` + `CPU` line, no
  aggregation); handy for debugging one run:
  ```bash
  MODE=latency QOS=1 COUNT=50000 ./bench-once.sh
  ```

All of them build the binaries, derive the peer role from `MODE`, match the peer's payload for
`inbound`, and (for TLS) generate certs automatically. Override core pinning with `CLIENT_CORES` /
`PEER_CORES` (defaults `2,3` and `4,5` suit an F8s_v2); add `NETEM_DELAY=5ms` for a controlled
loopback RTT (needs root).

## Manual usage (two terminals)

If you'd rather drive the two processes yourself, start the peer first, then point the client at it.
Run both from this directory. **Always build `--release`** — a dev build is several times slower.

| Client `MODE` | Peer `ROLE` | Measures |
|---|---|---|
| `inbound` | `feed` | client **receive** throughput (peer firehoses PUBLISHes) |
| `latency` | `sink` | **round-trip** publish→PUBACK latency (QoS 1) |
| `throughput` | `sink` | client **send** throughput, pipelined |

```bash
# Terminal A — peer (inbound example; PAYLOAD_BYTES must match the client)
ROLE=feed PORT=1883 PAYLOAD_BYTES=256 cargo run --release -p bench_peer

# Terminal B — measured client
MODE=inbound HOST=127.0.0.1 PORT=1883 PAYLOAD_BYTES=256 \
  WARMUP=5000 COUNT=200000 LABEL=main cargo run --release -p bench_client
```

The peer accepts a fresh connection per client run, so you can leave it up and fire many client runs.

## TLS

Generate a self-signed cert for local testing (used as **both** the peer's identity and the client's
trust anchor). `certs/` is gitignored.

```bash
./gen-test-certs.sh                 # writes certs/server.{crt,key}

# peer:
ROLE=feed TLS=1 PORT=8883 CERT_FILE=certs/server.crt KEY_FILE=certs/server.key \
  PAYLOAD_BYTES=256 cargo run --release -p bench_peer
# client:
MODE=inbound TRANSPORT=tls PORT=8883 CA_FILE=certs/server.crt HOST=127.0.0.1 \
  PAYLOAD_BYTES=256 COUNT=200000 cargo run --release -p bench_client
```

> **TLS throughput caveat:** unlike plaintext, the peer must **encrypt live** (TLS records can't be
> pre-encoded or replayed), so it is no longer trivially faster than the client — single-session TLS
> throughput is bounded by `min(peer encrypt, client decrypt)`. For the crypto-cost signal, rely on
> the client's **CPU-per-message** (below), which stays un-confounded, and use `latency` mode for
> TLS round-trip.

## Reading the output

Each client run prints a human summary plus one machine-readable line:

```
RESULT {"label":"main","mode":"inbound","transport":"tcp","qos":1,"payload_bytes":256,
        "inflight":1,"interval_us":0,"count":200000,"wall_s":0.83,"msgs_per_s":240093.4,
        "mib_per_s":58.6,"lat_kind":"inter-arrival","lat_us":{"min":0.1,"p50":0.5,...}}
```

- **`msgs_per_s` / `mib_per_s`** — the headline for `inbound` and `throughput`.
- **`lat_us`** percentiles — labeled by `lat_kind`: `op latency` (round-trip / per-op for
  latency/throughput) or `inter-arrival` (reader-path jitter for `inbound` — *not* a round trip).

The `bench-once.sh` primitive adds a CPU line from `/usr/bin/time` (per rep; `bench-workload.sh`
aggregates these across reps):

```
CPU {"user_s":0.10,"sys_s":0.30,"cpu_us_per_msg":80.0,"max_rss_kb":5808}
```

`cpu_us_per_msg` is measured on the client process alone, so it stays clean even under same-host
contention — it's the sharpest signal for crypto/copy-path regressions.

### Histograms

Every record also carries `hist_ns` — the full latency/inter-arrival distribution as log-spaced
HdrHistogram buckets (`[upper_bound_ns, count]`), so a histogram can be reconstructed offline without
keeping every raw sample. Render it with [`histogram.py`](histogram.py), which sums the buckets
across a config's reps and prints a text histogram:

```bash
python3 histogram.py results.jsonl <config> [label]
```

## Comparing builds (A/B)

Run the suite on each git ref, tagged with `LABEL`; results accumulate in `results.jsonl` and the
second run prints a **per-config** comparison (median deltas of the latest label vs. the baseline,
flagged against the baseline's run-to-run CV):

```bash
RESET=1 LABEL=main     ./bench.sh   # build A (fresh results file)
git checkout my-refactor
        LABEL=refactor ./bench.sh   # build B -> per-config comparison
```

Read it as: **latency and `cpu_us_per_msg` going up = regression; `msgs_per_s` going down =
regression.** The `note` column flags whether a delta exceeds the baseline's CV — a rough signal, not
a formal test. (`bench-workload.sh` does the same for a single config; both share
[`aggregate.py`](aggregate.py).)

## How to run it for meaningful numbers

- **Establish the noise floor first:** run the *same* build twice under two labels; the delta you see
  is your detection threshold. Only trust A/B deltas larger than that band.
- **Size `COUNT` for the tail:** stable p99.9 needs ~10⁵ operations per run.
- **Use enough reps** (`REPS=8`+; more if CV% is wide relative to the effect you're chasing), and read
  **tails (p99/p99.9) and `cpu_us_per_msg`**, not means.
- The minimum latency across reps is a useful low-noise "true cost" estimator (wall-clock noise only
  ever adds time).

## Environment variables

**`bench_client`** — connection: `HOST` `PORT` `TRANSPORT`(tcp|tls) `CLIENT_ID` `USERNAME`
`PASSWORD` `CA_FILE` `CERT_FILE` `KEY_FILE` `CONNECT_TIMEOUT_SECS` `KEEPALIVE_SECS`; workload:
`MODE`(latency|throughput|inbound) `QOS`(0|1) `TOPIC` `PAYLOAD_BYTES` `COUNT` `WARMUP` `INFLIGHT`
`INTERVAL_US` `LABEL`.

**`bench_peer`** — `ROLE`(feed|sink) `BIND` `PORT` `TLS`(0|1) `CERT_FILE` `KEY_FILE` `TOPIC`
`PAYLOAD_BYTES` `BATCH` `RATE`(feed; 0=max).

**`bench-once.sh`** — all of the client knobs, plus `CLIENT_CORES` `PEER_CORES` `NETEM_DELAY`
`CERT_DIR` `BATCH` `RATE`.

**`bench-workload.sh`** — all of the `bench-once.sh` knobs, plus `REPS` `LABEL` `RESULTS_FILE` `RESET`
`CONFIG`.

**`bench.sh`** — `REPS` `LABEL` `RESULTS_FILE` `RESET` (+ pinning / `NETEM_DELAY` passed
through); the per-config workloads are a curated list inside the script.

Pass `--help` (or `HELP=1`) to either binary, or `-h` to any script, for the full list.

## Limitations & caveats

- **QoS 2 is not implemented** (the client doesn't implement it either).
- **`inbound` requires `PAYLOAD_BYTES` to match** between peer (`feed`) and client, or the reported
  MiB/s is wrong.
- **Loopback** exercises all of the client's transport software (syscalls, kernel TCP, TLS, framing,
  decode, buffer pool) but not real NIC offloads — which is correct for a *software* regression
  detector.
- **Closed-loop workloads** (each op waits for completion): tail latency under load can be
  under-reported (coordinated omission). An open-loop / rate-based mode is a future improvement.
- TLS throughput is `min(peer, client)`-bound (see the TLS caveat above).

## Layout

```
iso_bench/                # detached cargo workspace (not part of the library's build)
  bench_client/           # the measured MQTT client harness
  bench_peer/             # the independent stand-in peer (TCP + TLS)
  bench.sh                # PRIMARY: full curated suite (all workloads) + per-config A/B
  bench-workload.sh       # one config: N reps + aggregate stats + A/B
  bench-once.sh           # single run (peer + pinned, timed client)
  aggregate.py            # results aggregator (median/CV%/A-B); per-config, or run by hand for all
  histogram.py            # renders a text histogram from the per-rep hist_ns buckets
  gen-test-certs.sh       # self-signed TLS cert generator (local testing only)
```
