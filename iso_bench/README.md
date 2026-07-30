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
  peer on **separate physical cores**. **Open-loop `pub-latency` (and `recv-latency`) want one *extra*
  core** (client 3 total, or peer for `recv-latency`) — the pacer busy-spins a core, so give it a
  dedicated one to keep the measured side at its full budget (e.g. `CLIENT_CORES=2,3,4 PEER_CORES=5,6`).
  F8s_v2 still fits; `F4s_v2` is then too tight.
- The tooling runs anywhere Linux + Rust is available; the VM above is only the *recommended* target
  for trustworthy numbers.

> **Azure guest caveat:** you generally cannot set the CPU governor / turbo / C-states from inside
> the guest. Determinism therefore comes from **core pinning + repeated, alternating runs + reading
> tails (p99/p99.9)**, not from power settings.

## Prerequisites

On a fresh VM, run [`check-prereqs.sh`](check-prereqs.sh) to detect and install everything below
(apt/dnf/yum; `--check` reports without installing):

```bash
./check-prereqs.sh          # install what's missing
./check-prereqs.sh --check  # report only (exit 1 if anything required is missing)
```

- A Rust toolchain (the repo pins one via `rust-toolchain.toml`).
- A C compiler + `pkg-config` + libssl headers (`libssl-dev` / `openssl-devel`) — to build the
  `openssl` crate.
- `taskset` (from `util-linux`) — for core pinning.
- GNU `time` at `/usr/bin/time` — for CPU-per-message (optional; the wrapper degrades gracefully).
- `openssl` CLI — only for generating TLS test certs.
- `tc` (from `iproute2`) and root — only if you use `NETEM_DELAY` to inject a controlled RTT.

## Quick start (recommended: `bench.sh`)

[`bench.sh`](bench.sh) is the main entry point: it runs the **full curated
suite** — the four modes (`pub-latency`, `pub-throughput`, `recv-throughput`, `recv-latency`) over
TCP and TLS, plus variants that isolate one path each (QoS 0 send, small-payload throughput,
large-payload latency, QoS 1 receive+PUBACK) and two **open-loop latency-under-load** configs
(coordinated-omission-correct) — `REPS`
reps per config, and prints a per-config summary plus (for A/B) a per-config comparison. One command
covers the whole regression surface.

```bash
LABEL=main ./bench.sh          # full suite (~10 reps per config)
```

Under the suite sit two lower-level entry points you can use directly:

- [`bench-workload.sh`](bench-workload.sh) — **one config**, `REPS` reps, aggregated (median/mean/min/max/CV%):
  ```bash
  LABEL=main REPS=8 MODE=pub-latency QOS=1 COUNT=100000 ./bench-workload.sh
  LABEL=main REPS=8 MODE=recv-throughput PAYLOAD_BYTES=16384 COUNT=300000 ./bench-workload.sh
  ```
- [`bench-once.sh`](bench-once.sh) — **one config, one rep** (a single `RESULT` + `CPU` line, no
  aggregation); handy for debugging one run:
  ```bash
  MODE=pub-latency QOS=1 COUNT=50000 ./bench-once.sh
  ```

All of them build the binaries, derive the peer role from `MODE`, match the peer's payload for
the `recv-*` modes, and (for TLS) generate certs automatically. Override core pinning with `CLIENT_CORES` /
`PEER_CORES` (defaults `2,3` and `4,5` suit an F8s_v2); add `NETEM_DELAY=5ms` for a controlled
loopback RTT (needs root).

## Manual usage (two terminals)

If you'd rather drive the two processes yourself, start the peer first, then point the client at it.
Run both from this directory. **Always build `--release`** — a dev build is several times slower.

| Client `MODE` | Peer `ROLE` | Measures |
|---|---|---|
| `recv-throughput` | `feed` | client **receive** throughput (peer firehoses PUBLISHes; `QOS=1` on both adds the receive-side PUBACK path) |
| `recv-latency` | `feed` (`STAMP=1`) | per-message **delivery** latency wire→app (peer stamps send time; paced via `RATE`) |
| `pub-latency` | `sink` | **round-trip** publish→PUBACK latency (QoS 1) |
| `pub-throughput` | `sink` | client **send** throughput, pipelined |

```bash
# Terminal A — peer (recv-throughput example; PAYLOAD_BYTES must match the client)
ROLE=feed PORT=1883 PAYLOAD_BYTES=256 cargo run --release -p bench_peer

# Terminal B — measured client
MODE=recv-throughput HOST=127.0.0.1 PORT=1883 PAYLOAD_BYTES=256 \
  WARMUP=5000 COUNT=200000 LABEL=main cargo run --release -p bench_client
```

The peer accepts a fresh connection per client run, so you can leave it up and fire many client runs.

### `pub-latency`: closed-loop vs open-loop

`pub-latency` mode has two pacing strategies:

- **Closed-loop** (default, `TARGET_RATE=0`): each op waits for its own completion before the next
  is sent — one operation in flight at a time. Reports the client's intrinsic per-op cost with a hot
  socket; the achieved rate is whatever the round-trip allows. `INTERVAL_US` optionally adds a fixed
  sleep between ops. This is the stable, low-variance number for A/B regression comparison.
- **Open-loop** (`TARGET_RATE=<msgs/s>`): ops are issued on a fixed schedule (`intended = start +
  i/rate`) regardless of whether prior ops have completed, and each latency is measured **from its
  intended send time**, not from when it actually left. This is the coordinated-omission correction:
  if the client falls behind, the queueing delay shows up in the tail instead of being hidden. Use
  it to draw a **latency-vs-offered-rate curve** and find the knee where tails blow up.

```bash
# closed-loop (intrinsic per-op cost)
MODE=pub-latency QOS=1 COUNT=20000 ./bench-once.sh

# open-loop sweep (latency under load) — watch p99/p99.9 climb as rate approaches saturation
for r in 2000 20000 100000 160000; do
  TARGET_RATE=$r MODE=pub-latency QOS=1 COUNT=20000 ./bench-once.sh
done
```

> Open-loop pacing busy-spins the last few ms up to each `intended` instant (tokio's ~1ms timer is
> too coarse for us-scale latencies), so it burns a core on the pinned client — fine for a dedicated
> benchmark VM, and the cost is systematic so it cancels in same-rate A/B comparisons. **Pin one
> extra client core for open-loop** (e.g. `CLIENT_CORES=2,3,4`) so the spin gets its own core and
> doesn't steal from the measured client — otherwise the latency-vs-rate curve (and the apparent
> saturation knee) is pulled in below the client's true capacity. `bench-once.sh` warns if you run
> open-loop with fewer than 3 client cores.

## TLS

Generate a self-signed cert for local testing (used as **both** the peer's identity and the client's
trust anchor). `certs/` is gitignored.

```bash
./gen-test-certs.sh                 # writes certs/server.{crt,key}

# peer:
ROLE=feed TLS=1 PORT=8883 CERT_FILE=certs/server.crt KEY_FILE=certs/server.key \
  PAYLOAD_BYTES=256 cargo run --release -p bench_peer
# client:
MODE=recv-throughput TRANSPORT=tls PORT=8883 CA_FILE=certs/server.crt HOST=127.0.0.1 \
  PAYLOAD_BYTES=256 COUNT=200000 cargo run --release -p bench_client
```

> **TLS throughput caveat:** unlike plaintext, the peer must **encrypt live** (TLS records can't be
> pre-encoded or replayed), so it is no longer trivially faster than the client — single-session TLS
> throughput is bounded by `min(peer encrypt, client decrypt)`. For the crypto-cost signal, rely on
> the client's **CPU-per-message** (below), which stays un-confounded, and use `pub-latency` mode for
> TLS round-trip.

## Reading the output

Each client run prints a human summary plus one machine-readable line:

```
RESULT {"label":"main","mode":"recv-throughput","transport":"tcp","qos":0,"payload_bytes":256,
        "inflight":1,"interval_us":0,"count":200000,"wall_s":0.83,"msgs_per_s":240093.4,
        "mib_per_s":58.6,"lat_kind":"inter-arrival","lat_us":{"min":0.1,"p50":0.5,...}}
```

- **`msgs_per_s` / `mib_per_s`** — the headline for the `recv-throughput` and `pub-throughput` modes.
- **`lat_us`** percentiles — labeled by `lat_kind`: `op latency` (round-trip / per-op for the
  `pub-*` modes), `inter-arrival` (reader-path jitter for `recv-throughput` — *not* a round trip), or
  `delivery latency` (wire→app one-way for `recv-latency`).

The `bench-once.sh` primitive adds a CPU line from `/usr/bin/time` (per rep; `bench-workload.sh`
aggregates these across reps):

```
CPU {"user_s":0.10,"sys_s":0.30,"cpu_us_per_msg":80.0,"max_rss_kb":5808}
```

`cpu_us_per_msg` is measured on the client process alone, so it stays clean even under same-host
contention — it's the sharpest signal for crypto/copy-path regressions.

### Rendering the results (`report.py`)

[`report.py`](report.py) turns a results file into a human report: an **overview**, then per-config
**statistic tables** (median / mean / min / max / CV% for throughput, latency percentiles, and
`cpu_us_per_msg` / `max_rss_kb`), an **A/B comparison** when a config has ≥ 2 labels (delta% + a
`better` / `WORSE` / `~noise` verdict), and a **text histogram** of each distribution (summed from
the per-rep `hist_ns` HdrHistogram buckets — `[upper_bound_ns, count]` — so it reconstructs offline
without keeping raw samples).

```bash
python3 report.py results.jsonl                    # full report (all configs + histograms)
python3 report.py results.jsonl --config pub-lat-tcp   # one config
python3 report.py results.jsonl --label main       # one build
python3 report.py results.jsonl --no-hist          # tables only
python3 report.py results.jsonl --hist-only        # histograms only
```

The suite prints the tables (`--no-hist`, scoped to each config) inline as it runs; run `report.py`
by hand afterward for the full view including histograms.

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
regression.** The `verdict` column flags whether a delta exceeds the baseline's CV — a rough signal,
not a formal test. (`bench-workload.sh` prints the same for a single config inline; both render via
[`report.py`](report.py).)

> **⚠ The harness runs from each branch — it is *not* pinned.** `git checkout my-refactor` switches
> the whole tree, so build B runs **`my-refactor`'s copy of `iso_bench`**, not build A's. A valid A/B
> therefore **assumes the harness and workload definitions are identical on both branches** (same
> suite, `COUNT`s, payloads, pinning) — only the library under `src/` should differ. If the harness
> drifted between branches, the comparison is confounded. To keep this honest, every record is
> stamped with provenance (git SHA + dirty flag, `rustc`, host) and `report.py` **flags workload
> drift** (mismatched `count` / `payload_bytes` / `qos` / `inflight` / `target_rate` across labels)
> and **toolchain/host drift** — treat any such warning as "these numbers aren't comparable." A
> fixed, out-of-tree harness (one instrument built against each library ref) is the eventual fix once
> the API stabilizes; until then, keep `iso_bench` changes on a shared base and rebase feature
> branches onto it before benchmarking.

## How to run it for meaningful numbers

- **Warm the box first — this matters more than reps.** A cold/fresh VM (post-boot, or right after
  `apt`/`cargo build`) drifts several percent over the first minute as turbo ramps, caches fill, and
  background work settles — enough to fake a regression. Measured on an F8s_v2: p99 CV was **8–10%
  cold vs ~1% warm**, and two runs of the *same* build differed 6% (throughput) / 22% (p99) cold vs
  `~noise` warm. `bench.sh` auto-runs a throwaway warm-up block (`WARMUP_REPS`, default 8; set `0` to
  skip); if you drive `bench-workload.sh` directly, warm the box yourself first.
- **Establish the noise floor first:** run the *same* build twice under two labels; the delta you see
  is your detection threshold. Only trust A/B deltas larger than that band.
- **Size `COUNT` for the tail:** stable p99 needs ~10⁵ operations per run (p99.9 needs ~10× more).
- **`REPS=10` is plenty for p99 on a warm, pinned VM** (p99 CV ~1% → resolves ~1% deltas). Read
  **p99, throughput, and `cpu_us_per_msg`**; treat **p99.9 as directional and ignore `max`** (their
  CV is 15–100%+). Only bump `REPS` if the calibration shows a wide CV for a metric you care about.
- The minimum latency across reps is a useful low-noise "true cost" estimator (wall-clock noise only
  ever adds time).

## Environment variables

**`bench_client`** — connection: `HOST` `PORT` `TRANSPORT`(tcp|tls) `CLIENT_ID` `USERNAME`
`PASSWORD` `CA_FILE` `CERT_FILE` `KEY_FILE` `CONNECT_TIMEOUT_SECS` `KEEPALIVE_SECS`; workload:
`MODE`(pub-latency|pub-throughput|recv-throughput|recv-latency) `QOS`(0|1) `TOPIC` `PAYLOAD_BYTES`
`COUNT` `WARMUP` `INFLIGHT` `INTERVAL_US` `TARGET_RATE`(pub-latency; 0=closed-loop) `LABEL`.

**`bench_peer`** — `ROLE`(feed|sink) `BIND` `PORT` `TLS`(0|1) `CERT_FILE` `KEY_FILE` `TOPIC`
`PAYLOAD_BYTES` `QOS`(feed; 0|1) `WINDOW`(feed QoS 1 in-flight) `STAMP`(feed recv-latency) `BATCH`
`RATE`(feed; 0=max).

**`bench-once.sh`** — all of the client knobs, plus `CLIENT_CORES` `PEER_CORES` `NETEM_DELAY`
`CERT_DIR` `BATCH` `RATE`.

**`bench-workload.sh`** — all of the `bench-once.sh` knobs, plus `REPS` `LABEL` `RESULTS_FILE` `RESET`
`CONFIG`.

**`bench.sh`** — `REPS` `WARMUP_REPS` `LABEL` `RESULTS_FILE` `RESET` (+ pinning / `NETEM_DELAY` passed
through); the per-config workloads are a curated list inside the script.

Pass `--help` (or `HELP=1`) to either binary, or `-h` to any script, for the full list.

## Limitations & caveats

- **QoS 2 is not implemented** (the client doesn't implement it either).
- **The `recv-*` modes require `PAYLOAD_BYTES` to match** between peer (`feed`) and client, or the
  reported MiB/s is wrong.
- **`recv-latency` uses a shared wall clock.** The peer stamps each publish's send time and the
  client differences it at delivery, which is only valid on a *single host* (both read the same
  `SystemTime`) — which the whole tool already requires. Needs `PAYLOAD_BYTES >= 8`; the peer paces
  precisely (busy-spins a core, like open-loop) so absolute delivery latency isn't a send-pacing
  artifact.
- **Inbound QoS 1 needs flow control.** The client keeps each incoming QoS 1 packet id pending until
  it PUBACKs and *asserts (panics) if a still-pending id is reused*, so `feed QOS=1` caps in-flight to
  `WINDOW` and never reuses a live id (compliant-server behavior). The client's incoming-publish
  channel is **unbounded by design**, so a receiver slower than the network would grow memory without
  bound — the harness acks promptly (staying producer-bound), and `max_rss_kb` in the results
  surfaces any growth.
- **Loopback** exercises all of the client's transport software (syscalls, kernel TCP, TLS, framing,
  decode, buffer pool) but not real NIC offloads — which is correct for a *software* regression
  detector.
- **Closed-loop is the default** (each op waits for completion), which reports intrinsic per-op cost
  but under-reports tail latency under load (coordinated omission). Use **open-loop** `pub-latency`
  (`TARGET_RATE=<msgs/s>`, above) for coordinated-omission-correct tails and latency-under-load
  curves.
- **An open-loop knee can be the *peer*, not the client.** The `sink` peer parses every publish to
  echo its PUBACK and tops out around ~190k/s locally, so an open-loop latency knee near that rate
  may be the *harness* saturating rather than the client. Keep sweeps well below the peer's echo
  ceiling, give the peer enough cores (`PEER_CORES`), and trust the knee only in that regime.
- **Open-loop holds every un-acked op in flight** (~2 KB each). If the offered rate stays below
  client capacity the backlog is tiny, but if you deliberately overload, the backlog approaches
  `COUNT` \u2014 so a long overloaded run (`COUNT` \u2273 500k) can use ~`COUNT`\u00d72 KB of RAM. `bench-once.sh`
  notes this above `COUNT=500000`; keep overload probes short.
- TLS throughput is `min(peer, client)`-bound (see the TLS caveat above).

## Layout

```
iso_bench/                # detached cargo workspace (not part of the library's build)
  bench_client/           # the measured MQTT client harness
  bench_peer/             # the independent stand-in peer (TCP + TLS)
  bench.sh                # PRIMARY: full curated suite (all workloads) + per-config A/B
  bench-workload.sh       # one config: N reps + aggregate stats + A/B
  bench-once.sh           # single run (peer + pinned, timed client)
  report.py               # human report: overview + stat tables + A/B + histograms
  check-prereqs.sh        # detect + install build/run prerequisites (apt/dnf/yum)
  gen-test-certs.sh       # self-signed TLS cert generator (local testing only)
```
