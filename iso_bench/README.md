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

> **Before gating CI on this, read [SENSITIVITY.md](SENSITIVITY.md).** Measured over 236 runs: the
> suite reliably catches regressions at ~1% and is blind below ~0.5% — but *any* source edit, even
> dead code in a function that never runs, shifts inlining and code layout enough to cost a real
> 0.5–1%. So a comparison of two builds that "should not" differ in performance will often flag
> something, and the flag is not a measurement error.

## Simple usage

Install the prerequisites once, then run the suite on one build — or compare two builds for
regressions (the tool's main job). Recommended box: **Azure `F16s_v2`** (see [How it works](#how-it-works)).

### Prerequisites

On a fresh VM, run [`install-prereqs.sh`](install-prereqs.sh) to detect and install everything below
(apt/dnf/yum; `--check` reports without installing):

```bash
./install-prereqs.sh          # install what's missing
./install-prereqs.sh --check  # report only (exit 1 if anything required is missing)
```

- A Rust toolchain (the repo pins one via `rust-toolchain.toml`).
- `curl` — to fetch the Rust toolchain installer (rustup) on a fresh machine.
- A C compiler + `pkg-config` + libssl headers (`libssl-dev` / `openssl-devel`) — to build the
  `openssl` crate.
- `taskset` (from `util-linux`) — for core pinning.
- GNU `time` at `/usr/bin/time` — for CPU-per-message (optional; the wrapper degrades gracefully).
- `openssl` CLI — only for generating TLS test certs.
- `tc` (from `iproute2`) and root — only if you use `NETEM_DELAY` to inject a controlled RTT.

### Run the suite

Build, run the full curated suite, and print a per-config summary:

```bash
LABEL=main ./bench.sh             # build + run the full suite (prints tables as it goes)
python3 report.py results.jsonl   # optional: full report with histograms
```

`bench.sh` prints a per-config table (throughput, p50/p90/p99, cpu-per-msg) as it runs.

> **⚠ Suite runs cannot be compared historically without an anchor.**
> First-class support for built-in anchoring will be provided in the future.
> **If you need to compare historical builds use A/B testing below**


### Comparing builds (A/B)

Compare two builds with [`bench-compare.sh`](bench-compare.sh). It runs them **interleaved** —
alternating the two builds rep-by-rep against one shared peer — so environmental drift (turbo,
thermal, noisy neighbours) cancels out and only real differences remain; `report.py` prints a
per-config **paired verdict**. (The full rationale, the sequential alternative, and caveats are in
[A/B details](#ab-details) under Advanced usage.)

Build each ref into its own target (git worktrees keep them independent), then run:

```bash
git worktree add ../iso-main main
( cd ../iso-main/iso_bench && CARGO_TARGET_DIR=/tmp/t-main cargo build --release -p bench_client )
CARGO_TARGET_DIR=/tmp/t-cur cargo build --release -p bench_client

CUR_BIN=/tmp/t-cur/release/bench_client  REF_BIN=/tmp/t-main/release/bench_client \
  CUR_LABEL=branch REF_LABEL=main ./bench-compare.sh
```

**Prefer [`bench-multibuild.sh`](bench-multibuild.sh) for a real gate.** It does the above, except it
builds each side *several times* with different code layouts and spreads the reps across them:

```bash
git worktree add ../iso-main main
REF_SRC=../iso-main CUR_SRC=.. REF_LABEL=main CUR_LABEL=branch ./bench-multibuild.sh
```

Building each side once makes code layout a **constant inside the measurement** — the same two
binaries in every rep and round, so replication cannot average it away. Any source edit shifts
function placement and inlining, and those shifts cost the same order as the regressions worth
catching. Measured here: a one-line diff whose work runs once per connection against 50,000 measured
messages — so it cannot cost anything real — flagged **6 of 6** single-build runs but only **1 of 4**
multibuild runs, while a genuine ~1.5% regression was caught **just as well either way** — 32–33 gated
cells under multibuild against 32–35 single-build at the same rep count, and near-identical raw effect
sizes (median |delta| 3.7% vs 3.8%). Multibuild buys artefact suppression at no cost in detection; it
does not improve detection. Costs ~15 s per extra build against a suite that runs for over an hour;
the measured work is unchanged. `BUILDS=1` reverts to the single-build behaviour above.

`bench-compare.sh` prints the paired comparison as it finishes; re-render it any time (add
histograms):

```bash
python3 report.py results.jsonl --baseline main
```

See [Reading the report](#reading-the-report) for what each column means.

### Reading the report

Both `bench.sh` and `bench-compare.sh` print a per-config table (and `report.py` re-renders the same
data, plus histograms). Each row is one **metric**, summarised across the config's reps. Each config
measures one kind of latency — the table header says which: *op latency* for closed-loop publish,
*delivery latency* for `recv-latency`, and *inter-arrival* gap for `recv-throughput`.

- **throughput** — messages per second, and the same rate expressed as **MiB/s** at the payload size.
  Higher is better. Paced configs hold this fixed at the offered rate.
- **p50 / p90 / p99** — latency percentiles from the per-message HdrHistogram: p50 is the typical
  case, p90 the early tail, p99 the deep tail (where regressions usually surface first). Lower is better.
- **max** — the single worst sample in the run. Useful context but very noisy (one OS hiccup sets it),
  so it's shown as `info` and never gated; trust p99 for the tail.
- **cpu/msg** — CPU microseconds per message (client process only, user + sys). An efficiency figure
  independent of wall-clock: it catches extra work (copies, allocs, syscalls) even when latency hides
  it, and it's the cleanest cross-config signal. Lower is better.
- **max rss** — peak resident memory (physical pages) the process held; a leak/bloat detector. Lower
  is better. Page-quantized and low-variance, so it needs a larger move before it flags.

In the **single-build** tables (`bench.sh`) the columns are **median / mean / min / max** across reps
and **CV%** (coefficient of variation = run-to-run noise; a low CV means a stable metric).

In each config's **paired A/B** table (comparing ≥ 2 builds) the verdict is decided by **replication**:
`bench-compare.sh` runs the whole suite **twice** (2 rounds, a full suite apart), and the significance
gate (Wilcoxon `p < 0.05` and `|Δ|` over the floor) runs *per round*. The `rnd1 p` / `rnd2 p` columns
show each round's p with an arrow for direction; **`raw Δ%`** is the pooled per-pair median and
**`adj Δ%`** the noise-corrected effect. The **`verdict`**:

- **`better` / `WORSE`** — the metric fires the **same direction in every round** (reproduced). This is
  the only hard signal — an effect that shows up independently in both passes.
- **`~noise*`** — fired in **some but not all** rounds. Seen once, didn't replicate → treated as noise;
  the `*` flags it for the curious. `raw Δ%` still shows the size; `adj Δ%` is `0.0`.
- **`~noise`** — didn't fire, or fired in **opposite** directions across rounds (contradicted).
- **`info`** — a metric that isn't gated (shown for context only).

> Tip: a real regression usually moves **throughput and `cpu/msg` together** — read them as a pair.
> (A single-pass run — e.g. `bench.sh` — falls back to "fires → verdict" with no replication check.)

## Advanced usage
*The canonical use of this tool is the simple usage above. It is not recommended to use these more granular pieces of the tooling directly.*

### A/B details

**Why interleaved.** On a cloud VM you can't pin turbo, so running the *whole* suite for build A and
*then* the whole suite for build B lets slow drift (turbo, thermal, neighbours) between the two blocks
masquerade as a regression. `bench-compare.sh` alternates the two builds **rep-by-rep in randomized
order** against one shared peer, so the drift is common to each adjacent pair and **cancels in the
per-pair delta**. `report.py` then uses a **paired** test (Wilcoxon signed-rank on the per-pair
deltas) whose threshold self-calibrates per config — far tighter than the CV band.

**Why replicated.** The paired delta cancels drift but not *arm-luck* — the fast per-pair scheduling
variance that, over one config's pairs, can push a correlated group of metrics off zero by chance. So
`bench-compare.sh` runs the whole suite **twice** (`ROUNDS`, default 2), a full suite apart, and a
verdict requires the effect to **reproduce in both rounds**. Arm-luck is independent between rounds, so
it rarely agrees twice; a real regression reproduces. This is what replaced the earlier cross-metric
"coherence" gate (`config_factors` is retained for a possible future sibling/family grouping).

**Different library APIs compare fine.** Each binary carries its own copy of the harness compiled
against its own library API, so you only adapt `bench_client`'s call sites on the changed branch; the
env knobs and `RESULT` schema (the external contract) must stay identical. The reference can also be
a **frozen anchor** binary: record the current-vs-anchor ratio each session and those ratios are
comparable across time (drift already cancelled) — the only trustworthy way to do historical
comparison on a box you can't pin.

**Sequential (simpler, but drift-confounded).** Run the suite on each git ref, tagged with `LABEL`;
results accumulate in `results.jsonl` and the second run prints a per-config comparison (flagged
against the baseline's run-to-run CV). Fine for a quick look on a quiet, warm box; prefer
`bench-compare.sh` for a real gate.

```bash
RESET=1 LABEL=main     ./bench.sh   # build A (fresh results file)
git checkout my-refactor
        LABEL=refactor ./bench.sh   # build B -> per-config comparison
```

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

### The scripts

[`bench.sh`](bench.sh) is the main entry point — it runs the **full curated suite** (the four modes
`pub-latency` / `pub-throughput` / `recv-throughput` / `recv-latency` over TCP and TLS, plus variants
that isolate one path each — QoS 0 send, small-payload throughput, large-payload latency, QoS 1
receive+PUBACK — and two coordinated-omission-correct **open-loop** configs), `REPS` reps per config,
printing a per-config summary plus (for A/B) a per-config comparison. Under it sit two lower-level
entry points you can use directly:

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
`PEER_CORES` (defaults `2,4` and `8,10` put one worker per physical core on an F16s_v2); add
`NETEM_DELAY=5ms` for a controlled loopback RTT (needs root).

### Manual usage (two terminals)

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
> extra client core for open-loop** (e.g. `CLIENT_CORES=2,4,6`) so the spin gets its own core and
> doesn't steal from the measured client — otherwise the latency-vs-rate curve (and the apparent
> saturation knee) is pulled in below the client's true capacity. `bench-once.sh` warns if you run
> open-loop with fewer than 3 client cores.

### TLS

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

### Reading the output

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

#### Rendering the results (`report.py`)

[`report.py`](report.py) turns a results file into a human report: an **overview**, then per-config
**statistic tables** (median / mean / min / max / CV% for throughput, latency percentiles, and
`cpu_us_per_msg` / `max_rss_kb`), an **A/B comparison** when a config has ≥ 2 labels (`raw Δ%` +
per-round `p` + noise-corrected `adj Δ%` + a replication-gated `better` / `WORSE` / `~noise*` / `~noise`
verdict), and a **text histogram** of each distribution (summed from the per-rep `hist_ns` HdrHistogram
buckets — `[upper_bound_ns, count]` — so it reconstructs offline without keeping raw samples).

A `better` / `WORSE` verdict requires the flagged metric to **reproduce** — fire the same direction in
every replication round (`bench-compare.sh` runs the suite twice by default). A metric that fires in
some-but-not-all rounds is demoted to `~noise*` — see the [Reading the report](#reading-the-report)
verdict list for the full scale.

```bash
python3 report.py results.jsonl                    # full report (all configs + histograms)
python3 report.py results.jsonl --config pub-lat-tcp   # one config
python3 report.py results.jsonl --label main       # one build
python3 report.py results.jsonl --baseline main    # force the A/B baseline label
python3 report.py results.jsonl --no-hist          # tables only
python3 report.py results.jsonl --hist-only        # histograms only
```

The suite prints the tables (`--no-hist`, scoped to each config) inline as it runs; run `report.py`
by hand afterward for the full view including histograms.

### Environment variables

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
through); the per-config workloads are a curated list in `suite.sh`.

**`bench-compare.sh`** — `CUR_BIN` `REF_BIN` (required, prebuilt binaries) `CUR_LABEL` `REF_LABEL`
`CUR_SHA` `REF_SHA` `PEER_BIN` `REPS` `ROUNDS` (replication rounds — whole suite run this many times,
default 2; a verdict must reproduce across them) `WARMUP_REPS` `RESULTS_FILE` `RESET`; interleaves the
two binaries and renders a paired A/B (`report.py --baseline $REF_LABEL`).

`CUR_BINS` / `REF_BINS` (colon-separated) accept SEVERAL binaries per arm, each compiled from the
same source with a different code layout; reps are then spread across them. Building each arm once
makes layout a fixed constant inside the point estimate that no amount of replication removes — a
one-line diff that executes once per connection was measured flagging 6 of 6 single-build runs and
1 of 4 multibuild runs, entirely through inlining and layout shifts. Randomising layout is the
standard remedy (Mytkowicz et al., ASPLOS 2009; Curtsinger & Berger, *Stabilizer*, ASPLOS 2013).
You must build the variants yourself — e.g. repeat the `cargo build` above with
`RUSTFLAGS="-C llvm-args=-align-all-functions=5"`, `=6`, and
`-C llvm-args=-align-all-nofallthru-blocks=4` into separate `CARGO_TARGET_DIR`s — and pass the paths
joined by `:`. Both lists must have the same length.

Pass `--help` (or `HELP=1`) to either binary, or `-h` to any script, for the full list.

## How it works

*Background — you don't need any of this just to run the benchmark.*

### The isolation trick — a stand-in peer

To measure the client in isolation we do **not** use a real broker (a broker's own latency and
throughput would confound the result). Instead the client under test talks to a trivial, controlled
peer that stands in for "the other end of the connection":

| Component | What it is |
|---|---|
| **`bench_client`** | The instrumented MQTT client (uses `ms-mqtt-client`). Runs a workload and prints a machine-readable `RESULT` line. |
| **`bench_peer`** | An independent MQTT 5 peer that hand-rolls the minimal wire bytes and does **not** depend on `ms-mqtt-client`, so its behavior is invariant across client builds. It either firehoses messages at the client or drains/acks the client's messages. |

Because the peer is trivial and build-invariant, any difference between two runs is attributable to
the **client**.

### Recommended target & core pinning

- **Azure VM: `Standard_F16s_v2`** (16 vCPU, compute-optimized, **non-burstable**). It is
  hyperthreaded, so 16 vCPUs = **8 physical cores** (2 sibling threads each — check with `lscpu -e`).
  Avoid **B-series** (burstable — credit throttling ruins measurements).
- **A single VM, loopback only.** Keeping both processes on one host removes the network as a
  variable and holds the hardware constant across A/B runs — exactly what a regression detector
  wants. Containers (bridge networking adds a confound) and multi-VM (network becomes a variable)
  are intentionally avoided.
- **Core budget (why 8 physical cores):** the client wants ~2 cores (its connection reader and
  workload consumer run concurrently), the peer ~1–2 (more under TLS, which encrypts live), and the
  OS needs headroom. The point of 8 physical cores is to give the client and peer their **own
  physical cores with the hyperthread siblings left idle** (no shared execution units) and leave
  spare cores for the OS/IRQs to drift onto. Defaults pin the client to `2,4` (physical cores 1,2)
  and the peer to `8,10` (cores 4,5). **Open-loop `pub-latency` wants one *extra* client core** for
  the busy-spin pacer (`CLIENT_CORES=2,4,6 PEER_CORES=8,10`). An F8s_v2 (4 physical cores) also runs
  but forces the client and peer onto hyperthread siblings — a measurable noise source.
- The tooling runs anywhere Linux + Rust is available; the VM above is only the *recommended* target
  for trustworthy numbers.

> **Azure guest caveat:** you generally cannot set the CPU governor / turbo / C-states from inside
> the guest, so some turbo wander is unavoidable. Determinism therefore comes from **physical-core
> pinning + interleaved A/B (`bench-compare.sh`) + reading the right metrics**, not from power
> settings.

> **Do NOT hard-isolate the bench cores (`isolcpus` / `nosmt` / `rcu_nocbs`).** It was tried and
> measured: on a same-build noise run it *collapsed* the throughput and open-loop configs
> (throughput −28% to −56%; `pub-lat-open-tcp` could no longer sustain its target rate, p50 latency
> went from ~63 µs to ~1.9 s, and RSS ballooned to ~267 MB from send-queue backlog). Cause:
> `isolcpus` walls the pinned cores off from the scheduler so **network softirq/IRQ processing can
> no longer run on them**, and `nosmt` halves logical CPUs — together they starve the throughput
> path of the aggregate CPU it needs. The *only* thing that improved was single-thread latency
> (cpu/msg −23%), purely from turbo headroom with fewer active cores — not worth breaking half the
> matrix. Since `%steal` is already ~0 on a dedicated F16 (host floor is clean), the payoff is nil.
> Keep hyperthreading **on**, no `isolcpus`; rely on pinning + interleaved A/B. At most, quiet the
> box by hand (THP → `never`, stop background timers/`unattended-upgrades`) — but even that showed no
> clear tail win here, so the stock VM is the recommended configuration.

### Getting trustworthy numbers

- **Warm the box first — this matters more than reps.** A cold/fresh VM (post-boot, or right after
  `apt`/`cargo build`) drifts several percent over the first minute as turbo ramps, caches fill, and
  background work settles — enough to fake a regression. Measured on an Azure F-series VM: p99 CV was **8–10%
  cold vs ~1% warm**, and two runs of the *same* build differed 6% (throughput) / 22% (p99) cold vs
  `~noise` warm. `bench.sh` auto-runs a throwaway warm-up block (`WARMUP_REPS`, default 8; set `0` to
  skip); if you drive `bench-workload.sh` directly, warm the box yourself first.
- **Establish the noise floor first:** run the *same* build twice under two labels; the delta you see
  is your detection threshold. Only trust A/B deltas larger than that band.
- **Size `COUNT` for the tail:** stable p99 needs ~10⁵–5×10⁵ ops per run (the paced open-loop and
  recv-latency configs run 5×10⁵ for a solid p99). p99.9 needs ~10⁶ — more than the suite runs, so it
  is **not reported**; the raw distribution is still in the histogram if you ever need it.
- **`REPS` defaults to 14** in `bench-compare.sh`. 10 resolves ~1% deltas on a warm, pinned VM
  (p99 CV ~1%) and is still plenty for a *single-build* comparison; 14 is for the multibuild path
  below, where reps are spread across several code layouts and a between-build variance component
  (measured at 21–30% of total) has to be averaged out too. Set `REPS=10` if you are comparing two
  single binaries. Read **p99, throughput, and `cpu_us_per_msg`**; **ignore `max`** (single-sample,
  CV up to ~100%).
- The minimum latency across reps is a useful low-noise "true cost" estimator (wall-clock noise only
  ever adds time).

### Limitations & caveats

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

### Layout

```
iso_bench/                # detached cargo workspace (not part of the library's build)
  bench_client/           # the measured MQTT client harness
  bench_peer/             # the independent stand-in peer (TCP + TLS)
  bench.sh                # PRIMARY: full curated suite (all workloads), single-label runs
  bench-compare.sh        # interleaved head-to-head A/B of two prebuilt binaries (paired stats)
  bench-workload.sh       # one config: N reps + aggregate stats + A/B
  bench-once.sh           # single run (peer + pinned, timed client)
  suite.sh                # shared curated config list (sourced by bench.sh + bench-compare.sh)
  record.py               # shared JSONL record writer (one place for the schema)
  report.py               # human report: overview + stat tables + A/B (paired) + histograms
  test_report.py          # requirement-based tests for report.py (python3 -m unittest test_report.py)
  install-prereqs.sh      # detect + install build/run prerequisites (apt/dnf/yum)
  gen-test-certs.sh       # self-signed TLS cert generator (local testing only)
```
