#!/usr/bin/env bash
# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
#
# Runs the FULL curated benchmark suite -- the main entry point for regression detection.
#
# It runs a hand-picked set of representative configs (the four modes pub-latency / pub-throughput /
# recv-throughput / recv-latency, each over TCP and TLS), REPS reps each, via bench-workload.sh --
# each config prints its OWN summary + A/B as it finishes. The suite is CURATED (not a full
# cross-product) to keep the runtime bounded; each config is sized per workload (latency configs for
# tail stability, throughput for a steady window) rather than a single shared COUNT.
#
# A/B workflow (build each git ref, then compare):
#   RESET=1 LABEL=main     ./bench.sh
#   git checkout my-refactor
#           LABEL=refactor ./bench.sh   # prints the per-config comparison
#
# Env:
#   REPS          reps per config                   (default 10)
#   WARMUP_REPS   throwaway warm-up runs before the suite (default 8; 0 = skip if box already warm)
#   LABEL         tag for this build                (default: git short SHA)
#   RESULTS_FILE  JSONL accumulator                 (default: ./results.jsonl)
#   RESET         1 = truncate RESULTS_FILE first   (default 0; use on the first build of an A/B)
#   CLIENT_CORES / PEER_CORES / NETEM_DELAY / CERT_DIR   (passed through to bench-once.sh)
#
# For a single ad-hoc config use bench-workload.sh; for one un-aggregated run use bench-once.sh.
set -euo pipefail

self="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/$(basename "${BASH_SOURCE[0]}")"
script_dir="$(dirname "$self")"

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
    while IFS= read -r line; do
        [[ "$line" == '#'* ]] || break
        line="${line#\#}"
        printf '%s\n' "${line# }"
    done < <(tail -n +4 "$self")
    exit 0
fi

cd "$script_dir"

REPS="${REPS:-10}"
WARMUP_REPS="${WARMUP_REPS:-8}"
LABEL="${LABEL:-$(git rev-parse --short HEAD 2>/dev/null || echo run)}"
RESULTS_FILE="${RESULTS_FILE:-$script_dir/results.jsonl}"
RESET="${RESET:-0}"

# Curated suite lives in suite.sh (shared with bench-compare.sh so the gate and the interleaved A/B
# drive the exact same configs). Defines the bash array `suite`.
# shellcheck source=suite.sh
source "$script_dir/suite.sh"

[[ "$RESET" == "1" ]] && : >"$RESULTS_FILE"

# Fail loud if the toolchain is missing/unsourced -- otherwise the warm-up swallows the build error
# and the first config aborts silently on a fresh box (rustup adds ~/.cargo/bin only to new shells).
command -v cargo >/dev/null || {
    echo "ERROR: cargo not on PATH. Run ./install-prereqs.sh, then: source ~/.cargo/env" >&2
    exit 1
}

echo "== iso_bench SUITE: label='$LABEL' reps=$REPS configs=${#suite[@]} -> $RESULTS_FILE ==" >&2

# Warm the box before measuring: a cold/fresh VM drifts several % over the first minutes as the CPU
# ramps to turbo and background work settles, which reads as false A/B deltas LARGER than real
# regressions. IMPORTANT: warm up with a CPU-SATURATING throughput-TLS load, not latency -- latency
# is RTT-bound (CPU idle) so it never ramps turbo, leaving CPU-sensitive configs (TLS crypto, large
# copy) biased low when measured first. Throughput-TLS ramps every core and warms the crypto path.
# Not recorded. Set WARMUP_REPS=0 to skip when the box is already warm.
if ((WARMUP_REPS > 0)); then
    echo "== warm-up: $WARMUP_REPS discarded CPU-saturating runs (skip with WARMUP_REPS=0) ==" >&2
    for ((w = 1; w <= WARMUP_REPS; w++)); do
        printf '   warm-up %d/%d\r' "$w" "$WARMUP_REPS" >&2
        # Output fully discarded -- no summary/tables, nothing recorded to results.
        env MODE=pub-throughput QOS=1 TRANSPORT=tls PAYLOAD_BYTES=16384 INFLIGHT=64 COUNT=300000 \
            ./bench-once.sh >/dev/null 2>&1 || true
    done
    echo "   warm-up done          " >&2
fi

i=0
for cfg in "${suite[@]}"; do
    i=$((i + 1))
    echo "" >&2
    echo ">>> config [$i/${#suite[@]}]: $cfg" >&2
    # Word-splitting of $cfg into KEY=VAL args is intentional.
    # shellcheck disable=SC2086
    env $cfg RESET=0 REPS="$REPS" LABEL="$LABEL" RESULTS_FILE="$RESULTS_FILE" \
        ./bench-workload.sh
done

echo "" >&2
echo "== suite done: label='$LABEL' -> $RESULTS_FILE ==" >&2
echo "   (full human report incl. histograms: python3 report.py $RESULTS_FILE)" >&2
