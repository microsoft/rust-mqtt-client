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
LABEL="${LABEL:-$(git rev-parse --short HEAD 2>/dev/null || echo run)}"
RESULTS_FILE="${RESULTS_FILE:-$script_dir/results.jsonl}"
RESET="${RESET:-0}"

# Curated suite. Each entry is a config name plus the bench-workload.sh env for that config. It spans
# the distinct client code paths, not a full cross-product: three modes x {tcp,tls} at the primary
# payload, plus variants that isolate one path each. Several are TCP-only because they isolate client
# LOGIC and the crypto path is already covered by the *-tls configs: small-payload throughput on both
# send and receive (per-message overhead vs the 16 KiB per-byte regime), QoS 0 send (no pkid/ack
# machinery), and large-payload latency (big-message round-trip). QoS 1 inbound (recv-tput-q1-*) is BOTH tcp and tls: the
# receive-side PUBACK path (peer feeds QoS 1 with a flow-control window, client acks each) genuinely
# differs by transport -- TLS must encrypt every tiny PUBACK, a high-rate crypto load not covered
# elsewhere. recv-lat-* measure per-message DELIVERY latency (wire->app): the peer stamps each
# publish's send time (paced precisely) and the client records now-stamp at delivery -- this catches a
# uniform delivery delay that inter-arrival (a derivative) is blind to. The pub-lat-open-* configs measure
# coordinated-omission-correct latency UNDER LOAD at a fixed offered rate (~40% of the ~200k/132k
# tcp/tls capacity), which catches tail regressions the closed-loop lat-* configs (1 op in flight)
# can't see; they pin one EXTRA client core (2,3,4 / peer 5,6) because the open-loop pacer busy-spins
# a core -- see the open-loop notes in README. Payloads: 64 B small (per-op), 16 KiB large (per-byte).
# COUNTs: latency ~1e5 (stable p99); throughput/inbound ~3e5 (>=~1 s steady window). Edit this list to
# change what the gate covers.
#
# No QoS 0 latency config on purpose: the client's QoS 0 completion token fires at queue admission,
# BEFORE encode + socket write (session.rs completes the notifier as it dequeues), so it would time
# scheduling/admission, not send cost -- pub-tput-qos0 already covers the QoS 0 send path. If that token
# is ever changed to fire after the write, a QoS 0 latency config becomes worthwhile.
#
# recv-latency is QoS 0 only: a QoS 1 recv-latency is confounded because the harness must PUBACK each
# message, and the client sends PUBACKs through one connection task + a capacity-1 channel -- that
# serial path is the bottleneck. Serial acking blocks the receive loop (latency tail balloons, ~2.8ms
# p99); parallel acking (spawn accept() per msg) measured 3-6x WORSE throughput because it floods the
# runtime and starves that connection task. So QoS 1 delivery latency can't be measured cleanly from
# the harness -- it needs the client's ack path restructured. TODO: revisit if/when that changes.
suite=(
    "CONFIG=pub-lat-tcp      MODE=pub-latency    QOS=1 TRANSPORT=tcp PAYLOAD_BYTES=64    COUNT=100000"
    "CONFIG=pub-lat-tls      MODE=pub-latency    QOS=1 TRANSPORT=tls PAYLOAD_BYTES=64    COUNT=100000"
    "CONFIG=pub-lat-large    MODE=pub-latency    QOS=1 TRANSPORT=tcp PAYLOAD_BYTES=16384 COUNT=100000"
    "CONFIG=pub-lat-open-tcp MODE=pub-latency    QOS=1 TRANSPORT=tcp PAYLOAD_BYTES=64 TARGET_RATE=80000 COUNT=100000 CLIENT_CORES=2,3,4 PEER_CORES=5,6"
    "CONFIG=pub-lat-open-tls MODE=pub-latency    QOS=1 TRANSPORT=tls PAYLOAD_BYTES=64 TARGET_RATE=50000 COUNT=100000 CLIENT_CORES=2,3,4 PEER_CORES=5,6"
    "CONFIG=pub-tput-tcp     MODE=pub-throughput QOS=1 TRANSPORT=tcp PAYLOAD_BYTES=16384 INFLIGHT=64 COUNT=300000"
    "CONFIG=pub-tput-tls     MODE=pub-throughput QOS=1 TRANSPORT=tls PAYLOAD_BYTES=16384 INFLIGHT=64 COUNT=300000"
    "CONFIG=pub-tput-small   MODE=pub-throughput QOS=1 TRANSPORT=tcp PAYLOAD_BYTES=64    INFLIGHT=64 COUNT=300000"
    "CONFIG=pub-tput-qos0    MODE=pub-throughput QOS=0 TRANSPORT=tcp PAYLOAD_BYTES=64    INFLIGHT=64 COUNT=300000"
    "CONFIG=recv-tput-tcp    MODE=recv-throughput QOS=0 TRANSPORT=tcp PAYLOAD_BYTES=16384 COUNT=300000"
    "CONFIG=recv-tput-tls    MODE=recv-throughput QOS=0 TRANSPORT=tls PAYLOAD_BYTES=16384 COUNT=300000"
    "CONFIG=recv-tput-small  MODE=recv-throughput QOS=0 TRANSPORT=tcp PAYLOAD_BYTES=64    COUNT=300000"
    "CONFIG=recv-tput-q1-tcp MODE=recv-throughput QOS=1 TRANSPORT=tcp PAYLOAD_BYTES=16384 COUNT=300000"
    "CONFIG=recv-tput-q1-tls MODE=recv-throughput QOS=1 TRANSPORT=tls PAYLOAD_BYTES=16384 COUNT=300000"
    "CONFIG=recv-lat-tcp     MODE=recv-latency   QOS=0 TRANSPORT=tcp PAYLOAD_BYTES=256 RATE=50000 BATCH=1 COUNT=100000"
    "CONFIG=recv-lat-tls     MODE=recv-latency   QOS=0 TRANSPORT=tls PAYLOAD_BYTES=256 RATE=50000 BATCH=1 COUNT=100000"
)

[[ "$RESET" == "1" ]] && : >"$RESULTS_FILE"

echo "== iso_bench SUITE: label='$LABEL' reps=$REPS configs=${#suite[@]} -> $RESULTS_FILE ==" >&2

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
