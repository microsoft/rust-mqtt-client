#!/usr/bin/env bash
# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
#
# Single-run orchestration wrapper for the bench tooling (the primitive behind bench-workload.sh).
#
# Purpose: a SOFTWARE-REGRESSION detector, not a realistic real-world benchmark. It runs everything
# on ONE VM over loopback so the network is not a confound, pins the peer and client to disjoint
# cores, wraps the client in /usr/bin/time for CPU-per-message, captures the client's RESULT line,
# and tears the peer down. For statistically meaningful comparisons, prefer bench-workload.sh, which runs
# many reps of this and aggregates them.
#
# It starts a `bench_peer` in the role implied by MODE (inbound->feed, latency/throughput->sink),
# waits for it to listen, runs one `bench_client` config, then cleans up.
#
# Rigor notes (see the design discussion): run the SAME config twice on the same build first to
# learn the noise floor, then only trust deltas larger than that band. Read tails (p99/p99.9) and
# CPU-per-msg, not means. `NETEM_DELAY` adds a controlled loopback RTT (a reproducible test
# condition, not realism) and needs root.
#
# All configuration is via environment variables (same knobs as bench_client, plus orchestration):
#   Workload:  MODE(latency|throughput|inbound) QOS TRANSPORT(tcp|tls) PAYLOAD_BYTES COUNT WARMUP
#              INFLIGHT INTERVAL_US TARGET_RATE TOPIC LABEL HOST PORT
#   Peer:      BATCH RATE            (feed only)
#   Pinning:   CLIENT_CORES PEER_CORES   (taskset masks; defaults suit an 8-vCPU F8s_v2)
#   Extras:    NETEM_DELAY (e.g. 5ms, needs root)  CERT_DIR (TLS)
#
# Usage: ./bench-once.sh            (all via env)
#        MODE=inbound PAYLOAD_BYTES=256 COUNT=200000 ./bench-once.sh
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

# ---- config -------------------------------------------------------------------------------------
MODE="${MODE:-latency}"
QOS="${QOS:-1}"
TRANSPORT="${TRANSPORT:-tcp}"
PAYLOAD_BYTES="${PAYLOAD_BYTES:-64}"
COUNT="${COUNT:-50000}"
WARMUP="${WARMUP:-2000}"
INFLIGHT="${INFLIGHT:-32}"
INTERVAL_US="${INTERVAL_US:-0}"
TARGET_RATE="${TARGET_RATE:-0}"
TOPIC="${TOPIC:-bench/run}"
LABEL="${LABEL:-}"
HOST="${HOST:-127.0.0.1}"
PORT="${PORT:-}"
BATCH="${BATCH:-64}"
RATE="${RATE:-0}"

# Defaults assume >= 6 cores (e.g. F8s_v2): client and peer on separate physical cores, OS elsewhere.
# Override for your box; taskset will error on nonexistent cores.
CLIENT_CORES="${CLIENT_CORES:-2,3}"
PEER_CORES="${PEER_CORES:-4,5}"
NETEM_DELAY="${NETEM_DELAY:-}"
CERT_DIR="${CERT_DIR:-$script_dir/certs}"

# Default port by transport.
if [[ -z "$PORT" ]]; then
    [[ "$TRANSPORT" == "tls" ]] && PORT=8883 || PORT=1883
fi

# Peer role implied by the client mode.
case "$MODE" in
    inbound) PEER_ROLE=feed ;;
    latency | throughput) PEER_ROLE=sink ;;
    *)
        echo "unknown MODE '$MODE' (expected latency|throughput|inbound)" >&2
        exit 2
        ;;
esac

# Open-loop latency busy-spins one core to pace precisely; give it a dedicated core so it can't
# steal cycles from the measured client and distort the latency-vs-rate curve (need >=3: 2 for
# client work + 1 pacer).
if [[ "$MODE" == "latency" ]] && awk "BEGIN{exit !($TARGET_RATE > 0)}"; then
    client_core_count=0
    IFS=',' read -ra _cores <<<"$CLIENT_CORES"
    for _part in "${_cores[@]}"; do
        if [[ "$_part" == *-* ]]; then
            client_core_count=$((client_core_count + ${_part#*-} - ${_part%-*} + 1))
        else
            client_core_count=$((client_core_count + 1))
        fi
    done
    if ((client_core_count < 3)); then
        echo "warning: open-loop (TARGET_RATE=$TARGET_RATE) busy-spins a core to pace, but" >&2
        echo "         CLIENT_CORES='$CLIENT_CORES' pins only $client_core_count core(s). The spin" >&2
        echo "         will steal from the client and bias the latency-vs-rate curve; pin >=3, e.g." >&2
        echo "         CLIENT_CORES=2,3,4 (2 for client work + 1 for the pacer)." >&2
    fi
    # Open-loop keeps every un-acked op in flight (~2 KB each); if the offered rate exceeds the
    # client's capacity the backlog approaches COUNT, so a long overloaded run can eat a lot of RAM.
    if ((COUNT > 500000)); then
        echo "note: open-loop with COUNT=$COUNT can hold up to ~$((COUNT * 2 / 1024)) MB of in-flight" >&2
        echo "      backlog IF the offered rate exceeds client capacity. Keep COUNT modest when" >&2
        echo "      probing ABOVE capacity, or ensure TARGET_RATE is sustainable." >&2
    fi
fi

# ---- tooling ------------------------------------------------------------------------------------
command -v taskset >/dev/null || {
    echo "ERROR: taskset not found (install util-linux)" >&2
    exit 1
}
TIME_BIN=/usr/bin/time
if [[ ! -x "$TIME_BIN" ]]; then
    echo "warning: /usr/bin/time (GNU time) not found -- CPU-per-msg will be unavailable" >&2
    TIME_BIN=""
fi

target_dir="${CARGO_TARGET_DIR:-$script_dir/target}"
client_bin="$target_dir/release/bench_client"
peer_bin="$target_dir/release/bench_peer"

# ---- build (before measuring, so cargo isn't compiling during the run) --------------------------
echo "building release binaries..." >&2
cargo build --release -q -p bench_client -p bench_peer

# ---- TLS certs (server-auth only; peer serves, client trusts) -----------------------------------
peer_tls_env=()
client_tls_env=(TRANSPORT="$TRANSPORT")
if [[ "$TRANSPORT" == "tls" ]]; then
    if [[ ! -f "$CERT_DIR/server.crt" || ! -f "$CERT_DIR/server.key" ]]; then
        echo "generating TLS certs in $CERT_DIR..." >&2
        ./gen-test-certs.sh "$CERT_DIR" >/dev/null
    fi
    peer_tls_env=(TLS=1 CERT_FILE="$CERT_DIR/server.crt" KEY_FILE="$CERT_DIR/server.key")
    client_tls_env+=(CA_FILE="$CERT_DIR/server.crt")
fi

# ---- optional controlled loopback RTT (needs root) ----------------------------------------------
netem_applied=0
if [[ -n "$NETEM_DELAY" ]]; then
    if tc qdisc add dev lo root netem delay "$NETEM_DELAY" 2>/dev/null; then
        netem_applied=1
        echo "applied netem delay $NETEM_DELAY on lo" >&2
    else
        echo "warning: could not apply netem (need root/CAP_NET_ADMIN); continuing without" >&2
    fi
fi

peer_log="$(mktemp)"
time_out="$(mktemp)"
result_out="$(mktemp)"
cleanup() {
    [[ -n "${peer_pid:-}" ]] && kill "$peer_pid" 2>/dev/null || true
    [[ "$netem_applied" == "1" ]] && tc qdisc del dev lo root 2>/dev/null || true
    rm -f "$peer_log" "$time_out" "$result_out" 2>/dev/null || true
}
trap cleanup EXIT

# ---- start peer (pinned) ------------------------------------------------------------------------
echo "starting bench_peer[$PEER_ROLE] on cores $PEER_CORES, ${TRANSPORT}://$HOST:$PORT ..." >&2
env ROLE="$PEER_ROLE" BIND="$HOST" PORT="$PORT" PAYLOAD_BYTES="$PAYLOAD_BYTES" TOPIC="$TOPIC" \
    BATCH="$BATCH" RATE="$RATE" "${peer_tls_env[@]}" \
    taskset -c "$PEER_CORES" "$peer_bin" >"$peer_log" 2>&1 &
peer_pid=$!

# Wait for the peer to report it is listening.
for _ in $(seq 1 200); do
    grep -q "listening on" "$peer_log" 2>/dev/null && break
    kill -0 "$peer_pid" 2>/dev/null || {
        echo "ERROR: peer exited during startup:" >&2
        cat "$peer_log" >&2
        exit 1
    }
    sleep 0.05
done

# ---- run client (pinned, timed) -----------------------------------------------------------------
echo "running bench_client[$MODE] on cores $CLIENT_CORES (COUNT=$COUNT, PAYLOAD_BYTES=$PAYLOAD_BYTES)..." >&2
client_env=(
    MODE="$MODE" QOS="$QOS" HOST="$HOST" PORT="$PORT" PAYLOAD_BYTES="$PAYLOAD_BYTES"
    COUNT="$COUNT" WARMUP="$WARMUP" INFLIGHT="$INFLIGHT" INTERVAL_US="$INTERVAL_US" TOPIC="$TOPIC"
    TARGET_RATE="$TARGET_RATE"
    "${client_tls_env[@]}"
)
[[ -n "$LABEL" ]] && client_env+=(LABEL="$LABEL")

if [[ -n "$TIME_BIN" ]]; then
    env "${client_env[@]}" "$TIME_BIN" -v -o "$time_out" \
        taskset -c "$CLIENT_CORES" "$client_bin" | tee "$result_out"
else
    env "${client_env[@]}" taskset -c "$CLIENT_CORES" "$client_bin" | tee "$result_out"
fi

# ---- CPU-per-message ----------------------------------------------------------------------------
if [[ -n "$TIME_BIN" && -s "$time_out" ]]; then
    user_s=$(awk -F': ' '/User time/{print $2}' "$time_out")
    sys_s=$(awk -F': ' '/System time/{print $2}' "$time_out")
    rss_kb=$(awk -F': ' '/Maximum resident set size/{print $2}' "$time_out")
    cpu_us_per_msg=$(awk -v u="${user_s:-0}" -v s="${sys_s:-0}" -v c="$COUNT" \
        'BEGIN{ if (c>0) printf "%.3f", (u+s)/c*1e6; else print "0" }')
    printf 'CPU {"user_s":%s,"sys_s":%s,"cpu_us_per_msg":%s,"max_rss_kb":%s}\n' \
        "${user_s:-0}" "${sys_s:-0}" "$cpu_us_per_msg" "${rss_kb:-0}"
fi

echo "done." >&2
