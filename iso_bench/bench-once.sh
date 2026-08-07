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
# It starts a `bench_peer` in the role implied by MODE (recv-*->feed, pub-*->sink),
# waits for it to listen, runs one `bench_client` config, then cleans up.
#
# Rigor notes (see the design discussion): run the SAME config twice on the same build first to
# learn the noise floor, then only trust deltas larger than that band. Read tails (p99/p99.9) and
# CPU-per-msg, not means. `NETEM_DELAY` adds a controlled loopback RTT (a reproducible test
# condition, not realism) and needs root.
#
# All configuration is via environment variables (same knobs as bench_client, plus orchestration):
#   Workload:  MODE(pub-latency|pub-throughput|recv-throughput|recv-latency) QOS TRANSPORT(tcp|tls) PAYLOAD_BYTES COUNT WARMUP
#              INFLIGHT INTERVAL_US TARGET_RATE TOPIC LABEL HOST PORT
#   Peer:      BATCH RATE            (feed only)
#   Pinning:   CLIENT_CORES PEER_CORES   (taskset masks; defaults suit a 16-vCPU F16s_v2)
#   Extras:    NETEM_DELAY (e.g. 5ms, needs root)  CERT_DIR (TLS)
#              LAYOUT_PAD  pad argv+env to a fixed size so the two A/B arms get identical stack
#                          layout (default 512; 0 disables) -- see the padding block below
#
# Usage: ./bench-once.sh            (all via env)
#        MODE=recv-throughput PAYLOAD_BYTES=256 COUNT=200000 ./bench-once.sh
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
MODE="${MODE:-pub-latency}"
QOS="${QOS:-1}"
TRANSPORT="${TRANSPORT:-tcp}"
PAYLOAD_BYTES="${PAYLOAD_BYTES:-64}"
COUNT="${COUNT:-50000}"
# Operations discarded before measurement starts.
#
# 50000 was tried and REVERTED. The claim was that it fixes p99 on open-loop configs as well as
# quadrupling COUNT does (pub-lat-open-tcp baseline p99 1261.5 us -> ~118 us). Measured with it
# actually deployed, across two hosts: mqttbench landed at 655.2 us and mqttbench2 at 1080.1 us for
# that config, against a 117.6 us target, with per-pair p99 sd of 251-321 rather than the 38.6 the
# original single run reported. Two other configs did hit the target on one host and not the other.
# Partial and inconsistent, not a fix.
#
# The likely reason the two are not interchangeable: open-loop configs measure latency from the
# INTENDED send time, so early slowness becomes a queue backlog. Discarding operations does not drain
# that backlog if the client never catches up within the warm-up; extending COUNT works because it
# dilutes the transient across a longer measured window. Different mechanisms.
#
# If p99 on open-loop configs matters, use 4x COUNT (~25 s/rep) and accept the cost, or treat p99 as
# info-only there. Do not raise this number again without measuring the deployed result on both hosts.
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

# Defaults target a 16-vCPU F16s_v2 (8 physical cores, 2 HT siblings each): one client/peer worker
# per PHYSICAL core with the sibling left idle (no intra-process HT contention), OS + IRQs + spare
# cores absorb background. Cores 2,4 = phys 1,2; 8,10 = phys 4,5 (see `lscpu -e`). Override for your
# box; taskset will error on nonexistent cores.
CLIENT_CORES="${CLIENT_CORES:-2,4}"
PEER_CORES="${PEER_CORES:-8,10}"
NETEM_DELAY="${NETEM_DELAY:-}"
CERT_DIR="${CERT_DIR:-$script_dir/certs}"
# Target byte-count for the client's layout-sensitive argv/env fields; see the padding block below.
# 0 disables padding. Raise it if the warning there fires.
LAYOUT_PAD="${LAYOUT_PAD:-512}"

# Default port by transport.
if [[ -z "$PORT" ]]; then
    [[ "$TRANSPORT" == "tls" ]] && PORT=8883 || PORT=1883
fi

# Peer role implied by the client mode.
case "$MODE" in
    recv-throughput | recv-latency) PEER_ROLE=feed ;;
    pub-latency | pub-throughput) PEER_ROLE=sink ;;
    *)
        echo "unknown MODE '$MODE' (expected pub-latency|pub-throughput|recv-throughput|recv-latency)" >&2
        exit 2
        ;;
esac

# recv-latency needs the peer to stamp each publish's payload with its send time (epoch nanos).
if [[ "$MODE" == "recv-latency" ]]; then STAMP=1; else STAMP="${STAMP:-0}"; fi

# Open-loop latency busy-spins one core to pace precisely; give it a dedicated core so it can't
# steal cycles from the measured client and distort the latency-vs-rate curve (need >=3: 2 for
# client work + 1 pacer).
if [[ "$MODE" == "pub-latency" ]] && awk "BEGIN{exit !($TARGET_RATE > 0)}"; then
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
        echo "         CLIENT_CORES=2,4,6 (2 for client work + 1 for the pacer)." >&2
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
client_bin="${CLIENT_BIN:-$target_dir/release/bench_client}"
peer_bin="${PEER_BIN:-$target_dir/release/bench_peer}"

# ---- build (before measuring, so cargo isn't compiling during the run) --------------------------
# Skip the build when both binaries are supplied prebuilt -- bench-compare.sh passes CLIENT_BIN/PEER_BIN
# to interleave two already-built revisions without rebuilding between reps.
if [[ -z "${CLIENT_BIN:-}" || -z "${PEER_BIN:-}" ]]; then
    echo "building release binaries..." >&2
    cargo build --release -q -p bench_client -p bench_peer
fi
for _b in "$client_bin" "$peer_bin"; do
    [[ -x "$_b" ]] || {
        echo "ERROR: binary not found or not executable: $_b" >&2
        exit 1
    }
done

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
    QOS="$QOS" STAMP="$STAMP" BATCH="$BATCH" RATE="$RATE" "${peer_tls_env[@]}" \
    taskset -c "$PEER_CORES" "$peer_bin" >"$peer_log" 2>&1 &
peer_pid=$!

# Wait for the peer to report it is listening.
peer_ready=0
for _ in $(seq 1 200); do
    if grep -q "listening on" "$peer_log" 2>/dev/null; then peer_ready=1; break; fi
    kill -0 "$peer_pid" 2>/dev/null || {
        echo "ERROR: peer exited during startup:" >&2
        cat "$peer_log" >&2
        exit 1
    }
    sleep 0.05
done
# Loop can exhaust with the peer still alive but never ready -- don't start the client blind.
if ((peer_ready == 0)); then
    echo "ERROR: peer never reported 'listening on' within ~10s (still running); aborting:" >&2
    cat "$peer_log" >&2
    exit 1  # cleanup trap kills the peer
fi

# ---- run client (pinned, timed) -----------------------------------------------------------------
echo "running bench_client[$MODE] on cores $CLIENT_CORES (COUNT=$COUNT, PAYLOAD_BYTES=$PAYLOAD_BYTES)..." >&2
client_env=(
    MODE="$MODE" QOS="$QOS" HOST="$HOST" PORT="$PORT" PAYLOAD_BYTES="$PAYLOAD_BYTES"
    COUNT="$COUNT" WARMUP="$WARMUP" INFLIGHT="$INFLIGHT" INTERVAL_US="$INTERVAL_US" TOPIC="$TOPIC"
    TARGET_RATE="$TARGET_RATE"
    "${client_tls_env[@]}"
)
[[ -n "$LABEL" ]] && client_env+=(LABEL="$LABEL")

# argv and environ are copied onto the top of the initial stack, so their TOTAL byte count shifts
# every stack address below them and with it cache-set/alignment behaviour (Mytkowicz et al., ASPLOS
# '09, measured up to ~10% swings from environment size alone). In an interleaved A/B the two arms
# differ in exactly the fields below -- CLIENT_BIN path and LABEL -- so one arm would carry a fixed
# layout offset in EVERY rep. That is a systematic bias, not noise: it survives pairing and it
# reproduces across rounds, which is precisely what report.py reads as a real regression. Pad the
# variable-length fields to a constant total so both arms get identical layout.
if ((LAYOUT_PAD > 0)); then
    # argv[0] and CLIENT_BIN= both carry the path, hence 2x.
    pad_len=$((LAYOUT_PAD - 2 * ${#client_bin} - ${#LABEL}))
    if ((pad_len < 0)); then
        echo "warning: LAYOUT_PAD=$LAYOUT_PAD too small for this binary path; A/B arms may differ in" >&2
        echo "         stack layout. Raise it above $((2 * ${#client_bin} + ${#LABEL}))." >&2
    else
        printf -v pad_str '%*s' "$pad_len" ''
        client_env+=(ISO_BENCH_PAD="${pad_str// /x}")
    fi
fi

if [[ -n "$TIME_BIN" ]]; then
    env "${client_env[@]}" "$TIME_BIN" -v -o "$time_out" \
        taskset -c "$CLIENT_CORES" "$client_bin" | tee "$result_out"
else
    env "${client_env[@]}" taskset -c "$CLIENT_CORES" "$client_bin" | tee "$result_out"
fi

# ---- CPU-per-message ----------------------------------------------------------------------------
# Two accountings, deliberately both emitted.
#
# WINDOWED (preferred): the client samples getrusage around its measured loop and reports the delta
# on the RESULT line, so numerator and denominator describe the same span. See bench_client/src/usage.rs.
#
# PROCESS (proc_*, from /usr/bin/time): the whole process -- startup, connect, TLS handshake, every
# warm-up op, measured loop, teardown -- divided by measured ops alone. This is what the metric used
# to mean, and why it moved ~15% on a warm-up-only A/B in which the binaries were byte-identical.
# Kept because it still detects startup/teardown regressions the window cannot see, and because it
# lets the two definitions be compared across the corpus boundary this change creates.
win_cpu="$(sed -n 's/.*"cpu":{\([^}]*\)}.*/\1/p' "$result_out" | head -1)"
if [[ -n "$win_cpu" ]]; then
    fld() { sed -n "s/.*\"$1\":\([^,}]*\).*/\1/p" <<<"$win_cpu"; }
    printf 'CPU {"user_s":%s,"sys_s":%s,"cpu_us_per_msg":%s,"max_rss_kb":%s,"cpu_window":"measured","windowed_rss":%s' \
        "$(fld user_s)" "$(fld sys_s)" "$(fld cpu_us_per_msg)" "$(fld max_rss_kb)" "$(fld windowed_rss)"
    if [[ -n "$TIME_BIN" && -s "$time_out" ]]; then
        p_user=$(awk -F': ' '/User time/{print $2}' "$time_out")
        p_sys=$(awk -F': ' '/System time/{print $2}' "$time_out")
        p_rss=$(awk -F': ' '/Maximum resident set size/{print $2}' "$time_out")
        p_per=$(awk -v u="${p_user:-0}" -v s="${p_sys:-0}" -v c="$COUNT" \
            'BEGIN{ if (c>0) printf "%.3f", (u+s)/c*1e6; else print "0" }')
        printf ',"proc_user_s":%s,"proc_sys_s":%s,"proc_cpu_us_per_msg":%s,"proc_max_rss_kb":%s' \
            "${p_user:-0}" "${p_sys:-0}" "$p_per" "${p_rss:-0}"
    fi
    printf '}\n'
elif [[ -n "$TIME_BIN" && -s "$time_out" ]]; then
    # Pre-windowing bench_client (no "cpu" object on RESULT). Tag it so the two definitions are never
    # pooled unknowingly -- report.py gates cpu_us_per_msg, and mixing them would compare a warm-up-
    # inclusive number against a warm-up-free one and read the difference as a regression.
    user_s=$(awk -F': ' '/User time/{print $2}' "$time_out")
    sys_s=$(awk -F': ' '/System time/{print $2}' "$time_out")
    rss_kb=$(awk -F': ' '/Maximum resident set size/{print $2}' "$time_out")
    cpu_us_per_msg=$(awk -v u="${user_s:-0}" -v s="${sys_s:-0}" -v c="$COUNT" \
        'BEGIN{ if (c>0) printf "%.3f", (u+s)/c*1e6; else print "0" }')
    printf 'CPU {"user_s":%s,"sys_s":%s,"cpu_us_per_msg":%s,"max_rss_kb":%s,"cpu_window":"process"}\n' \
        "${user_s:-0}" "${sys_s:-0}" "$cpu_us_per_msg" "${rss_kb:-0}"
fi

echo "done." >&2
