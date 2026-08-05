#!/usr/bin/env bash
# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
#
# INTERLEAVED head-to-head A/B: runs two prebuilt bench_client binaries against ONE shared peer,
# alternating them rep-by-rep in randomized order, so slow environmental drift (turbo, neighbours,
# thermal) is COMMON to each adjacent pair and cancels in the per-pair delta. report.py then uses a
# PAIRED test whose threshold self-calibrates from the paired-delta spread -- far tighter than the
# sequential "whole suite A, then whole suite B" flow, which is confounded by between-block drift.
#
# It does NOT build the two client binaries -- you build them (a fresh cargo target per ref keeps them
# independent), then point this at both. The reference can be the OTHER branch (a gate) or a frozen
# anchor binary (records a drift-normalised ratio you can compare across time). The peer is
# build-invariant, so it is built once here (or pass PEER_BIN).
#
#   # build both revisions into separate targets (example with git worktrees):
#   git worktree add ../iso-main main
#   ( cd ../iso-main/iso_bench && CARGO_TARGET_DIR=/tmp/t-main cargo build --release -p bench_client )
#   CARGO_TARGET_DIR=/tmp/t-cur cargo build --release -p bench_client
#   CUR_BIN=/tmp/t-cur/release/bench_client  REF_BIN=/tmp/t-main/release/bench_client \
#     CUR_LABEL=branch REF_LABEL=main ./bench-compare.sh
#
# Env:
#   CUR_BIN       current/new build's bench_client binary            (required)
#   REF_BIN       reference build's bench_client binary (branch|anchor) (required)
#   CUR_LABEL     tag for the current build                          (default: current)
#   REF_LABEL     tag for the reference build (report BASELINE)      (default: reference)
#   CUR_SHA / REF_SHA   provenance git SHA per binary                (default: unknown)
#   PEER_BIN      prebuilt bench_peer; built here if unset
#   REPS          interleaved PAIRS per config per round             (default 10)
#   SEED          seed for the per-config interleave shuffle         (default: random)
#                 Recorded into every JSONL record; reuse it to replay an identical run order.
#   ROUNDS        replication rounds: whole suite run this many times (default 2). A verdict needs the
#                 effect to reproduce across rounds -- rounds are a full suite apart so arm-luck differs.
#   WARMUP_REPS   throwaway CPU-saturating warm-up runs first        (default 8; 0 = skip)
#   RESULTS_FILE  JSONL accumulator                                  (default: ./results.jsonl)
#   RESET         1 = truncate RESULTS_FILE first                    (default 1)
#   CERT_DIR / NETEM_DELAY   passed through to bench-once.sh
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

: "${CUR_BIN:?set CUR_BIN to the current build's bench_client binary}"
: "${REF_BIN:?set REF_BIN to the reference build's bench_client binary}"
CUR_LABEL="${CUR_LABEL:-current}"
REF_LABEL="${REF_LABEL:-reference}"
CUR_SHA="${CUR_SHA:-unknown}"
REF_SHA="${REF_SHA:-unknown}"
REPS="${REPS:-10}"
ROUNDS="${ROUNDS:-2}"  # whole-suite replication rounds; report.py requires a verdict to reproduce across them
SEED="${SEED:-$RANDOM}"
RANDOM="$SEED"  # seed once: the whole run's shuffle sequence is then a function of SEED alone
WARMUP_REPS="${WARMUP_REPS:-8}"
RESULTS_FILE="${RESULTS_FILE:-$script_dir/results.jsonl}"
RESET="${RESET:-1}"

command -v python3 >/dev/null || {
    echo "ERROR: python3 is required for aggregation" >&2
    exit 1
}
for b in "$CUR_BIN" "$REF_BIN"; do
    [[ -x "$b" ]] || {
        echo "ERROR: client binary not found or not executable: $b" >&2
        exit 1
    }
done
if [[ -z "${PEER_BIN:-}" ]]; then
    command -v cargo >/dev/null || {
        echo "ERROR: cargo not on PATH (needed to build the peer). Run ./install-prereqs.sh, then: source ~/.cargo/env" >&2
        exit 1
    }
    echo "building bench_peer (build-invariant reference peer) ..." >&2
    cargo build --release -q -p bench_peer
    PEER_BIN="${CARGO_TARGET_DIR:-$script_dir/target}/release/bench_peer"
fi
export PEER_BIN

# shellcheck source=suite.sh
source "$script_dir/suite.sh"

prov_rustc="$(rustc --version 2>/dev/null | awk '{print $2}' || true)"
prov_host="$(hostname 2>/dev/null || echo unknown)"

[[ "$RESET" == "1" ]] && : >"$RESULTS_FILE"

# Run one measured rep of one binary and append its record (tagged with the pair index and round).
run_and_record() {
    local bin="$1" label="$2" sha="$3" pair="$4" cfg="$5" cfg_name="$6" round="$7"
    local err_log out result_line cpu_line
    err_log="$(mktemp)"
    # shellcheck disable=SC2086
    if ! out="$(env $cfg CLIENT_BIN="$bin" PEER_BIN="$PEER_BIN" LABEL="$label" ./bench-once.sh 2>"$err_log")"; then
        echo "FAILED ($label, $cfg_name)" >&2
        cat "$err_log" >&2
        rm -f "$err_log"
        exit 1
    fi
    rm -f "$err_log"
    result_line="$(grep '^RESULT ' <<<"$out" || true)"
    cpu_line="$(grep '^CPU ' <<<"$out" || true)"
    [[ -n "$result_line" ]] || {
        echo "no RESULT line ($label, $cfg_name)" >&2
        echo "$out" >&2
        exit 1
    }
    RESULT_JSON="${result_line#RESULT }" CPU_JSON="${cpu_line#CPU }" \
        REC_LABEL="$label" REC_CONFIG="$cfg_name" REP="$pair" REC_PAIR="$pair" REC_ROUND="$round" OUT_FILE="$RESULTS_FILE" \
        PROV_SHA="$sha" PROV_DIRTY=0 PROV_RUSTC="$prov_rustc" PROV_HOST="$prov_host" PROV_SEED="$SEED" \
        python3 "$script_dir/record.py" >/dev/null
}

# Fill the global `order` with a balanced, shuffled per-pair order: zeros (CUR first) and ones (REF
# first). With even REPS the split is exact (REPS/2 each); with odd REPS an even split is impossible,
# so the single leftover run goes to a RANDOM side per config -- no build is systematically favored.
# The shuffle then keeps anti-aliasing.
make_order() {
    order=()
    local i j t zeros=$((REPS / 2))
    # odd REPS: hand the leftover run to CUR or REF by coin flip so the tiny imbalance has no fixed sign
    if ((REPS % 2 == 1)) && ((RANDOM % 2 == 0)); then zeros=$((zeros + 1)); fi
    for ((i = 0; i < REPS; i++)); do
        if ((i < zeros)); then order+=(0); else order+=(1); fi
    done
    for ((i = REPS - 1; i > 0; i--)); do  # Fisher-Yates
        j=$((RANDOM % (i + 1)))
        t="${order[i]}"
        order[i]="${order[j]}"
        order[j]="$t"
    done
}

echo "== iso_bench COMPARE (interleaved): [$CUR_LABEL] vs baseline [$REF_LABEL], reps=$REPS rounds=$ROUNDS configs=${#suite[@]} seed=$SEED ==" >&2

# Warm the box (CPU-saturating throughput-TLS), alternating binaries so both crypto paths warm; not
# recorded. See bench.sh for why latency warm-ups don't ramp turbo.
if ((WARMUP_REPS > 0)); then
    echo "== warm-up: $WARMUP_REPS discarded CPU-saturating runs (skip with WARMUP_REPS=0) ==" >&2
    # Coin-flip which arm LEADS the alternation. Alternating from a fixed start means the arm that runs
    # LAST before the measured phase is always the same one (CUR, at even WARMUP_REPS), so it enters
    # measurement with marginally warmer caches -- small, but a fixed-sign advantage to one build, which
    # is exactly the kind of asymmetry make_order goes to some trouble to avoid in the measured pairs.
    # The flip keeps the ref/cur split exact for even WARMUP_REPS while randomising who goes last.
    warm_lead=$((RANDOM % 2))
    for ((w = 1; w <= WARMUP_REPS; w++)); do
        printf '   warm-up %d/%d\r' "$w" "$WARMUP_REPS" >&2
        warm_bin="$REF_BIN"
        (((w + warm_lead) % 2 == 0)) && warm_bin="$CUR_BIN"
        env MODE=pub-throughput QOS=1 TRANSPORT=tls PAYLOAD_BYTES=16384 INFLIGHT=64 COUNT=300000 \
            CLIENT_BIN="$warm_bin" PEER_BIN="$PEER_BIN" ./bench-once.sh >/dev/null 2>&1 || true
    done
    echo "   warm-up done          " >&2
fi

# Rounds are the OUTER loop so each config's two rounds are a full suite apart in time -- their
# arm-luck (fast per-pair scheduling variance) is then independent, which is what lets report.py
# use round agreement as corroboration. Duplicating a config back-to-back would NOT achieve that.
for ((round = 1; round <= ROUNDS; round++)); do
    ((ROUNDS > 1)) && echo "" >&2 && echo "=== round $round/$ROUNDS ===" >&2
    i=0
    for cfg in "${suite[@]}"; do
        i=$((i + 1))
        cfg_name="${cfg%% *}"
        cfg_name="${cfg_name#CONFIG=}"
        echo "" >&2
        echo ">>> [round $round/$ROUNDS] config [$i/${#suite[@]}]: $cfg_name" >&2
        make_order
        for ((p = 1; p <= REPS; p++)); do
            printf '   [pair %d/%d] interleaving %s / %s ...\r' "$p" "$REPS" "$CUR_LABEL" "$REF_LABEL" >&2
            # Balanced shuffled order (see make_order): no build is systematically the 2nd runner.
            if ((order[p - 1] == 0)); then
                run_and_record "$CUR_BIN" "$CUR_LABEL" "$CUR_SHA" "$p" "$cfg" "$cfg_name" "$round"
                run_and_record "$REF_BIN" "$REF_LABEL" "$REF_SHA" "$p" "$cfg" "$cfg_name" "$round"
            else
                run_and_record "$REF_BIN" "$REF_LABEL" "$REF_SHA" "$p" "$cfg" "$cfg_name" "$round"
                run_and_record "$CUR_BIN" "$CUR_LABEL" "$CUR_SHA" "$p" "$cfg" "$cfg_name" "$round"
            fi
        done
        echo "   $cfg_name: $REPS pairs done                        " >&2
    done
done

echo "" >&2
echo "== compare done -> $RESULTS_FILE ==" >&2
python3 "$script_dir/report.py" "$RESULTS_FILE" --baseline "$REF_LABEL" --no-hist
echo "   (full report incl. histograms: python3 report.py $RESULTS_FILE --baseline $REF_LABEL)" >&2
