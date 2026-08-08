#!/usr/bin/env bash
# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
#
# Builds two source trees SEVERAL times each with different code layouts, then runs the interleaved
# A/B suite across all of them. This is the front door for bench-compare.sh: it produces the
# CUR_BINS/REF_BINS lists that script consumes, so layout stops being a fixed property of the
# comparison.
#
# # Why not just build each side once
#
# Building each arm once makes code layout a CONSTANT inside the measurement: the same two binaries in
# every rep, every round, every repeat. Replication cannot remove it, because it is not noise -- it is
# a fixed offset sitting in the point estimate. Function placement, alignment and inlining shift when
# ANY source line moves, and those shifts cost the same order as the regressions worth catching.
#
# HOW MUCH THAT IS WORTH, MEASURED. A one-line diff whose injected work runs once per connection
# against 50,000 measured messages -- so it cannot cost anything real -- flagged 6 of 6 single-build
# runs on the PRE-WINDOWING harness. That is what motivated this script. It does not survive matching:
#
#     current harness, reps matched at 14:   multibuild 2/6   single-build 2/8   Fisher p = 1.000
#     pre-windowing, reps=10, single-build:  6/6                       vs 2/8    Fisher p = 0.010
#
# Both of those last two are SINGLE-BUILD, so layout cannot explain the difference between them. What
# differs is rep count and cpu/rss accounting -- cpu_us_per_msg fired in 5 of the 6, back when its
# numerator covered startup, the TLS handshake and every warm-up op. The false-positive problem this
# script was built for was substantially a broken metric.
#
# Read p = 1.000 as a BOUND: at n=6 vs n=8 only a split as wide as ~6/8 vs 0-1/8 would have cleared
# p<0.05, so a modest benefit would be invisible. What is established is that there is no LARGE one.
#
# while genuine regressions (a bounded busy-spin per outgoing packet) were detected ALMOST as well. At
# the same rep count, across two effect sizes and two hosts, multibuild scored 83-100% of single-build:
#
#     ~0.9% regression   20 vs 21   and   19 vs 23 gated cells
#     ~3.7% regression   32 vs 32   and   33 vs 35
#
# So the trade is a few percent of detection -- ~5% typical, 17% worst measured -- for roughly 83% of
# the layout false positives. Two earlier readings of these runs were wrong and are worth not
# repeating: one compared across different rep counts (reps alone moves detection a lot, 26->32 going
# 10->14), and one then concluded "no cost" from the reps-matched pair at a single large effect size,
# before the threshold-band case was measured. See SENSITIVITY.md.
#
# Randomising layout is the standard remedy: Mytkowicz et al., "Producing Wrong Data Without Doing
# Anything Obviously Wrong!" (ASPLOS 2009); Curtsinger & Berger, "Stabilizer" (ASPLOS 2013).
# bench-compare.sh already randomises execution order, stack/env padding and warm-up arm; the binary
# was the last thing held fixed.
#
# # What it costs
#
# One extra `cargo build` per variant per arm (~15 s each, so ~2 minutes for the default 5 variants on
# both arms) against a suite that runs for over an hour. The MEASURED work is unchanged: reps are
# spread across the builds, not multiplied by them.
#
# REPS defaults to 14 here rather than bench-compare.sh's own 14-for-multibuild reasoning being a
# coincidence -- spreading reps across builds introduces a between-build variance component measured
# at 21-30% of total, so ~1.4x the pairs are needed to recover the power a single-build suite had.
#
# # Usage
#
#   REF_SRC=../iso-main CUR_SRC=. ./bench-multibuild.sh
#
#   # typical: compare a branch against main using a git worktree
#   git worktree add ../iso-main main
#   REF_SRC=../iso-main CUR_SRC=.. REF_LABEL=main CUR_LABEL=branch ./bench-multibuild.sh
#
# Env:
#   REF_SRC       repo root of the BASELINE tree (must contain iso_bench/)   (required)
#   CUR_SRC       repo root of the CANDIDATE tree                            (default: REF_SRC => A/A)
#   BUILDS        layout variants per arm; 1 disables multibuild             (default 5)
#   BUILD_DIR     where target dirs go                                       (default: mktemp -d)
#   KEEP_BUILDS   1 = keep BUILD_DIR on exit (default: remove if we made it)
#   REF_RUSTFLAGS / CUR_RUSTFLAGS   extra per-arm codegen flags, layout variants append to these
#   Everything else (REPS, ROUNDS, SEED, CUR_LABEL, REF_LABEL, CUR_SHA, REF_SHA, WARMUP_REPS,
#   RESULTS_FILE, RESET, CERT_DIR, NETEM_DELAY) is passed straight through to bench-compare.sh.
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

: "${REF_SRC:?set REF_SRC to the baseline repo root -- the directory that contains iso_bench/}"
CUR_SRC="${CUR_SRC:-$REF_SRC}"
BUILDS="${BUILDS:-5}"
REF_RUSTFLAGS="${REF_RUSTFLAGS:-}"
CUR_RUSTFLAGS="${CUR_RUSTFLAGS:-}"

command -v cargo >/dev/null || {
    echo "ERROR: cargo not on PATH. Run ./install-prereqs.sh, then: source ~/.cargo/env" >&2
    exit 1
}
for d in "$REF_SRC" "$CUR_SRC"; do
    [[ -d "$d/iso_bench" ]] || {
        echo "ERROR: '$d' has no iso_bench/ -- point REF_SRC/CUR_SRC at a REPO ROOT, not at iso_bench" >&2
        exit 1
    }
done

# ---- the arms must differ only in the LIBRARY ---------------------------------------------------
# If the harness differs too, the two arms were measured with different instruments and the verdict is
# meaningless. Refuse rather than emit a confidently wrong answer -- report.py's drift check compares
# recorded workload parameters, which does not catch an edited script. Cheap, and it has caught real
# mistakes: it is easy to fix a harness bug on one branch and forget the other.
if [[ "$(cd "$REF_SRC" && pwd)" != "$(cd "$CUR_SRC" && pwd)" ]]; then
    if ! diff -r -q "$REF_SRC/iso_bench" "$CUR_SRC/iso_bench" \
         -x target -x results.jsonl -x certs -x __pycache__ -x 'Cargo.lock' >/tmp/iso-harness-diff.$$ 2>&1; then
        echo "ERROR: the two trees' iso_bench/ differ -- arms must differ only in the library source." >&2
        sed 's/^/  /' /tmp/iso-harness-diff.$$ >&2
        rm -f /tmp/iso-harness-diff.$$
        exit 3
    fi
    rm -f /tmp/iso-harness-diff.$$
fi

# ---- layout variants ----------------------------------------------------------------------------
# Two mechanisms, not one family: function alignment moves whole functions onto boundaries, block
# alignment moves basic blocks WITHIN functions. Mixing them samples the layout space more broadly
# than several points on one axis would. Variant 0 is empty = the natural layout, so BUILDS=1
# reproduces the old single-build behaviour exactly.
#
# CAVEAT worth knowing: alignment padding is a UNIFORM perturbation -- every function moves -- whereas
# a real source diff moves some and not others. These are a few points from two families, not a random
# sample of the layout space. It is a large improvement on holding layout fixed, not a solved problem.
LAYOUT_VARIANTS=(
    ""
    "-C llvm-args=-align-all-functions=5"
    "-C llvm-args=-align-all-functions=6"
    "-C llvm-args=-align-all-nofallthru-blocks=4"
    "-C llvm-args=-align-all-functions=5 -C llvm-args=-align-all-nofallthru-blocks=5"
)

if [[ -n "${BUILD_DIR:-}" ]]; then
    mkdir -p "$BUILD_DIR"
    made_build_dir=0
else
    BUILD_DIR="$(mktemp -d -t iso-multibuild-XXXXXX)"
    made_build_dir=1
fi
cleanup() {
    if [[ "$made_build_dir" == "1" && "${KEEP_BUILDS:-0}" != "1" ]]; then
        rm -rf "$BUILD_DIR"
    else
        echo "build products kept in $BUILD_DIR" >&2
    fi
}
trap cleanup EXIT

build_log="$BUILD_DIR/build.log"
: >"$build_log"

# Separate CARGO_TARGET_DIR per (role, variant) so builds never share artifacts -- a shared target dir
# would let one variant's objects satisfy another's, silently collapsing the layouts back into one.
build_one() {
    local src="$1" role="$2" flags="$3" idx="$4"
    local out="$BUILD_DIR/target-$role$idx"
    ( cd "$src/iso_bench" && CARGO_TARGET_DIR="$out" RUSTFLAGS="$flags" \
        cargo build --release -p bench_client ) >>"$build_log" 2>&1 || return 1
    echo "$out/release/bench_client"
}

echo "== building $BUILDS layout variant(s) per arm into $BUILD_DIR ==" >&2
REF_BINS=""
CUR_BINS=""
same_arm=0
[[ "$(cd "$REF_SRC" && pwd)" == "$(cd "$CUR_SRC" && pwd)" && "$REF_RUSTFLAGS" == "$CUR_RUSTFLAGS" ]] && same_arm=1

for ((bi = 0; bi < BUILDS; bi++)); do
    variant="${LAYOUT_VARIANTS[bi % ${#LAYOUT_VARIANTS[@]}]}"
    printf '   [%d/%d] baseline%s\r' "$((bi + 1))" "$BUILDS" "${variant:+ (}${variant}${variant:+)}" >&2
    b="$(build_one "$REF_SRC" ref "$REF_RUSTFLAGS $variant" "$bi")" || {
        echo "" >&2
        echo "ERROR: baseline build failed for variant $bi -- see $build_log" >&2
        tail -20 "$build_log" >&2
        exit 4
    }
    REF_BINS="${REF_BINS:+$REF_BINS:}$b"
    if ((same_arm)); then
        # Same tree AND same flags => byte-identical binary, so build once and point both arms at it.
        # This is the A/A shape; building twice would waste ~15 s and prove nothing.
        CUR_BINS="${CUR_BINS:+$CUR_BINS:}$b"
    else
        printf '   [%d/%d] candidate%s\r' "$((bi + 1))" "$BUILDS" "${variant:+ (}${variant}${variant:+)}" >&2
        c="$(build_one "$CUR_SRC" cur "$CUR_RUSTFLAGS $variant" "$bi")" || {
            echo "" >&2
            echo "ERROR: candidate build failed for variant $bi -- see $build_log" >&2
            tail -20 "$build_log" >&2
            exit 4
        }
        CUR_BINS="${CUR_BINS:+$CUR_BINS:}$c"
    fi
done
echo "   built $BUILDS variant(s) per arm                              " >&2

# bench_peer is build-invariant (its manifest deliberately does not depend on ms-mqtt-client), so one
# build serves both arms and cannot itself drift between them.
if [[ -z "${PEER_BIN:-}" ]]; then
    echo "== building bench_peer (build-invariant reference peer) ==" >&2
    ( cd "$REF_SRC/iso_bench" && CARGO_TARGET_DIR="$BUILD_DIR/target-peer" \
        cargo build --release -q -p bench_peer ) >>"$build_log" 2>&1 || {
        echo "ERROR: bench_peer build failed -- see $build_log" >&2
        tail -20 "$build_log" >&2
        exit 4
    }
    PEER_BIN="$BUILD_DIR/target-peer/release/bench_peer"
fi

# REPS is bench-compare.sh's own default (14, sized for multibuild). Drop to 10 when the caller has
# explicitly disabled multibuild, so BUILDS=1 costs what single-build always cost.
if ((BUILDS == 1)) && [[ -z "${REPS:-}" ]]; then
    REPS=10
fi

# Export rather than using a `VAR=x exec ...` prefix: the values come from expansions, and an expanded
# `REPS=2` is parsed as a COMMAND, not as an assignment -- which failed with "REPS=2: command not
# found" the first time this was written.
export CUR_BIN="${CUR_BINS%%:*}" REF_BIN="${REF_BINS%%:*}"
export CUR_BINS REF_BINS PEER_BIN
if [[ -n "${REPS:-}" ]]; then
    export REPS
fi
exec "$script_dir/bench-compare.sh"
