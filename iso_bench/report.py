#!/usr/bin/env python3
# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
#
# Human-readable renderer for a JSONL results file (written by bench-workload.sh / bench.sh). It is
# the "read the results" tool: an overview, then per-config statistic tables (median / mean / min /
# max / CV%), an A/B comparison when a config has >= 2 labels, and a text histogram of the latency /
# inter-arrival distribution (summed from the per-rep `hist_ns` buckets).
#
# Grouping is by CONFIG, so this works unchanged for a single config (bench-workload.sh scopes to
# its own with --config) or a whole suite (bench.sh writes every config to one file).
#
# Usage:
#   report.py [results.jsonl] [--config NAME] [--label NAME] [--baseline NAME] [--no-hist | --hist-only]
#
#   --config NAME   only this config          --no-hist    tables only (no histograms)
#   --label NAME    only this build label      --hist-only  histograms only (no tables)
#   --baseline NAME force the A/B baseline label (else first-seen); interleaved data uses a paired test
import argparse
import collections
import json
import math
import random
import statistics as st
import sys

# (json key, display label, unit, "up"|"down" = which direction is BETTER for A/B verdicts)
METRICS = [
    ("msgs_per_s", "throughput", "msg/s", "up"),
    ("mib_per_s", "throughput", "MiB/s", "up"),
    ("lat_p50", "p50", "us", "down"),
    ("lat_p90", "p90", "us", "down"),
    ("lat_p99", "p99", "us", "down"),
    ("lat_max", "max", "us", "down"),
    ("cpu_us_per_msg", "cpu/msg", "us", "down"),
    ("max_rss_kb", "max rss", "KB", "down"),
]


def config_of(r):
    return r.get("config") or (
        f"{r.get('mode')}-{r.get('transport')}-q{r.get('qos')}-{r.get('payload_bytes')}b"
    )


def ordered(seq):
    """Distinct values, preserving first-seen order (labels/configs keep run order)."""
    out = []
    for x in seq:
        if x not in out:
            out.append(x)
    return out


def cv(xs):
    """Coefficient of variation (%) -- run-to-run noise. 0 for < 2 samples."""
    if len(xs) < 2:
        return 0.0
    mean = st.mean(xs)
    return 0.0 if mean == 0 else st.pstdev(xs) / mean * 100.0


def fmt_num(x):
    ax = abs(x)
    if ax >= 1000:
        return f"{x:,.0f}"
    if ax >= 100:
        return f"{x:.1f}"
    return f"{x:.2f}"


def rule(char="=", width=74):
    return char * width


def config_meta(rows):
    """One-line descriptor + latency-kind label for a config, from a representative row."""
    r = rows[0]
    parts = [r.get("mode", "?"), r.get("transport", "?"), f"qos{r.get('qos')}"]
    parts.append(f"{r.get('payload_bytes', '?')}B")
    if (r.get("target_rate") or 0) > 0:
        parts.append(f"open-loop {fmt_num(r['target_rate'])}/s")
    kind = r.get("lat_kind", "latency")
    return ", ".join(str(p) for p in parts), kind


def print_overview(rows, configs):
    labels = ordered(r["label"] for r in rows)
    print(rule())
    print(f" iso_bench results  ({len(rows)} records)")
    print(rule())
    print(f" builds (labels): {', '.join(labels)}")
    print(f" configs:         {len(configs)}")
    seeds = ordered(r["seed"] for r in rows if r.get("seed") is not None)
    if seeds:
        print(f" interleave seed: {', '.join(str(s) for s in seeds)}   (replay with SEED=<seed>)")

    # Per-build provenance -- so cross-branch comparisons are auditable. A/B assumes the harness /
    # workload is identical across labels; differing toolchain or host is a confound worth flagging.
    prov = {lbl: first_with(rows, lbl, "git_sha") for lbl in labels}
    if any(prov.values()):
        print()
        for lbl in labels:
            r = prov.get(lbl) or {}
            sha = r.get("git_sha", "?")
            if r.get("git_dirty"):
                sha += "-dirty"
            print(f"   [{lbl:<12}] sha={sha:<16} rustc={r.get('rustc', '?'):<9} host={r.get('host', '?')}")
        for field, what in (("rustc", "toolchain"), ("host", "host")):
            vals = {(prov.get(l) or {}).get(field) for l in labels if prov.get(l)}
            if len(vals) > 1:
                print(f"   !! {what} differs across builds ({', '.join(sorted(map(str, vals)))}) -- a confound")

    # cpu_us_per_msg / max_rss_kb have two incompatible definitions: "measured" (getrusage bracketing
    # the measured loop) and "process" (/usr/bin/time over the whole run, warm-up included). They can
    # differ threefold on identical work, so a file mixing them makes those two metrics meaningless.
    # Warn loudly rather than in a footnote -- an unnoticed mix reads as a large clean regression.
    windows = {r.get("cpu_window", "process") for r in rows if "cpu_us_per_msg" in r}
    if len(windows) > 1:
        print()
        print(f"   !! cpu_us_per_msg/max_rss_kb mix accountings ({', '.join(sorted(windows))}) --")
        print("      'process' includes warm-up, 'measured' does not; those cells are NOT comparable")
    elif windows == {"process"}:
        print("   note: cpu/rss are process-wide (warm-up included) -- pre-windowing bench_client")

    print()
    print(f" {'config':<12}{'build':<14}{'reps':>5}   description")
    for c in configs:
        crows = [r for r in rows if config_of(r) == c]
        desc, _ = config_meta(crows)
        for lbl in ordered(r["label"] for r in crows):
            n = sum(1 for r in crows if r["label"] == lbl)
            print(f" {c:<12}{lbl:<14}{n:>5}   {desc}")


def first_with(rows, label, key):
    """First record for a label that carries `key` (provenance is identical across a label's reps)."""
    for r in rows:
        if r.get("label") == label and key in r:
            return r
    return None


# Workload params that MUST match across labels for an A/B to be valid (the instrument, not the
# specimen). If any differ, the two labels measured different things -- flag loudly, don't compare.
DRIFT_KEYS = ["mode", "transport", "qos", "payload_bytes", "count", "inflight", "target_rate"]


def workload_drift(rows, config, labels):
    """Workload params that differ across labels for one config -> [(key, {label: value})]; [] = clean.
    Shared by the human report and --json so both read drift from the same place."""
    drifted = []
    for key in DRIFT_KEYS:
        per_label = {}
        for lbl in labels:
            vals = [r[key] for r in rows if config_of(r) == config and r["label"] == lbl and key in r]
            if vals:
                per_label[lbl] = vals[0]
        if len({str(v) for v in per_label.values()}) > 1:
            drifted.append((key, per_label))
    return drifted


def print_drift(rows, config, labels):
    """Warn if the workload definition drifted across labels. Returns True if drift was found."""
    drifted = workload_drift(rows, config, labels)
    if drifted:
        print("\n  !! WORKLOAD DRIFT across labels -- this A/B compares different workloads:")
        for key, per_label in drifted:
            cells = "  ".join(f"[{l}]={v}" for l, v in per_label.items())
            print(f"       {key}: {cells}")
        print("     (same harness/config assumed; verdicts below are NOT trustworthy)")
    return bool(drifted)


def series(rows, config, label, metric):
    return [
        r[metric]
        for r in rows
        if config_of(r) == config and r["label"] == label and r.get(metric) is not None
    ]


def print_summary_table(rows, config, labels, kind):
    # cv% below is POOLED over all rounds (a single number, not per-round). This blends within-round
    # noise with any between-round drift -- an honest total-variability read. Revisit if a per-round
    # split is wanted (round consistency is already visible in the paired table's rndN columns).
    nr = len(rounds_of(rows, config))
    for label in labels:
        n = sum(1 for r in rows if config_of(r) == config and r["label"] == label)
        rtag = f"  ({nr} rounds × {n // nr})" if nr >= 2 and n % nr == 0 else ""
        print(f"\n  [{label}]  {n} reps{rtag}   (latency rows = {kind})")
        print(
            f"  {'metric':<12}{'unit':>7}{'median':>13}{'mean':>13}"
            f"{'min':>13}{'max':>13}{'cv%':>7}"
        )
        print(f"  {rule('-', 78)}")
        for key, disp, unit, _ in METRICS:
            xs = series(rows, config, label, key)
            if not xs:
                continue
            print(
                f"  {disp:<12}{unit:>7}{fmt_num(st.median(xs)):>13}{fmt_num(st.mean(xs)):>13}"
                f"{fmt_num(min(xs)):>13}{fmt_num(max(xs)):>13}{cv(xs):>7.1f}"
            )


def info_metrics(rep):
    """Metrics shown as 'info' (context only, never gated) for a config. lat_max is a single worst
    sample (p99/p90 carry the tail); inter-arrival p50 is recv-throughput delivery cadence (throughput
    carries the rate, p90/p99 the jitter); QoS 0 op-latency p50 is queue-ADMISSION time, not send cost.
    QoS 0 THROUGHPUT stays gated -- its publish queue is bounded, so its rate tracks the real send rate."""
    info = {"lat_max"}
    if rep.get("lat_kind") == "inter-arrival":
        info.add("lat_p50")
    if rep.get("mode") == "pub-throughput" and rep.get("qos") in (0, "0"):
        info.add("lat_p50")
    return info


def print_comparison(rows, config, labels):
    base, latest = labels[0], labels[-1]
    print(f"\n  A/B comparison  (median; baseline=[{base}], delta=[{latest}])")
    header = "".join(f"{('[' + l + ']'):>14}" for l in labels)
    print(f"  {'metric':<12}{header}{'delta%':>10}{'verdict':>11}")
    print(f"  {rule('-', 12 + 14 * len(labels) + 21)}")
    rep = next((r for r in rows if config_of(r) == config), {})
    info_only = info_metrics(rep)
    for key, disp, _, better in METRICS:
        bxs = series(rows, config, base, key)
        if not bxs:
            continue
        bmed, bcv = st.median(bxs), cv(bxs)
        cells = "".join(
            f"{(fmt_num(st.median(series(rows, config, l, key))) if series(rows, config, l, key) else '-'):>14}"
            for l in labels
        )
        lxs = series(rows, config, latest, key)
        if lxs and bmed:
            d = (st.median(lxs) - bmed) / bmed * 100.0
            if key in info_only:
                verdict = "info"
            elif abs(d) <= max(bcv, 1.0):
                verdict = "~noise"
            else:
                improved = (d > 0) if better == "up" else (d < 0)
                verdict = "better" if improved else "WORSE"
            print(f"  {disp:<12}{cells}{d:>9.1f}%{verdict:>11}")
        else:
            print(f"  {disp:<12}{cells}{'-':>10}{'-':>11}")


# ---- interleaved (paired) A/B ---------------------------------------------------------------------
# bench-compare.sh interleaves the two builds rep-by-rep and stamps each record with a `pair` index.
# Environmental drift is then COMMON to each pair, so the per-pair delta cancels it and the threshold
# self-calibrates from the paired-delta spread -- no CV-band guess needed.
PAIRED_FLOOR_PCT = 0.5  # ignore statistically-consistent deltas below the measurement grain
# Per-metric practical floor (%): max_rss is page-quantized and ultra-low-variance, so a 1-2 page
# shift reads as "significant" but is meaningless -- require a real move before flagging.
PAIRED_FLOOR = {"max_rss_kb": 2.0}


def has_pairs(rows, config):
    return any(config_of(r) == config and r.get("pair") is not None for r in rows)


def paired_map(rows, config, label, key, rnd=None):
    """pair index -> metric value, for one (config, label[, round]). First value wins per pair."""
    out = {}
    for r in rows:
        if config_of(r) == config and r.get("label") == label and r.get("pair") is not None and r.get(key) is not None:
            if rnd is not None and r.get("round", 1) != rnd:
                continue
            out.setdefault(r["pair"], r[key])
    return out


def rounds_of(rows, config):
    """Sorted distinct replication rounds present for a config (default [1] when records are untagged)."""
    rs = sorted({r.get("round", 1) for r in rows if config_of(r) == config and r.get("pair") is not None})
    return rs or [1]


def _two_sided_p_from_z(z):
    return math.erfc(abs(z) / math.sqrt(2.0))


def _sign_test_p(deltas):
    """Exact two-sided sign test (binomial p=0.5) -- used when n is too small for the normal approx."""
    pos = sum(1 for x in deltas if x > 0)
    neg = sum(1 for x in deltas if x < 0)
    n = pos + neg
    if n == 0:
        return 1.0
    k = min(pos, neg)
    tail = sum(math.comb(n, i) for i in range(k + 1)) / (2.0 ** n)
    return min(1.0, 2.0 * tail)


def wilcoxon_p(deltas):
    """Two-sided p that the paired diffs are symmetric about 0 (Wilcoxon signed-rank, normal approx
    with continuity correction). A rough signal for small n, not an exact test."""
    d = [x for x in deltas if x != 0.0]
    n = len(d)
    if n < 6:
        return _sign_test_p(deltas)
    mags = sorted((abs(x), (1.0 if x > 0 else -1.0)) for x in d)
    ranks = [0.0] * n
    i = 0
    while i < n:  # average ranks within ties
        j = i
        while j + 1 < n and mags[j + 1][0] == mags[i][0]:
            j += 1
        avg = (i + j) / 2.0 + 1.0
        for k in range(i, j + 1):
            ranks[k] = avg
        i = j + 1
    w_signed = sum(sign * ranks[k] for k, (_, sign) in enumerate(mags))
    var = n * (n + 1) * (2 * n + 1) / 6.0
    if var == 0:
        return 1.0
    z = (abs(w_signed) - 1.0) / math.sqrt(var)
    return min(1.0, _two_sided_p_from_z(z))


_BOOT = 2000  # bootstrap resamples for the noise-adjusted effect (fixed seed -> reproducible report)


def adj_delta(deltas):
    """Noise-corrected effect: the raw median per-pair delta SHRUNK toward zero in proportion to its
    own sampling uncertainty (positive-part James-Stein / empirical-Bayes shrinkage). A delta that is
    mostly noise collapses toward 0; a well-resolved one keeps ~its full magnitude. Model-based and
    rough for small n -- read it as an intuition of 'how much really changed', not an exact figure."""
    n = len(deltas)
    if n < 2:
        return 0.0
    med = st.median(deltas)
    if med == 0.0:
        return 0.0
    rng = random.Random(20260730)
    meds = [st.median([deltas[rng.randrange(n)] for _ in range(n)]) for _ in range(_BOOT)]
    se = st.pstdev(meds)  # bootstrap standard error of the median
    if se == 0.0:
        return med
    snr2 = (med / se) ** 2
    return med * max(0.0, 1.0 - 1.0 / snr2)


# Within-config corroboration (partner + sibling coherence) was removed in favour of REPLICATION:
# a verdict now requires the effect to reproduce across rounds (see compute_replicated). config_factors
# is kept only so the sibling / code-path 'family' idea can be reintroduced without re-deriving the axes.
def config_factors(rep):
    """The workload axes that define a config: mode, transport, qos, payload, and paced/open-loop.
    Currently UNUSED by the verdict (pure replication) -- retained as the taxonomy for a future
    sibling/family grouping so configs can be sorted/related without rebuilding this."""
    return {
        "mode": rep.get("mode"),
        "transport": rep.get("transport"),
        "qos": str(rep.get("qos")),
        "payload_bytes": rep.get("payload_bytes"),
        "paced": bool((rep.get("target_rate") or 0) > 0),
    }


def compute_replicated(rows, config, labels):
    """Per-metric REPLICATED verdict for one config. The significance gate (Wilcoxon p<0.05 and |median
    delta| over the floor) runs SEPARATELY per replication round; a metric earns better/WORSE only if it
    fires the SAME direction in EVERY round (reproduced). Fires in some-but-not-all rounds -> '~noise*';
    none or contradictory -> '~noise'. With a single round (no replication) it falls back to
    fire -> verdict, uncorroborated. Each metric dict carries per-round p/direction for display."""
    base, latest = labels[0], labels[-1]
    rep = next((r for r in rows if config_of(r) == config), {})
    info_only = info_metrics(rep)
    rounds = rounds_of(rows, config)
    out = []
    for key, disp, _, better in METRICS:
        info = key in info_only
        floor = PAIRED_FLOOR.get(key, PAIRED_FLOOR_PCT)
        per_round, all_deltas, all_b, all_l = [], [], [], []
        for rnd in rounds:
            bmap = paired_map(rows, config, base, key, rnd)
            lmap = paired_map(rows, config, latest, key, rnd)
            pairs = sorted(set(bmap) & set(lmap))
            all_b += [bmap[p] for p in pairs]
            all_l += [lmap[p] for p in pairs]
            deltas = [(lmap[p] - bmap[p]) / bmap[p] * 100.0 for p in pairs if bmap[p]]
            all_deltas += deltas
            if len(deltas) >= 2 and not info:
                p, med = wilcoxon_p(deltas), st.median(deltas)
                improved = (med > 0) if better == "up" else (med < 0)
                per_round.append({"p": p, "arrow": "↑" if med > 0 else "↓",
                                  "dir": "better" if improved else "WORSE",
                                  "fired": p < 0.05 and abs(med) >= floor})
            else:
                per_round.append(None)
        m = {"key": key, "disp": disp, "info": info, "per_round": per_round,
             "bmed": st.median(all_b) if all_b else None,
             "lmed": st.median(all_l) if all_l else None,
             "raw_med": st.median(all_deltas) if all_deltas else 0.0,
             "verdict": "-", "adj": None}
        fired = [pr for pr in per_round if pr and pr["fired"]]
        dirs = {pr["dir"] for pr in fired}
        if info:
            m["verdict"] = "info"
        elif m["bmed"] is None or all(pr is None for pr in per_round):
            m["verdict"] = "-"
        elif len(rounds) >= 2:
            if len(fired) == len(rounds) and len(dirs) == 1:      # reproduced in every round
                m["verdict"], m["adj"] = next(iter(dirs)), adj_delta(all_deltas)
            elif len(dirs) > 1:                                   # rounds disagree on direction
                m["verdict"] = "~noise"
            elif fired:                                           # fired in some rounds, not all
                m["verdict"] = "~noise*"
            else:
                m["verdict"] = "~noise"
        else:                                                     # single round: no replication
            if fired:
                m["verdict"], m["adj"] = fired[0]["dir"], adj_delta(all_deltas)
            else:
                m["verdict"] = "~noise"
        out.append(m)
    return base, latest, rounds, out


def print_replicated_table(base, latest, rounds, comp, kind=None, qos0_pub=False):
    rnd_hdr = "".join(f"{('rnd' + str(r) + ' p'):>8}" for r in rounds)
    tag = f"×{len(rounds)} rounds" if len(rounds) >= 2 else "1 round"
    print(f"\n  PAIRED A/B  (interleaved {tag}; baseline=[{base}], delta=[{latest}])")
    print(f"  {'metric':<12}{('[' + base + ']'):>12}{('[' + latest + ']'):>12}{'raw Δ%':>8}{rnd_hdr}{'adj Δ%':>7}{'verdict':>10}")
    print(f"  {rule('-', 61 + 8 * len(rounds))}")
    for m in comp:
        if m["bmed"] is None:
            dash = "".join(f"{'-':>8}" for _ in rounds)
            print(f"  {m['disp']:<12}{'-':>12}{'-':>12}{'-':>8}{dash}{'-':>7}{'-':>10}")
            continue
        rnd_cells = ""
        for pr in m["per_round"]:
            if pr is None or m["info"]:
                rnd_cells += f"{'·':>8}"
            else:
                ps = "<.001" if pr["p"] < 0.001 else f"{pr['p']:.3f}"
                rnd_cells += f"{(pr['arrow'] + ps):>8}"
        adj_str = f"{m['adj']:.1f}" if m["adj"] is not None else ("-" if m["info"] else "0.0")
        print(f"  {m['disp']:<12}{fmt_num(m['bmed']):>12}{fmt_num(m['lmed']):>12}{m['raw_med']:>7.1f}%{rnd_cells}{adj_str:>7}{m['verdict']:>10}")
    if kind == "inter-arrival":
        print("    note: inter-arrival = the gap between consecutive deliveries. p50 is intra-burst")
        print("    packing (a read-batch artifact) so it's 'info'; throughput carries the rate and")
        print("    p90/p99 the delivery stalls.")
    if qos0_pub:
        print("    note: QoS 0 publish completes at queue admission (before encode+write), so p50 is")
        print("    admission/queueing time, not send cost -- it's 'info'; read throughput + cpu/msg.")


def print_histogram(rows, config, label, kind):
    selected = [
        r for r in rows if config_of(r) == config and r["label"] == label and "hist_ns" in r
    ]
    if not selected:
        return
    buckets = collections.defaultdict(int)
    for r in selected:
        for upper_ns, count in r["hist_ns"]:
            buckets[upper_ns] += count
    if not buckets:
        return
    uppers = sorted(buckets)
    total = sum(buckets.values())
    peak = max(buckets.values())
    width = 28

    print(f"\n  histogram  [{label}]  {kind}  reps={len(selected)}  samples={total:,}")
    print(f"  {'bucket (us)':>17}  {'count':>10}  {'pct':>6}  {'cum':>6}")
    lower = 0.0
    cum = 0
    for upper in uppers:
        count = buckets[upper]
        cum += count
        bar = "#" * round(count / peak * width) if peak else ""
        upper_us = upper / 1000.0
        print(
            f"  {lower:8.1f}-{upper_us:8.1f}  {count:>10,}  "
            f"{count / total * 100:5.1f}%  {cum / total * 100:5.1f}%  {bar}"
        )
        lower = upper_us


# ---- machine-readable output -----------------------------------------------------------------------
# --json emits the SAME verdicts the tables show -- compute_replicated() stays the single source of
# truth for the statistics, this only serialises what it already returned. Automation must never scrape
# the ASCII tables: those column widths are formatting, and a width change would silently break callers.
JSON_SCHEMA = 1

_METRIC_META = {key: (unit, better) for key, _, unit, better in METRICS}


def unreplicated_configs(paired_comp):
    """Configs whose verdicts come from a single round, i.e. were never replicated.

    The suite is built around ROUNDS>=2: compute_replicated only calls a metric better/WORSE if it
    fires the same direction in EVERY round. With one round there is nothing to reproduce against, so
    it falls back to fire -> verdict -- and that verdict prints identically to a replicated one while
    being far weaker. Measured on the F16 lab: the per-cell false-positive rate is 2.61% at ROUNDS=1
    versus 0.03% at ROUNDS=2, and 28 of 35 otherwise-clean A/A runs would have shown a spurious flag.
    bench-compare.sh defaults to ROUNDS=2; single-round data is off the intended path, so say so
    rather than let the output imply the usual guarantee."""
    return sorted(c for c, (_, _, rounds, _) in paired_comp.items() if len(rounds) < 2)


def json_payload(rows, configs, paired_comp, path):
    """The whole run as one JSON object: per-config/per-metric verdicts plus a family-wise summary.

    `summary.any_flagged` is deliberately top-level because it is the number a user actually
    experiences -- a suite run either shows a non-'~noise' verdict somewhere or it doesn't. On an A/A
    run (identical binary both arms) every flagged cell is a false positive by construction, so the
    rate of any_flagged over repeated A/A runs IS the family-wise false-positive rate. Per-cell counts
    are kept alongside it to show WHICH configs/metrics are the noisy ones."""
    labels = ordered(r["label"] for r in rows)
    prov = {}
    for lbl in labels:
        r = first_with(rows, lbl, "git_sha") or {}
        prov[lbl] = {"git_sha": r.get("git_sha"), "git_dirty": bool(r.get("git_dirty")),
                     "rustc": r.get("rustc"), "host": r.get("host")}
    # Same confounds print_overview warns about: differing toolchain/host means the two labels were
    # not measured by the same instrument.
    confounds = []
    for field, what in (("rustc", "toolchain"), ("host", "host")):
        vals = {prov[l][field] for l in labels if prov[l].get(field) is not None}
        if len(vals) > 1:
            confounds.append({"kind": what, "field": field, "values": sorted(map(str, vals))})

    out_configs, flagged = [], []
    n_gated = n_flagged = n_soft = 0
    for config in configs:
        crows = [r for r in rows if config_of(r) == config]
        clabels = ordered(r["label"] for r in crows)
        desc, kind = config_meta(crows)
        entry = {
            "config": config, "desc": desc, "lat_kind": kind, "labels": clabels,
            "paired": config in paired_comp,
            "reps": {l: sum(1 for r in crows if r["label"] == l) for l in clabels},
            "workload": {k: crows[0].get(k) for k in DRIFT_KEYS},
            "workload_drift": [{"key": k, "values": {l: v for l, v in pl.items()}}
                               for k, pl in workload_drift(rows, config, clabels)],
        }
        if config in paired_comp:
            base, latest, rounds, comp = paired_comp[config]
            entry.update(baseline=base, latest=latest, rounds=list(rounds))
            metrics = []
            for m in comp:
                unit, better = _METRIC_META[m["key"]]
                metrics.append({
                    "key": m["key"], "display": m["disp"], "unit": unit, "better": better,
                    "info": m["info"], "verdict": m["verdict"],
                    "baseline_median": m["bmed"], "latest_median": m["lmed"],
                    "raw_delta_pct": m["raw_med"], "adj_delta_pct": m["adj"],
                    "per_round": [
                        None if pr is None else
                        {"round": rnd, "p": pr["p"], "fired": pr["fired"],
                         "delta_sign": "up" if pr["arrow"] == "↑" else "down", "dir": pr["dir"]}
                        for rnd, pr in zip(rounds, m["per_round"])],
                })
                if not m["info"] and m["verdict"] != "-":     # a gated cell: eligible to fire
                    n_gated += 1
                    if m["verdict"] in ("better", "WORSE"):
                        n_flagged += 1
                        flagged.append({"config": config, "metric": m["key"], "verdict": m["verdict"],
                                        "raw_delta_pct": m["raw_med"], "adj_delta_pct": m["adj"]})
                    elif m["verdict"] == "~noise*":
                        n_soft += 1
            entry["metrics"] = metrics
        out_configs.append(entry)

    return {
        "schema": JSON_SCHEMA,
        "path": path,
        "records": len(rows),
        "labels": labels,
        "provenance": prov,
        "confounds": confounds,
        "seeds": ordered(r["seed"] for r in rows if r.get("seed") is not None),
        "configs": out_configs,
        "summary": {
            "n_configs": len(out_configs),
            "gated_cells": n_gated,          # cells that COULD fire (excludes 'info' and no-data)
            "flagged_cells": n_flagged,      # better/WORSE
            "soft_cells": n_soft,            # '~noise*': fired in some rounds, not all
            "any_flagged": n_flagged > 0,    # the family-wise event
            "flagged": flagged,
            # Verdicts on these configs were never replicated across rounds -- see unreplicated_configs.
            "unreplicated_configs": unreplicated_configs(paired_comp),
            # Any of these means the verdicts above are not trustworthy -- check before counting them.
            "confounded": bool(confounds) or any(c["workload_drift"] for c in out_configs),
        },
    }


def main():
    ap = argparse.ArgumentParser(
        description="Render an iso_bench results.jsonl file for human reading."
    )
    ap.add_argument("path", nargs="?", default="results.jsonl", help="results JSONL file")
    ap.add_argument("--config", help="only this config")
    ap.add_argument("--label", help="only this build label")
    ap.add_argument("--baseline", help="label to treat as the A/B baseline (else first-seen)")
    g = ap.add_mutually_exclusive_group()
    g.add_argument("--no-hist", action="store_true", help="tables only (no histograms)")
    g.add_argument("--hist-only", action="store_true", help="histograms only (no tables)")
    g.add_argument("--json", action="store_true",
                   help="emit verdicts as JSON on stdout instead of tables (for automation)")
    args = ap.parse_args()

    try:
        rows = [json.loads(line) for line in open(args.path) if line.strip()]
    except FileNotFoundError:
        sys.exit(f"no such results file: {args.path}")
    if args.config:
        rows = [r for r in rows if config_of(r) == args.config]
    if args.label:
        rows = [r for r in rows if r.get("label") == args.label]
    if not rows:
        sys.exit("no results for that selection")

    configs = ordered(config_of(r) for r in rows)

    # Interleaved (paired) stats per config -- PURE REPLICATION: a verdict requires the effect to
    # reproduce across rounds (no cross-config coherence; config_factors retained for a future family hook).
    # Computed before any printing so --json can return without emitting a byte of human output.
    paired_comp = {}
    for config in configs:
        crows = [r for r in rows if config_of(r) == config]
        if not has_pairs(crows, config):
            continue
        labels = ordered(r["label"] for r in crows)
        if args.baseline and args.baseline in labels:
            labels = [args.baseline] + [l for l in labels if l != args.baseline]
        paired_comp[config] = compute_replicated(rows, config, labels)

    if args.json:
        json.dump(json_payload(rows, configs, paired_comp, args.path), sys.stdout, indent=2)
        sys.stdout.write("\n")
        return

    if not args.hist_only:
        print_overview(rows, configs)

    # Printed once, before any table, because it qualifies every verdict below it.
    unrep = unreplicated_configs(paired_comp)
    if unrep:
        scope = "all configs" if len(unrep) == len(paired_comp) else f"{len(unrep)} config(s)"
        print(f"\n  NOTE: single round ({scope}) -- verdicts below are NOT replicated. The suite is"
              f"\n        meant to run ROUNDS>=2 (bench-compare.sh default), where a verdict must"
              f"\n        reproduce in every round; measured per-cell false-positive rate is 2.61%"
              f"\n        at one round vs 0.03% at two. Treat these as unconfirmed.")

    any_ab = False
    any_paired = False
    for config in configs:
        crows = [r for r in rows if config_of(r) == config]
        labels = ordered(r["label"] for r in crows)
        if args.baseline and args.baseline in labels:
            labels = [args.baseline] + [l for l in labels if l != args.baseline]
        desc, kind = config_meta(crows)

        print(f"\n{rule()}")
        print(f" config: {config}   ({desc})")
        print(rule())

        if not args.hist_only:
            print_summary_table(rows, config, labels, kind)
            if len(labels) >= 2:
                any_ab = True
                print_drift(rows, config, labels)
                if config in paired_comp:
                    any_paired = True
                    base, latest, rounds, comp = paired_comp[config]
                    rep0 = crows[0]
                    qos0_pub = rep0.get("mode") == "pub-throughput" and rep0.get("qos") in (0, "0")
                    print_replicated_table(base, latest, rounds, comp, kind, qos0_pub)
                else:
                    print_comparison(rows, config, labels)

        if not args.no_hist:
            for label in labels:
                print_histogram(rows, config, label, kind)

    if any_ab and not args.hist_only:
        print(f"\n{rule('-')}")
        print(" Reading A/B: latency_* / cpu_us_per_msg UP = regression; throughput DOWN = regression.")
        if any_paired:
            print(" PAIRED A/B (interleaved, replicated): raw Δ% is the MEDIAN per-pair delta, pooled over")
            print(" rounds; each 'rndN p' is that round's Wilcoxon p with an arrow for direction. A round")
            print(" FIRES when p<0.05 AND |Δ| clears its floor. A metric earns better/WORSE only if it fires")
            print(" the SAME direction in EVERY round (reproduced); some-but-not-all rounds -> '~noise*';")
            print(" none or contradictory -> '~noise'. adj Δ% is the noise-corrected effect (James-Stein),")
            print(" shown only for a reproduced verdict. Tip: a real regression usually moves throughput and")
            print(" cpu/msg together -- read them as a pair. Non-interleaved configs flag deltas over baseline CV.")
        else:
            print(" 'verdict' compares the LATEST label to the baseline and flags deltas larger than the")
            print(" baseline's run-to-run CV (a rough signal, not a formal significance test).")
        print(" 'info' = shown for context, never a verdict (heavy-tailed max; recv inter-arrival p50 and")
        print("          QoS 0 op-latency p50 measure delivery cadence / queue admission, not send cost).")


if __name__ == "__main__":
    main()
