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


def print_drift(rows, config, labels):
    """Warn if the workload definition drifted across labels. Returns True if drift was found."""
    drifted = []
    for key in DRIFT_KEYS:
        per_label = {}
        for lbl in labels:
            vals = [r[key] for r in rows if config_of(r) == config and r["label"] == lbl and key in r]
            if vals:
                per_label[lbl] = vals[0]
        if len({str(v) for v in per_label.values()}) > 1:
            drifted.append((key, per_label))
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
    for label in labels:
        n = sum(1 for r in rows if config_of(r) == config and r["label"] == label)
        print(f"\n  [{label}]  {n} reps   (latency rows = {kind})")
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


def print_comparison(rows, config, labels):
    base, latest = labels[0], labels[-1]
    print(f"\n  A/B comparison  (median; baseline=[{base}], delta=[{latest}])")
    header = "".join(f"{('[' + l + ']'):>14}" for l in labels)
    print(f"  {'metric':<12}{header}{'delta%':>10}{'verdict':>11}")
    print(f"  {rule('-', 12 + 14 * len(labels) + 21)}")
    rep = next((r for r in rows if config_of(r) == config), {})
    # Metrics that must NOT be gated as pass/fail for this config (shown as 'info'):
    #   lat_max            -- a single worst sample per rep, far too heavy-tailed to judge.
    #   QoS 0 pub tput/p50 -- no wire-completion signal, so these time queue admission +
    #                         scheduler interleaving, not send cost (read cpu/msg + p99 instead).
    info_only = {"lat_max"}
    if rep.get("mode") == "pub-throughput" and rep.get("qos") in (0, "0"):
        info_only |= {"msgs_per_s", "mib_per_s", "lat_p50"}
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


def paired_map(rows, config, label, key):
    """pair index -> metric value, for one (config, label). First value wins per pair."""
    out = {}
    for r in rows:
        if config_of(r) == config and r.get("label") == label and r.get("pair") is not None and r.get(key) is not None:
            out.setdefault(r["pair"], r[key])
    return out


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


# Within-config corroboration partners: metrics that a REAL regression moves together, so a lone flag
# is suspect. Latency percentiles co-move (a shifted distribution lifts several); throughput and
# cpu/msg are inversely linked (fewer msg/s <-> more us/msg). msgs<->mib are the same signal so they
# do NOT corroborate each other; max_rss has no within-config partner (sibling-only).
WITHIN_PARTNERS = {
    "lat_p50": {"lat_p90", "lat_p99"},
    "lat_p90": {"lat_p50", "lat_p99"},
    "lat_p99": {"lat_p50", "lat_p90"},
    "msgs_per_s": {"cpu_us_per_msg"},
    "mib_per_s": {"cpu_us_per_msg"},
    "cpu_us_per_msg": {"msgs_per_s", "mib_per_s"},
}


def config_factors(rep):
    """The workload axes of a config EXCEPT transport -- two configs with equal factors and different
    transport are a transport-contrast pair (see suite.sh matrix)."""
    return (
        rep.get("mode"),
        str(rep.get("qos")),
        rep.get("payload_bytes"),
        1 if (rep.get("target_rate") or 0) > 0 else 0,  # paced / open-loop
    )


def compute_paired(rows, config, labels):
    """Per-metric paired stats for one config (no printing). `fired` = passes the raw significance gate
    (Wilcoxon p<0.05 and |median delta| over the floor); `direction` is better/WORSE. Corroboration
    (coherence) decides later whether a fired flag becomes a verdict or a soft 'watch'."""
    base, latest = labels[0], labels[-1]
    rep = next((r for r in rows if config_of(r) == config), {})
    # Not gated (shown as 'info'): lat_max is a single worst sample; inter-arrival p50 is recv-throughput
    # reader cadence (tracks throughput); QoS 0 pub-throughput tput/p50 measure queue admission, not send cost.
    info_only = {"lat_max"}
    if rep.get("lat_kind") == "inter-arrival":
        info_only |= {"lat_p50"}
    if rep.get("mode") == "pub-throughput" and rep.get("qos") in (0, "0"):
        info_only |= {"msgs_per_s", "mib_per_s", "lat_p50"}
    out = []
    n_pairs = 0
    for key, disp, _, better in METRICS:
        bmap = paired_map(rows, config, base, key)
        lmap = paired_map(rows, config, latest, key)
        pairs = sorted(set(bmap) & set(lmap))
        n_pairs = max(n_pairs, len(pairs))
        m = {"key": key, "disp": disp, "better": better, "info": key in info_only,
             "bmed": None, "lmed": None, "med_d": 0.0, "deltas": [], "p": None,
             "fired": False, "direction": None}
        if len(pairs) >= 2:
            m["bmed"] = st.median([bmap[p] for p in pairs])
            m["lmed"] = st.median([lmap[p] for p in pairs])
            deltas = [(lmap[p] - bmap[p]) / bmap[p] * 100.0 for p in pairs if bmap[p]]
            m["deltas"] = deltas
            m["med_d"] = st.median(deltas) if deltas else 0.0
            if not m["info"] and deltas:
                p = wilcoxon_p(deltas)
                m["p"] = p
                if p < 0.05 and abs(m["med_d"]) >= PAIRED_FLOOR.get(key, PAIRED_FLOOR_PCT):
                    improved = (m["med_d"] > 0) if better == "up" else (m["med_d"] < 0)
                    m["fired"] = True
                    m["direction"] = "better" if improved else "WORSE"
        out.append(m)
    return base, latest, n_pairs, out


def coherence(comp_by_config, reps_by_config):
    """Confirm each fired flag by CORROBORATION -- a real regression is coherent, a chance flag is
    isolated. A flag is confirmed if the SAME metric moves the same way in the config's transport-
    contrast sibling, OR a within-config partner metric (WITHIN_PARTNERS) fires the same direction.
    Uncorroborated flags are downgraded to 'watch'. Returns {config: set(confirmed metric keys)}."""
    fired = {c: {m["key"]: m["direction"] for m in comp if m["fired"]} for c, comp in comp_by_config.items()}
    factors = {c: config_factors(reps_by_config[c]) for c in comp_by_config}
    transport = {c: reps_by_config[c].get("transport") for c in comp_by_config}
    confirmed = {}
    for c, comp in comp_by_config.items():
        sibs = [o for o in comp_by_config if o != c and factors[o] == factors[c] and transport[o] != transport[c]]
        conf = set()
        for m in comp:
            if not m["fired"]:
                continue
            k, d = m["key"], m["direction"]
            within = any(fired[c].get(p) == d for p in WITHIN_PARTNERS.get(k, ()))
            cross = any(fired[s].get(k) == d for s in sibs)
            if within or cross:
                conf.add(k)
        confirmed[c] = conf
    return confirmed


def print_paired_table(base, latest, n_pairs, comp, confirmed):
    print(f"\n  PAIRED A/B  (interleaved; baseline=[{base}], delta=[{latest}], {n_pairs} pairs)")
    print(f"  {'metric':<12}{('[' + base + ']'):>14}{('[' + latest + ']'):>14}{'raw Δ%':>10}{'p':>8}{'adj Δ%':>8}{'verdict':>11}")
    print(f"  {rule('-', 12 + 28 + 10 + 8 + 8 + 11)}")
    for m in comp:
        if m["bmed"] is None:
            print(f"  {m['disp']:<12}{'-':>14}{'-':>14}{'-':>10}{'-':>8}{'-':>8}{'-':>11}")
            continue
        cells = f"{fmt_num(m['bmed']):>14}{fmt_num(m['lmed']):>14}"
        if m["info"]:
            verdict, p_str, adj_str = "info", "-", "-"
        elif m["p"] is None:
            verdict, p_str, adj_str = "-", "-", "-"
        elif m["fired"] and m["key"] in confirmed:
            p_str = "<.001" if m["p"] < 0.001 else f"{m['p']:.3f}"
            verdict = m["direction"]
            adj_str = f"{adj_delta(m['deltas']):.1f}"
        else:
            p_str = "<.001" if m["p"] < 0.001 else f"{m['p']:.3f}"
            # significant but isolated (no sibling/partner) is almost always chance -> noise, marked '*'
            verdict, adj_str = ("~noise*" if m["fired"] else "~noise"), "0.0"
        print(f"  {m['disp']:<12}{cells}{m['med_d']:>9.1f}%{p_str:>8}{adj_str:>8}{verdict:>11}")


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

    if not args.hist_only:
        print_overview(rows, configs)

    # Paired stats are computed for ALL configs first so the coherence pass can corroborate a flag
    # against its transport-contrast sibling (a cross-config check) before any verdict is printed.
    paired_comp, reps_by_config = {}, {}
    for config in configs:
        crows = [r for r in rows if config_of(r) == config]
        if not has_pairs(crows, config):
            continue
        labels = ordered(r["label"] for r in crows)
        if args.baseline and args.baseline in labels:
            labels = [args.baseline] + [l for l in labels if l != args.baseline]
        paired_comp[config] = compute_paired(rows, config, labels)
        reps_by_config[config] = crows[0]
    confirmed_map = coherence({c: paired_comp[c][3] for c in paired_comp}, reps_by_config)

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
                    base, latest, n_pairs, comp = paired_comp[config]
                    print_paired_table(base, latest, n_pairs, comp, confirmed_map[config])
                else:
                    print_comparison(rows, config, labels)

        if not args.no_hist:
            for label in labels:
                print_histogram(rows, config, label, kind)

    if any_ab and not args.hist_only:
        print(f"\n{rule('-')}")
        print(" Reading A/B: latency_* / cpu_us_per_msg UP = regression; throughput DOWN = regression.")
        if any_paired:
            print(" PAIRED A/B (interleaved): raw Δ% is the MEDIAN per-pair delta; p is the Wilcoxon")
            print(" signed-rank p-value; adj Δ% is the noise-corrected change -- 0.0 when indistinguishable")
            print(" from noise, else the raw delta shrunk toward zero by its residual jitter (James-Stein);")
            print(" read it as 'the real change is ~this %'. A metric passes the gate when p<0.05 AND")
            print(" |raw Δ%| >= its floor; the verdict is then better/WORSE only if COHERENT -- corroborated")
            print(" by its transport-contrast sibling or a within-config partner metric (co-moving latency")
            print(" percentiles, throughput<->cpu/msg). A significant but ISOLATED flag (nothing corroborates")
            print(" it) is marked '~noise*' -- treated as noise, since a lone metric moving with no support is")
            print(" almost always chance; re-run or find a coherent pattern to promote it. adj Δ% is 0.0")
            print(" unless coherent. Non-interleaved configs flag deltas larger than baseline CV.")
        else:
            print(" 'verdict' compares the LATEST label to the baseline and flags deltas larger than the")
            print(" baseline's run-to-run CV (a rough signal, not a formal significance test).")
        print(" 'info' = shown for context, never a verdict (heavy-tailed max; QoS 0 throughput/p50")
        print("          measure queue admission, not send cost -- read cpu/msg + p99 there).")


if __name__ == "__main__":
    main()
