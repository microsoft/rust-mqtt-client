#!/usr/bin/env python3
# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
#
# Requirement-based tests for report.py's verdict engine. Each asserts documented behaviour by driving
# compute_replicated() with synthetic per-pair deltas and checking the STRUCTURED verdict -- not the
# printed text, so formatting changes don't break these. Tests are parameterised over the behavioural
# axes: config type -> info set; metric -> (floor, direction); round pattern -> verdict class.
#
# Run:  python3 -m unittest test_report.py        (from iso_bench/)
#   or: python3 test_report.py
import json
import os
import subprocess
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import report  # noqa: E402

ALL_METRICS = {m[0] for m in report.METRICS}   # every metric report.py knows about
# Always-'info', for two different reasons: lat_max is a single worst sample (p90/p99 carry the tail),
# and mib_per_s is msgs_per_s times a per-config constant, so gating it tested the same measurement
# twice -- 96 of 97 throughput findings in the corpus fired as a duplicate pair.
ALWAYS_INFO = {"lat_max", "mib_per_s"}
GATED_METRICS = ALL_METRICS - ALWAYS_INFO

# Per gated metric: (floor %, "up"|"down" = which direction is BETTER). Hard-coded independently of
# report.py (deriving it from report.py would assert nothing); the single source for the floor and
# direction axes below. Kept in sync with report.py by TestMetricCoverage.
METRIC_SPEC = {
    "msgs_per_s":     (1.0, "up"),
    "lat_p50":        (1.0, "down"),
    "lat_p90":        (1.0, "down"),
    "lat_p99":        (1.0, "down"),
    "cpu_us_per_msg": (1.0, "down"),
    "max_rss_kb":     (2.0, "down"),
}

# One representative base value per metric; only the deltas below matter to the verdict.
BASE = {"lat_p50": 80.0, "lat_p90": 95.0, "lat_p99": 130.0, "lat_max": 8000.0,
        "cpu_us_per_msg": 30.0, "max_rss_kb": 14000.0, "msgs_per_s": 60000.0, "mib_per_s": 900.0}


def steady(v, n=10):
    """n per-pair deltas (%), all the same sign near v -> Wilcoxon-significant with median ~v."""
    return [v + 0.02 * (i - n // 2) for i in range(n)]


def scatter(n=10):
    """Balanced +/- deltas -> median 0, not significant (pure noise)."""
    return [0.3 if i % 2 == 0 else -0.3 for i in range(n)]


def make_rows(config, per_round, *, mode="pub-latency", transport="tcp", qos=1,
              payload=64, target_rate=0, lat_kind="op latency", tag_round=True):
    """Build synthetic paired rows. `per_round` is a list (one entry per round) of {metric: [d% per
    pair]}; the branch value is base*(1+d/100) so compute_replicated sees exactly those deltas."""
    rows = []
    for ridx, deltas in enumerate(per_round, start=1):
        n = max((len(v) for v in deltas.values()), default=10)
        for pair in range(1, n + 1):
            for label, is_latest in (("main", False), ("branch", True)):
                r = {"label": label, "config": config, "mode": mode, "transport": transport,
                     "qos": qos, "payload_bytes": payload, "target_rate": target_rate,
                     "lat_kind": lat_kind, "pair": pair, "rep": pair,
                     "git_sha": "x", "rustc": "1.88.0", "host": "h"}
                if tag_round:
                    r["round"] = ridx
                for k, b in BASE.items():
                    d = deltas.get(k, [0.0] * n)[pair - 1] if is_latest else 0.0
                    r[k] = b * (1 + d / 100.0)
                rows.append(r)
    return rows


def verdict_of(rows, config, key):
    _, _, _, out = report.compute_replicated(rows, config, ["main", "branch"])
    return next(m["verdict"] for m in out if m["key"] == key)


def reproduce(key, delta):
    """Two rounds that both move `key` by ~delta% (the common 'reproduced' shape)."""
    return make_rows("c", [{key: steady(delta)}, {key: steady(delta)}])


class TestMetricCoverage(unittest.TestCase):
    """Guard: the per-metric test tables stay in sync with report.py's metric set."""

    def test_tables_cover_every_metric(self):
        # If report.py adds a metric (or changes what's always-info), update METRIC_SPEC + BASE.
        self.assertEqual(set(METRIC_SPEC), GATED_METRICS)
        self.assertEqual(set(BASE), ALL_METRICS)


class TestInfoMetrics(unittest.TestCase):
    """`info_metrics` returns EXACTLY the carve-outs for a config; every other metric is gated."""

    # (config rep, expected info set). Equality => everything not listed is gated.
    CASES = [
        ({"mode": "pub-latency",     "qos": 1, "lat_kind": "op latency"},       ALWAYS_INFO),
        ({"mode": "pub-throughput",  "qos": 1, "lat_kind": "op latency"},       ALWAYS_INFO),
        ({"mode": "recv-latency",    "qos": 0, "lat_kind": "delivery latency"}, ALWAYS_INFO),
        ({"mode": "recv-throughput", "qos": 0, "lat_kind": "inter-arrival"},    ALWAYS_INFO | {"lat_p50"}),
        ({"mode": "pub-throughput",  "qos": 0, "lat_kind": "op latency"},       ALWAYS_INFO | {"lat_p50"}),
    ]

    def test_info_set_per_config(self):
        for rep, expected in self.CASES:
            with self.subTest(mode=rep["mode"], qos=rep["qos"], kind=rep["lat_kind"]):
                self.assertLessEqual(expected, ALL_METRICS)  # test names only real metrics
                self.assertEqual(report.info_metrics(rep), expected)


class TestReplicationVerdicts(unittest.TestCase):
    """Two axes: round pattern -> verdict class, and each metric's better/WORSE direction."""

    # Round pattern -> verdict. This logic lives in compute_replicated (not per metric), so one
    # representative gated metric exercises it: lat_p99 is 'down'-is-better, so an UP move reads WORSE.
    ROUND_PATTERNS = [
        ("reproduced",     steady(2.0), steady(2.0),  "WORSE"),
        ("one round only", steady(2.0), None,         "~noise*"),
        ("opposite dirs",  steady(2.0), steady(-2.0), "~noise"),
        ("neither fires",  scatter(),   scatter(),    "~noise"),
    ]

    def test_round_pattern_to_verdict(self):
        for label, r1, r2, expected in self.ROUND_PATTERNS:
            with self.subTest(pattern=label):
                per_round = [{"lat_p99": r1}, {} if r2 is None else {"lat_p99": r2}]
                self.assertEqual(verdict_of(make_rows("c", per_round), "c", "lat_p99"), expected)

    def test_direction_convention(self):
        # A reproduced move in each metric's BAD direction is WORSE; the GOOD direction is better.
        # 3% clears every floor (max_rss's is 2%). Catches a flipped `better` field or a sign bug.
        for key, (_, better) in METRIC_SPEC.items():
            good = 3.0 if better == "up" else -3.0
            for delta, expected in ((good, "better"), (-good, "WORSE")):
                with self.subTest(metric=key, expected=expected):
                    self.assertEqual(verdict_of(reproduce(key, delta), "c", key), expected)

    def test_reproduced_verdict_carries_adj(self):
        _, _, _, out = report.compute_replicated(reproduce("lat_p99", 2.0), "c", ["main", "branch"])
        self.assertIsNotNone(next(m["adj"] for m in out if m["key"] == "lat_p99"))


class TestFloors(unittest.TestCase):
    """Each gated metric fires only when the reproduced move clears ITS floor (max_rss: 2%, else 0.5%)."""

    def test_below_floor_is_noise(self):
        for key, (floor, _) in METRIC_SPEC.items():
            with self.subTest(metric=key):  # significant but under the floor -> ~noise
                self.assertEqual(verdict_of(reproduce(key, floor * 0.6), "c", key), "~noise")

    def test_above_floor_fires(self):
        for key, (floor, _) in METRIC_SPEC.items():
            with self.subTest(metric=key):  # over the floor and reproduced -> a real verdict
                self.assertIn(verdict_of(reproduce(key, floor * 1.6), "c", key), ("better", "WORSE"))


class TestSingleRoundFallback(unittest.TestCase):
    """Untagged (single-pass) data: fire -> verdict (no replication check); flat -> ~noise."""

    def test_single_round_fires_to_verdict(self):
        rows = make_rows("c", [{"cpu_us_per_msg": steady(2.0)}], tag_round=False)
        self.assertEqual(verdict_of(rows, "c", "cpu_us_per_msg"), "WORSE")

    def test_single_round_flat_is_noise(self):
        rows = make_rows("c", [{"cpu_us_per_msg": scatter()}], tag_round=False)
        self.assertEqual(verdict_of(rows, "c", "cpu_us_per_msg"), "~noise")


class TestEndToEnd(unittest.TestCase):
    """report.py renders a two-round file without crashing (guards the whole pipeline)."""

    def test_report_renders(self):
        rows = make_rows("recv-tput-tls", [{"cpu_us_per_msg": steady(2.0)}, {"cpu_us_per_msg": steady(2.0)}],
                         mode="recv-throughput", transport="tls", qos=0, payload=16384, lat_kind="inter-arrival")
        with tempfile.NamedTemporaryFile("w", suffix=".jsonl", delete=False) as f:
            for r in rows:
                f.write(json.dumps(r) + "\n")
            path = f.name
        try:
            here = os.path.dirname(os.path.abspath(__file__))
            out = subprocess.run([sys.executable, os.path.join(here, "report.py"), path,
                                  "--baseline", "main", "--no-hist"], capture_output=True, text=True)
            self.assertEqual(out.returncode, 0, out.stderr)
            self.assertIn("PAIRED A/B", out.stdout)
            self.assertIn("WORSE", out.stdout)
        finally:
            os.unlink(path)


class TestJsonOutput(unittest.TestCase):
    """--json is what automation reads, so it must carry the SAME verdicts the tables show and must
    never regress into needing a scraper. (The tables genuinely cannot be scraped: 'max rss' contains
    a space, and msgs_per_s/mib_per_s BOTH render as 'throughput' -- indistinguishable by name.)"""

    # One clean config, one with a reproduced throughput+cpu regression, one that fires in round 1 only.
    def _payload(self, extra_args=()):
        rows = make_rows("clean-tcp", [{}, {}])
        reg = {"cpu_us_per_msg": steady(6.0), "msgs_per_s": steady(-5.0)}
        rows += make_rows("regressed-tcp", [reg, reg], mode="pub-throughput")
        rows += make_rows("flaky-tls", [{"lat_p99": steady(4.0)}, {}], transport="tls")
        with tempfile.NamedTemporaryFile("w", suffix=".jsonl", delete=False) as f:
            for r in rows:
                f.write(json.dumps(r) + "\n")
            path = f.name
        try:
            here = os.path.dirname(os.path.abspath(__file__))
            out = subprocess.run([sys.executable, os.path.join(here, "report.py"), path,
                                  "--baseline", "main", "--json", *extra_args],
                                 capture_output=True, text=True)
            self.assertEqual(out.returncode, 0, out.stderr)
            return rows, json.loads(out.stdout)   # parses => stdout was pure JSON, no human output
        finally:
            os.unlink(path)

    def test_verdicts_match_compute_replicated(self):
        """The serialiser must not re-derive anything -- every cell equals compute_replicated()."""
        rows, doc = self._payload()
        for c in doc["configs"]:
            _, _, _, out = report.compute_replicated(rows, c["config"], ["main", "branch"])
            self.assertEqual([m["verdict"] for m in c["metrics"]], [m["verdict"] for m in out],
                             f"{c['config']}: json verdicts diverged from compute_replicated")
            self.assertEqual([m["key"] for m in c["metrics"]], [m["key"] for m in out])

    def test_ambiguous_display_names_are_distinguishable(self):
        rows, doc = self._payload()
        reg = next(c for c in doc["configs"] if c["config"] == "regressed-tcp")
        by_key = {m["key"]: m for m in reg["metrics"]}
        self.assertEqual(by_key["msgs_per_s"]["verdict"], "WORSE")
        # Same display name ("throughput"), so the JSON must still separate them by key -- but
        # mib_per_s is no longer a verdict of its own. In real data it CANNOT disagree with
        # msgs_per_s; this fixture gives it a different delta only to prove the keys don't collide.
        self.assertEqual(by_key["mib_per_s"]["verdict"], "info")
        self.assertEqual(by_key["cpu_us_per_msg"]["verdict"], "WORSE")

    def test_family_wise_summary(self):
        """summary is the FP/FN ledger's input: gated cells, flagged cells, and the family-wise event."""
        _, doc = self._payload()
        s = doc["summary"]
        self.assertEqual(s["gated_cells"], 3 * len(GATED_METRICS))   # every metric gated in these configs
        self.assertEqual(s["flagged_cells"], 2)                      # msgs_per_s + cpu_us_per_msg
        self.assertEqual(s["soft_cells"], 1)                         # lat_p99 fired one round -> ~noise*
        self.assertTrue(s["any_flagged"])
        self.assertFalse(s["confounded"])
        self.assertEqual({(f["config"], f["metric"]) for f in s["flagged"]},
                         {("regressed-tcp", "msgs_per_s"), ("regressed-tcp", "cpu_us_per_msg")})

    def test_clean_run_reports_no_flags(self):
        """The A/A shape: identical arms must yield any_flagged=False, or the FP rate is meaningless."""
        rows = make_rows("aa-tcp", [{}, {}])
        with tempfile.NamedTemporaryFile("w", suffix=".jsonl", delete=False) as f:
            for r in rows:
                f.write(json.dumps(r) + "\n")
            path = f.name
        try:
            here = os.path.dirname(os.path.abspath(__file__))
            out = subprocess.run([sys.executable, os.path.join(here, "report.py"), path,
                                  "--baseline", "main", "--json"], capture_output=True, text=True)
            doc = json.loads(out.stdout)
            self.assertFalse(doc["summary"]["any_flagged"])
            self.assertEqual(doc["summary"]["flagged_cells"], 0)
        finally:
            os.unlink(path)

    def test_confound_is_surfaced(self):
        """A toolchain mismatch invalidates the comparison -- automation must see it without parsing text."""
        rows = make_rows("c", [{}, {}])
        for r in rows:
            if r["label"] == "branch":
                r["rustc"] = "1.99.0"
        with tempfile.NamedTemporaryFile("w", suffix=".jsonl", delete=False) as f:
            for r in rows:
                f.write(json.dumps(r) + "\n")
            path = f.name
        try:
            here = os.path.dirname(os.path.abspath(__file__))
            out = subprocess.run([sys.executable, os.path.join(here, "report.py"), path,
                                  "--baseline", "main", "--json"], capture_output=True, text=True)
            doc = json.loads(out.stdout)
            self.assertTrue(doc["summary"]["confounded"])
            self.assertEqual([c["kind"] for c in doc["confounds"]], ["toolchain"])
        finally:
            os.unlink(path)


class TestUnreplicatedNotice(unittest.TestCase):
    """A single-round run still emits verdicts, and they LOOK identical to replicated ones. The
    per-cell false-positive rate is ~90x worse there (2.61% vs 0.03%), so both output paths must
    say so -- the tables in prose, the JSON as a field automation can branch on."""

    def _run(self, per_round, *extra_args):
        rows = make_rows("c-tcp", per_round, tag_round=len(per_round) > 1)
        with tempfile.NamedTemporaryFile("w", suffix=".jsonl", delete=False) as f:
            for r in rows:
                f.write(json.dumps(r) + "\n")
            path = f.name
        try:
            here = os.path.dirname(os.path.abspath(__file__))
            out = subprocess.run([sys.executable, os.path.join(here, "report.py"), path,
                                  "--baseline", "main", *extra_args], capture_output=True, text=True)
            self.assertEqual(out.returncode, 0, out.stderr)
            return out.stdout
        finally:
            os.unlink(path)

    def test_single_round_warns_in_tables(self):
        self.assertIn("NOTE: single round", self._run([{"cpu_us_per_msg": steady(6.0)}]))

    def test_two_rounds_do_not_warn(self):
        reg = {"cpu_us_per_msg": steady(6.0)}
        self.assertNotIn("NOTE: single round", self._run([reg, reg]))

    def test_json_carries_the_same_fact(self):
        reg = {"cpu_us_per_msg": steady(6.0)}
        one = json.loads(self._run([reg], "--json"))
        two = json.loads(self._run([reg, reg], "--json"))
        self.assertEqual(one["summary"]["unreplicated_configs"], ["c-tcp"])
        self.assertEqual(two["summary"]["unreplicated_configs"], [])


if __name__ == "__main__":
    unittest.main()
