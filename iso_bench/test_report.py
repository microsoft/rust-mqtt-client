#!/usr/bin/env python3
# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
#
# Requirement-based tests for report.py's verdict engine. Each test asserts ONE documented behaviour
# (the test name is the requirement), driving compute_replicated() with synthetic per-pair deltas and
# checking the structured verdict -- NOT the printed text, so formatting changes don't break these.
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


class TestInfoMetrics(unittest.TestCase):
    """Which metrics are shown as 'info' (context only, never gated) per config type."""

    def test_lat_max_is_always_info(self):
        self.assertIn("lat_max", report.info_metrics({"mode": "pub-latency", "qos": 1}))

    def test_inter_arrival_p50_is_info(self):
        self.assertIn("lat_p50", report.info_metrics({"mode": "recv-throughput", "lat_kind": "inter-arrival"}))

    def test_qos0_pub_p50_is_info(self):
        self.assertIn("lat_p50", report.info_metrics({"mode": "pub-throughput", "qos": 0}))

    def test_qos0_pub_throughput_is_gated_not_info(self):
        # Regression guard: QoS 0 throughput tracks the real send rate (bounded queue), so it must gate.
        info = report.info_metrics({"mode": "pub-throughput", "qos": 0})
        self.assertNotIn("msgs_per_s", info)
        self.assertNotIn("mib_per_s", info)


class TestReplicationVerdicts(unittest.TestCase):
    """A verdict requires the effect to reproduce across every round."""

    def test_reproduced_regression_is_WORSE(self):
        rows = make_rows("c", [{"lat_p99": steady(2.0)}, {"lat_p99": steady(2.0)}])
        self.assertEqual(verdict_of(rows, "c", "lat_p99"), "WORSE")

    def test_reproduced_improvement_is_better(self):
        rows = make_rows("c", [{"lat_p99": steady(-2.0)}, {"lat_p99": steady(-2.0)}])
        self.assertEqual(verdict_of(rows, "c", "lat_p99"), "better")

    def test_throughput_drop_is_WORSE(self):
        rows = make_rows("c", [{"msgs_per_s": steady(-2.0)}, {"msgs_per_s": steady(-2.0)}])
        self.assertEqual(verdict_of(rows, "c", "msgs_per_s"), "WORSE")

    def test_fires_one_round_only_is_noise_star(self):
        rows = make_rows("c", [{"lat_p99": steady(2.0)}, {}])  # round 2 flat
        self.assertEqual(verdict_of(rows, "c", "lat_p99"), "~noise*")

    def test_opposite_directions_is_noise(self):
        rows = make_rows("c", [{"lat_p99": steady(2.0)}, {"lat_p99": steady(-2.0)}])
        self.assertEqual(verdict_of(rows, "c", "lat_p99"), "~noise")

    def test_neither_round_fires_is_noise(self):
        rows = make_rows("c", [{"lat_p99": scatter()}, {"lat_p99": scatter()}])
        self.assertEqual(verdict_of(rows, "c", "lat_p99"), "~noise")

    def test_reproduced_verdict_carries_adj(self):
        rows = make_rows("c", [{"lat_p99": steady(2.0)}, {"lat_p99": steady(2.0)}])
        _, _, _, out = report.compute_replicated(rows, "c", ["main", "branch"])
        self.assertIsNotNone(next(m["adj"] for m in out if m["key"] == "lat_p99"))


class TestFloors(unittest.TestCase):
    """A significant-but-tiny move below the metric's floor must not fire."""

    def test_below_default_floor_is_noise(self):
        rows = make_rows("c", [{"cpu_us_per_msg": steady(0.3)}, {"cpu_us_per_msg": steady(0.3)}])
        self.assertEqual(verdict_of(rows, "c", "cpu_us_per_msg"), "~noise")

    def test_max_rss_below_2pct_floor_is_noise(self):
        rows = make_rows("c", [{"max_rss_kb": steady(1.0)}, {"max_rss_kb": steady(1.0)}])
        self.assertEqual(verdict_of(rows, "c", "max_rss_kb"), "~noise")

    def test_max_rss_above_2pct_floor_fires(self):
        rows = make_rows("c", [{"max_rss_kb": steady(3.0)}, {"max_rss_kb": steady(3.0)}])
        self.assertEqual(verdict_of(rows, "c", "max_rss_kb"), "WORSE")


class TestSingleRoundFallback(unittest.TestCase):
    """Untagged (single-pass) data falls back to fire -> verdict, with no replication check."""

    def test_single_round_fires_to_verdict(self):
        rows = make_rows("c", [{"cpu_us_per_msg": steady(2.0)}], tag_round=False)
        self.assertEqual(verdict_of(rows, "c", "cpu_us_per_msg"), "WORSE")


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


if __name__ == "__main__":
    unittest.main()
