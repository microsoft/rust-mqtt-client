# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import json
from pathlib import Path
import tempfile
import unittest

from report_advisories import collect_scans, validate_ignored_advisories


FIRST = "RUSTSEC-2026-0001"
SECOND = "RUSTSEC-2026-0002"


class IgnoredAdvisoriesTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.reports = Path(directory.name)

    def write_scan(self, resolution, unused=(), ignored=()):
        directory = self.reports / f"dependency-advisories-{resolution}"
        directory.mkdir()
        diagnostics = [
            {
                "type": "diagnostic",
                "fields": {
                    "code": code,
                    "severity": severity,
                    "message": message,
                    "graphs": [],
                    "labels": [
                        {"span": advisory, "message": message, "line": 1, "column": 1},
                        {"span": "Reviewed exception", "message": "ignore reason"},
                    ],
                },
            }
            for advisories, code, severity, message in (
                (unused, "advisory-not-detected", "warning", "advisory was not encountered"),
                (ignored, "advisory-ignored", "note", "advisory ignored"),
            )
            for advisory in advisories
        ]
        diagnostics.append({
            "type": "summary",
            "fields": {"advisories": {"errors": 0, "warnings": len(unused), "notes": len(ignored)}},
        })
        (directory / "diagnostics.jsonl").write_text(
            "".join(json.dumps(event) + "\n" for event in diagnostics)
        )
        (directory / "advisories.jsonl").write_text(json.dumps({
            "settings": {"ignore": sorted(set(unused) | set(ignored))},
            "vulnerabilities": [
                {
                    "advisory": {"id": advisory, "package": "example"},
                    "package": {"name": "example", "version": "1.0.0"},
                    "affected": None,
                    "versions": {"patched": [">=1.1.0"], "unaffected": []},
                }
                for advisory in ignored
            ],
            "warnings": {},
        }) + "\n")
        (directory / "exit-code.txt").write_text("0\n")
        return directory

    def test_rejects_ignore_unused_by_both_resolutions(self):
        self.write_scan("latest", unused=(SECOND, FIRST))
        self.write_scan("minimal", unused=(FIRST, SECOND))
        with self.assertRaisesRegex(
            ValueError, f"Remove unused deny.toml advisory ignores.*{FIRST}, {SECOND}",
        ):
            validate_ignored_advisories(self.reports)

    def test_preserves_ignore_needed_by_either_resolution(self):
        for needed in ("latest", "minimal", "both"):
            with self.subTest(needed=needed):
                root = self.reports / needed
                root.mkdir()
                original = self.reports
                self.reports = root
                for resolution in ("latest", "minimal"):
                    if needed in (resolution, "both"):
                        self.write_scan(resolution, ignored=(FIRST,))
                    else:
                        self.write_scan(resolution, unused=(FIRST,))
                validate_ignored_advisories(root)
                self.assertEqual(collect_scans(root, "success"), ({}, []))
                self.reports = original

    def test_rejects_only_the_shared_unused_ignores(self):
        self.write_scan("latest", unused=(FIRST, SECOND))
        self.write_scan("minimal", unused=(FIRST,), ignored=(SECOND,))
        with self.assertRaises(ValueError) as error:
            validate_ignored_advisories(self.reports)
        self.assertIn(FIRST, str(error.exception))
        self.assertNotIn(SECOND, str(error.exception))

    def test_accepts_no_ignores(self):
        self.write_scan("latest")
        self.write_scan("minimal")
        validate_ignored_advisories(self.reports)

    def test_configuration_warnings_do_not_affect_advisory_count(self):
        self.write_scan("latest")
        directory = self.write_scan("minimal")
        path = directory / "diagnostics.jsonl"
        warnings = [
            {
                "type": "diagnostic",
                "fields": {"severity": "warning", "message": "duplicate items detected"},
            },
            {
                "type": "diagnostic",
                "fields": {
                    "severity": "warning",
                    "code": "deprecated",
                    "message": "deprecated configuration field",
                },
            },
        ]
        path.write_text(
            "".join(json.dumps(event) + "\n" for event in warnings) + path.read_text()
        )
        validate_ignored_advisories(self.reports)

    def test_missing_scan_does_not_pass_when_other_has_no_unused_ignores(self):
        self.write_scan("latest")
        with self.assertRaises(FileNotFoundError):
            validate_ignored_advisories(self.reports)

    def test_incomplete_scan_does_not_pass(self):
        self.write_scan("latest")
        directory = self.write_scan("minimal", unused=(FIRST,))
        path = directory / "diagnostics.jsonl"
        path.write_text(path.read_text().splitlines()[0] + "\n")
        with self.assertRaisesRegex(ValueError, "scanner completion summary"):
            validate_ignored_advisories(self.reports)

    def test_missing_unused_warning_does_not_pass(self):
        self.write_scan("latest")
        directory = self.write_scan("minimal", unused=(FIRST,))
        path = directory / "diagnostics.jsonl"
        path.write_text(path.read_text().splitlines()[-1] + "\n")
        with self.assertRaisesRegex(ValueError, "warning summary and diagnostics disagree"):
            validate_ignored_advisories(self.reports)

    def test_configuration_warning_cannot_hide_missing_advisory_warning(self):
        self.write_scan("latest")
        directory = self.write_scan("minimal", unused=(FIRST,))
        path = directory / "diagnostics.jsonl"
        warning = {
            "type": "diagnostic",
            "fields": {"severity": "warning", "message": "duplicate items detected"},
        }
        path.write_text(
            json.dumps(warning) + "\n" + path.read_text().splitlines()[-1] + "\n"
        )
        with self.assertRaisesRegex(ValueError, "warning summary and diagnostics disagree"):
            validate_ignored_advisories(self.reports)

    def test_invalid_unused_advisory_does_not_pass(self):
        self.write_scan("latest", unused=("not-an-advisory",))
        self.write_scan("minimal")
        with self.assertRaisesRegex(ValueError, "Invalid unused advisory identifier"):
            validate_ignored_advisories(self.reports)


if __name__ == "__main__":
    unittest.main()
