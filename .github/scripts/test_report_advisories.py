# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import copy
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import report_advisories as reporter


REPOSITORY = "microsoft/rust-mqtt-client"
RUN_URL = f"https://github.com/{REPOSITORY}/actions/runs/123"


def diagnostic(code="unsound", severity="error", version="1.38.2"):
    return {
        "type": "diagnostic",
        "fields": {
            "code": code,
            "severity": severity,
            "message": "AsyncFd constructors violate I/O safety",
            "advisory": {
                "id": "RUSTSEC-2026-0386",
                "package": "tokio",
                "title": "AsyncFd constructors violate I/O safety",
                "description": "Safe constructors can accept an invalid file descriptor.",
                "date": "2026-10-13",
                "informational": code,
                "aliases": ["GHSA-example"],
                "cvss": None,
                "references": ["https://github.com/tokio-rs/tokio/issues/8539"],
                "url": "https://github.com/tokio-rs/tokio/pull/8587",
            },
            "graphs": [{
                "Krate": {"name": "tokio", "version": version},
                "parents": [
                    {"Krate": {"name": "ms-mqtt-client", "version": "0.1.0"}},
                    {
                        "Krate": {"name": "tokio-openssl", "version": "0.6.0"},
                        "parents": [{
                            "Krate": {"name": "ms-mqtt-client", "version": "0.1.0"},
                            "repeat": True,
                        }],
                    },
                ],
            }],
            "notes": ["ID: RUSTSEC-2026-0386", "Solution: Upgrade to >=1.53.3"],
        },
    }


class ReportTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)

    def write_scan(self, resolution, diagnostics=(), status=None):
        directory = self.root / f"dependency-advisories-{resolution}"
        directory.mkdir(exist_ok=True)
        errors = sum(item["fields"]["severity"] == "error" for item in diagnostics)
        events = [*diagnostics, {
            "type": "summary", "fields": {"advisories": {"errors": errors}},
        }]
        (directory / "diagnostics.jsonl").write_text(
            "\n".join(json.dumps(item) for item in events) + "\n"
        )
        (directory / "exit-code.txt").write_text(str(int(errors > 0) if status is None else status))
        return directory

    def test_clean_scans_do_not_call_github(self):
        for resolution in reporter.RESOLUTIONS:
            self.write_scan(resolution)
        with patch.object(reporter, "gh") as gh:
            status = reporter.report(
                self.root, REPOSITORY, RUN_URL, "success", self.root / "summary.md"
            )
        self.assertEqual(status, 0)
        gh.assert_not_called()
        self.assertIn("Both dependency resolutions completed", (self.root / "summary.md").read_text())

    def test_same_advisory_in_both_resolutions_is_one_detailed_issue(self):
        self.write_scan("latest", [diagnostic(version="1.53.2")])
        self.write_scan("minimal", [diagnostic()])
        findings, failures = reporter.collect_scans(self.root, "failure")
        issues = reporter.render_issues(findings, failures, RUN_URL)
        self.assertEqual(len(issues), 1)
        self.assertEqual(issues[0].key, "RUSTSEC-2026-0386")
        body = reporter.issue_body(issues[0], REPOSITORY)
        for expected in (
            "https://rustsec.org/advisories/RUSTSEC-2026-0386",
            "https://github.com/RustSec/advisory-db",
            "https://github.com/tokio-rs/tokio/pull/8587",
            "https://github.com/tokio-rs/tokio/issues/8539",
            "GHSA-example", "2026-10-13", "Not provided by the advisory",
            "latest: `tokio 1.53.2`", "minimal: `tokio 1.38.2`",
            "<- tokio-openssl v0.6.0", "ms-mqtt-client v0.1.0 (already shown)",
            "Safe constructors", "Solution: Upgrade to >=1.53.3", RUN_URL,
            "not confirmation", "Maintainer assessment", "SECURITY.md",
        ):
            with self.subTest(expected=expected):
                self.assertIn(expected, body)

    def test_each_advisory_classification_is_reported(self):
        for code in sorted(reporter.ADVISORY_CODES):
            with self.subTest(code=code):
                directory = self.write_scan("minimal", [diagnostic(code=code)])
                self.assertEqual(len(reporter.read_scan(directory)), 1)

    def test_multiple_advisories_and_versions_are_not_lost(self):
        first = diagnostic()
        second = diagnostic(version="1.53.2")
        third = diagnostic()
        third["fields"]["advisory"]["id"] = "RUSTSEC-2026-9999"
        self.write_scan("latest", [first, second, third])
        self.write_scan("minimal")
        findings, failures = reporter.collect_scans(self.root, "failure")
        self.assertFalse(failures)
        self.assertEqual(len(findings), 2)
        self.assertEqual(len(findings["RUSTSEC-2026-0386"]), 2)

    def test_reviewed_note_does_not_create_issue(self):
        self.write_scan("latest")
        self.write_scan("minimal", [diagnostic(severity="note")])
        findings, failures = reporter.collect_scans(self.root, "success")
        self.assertEqual(findings, {})
        self.assertEqual(failures, [])

    def test_yanked_version_has_registry_source_not_fake_advisory(self):
        event = diagnostic(code="yanked")
        del event["fields"]["advisory"]
        del event["fields"]["notes"]
        event["fields"]["message"] = "detected yanked crate"
        self.write_scan("latest", [event])
        self.write_scan("minimal")
        findings, failures = reporter.collect_scans(self.root, "failure")
        issue = reporter.render_issues(findings, failures, RUN_URL)[0]
        self.assertEqual(issue.key, "yanked:tokio:1.38.2")
        self.assertIn("https://crates.io/crates/tokio/1.38.2", issue.report)
        self.assertIn("not itself a RustSec advisory", issue.report)

    def test_dependency_paths_distinguish_build_and_dev_dependencies(self):
        fields = diagnostic()["fields"]
        fields["graphs"][0]["parents"][0]["Krate"]["kind"] = "dev"
        fields["graphs"][0]["parents"][1]["Krate"]["kind"] = "build"
        issue = reporter.render_finding("RUSTSEC-2026-0386", [("minimal", fields)], RUN_URL)
        self.assertIn("ms-mqtt-client v0.1.0 (dev dependency)", issue.report)
        self.assertIn("tokio-openssl v0.6.0 (build dependency)", issue.report)

    def test_missing_matrix_leg_is_an_infrastructure_issue(self):
        self.write_scan("latest")
        findings, failures = reporter.collect_scans(self.root, "failure")
        issues = reporter.render_issues(findings, failures, RUN_URL)
        self.assertEqual(len(issues), 1)
        self.assertEqual(issues[0].key, "scan-failure")
        self.assertIn("minimal:", issues[0].report)
        self.assertIn("not a clean", issues[0].report)

    def test_malformed_and_incomplete_reports_fail_explicitly(self):
        cases = {
            "invalid JSON": "{broken",
            "no summary": json.dumps(diagnostic()),
            "unknown structure": json.dumps({"type": "summary", "fields": {}}),
        }
        for name, content in cases.items():
            with self.subTest(name=name):
                self.write_scan("latest")
                directory = self.write_scan("minimal")
                (directory / "diagnostics.jsonl").write_text(content)
                findings, failures = reporter.collect_scans(self.root, "failure")
                self.assertEqual(findings, {})
                self.assertEqual(len(failures), 1)

    def test_invalid_exit_status_is_not_a_clean_scan(self):
        for status in (1, 2, 101):
            with self.subTest(status=status):
                directory = self.write_scan("minimal", status=status)
                with self.assertRaises(ValueError):
                    reporter.read_scan(directory)

    def test_success_status_cannot_hide_error_diagnostic(self):
        directory = self.write_scan("minimal", [diagnostic()], status=0)
        with self.assertRaisesRegex(ValueError, "disagree"):
            reporter.read_scan(directory)

    def test_unsupported_error_diagnostic_is_infrastructure_failure(self):
        directory = self.write_scan("minimal", [diagnostic(code="index-failure")])
        with self.assertRaisesRegex(ValueError, "index-failure"):
            reporter.read_scan(directory)

    def test_fatal_error_log_is_not_a_clean_scan(self):
        directory = self.write_scan("minimal")
        with (directory / "diagnostics.jsonl").open("a") as output:
            output.write(json.dumps({
                "type": "log", "fields": {"level": "ERROR", "message": "database fetch failed"},
            }) + "\n")
        with self.assertRaisesRegex(ValueError, "database fetch failed"):
            reporter.read_scan(directory)

    def test_scanner_bug_or_unknown_event_cannot_report_success(self):
        for event in (
            {"type": "diagnostic", "fields": {"severity": "bug", "message": "internal error"}},
            {"type": "unexpected", "fields": {}},
        ):
            with self.subTest(event=event):
                directory = self.write_scan("minimal")
                with (directory / "diagnostics.jsonl").open("a") as output:
                    output.write(json.dumps(event) + "\n")
                with self.assertRaises(ValueError):
                    reporter.read_scan(directory)

    def test_failed_job_with_clean_reports_is_infrastructure_failure(self):
        for resolution in reporter.RESOLUTIONS:
            self.write_scan(resolution)
        findings, failures = reporter.collect_scans(self.root, "failure")
        self.assertFalse(findings)
        self.assertIn("despite clean reports", failures[0])

    def test_advisory_and_incomplete_scan_both_get_reported(self):
        self.write_scan("minimal", [diagnostic()])
        findings, failures = reporter.collect_scans(self.root, "failure")
        issues = reporter.render_issues(findings, failures, RUN_URL)
        self.assertEqual({issue.key for issue in issues}, {"RUSTSEC-2026-0386", "scan-failure"})

    def test_invalid_advisory_or_graph_is_not_silently_accepted(self):
        for mutate in (
            lambda fields: fields["advisory"].update(id="unexpected-id"),
            lambda fields: fields["advisory"].update(package="other-crate"),
            lambda fields: fields.update(graphs=[]),
            lambda fields: fields["graphs"][0].update(parents=[{}]),
            lambda fields: fields.update(notes=None),
            lambda fields: fields["advisory"].update(references=None),
        ):
            event = diagnostic()
            mutate(event["fields"])
            self.write_scan("latest")
            self.write_scan("minimal", [event])
            findings, failures = reporter.collect_scans(self.root, "failure")
            self.assertFalse(findings)
            self.assertTrue(failures)

    def test_unhandled_findings_fail_after_publishing(self):
        self.write_scan("latest")
        self.write_scan("minimal", [diagnostic()])
        with patch.object(reporter, "publish_issues") as publish:
            status = reporter.report(
                self.root, REPOSITORY, RUN_URL, "failure", self.root / "summary.md"
            )
        self.assertEqual(status, 1)
        self.assertEqual(len(publish.call_args.args[0]), 1)
        self.assertIn("Unhandled dependency findings: **1**", (self.root / "summary.md").read_text())

    def test_large_description_is_explicitly_truncated(self):
        event = diagnostic()
        event["fields"]["advisory"]["description"] = "a" * 15000
        issue = reporter.render_finding("RUSTSEC-2026-0386", [("minimal", event["fields"])], RUN_URL)
        self.assertIn("[Truncated;", issue.report)
        self.assertIn("full scan artifact", issue.report)

    def test_large_graphs_stay_within_github_issue_body_limit(self):
        fields = diagnostic()["fields"]
        fields["graphs"][0]["parents"] *= 1000
        occurrences = [(resolution, fields) for resolution in reporter.RESOLUTIONS] * 4
        issue = reporter.render_finding("RUSTSEC-2026-0386", occurrences, RUN_URL)
        body = reporter.issue_body(issue, REPOSITORY)
        self.assertLess(len(body), 65536)
        self.assertIn("[Truncated;", body)
        self.assertEqual(sum(line.startswith("```") for line in body.splitlines()) % 2, 0)

    def test_truncation_closes_upstream_markdown_code_fences(self):
        for fence in ("```", "````", "~~~"):
            with self.subTest(fence=fence):
                text = reporter.bounded(f"{fence}text\n" + "a" * 15000)
                self.assertIn(f"\n{fence}\n[Truncated;", text)


class GitHubTests(unittest.TestCase):
    def setUp(self):
        self.issue = reporter.render_finding(
            "RUSTSEC-2026-0386", [("minimal", diagnostic()["fields"])], RUN_URL
        )
        self.current = {
            "number": 173, "state": "open",
            "body": reporter.issue_body(self.issue, REPOSITORY),
            "html_url": f"https://github.com/{REPOSITORY}/issues/173",
        }

    def test_new_issue_is_created_once_with_labels_and_template(self):
        with patch.object(reporter, "gh", side_effect=["[[]]", "https://example.test/issue\n"]) as gh:
            reporter.publish_issues([self.issue], REPOSITORY)
        self.assertEqual(gh.call_count, 2)
        self.assertIn("--paginate", gh.call_args_list[0].args)
        self.assertIn("--slurp", gh.call_args_list[0].args)
        self.assertEqual(gh.call_args.args[:2], ("issue", "create"))
        self.assertIn("dependencies", gh.call_args.args)
        self.assertIn("infrastructure", gh.call_args.args)
        self.assertIn("Maintainer assessment", gh.call_args.kwargs["body"])

    def test_repeat_updates_only_generated_block_without_daily_comments(self):
        self.current["body"] += "\nHuman assessment: transitive callers were checked.\n"
        updated = copy.deepcopy(self.issue)
        updated.report = updated.report.replace("/runs/123", "/runs/456")
        with patch.object(reporter, "gh", side_effect=[
            json.dumps([[self.current]]), "updated",
        ]) as gh:
            reporter.publish_issues([updated], REPOSITORY)
        self.assertEqual(gh.call_count, 2)
        self.assertEqual(gh.call_args.args[:2], ("issue", "edit"))
        body = gh.call_args.kwargs["body"]
        self.assertIn("Human assessment: transitive callers were checked.", body)
        self.assertIn("/runs/456", body)
        self.assertNotIn("/runs/123", body)

    def test_identical_rerun_does_not_edit_or_comment(self):
        with patch.object(reporter, "gh", return_value=json.dumps([[self.current]])) as gh:
            reporter.publish_issues([self.issue], REPOSITORY)
        gh.assert_called_once()

    def test_recurring_closed_issue_is_reopened_not_duplicated(self):
        self.current["state"] = "closed"
        with patch.object(reporter, "gh", side_effect=[
            json.dumps([[self.current]]), "reopened",
        ]) as gh:
            reporter.publish_issues([self.issue], REPOSITORY)
        self.assertEqual(gh.call_args.args[:3], ("issue", "reopen", "173"))

    def test_pull_requests_are_not_treated_as_issues(self):
        self.current["pull_request"] = {}
        with patch.object(reporter, "gh", side_effect=[
            json.dumps([[self.current]]), "created",
        ]) as gh:
            reporter.publish_issues([self.issue], REPOSITORY)
        self.assertEqual(gh.call_args.args[:2], ("issue", "create"))

    def test_duplicate_markers_fail_instead_of_choosing_an_issue(self):
        with patch.object(reporter, "gh", return_value=json.dumps([
            [self.current], [self.current],
        ])):
            with self.assertRaisesRegex(ValueError, "Multiple tracking"):
                reporter.publish_issues([self.issue], REPOSITORY)

    def test_missing_managed_markers_do_not_overwrite_human_content(self):
        self.current["body"] = self.current["body"].replace(reporter.REPORT_END, "")
        with patch.object(reporter, "gh", return_value=json.dumps([[self.current]])) as gh:
            with self.assertRaisesRegex(ValueError, "invalid report markers"):
                reporter.publish_issues([self.issue], REPOSITORY)
        gh.assert_called_once()

    def test_reversed_managed_markers_do_not_overwrite_human_content(self):
        marker = "<!-- dependency-advisory:RUSTSEC-2026-0386 -->"
        self.current["body"] = marker + reporter.REPORT_END + reporter.REPORT_START
        with patch.object(reporter, "gh", return_value=json.dumps([[self.current]])) as gh:
            with self.assertRaisesRegex(ValueError, "reversed report markers"):
                reporter.publish_issues([self.issue], REPOSITORY)
        gh.assert_called_once()

    def test_github_errors_are_explicit(self):
        result = reporter.subprocess.CompletedProcess(["gh"], 1, "", "permission denied")
        with patch.object(reporter.subprocess, "run", return_value=result):
            with self.assertRaisesRegex(RuntimeError, "permission denied"):
                reporter.gh("issue", "create")


if __name__ == "__main__":
    unittest.main()
