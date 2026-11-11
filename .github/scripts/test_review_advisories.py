# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

from contextlib import ExitStack, redirect_stderr, redirect_stdout
import io
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import review_advisories as review
from report_advisories import ADVISORY_LABEL, REPORT_END, REPORT_START, REVIEW_LABEL


REPOSITORY = "owner/repository"
CONTINUATION = (
    "workflow", "run", "advisory-risk-review.lock.yml", "--repo", REPOSITORY, "--ref", "main",
)


class ReviewAdvisoriesTests(unittest.TestCase):
    def setUp(self):
        self.issues = {}
        self.comments = {}
        self.dispatches = []
        self.dispatch_error = None
        self.stdout = io.StringIO()
        context = ExitStack()
        self.addCleanup(context.close)
        context.enter_context(redirect_stdout(self.stdout))
        context.enter_context(redirect_stderr(io.StringIO()))
        context.enter_context(patch.object(review, "gh", side_effect=self.github))
        context.enter_context(patch.object(
            review, "issue_comments", side_effect=lambda repository, number: self.comments[number],
        ))

    def github(self, *arguments):
        if arguments[0] == "api":
            if arguments[1] == "--method":
                return json.dumps([list(self.issues.values())])
            return json.dumps(self.issues[int(arguments[1].rsplit("/", 1)[1])])
        if arguments[:2] == ("issue", "edit"):
            number = int(arguments[2])
            self.assertEqual(arguments[3:], ("--repo", REPOSITORY, "--remove-label", REVIEW_LABEL))
            self.issues[number]["labels"] = [
                label for label in self.issues[number]["labels"] if label["name"] != REVIEW_LABEL
            ]
            return ""
        if arguments == CONTINUATION:
            self.dispatches.append(arguments)
            if self.dispatch_error:
                raise RuntimeError(self.dispatch_error)
            return ""
        self.fail(f"Unexpected GitHub command: {arguments}")

    def add_finding(self, number, complete=True):
        state = {
            "advisory_key": f"RUSTSEC-2026-{number:04d}",
            "fingerprint": f"{number:064x}",
            "source_revision": "a" * 40,
            "report_complete": complete,
        }
        self.issues[number] = {
            "number": number,
            "state": "open",
            "user": {"login": "github-actions[bot]"},
            "labels": [{"name": ADVISORY_LABEL}, {"name": REVIEW_LABEL}],
            "html_url": f"https://github.com/{REPOSITORY}/issues/{number}",
            "body": (
                f"<!-- dependency-advisory:{state['advisory_key']} -->\n"
                f"{REPORT_START}\n"
                f"<!-- dependency-advisory-review:{json.dumps(state)} -->\n"
                f"Captured scan evidence.\n{REPORT_END}"
            ),
        }
        self.comments[number] = []

    def prepare(self, issue_number=""):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "output"
            review.prepare(REPOSITORY, output, issue_number)
            outputs = dict(line.split("=", 1) for line in output.read_text().splitlines())
        queue = json.loads(outputs["queue"])
        self.assertEqual(outputs["has_reviews"], str(bool(queue)).lower())
        return outputs, queue

    def publish(self, queue):
        for request in queue:
            self.comments[request["issue_number"]].append({
                "user": {"login": "github-actions[bot]"},
                "body": (
                    "**Automated Copilot risk assessment**\n\n"
                    f"```json\n{json.dumps(request)}\n```\n"
                    "<!-- gh-aw-agentic-workflow: workflow_id: advisory-risk-review -->"
                ),
            })

    def test_seven_findings_drain_in_two_batches(self):
        for number in reversed(range(197, 204)):
            self.add_finding(number)
        outputs, queue = self.prepare()
        self.assertEqual([entry["issue_number"] for entry in queue], list(range(197, 202)))
        self.assertEqual(outputs["has_more_reviews"], "true")
        for entry in queue:
            self.assertEqual(
                entry["report_url"], f"https://github.com/{REPOSITORY}/issues/{entry['issue_number']}",
            )
        self.publish(queue)
        review.finalize(REPOSITORY, queue, continuation_ref="main")
        self.assertEqual(self.dispatches, [CONTINUATION])

        outputs, queue = self.prepare()
        self.assertEqual([entry["issue_number"] for entry in queue], [202, 203])
        self.assertEqual(outputs["has_more_reviews"], "false")
        self.publish(queue)
        review.finalize(REPOSITORY, queue)
        self.assertEqual(self.dispatches, [CONTINUATION])
        self.assertTrue(all(
            REVIEW_LABEL not in {label["name"] for label in issue["labels"]}
            for issue in self.issues.values()
        ))
        outputs, queue = self.prepare()
        self.assertEqual(queue, [])
        self.assertEqual(outputs["has_more_reviews"], "false")

    def test_batch_boundaries(self):
        for count in (0, 1, 5, 6, 10, 11):
            with self.subTest(count=count):
                self.issues.clear()
                self.comments.clear()
                for number in range(1, count + 1):
                    self.add_finding(number)
                outputs, queue = self.prepare()
                self.assertEqual(len(queue), min(count, 5))
                self.assertEqual(outputs["has_more_reviews"], str(count > 5).lower())

    def test_ineligible_reports_do_not_count_as_overflow(self):
        for number in range(1, 11):
            self.add_finding(number, complete=number != 6)
        self.issues[7]["body"] = "<!-- dependency-advisory:RUSTSEC-2026-0007 -->"
        self.issues[8]["state"] = "closed"
        self.issues[9]["user"]["login"] = "maintainer"
        self.issues[10]["labels"] = [{"name": ADVISORY_LABEL}]
        outputs, queue = self.prepare()
        self.assertEqual([entry["issue_number"] for entry in queue], list(range(1, 6)))
        self.assertEqual(outputs["has_more_reviews"], "false")

    def test_completed_overflow_is_reused_without_continuation(self):
        for number in range(1, 7):
            self.add_finding(number)
        _, completed = self.prepare("6")
        self.publish(completed)
        outputs, queue = self.prepare()
        self.assertEqual(len(queue), 5)
        self.assertEqual(outputs["has_more_reviews"], "false")
        self.assertNotIn({"name": REVIEW_LABEL}, self.issues[6]["labels"])

    def test_explicit_issue_number_does_not_drain_other_issues(self):
        for number in range(197, 204):
            self.add_finding(number)
        outputs, queue = self.prepare("203")
        self.assertEqual([entry["issue_number"] for entry in queue], [203])
        self.assertEqual(outputs["has_more_reviews"], "false")
        self.publish(queue)
        review.finalize(REPOSITORY, queue)
        self.assertEqual(self.dispatches, [])
        self.assertIn({"name": REVIEW_LABEL}, self.issues[197]["labels"])

    def test_missing_assessment_stops_continuation(self):
        for number in range(1, 7):
            self.add_finding(number)
        _, queue = self.prepare()
        self.publish(queue[:-1])
        with self.assertRaisesRegex(ValueError, "No matching Copilot assessment.*5"):
            review.finalize(REPOSITORY, queue, continuation_ref="main")
        self.assertEqual(self.dispatches, [])
        self.assertIn({"name": REVIEW_LABEL}, self.issues[5]["labels"])

    def test_stale_or_unsafe_assessment_stops_continuation(self):
        for reason in ("stale", "author", "workflow", "threat-detected", "threat-engine-error"):
            with self.subTest(reason=reason):
                self.issues.clear()
                self.comments.clear()
                for number in range(1, 7):
                    self.add_finding(number)
                _, queue = self.prepare()
                self.publish(queue)
                comment = self.comments[5][-1]
                if reason == "stale":
                    comment["body"] = comment["body"].replace(queue[-1]["fingerprint"], "f" * 64)
                elif reason == "author":
                    comment["user"]["login"] = "maintainer"
                elif reason == "workflow":
                    comment["body"] = comment["body"].replace(
                        "workflow_id: advisory-risk-review", "workflow_id: unrelated",
                    )
                else:
                    comment["body"] += f"\n<!-- gh-aw-{reason} -->"
                with self.assertRaisesRegex(ValueError, "No matching Copilot assessment.*5"):
                    review.finalize(REPOSITORY, queue, continuation_ref="main")
                self.assertEqual(self.dispatches, [])
                self.assertIn({"name": REVIEW_LABEL}, self.issues[5]["labels"])

    def test_changed_finding_does_not_continue_automatically(self):
        for number in range(1, 7):
            self.add_finding(number)
        _, queue = self.prepare()
        self.publish(queue)
        self.issues[5]["body"] = self.issues[5]["body"].replace(queue[-1]["fingerprint"], "f" * 64)
        review.finalize(REPOSITORY, queue, continuation_ref="main")
        self.assertEqual(self.dispatches, [])
        self.assertIn({"name": REVIEW_LABEL}, self.issues[5]["labels"])

    def test_dispatch_failure_is_reported(self):
        for number in range(1, 7):
            self.add_finding(number)
        _, queue = self.prepare()
        self.publish(queue)
        self.dispatch_error = "Workflow dispatch was denied"
        with self.assertRaisesRegex(RuntimeError, self.dispatch_error):
            review.finalize(REPOSITORY, queue, continuation_ref="main")
        _, remaining = self.prepare()
        self.assertEqual([entry["issue_number"] for entry in remaining], [6])

    def test_empty_batch_never_dispatches(self):
        review.finalize(REPOSITORY, [], continuation_ref="main")
        self.assertEqual(self.dispatches, [])

    def test_main_passes_continuation_ref(self):
        for ref in ("", "main"):
            with self.subTest(ref=ref), patch.object(review, "finalize") as finalize:
                with patch.dict(os.environ, {
                    "REVIEW_QUEUE": "[]", "REVIEW_CONTINUATION_REF": ref,
                }), patch("sys.argv", [
                    "review_advisories.py", "finalize", "--repository", REPOSITORY,
                ]):
                    self.assertEqual(review.main(), 0)
                finalize.assert_called_once_with(REPOSITORY, [], continuation_ref=ref)


if __name__ == "__main__":
    unittest.main()
