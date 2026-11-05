# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import copy
import io
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from urllib.error import HTTPError

import issue_to_ado as sync


PARENT_ID = 34702979
ISSUE_URL = "https://github.com/microsoft/rust-mqtt-client/issues/200"


def event(action="opened", issue_type="Bug"):
    return {
        "action": action,
        "repository": {"name": "rust-mqtt-client", "full_name": "microsoft/rust-mqtt-client"},
        "issue": {
            "number": 200, "html_url": ISSUE_URL, "title": "Original title",
            "body": "Original description", "state": "open", "type": {"name": issue_type},
        },
    }


def bug(state="New", parent_id=PARENT_ID):
    relations = [{"rel": "Hyperlink", "url": ISSUE_URL}]
    if parent_id is not None:
        relations.append({
            "rel": sync.PARENT_RELATION,
            "url": f"https://dev.azure.com/msazure/_apis/wit/workItems/{parent_id}",
        })
    return {
        "id": 123, "rev": 4,
        "fields": {
            "System.TeamProject": "One", "System.WorkItemType": "Bug", "System.State": state,
        },
        "relations": relations,
    }


class FakeAzureDevOps:
    organization = "msazure"
    project = "One"

    def __init__(self, item=None):
        self.item = item
        self.matches = [{"id": item["id"]}] if item is not None else []
        self.calls = []
        self.parent = {
            "fields": {"System.TeamProject": "One", "System.AreaPath": "One\\SDKs\\MqttClient"},
        }

    def request(self, method, path, document=None):
        self.calls.append((method, path, copy.deepcopy(document)))
        if method == "POST" and path == "wiql":
            return {"workItems": self.matches}
        if method == "GET" and path == f"workitems/{PARENT_ID}":
            return copy.deepcopy(self.parent)
        if method == "GET" and path == "workitems/123?$expand=relations":
            return copy.deepcopy(self.item)
        if method == "PATCH" and path == "workitems/123?$expand=relations":
            if document[0] != {"op": "test", "path": "/rev", "value": self.item["rev"]}:
                raise AssertionError("Parent updates must use optimistic concurrency")
            for operation in document[1:]:
                if operation["path"] == "/relations/-":
                    self.item["relations"].append(operation["value"])
                elif operation["path"] == "/fields/System.State":
                    self.item["fields"]["System.State"] = operation["value"]
                else:
                    raise AssertionError(f"Unexpected mutation: {operation}")
            self.item["rev"] += 1
            return copy.deepcopy(self.item)
        raise AssertionError(f"Unexpected request: {method} {path}")


class IssueSyncTests(unittest.TestCase):
    def setUp(self):
        self.latest = event()["issue"]
        self.latest["title"] = "Current title"
        self.latest["body"] = "Current description"
        self.load = patch("issue_to_ado.load_issue", return_value=self.latest).start()
        self.addCleanup(patch.stopall)

    def test_new_bug_uses_parent_area_without_rewriting_the_event(self):
        source = event()
        prepared = sync.prepare(FakeAzureDevOps(), source, PARENT_ID)
        self.assertEqual(source, event())
        self.assertEqual(prepared, {
            "existing_id": None, "parent_id": PARENT_ID, "area_path": "One\\SDKs\\MqttClient",
        })
        self.load.assert_not_called()

    def test_only_new_native_bugs_are_eligible_for_creation(self):
        for issue_type in ("Task", "Feature", None):
            with self.subTest(issue_type=issue_type):
                source = event(issue_type=issue_type)
                source["issue"]["labels"] = [{"name": "bug"}]
                self.assertIsNone(sync.prepare(FakeAzureDevOps(), source, PARENT_ID))
        source = event()
        source["issue"]["type"] = None
        self.assertIsNone(sync.prepare(FakeAzureDevOps(), source, PARENT_ID))
        self.load.assert_not_called()

    def test_historical_edits_and_closures_do_not_create_items(self):
        for action in ("edited", "closed"):
            with self.subTest(action=action):
                ado = FakeAzureDevOps()
                self.assertIsNone(sync.prepare(ado, event(action), PARENT_ID))
                self.assertEqual([call[0] for call in ado.calls], ["POST"])
        self.load.assert_not_called()

    def test_unsupported_events_and_pull_requests_fail(self):
        for action in ("reopened", "typed", "created", "assigned"):
            with self.subTest(action=action):
                with self.assertRaisesRegex(ValueError, "Unsupported"):
                    sync.prepare(FakeAzureDevOps(), event(action), PARENT_ID)
        source = event()
        source["issue"]["pull_request"] = {}
        with self.assertRaisesRegex(ValueError, "Pull requests"):
            sync.prepare(FakeAzureDevOps(), source, PARENT_ID)

    def test_ambiguous_matches_fail_before_mutation(self):
        ado = FakeAzureDevOps(bug())
        ado.matches.append({"id": 456})
        with self.assertRaisesRegex(RuntimeError, "Multiple ADO"):
            sync.prepare(ado, event(), PARENT_ID)
        self.load.assert_not_called()

    def test_unrelated_matches_fail_before_mutation(self):
        for wrong_field, value in (("System.TeamProject", "Other"), ("System.WorkItemType", "Task")):
            with self.subTest(field=wrong_field):
                item = bug()
                item["fields"][wrong_field] = value
                with self.assertRaisesRegex(RuntimeError, "not a Bug linked"):
                    sync.prepare(FakeAzureDevOps(item), event(), PARENT_ID)
        item = bug()
        item["relations"][0]["url"] = "https://github.com/other/repo/issues/200"
        with self.assertRaisesRegex(RuntimeError, "not a Bug linked"):
            sync.prepare(FakeAzureDevOps(item), event(), PARENT_ID)

    def test_wrong_parent_fails_instead_of_reparenting(self):
        ado = FakeAzureDevOps(bug(parent_id=999))
        with self.assertRaisesRegex(RuntimeError, "refusing to reparent"):
            sync.prepare(ado, event("edited"), PARENT_ID)
        self.load.assert_not_called()

    def test_parent_project_and_area_are_validated(self):
        ado = FakeAzureDevOps()
        ado.parent["fields"]["System.TeamProject"] = "Other"
        with self.assertRaisesRegex(RuntimeError, "different project"):
            sync.prepare(ado, event(), PARENT_ID)
        for area in ("", "One\nbad", "One\rbad", None):
            with self.subTest(area=area):
                ado = FakeAzureDevOps()
                ado.parent["fields"]["System.AreaPath"] = area
                with self.assertRaisesRegex(ValueError, "valid area path"):
                    sync.prepare(ado, event(), PARENT_ID)

    def test_invalid_issue_numbers_fail(self):
        for number in (-1, 0, "200", True):
            with self.subTest(number=number):
                source = event()
                source["issue"]["number"] = number
                with self.assertRaisesRegex(ValueError, "positive integer"):
                    sync.prepare(FakeAzureDevOps(), source, PARENT_ID)

    def test_creation_retry_and_edits_preserve_manual_state(self):
        for action in ("opened", "edited"):
            with self.subTest(action=action):
                ado = FakeAzureDevOps(bug(state="Active"))
                source = event(action)
                prepared = sync.prepare(ado, source, PARENT_ID)
                self.assertEqual(sync.complete(ado, source, prepared), 123)
                self.assertEqual(ado.item["fields"]["System.State"], "Active")
                self.assertFalse(any(call[0] == "PATCH" for call in ado.calls))

    def test_closed_event_preserves_action_and_requires_done(self):
        ado = FakeAzureDevOps(bug(state="Active"))
        source = event("closed")
        prepared = sync.prepare(ado, source, PARENT_ID)
        with self.assertRaisesRegex(RuntimeError, "transition.*Done"):
            sync.complete(ado, source, prepared)
        ado.item["fields"]["System.State"] = "Done"
        self.assertEqual(sync.complete(ado, source, prepared), 123)

    def test_issue_closed_during_initial_creation_finishes_done(self):
        self.latest["state"] = "closed"
        ado = FakeAzureDevOps()
        prepared = sync.prepare(ado, event(), PARENT_ID)
        ado.item = bug(parent_id=None)
        ado.matches = [{"id": 123}]
        self.assertEqual(sync.complete(ado, event(), prepared), 123)
        self.assertEqual(ado.item["fields"]["System.State"], "Done")
        self.assertTrue(sync.has_parent(ado.item, PARENT_ID))

    def test_new_bug_gets_parent_and_new_state(self):
        ado = FakeAzureDevOps()
        prepared = sync.prepare(ado, event(), PARENT_ID)
        ado.item = bug(state="Approved", parent_id=None)
        ado.matches = [{"id": 123}]
        self.assertEqual(sync.complete(ado, event(), prepared), 123)
        self.assertTrue(sync.has_parent(ado.item, PARENT_ID))
        self.assertEqual(ado.item["fields"]["System.State"], "New")
        mutations = [call for call in ado.calls if call[0] == "PATCH"]
        self.assertEqual(len(mutations), 1)
        self.assertEqual(
            [operation["path"] for operation in mutations[0][2]],
            ["/rev", "/relations/-", "/fields/System.State"],
        )

    def test_retry_can_recover_missing_parent_without_resetting_state(self):
        ado = FakeAzureDevOps(bug(state="Active", parent_id=None))
        prepared = sync.prepare(ado, event(), PARENT_ID)
        self.assertEqual(sync.complete(ado, event(), prepared), 123)
        self.assertEqual(ado.item["fields"]["System.State"], "Active")
        self.assertTrue(sync.has_parent(ado.item, PARENT_ID))

    def test_missing_upstream_result_and_changed_identity_fail(self):
        ado = FakeAzureDevOps()
        prepared = sync.prepare(ado, event(), PARENT_ID)
        with self.assertRaisesRegex(RuntimeError, "did not create"):
            sync.complete(ado, event(), prepared)
        prepared["existing_id"] = 456
        ado.item = bug()
        ado.matches = [{"id": 123}]
        with self.assertRaisesRegex(RuntimeError, "different ADO Bug"):
            sync.complete(ado, event(), prepared)

    def test_mismatched_refreshed_issue_fails(self):
        ado = FakeAzureDevOps()
        prepared = sync.prepare(ado, event(), PARENT_ID)
        ado.item = bug(parent_id=None)
        ado.matches = [{"id": 123}]
        self.latest["number"] = 201
        with self.assertRaisesRegex(ValueError, "does not match"):
            sync.complete(ado, event(), prepared)

    def test_http_request_uses_bearer_auth_patch_type_and_api_version(self):
        ado = sync.AzureDevOps("msazure", "Project name", "test-token")
        with patch("issue_to_ado.urlopen") as send:
            send.return_value.__enter__.return_value = io.StringIO('{"id": 123}')
            result = ado.request("PATCH", "workitems/123?$expand=relations", [])
        request = send.call_args.args[0]
        self.assertEqual(result["id"], 123)
        self.assertEqual(request.headers["Authorization"], "Bearer test-token")
        self.assertEqual(request.headers["Content-type"], "application/json-patch+json")
        self.assertEqual(
            request.full_url,
            "https://dev.azure.com/msazure/Project%20name/_apis/wit/"
            "workitems/123?$expand=relations&api-version=7.1",
        )
        self.assertEqual(json.loads(request.data), [])
        self.assertEqual(send.call_args.kwargs["timeout"], 30)

    def test_http_errors_are_not_treated_as_missing_items(self):
        ado = sync.AzureDevOps("msazure", "One", "test-token")
        for status in (401, 403, 429, 500):
            with self.subTest(status=status):
                error = HTTPError("https://dev.azure.com", status, "Failure", {}, None)
                with patch("issue_to_ado.urlopen", side_effect=error):
                    with self.assertRaisesRegex(RuntimeError, f"HTTP {status}"):
                        ado.request("POST", "wiql", {"query": "SELECT [System.Id] FROM WorkItems"})

    def test_cli_passes_separate_context_without_overriding_the_webhook(self):
        ado = FakeAzureDevOps()
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "github-event.json"
            source.write_text(json.dumps(event()))
            output = root / "output"
            summary = root / "summary"
            environment = {
                "ADO_ORGANIZATION": "msazure", "ADO_PROJECT": "One",
                "ADO_TOKEN": "test-token", "ADO_PARENT_ID": str(PARENT_ID),
                "GITHUB_EVENT_PATH": str(source), "GITHUB_OUTPUT": str(output),
                "GITHUB_STEP_SUMMARY": str(summary), "RUNNER_TEMP": str(root),
            }
            with patch.dict(os.environ, environment), patch(
                "issue_to_ado.AzureDevOps", return_value=ado,
            ):
                with patch("sys.argv", ["issue_to_ado.py", "prepare"]):
                    self.assertEqual(sync.main(), 0)
                outputs = dict(line.split("=", 1) for line in output.read_text().splitlines())
                self.assertEqual(outputs["sync"], "true")
                self.assertEqual(outputs["area_path"], "One\\SDKs\\MqttClient")
                self.assertNotIn("event_path", outputs)
                self.assertEqual(json.loads(source.read_text()), event())
                context = Path(outputs["context_path"])
                self.assertEqual(set(json.loads(context.read_text())), {
                    "existing_id", "parent_id", "area_path",
                })

                ado.item = bug(parent_id=None)
                ado.matches = [{"id": 123}]
                with patch.dict(os.environ, {"ADO_SYNC_CONTEXT": str(context)}), patch(
                    "sys.argv", ["issue_to_ado.py", "complete"],
                ):
                    self.assertEqual(sync.main(), 0)
                self.assertTrue(sync.has_parent(ado.item, PARENT_ID))
                self.assertIn(
                    "https://dev.azure.com/msazure/One/_workitems/edit/123", summary.read_text(),
                )

    def test_cli_missing_credentials_fails_explicitly(self):
        with patch.dict(os.environ, {
            "ADO_ORGANIZATION": "msazure", "ADO_PROJECT": "One",
        }, clear=True), patch("sys.argv", ["issue_to_ado.py", "prepare"]), patch(
            "sys.stderr", new_callable=io.StringIO,
        ) as errors:
            self.assertEqual(sync.main(), 1)
        self.assertIn("::error::", errors.getvalue())
        self.assertIn("ADO_TOKEN", errors.getvalue())


if __name__ == "__main__":
    unittest.main()
