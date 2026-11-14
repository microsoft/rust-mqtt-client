# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import copy
import io
import json
import os
from pathlib import Path
import unittest
from unittest.mock import patch
from urllib.error import HTTPError

import issue_to_ado as sync


PARENT_ID = 34702979
ISSUE_URL = "https://github.com/microsoft/rust-mqtt-client/issues/200"


def event(action="opened", issue_type="Bug"):
    return {
        "action": action,
        "repository": {
            "id": sync.GITHUB_REPOSITORY_ID, "name": "rust-mqtt-client",
            "full_name": sync.GITHUB_REPOSITORY,
            "html_url": f"https://github.com/{sync.GITHUB_REPOSITORY}",
            "owner": {"login": "microsoft"},
        },
        "sender": {"login": "reporter"},
        "issue": {
            "number": 200, "html_url": ISSUE_URL, "title": "Original title",
            "body": "Original description", "state": "open", "type": {"name": issue_type},
            "user": {"login": "reporter"}, "closed_at": None,
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
    token = "test-token"

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
        self.load_patch = patch("issue_to_ado.load_issue", return_value=self.latest)
        self.load = self.load_patch.start()
        self.addCleanup(patch.stopall)

    def test_new_bug_uses_parent_area_and_refreshes_stale_content(self):
        source = event()
        prepared = sync.prepare(FakeAzureDevOps(), source, PARENT_ID)
        self.assertEqual(source, event())
        self.assertEqual({key: value for key, value in prepared.items() if key != "event"}, {
            "existing_id": None, "parent_id": PARENT_ID, "area_path": "One\\SDKs\\MqttClient",
        })
        self.assertEqual(prepared["event"]["issue"]["title"], "Current title")
        self.assertEqual(prepared["event"]["issue"]["body"], "Current description")
        self.assertEqual(prepared["event"]["action"], "edited")
        self.load.assert_called_once_with(source)

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

    def test_unsubscribed_actions_are_ignored_and_pull_requests_fail(self):
        for action in ("reopened", "typed", "created", "assigned"):
            with self.subTest(action=action):
                self.assertIsNone(sync.prepare(FakeAzureDevOps(), event(action), PARENT_ID))
        self.load.assert_not_called()
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
                self.assertEqual(sync.complete(ado, prepared["event"], prepared), 123)
                self.assertEqual(ado.item["fields"]["System.State"], "Active")
                self.assertFalse(any(call[0] == "PATCH" for call in ado.calls))

    def test_closed_event_preserves_action_and_requires_done(self):
        ado = FakeAzureDevOps(bug(state="Active"))
        source = event("closed")
        prepared = sync.prepare(ado, source, PARENT_ID)
        with self.assertRaisesRegex(RuntimeError, "transition.*Done"):
            sync.complete(ado, prepared["event"], prepared)
        ado.item["fields"]["System.State"] = "Done"
        self.assertEqual(sync.complete(ado, prepared["event"], prepared), 123)

    def test_issue_closed_during_initial_creation_finishes_done(self):
        self.latest["state"] = "closed"
        ado = FakeAzureDevOps()
        prepared = sync.prepare(ado, event(), PARENT_ID)
        self.assertEqual(prepared["event"]["action"], "closed")
        ado.item = bug(state="Done", parent_id=None)
        ado.matches = [{"id": 123}]
        self.assertEqual(sync.complete(ado, prepared["event"], prepared), 123)
        self.assertEqual(ado.item["fields"]["System.State"], "Done")
        self.assertTrue(sync.has_parent(ado.item, PARENT_ID))

    def test_new_bug_gets_parent_and_new_state(self):
        ado = FakeAzureDevOps()
        prepared = sync.prepare(ado, event(), PARENT_ID)
        ado.item = bug(state="Approved", parent_id=None)
        ado.matches = [{"id": 123}]
        self.assertEqual(sync.complete(ado, prepared["event"], prepared), 123)
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
        self.assertEqual(sync.complete(ado, prepared["event"], prepared), 123)
        self.assertEqual(ado.item["fields"]["System.State"], "Active")
        self.assertTrue(sync.has_parent(ado.item, PARENT_ID))

    def test_missing_upstream_result_and_changed_identity_fail(self):
        ado = FakeAzureDevOps()
        prepared = sync.prepare(ado, event(), PARENT_ID)
        with self.assertRaisesRegex(RuntimeError, "did not create"):
            sync.complete(ado, prepared["event"], prepared)
        prepared["existing_id"] = 456
        ado.item = bug()
        ado.matches = [{"id": 123}]
        with self.assertRaisesRegex(RuntimeError, "different ADO Bug"):
            sync.complete(ado, prepared["event"], prepared)

    def test_mismatched_refreshed_issue_fails(self):
        ado = FakeAzureDevOps()
        self.latest["number"] = 201
        with self.assertRaisesRegex(ValueError, "does not match"):
            sync.prepare(ado, event(), PARENT_ID)
        self.assertFalse(any(call[0] == "PATCH" for call in ado.calls))

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

    def test_pipeline_runs_action_and_verifies_parent_link(self):
        ado = FakeAzureDevOps()

        def action(client, context):
            self.assertIs(client, ado)
            self.assertEqual(context["event"]["issue"]["title"], "Current title")
            self.assertEqual(context["event"]["action"], "edited")
            ado.item = bug(parent_id=None)
            ado.matches = [{"id": 123}]

        with patch.dict(os.environ, self.pipeline_environment()), patch(
            "issue_to_ado.AzureDevOps", return_value=ado,
        ), patch("issue_to_ado.run_action", side_effect=action) as run:
            self.assertEqual(sync.main(), 0)
        run.assert_called_once()
        self.assertTrue(sync.has_parent(ado.item, PARENT_ID))

    @staticmethod
    def pipeline_environment():
        return {
            "ADO_ORGANIZATION": "msazure", "ADO_PROJECT": "One",
            "ADO_TOKEN": "test-token", "ADO_PARENT_ID": str(PARENT_ID),
            "WEBHOOK_REPOSITORY_ID": str(sync.GITHUB_REPOSITORY_ID),
            "WEBHOOK_REPOSITORY": sync.GITHUB_REPOSITORY,
            "WEBHOOK_ACTION": "opened", "WEBHOOK_ISSUE_NUMBER": "200",
            "WEBHOOK_ISSUE_TYPE": "Bug", "WEBHOOK_SENDER": "reporter", "WEBHOOK_PING_ID": "",
            "BUILD_SOURCEBRANCH": "refs/heads/main", "BUILD_REASON": "ResourceTrigger",
        }

    def test_cli_missing_credentials_fails_explicitly(self):
        environment = self.pipeline_environment()
        del environment["ADO_TOKEN"]
        with patch.dict(os.environ, environment, clear=True), patch(
            "sys.stderr", new_callable=io.StringIO,
        ) as errors:
            self.assertEqual(sync.main(), 1)
        self.assertIn("##vso[task.logissue type=error]", errors.getvalue())
        self.assertIn("ADO_TOKEN", errors.getvalue())

    def test_downloads_only_pinned_action_and_passes_data_without_shell_expansion(self):
        ado = FakeAzureDevOps()
        prepared = sync.prepare(ado, event(), PARENT_ID)
        prepared["event"]["issue"]["body"] = '$(touch /tmp/not-executed)\n##vso[task.setvariable]x'
        captured = {}

        def execute(command, **kwargs):
            self.assertTrue(kwargs["check"])
            self.assertNotIn("shell", kwargs)
            if command[0] == "node":
                environment = kwargs["env"]
                captured["event_path"] = Path(environment["GITHUB_EVENT_PATH"])
                captured["event"] = json.loads(captured["event_path"].read_text())
                captured["environment"] = environment

        with patch.dict(os.environ, {
            "github_token": "must-not-write-to-github", "ado_bypassrules": "true",
        }), patch("issue_to_ado.subprocess.run", side_effect=execute) as run:
            sync.run_action(ado, prepared)
        commands = [call.args[0] for call in run.call_args_list]
        self.assertEqual(len(commands), 4)
        self.assertEqual(commands[1][-4:], [
            "--quiet", "--depth=1", sync.ACTION_REPOSITORY, sync.ACTION_REVISION,
        ])
        self.assertEqual(commands[2][-2:], ["--detach", "FETCH_HEAD"])
        self.assertEqual(captured["event"], prepared["event"])
        self.assertFalse(captured["event_path"].exists())
        self.assertEqual(captured["environment"]["ado_token"], "test-token")
        self.assertEqual(captured["environment"]["ado_wit"], "Bug")
        self.assertEqual(captured["environment"]["ado_close_state"], "Done")
        self.assertEqual(captured["environment"]["github_token"], "")
        self.assertNotIn("ado_bypassrules", captured["environment"])

    def test_large_unicode_issue_body_is_a_file_not_a_process_environment_value(self):
        prepared = sync.prepare(FakeAzureDevOps(), event(), PARENT_ID)
        body = "\U0001f600" * 65536
        prepared["event"]["issue"]["body"] = body

        def execute(command, **kwargs):
            if command[0] == "node":
                environment = kwargs["env"]
                path = Path(environment["GITHUB_EVENT_PATH"])
                self.assertGreater(path.stat().st_size, 128 * 1024)
                self.assertEqual(json.loads(path.read_text())["issue"]["body"], body)
                self.assertNotIn(body, environment.values())

        with patch("issue_to_ado.subprocess.run", side_effect=execute):
            sync.run_action(FakeAzureDevOps(), prepared)

    def test_repository_identity_is_checked_before_any_api_call(self):
        for field, value in (("id", 1), ("full_name", "other/repo"), ("name", "other")):
            with self.subTest(field=field):
                source = event()
                source["repository"][field] = value
                ado = FakeAzureDevOps()
                with self.assertRaisesRegex(ValueError, "immutable GitHub repository"):
                    sync.prepare(ado, source, PARENT_ID)
                self.assertEqual(ado.calls, [])
        self.load.assert_not_called()

    def test_null_body_is_normalized_for_the_upstream_action(self):
        self.latest["body"] = None
        prepared = sync.prepare(FakeAzureDevOps(), event(), PARENT_ID)
        self.assertEqual(prepared["event"]["issue"]["body"], "")

    def test_empty_token_and_manual_event_injection_fail_explicitly(self):
        for field, value in (("ADO_TOKEN", ""), ("BUILD_REASON", "Manual")):
            with self.subTest(field=field):
                environment = {**self.pipeline_environment(), field: value}
                with patch.dict(os.environ, environment), patch(
                    "issue_to_ado.run_action",
                ) as run, patch("sys.stderr", new_callable=io.StringIO) as errors:
                    self.assertEqual(sync.main(), 1)
                run.assert_not_called()
                self.assertIn("##vso[task.logissue type=error]", errors.getvalue())

    def test_malformed_metadata_fails_explicitly(self):
        for field, value in (
            ("WEBHOOK_REPOSITORY", "not-a-repository"),
            ("WEBHOOK_REPOSITORY_ID", "not-an-id"),
            ("WEBHOOK_ISSUE_NUMBER", "not-an-id"),
            ("WEBHOOK_ISSUE_NUMBER", "-1"),
        ):
            with self.subTest(field=field, value=value):
                environment = {**self.pipeline_environment(), field: value}
                with patch.dict(os.environ, environment), patch(
                    "sys.stderr", new_callable=io.StringIO,
                ) as errors:
                    self.assertEqual(sync.main(), 1)
                self.assertIn("##vso[task.logissue type=error]", errors.getvalue())

    def test_non_main_event_cannot_run_the_action(self):
        environment = {
            **self.pipeline_environment(), "BUILD_SOURCEBRANCH": "refs/heads/untrusted",
        }
        with patch.dict(os.environ, environment), patch(
            "issue_to_ado.run_action",
        ) as run, patch("sys.stderr", new_callable=io.StringIO) as errors:
            self.assertEqual(sync.main(), 1)
        run.assert_not_called()
        self.assertIn("trusted refs/heads/main", errors.getvalue())

    def test_manual_initialization_and_webhook_ping_do_not_modify_work_items(self):
        for reason in ("Manual", "ResourceTrigger"):
            with self.subTest(reason=reason):
                environment = {
                    **self.pipeline_environment(),
                    "BUILD_REASON": reason, "WEBHOOK_ISSUE_NUMBER": "", "WEBHOOK_ACTION": "",
                    "WEBHOOK_PING_ID": "1" if reason == "ResourceTrigger" else "",
                    "WEBHOOK_REPOSITORY": sync.GITHUB_REPOSITORY if reason == "ResourceTrigger" else "",
                }
                with patch.dict(os.environ, environment), patch("issue_to_ado.AzureDevOps") as client:
                    self.assertEqual(sync.main(), 0)
                client.assert_not_called()
        self.load.assert_not_called()

    def test_public_issue_refresh_does_not_send_the_ado_token_to_github(self):
        self.load_patch.stop()
        with patch("issue_to_ado.urlopen") as send:
            send.return_value.__enter__.return_value = io.StringIO(json.dumps(self.latest))
            self.assertEqual(sync.load_issue(event()), self.latest)
        request = send.call_args.args[0]
        self.assertEqual(request.full_url, f"https://api.github.com/repos/{sync.GITHUB_REPOSITORY}/issues/200")
        self.assertNotIn("Authorization", request.headers)
        self.assertEqual(send.call_args.kwargs["timeout"], 30)

    def test_public_issue_rate_limits_fail_before_upstream_mutations(self):
        self.load_patch.stop()
        error = HTTPError("https://api.github.com", 403, "Rate limited", {}, None)
        with patch.dict(os.environ, self.pipeline_environment()), patch(
            "issue_to_ado.AzureDevOps", return_value=FakeAzureDevOps(),
        ), patch("issue_to_ado.urlopen", side_effect=error), patch(
            "issue_to_ado.run_action",
        ) as run, patch("sys.stderr", new_callable=io.StringIO) as errors:
            self.assertEqual(sync.main(), 1)
        run.assert_not_called()
        self.assertIn("rate limiting", errors.getvalue())


if __name__ == "__main__":
    unittest.main()
