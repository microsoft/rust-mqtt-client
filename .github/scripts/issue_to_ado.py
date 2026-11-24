#!/usr/bin/env python3
# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
from urllib.error import HTTPError
from urllib.parse import quote
from urllib.request import Request, urlopen


PARENT_RELATION = "System.LinkTypes.Hierarchy-Reverse"
GITHUB_REPOSITORY = "microsoft/rust-mqtt-client"
GITHUB_REPOSITORY_ID = 1013299075
ACTION_REPOSITORY = "https://github.com/danhellem/github-actions-issue-to-work-item.git"
ACTION_REVISION = "70e94b931901465f5550532c7a061b0d103b4b10"
SUPPORTED_ACTIONS = {"opened", "edited", "closed", "labeled"}
REQUIRED_LABEL = "planned"
WORK_ITEM_TYPES = {"Bug": "Bug", "Feature": "Product Backlog Item"}


class AzureDevOps:
    def __init__(self, organization, project, token):
        self.organization = organization
        self.project = project
        self.token = token
        self.url = f"https://dev.azure.com/{organization}/{quote(project, safe='')}/_apis/wit"

    def request(self, method, path, document=None):
        separator = "&" if "?" in path else "?"
        request = Request(
            f"{self.url}/{path}{separator}api-version=7.1",
            data=json.dumps(document).encode() if document is not None else None,
            headers={
                "Authorization": f"Bearer {self.token}",
                "Content-Type": (
                    "application/json-patch+json" if method == "PATCH" else "application/json"
                ),
            },
            method=method,
        )
        try:
            with urlopen(request, timeout=30) as response:
                return json.load(response)
        except HTTPError as error:
            raise RuntimeError(
                f"Azure DevOps {method} {path} failed: HTTP {error.code} {error.reason}"
            ) from error


def issue_number(event):
    number = event["issue"]["number"]
    if not isinstance(number, int) or isinstance(number, bool) or number <= 0:
        raise ValueError("The GitHub issue number must be a positive integer")
    return number


def load_issue(event):
    number = issue_number(event)
    request = Request(
        f"https://api.github.com/repos/{GITHUB_REPOSITORY}/issues/{number}",
        headers={
            "Accept": "application/vnd.github+json",
            "User-Agent": "rust-mqtt-client-issue-sync",
            "X-GitHub-Api-Version": "2022-11-28",
        },
    )
    try:
        with urlopen(request, timeout=30) as response:
            return json.load(response)
    except HTTPError as error:
        raise RuntimeError(
            f"GitHub issue #{number} refresh failed: HTTP {error.code} {error.reason}. "
            "Resolve API access or rate limiting before retrying the pipeline."
        ) from error


def validate_repository(event):
    repository = event.get("repository")
    if not isinstance(repository, dict) or (
        repository.get("id") != GITHUB_REPOSITORY_ID
        or repository.get("full_name") != GITHUB_REPOSITORY
        or repository.get("name") != GITHUB_REPOSITORY.split("/")[1]
    ):
        raise ValueError("The webhook must come from the expected immutable GitHub repository")


def webhook_event():
    repository = os.environ["WEBHOOK_REPOSITORY"]
    owner, name = repository.split("/")
    event = {
        "repository": {
            "id": int(os.environ["WEBHOOK_REPOSITORY_ID"]),
            "name": name,
            "full_name": repository,
            "html_url": f"https://github.com/{repository}",
            "owner": {"login": owner},
        },
        "sender": {"login": os.environ["WEBHOOK_SENDER"]},
    }
    validate_repository(event)
    if os.environ.get("WEBHOOK_PING_ID") and not os.environ["WEBHOOK_ACTION"]:
        return None
    number = int(os.environ["WEBHOOK_ISSUE_NUMBER"])
    event["action"] = os.environ["WEBHOOK_ACTION"]
    event["issue"] = {
        "number": number,
        "html_url": f"https://github.com/{repository}/issues/{number}",
        "type": {"name": os.environ["WEBHOOK_ISSUE_TYPE"]},
    }
    return event


def find_item(ado, event, work_item_type):
    number = issue_number(event)
    repository = event["repository"]["name"].replace("'", "''")
    # Match the upstream action's lookup, rejecting ambiguous or unrelated matches.
    result = ado.request("POST", "wiql", {
        "query": (
            "SELECT [System.Id] FROM WorkItems WHERE [System.TeamProject] = @project "
            f"AND [System.Title] CONTAINS '(GitHub Issue #{number})' "
            "AND [System.Tags] CONTAINS 'GitHub Issue' "
            f"AND [System.Tags] CONTAINS '{repository}'"
        )
    })["workItems"]
    if len(result) > 1:
        raise RuntimeError(f"Multiple ADO items match GitHub issue #{number}; resolve duplicates first")
    if not result:
        return None
    item = ado.request("GET", f"workitems/{result[0]['id']}?$expand=relations")
    if (
        item["fields"]["System.TeamProject"] != ado.project
        or item["fields"]["System.WorkItemType"] != work_item_type
        or not any(
            relation["rel"] == "Hyperlink" and relation["url"] == event["issue"]["html_url"]
            for relation in item.get("relations", [])
        )
    ):
        raise RuntimeError(
            f"The matching ADO item is not a {work_item_type} linked to this GitHub issue; "
            "resolve work-item type changes manually"
        )
    return item


def has_parent(item, parent_id):
    parents = [
        relation for relation in item.get("relations", [])
        if relation["rel"] == PARENT_RELATION
    ]
    if len(parents) > 1 or any(
        relation["url"].rstrip("/").rsplit("/", 1)[-1] != str(parent_id)
        for relation in parents
    ):
        raise RuntimeError(
            f"ADO work item {item['id']} already has a different parent; refusing to reparent"
        )
    return bool(parents)


def prepare(ado, event, parent_id, enhancement_parent_id=None):
    validate_repository(event)
    action = event["action"]
    if not isinstance(action, str) or not isinstance(event.get("issue"), dict):
        raise ValueError("The webhook must contain an issue object and an action string")
    if action not in SUPPORTED_ACTIONS:
        print(f"Ignoring issue action {action!r}; no work items modified.")
        return None
    if "pull_request" in event["issue"]:
        raise ValueError("Pull requests must not be synchronized as work items")

    issue_number(event)
    issue = load_issue(event)
    if not isinstance(issue, dict):
        raise ValueError("The GitHub API must return an issue object")
    if (
        issue["number"] != event["issue"]["number"]
        or issue["html_url"] != event["issue"]["html_url"]
        or "pull_request" in issue
        or issue["state"] not in {"open", "closed"}
        or not isinstance(issue["title"], str)
        or not issue["title"]
        or (issue["body"] is not None and not isinstance(issue["body"], str))
    ):
        raise ValueError("The refreshed GitHub issue does not match the event")
    issue_type = issue["type"]
    if issue_type is not None and not isinstance(issue_type, dict):
        raise ValueError("The current GitHub issue type must be an object or null")
    type_name = (issue_type or {}).get("name")
    if type_name is not None and not isinstance(type_name, str):
        raise ValueError("The current GitHub issue type name must be a string")
    labels = issue["labels"]
    if not isinstance(labels, list) or any(
        not isinstance(label, dict) or not isinstance(label.get("name"), str)
        for label in labels
    ):
        raise ValueError("The current GitHub issue labels must contain label-name objects")
    if type_name not in WORK_ITEM_TYPES or not any(
        label["name"].casefold() == REQUIRED_LABEL for label in labels
    ):
        print("Issue is not currently a Bug or Feature labeled planned; synchronization is paused.")
        return None
    work_item_type = WORK_ITEM_TYPES[type_name]
    if type_name == "Feature":
        if enhancement_parent_id is None:
            raise ValueError("ADO_ENHANCEMENT_PARENT_ID must be configured for Feature issues")
        parent_id = enhancement_parent_id
    if issue["body"] is None:
        issue["body"] = ""
    prepared_event = dict(event, issue=issue)
    item = find_item(ado, prepared_event, work_item_type)
    # The upstream action creates missing items even for edits and closures.
    if item is None and action not in {"opened", "labeled"}:
        print("No mirrored ADO item exists; skipping this historical or ineligible issue.")
        return None
    if item is not None:
        has_parent(item, parent_id)
    parent = ado.request("GET", f"workitems/{parent_id}")
    if parent["fields"]["System.TeamProject"] != ado.project:
        raise RuntimeError("The configured ADO parent is in a different project")
    if parent["fields"]["System.WorkItemType"] != "Feature":
        raise RuntimeError("The configured ADO parent must be a Feature")
    area = parent["fields"]["System.AreaPath"]
    if not isinstance(area, str) or not area or "\n" in area or "\r" in area:
        raise ValueError("The parent must have a valid area path")
    if action in {"opened", "labeled"}:
        # Label events opt in without invoking the upstream action's label/tag copying.
        prepared_event["action"] = "closed" if issue["state"] == "closed" else "edited"
    return {
        "existing_id": item["id"] if item is not None else None,
        "parent_id": parent_id,
        "work_item_type": work_item_type,
        "area_path": area,
        "event": prepared_event,
    }


def complete(ado, event, context):
    item = find_item(ado, event, context["work_item_type"])
    if item is None:
        raise RuntimeError("The upstream action did not create or retain the expected ADO item")
    if context["existing_id"] is not None and context["existing_id"] != item["id"]:
        raise RuntimeError("The upstream action selected a different ADO item")
    patch = []
    if not has_parent(item, context["parent_id"]):
        patch.append({
            "op": "add",
            "path": "/relations/-",
            "value": {
                "rel": PARENT_RELATION,
                "url": (
                    f"https://dev.azure.com/{ado.organization}/_apis/wit/workItems/"
                    f"{context['parent_id']}"
                ),
            },
        })
    expected_state = None
    if event["action"] == "closed":
        expected_state = "Done"
        if item["fields"]["System.State"] != "Done":
            raise RuntimeError("The upstream action did not transition the ADO item to Done")
    elif context["existing_id"] is None:
        expected_state = "New"
        # The upstream action accepts ado_new_state but does not apply it at creation.
        if item["fields"]["System.State"] != expected_state:
            patch.append({"op": "add", "path": "/fields/System.State", "value": expected_state})
    if patch:
        item = ado.request(
            "PATCH", f"workitems/{item['id']}?$expand=relations",
            [{"op": "test", "path": "/rev", "value": item["rev"]}, *patch],
        )
    if not has_parent(item, context["parent_id"]):
        raise RuntimeError("The ADO item does not have the configured parent")
    if expected_state is not None and item["fields"]["System.State"] != expected_state:
        raise RuntimeError(f"The ADO item is not in the expected {expected_state} state")
    return item["id"]


def run_action(ado, context):
    with tempfile.TemporaryDirectory(prefix="github-issue-sync-") as directory:
        root = Path(directory)
        action_directory = root / "action"
        subprocess.run(["git", "init", "--quiet", str(action_directory)], check=True)
        subprocess.run(
            [
                "git", "-C", str(action_directory), "-c", "credential.helper=",
                "fetch", "--quiet", "--depth=1", ACTION_REPOSITORY, ACTION_REVISION,
            ],
            check=True,
            env={**os.environ, "GIT_TERMINAL_PROMPT": "0"},
        )
        subprocess.run(
            ["git", "-C", str(action_directory), "checkout", "--quiet", "--detach", "FETCH_HEAD"],
            check=True,
        )
        event_path = root / "event.json"
        event_path.write_text(json.dumps(context["event"]))
        environment = {
            **os.environ,
            "GITHUB_EVENT_PATH": str(event_path),
            "ado_token": ado.token,
            "ado_organization": ado.organization,
            "ado_project": ado.project,
            "ado_area_path": context["area_path"],
            "ado_wit": context["work_item_type"],
            "ado_new_state": "New",
            "ado_close_state": "Done",
            "github_token": "",
            "log_level": "100",
        }
        environment.pop("ado_bypassrules", None)
        print(f"Running upstream issue synchronization action at {ACTION_REVISION}.", flush=True)
        # The pinned action includes its node_modules; no npm install is needed.
        subprocess.run(
            ["node", str(action_directory / "index.js")],
            check=True,
            cwd=action_directory,
            env=environment,
        )


def main():
    try:
        if os.environ.get("BUILD_REASON") == "Manual" and not os.environ.get("WEBHOOK_REPOSITORY"):
            print("Manual initialization without a webhook; no work items modified.")
            return 0
        if os.environ["BUILD_SOURCEBRANCH"] != "refs/heads/main":
            raise ValueError("Issue synchronization must run from trusted refs/heads/main")
        if os.environ["BUILD_REASON"] != "ResourceTrigger":
            raise ValueError("Issue synchronization requires an incoming webhook resource trigger")
        event = webhook_event()
        if event is None:
            print("GitHub webhook ping received; no work items modified.")
            return 0
        token = os.environ["ADO_TOKEN"]
        if not token:
            raise ValueError("ADO_TOKEN must contain an Azure DevOps access token")
        for name in (
            "ADO_ORGANIZATION", "ADO_PROJECT", "ADO_PARENT_ID", "ADO_ENHANCEMENT_PARENT_ID",
        ):
            value = os.environ[name]
            if not value.strip() or value.startswith("$("):
                raise ValueError(f"{name} must be configured in private pipeline variables")
        parent_id = int(os.environ["ADO_PARENT_ID"])
        enhancement_parent_id = int(os.environ["ADO_ENHANCEMENT_PARENT_ID"])
        if parent_id <= 0 or enhancement_parent_id <= 0:
            raise ValueError("ADO parent IDs must be positive")
        ado = AzureDevOps(
            os.environ["ADO_ORGANIZATION"], os.environ["ADO_PROJECT"], token,
        )
        context = prepare(ado, event, parent_id, enhancement_parent_id)
        if context is None:
            return 0
        run_action(ado, context)
        item_id = complete(ado, context["event"], context)
        print(
            f"Synchronized ADO {context['work_item_type']} https://dev.azure.com/{ado.organization}/"
            f"{quote(ado.project, safe='')}/_workitems/edit/{item_id} "
            f"under parent {context['parent_id']}."
        )
    except (
        KeyError, ValueError, RuntimeError, OSError, subprocess.CalledProcessError,
    ) as error:
        message = str(error).replace("%", "%25").replace("\r", "%0D").replace("\n", "%0A")
        print(f"##vso[task.logissue type=error]{message}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
