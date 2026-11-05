#!/usr/bin/env python3
# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import argparse
import json
import os
from pathlib import Path
import subprocess
import sys
from urllib.error import HTTPError, URLError
from urllib.parse import quote
from urllib.request import Request, urlopen


PARENT_RELATION = "System.LinkTypes.Hierarchy-Reverse"


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


def load_issue(event):
    result = subprocess.run(
        [
            "gh", "api",
            f"repos/{event['repository']['full_name']}/issues/{event['issue']['number']}",
        ],
        check=True,
        stdout=subprocess.PIPE,
        text=True,
    )
    return json.loads(result.stdout)


def find_item(ado, event):
    number = event["issue"]["number"]
    if not isinstance(number, int) or isinstance(number, bool) or number <= 0:
        raise ValueError("The GitHub issue number must be a positive integer")
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
        or item["fields"]["System.WorkItemType"] != "Bug"
        or not any(
            relation["rel"] == "Hyperlink" and relation["url"] == event["issue"]["html_url"]
            for relation in item.get("relations", [])
        )
    ):
        raise RuntimeError("The matching ADO item is not a Bug linked to this GitHub issue")
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
        raise RuntimeError(f"ADO Bug {item['id']} already has a different parent; refusing to reparent")
    return bool(parents)


def prepare(ado, event, parent_id):
    action = event["action"]
    if action not in {"opened", "edited", "closed"}:
        raise ValueError(f"Unsupported issue action: {action}")
    if "pull_request" in event["issue"]:
        raise ValueError("Pull requests must not be synchronized as Bugs")
    if action == "opened" and (event["issue"].get("type") or {}).get("name") != "Bug":
        return None

    item = find_item(ado, event)
    # The upstream action creates missing items even for edits and closures.
    if item is None and action != "opened":
        print("No mirrored ADO Bug exists; skipping this historical or ineligible issue.")
        return None
    if item is not None:
        has_parent(item, parent_id)
    parent = ado.request("GET", f"workitems/{parent_id}")
    if parent["fields"]["System.TeamProject"] != ado.project:
        raise RuntimeError("The configured ADO parent is in a different project")
    area = parent["fields"]["System.AreaPath"]
    if not isinstance(area, str) or not area or "\n" in area or "\r" in area:
        raise ValueError("The parent must have a valid area path")

    return {
        "existing_id": item["id"] if item is not None else None,
        "parent_id": parent_id,
        "area_path": area,
    }


def complete(ado, event, context):
    item = find_item(ado, event)
    if item is None:
        raise RuntimeError("The upstream action did not create or retain the expected ADO Bug")
    if context["existing_id"] is not None and context["existing_id"] != item["id"]:
        raise RuntimeError("The upstream action selected a different ADO Bug")
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
            raise RuntimeError("The upstream action did not transition the ADO Bug to Done")
    elif event["action"] == "opened":
        issue = load_issue(event)
        if (
            issue["number"] != event["issue"]["number"]
            or issue["html_url"] != event["issue"]["html_url"]
            or issue["state"] not in {"open", "closed"}
        ):
            raise ValueError("The refreshed GitHub issue does not match the event")
        # A close can precede creation; retries must not reset an existing manual state.
        expected_state = "Done" if issue["state"] == "closed" else (
            "New" if context["existing_id"] is None else None
        )
        if expected_state is not None and item["fields"]["System.State"] != expected_state:
            patch.append({"op": "add", "path": "/fields/System.State", "value": expected_state})
    if patch:
        item = ado.request(
            "PATCH", f"workitems/{item['id']}?$expand=relations",
            [{"op": "test", "path": "/rev", "value": item["rev"]}, *patch],
        )
    if not has_parent(item, context["parent_id"]):
        raise RuntimeError("The ADO Bug does not have the configured parent")
    if expected_state is not None and item["fields"]["System.State"] != expected_state:
        raise RuntimeError(f"The ADO Bug is not in the expected {expected_state} state")
    return item["id"]


def main():
    parser = argparse.ArgumentParser(description="Guard and parent-link the upstream issue sync action")
    parser.add_argument("phase", choices=("prepare", "complete"))
    args = parser.parse_args()
    try:
        ado = AzureDevOps(
            os.environ["ADO_ORGANIZATION"], os.environ["ADO_PROJECT"], os.environ["ADO_TOKEN"],
        )
        event = json.loads(Path(os.environ["GITHUB_EVENT_PATH"]).read_text())
        if args.phase == "prepare":
            parent_id = int(os.environ["ADO_PARENT_ID"])
            if parent_id <= 0:
                raise ValueError("ADO_PARENT_ID must be positive")
            prepared = prepare(ado, event, parent_id)
            with Path(os.environ["GITHUB_OUTPUT"]).open("a") as output:
                output.write(f"sync={'true' if prepared is not None else 'false'}\n")
                if prepared is not None:
                    path = Path(os.environ["RUNNER_TEMP"]) / "ado-bug-context.json"
                    path.write_text(json.dumps(prepared))
                    output.write(f"context_path={path}\n")
                    output.write(f"area_path={prepared['area_path']}\n")
        else:
            context = json.loads(Path(os.environ["ADO_SYNC_CONTEXT"]).read_text())
            item_id = complete(ado, event, context)
            print(f"Synchronized ADO Bug {item_id} under parent {context['parent_id']}.")
            with Path(os.environ["GITHUB_STEP_SUMMARY"]).open("a") as summary:
                summary.write(
                    f"ADO Bug: https://dev.azure.com/{ado.organization}/"
                    f"{quote(ado.project, safe='')}/_workitems/edit/{item_id}\n"
                )
    except (KeyError, ValueError, RuntimeError, URLError, subprocess.CalledProcessError) as error:
        print(f"::error::{error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
