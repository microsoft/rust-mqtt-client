#!/usr/bin/env python3
# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import argparse
import json
import os
from pathlib import Path
import re
import sys

from report_advisories import ADVISORY_LABEL, REVIEW_LABEL, gh, review_state


WORKFLOW_MARKER = "<!-- gh-aw-workflow-id: advisory-risk-review -->"
MAX_REVIEWS = 5


def completed_review(repository, number, state):
    pages = json.loads(gh(
        "api", "--method", "GET", "--paginate", "--slurp",
        f"repos/{repository}/issues/{number}/comments", "-f", "per_page=100",
    ))
    for page in pages:
        for comment in page:
            body = comment["body"]
            if comment["user"]["login"] != "github-actions[bot]" or WORKFLOW_MARKER not in body:
                continue
            for block in re.findall(r"```json\s*\n(.*?)\n```", body, re.DOTALL):
                try:
                    data = json.loads(block)
                except json.JSONDecodeError:
                    continue
                if isinstance(data, dict) and (
                    data.get("advisory_key") == state["advisory_key"]
                    and data.get("fingerprint") == state["fingerprint"]
                    and isinstance(data.get("source_revision"), str)
                    and re.fullmatch(r"[0-9a-f]{40}", data["source_revision"])
                ):
                    return True
    return False


def clear_queue(repository, number):
    gh("issue", "edit", str(number), "--repo", repository, "--remove-label", REVIEW_LABEL)


def prepare(repository, output, issue_number="", source_revision=""):
    if issue_number:
        if not issue_number.isdecimal() or int(issue_number) < 1:
            raise ValueError("Invalid issue number")
        issues = [json.loads(gh("api", f"repos/{repository}/issues/{issue_number}"))]
    else:
        pages = json.loads(gh(
            "api", "--method", "GET", "--paginate", "--slurp",
            f"repos/{repository}/issues", "-f", "state=open",
            "-f", f"labels={ADVISORY_LABEL},{REVIEW_LABEL}", "-f", "per_page=100",
        ))
        issues = [issue for page in pages for issue in page]
    queue = []
    unique_issues = {issue["number"]: issue for issue in issues}
    for issue in sorted(unique_issues.values(), key=lambda item: item["number"]):
        labels = {label["name"].casefold() for label in issue["labels"]}
        if (
            "pull_request" in issue
            or issue["state"] != "open"
            or issue["user"]["login"] != "github-actions[bot]"
            or not {ADVISORY_LABEL, REVIEW_LABEL}.issubset(labels)
        ):
            print(f"Skipping issue #{issue['number']}: not a queued workflow-created finding.")
            continue
        state = review_state(issue["body"] or "")
        if state is None:
            raise ValueError(f"Issue #{issue['number']} lacks dependency review metadata")
        marker = f"<!-- dependency-advisory:{state['advisory_key']} -->"
        if marker not in issue["body"]:
            raise ValueError(f"Issue #{issue['number']} has inconsistent advisory identity")
        if source_revision and state["source_revision"] != source_revision:
            print(f"Skipping issue #{issue['number']}: its scan has already been superseded.")
            continue
        if completed_review(repository, issue["number"], state):
            clear_queue(repository, issue["number"])
            print(f"Reusing completed assessment for issue #{issue['number']}.")
            continue
        if len(queue) == MAX_REVIEWS:
            print(f"Deferring issue #{issue['number']}: the per-run review limit is {MAX_REVIEWS}.")
            continue
        queue.append({"issue_number": issue["number"], **state})
    with output.open("a") as stream:
        stream.write(f"has_reviews={str(bool(queue)).lower()}\n")
        stream.write("queue=" + json.dumps(queue, separators=(",", ":")) + "\n")
    print(f"Queued {len(queue)} dependency risk assessment(s).")


def finalize(repository, queue):
    incomplete = []
    for request in queue:
        number = request["issue_number"]
        issue = json.loads(gh("api", f"repos/{repository}/issues/{number}"))
        current = review_state(issue["body"] or "")
        if current is None or current["fingerprint"] != request["fingerprint"]:
            print(f"Keeping issue #{number} queued: the dependency finding changed during review.")
            continue
        if not completed_review(repository, number, request):
            incomplete.append(str(number))
            continue
        clear_queue(repository, number)
        print(f"Completed dependency risk assessment for issue #{number}.")
    if incomplete:
        raise ValueError(
            "No matching Copilot assessment was published for issue(s): " + ", ".join(incomplete)
        )


def main():
    parser = argparse.ArgumentParser(description="Prepare and finalize comment-only Copilot reviews.")
    parser.add_argument("operation", choices=["prepare", "finalize"])
    parser.add_argument("--repository", required=True)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--issue-number", default="")
    parser.add_argument("--source-revision", default="")
    args = parser.parse_args()
    try:
        if args.operation == "prepare":
            if args.output is None:
                raise ValueError("--output is required for prepare")
            prepare(args.repository, args.output, args.issue_number, args.source_revision)
        else:
            finalize(args.repository, json.loads(os.environ["REVIEW_QUEUE"]))
    except (OSError, ValueError, RuntimeError, KeyError, TypeError) as error:
        print(f"Dependency risk review failed: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
