#!/usr/bin/env python3
# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import argparse
import json
import os
from pathlib import Path
import sys

from report_advisories import (
    ADVISORY_LABEL, REVIEW_LABEL, completed_review, current_scan, gh, issue_comments, review_state,
    tracking_key, workflow_authored,
)


MAX_REVIEWS = 5


def clear_queue(repository, number):
    gh("issue", "edit", str(number), "--repo", repository, "--remove-label", REVIEW_LABEL)


def prepare(repository, output, issue_number=""):
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
            or not workflow_authored(issue)
            or not {ADVISORY_LABEL, REVIEW_LABEL}.issubset(labels)
        ):
            print(f"Skipping issue #{issue['number']}: not a queued workflow-created finding.")
            continue
        comments = issue_comments(repository, issue["number"])
        published = current_scan(issue, comments)
        if published is None:
            print(
                f"Keeping issue #{issue['number']} queued: its generated scan report is missing. "
                "Rerun the scanner or review the issue manually.",
                file=sys.stderr,
            )
            continue
        state = review_state(published["body"] or "", tracking_key(issue["body"] or ""))
        if state is None:
            raise ValueError(f"Issue #{issue['number']} lacks dependency review metadata")
        if not state["report_complete"]:
            print(
                f"Keeping issue #{issue['number']} queued: its scan evidence exceeds the report "
                "size limit. Assess the complete scan artifacts manually.",
                file=sys.stderr,
            )
            continue
        if completed_review(comments, state):
            clear_queue(repository, issue["number"])
            print(f"Reusing completed assessment for issue #{issue['number']}.")
            continue
        if len(queue) == MAX_REVIEWS:
            print(f"Deferring issue #{issue['number']}: the per-run review limit is {MAX_REVIEWS}.")
            continue
        queue.append({"issue_number": issue["number"], "report_url": published["html_url"], **state})
    with output.open("a") as stream:
        stream.write(f"has_reviews={str(bool(queue)).lower()}\n")
        stream.write("queue=" + json.dumps(queue, separators=(",", ":")) + "\n")
    print(f"Queued {len(queue)} dependency risk assessment(s).")


def finalize(repository, queue):
    incomplete = []
    for request in queue:
        number = request["issue_number"]
        issue = json.loads(gh("api", f"repos/{repository}/issues/{number}"))
        comments = issue_comments(repository, number)
        published = current_scan(issue, comments)
        if published is None:
            incomplete.append(str(number))
            continue
        current = review_state(published["body"] or "", tracking_key(issue["body"] or ""))
        if current is None or (
            current["advisory_key"] != request["advisory_key"]
            or current["fingerprint"] != request["fingerprint"]
        ):
            print(f"Keeping issue #{number} queued: the dependency finding changed during review.")
            continue
        if not current["report_complete"] or not completed_review(comments, request):
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
    args = parser.parse_args()
    try:
        if args.operation == "prepare":
            if args.output is None:
                raise ValueError("--output is required for prepare")
            prepare(args.repository, args.output, args.issue_number)
        else:
            finalize(args.repository, json.loads(os.environ["REVIEW_QUEUE"]))
    except (OSError, ValueError, RuntimeError, KeyError, TypeError) as error:
        print(f"Dependency risk review failed: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
