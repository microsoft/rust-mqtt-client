#!/usr/bin/env python3
# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import argparse
import json
import os
from pathlib import Path
import sys

from report_advisories import (
    ADVISORY_LABEL, REVIEW_LABEL, REVIEW_WORKFLOW_ID, completed_review, current_scan, gh,
    issue_comments, review_state, tracking_key, workflow_authored,
)


MAX_REVIEWS = 5


def clear_queue(repository, number):
    gh("issue", "edit", str(number), "--repo", repository, "--remove-label", REVIEW_LABEL)


def prepare(repository, output, issue_number="", non_publishing=False):
    if issue_number:
        if not issue_number.isdecimal() or int(issue_number) < 1:
            raise ValueError("Invalid issue number")
        issues = [json.loads(gh("api", f"repos/{repository}/issues/{issue_number}"))]
    else:
        labels = ADVISORY_LABEL if non_publishing else f"{ADVISORY_LABEL},{REVIEW_LABEL}"
        pages = json.loads(gh(
            "api", "--method", "GET", "--paginate", "--slurp",
            f"repos/{repository}/issues", "-f", f"state={'all' if non_publishing else 'open'}",
            "-f", f"labels={labels}", "-f", "per_page=100",
        ))
        issues = [issue for page in pages for issue in page]
    queue = []
    has_more_reviews = False
    unique_issues = {issue["number"]: issue for issue in issues}
    for issue in sorted(unique_issues.values(), key=lambda item: item["number"]):
        labels = {label["name"].casefold() for label in issue["labels"]}
        required_labels = {ADVISORY_LABEL} if non_publishing else {ADVISORY_LABEL, REVIEW_LABEL}
        if (
            "pull_request" in issue
            or (not non_publishing and issue["state"] != "open")
            or not workflow_authored(issue)
            or not required_labels.issubset(labels)
        ):
            print(f"Skipping issue #{issue['number']}: not an eligible workflow-created finding.")
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
        if not non_publishing and completed_review(comments, state):
            clear_queue(repository, issue["number"])
            print(f"Reusing completed assessment for issue #{issue['number']}.")
            continue
        if len(queue) == MAX_REVIEWS:
            has_more_reviews = True
            print(f"Deferring issue #{issue['number']}: the per-run review limit is {MAX_REVIEWS}.")
            continue
        queue.append({"issue_number": issue["number"], "report_url": published["html_url"], **state})
        if non_publishing:
            break
    if non_publishing and not queue:
        raise ValueError("Non-publishing review requires a complete captured dependency finding")
    with output.open("a") as stream:
        stream.write(f"has_reviews={str(bool(queue)).lower()}\n")
        stream.write(f"has_more_reviews={str(has_more_reviews).lower()}\n")
        stream.write("queue=" + json.dumps(queue, separators=(",", ":")) + "\n")
    print(f"Queued {len(queue)} dependency risk assessment(s).")


def finalize(
    repository, queue, continuation_ref="", non_publishing=False,
    staged_outcomes=None, agent_output=None,
):
    if non_publishing:
        if not queue or staged_outcomes is None or agent_output is None:
            raise ValueError("Non-publishing finalization requires a replay queue and assessment artifacts")
        outcomes = [
            json.loads(line) for line in staged_outcomes.read_text().splitlines() if line.strip()
        ]
        if any(
            not isinstance(outcome, dict)
            or outcome.get("staged") is not True
            or outcome.get("status") != "staged"
            or outcome.get("url") is not None
            for outcome in outcomes
        ):
            raise ValueError("Non-publishing review produced a failed, skipped, or applied output")
        comments = [outcome for outcome in outcomes if outcome.get("type") == "add_comment"]
        if len(comments) != len(queue):
            raise ValueError("Non-publishing review did not stage one assessment per queued finding")
        payload = json.loads(agent_output.read_text())
        if not isinstance(payload, dict) or not isinstance(payload.get("items"), list):
            raise ValueError("Non-publishing review lacks valid assessment payloads")
        items = payload["items"]
        expected = {request["issue_number"]: request for request in queue}
        matched = set()
        for outcome in comments:
            index = outcome.get("messageIndex")
            if type(index) is not int or not 0 <= index < len(items):
                raise ValueError("Non-publishing review lacks a matching staged assessment payload")
            item = items[index]
            if not isinstance(item, dict):
                raise ValueError("Non-publishing review lacks a valid staged assessment payload")
            number = item.get("item_number")
            if isinstance(number, str) and number.isascii() and number.isdecimal():
                number = int(number)
            request = expected.get(number) if type(number) is int else None
            if (
                item.get("type") != "add_comment"
                or request is None
                or number in matched
                or item.get("data") != {
                    key: request[key] for key in ("advisory_key", "fingerprint", "source_revision")
                }
            ):
                raise ValueError("Non-publishing review assessment does not match the replayed finding")
            matched.add(number)
        print(f"Verified {len(comments)} staged assessment(s); no queue state was changed.")
        return
    incomplete = []
    completed = 0
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
        completed += 1
        print(f"Completed dependency risk assessment for issue #{number}.")
    if incomplete:
        raise ValueError(
            "No matching Copilot assessment was published for issue(s): " + ", ".join(incomplete)
        )
    if continuation_ref and queue and completed == len(queue):
        gh(
            "workflow", "run", f"{REVIEW_WORKFLOW_ID}.lock.yml",
            "--repo", repository, "--ref", continuation_ref,
        )
        print("Dispatched the next batch of queued dependency risk assessments.")


def main():
    parser = argparse.ArgumentParser(description="Prepare and finalize comment-only Copilot reviews.")
    parser.add_argument("operation", choices=["prepare", "finalize"])
    parser.add_argument("--repository", required=True)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--issue-number", default="")
    parser.add_argument("--non-publishing", choices=["true", "false"], default="false")
    parser.add_argument("--staged-outcomes", type=Path)
    parser.add_argument("--agent-output", type=Path)
    args = parser.parse_args()
    try:
        if args.operation == "prepare":
            if args.output is None:
                raise ValueError("--output is required for prepare")
            prepare(
                args.repository, args.output, args.issue_number,
                non_publishing=args.non_publishing == "true",
            )
        else:
            finalize(
                args.repository, json.loads(os.environ["REVIEW_QUEUE"]),
                continuation_ref=os.environ.get("REVIEW_CONTINUATION_REF", ""),
                non_publishing=args.non_publishing == "true",
                staged_outcomes=args.staged_outcomes,
                agent_output=args.agent_output,
            )
    except (OSError, ValueError, RuntimeError, KeyError, TypeError) as error:
        print(f"Dependency risk review failed: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
