#!/usr/bin/env python3
# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import argparse
from dataclasses import dataclass
import hashlib
import json
from pathlib import Path
import re
import subprocess
import sys


ADVISORY_CODES = {"vulnerability", "unsound", "unmaintained", "notice"}
REPORT_START = "<!-- dependency-advisory-report:start -->"
REPORT_END = "<!-- dependency-advisory-report:end -->"
RESOLUTIONS = ("latest", "minimal")
ADVISORY_LABEL = "dependency-advisory"
ADVISORY_ISSUE_TYPE = "Task"
REVIEW_LABEL = "copilot-risk-review"
REVIEW_STATE = re.compile(r"<!-- dependency-advisory-review:(\{[^\n]*\}) -->")


@dataclass
class Issue:
    key: str
    title: str
    report: str
    review: dict | None = None


def workflow_authored(item):
    return (item.get("user") or {}).get("login") == "github-actions[bot]"


def review_state(body):
    matches = REVIEW_STATE.findall(body)
    if not matches:
        return None
    if len(matches) != 1:
        raise ValueError("Multiple dependency review states in one issue")
    state = json.loads(matches[0])
    if not re.fullmatch(r"[0-9a-f]{64}", state["fingerprint"]):
        raise ValueError("Invalid dependency review fingerprint")
    if not re.fullmatch(r"[0-9a-f]{40}", state["source_revision"]):
        raise ValueError("Invalid dependency review source revision")
    if not isinstance(state["advisory_key"], str):
        raise ValueError("Invalid dependency review advisory key")
    return state


def attach_review_state(issues, findings, repository):
    revision = subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
    source = subprocess.check_output(
        ["git", "ls-tree", "-r", "HEAD", "--", "src", "Cargo.toml", "build.rs"], text=True
    )
    for issue in issues:
        if issue.key not in findings:
            continue
        semantic_findings = []
        for resolution, fields in findings[issue.key]:
            semantic_fields = dict(fields)
            if "labels" in fields:
                semantic_fields["labels"] = [
                    {name: value for name, value in label.items() if name not in {"line", "column"}}
                    for label in fields["labels"]
                ]
            semantic_findings.append((resolution, semantic_fields))
        fingerprint = hashlib.sha256(json.dumps(
            {"source": source, "findings": semantic_findings}, sort_keys=True
        ).encode()).hexdigest()
        issue.review = {
            "advisory_key": issue.key,
            "fingerprint": fingerprint,
            "source_revision": revision,
        }
        state = json.dumps(issue.review, sort_keys=True, separators=(",", ":"))
        issue.report = (
            f"<!-- dependency-advisory-review:{state} -->\n"
            f"**Source revision:** [{revision[:12]}]"
            f"(https://github.com/{repository}/tree/{revision})\n\n{issue.report}"
        )


def graph_lines(node, depth=0):
    package = node["Krate"]
    if not all(isinstance(package[key], str) for key in ("name", "version")):
        raise ValueError("Invalid dependency graph package")
    label = f"{package['name']} v{package['version']}"
    if package.get("kind") in ("dev", "build"):
        label += f" ({package['kind']} dependency)"
    if node.get("repeat"):
        label += " (already shown)"
    lines = ["  " * depth + ("<- " if depth else "") + label]
    for parent in node.get("parents", []):
        lines.extend(graph_lines(parent, depth + 1))
    return lines


def finding_key(fields):
    package = fields["graphs"][0]["Krate"]
    if fields["code"] == "yanked":
        return f"yanked:{package['name']}:{package['version']}"
    advisory = fields["advisory"]
    if not re.fullmatch(r"RUSTSEC-\d{4}-\d{4,}", advisory["id"]):
        raise ValueError(f"Unsupported advisory identifier: {advisory['id']}")
    if advisory["package"] != package["name"]:
        raise ValueError("Advisory package does not match the dependency graph")
    for name in ("title", "description", "date"):
        if not isinstance(advisory[name], str):
            raise ValueError(f"Invalid advisory {name}")
    for name in ("aliases", "references"):
        values = advisory.get(name, [])
        if not isinstance(values, list) or not all(isinstance(value, str) for value in values):
            raise ValueError(f"Invalid advisory {name}")
    return advisory["id"]


def read_scan(directory):
    status = int((directory / "exit-code.txt").read_text())
    if status not in (0, 1):
        raise ValueError(f"Scanner exited abnormally with status {status}")
    events = [
        json.loads(line)
        for line in (directory / "diagnostics.jsonl").read_text().splitlines()
        if line.strip()
    ]
    if any(event["type"] not in {"diagnostic", "summary", "log"} for event in events):
        raise ValueError("Unsupported scanner event type")
    summaries = [event["fields"] for event in events if event["type"] == "summary"]
    if len(summaries) != 1:
        raise ValueError("Missing or duplicate scanner completion summary")
    diagnostics = [
        event["fields"]
        for event in events
        if event["type"] == "diagnostic" and event["fields"]["severity"] == "error"
    ]
    errors = summaries[0]["advisories"]["errors"]
    if type(errors) is not int or errors < 0:
        raise ValueError("Invalid scanner error count")
    if errors != len(diagnostics) or status != int(errors > 0):
        raise ValueError("Scanner status, summary, and diagnostics disagree")

    for event in events:
        if event["type"] == "diagnostic" and event["fields"].get("code") == "index-failure":
            raise ValueError(f"Scanner index failure: {event['fields']['message']}")
        if event["type"] == "diagnostic" and event["fields"]["severity"] == "bug":
            raise ValueError(f"Scanner bug: {event['fields']['message']}")
        if event["type"] == "log" and event["fields"]["level"].lower() == "error":
            raise ValueError(f"Scanner error: {event['fields']['message']}")
    for fields in diagnostics:
        if fields["code"] not in ADVISORY_CODES | {"yanked"}:
            raise ValueError(f"Scanner diagnostic {fields['code']}: {fields['message']}")
        notes = fields.get("notes", [])
        if not isinstance(notes, list) or not all(
            isinstance(note, str) for note in notes
        ):
            raise ValueError("Invalid scanner diagnostic notes")
        finding_key(fields)
        for graph in fields["graphs"]:
            graph_lines(graph)
    return diagnostics


def collect_scans(root, scan_result):
    findings = {}
    failures = []
    for resolution in RESOLUTIONS:
        try:
            diagnostics = read_scan(root / f"dependency-advisories-{resolution}")
            for fields in diagnostics:
                findings.setdefault(finding_key(fields), []).append((resolution, fields))
        except (OSError, ValueError, KeyError, TypeError, IndexError) as error:
            failures.append(f"{resolution}: {error}")
    if scan_result != "success" and not findings and not failures:
        failures.append(
            f"Scan job result was {scan_result} despite clean reports; "
            "inspect setup and artifact upload steps."
        )
    return findings, failures


def bounded(text, limit=12000):
    if len(text) <= limit:
        return text
    text = text[:limit]
    open_fence = None
    for fence in re.findall(r"(?m)^(`{3,}|~{3,})", text):
        if open_fence is None:
            open_fence = fence
        elif fence[0] == open_fence[0] and len(fence) >= len(open_fence):
            open_fence = None
    if open_fence:
        text += "\n" + open_fence
    return text + "\n[Truncated; see the full scan artifact linked from the run.]"


def render_finding(key, occurrences, run_url):
    fields = occurrences[0][1]
    package = fields["graphs"][0]["Krate"]
    lines = [
        "## Dependency finding",
        "",
        "> This is a dependency-version match requiring applicability review, "
        "not confirmation that this library is exploitable.",
        "",
        f"- **Dependency:** `{package['name']}`",
        f"- **Finding:** `{key}`",
        f"- **Classification:** {fields['code']}",
        "- **Resolved versions:** " + ", ".join(
            f"{resolution}: `{occurrence['graphs'][0]['Krate']['version']}`"
            for resolution, occurrence in occurrences
        ),
        f"- **Latest scan:** [workflow run and complete artifacts]({run_url})",
    ]
    if fields["code"] == "yanked":
        title = f"Dependency issue requires triage: {package['name']} {package['version']} (yanked)"
        lines.extend([
            f"- **Source:** [crates.io registry]"
            f"(https://crates.io/crates/{package['name']}/{package['version']})",
            "",
            "### Registry finding",
            "",
            fields["message"],
            "",
            "The resolved version is yanked. Review the registry and upstream releases "
            "for a supported replacement; this is not itself a RustSec advisory.",
        ])
    else:
        advisory = fields["advisory"]
        title = f"Dependency advisory requires triage: {key} ({package['name']})"
        lines.extend([
            f"- **Advisory:** [{key}](https://rustsec.org/advisories/{key})",
            "- **Database:** [RustSec advisory database]"
            "(https://github.com/RustSec/advisory-db)",
            f"- **Advisory date:** {advisory['date']}",
            f"- **Aliases:** {', '.join(advisory.get('aliases', [])) or 'None reported'}",
            f"- **CVSS vector:** {advisory.get('cvss') or 'Not provided by the advisory'}",
        ])
        if advisory.get("url"):
            lines.append(f"- **Upstream announcement/fix:** {advisory['url']}")
        lines.extend(["", f"### {advisory['title']}", "", bounded(advisory["description"])])
        references = advisory.get("references", [])
        if references:
            lines.extend(["", "### Upstream references", ""])
            lines.extend(f"- {reference}" for reference in references)
    lines.extend(["", "### Affected resolutions and dependency paths", ""])
    for resolution, occurrence in occurrences:
        root = occurrence["graphs"][0]["Krate"]
        lines.extend([
            f"#### {resolution}: `{root['name']} {root['version']}`",
            "",
            "Each indented parent depends on the package above it.",
            "",
            "```text",
            bounded("\n\n".join("\n".join(graph_lines(graph)) for graph in occurrence["graphs"])),
            "```",
            "",
        ])
        lines.extend(note for note in occurrence.get("notes", []) if note.startswith("Solution:"))
        lines.append("")
    lines.append(
        "An upstream patched version is remediation guidance, not an automatic "
        "requirement to raise this library's dependency floor."
    )
    return Issue(key, title, bounded("\n".join(lines), limit=50000))


def render_issues(findings, failures, run_url):
    issues = [
        render_finding(key, occurrences, run_url)
        for key, occurrences in sorted(findings.items())
    ]
    if failures:
        issues.append(Issue(
            "scan-failure",
            "Dependency advisory scan failed",
            "## Dependency advisory infrastructure failure\n\n"
            "One or more scans did not complete successfully. This is **not a clean "
            "security scan** and is separate from Nightly test failures.\n\n"
            f"[Workflow logs and available artifacts]({run_url})\n\n"
            "```text\n" + bounded("\n".join(failures)) + "\n```",
        ))
    return issues


def issue_body(issue, repository):
    marker = f"<!-- dependency-advisory:{issue.key} -->"
    body = f"{marker}\n{REPORT_START}\n{issue.report}\n{REPORT_END}\n\n"
    if issue.key == "scan-failure":
        return body + (
            "## Maintainer follow-up\n\n"
            "- [ ] Inspect the failed scan/setup/reporting steps.\n"
            "- [ ] Restore scanning and confirm a complete subsequent run.\n"
        )
    return body + (
        "## Maintainer assessment\n\n"
        "- [ ] Assign a maintainer and assess direct/transitive API reachability, "
        "supported features/targets, and triggering conditions.\n"
        "- [ ] If applicable, link the fix or mitigation; update a dependency floor "
        "only when necessary for safe library operation.\n"
        "- [ ] If not applicable, add a reviewed `deny.toml` exception "
        "with a specific reason.\n"
        "- [ ] Confirm the expected result in a subsequent scan before closing.\n\n"
        "**Assessment / resolution:**\n\n"
        "Record findings here; subsequent runs preserve this section.\n\n"
        "Do not disclose newly discovered vulnerabilities in public comments. "
        f"Follow the [security policy](https://github.com/{repository}/blob/main/SECURITY.md).\n"
    )


def gh(*arguments, body=None):
    result = subprocess.run(
        ["gh", *arguments], input=body, capture_output=True, text=True, check=False
    )
    if result.returncode:
        raise RuntimeError(f"GitHub command failed: {result.stderr.strip()}")
    return result.stdout


def ensure_review_labels(repository):
    pages = json.loads(gh(
        "api", "--method", "GET", "--paginate", "--slurp",
        f"repos/{repository}/labels", "-f", "per_page=100",
    ))
    existing = {label["name"].casefold() for page in pages for label in page}
    definitions = {
        ADVISORY_LABEL: ("5319e7", "Dependency finding requiring applicability triage"),
        REVIEW_LABEL: ("fbca04", "Queue a comment-only Copilot dependency risk assessment"),
    }
    for name, (color, description) in definitions.items():
        if name not in existing:
            gh(
                "label", "create", name, "--repo", repository,
                "--color", color, "--description", description,
            )


def publish_issues(issues, repository):
    if not issues:
        return
    if len({issue.key for issue in issues}) != len(issues):
        raise ValueError("Duplicate advisory keys in the report")
    if any(issue.review for issue in issues):
        ensure_review_labels(repository)
    pages = json.loads(gh(
        "api", "--method", "GET", "--paginate", "--slurp",
        f"repos/{repository}/issues", "-f", "state=all", "-f", "per_page=100",
    ))
    existing = {
        item["number"]: item
        for page in pages for item in page
        if "pull_request" not in item and workflow_authored(item)
    }.values()
    for issue in issues:
        marker = f"<!-- dependency-advisory:{issue.key} -->"
        matches = [item for item in existing if marker in (item["body"] or "")]
        if len(matches) > 1:
            raise ValueError(f"Multiple tracking issues match {issue.key}")
        if not matches:
            labels = ["dependencies"]
            labels.extend(
                [ADVISORY_LABEL, REVIEW_LABEL] if issue.review else ["infrastructure"]
            )
            creation_arguments = [argument for label in labels for argument in ("--label", label)]
            if issue.review:
                creation_arguments.extend(["--type", ADVISORY_ISSUE_TYPE])
            url = gh(
                "issue", "create", "--repo", repository, "--title", issue.title,
                *creation_arguments,
                "--body-file", "-", body=issue_body(issue, repository),
            )
            print(f"Created {issue.key}: {url.strip()}")
            continue
        current = matches[0]
        body = current["body"]
        previous_review = review_state(body) if issue.review else None
        if body.count(REPORT_START) != 1 or body.count(REPORT_END) != 1:
            raise ValueError(f"Tracking issue #{current['number']} has invalid report markers")
        start = body.index(REPORT_START)
        end = body.index(REPORT_END)
        if end < start:
            raise ValueError(f"Tracking issue #{current['number']} has reversed report markers")
        end += len(REPORT_END)
        updated = body[:start] + f"{REPORT_START}\n{issue.report}\n{REPORT_END}" + body[end:]
        number = str(current["number"])
        if updated != body:
            gh("issue", "edit", number, "--repo", repository, "--body-file", "-", body=updated)
        if current["state"] == "closed":
            gh("issue", "reopen", number, "--repo", repository)
        if issue.review:
            updates = ["--add-label", ADVISORY_LABEL]
            if not previous_review or previous_review["fingerprint"] != issue.review["fingerprint"]:
                updates.extend(["--add-label", REVIEW_LABEL])
            if current.get("type") is None:
                updates.extend(["--type", ADVISORY_ISSUE_TYPE])
            gh("issue", "edit", number, "--repo", repository, *updates)
        print(f"Updated {issue.key}: {current['html_url']}")


def report(reports, repository, run_url, scan_result, summary):
    findings, failures = collect_scans(reports, scan_result)
    issues = render_issues(findings, failures, run_url)
    if findings:
        attach_review_state(issues, findings, repository)
    with summary.open("a") as output:
        output.write("# Dependency advisory scans\n\n")
        output.write(f"Unhandled dependency findings: **{len(findings)}**\n\n")
        for issue in issues:
            output.write(f"## {issue.title}\n\n{issue.report}\n\n")
        if not issues:
            output.write("Both dependency resolutions completed with no unhandled findings.\n")
    publish_issues(issues, repository)
    return int(bool(issues))


def main():
    parser = argparse.ArgumentParser(description="Report standalone dependency advisory scans.")
    parser.add_argument("--reports", type=Path, required=True)
    parser.add_argument("--repository", required=True)
    parser.add_argument("--run-url", required=True)
    parser.add_argument("--scan-result", choices=["success", "failure", "cancelled", "skipped"], required=True)
    parser.add_argument("--summary", type=Path, required=True)
    args = parser.parse_args()
    try:
        return report(args.reports, args.repository, args.run_url, args.scan_result, args.summary)
    except (OSError, ValueError, RuntimeError) as error:
        print(f"Dependency advisory reporting failed: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
