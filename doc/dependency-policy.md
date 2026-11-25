# Dependency support and advisory policy

## Why a library needs more than one dependency graph

An application can deploy a particular dependency graph recorded in a lockfile.
This library instead declares dependency requirements in [Cargo.toml](../Cargo.toml),
and consuming applications resolve versions within those requirements alongside
their other dependencies. Our declared lower bounds are therefore compatibility
promises, not just versions used during development.

The library does not commit its root lockfile. Even a committed library lockfile
would not constrain a consuming application's resolution. A clean scan of the
latest dependencies does not establish that older permitted versions are free of
known issues.

## Dependency coverage

The [PR test matrix](../.github/workflows/pr.yaml) checks two dependency sets
(called resolutions by Cargo) on both the minimum supported Rust version (MSRV)
and the current stable compiler:

- **Latest compatible:** the newest versions allowed by our requirements.
- **Minimum direct:** our direct dependencies at the lowest versions their
  requirements permit, using Cargo's `-Z direct-minimal-versions` resolver.
  Transitive dependencies resolve normally. This checks our own lower-bound
  promises without requiring every third-party crate to declare accurate minimums
  throughout the entire graph.

The [Dependency advisories workflow](../.github/workflows/advisories.yaml) scans
both dependency sets with cargo-deny against RustSec. It runs on a schedule because
advisory data can change even when our code does not. It also validates PRs that
change [Cargo.toml](../Cargo.toml), [deny.toml](../deny.toml), or the scan's
workflow, toolchain, or reporting script.

These are two useful samples, not exhaustive coverage of every permitted version,
feature, target, or consumer dependency graph. We rely on SemVer compatibility for
intermediate versions and investigate reported incompatibilities. Applications
should also audit their final resolved dependencies.

## Pull-request advisory validation

Every PR receives a **Dependency advisory validation** check. The workflow compares
the PR's merge base and head using Git. Any change to the root `Cargo.toml` or
`deny.toml` requires both dependency scans, including changes to comments,
formatting, additions, and deletions. Changes to the advisory workflow, pinned
Rust toolchain, or reporting script also trigger both scans.
There is no TOML parsing or section filtering.
Other PRs do not run the advisory scans. Unavailable Git revisions or failed
change detection fail validation rather than silently skipping it.

The scans use the exact PR head, with the same pinned tool and dependency-selection
commands as scheduled runs. The scan workflow sets `NON_PUBLISHING` to `true` for
PRs, follows the **Dry run** (`non_publishing`) option for manual runs, and uses
`false` for scheduled runs. The manual option defaults to `false`. Both modes use
the same [reporter](../.github/scripts/report_advisories.py). In non-publishing mode, it
parses the reports and writes the Actions summary without calling GitHub.
Scan files are still saved as Actions artifacts, and findings or scan failures
still fail PR validation. No issues, comments, or review labels are changed.
Neither PR scans nor manual dry runs start a normal AI review.
The separate AI review workflow can run a test assessment on its own relevant PRs.
Manual runs always scan both resolutions, including dry runs. Publishing runs
must use the default branch; a publishing request on another branch or a tag
fails before any issue updates. Dry runs may use a topic branch or tag.

The publishing job explicitly starts the risk-review workflow after reporting.
It passes the same mode flag, rather than relying on a workflow-completion event
that could accidentally start a publishing review after a dry run.

The final check always runs on PRs. When none of those inputs changed, it succeeds
without running the scans. Otherwise, change detection and both scans must
succeed. Each scan verifies its report with the existing completeness checks
before uploading artifacts. The final check also reads both reports and rejects
any advisory ignore that cargo-deny reports as unused in **both** resolutions,
listing the advisory IDs to remove from [deny.toml](../deny.toml). An ignore needed
by either resolution remains valid, even if the other graph is already patched.
This checks the two saved dependency sets, not every dependency graph a consumer
could resolve.
Skipped, cancelled, failed, or incomplete required scans do not pass validation.
Before closing an advisory through a remediation or exception PR, link a passing
run at its latest head; ordinary PR tests or a local latest-only scan are not a
substitute. The scan verifies the proposed policy or dependency change, not the
correctness of the maintainer's applicability judgment.

## Relationship to GitHub security tooling

GitHub supports Cargo and [imports RustSec into its advisory database](https://docs.github.com/en/code-security/concepts/vulnerability-reporting-and-management/github-advisory-database).
Dependabot alerts use GitHub-reviewed advisories and the dependencies identified
for the repository. They do not automatically exercise our latest/minimum-direct
resolution matrix.

The dedicated scan complements those alerts: Cargo produces the dependency graphs
we intend to check, and cargo-deny reads RustSec directly rather than depending on
an advisory first appearing in GitHub's reviewed database. RustSec also includes
findings such as unsoundness and unmaintained crates, and cargo-deny checks for
yanked versions under our [advisory configuration](../deny.toml).

This is library-specific coverage, not a claim that Cargo-based scanning is
universally stronger than GitHub's security tooling.

## Applicability and remediation

A package/version match requires investigation; it is not by itself proof that
supported use of this library is exploitable. Assess:

- Which resolved versions, features, and targets are affected.
- Whether our direct or transitive API usage can reach the affected behavior.
- What triggering conditions, exposure, and impact exist for supported use.

Dependencies that expose a vulnerability through supported library usage remain
our responsibility. Fix or mitigate applicable findings, including raising a
minimum dependency version when necessary for safe library operation. Do not raise it
merely to make a version-based scan pass when the finding does not apply.

A reviewed non-applicability exception in [deny.toml](../deny.toml) requires a
specific reason explaining why it does not apply. An assessment link is not
required. If evidence is insufficient, keep the finding unresolved rather than
assuming the library is unaffected. Consider compatibility and MSRV effects when
changing dependency requirements. Remove exceptions that are no longer needed
after a dependency is upgraded, removed, or replaced; dependency-update PRs must
pass the unused-ignore check described above.

## Separate advisory triage from test failures

Unhandled findings fail the advisory workflow and open or update tracking issues.
Scanner failures are reported separately as infrastructure problems.
[Nightly](../.github/workflows/nightly.yaml) remains responsible for its actual
test and documentation-link failures; a newly published advisory must not obscure
those failures or turn unrelated PR checks red. Advisory triage is separate from
tests, not optional remediation for applicable vulnerabilities.

## Automated assessments

The [automated Copilot assessment](../.github/workflows/advisory-risk-review.md)
provides evidence and recommendations in an issue comment. It does not decide
exceptions, change dependencies, or replace maintainer review.

There are two review modes:

- **Normal review:** assess pending findings and post assessment comments. Issues
  must have both `dependency-advisory` and `copilot-risk-review` labels. The latter
  is the **pending-review label**; removing it marks the automated review complete,
  not the dependency problem resolved.
- **Test review / dry run** (`non_publishing: true`): validate the review workflow
  using one existing, complete scan report. Generate a new AI assessment without posting or
  editing comments, changing issue labels, or marking the finding reviewed. The
  finding can be closed or already reviewed, so the test still runs when no new
  reviews are pending. It uses the versions and code revision in that saved
  report; it does not reuse an old AI response or scan today's dependencies again.

Same-repository PRs changing the review workflow, action pin cache, or advisory
scripts automatically use test mode. Manual runs can enable `non_publishing`,
including on a topic branch, and use `issue_number` to select an existing report.
Publishing is allowed only on the default branch; a manual publishing request
on another branch or a tag fails before any issue reads or updates.
The workflow computes one effective `NON_PUBLISHING` flag: it is always true for
PRs, and otherwise follows the manual input, which defaults to false. It passes
that flag as `--non-publishing` to both `prepare` and `finalize` in
[review_advisories.py](../.github/scripts/review_advisories.py). Comment processing,
safety checks, and test-result downloads use the same flag. Separate trigger
checks still control which branches may start a run.
Test findings need only the `dependency-advisory` label, not the pending-review
label. Tests still call the real AI service and consume credits within the
workflow's limits.

Test assessments are saved with the Actions run, not posted to issues. They remain
visible in that run's logs and artifacts; test mode is not a privacy feature.
The workflow framework calls these saved, unposted results **staged outputs**.
Generated artifact names and JSON status values keep that terminology.
Verification fails if an assessment is missing, failed, skipped, posted, or does
not match the selected issue and saved advisory details. Test runs never start
another batch of reviews.

The Actions UI uses these job names:

| Job | Purpose |
| --- | --- |
| `prepare` | Select pending findings for normal reviews, or one saved report for a test. |
| `activation` | Prepare the AI instructions, configuration, and run metadata. |
| `agent` | Generate an AI assessment. |
| `detection` | Run safety checks on the generated assessment. |
| `safe_outputs` | Process the comment: post it in normal mode, or save it in test mode. |
| `finalize` | Verify a posted comment and clear its pending-review label, or verify test results without changing issues. |
| `conclusion` | Summarize the workflow results and AI usage. |

### Normal review behavior

- The initial issue description contains the full advisory report and space for
  maintainer reasoning. Copilot adds a separate analysis comment; the scanner does
  not add a comment duplicating the initial report or rewrite existing descriptions.
- A changed finding adds a follow-up scan comment and reapplies the
  `copilot-risk-review` label. The scan's publishing job starts the follow-up review,
  because labels applied with `GITHUB_TOKEN` do not themselves trigger Actions.
- Use the selected report URL: the initial issue description, or the latest follow-up
  when the findings have changed. Inspect library source at that report's scanned
  revision, which may precede the latest run if the relevant inputs are unchanged.
  Consider each version of the advisory data saved by the two dependency scans;
  the current upstream advisory may differ from what the scan saved.
- Unchanged library source, resolved findings, and advisory data reuse the existing
  assessment. A new scan date or run link alone does not trigger reassessment.
  When those inputs meaningfully change, the assessment comment is replaced rather
  than adding another assessment. Historical scan-update comments remain intact.
- Each run reviews at most five findings within its existing time and AI-credit
  budgets. If eligible findings remain beyond that batch, successful finalization
  automatically starts the next run on the default branch. Within the review
  workflow, only the finalization job receives `actions: write` to start another
  batch. The scan's publishing job has that permission to start the initial review.
  The AI job reads repository and issue data; the separate comment-processing
  job posts its assessment.
- Automatic continuation requires every selected assessment to be verified and
  marked reviewed by removing its pending-review label. Failed, missing, unsafe,
  or changed findings stop automatic follow-up runs. Remaining findings retain
  their pending-review labels for a later scan or manual workflow run. A run
  targeting one issue does not select other issues.
- Only reports containing all saved scan evidence are eligible for automated review.
  If evidence exceeds the report size limit, assess the full scan artifacts
  manually rather than accepting an assessment based on truncated evidence.
- Missing generated reports remain pending for a scanner retry or manual review.
  Missing and oversized reports do not prevent other reviewable findings from
  being assessed.
- Remove the pending-review label only after the trusted workflow posts a matching
  assessment. Missing or outdated assessments and safety-check warnings or errors
  do not mark a review complete.

Report newly discovered, non-public vulnerabilities according to the
[security reporting policy](../SECURITY.md), not in public tracking issues.
