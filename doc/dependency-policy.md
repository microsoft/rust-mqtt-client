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

The [PR test matrix](../.github/workflows/pr.yaml) checks two resolutions on both
the minimum supported Rust version (MSRV) and the current stable compiler:

- **Latest compatible:** the newest versions allowed by our requirements.
- **Minimum direct:** our direct dependencies at the lowest versions their
  requirements permit, using Cargo's `-Z direct-minimal-versions` resolver.
  Transitive dependencies resolve normally. This checks our own lower-bound
  promises without requiring every third-party crate to declare accurate minimums
  throughout the entire graph.

The [Dependency advisories workflow](../.github/workflows/advisories.yaml) scans
both resolutions with cargo-deny against RustSec. It runs on a schedule because
advisory data can change even when our code does not.

These are two useful samples, not exhaustive coverage of every permitted version,
feature, target, or consumer dependency graph. We rely on SemVer compatibility for
intermediate versions and investigate reported incompatibilities. Applications
should also audit their final resolved dependencies.

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
dependency floor when necessary for safe library operation. Do not raise a floor
merely to make a version-based scan pass when the finding does not apply.

A reviewed non-applicability exception in [deny.toml](../deny.toml) requires a
specific reason explaining why it does not apply. An assessment link is not
required. If evidence is insufficient, keep the finding unresolved rather than
assuming the library is unaffected. Consider compatibility and MSRV effects when
changing dependency requirements.

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

- Use the current bot-owned scan comment, not the initial snapshot in the issue
  description, and inspect library source at the scanned revision. Consider every
  distinct advisory snapshot captured by the two resolutions; current upstream
  advisory text may differ from the captured evidence.
- Only reports with complete captured evidence are eligible for automated review.
  If evidence exceeds the report size limit, assess the full scan artifacts
  manually rather than accepting an assessment based on truncated evidence.
- Missing scan comments remain pending for a scanner retry or manual review.
  Missing and oversized reports do not prevent other reviewable findings from
  being assessed.
- Clear pending work only after a matching assessment from the trusted workflow
  is published. Missing, stale, or framework-flagged assessments, including threat
  detections and detection-engine errors, do not complete the request.

Report newly discovered, non-public vulnerabilities according to the
[security reporting policy](../SECURITY.md), not in public tracking issues.
