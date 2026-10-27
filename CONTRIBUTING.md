# Contributing

This project welcomes contributions and suggestions. Most contributions require you to
agree to a Contributor License Agreement (CLA) declaring that you have the right to,
and actually do, grant us the rights to use your contribution. For details, visit
https://cla.microsoft.com.

When you submit a pull request, a CLA-bot will automatically determine whether you need
to provide a CLA and decorate the PR appropriately (e.g., label, comment). Simply follow the
instructions provided by the bot. You will only need to do this once across all repositories using our CLA.

This project has adopted the [Microsoft Open Source Code of Conduct](https://opensource.microsoft.com/codeofconduct/).
For more information see the [Code of Conduct FAQ](https://opensource.microsoft.com/codeofconduct/faq/)
or contact [opencode@microsoft.com](mailto:opencode@microsoft.com) with any additional questions or comments.

## Dependency advisory triage

The [Dependency advisories workflow](.github/workflows/advisories.yaml) runs daily at
11:30 UTC and can also be started manually. It scans both latest compatible and
minimum direct dependency versions. Its results and notifications are independent
of the [Nightly workflow](.github/workflows/nightly.yaml), and do not gate unrelated PRs.

An unhandled finding opens or updates a dependency-specific issue containing the
advisory source, affected resolved versions, dependency paths, upstream description,
and remediation guidance. Repeated findings update the generated report without
replacing the maintainer's assessment or posting daily comments. A recurring finding
reopens its existing issue. Scanner or reporting failures are not clean scans.

A maintainer should take ownership and assess the affected APIs, transitive callers,
supported features and targets, and triggering conditions. If the library exposes
the affected behavior, fix or mitigate it, raising a dependency floor when necessary.
If the finding does not apply, submit a reviewed exception in
[deny.toml](deny.toml) with the advisory ID, a specific `reason`, and a link to the
assessment. Being a library is not itself a reason to ignore an advisory. Revisit
exceptions when the relevant integrations change and during release preparation.
Close the issue after the fix or assessment is merged and a subsequent scan confirms
the expected result.

Only already-public upstream advisory information belongs in these automated
reports. Follow [the security reporting policy](SECURITY.md) for newly discovered
vulnerabilities in this library.

The reporting helper uses Python's standard library. Run its tests with:

```bash
python3 -m unittest discover -s .github/scripts -p 'test_*.py'
```