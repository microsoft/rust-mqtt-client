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

## Workflow changes

The [PR gate](.github/workflows/pr.yaml) validates every checked-in native and
generated GitHub workflow, including scheduled-only workflows. It checks workflow
syntax, expressions, dependency references, and immutable action pins. The existing
`Check (pinned toolchain)` job also requires workflow validation to succeed, so a
failed or skipped validation cannot silently pass the gate.

Agentic workflow Markdown and generated lock files must remain paired. Validation
checks the recorded compiler version and setup action pin against the independently
pinned CI compiler/runtime pair, then regenerates every agentic workflow in a
temporary checkout. Changes to generated workflows or the action pin cache are
rejected. The source checkout is left unchanged.

When updating a gh-aw runtime action, update the compiler version, checksum, and
matching setup action SHA in the PR gate, then regenerate and commit the Markdown
source, lock file, and pin cache together. Keep action security updates; do not
edit generated lock files independently or disable their checks.

For local validation, install actionlint v1.7.12 and the matching gh-aw compiler
(currently v1.2.2), then run:

```bash
python3 -B .github/scripts/check_workflows.py \
  --compiler /path/to/gh-aw \
  --setup-sha 53d0749e19424a918acd82b4d140264ce48f2f14
```

These checks do not execute scheduled workflows, spend AI credits, or publish
issues or assessments. Upstream behavioral regressions that pass static validation
still require targeted smoke tests or scheduled execution.