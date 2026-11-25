# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import json
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

import check_workflows as workflows


class GeneratedWorkflowTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name) / "source"
        self.snapshot = Path(temporary.name) / "snapshot"
        self.directory = self.root / ".github/workflows"
        self.directory.mkdir(parents=True)
        (self.snapshot / ".github/workflows").mkdir(parents=True)
        self.source = self.directory / "review.md"
        self.source.write_text("---\nengine: copilot\n---\nAssess findings.\n")
        self.lock = self.source.with_suffix(".lock.yml")
        self.sha = "a" * 40
        self.expected = (
            f'# gh-aw-metadata: {json.dumps({"compiler_version": "v1.2.2"})}\n'
            f'# gh-aw-manifest: {json.dumps({"actions": [{"repo": "github/gh-aw-actions/setup", "sha": self.sha}]})}\n'
            f"jobs:\n  activation:\n    steps:\n      - uses: github/gh-aw-actions/setup@{self.sha}\n"
        )
        self.lock.write_text(self.expected)
        self.calls = []
        compiler = patch.object(workflows.subprocess, "run", side_effect=self.compile)
        self.addCleanup(compiler.stop)
        compiler.start()

    def compile(self, command, *, cwd, check):
        self.calls.append(command)
        (cwd / self.lock.relative_to(self.root)).write_text(self.expected)
        return subprocess.CompletedProcess(command, 0)

    def validate(self):
        workflows.check_generated_workflows(self.root, self.snapshot, "gh-aw", "v1.2.2", self.sha)

    def test_matching_generation_preserves_the_source_workspace(self):
        self.validate()
        self.assertEqual(self.lock.read_text(), self.expected)
        self.assertEqual(len(self.calls), 1)

    def test_action_update_without_regeneration_is_rejected(self):
        changed = self.expected.replace(f"setup@{self.sha}", f"setup@{'b' * 40}")
        self.lock.write_text(changed)
        with self.assertRaisesRegex(ValueError, "generated workflow is stale"):
            self.validate()
        self.assertEqual(self.lock.read_text(), changed)

    def test_manifest_update_requires_a_matching_ci_runtime(self):
        changed = self.expected.replace(self.sha, "b" * 40)
        self.lock.write_text(changed)
        with self.assertRaisesRegex(ValueError, "does not match the pinned CI compiler/runtime pair"):
            self.validate()
        self.assertEqual(self.calls, [])
        self.assertEqual(self.lock.read_text(), changed)

    def test_compiler_upgrade_requires_a_matching_ci_compiler(self):
        self.lock.write_text(self.expected.replace("v1.2.2", "v1.3.0"))
        with self.assertRaisesRegex(ValueError, "requires gh-aw v1.3.0, but CI uses v1.2.2"):
            self.validate()
        self.assertEqual(self.calls, [])

    def test_missing_generated_workflow_is_rejected(self):
        self.lock.unlink()
        with self.assertRaisesRegex(ValueError, "missing generated workflow"):
            self.validate()

    def test_plain_markdown_is_not_treated_as_a_workflow(self):
        (self.directory / "README.md").write_text("# Workflow documentation\n")
        self.validate()
        self.assertEqual(len(self.calls), 1)

    def test_orphan_generated_workflow_is_rejected(self):
        self.source.unlink()
        with self.assertRaisesRegex(ValueError, "no Markdown source"):
            self.validate()


if __name__ == "__main__":
    unittest.main()
