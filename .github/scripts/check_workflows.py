# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import argparse
import json
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tempfile


SETUP_REPOSITORIES = {
    "github/gh-aw-actions/setup": "action",
    "github/gh-aw/actions/setup": "release",
}


def compile_configuration(lock):
    headers = {}
    for line in lock.read_text(encoding="utf-8").splitlines()[:2]:
        for name in ("metadata", "manifest"):
            prefix = f"# gh-aw-{name}: "
            if line.startswith(prefix):
                value = json.loads(line[len(prefix):])
                if not isinstance(value, dict) or name in headers:
                    raise ValueError(f"{lock}: invalid compiler {name} header")
                headers[name] = value
    if headers.keys() != {"metadata", "manifest"}:
        raise ValueError(f"{lock}: missing compiler metadata or manifest")
    version = headers["metadata"].get("compiler_version")
    if not isinstance(version, str) or not re.fullmatch(r"v\d+\.\d+\.\d+", version):
        raise ValueError(f"{lock}: invalid compiler version")
    actions = headers["manifest"].get("actions")
    if not isinstance(actions, list) or not all(isinstance(action, dict) for action in actions):
        raise ValueError(f"{lock}: invalid action manifest")
    setup = [action for action in actions if action.get("repo") in SETUP_REPOSITORIES]
    if len(setup) != 1:
        raise ValueError(f"{lock}: expected one pinned gh-aw setup action")
    sha = setup[0].get("sha")
    if not isinstance(sha, str) or not re.fullmatch(r"[0-9a-f]{40}", sha):
        raise ValueError(f"{lock}: setup action must use a full commit SHA")
    return version, SETUP_REPOSITORIES[setup[0]["repo"]], sha


def snapshot_workspace(root, destination):
    subprocess.run(
        ["git", "clone", "--quiet", "--no-hardlinks", str(root), str(destination)], check=True,
    )
    remote = subprocess.check_output(
        ["git", "remote", "get-url", "origin"], cwd=root, text=True,
    ).strip()
    subprocess.run(
        ["git", "remote", "set-url", "origin", remote], cwd=destination, check=True,
    )
    changes = subprocess.check_output(["git", "diff", "--binary", "HEAD"], cwd=root)
    if changes:
        subprocess.run(["git", "apply", "--binary"], input=changes, cwd=destination, check=True)
    untracked = subprocess.check_output(
        ["git", "ls-files", "--others", "--exclude-standard", "-z"], cwd=root,
    )
    for name in untracked.split(b"\0"):
        if name:
            relative = Path(name.decode("utf-8"))
            target = destination / relative
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(root / relative, target, follow_symlinks=False)


def check_generated_workflows(root, snapshot, compiler, version, setup_sha):
    directory = root / ".github/workflows"
    for lock in sorted(directory.glob("*.lock.yml")):
        source = lock.with_name(lock.name.removesuffix(".lock.yml") + ".md")
        if not source.is_file():
            raise ValueError(f"{lock}: generated workflow has no Markdown source")
    for source in sorted(directory.glob("*.md")):
        lock = source.with_suffix(".lock.yml")
        if not lock.is_file():
            content = source.read_text(encoding="utf-8").lstrip("\ufeff \t\r\n")
            opening = content.splitlines()[0].strip() if content else ""
            if opening != "---":
                continue
            raise ValueError(f"{source}: missing generated workflow; run gh aw compile")
        compiled_version, mode, sha = compile_configuration(lock)
        if version != compiled_version:
            raise ValueError(
                f"{lock}: requires gh-aw {compiled_version}, but CI uses {version}; "
                "update the compiler version and checksum in pr.yaml",
            )
        if sha != setup_sha:
            raise ValueError(
                f"{lock}: setup action does not match the pinned CI compiler/runtime pair; "
                "update the compiler version, checksum, and setup SHA together",
            )
        subprocess.run([
            compiler, "compile", str(source.relative_to(root)),
            "--action-mode", mode, "--action-tag", sha, "--no-check-update",
        ], cwd=snapshot, check=True)
        generated = snapshot / lock.relative_to(root)
        if generated.read_bytes() != lock.read_bytes():
            raise ValueError(
                f"{lock}: generated workflow is stale; regenerate with gh-aw "
                f"{version} and the matching setup action pin, then commit the result",
            )
    cache = Path(".github/aw/actions-lock.json")
    original = (root / cache).read_bytes() if (root / cache).exists() else None
    regenerated = (snapshot / cache).read_bytes() if (snapshot / cache).exists() else None
    if original != regenerated:
        raise ValueError(f"{cache}: generated action pin cache is stale; regenerate and commit it")


def main():
    parser = argparse.ArgumentParser(description="Validate native and generated GitHub workflows.")
    parser.add_argument("--compiler", required=True, help="Path to the pinned gh-aw compiler binary")
    parser.add_argument("--setup-sha", required=True, help="Matching gh-aw setup action commit SHA")
    arguments = parser.parse_args()
    root = Path.cwd()
    compiler = str(Path(arguments.compiler).resolve())
    try:
        if not re.fullmatch(r"[0-9a-f]{40}", arguments.setup_sha):
            raise ValueError("The matching setup action must use a full commit SHA")
        output = subprocess.check_output([compiler, "version"], text=True).strip()
        prefix = "gh aw version "
        if not output.startswith(prefix):
            raise ValueError(f"Unrecognized compiler version output: {output}")
        version = output[len(prefix):]
        subprocess.run([compiler, "lint", ".github/workflows"], cwd=root, check=True)
        pins = json.loads(subprocess.check_output([
            compiler, "lint", ".github/workflows", "--rules", "action-pin", "--json",
        ], cwd=root, text=True))
        if not isinstance(pins, list):
            raise ValueError("Unrecognized action pin diagnostics")
        if pins:
            raise ValueError(f"Action references must be immutable:\n{json.dumps(pins, indent=2)}")
        with tempfile.TemporaryDirectory(prefix="workflow-validation-") as temporary:
            snapshot = Path(temporary) / "repository"
            snapshot_workspace(root, snapshot)
            check_generated_workflows(root, snapshot, compiler, version, arguments.setup_sha)
        print("All workflow definitions, generated workflows, and action pins are valid.")
    except (OSError, ValueError, subprocess.CalledProcessError) as error:
        print(f"::error::Workflow validation failed: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
