#!/usr/bin/env python3
"""
Validate that EMR-submitted PySpark scripts stay compatible with Python 3.7.

Amazon EMR release 6.15.0 ships Python 3.7 on-cluster, so these entrypoints
must avoid syntax/runtime features that only work on newer Python versions.
"""

from __future__ import annotations

import ast
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
TARGET_PYTHON = (3, 7)
EMR_SPARK_SCRIPTS = [
    REPO_ROOT / "scripts" / "aggregate.py",
    REPO_ROOT / "scripts" / "ingest.py",
    REPO_ROOT / "scripts" / "merge.py",
    REPO_ROOT / "scripts" / "q2_batch_inference.py",
    REPO_ROOT / "scripts" / "q2_build_features.py",
    REPO_ROOT / "scripts" / "q2_build_features_lite.py",
    REPO_ROOT / "scripts" / "q2_train_model.py",
    REPO_ROOT / "scripts" / "q2_train_model_lite.py",
]
BUILTIN_GENERIC_NAMES = {"dict", "list", "set", "tuple"}
FUTURE_ANNOTATIONS_IMPORT = "annotations"


def _supports_python_37_syntax(source: str, path: Path) -> list[str]:
    try:
        ast.parse(source, filename=str(path), feature_version=TARGET_PYTHON)
    except SyntaxError as exc:
        return [f"{path}: syntax is not compatible with Python 3.7 ({exc.msg})"]
    return []


def _has_future_annotations_import(tree: ast.AST) -> bool:
    for node in getattr(tree, "body", []):
        if not isinstance(node, ast.ImportFrom):
            continue
        if node.module != "__future__":
            continue
        if any(alias.name == FUTURE_ANNOTATIONS_IMPORT for alias in node.names):
            return True
    return False


def _iter_annotation_nodes(tree: ast.AST):
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef):
            if node.returns is not None:
                yield node.returns
            for arg in (
                node.args.posonlyargs
                + node.args.args
                + node.args.kwonlyargs
            ):
                if arg.annotation is not None:
                    yield arg.annotation
            if node.args.vararg and node.args.vararg.annotation is not None:
                yield node.args.vararg.annotation
            if node.args.kwarg and node.args.kwarg.annotation is not None:
                yield node.args.kwarg.annotation
        elif isinstance(node, ast.AsyncFunctionDef):
            if node.returns is not None:
                yield node.returns
            for arg in (
                node.args.posonlyargs
                + node.args.args
                + node.args.kwonlyargs
            ):
                if arg.annotation is not None:
                    yield arg.annotation
            if node.args.vararg and node.args.vararg.annotation is not None:
                yield node.args.vararg.annotation
            if node.args.kwarg and node.args.kwarg.annotation is not None:
                yield node.args.kwarg.annotation
        elif isinstance(node, ast.AnnAssign) and node.annotation is not None:
            yield node.annotation


def _is_builtin_generic_annotation(node: ast.AST) -> bool:
    return (
        isinstance(node, ast.Subscript)
        and isinstance(node.value, ast.Name)
        and node.value.id in BUILTIN_GENERIC_NAMES
    )


def _runtime_annotation_issues(tree: ast.AST, path: Path) -> list[str]:
    if _has_future_annotations_import(tree):
        return []

    issues: list[str] = []
    for annotation in _iter_annotation_nodes(tree):
        if _is_builtin_generic_annotation(annotation):
            annotation_text = ast.unparse(annotation)
            issues.append(
                f"{path}:{annotation.lineno}: runtime annotation "
                f"'{annotation_text}' requires 'from __future__ import annotations' "
                "for Python 3.7 compatibility"
            )
    return issues


def validate_file(path: Path) -> list[str]:
    source = path.read_text(encoding="utf-8")
    issues = _supports_python_37_syntax(source, path)
    if issues:
        return issues

    tree = ast.parse(source, filename=str(path))
    return _runtime_annotation_issues(tree, path)


def main() -> int:
    issues: list[str] = []
    missing_files = [path for path in EMR_SPARK_SCRIPTS if not path.exists()]
    if missing_files:
        for path in missing_files:
            issues.append(f"Missing expected EMR Spark script: {path}")
    else:
        for path in EMR_SPARK_SCRIPTS:
            issues.extend(validate_file(path))

    if issues:
        print("EMR Python compatibility check failed:")
        for issue in issues:
            print(f"- {issue}")
        return 1

    print("EMR Python compatibility check passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
