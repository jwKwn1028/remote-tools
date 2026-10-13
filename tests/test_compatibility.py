"""Enforce the Python 3.8 floor without needing a 3.8 interpreter installed."""

import ast
from pathlib import Path
import re
import unittest

ROOT = Path(__file__).resolve().parents[1]
MINIMUM_VERSION = (3, 8)
SELF = Path(__file__).resolve()
BUILTIN_GENERICS = {"list", "dict", "set", "tuple", "frozenset", "type"}
# Standard-library names that a 3.8 interpreter raises on at runtime, which the
# syntax gate below cannot see because they are ordinary attribute lookups.
NEWER_APIS = re.compile(
    r"\.(?:removeprefix|removesuffix|is_relative_to|is_mount|bit_count|root_dir)\("
    r"|\b(?:zoneinfo|graphlib)\b"
    r"|\bfunctools\.(?:cache|cached_property_clear)\b"
    r"|\bmath\.(?:lcm|nextafter|ulp)\b"
    r"|\bitertools\.pairwise\b")


def source_files():
    for directory in ("jobmon", "tests"):
        for path in sorted((ROOT / directory).glob("*.py")):
            yield path


def annotations(tree):
    for node in ast.walk(tree):
        if isinstance(node, (ast.AnnAssign, ast.arg)) and node.annotation:
            yield node.annotation
        elif isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.returns:
            yield node.returns


class CompatibilityTests(unittest.TestCase):
    def test_sources_parse_under_the_minimum_python(self):
        for path in source_files():
            with self.subTest(path=path.relative_to(ROOT)):
                try:
                    ast.parse(path.read_text(encoding="utf-8"), filename=str(path),
                              feature_version=MINIMUM_VERSION)
                except SyntaxError as exc:
                    self.fail("{} needs syntax newer than Python {}.{}: {}".format(
                        path.relative_to(ROOT), MINIMUM_VERSION[0], MINIMUM_VERSION[1], exc))

    def test_sources_avoid_standard_library_added_after_the_minimum_python(self):
        for path in source_files():
            if path.resolve() == SELF:
                continue
            found = NEWER_APIS.search(path.read_text(encoding="utf-8"))
            with self.subTest(path=path.relative_to(ROOT)):
                self.assertIsNone(found, "{} uses an API newer than Python {}.{}".format(
                    path.relative_to(ROOT), MINIMUM_VERSION[0], MINIMUM_VERSION[1]))

    def test_annotations_avoid_pep604_unions_and_builtin_generics(self):
        # Both parse cleanly on 3.8 and only fail when evaluated, so check the tree.
        for path in source_files():
            tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
            for annotation in annotations(tree):
                for node in ast.walk(annotation):
                    with self.subTest(path=path.relative_to(ROOT), line=annotation.lineno):
                        self.assertFalse(
                            isinstance(node, ast.BinOp) and isinstance(node.op, ast.BitOr),
                            "use typing.Optional or typing.Union instead of a PEP 604 union")
                        self.assertFalse(
                            isinstance(node, ast.Subscript) and isinstance(node.value, ast.Name)
                            and node.value.id in BUILTIN_GENERICS,
                            "use typing generics instead of subscripted builtins")


if __name__ == "__main__":
    unittest.main()
