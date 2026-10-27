"""Regression checks for publishing the repository without local information."""

import getpass
from pathlib import Path
import re
import socket
import unittest


ROOT = Path(__file__).resolve().parents[1]
PUBLIC_DOCUMENTS = (
    *sorted(ROOT.glob("*.md")),
    *sorted((ROOT / "examples").glob("*")),
    *(path for path in (ROOT / "LICENSE",) if path.is_file()),
    *sorted((ROOT / ".github/workflows").glob("*.yml")),
)
TEXT_SUFFIXES = {".md", ".py", ".sh", ".zsh", ".yml", ".yaml", ".toml", ".html", ".svg"}
TEXT_FILENAMES = {"LICENSE"}
# Never walk VCS or tool state: .git/config holds the committer's real address.
EXCLUDED_DIRECTORIES = {".git", ".ruff_cache", ".pytest_cache", "__pycache__", "downloads"}


def repository_text_files():
    for path in sorted(ROOT.rglob("*")):
        if not path.is_file() or not EXCLUDED_DIRECTORIES.isdisjoint(path.parts):
            continue
        if path.suffix in TEXT_SUFFIXES or path.name in TEXT_FILENAMES:
            yield path


class PublicReleaseTests(unittest.TestCase):
    def test_public_docs_have_no_credentials_or_absolute_user_homes(self):
        forbidden = {
            "absolute user home": re.compile(
                r"(?<![A-Za-z0-9_$])/(?:home|Users)/[A-Za-z0-9._-]+(?:/|\b)"
            ),
            "email address": re.compile(
                r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b"
            ),
            "IPv4 address": re.compile(r"(?<![\d.])(?:\d{1,3}\.){3}\d{1,3}(?![\d.])"),
            "private key": re.compile(r"BEGIN [A-Z ]*PRIVATE KEY"),
        }
        for path in PUBLIC_DOCUMENTS:
            text = path.read_text(encoding="utf-8")
            for label, pattern in forbidden.items():
                with self.subTest(path=path.relative_to(ROOT), value=label):
                    self.assertIsNone(
                        pattern.search(text),
                        f"remove {label} from {path.relative_to(ROOT)}",
                    )

    def test_current_machine_identity_is_not_committed(self):
        sensitive_values = {str(Path.home()), socket.gethostname()}
        username = getpass.getuser()
        if username not in {"root", "runner", "ubuntu", "user"}:
            sensitive_values.add(username)

        for path in repository_text_files():
            text = path.read_text(encoding="utf-8")
            for value in sensitive_values:
                with self.subTest(path=path.relative_to(ROOT), value=value):
                    self.assertNotIn(
                        value,
                        text,
                        f"remove local identity from {path.relative_to(ROOT)}",
                    )

    def test_repository_carries_a_license(self):
        license_file = ROOT / "LICENSE"
        self.assertTrue(license_file.is_file(), "a public repository needs an explicit license")
        self.assertIn("MIT License", license_file.read_text(encoding="utf-8"))

    def test_stale_generated_readme_is_not_present(self):
        self.assertFalse(
            (ROOT / "README.pdf").exists(),
            "README.pdf can retain obsolete private text; publish the Markdown sources",
        )

    def test_private_local_artifacts_are_ignored(self):
        ignore_entries = {
            line.strip()
            for line in (ROOT / ".gitignore").read_text(encoding="utf-8").splitlines()
            if line.strip() and not line.lstrip().startswith("#")
        }
        required = {
            ".ruff_cache/",
            ".env",
            ".env.*",
            "*.key",
            "*.pem",
            "/downloads/",
            "jobmon-*.tar.gz",
            "jobmon-*.html",
            "jobmon-*.svg",
            "jobmon-*.zip",
            "*.partial",
            "/README.pdf",
        }
        self.assertFalse(
            required - ignore_entries, "add missing public-release ignores"
        )


if __name__ == "__main__":
    unittest.main()
