"""Opt-in tests for shell integrations maintained outside this repository."""

import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]


def optional_path(variable):
    value = os.environ.get(variable)
    return Path(value).expanduser() if value else None


TEMPLATE = optional_path("REMOTE_HELPERS_FILE")
BASHRC = optional_path("REMOTE_BASHRC_FILE")


@unittest.skipUnless(
    TEMPLATE is not None
    and TEMPLATE.is_file()
    and shutil.which("chezmoi")
    and shutil.which("zsh"),
    "set REMOTE_HELPERS_FILE to test an external helper template",
)
class ChezmoiJobmonTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.suite_temp = tempfile.TemporaryDirectory()
        cls.rendered = Path(cls.suite_temp.name) / "remote.zsh"
        overrides = {"sshHpcHost": "test-hpc"}
        result = subprocess.run(
            [
                "chezmoi",
                "--override-data",
                json.dumps(overrides),
                "execute-template",
                "--file",
                str(TEMPLATE),
            ],
            capture_output=True,
            text=True,
            timeout=15,
        )
        if result.returncode:
            raise RuntimeError(result.stderr)
        cls.rendered.write_text(result.stdout)

    @classmethod
    def tearDownClass(cls):
        cls.suite_temp.cleanup()

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.base = Path(self.temp.name)
        self.capture = self.base / "ssh.json"
        fake = self.base / "ssh"
        fake.write_text(
            "#!" + sys.executable + "\nimport json,os,sys\n"
            "with open(os.environ['SSH_CAPTURE'],'a') as stream: "
            "stream.write(json.dumps({'args':sys.argv[1:], 'request':sys.stdin.read()})+'\\n')\n"
            "sys.stdout.write(os.environ.get('SSH_OUTPUT',''))\n"
            "sys.exit(int(os.environ.get('SSH_STATUS','0')))\n"
        )
        fake.chmod(0o700)
        self.env = {
            key: value
            for key, value in os.environ.items()
            if not key.startswith("HPC_JOBMON_")
        }
        self.env.update(
            PATH=str(self.base) + os.pathsep + os.environ["PATH"],
            SSH_CAPTURE=str(self.capture),
        )

    def tearDown(self):
        self.temp.cleanup()

    def run_zsh(self, command):
        return subprocess.run(
            ["zsh", "-f", "-c", 'source "$1"; ' + command, "test", str(self.rendered)],
            env=self.env,
            capture_output=True,
            text=True,
            timeout=15,
        )

    def calls(self):
        return [json.loads(line) for line in self.capture.read_text().splitlines()]

    def test_source_is_quiet_and_defaults_to_hpc(self):
        result = self.run_zsh('print -r -- "$HPC_JOBMON_HOST"')
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout, "test-hpc\n")
        self.assertFalse(self.capture.exists())
        quiet = self.run_zsh(":")
        self.assertEqual((quiet.stdout, quiet.stderr), ("", ""))

    def test_jobs_and_watch_use_fixed_command_and_json_stdin(self):
        result = self.run_zsh("hpc-jwatch 123_7 --json --interval 2 --count 3 --days 7")
        self.assertEqual(result.returncode, 0, result.stderr)
        call = self.calls()[0]
        self.assertEqual(len(self.calls()), 1)
        self.assertEqual(call["args"][-2], "test-hpc")
        self.assertEqual(
            json.loads(call["request"]),
            {
                "operation": "watch",
                "job_id": "123_7",
                "json": True,
                "interval": 2,
                "count": 3,
                "days": 7,
            },
        )
        self.assertIn("$HOME/.local/libexec/jobmon/jobmon.py", call["args"][-1])
        self.assertNotIn("123_7", call["args"][-1])
        self.assertIn("-T", call["args"])
        self.assertNotIn("-t", call["args"])
        self.assertNotIn("-S", call["args"])

    def test_invalid_jobs_options_and_empty_hosts_do_not_connect(self):
        for command in (
            'hpc-job "1; id"',
            'hpc-job "1.batch"',
            'hpc-job "1+0"',
            "hpc-jwatch 1 --interval 0",
            "hpc-jwatch 1 --interval 86401",
            "hpc-jwatch 1 --count 1000001",
            "hpc-jreport 1 --days 3651",
            "hpc-jobs --days 1",
            "HPC_JOBMON_HOST='' hpc-jobs",
        ):
            with self.subTest(command=command):
                result = self.run_zsh(command)
                self.assertEqual(result.returncode, 2, result.stderr)
                self.assertFalse(self.capture.exists())

    def test_override_and_remote_exit_status_are_preserved(self):
        self.env.update(HPC_JOBMON_HOST="test-other", SSH_STATUS="23")
        result = self.run_zsh("hpc-jreport 123 --json")
        self.assertEqual(result.returncode, 23)
        self.assertEqual(self.calls()[0]["args"][-2], "test-other")
        self.env["SSH_STATUS"] = "255"
        self.assertEqual(self.run_zsh("hpc-jdoctor").returncode, 255)

    def test_fetch_private_archive_without_changing_umask(self):
        self.env.update(
            SSH_OUTPUT="test archive bytes",
            FETCH_DEST=str(self.base / "downloads with spaces"),
        )
        result = self.run_zsh('umask 022; hpc-jfetch 123 "$FETCH_DEST"; umask')
        self.assertEqual(result.returncode, 0, result.stderr)
        path, mask = result.stdout.splitlines()
        self.assertEqual(mask, "022")
        self.assertEqual(Path(path).read_text(), "test archive bytes")
        self.assertEqual(Path(path).stat().st_mode & 0o777, 0o600)
        self.assertEqual(
            json.loads(self.calls()[0]["request"]),
            {"operation": "fetch", "job_id": "123"},
        )

    def test_failed_fetch_retains_partial_and_status(self):
        self.env.update(SSH_STATUS="255", FETCH_DEST=str(self.base / "downloads"))
        result = self.run_zsh('hpc-jfetch 123 "$FETCH_DEST"')
        self.assertEqual(result.returncode, 255)
        self.assertEqual(len(list((self.base / "downloads").glob("*.partial"))), 1)

    def test_deployment_is_explicit_and_uses_selected_host(self):
        script = self.base / "tools/shell/deploy-jobmon.sh"
        script.parent.mkdir(parents=True)
        script.write_text('#!/bin/bash\nprintf "%s\\n" "$@"\n')
        self.env["JOBMON_TEST_TOOLS_DIR"] = str(script.parent.parent)
        prefix = "HPC_JOBMON_TOOLS_DIR=$JOBMON_TEST_TOOLS_DIR"
        self.assertEqual(self.run_zsh(prefix + " hpc-jdeploy").stdout, "test-hpc\n")
        self.assertEqual(
            self.run_zsh(prefix + " hpc-jdeploy --apply").stdout,
            "test-hpc\n--apply\n",
        )
        self.assertEqual(
            self.run_zsh(prefix + " hpc-jdeploy --apply unexpected").returncode,
            2,
        )
        self.assertFalse(self.capture.exists())


@unittest.skipUnless(
    BASHRC is not None and BASHRC.is_file(),
    "set REMOTE_BASHRC_FILE to test an external cluster bashrc",
)
class RemoteBashrcTests(unittest.TestCase):
    def test_noninteractive_source_does_nothing(self):
        result = subprocess.run(
            [
                "bash",
                "--noprofile",
                "--norc",
                "-c",
                'source "$1"; declare -F jobmon || :',
                "test",
                str(BASHRC),
            ],
            capture_output=True,
            text=True,
            timeout=5,
        )
        self.assertEqual((result.returncode, result.stdout, result.stderr), (0, "", ""))

    def test_block_is_lazy_and_forwards_arguments_and_status(self):
        content = BASHRC.read_text()
        block = content.split("# BEGIN personal jobmon\n", 1)[1].split(
            "# END personal jobmon", 1
        )[0]
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory)
            remote = base / "remote account"
            script = remote / ".local/libexec/jobmon/jobmon.py"
            script.parent.mkdir(parents=True)
            script.write_text("# test entry point\n")
            env_file = remote / ".config/jobmon/env.sh"
            env_file.parent.mkdir(parents=True)
            env_file.write_text('export JOBMON_PYTHON="$JOBMON_TEST_INTERPRETER"\n')
            interpreter = base / "interpreter"
            interpreter.write_text('#!/bin/bash\nprintf "%s\\n" "$@"\nexit 23\n')
            interpreter.chmod(0o700)
            fragment = base / "fragment.bash"
            # A task-specific simulated home avoids touching real shell configuration.
            fragment.write_text(block.replace("$HOME", "${JOBMON_TEST_HOME}"))
            env = dict(
                os.environ,
                JOBMON_TEST_HOME=str(remote),
                JOBMON_TEST_INTERPRETER=str(interpreter),
            )
            result = subprocess.run(
                [
                    "bash",
                    "--noprofile",
                    "--norc",
                    "-c",
                    'source "$1"; jobmon report 123_7 --json "a b"',
                    "test",
                    str(fragment),
                ],
                env=env,
                capture_output=True,
                text=True,
                timeout=5,
            )
            self.assertEqual(result.returncode, 23)
            self.assertEqual(
                result.stdout.splitlines(),
                [str(script), "report", "123_7", "--json", "a b"],
            )
            quiet = subprocess.run(
                [
                    "bash",
                    "--noprofile",
                    "--norc",
                    "-c",
                    'source "$1"; test -z "${JOBMON_PYTHON:-}"',
                    "test",
                    str(fragment),
                ],
                env={
                    key: value for key, value in env.items() if key != "JOBMON_PYTHON"
                },
                capture_output=True,
                text=True,
            )
            self.assertEqual(
                (quiet.returncode, quiet.stdout, quiet.stderr), (0, "", "")
            )


if __name__ == "__main__":
    unittest.main()
