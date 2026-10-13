"""Exercise release upgrades and wrapper installation without a cluster."""

import contextlib
import hashlib
import io
import json
import os
from pathlib import Path
import shlex
import shutil
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "jobmon"))
import jobmon


class DeploymentTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.base = Path(self.temp.name)
        self.remote = self.base / "remote account"
        self.remote.mkdir()
        self.bundle = self.base / "source bundle"
        for directory in ("jobmon", "shell", "examples"):
            shutil.copytree(ROOT / directory, self.bundle / directory,
                            ignore=shutil.ignore_patterns("__pycache__"))
        self.source_wrapper = self.bundle / "examples/monitored-job.sh"
        self.wrapper = self.remote / ".local/bin/monitored-job.sh"
        self.current = self.remote / ".local/libexec/jobmon/current"
        self.config = self.remote / ".config/jobmon/env.sh"
        self.config.parent.mkdir(parents=True)
        self.config.write_text("export JOBMON_PYTHON=" + shlex.quote(sys.executable) + "\n")
        fake = self.base / "ssh"
        fake.write_text(
            "#!" + sys.executable + "\n"
            "import io,os,subprocess,sys,tarfile\n"
            "command = sys.argv[-1].replace('$HOME', '${REMOTE_TEST_HOME}')\n"
            "if 'tar -xf' in command and os.environ.get('TEST_CORRUPT_WRAPPER'):\n"
            "    output = io.BytesIO()\n"
            "    with tarfile.open(fileobj=io.BytesIO(sys.stdin.buffer.read())) as src:\n"
            "        with tarfile.open(fileobj=output,mode='w') as dst:\n"
            "            for member in src:\n"
            "                data = src.extractfile(member).read()\n"
            "                if member.name == 'monitored-job.sh': data += b'\\n# corrupted\\n'\n"
            "                member.size = len(data)\n"
            "                dst.addfile(member,io.BytesIO(data))\n"
            "    result = subprocess.run(['/bin/sh','-c',command],input=output.getvalue())\n"
            "else:\n"
            "    result = subprocess.run(['/bin/sh','-c',command])\n"
            "sys.exit(result.returncode)\n"
        )
        fake.chmod(0o700)
        self.env = {key: value for key, value in os.environ.items()
                    if not key.startswith(("JOBMON_", "HPC_JOBMON_", "TEST_CORRUPT_"))}
        self.env.update(PATH=str(self.base) + os.pathsep + os.environ["PATH"],
                        REMOTE_TEST_HOME=str(self.remote))

    def deploy(self):
        return subprocess.run(["bash", str(self.bundle / "shell/deploy-jobmon.sh"),
                               "test-alias", "--apply"],
                              env=self.env, capture_output=True, text=True, timeout=15)

    def install(self):
        result = self.deploy()
        self.assertEqual(result.returncode, 0, result.stderr + result.stdout)
        return self.current.resolve()

    def assert_no_staging_left(self):
        self.assertEqual(list(self.current.parent.glob("releases/.deploy.*")), [])
        self.assertEqual(list(self.wrapper.parent.glob(".monitored-job.sh.link.*")), [])

    def test_repeat_and_wrapper_upgrade_switch_one_release(self):
        first = self.install()
        self.assertEqual(self.install(), first)
        original = (first / "monitored-job.sh").read_bytes()
        self.source_wrapper.write_bytes(original + b"\n# wrapper update\n")
        second = self.install()
        self.assertNotEqual(first, second)
        self.assertEqual((first / "monitored-job.sh").read_bytes(), original)
        self.assertEqual(self.wrapper.resolve(), second / "monitored-job.sh")
        self.assertEqual(self.wrapper.read_bytes(), self.source_wrapper.read_bytes())
        for name in ("jobmon.py", "collect.py", "slurm_backend.py"):
            self.assertEqual((self.current.parent / name).resolve(), second / name)
        self.assert_no_staging_left()

    def test_identical_manual_copy_migrates_without_changing_bin_permissions(self):
        self.wrapper.parent.mkdir(parents=True)
        self.wrapper.parent.chmod(0o755)
        self.wrapper.write_bytes(self.source_wrapper.read_bytes())
        before = self.config.read_bytes()
        release = self.install()
        self.assertTrue(self.wrapper.is_symlink())
        self.assertEqual(self.wrapper.resolve(), release / "monitored-job.sh")
        self.assertEqual(self.wrapper.stat().st_mode & 0o777, 0o700)
        self.assertEqual(self.wrapper.parent.stat().st_mode & 0o777, 0o755)
        self.assertEqual(self.config.read_bytes(), before)

    def test_python_only_release_upgrades_to_include_wrapper(self):
        legacy = self.current.parent / "releases/legacy"
        legacy.mkdir(parents=True)
        manifest = []
        for name in ("jobmon.py", "slurm_backend.py", "collect.py"):
            data = (ROOT / "jobmon" / name).read_bytes()
            (legacy / name).write_bytes(data)
            manifest.append(hashlib.sha256(data).hexdigest() + "  " + name + "\n")
            (self.current.parent / name).symlink_to("current/" + name)
        (legacy / "SHA256SUMS").write_text("".join(manifest))
        self.current.symlink_to("releases/legacy")
        release = self.install()
        self.assertNotEqual(release, legacy)
        self.assertEqual(self.wrapper.resolve(), release / "monitored-job.sh")
        self.assertTrue((legacy / "jobmon.py").is_file())
        self.assertFalse((legacy / "monitored-job.sh").exists())
        self.assertEqual((self.current.parent / "collect.py").resolve(), release / "collect.py")

    def test_existing_files_directories_and_unrelated_symlinks_are_preserved(self):
        release = self.install()
        other = self.base / "user-wrapper.sh"
        other.write_bytes(self.source_wrapper.read_bytes())
        self.source_wrapper.write_bytes(self.source_wrapper.read_bytes() + b"\n# upgrade\n")
        for kind in ("file", "directory", "symlink", "dangling symlink"):
            with self.subTest(kind=kind):
                self.wrapper.unlink()
                if kind == "file":
                    self.wrapper.write_text("# custom wrapper\n")
                elif kind == "directory":
                    self.wrapper.mkdir()
                else:
                    self.wrapper.symlink_to(other if kind == "symlink" else self.base / "missing")
                result = self.deploy()
                self.assertNotEqual(result.returncode, 0)
                self.assertIn("refusing", result.stderr)
                self.assertEqual(self.current.resolve(), release)
                if kind == "file":
                    self.assertEqual(self.wrapper.read_text(), "# custom wrapper\n")
                elif kind == "directory":
                    self.assertTrue(self.wrapper.is_dir())
                else:
                    self.assertTrue(self.wrapper.is_symlink())
                    self.assertEqual(os.readlink(self.wrapper),
                                     str(other if kind == "symlink" else self.base / "missing"))
                self.assertEqual(other.read_bytes(), (release / "monitored-job.sh").read_bytes())
                self.assert_no_staging_left()
                if kind == "directory":
                    self.wrapper.rmdir()
                else:
                    self.wrapper.unlink()
                self.wrapper.symlink_to("../libexec/jobmon/current/monitored-job.sh")

    def test_invalid_wrapper_syntax_preserves_active_release(self):
        release = self.install()
        self.source_wrapper.write_text("#!/bin/bash\nif then\n")
        result = self.deploy()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("syntax error", result.stderr)
        self.assertEqual(self.current.resolve(), release)
        self.assertEqual(self.wrapper.resolve(), release / "monitored-job.sh")
        self.assert_no_staging_left()

    def test_corrupted_wrapper_fails_checksum_before_activation(self):
        release = self.install()
        self.env["TEST_CORRUPT_WRAPPER"] = "1"
        result = self.deploy()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("monitored-job.sh: FAILED", result.stdout)
        self.assertEqual(self.current.resolve(), release)
        self.assertEqual(self.wrapper.resolve(), release / "monitored-job.sh")
        self.assert_no_staging_left()

    @unittest.skipIf(os.geteuid() == 0, "root ignores the directory permissions this test relies on")
    def test_first_install_activates_current_before_the_entry_points(self):
        # Fail the wrapper link, which runs after current and the entry points.
        bin_dir = self.wrapper.parent
        bin_dir.mkdir(parents=True)
        bin_dir.chmod(0o500)
        self.addCleanup(bin_dir.chmod, 0o700)
        result = self.deploy()
        self.assertNotEqual(result.returncode, 0)
        self.assertTrue(self.current.is_symlink(), result.stderr)
        release = self.current.resolve()
        self.assertTrue((release / "SHA256SUMS").is_file())
        for name in ("jobmon.py", "collect.py", "slurm_backend.py"):
            self.assertEqual((self.current.parent / name).resolve(), release / name)
        bin_dir.chmod(0o700)
        self.assertEqual(self.install(), release)
        self.assertEqual(self.wrapper.resolve(), release / "monitored-job.sh")

    def test_unrelated_entry_point_file_is_refused_and_identical_copy_migrates(self):
        release = self.install()
        entry = self.current.parent / "jobmon.py"
        entry.unlink()
        entry.write_text("# local edit\n")
        result = self.deploy()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("refusing to overwrite unrelated file", result.stderr)
        self.assertEqual(entry.read_text(), "# local edit\n")
        self.assertFalse(entry.is_symlink())
        self.assertEqual(self.current.resolve(), release)
        self.assert_no_staging_left()

        entry.unlink()
        entry.write_bytes((self.bundle / "jobmon/jobmon.py").read_bytes())
        self.assertEqual(self.install(), release)
        self.assertTrue(entry.is_symlink())
        self.assertEqual(entry.resolve(), release / "jobmon.py")

    def test_existing_release_with_a_different_manifest_is_named(self):
        names = ("jobmon.py", "slurm_backend.py", "collect.py", "monitored-job.sh")
        paths = {name: self.bundle / ("examples" if name.endswith(".sh") else "jobmon") / name
                 for name in names}
        manifest = "".join(hashlib.sha256(paths[name].read_bytes()).hexdigest() + "  " + name + "\n"
                           for name in names)
        release = self.current.parent / "releases" / hashlib.sha256(manifest.encode()).hexdigest()
        release.mkdir(parents=True)
        (release / "SHA256SUMS").write_text(manifest.replace("a", "b", 1))
        result = self.deploy()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("holds a different manifest", result.stderr)
        self.assertFalse(self.current.exists())
        self.assert_no_staging_left()

    def test_unrelated_python_entry_symlinks_are_preserved(self):
        release = self.install()
        other = self.base / "custom.py"
        other.write_text("# custom implementation\n")
        for name in ("jobmon.py", "collect.py", "slurm_backend.py"):
            entry = self.current.parent / name
            for target in (other, self.base / "missing.py", Path("current/../custom.py")):
                with self.subTest(name=name, target=target):
                    entry.unlink()
                    entry.symlink_to(target)
                    result = self.deploy()
                    self.assertNotEqual(result.returncode, 0)
                    self.assertIn("refusing unrelated entry point symlink", result.stderr)
                    self.assertEqual(os.readlink(entry), str(target))
                    self.assertEqual(self.current.resolve(), release)
                    self.assertEqual(other.read_text(), "# custom implementation\n")
                    self.assert_no_staging_left()
                    entry.unlink()
                    entry.symlink_to("current/" + name)

    def test_unrelated_current_symlinks_are_preserved(self):
        release = self.install()
        for target in (self.base / "custom-release", Path("releases/../custom"),
                       Path("releases/nested/custom"), Path("releases/.."), Path("releases/")):
            with self.subTest(target=target):
                self.current.unlink()
                self.current.symlink_to(target)
                result = self.deploy()
                self.assertNotEqual(result.returncode, 0)
                self.assertIn("refusing unrelated current symlink", result.stderr)
                self.assertEqual(os.readlink(self.current), str(target))
                self.assert_no_staging_left()
                self.current.unlink()
                self.current.symlink_to("releases/" + release.name)

    def local_deploy(self, *arguments):
        # An ssh that must never run proves --local stays on this machine.
        no_ssh = self.base / "no-ssh"
        no_ssh.mkdir(exist_ok=True)
        marker = self.base / "ssh-was-called"
        (no_ssh / "ssh").write_text("#!/bin/sh\ntouch " + shlex.quote(str(marker)) + "\nexit 99\n")
        (no_ssh / "ssh").chmod(0o700)
        env = dict(self.env, HOME=str(self.remote), PATH=str(no_ssh) + os.pathsep + os.environ["PATH"])
        result = subprocess.run(["bash", str(self.bundle / "shell/deploy-jobmon.sh"), *arguments],
                                env=env, capture_output=True, text=True, timeout=15,
                                stdin=subprocess.DEVNULL)
        self.assertFalse(marker.exists(), "--local must not call ssh")
        return result

    def test_local_install_builds_the_same_release_as_an_ssh_install(self):
        result = self.local_deploy("--local", "--apply")
        self.assertEqual(result.returncode, 0, result.stderr + result.stdout)
        self.assertIn("Host: this machine (--local)", result.stdout)
        release = self.current.resolve()
        self.assertTrue((release / "SHA256SUMS").is_file())
        for name in ("jobmon.py", "collect.py", "slurm_backend.py"):
            self.assertEqual((self.current.parent / name).resolve(), release / name)
        self.assertEqual(self.wrapper.resolve(), release / "monitored-job.sh")
        self.assertEqual(self.wrapper.stat().st_mode & 0o777, 0o700)
        self.assert_no_staging_left()
        # A later deployment over SSH recognizes the managed layout and reuses it.
        self.assertEqual(self.install(), release)

    def test_local_previews_by_default_and_rejects_a_destination(self):
        preview = self.local_deploy("--local")
        self.assertEqual(preview.returncode, 0, preview.stderr)
        self.assertIn("Preview only", preview.stdout)
        self.assertFalse(self.current.parent.exists())
        both = self.local_deploy("--local", "test-alias", "--apply")
        self.assertEqual(both.returncode, 2)
        self.assertIn("either an SSH destination or --local", both.stderr)
        self.assertFalse(self.current.parent.exists())

    def test_deployed_wrapper_is_executable_and_requires_a_workload(self):
        self.install()
        result = subprocess.run([str(self.wrapper)], env=self.env, capture_output=True,
                                text=True, timeout=10)
        self.assertEqual(result.returncode, 2, result.stderr)
        self.assertIn("Usage: monitored-job.sh WORKLOAD_COMMAND", result.stderr)


class WrapperDoctorTests(unittest.TestCase):
    def test_doctor_reports_missing_nonexecutable_installed_and_broken_wrapper(self):
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory)
            wrapper = base / ".local/bin/monitored-job.sh"
            wrapper.parent.mkdir(parents=True)
            release_wrapper = base / "release/monitored-job.sh"
            release_wrapper.parent.mkdir()
            release_wrapper.write_text("#!/bin/bash\n")
            for mode, exists, executable in (("missing", False, False),
                                              ("nonexecutable", True, False),
                                              ("installed", True, True),
                                              ("broken", False, False)):
                with self.subTest(mode=mode):
                    if mode == "nonexecutable":
                        wrapper.symlink_to(release_wrapper)
                        release_wrapper.chmod(0o600)
                    elif mode == "installed":
                        release_wrapper.chmod(0o700)
                    elif mode == "broken":
                        release_wrapper.unlink()
                    output = io.StringIO()
                    with patch.object(jobmon.Path, "home", return_value=base), \
                         patch.object(jobmon, "data_root", return_value=base / "absent-state"), \
                         patch.object(jobmon.SlurmBackend, "doctor", return_value={"errors": []}), \
                         patch.dict(os.environ, {}, clear=True), contextlib.redirect_stdout(output):
                        jobmon.execute("doctor", as_json=True)
                    report = json.loads(output.getvalue())
                    self.assertEqual(report["batch_wrapper"],
                                     {"path": str(wrapper), "exists": exists, "executable": executable})
                    self.assertEqual(bool(report["errors"]), not executable)
                    if not executable:
                        self.assertIn("hpc-jdeploy --apply", report["errors"][0])


if __name__ == "__main__":
    unittest.main()
