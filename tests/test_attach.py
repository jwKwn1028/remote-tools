"""Late attachment with synthetic Slurm commands and an isolated workload."""

import contextlib
import fcntl
import io
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import Mock, patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "jobmon"))

import collect
import jobmon
from slurm_backend import JobmonError, SlurmBackend, identity
import test_jobmon
from test_jobmon import write_stat


class AttachTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.base = Path(self.temp.name)
        self.root = collect.private_directory(self.base / "state", create=True)
        collect.initialize_storage_probe(self.root, allow_allocation=True)
        self.row = {"job_id": "123", "job_name": "synthetic simulation", "state": "RUNNING",
                    "nodes": "1", "cpus": "8", "elapsed": "1:00", "reason_or_nodes": "node1"}
        self.backend = Mock()
        self.backend.jobs.return_value = [self.row]
        self.backend.allocation_id.return_value = "123"
        self.backend.step_overlap_options.return_value = ["--overlap"]
        self.environment = {"JOBMON_DATA_DIR": str(self.root), "JOBMON_PROCESS_TREE": "1"}

    def workload(self, job_id="123", raw="123"):
        env = dict(os.environ, SLURM_JOB_ID=raw, SLURM_JOB_NUM_NODES="1", **self.environment)
        if "_" in job_id:
            env["SLURM_ARRAY_JOB_ID"], env["SLURM_ARRAY_TASK_ID"] = job_id.split("_", 1)
        process = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)"],
                                   env=env, start_new_session=True)

        def stop():
            if process.poll() is None:
                process.terminate()
                process.wait(timeout=3)

        self.addCleanup(stop)
        return process

    def fake_commands(self, pid, mode="collect"):
        capture = self.base / "srun.json"
        launcher = self.base / "srun"
        launcher.write_text("#!" + sys.executable + "\nimport json,os,pathlib,sys,time\n"
                            "if '--help' in sys.argv: print('--overlap --jobid');sys.exit(0)\n"
                            "if '--version' in sys.argv: print('slurm 23.11.0');sys.exit(0)\n"
                            "payload=sys.stdin.read()\n"
                            "pathlib.Path(os.environ['CAPTURE']).write_text(json.dumps({'args':sys.argv[1:],"
                            "'request':json.loads(payload),'pid':os.getpid()}))\n"
                            + ("sys.stderr.write('synthetic startup failure');sys.exit(23)\n" if mode == "fail" else
                               "time.sleep(60)\n" if mode == "timeout" else
                               "request=json.loads(payload)\n"
                               "os.environ.pop('SLURM_ARRAY_JOB_ID',None);os.environ.pop('SLURM_ARRAY_TASK_ID',None)\n"
                               "os.environ.update(SLURM_JOB_ID=request['job_id_raw'],SLURM_JOB_NUM_NODES='1',"
                               "SLURM_JOB_NODELIST='node1',SLURMD_NODENAME='node1',SLURM_JOB_CPUS_PER_NODE='1')\n"
                               "read_fd,write_fd=os.pipe();os.write(write_fd,payload.encode());os.close(write_fd)\n"
                               "os.dup2(read_fd,0);os.close(read_fd);os.execv(sys.argv[-3],sys.argv[-3:])\n"))
        launcher.chmod(0o700)
        control = self.base / "scontrol"
        control.write_text("#!" + sys.executable + "\nimport sys\n"
                           "print('node1' if sys.argv[1]=='show' else "
                           + repr("PID JOBID STEPID LOCALID GLOBALID\n{} ".format(pid))
                           + "+sys.argv[2].split('.')[0]+' batch 0 0')\n")
        control.chmod(0o700)
        env = dict(self.environment, PATH=str(self.base) + os.pathsep + os.environ["PATH"], CAPTURE=str(capture))
        return env, capture

    def test_attach_collects_original_group_and_stops_without_a_workload_exit_record(self):
        workload = self.workload()
        env, capture = self.fake_commands(workload.pid)
        launched, popen = [], subprocess.Popen

        def start(*args, **kwargs):
            process = popen(*args, **kwargs)
            launched.append(process)
            self.addCleanup(lambda: self.stop_launcher(process))
            return process

        with patch.dict(os.environ, env), patch.object(jobmon.subprocess, "Popen", side_effect=start), \
                contextlib.redirect_stdout(io.StringIO()) as output:
            jobmon.attach_job("123", interval=1, backend=self.backend)
            self.assertTrue(collect.monitoring_active("123"))
            self.assertEqual(jobmon.attachable_jobs(self.backend), [])
        self.assertIn("Collection continues", output.getvalue())
        self.assertIsNone(workload.poll())
        request = json.loads(capture.read_text())
        self.assertIn("--jobid=123", request["args"])
        for option in ("--overlap", "--nodes=1", "--ntasks=1", "--gres=none", "--kill-on-bad-exit=0"):
            self.assertIn(option, request["args"])
        self.assertEqual(request["request"]["cpus"], 8)
        run = self.root / "123" / request["request"]["run_id"]
        meta = collect.read_json(run / "metadata.json")
        self.assertEqual(meta["allocated_cpus_by_node"], {"node1": 8})
        self.assertEqual(meta["job_name"], self.row["job_name"])
        self.assertEqual(meta["attachment"]["process_group"], os.getpgid(workload.pid))
        workload.terminate()
        workload.wait(timeout=3)
        launched[0].wait(timeout=5)
        with patch.dict(os.environ, env):
            self.assertFalse(collect.monitoring_active("123"))
            self.assertEqual(jobmon.attachable_jobs(self.backend), [self.row])
        rows = [json.loads(line) for path in run.glob("*.jsonl") for line in path.read_text().splitlines()]
        self.assertEqual(rows[0]["metrics"]["scope"], "process_tree_pgid")
        self.assertGreater(rows[0]["metrics"]["memory_bytes"], 0)
        self.assertTrue(rows[-1]["final"])
        self.assertIsNone(rows[-1]["metrics"]["cpu_seconds"])
        self.assertFalse((run / "completion.json").exists())

    def test_array_attachment_handles_a_legacy_step_without_array_environment(self):
        workload = self.workload("123_7", "456")
        env, capture = self.fake_commands(workload.pid)
        self.backend.jobs.return_value = [dict(self.row, job_id="123_7")]
        self.backend.allocation_id.return_value = "456"
        self.backend.step_overlap_options.return_value = []
        launched, popen = [], subprocess.Popen

        def start(*args, **kwargs):
            process = popen(*args, **kwargs)
            launched.append(process)
            self.addCleanup(lambda: self.stop_launcher(process))
            return process

        with patch.dict(os.environ, env), patch.object(jobmon.subprocess, "Popen", side_effect=start), \
                contextlib.redirect_stdout(io.StringIO()):
            jobmon.attach_job("123_7", interval=1, backend=self.backend)
        saved = json.loads(capture.read_text())
        self.assertEqual(saved["request"]["job_id_raw"], "456")
        run = self.root / "123_7" / saved["request"]["run_id"]
        meta = collect.read_json(run / "metadata.json")
        self.assertEqual((meta["job_id"], meta["job_id_raw"]), ("123_7", "456"))
        self.assertIsNone(workload.poll())
        workload.terminate()
        workload.wait(timeout=3)
        launched[0].wait(timeout=5)
        rows = [json.loads(line) for path in run.glob("*.jsonl") for line in path.read_text().splitlines()]
        self.assertTrue(all(row["job_id"] == "123_7" and row["job_id_raw"] == "456" for row in rows))
        self.assertTrue(rows[-1]["final"])

    def stop_launcher(self, process):
        if process.poll() is None:
            os.killpg(process.pid, signal.SIGTERM)
            try:
                process.wait(timeout=3)
            except subprocess.TimeoutExpired:
                os.killpg(process.pid, signal.SIGKILL)
                process.wait()

    def test_startup_failure_and_timeout_leave_the_workload_running(self):
        workload = self.workload()
        for mode in ("fail", "timeout"):
            env, capture = self.fake_commands(workload.pid, mode)
            with self.subTest(mode=mode), patch.dict(os.environ, env), \
                    patch.object(jobmon, "ATTACH_START_TIMEOUT", 0.2), self.assertRaises(JobmonError) as caught:
                jobmon.attach_job("123", backend=self.backend)
            if mode == "fail":
                self.assertIn("synthetic startup failure", str(caught.exception))
                self.assertIn("exit 23", str(caught.exception))
            self.assertIsNone(workload.poll())
            pid = json.loads(capture.read_text())["pid"]
            with self.assertRaises(ProcessLookupError):
                os.kill(pid, 0)

    def test_shell_attachment_survives_the_transport_process_exiting(self):
        workload = self.workload()
        env, capture = self.fake_commands(workload.pid)
        queue = self.base / "squeue"
        queue.write_text("#!" + sys.executable + "\nprint(" + repr(
            "123|{}|RUNNING|1:00|8|1|node1|synthetic simulation".format(identity())) + ")\n")
        queue.chmod(0o700)
        ssh = self.base / "ssh"
        ssh.write_text("#!" + sys.executable + "\nimport os,sys\n"
                       "os.execv(sys.executable,[sys.executable," + repr(str(ROOT / "jobmon/jobmon.py"))
                       + ",'_transport'])\n")
        ssh.chmod(0o700)
        env = dict(os.environ, **env, HPC_JOBMON_HOST="test-alias")
        result = subprocess.run(["zsh", "-f", "-c", 'source "$1"; hpc-jattach 123 --interval 1',
                                 "test", str(ROOT / "shell/jobmon.zsh")], env=env,
                                stdin=subprocess.DEVNULL, capture_output=True, text=True, timeout=8)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("Collection continues", result.stdout)
        self.assertIsNone(workload.poll())
        saved = json.loads(capture.read_text())
        self.assertEqual(saved["request"]["interval"], 1)
        os.kill(saved["pid"], 0)

        def stop():
            try:
                os.killpg(saved["pid"], signal.SIGTERM)
            except ProcessLookupError:
                pass

        self.addCleanup(stop)
        run = self.root / "123" / saved["request"]["run_id"]
        workload.terminate()
        workload.wait(timeout=3)
        deadline = time.monotonic() + 4
        while time.monotonic() < deadline:
            rows = [json.loads(line) for path in run.glob("*.jsonl") for line in path.read_text().splitlines()]
            if rows and rows[-1]["final"]:
                break
            time.sleep(0.05)
        self.assertTrue(rows[-1]["final"])
        self.assertFalse((run / "completion.json").exists())

    def test_picker_eligibility_uses_locks_including_legacy_collectors_and_reservations(self):
        directory = collect.private_directory(self.root / "123", create=True)
        locks = [directory / ".attach.lock", directory / "node1.collector.lock"]
        for path in locks:
            with self.subTest(path=path), collect.private_open(path, os.O_RDWR | os.O_CREAT, "r+") as stream:
                fcntl.flock(stream, fcntl.LOCK_EX)
                with patch.dict(os.environ, self.environment):
                    self.assertTrue(collect.monitoring_active("123"))
                    self.assertEqual(jobmon.attachable_jobs(self.backend), [])
        with patch.dict(os.environ, self.environment):
            self.assertFalse(collect.monitoring_active("123"))
            self.assertEqual(jobmon.attachable_jobs(self.backend), [self.row])
        run = collect.private_directory(directory / "run1", create=True)
        collect.write_json(run / "metadata.json", {"schema_version": 1, "uid": os.getuid(), "user": identity(),
            "job_id": "123", "job_id_raw": "123", "run_id": "run1", "expected_nodes": ["node1"],
            "allocated_cpus_by_node": {"node1": 8}, "errors": []})
        with collect.private_open(run / "node1.lock", os.O_RDWR | os.O_CREAT, "r+") as stream:
            fcntl.flock(stream, fcntl.LOCK_EX)
            with patch.dict(os.environ, self.environment):
                self.assertTrue(collect.monitoring_active("123"))
        with patch.dict(os.environ, self.environment):
            self.assertFalse(collect.monitoring_active("123"))

    def test_recent_samples_exclude_collectors_when_remote_locks_are_not_visible(self):
        directory = collect.private_directory(self.root / "123", create=True)
        run = collect.private_directory(directory / "run1", create=True)
        meta = {"schema_version": 1, "uid": os.getuid(), "user": identity(), "job_id": "123",
                "job_id_raw": "123", "run_id": "run1", "expected_nodes": ["node1"],
                "allocated_cpus_by_node": {"node1": 8}, "errors": []}
        collect.write_json(run / "metadata.json", meta)
        with collect.private_open(run / "node1.lock", os.O_RDWR | os.O_CREAT, "r+"):
            pass
        sample = {"schema_version": 1, "job_id": "123", "job_id_raw": "123", "run_id": "run1",
                  "stream_id": "node1--abcdef", "node": "node1", "timestamp": "2026-09-01T00:00:00+00:00",
                  "interval_seconds": 30, "metrics": {"scope": "process_tree_pgid"}, "final": False}
        path = run / "node1--abcdef.jsonl"
        collect.write_json(path, sample)
        now = collect.dt.datetime(2026, 9, 1, tzinfo=collect.dt.timezone.utc).timestamp()
        with patch.dict(os.environ, self.environment), patch.object(collect.time, "time", return_value=now + 20):
            self.assertTrue(collect.monitoring_active("123"))
            self.assertEqual(jobmon.attachable_jobs(self.backend), [])
        with collect.private_open(path, os.O_WRONLY | os.O_APPEND, "a") as stream:
            stream.write('{"unfinished":')
        self.assertTrue(collect.recent_samples(run, meta, now + 20))
        for changes, age in (({}, 100), ({}, -120), ({"final": True}, 20),
                             ({"job_id_raw": "456"}, 20), ({"job_id": "123_7"}, 20),
                             ({"interval_seconds": True}, 20), ({"timestamp": "bad"}, 20),
                             ({"node": "another-node"}, 20), ({"run_id": "other-run"}, 20)):
            with self.subTest(changes=changes, age=age):
                with collect.private_open(path, os.O_WRONLY | os.O_TRUNC, "w") as stream:
                    stream.write(json.dumps(dict(sample, **changes)) + "\n")
                self.assertFalse(collect.recent_samples(run, meta, now + age))
                with patch.dict(os.environ, self.environment), \
                        patch.object(collect.time, "time", return_value=now + age):
                    self.assertEqual(jobmon.attachable_jobs(self.backend), [self.row])

    def test_trusted_storage_symlinks_and_permissions_are_rejected(self):
        directory = collect.private_directory(self.root / "123", create=True)
        external = collect.private_directory(self.base / "external", create=True)
        (directory / "run1").symlink_to(external, target_is_directory=True)
        with patch.dict(os.environ, self.environment), self.assertRaises(JobmonError):
            jobmon.attachable_jobs(self.backend)
        (directory / "run1").unlink()
        (directory / ".attach.lock").symlink_to(self.base / "missing")
        with patch.dict(os.environ, self.environment), self.assertRaises(OSError):
            jobmon.attach_job("123", backend=self.backend)
        (directory / ".attach.lock").unlink()
        directory.chmod(0o755)
        with patch.dict(os.environ, self.environment), self.assertRaises(JobmonError):
            jobmon.attachable_jobs(self.backend)

    def test_compute_request_rejects_wrong_allocation_and_unmeasurable_scope_before_initializing(self):
        request = {"job_id": "123", "job_id_raw": "123", "run_id": "run1", "job_name": "synthetic",
                   "cpus": 8, "interval": 60}
        cases = [dict(request, job_id="456"), dict(request, job_id_raw="456"),
                 dict(request, cpus=True), dict(request, interval=0),
                 dict(request, command="bad"), dict(request, run_id="../invalid")]
        env = dict(self.environment, SLURM_JOB_ID="123", SLURM_JOB_NUM_NODES="1")
        with patch.dict(os.environ, env), patch.object(collect, "initialize") as initialize:
            for case in cases:
                with self.subTest(case=case), self.assertRaises(JobmonError):
                    collect.attach_collect(case, Mock())
            with patch.object(collect, "batch_process", return_value=Mock(group=400)), \
                    patch.object(collect, "discover_cgroup", return_value=(None, "no job cgroup")), \
                    patch.dict(os.environ, JOBMON_PROCESS_TREE="0"), self.assertRaises(JobmonError):
                collect.attach_collect(request, Mock())
            initialize.assert_not_called()

    def test_an_active_collector_prevents_another_run_on_the_same_node(self):
        directory = collect.private_directory(self.root / "123", create=True)
        run = collect.private_directory(directory / "run1", create=True)
        metadata = {"schema_version": 1, "uid": os.getuid(), "user": identity(), "job_id": "123",
                    "job_id_raw": "123", "run_id": "run1", "expected_nodes": ["node1"],
                    "allocated_cpus_by_node": {"node1": 8}, "errors": []}
        collect.write_json(run / "metadata.json", metadata)
        env = dict(self.environment, SLURM_JOB_ID="123", SLURMD_NODENAME="node1")
        with collect.private_open(directory / "node1.collector.lock", os.O_RDWR | os.O_CREAT, "r+") as stream:
            fcntl.flock(stream, fcntl.LOCK_EX)
            with patch.dict(os.environ, env), self.assertRaises(JobmonError):
                collect.collect(run, once=True)
        self.assertEqual(list(run.glob("*.jsonl")), [])

    def test_attached_collector_checks_every_five_seconds_and_saves_every_sixty(self):
        clock, checks, measurements = [0.0], [], []
        stop = Mock()
        stopped = [False]
        stop.is_set.side_effect = lambda: stopped[0]
        stop.set.side_effect = lambda: stopped.__setitem__(0, True)
        stop.wait.side_effect = lambda seconds: clock.__setitem__(0, clock[0] + seconds)
        target = Mock(group=400)
        target.alive.side_effect = lambda: checks.append(clock[0]) or True

        def measure(*args, **kwargs):
            measurements.append(clock[0])
            if len(measurements) == 3:
                stop.set()
            return {"scope": "process_tree_pgid", "cpu_seconds": clock[0], "memory_bytes": 1024,
                    "memory_peak_bytes": None, "errors": []}

        env = dict(self.environment, SLURM_JOB_ID="123", SLURM_JOB_NUM_NODES="1", SLURMD_NODENAME="node1",
                   SLURM_JOB_NODELIST="node1", SLURM_JOB_CPUS_PER_NODE="8")
        with patch.dict(os.environ, env):
            run = collect.initialize(runner=Mock(return_value="node1"))
            with patch.object(collect.time, "monotonic", side_effect=lambda: clock[0]), \
                    patch.object(collect, "discover_cgroup", return_value=(None, "no job cgroup")), \
                    patch.object(collect, "process_tree_sample", side_effect=measure), \
                    patch.object(collect, "utc_now", side_effect=lambda:
                                 "2026-09-01T00:{:02d}:00+00:00".format(int(clock[0] // 60))):
                collect.collect(run, stop=stop, target=target)
        self.assertEqual(measurements, [0, 60, 120])
        self.assertEqual(sorted(set(checks)), list(range(0, 121, 5)))
        rows = [json.loads(line) for path in run.glob("*.jsonl") for line in path.read_text().splitlines()]
        self.assertEqual(len(rows), 3)
        self.assertTrue(all(row["interval_seconds"] == 60 for row in rows))
        self.assertTrue(rows[-1]["final"])

    def test_candidate_listing_ignores_pending_completing_and_multi_node_jobs(self):
        self.backend.jobs.return_value = [self.row, dict(self.row, job_id="456", state="PENDING"),
                                          dict(self.row, job_id="789", state="COMPLETING"),
                                          dict(self.row, job_id="555", nodes="2")]
        with patch.dict(os.environ, self.environment):
            self.assertEqual(jobmon.attachable_jobs(self.backend), [self.row])

    def test_invalid_ineligible_and_duplicate_requests_never_start_a_step(self):
        for job_id, rows in (("../invalid", [self.row]), ("123", []),
                             ("123", [dict(self.row, state="PENDING")]),
                             ("123", [dict(self.row, nodes="2")]),
                             ("123", [dict(self.row, job_id="456")]),
                             ("123", [dict(self.row, cpus="unknown")])):
            with self.subTest(job_id=job_id, rows=rows), patch.dict(os.environ, self.environment), \
                    patch.object(jobmon.subprocess, "Popen") as start, self.assertRaises(JobmonError):
                self.backend.jobs.return_value = rows
                jobmon.attach_job(job_id, backend=self.backend)
            start.assert_not_called()
        self.backend.jobs.return_value = [self.row]
        directory = collect.private_directory(self.root / "123", create=True)
        with collect.private_open(directory / ".attach.lock", os.O_RDWR | os.O_CREAT, "r+") as stream:
            fcntl.flock(stream, fcntl.LOCK_EX)
            with patch.dict(os.environ, self.environment), self.assertRaises(JobmonError):
                jobmon.attach_job("123", backend=self.backend)

    def test_batch_identity_rejects_wrong_job_ambiguous_pids_and_pid_reuse(self):
        proc = self.base / "proc"
        proc.mkdir()
        write_stat(proc, 400, 400)
        entry = proc / "400"
        (entry / "environ").write_bytes(b"SLURM_JOB_ID=123\0")
        runner = Mock(return_value="PID JOBID STEPID LOCALID GLOBALID\n400 123 batch 0 0\n")
        target = collect.batch_process("123", "123", runner, proc)
        self.assertTrue(target.alive())
        self.assertEqual(target.group, 400)
        content = (entry / "stat").read_text().split()
        content[21] = "1"
        (entry / "stat").write_text(" ".join(content))
        self.assertFalse(target.alive())
        (entry / "environ").write_bytes(b"SLURM_JOB_ID=456\0")
        with self.assertRaises(JobmonError):
            collect.batch_process("123", "123", runner, proc)
        for response in ("400 456 batch 0 0", "400 123 0 0 0", "400 123 batch 0 0\n401 123 batch 0 0", ""):
            with self.subTest(response=response), self.assertRaises(JobmonError):
                collect.batch_process("123", "123", Mock(return_value=response), proc)
        runner.assert_called_with(["scontrol", "listpids", "123.batch"])

    def test_array_task_identity_and_foreign_batch_owner_are_verified(self):
        proc = self.base / "proc"
        proc.mkdir()
        write_stat(proc, 400, 400)
        (proc / "400/environ").write_bytes(
            b"SLURM_JOB_ID=456\0SLURM_ARRAY_JOB_ID=123\0SLURM_ARRAY_TASK_ID=7\0")
        runner = Mock(return_value="400 456 batch 0 0\n")
        self.assertTrue(collect.batch_process("123_7", "456", runner, proc).alive())
        with self.assertRaises(JobmonError):
            collect.batch_process("123_8", "456", runner, proc)
        with patch.object(collect.os, "getuid", return_value=os.getuid() + 1), self.assertRaises(JobmonError):
            collect.batch_process("123_7", "456", runner, proc)

    def test_transport_accepts_only_valid_attachment_fields(self):
        request = jobmon.transport_request(io.StringIO('{"operation":"attach","job_id":"123"}'))
        self.assertEqual(request["interval"], 60)
        for extra in ({"interval": 0}, {"interval": True}, {"json": True}, {"command": "bad"}, {"job_id": "1;bad"}):
            value = dict(operation="attach", job_id="123")
            value.update(extra)
            with self.subTest(extra=extra), self.assertRaises(JobmonError):
                jobmon.transport_request(io.StringIO(json.dumps(value)))

    def test_step_sharing_handles_modern_and_verified_legacy_slurm(self):
        for help_text, version, expected in (("--overlap", "slurm 25.05.0", ["--overlap"]),
                                              ("--jobid", "slurm-wlm 19.05.5", []),
                                              ("--jobid", "slurm 20.02.7", [])):
            runner = Mock(side_effect=[help_text, version])
            self.assertEqual(SlurmBackend(runner).step_overlap_options(), expected)
            self.assertEqual(runner.call_args_list[0].args[0], ["srun", "--help"])
        for version in ("unknown", "slurm 20.11.0", "slurm 25.05.0"):
            with self.subTest(version=version), self.assertRaises(JobmonError):
                SlurmBackend(Mock(side_effect=["--jobid", version])).step_overlap_options()

    def test_array_allocation_resolution_is_scoped_and_rejects_ambiguous_or_foreign_rows(self):
        runner = Mock(return_value="456|123_7|{}\n".format(identity()))
        backend = SlurmBackend(runner)
        self.assertEqual(backend.allocation_id("123"), "123")
        runner.assert_not_called()
        self.assertEqual(backend.allocation_id("123_7"), "456")
        for option in ("--user=" + identity(), "--jobs=123_7", "--array", "--format=%A|%i|%u"):
            self.assertIn(option, runner.call_args.args[0])
        for output in ("456|123_7|another-user", "456|123_8|{}".format(identity()),
                       "123_7|123_7|{}".format(identity()), runner.return_value * 2, ""):
            with self.subTest(output=output), self.assertRaises(JobmonError):
                SlurmBackend(Mock(return_value=output)).allocation_id("123_7")

    def test_legacy_array_launch_uses_numeric_id_but_keeps_canonical_history_identity(self):
        env, capture = self.fake_commands(400, mode="fail")
        self.backend.jobs.return_value = [dict(self.row, job_id="123_7")]
        self.backend.allocation_id.return_value = "456"
        self.backend.step_overlap_options.return_value = []
        with patch.dict(os.environ, env), self.assertRaises(JobmonError):
            jobmon.attach_job("123_7", backend=self.backend)
        saved = json.loads(capture.read_text())
        self.assertIn("--jobid=456", saved["args"])
        self.assertNotIn("--overlap", saved["args"])
        self.assertEqual(saved["request"]["job_id"], "123_7")

    def test_startup_diagnostics_are_bounded_and_escape_terminal_controls(self):
        log = self.base / "startup.log"
        with collect.private_open(log, os.O_WRONLY | os.O_CREAT | os.O_EXCL, "w") as stream:
            stream.write("old context\n" + "x" * 10000 + "\nlast error\x1b[31m\n")
        message = str(jobmon.attachment_error("startup failed", log))
        self.assertIn("last error\\x1b[31m", message)
        self.assertNotIn("old context", message)
        self.assertNotIn("\x1b", message)
        self.assertLess(len(message), 5000)


class AttachPickerTests(unittest.TestCase):
    job_table = test_jobmon.TransportTests.job_table
    pick = test_jobmon.TransportTests.pick

    def bulk(self, table, args=("-a",), failure="", failure_code=23):
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory)
            capture = base / "requests.jsonl"
            ssh = base / "ssh"
            ssh.write_text("#!" + sys.executable + "\nimport json,os,sys\n"
                           "request=json.loads(sys.stdin.read())\n"
                           "with open(os.environ['CAPTURE'],'a') as f: f.write(json.dumps(request)+'\\n')\n"
                           "if request['operation']=='attach-jobs': print(os.environ['CANDIDATES'],end='')\n"
                           "else:\n"
                           " print('attached '+request['job_id'])\n"
                           " if request['job_id']==os.environ['FAILURE']: sys.exit(int(os.environ['FAILURE_CODE']))\n")
            ssh.chmod(0o700)
            env = dict(os.environ, PATH=str(base) + os.pathsep + os.environ["PATH"], HPC_JOBMON_HOST="test-alias",
                       CAPTURE=str(capture), CANDIDATES=table, FAILURE=failure, FAILURE_CODE=str(failure_code))
            result = subprocess.run(["zsh", "-f", "-c", 'source "$1"; shift; hpc-jattach "$@"',
                                     "test", str(ROOT / "shell/jobmon.zsh"), *args], env=env,
                                    stdin=subprocess.DEVNULL, capture_output=True, text=True, timeout=5)
            requests = [json.loads(line) for line in capture.read_text().splitlines()] if capture.exists() else []
            return result, requests

    def test_bulk_attachment_uses_filtered_candidates_without_a_terminal_and_deduplicates(self):
        table = self.job_table(("900_0", "RUNNING"), ("900_1", "RUNNING"), ("900_0", "RUNNING"))
        for args, interval in ((("-a",), 60), (("--interval", "30", "--all"), 30)):
            with self.subTest(args=args):
                result, requests = self.bulk(table, args)
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertNotIn("Choose a running job", result.stdout + result.stderr)
                self.assertIn("Attached 2 of 2 eligible jobs", result.stdout)
                self.assertEqual(requests, [{"operation": "attach-jobs"},
                    {"operation": "attach", "job_id": "900_0", "interval": interval},
                    {"operation": "attach", "job_id": "900_1", "interval": interval}])

    def test_bulk_continues_after_a_job_failure_and_empty_lists_succeed(self):
        table = self.job_table(("900_0", "RUNNING"), ("900_1", "RUNNING"), ("900_2", "RUNNING"))
        result, requests = self.bulk(table, failure="900_1")
        self.assertEqual(result.returncode, 1, result.stderr)
        self.assertIn("could not attach job 900_1", result.stderr)
        self.assertIn("Attached 2 of 3 eligible jobs", result.stdout)
        self.assertEqual([item["job_id"] for item in requests[1:]], ["900_0", "900_1", "900_2"])
        result, requests = self.bulk(self.job_table())
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("no unmonitored", result.stdout)
        self.assertEqual(requests, [{"operation": "attach-jobs"}])

    def test_bulk_rejects_invalid_candidates_before_attaching_and_stops_on_interrupt(self):
        result, requests = self.bulk(self.job_table(("900_0", "RUNNING"), ("../invalid", "RUNNING")))
        self.assertEqual(result.returncode, 2)
        self.assertEqual(requests, [{"operation": "attach-jobs"}])
        table = self.job_table(("900_0", "RUNNING"), ("900_1", "RUNNING"))
        result, requests = self.bulk(table, failure="900_0", failure_code=130)
        self.assertEqual(result.returncode, 130)
        self.assertEqual(len(requests), 2)

    def test_attach_options_are_validated_before_any_connection(self):
        for args in (("123", "-a"), ("-a", "--interval", "0"), ("-a", "--interval", "86401"),
                     ("-a", "--interval"), ("-a", "--unknown"), ("../invalid",), ("123", "456")):
            with self.subTest(args=args):
                result, requests = self.bulk(self.job_table(), args)
                self.assertEqual(result.returncode, 2, result.stderr)
                self.assertEqual(requests, [])
        result, requests = self.bulk(self.job_table(), ("123", "--interval", "30"))
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(requests, [{"operation": "attach", "job_id": "123", "interval": 30}])

    def test_attach_picker_uses_same_keys_and_excludes_monitored_jobs(self):
        table = self.job_table(("900_0", "RUNNING"), ("900_1", "RUNNING"), ("900_2", "RUNNING"))
        for keys, chosen in (([b"J", b"\n"], "900_1"), ([b"j", b"j", b"K", b"\r"], "900_1"),
                             ([b"\x1b[B", b"\n"], "900_1")):
            with self.subTest(keys=keys):
                screen, status, requests = self.pick(table, keys, command="hpc-jattach")
                self.assertEqual(status, 0, screen)
                self.assertIn("Choose a running job to attach", screen)
                self.assertEqual(requests, [{"operation": "attach-jobs"},
                                            {"operation": "attach", "job_id": chosen, "interval": 60}])

    def test_attach_picker_cancel_and_no_candidates_start_no_collection(self):
        table = self.job_table(("900_0", "RUNNING"), ("900_1", "RUNNING"))
        for key in (b"q", b"\x03", b"\x1b"):
            screen, status, requests = self.pick(table, [key], command="hpc-jattach")
            self.assertEqual(status, 130, screen)
            self.assertEqual(requests, [{"operation": "attach-jobs"}])
            self.assertIn("terminal: isig echo", screen)
        screen, status, requests = self.pick(self.job_table(), command="hpc-jattach")
        self.assertEqual(status, 1, screen)
        self.assertIn("no unmonitored running single-node jobs", screen)


if __name__ == "__main__":
    unittest.main()
