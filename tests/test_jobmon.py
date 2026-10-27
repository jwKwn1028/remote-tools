import contextlib
import datetime as dt
import io
import json
import os
from pathlib import Path
import pty
import select
import shutil
import signal
import subprocess
import sys
import tarfile
import tempfile
import time
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "jobmon"))

import collect
import jobmon
import slurm_backend
from slurm_backend import (JobmonError, SlurmBackend, cpu_seconds, identity,
                           memory_bytes, normalize_allocation, requested_memory,
                           validate_job_id)


class SchedulerTests(unittest.TestCase):
    def test_job_identity_validation(self):
        for value in ("123", "123_0", "123_17"):
            self.assertEqual(validate_job_id(value), value)
        for value in ("", "0", "01", "-j1", "1.batch", "1+0", "1_[0-3]", "../1", "1;touch /tmp/x", "1\n", None, 1):
            with self.subTest(value=value), self.assertRaises(JobmonError):
                validate_job_id(value)

    def test_cpu_and_memory_normalization(self):
        self.assertEqual(cpu_seconds("1-02:03:04.5"), 93784.5)
        self.assertEqual(cpu_seconds("02:03.5"), 123.5)
        self.assertIsNone(cpu_seconds("Unknown"))
        self.assertEqual(memory_bytes("1.5G"), int(1.5 * 1024 ** 3))
        self.assertEqual(requested_memory("2Gc", 8, 2)["allocation_bytes"], 16 * 1024 ** 3)
        self.assertEqual(requested_memory("4Gn", 8, 2)["allocation_bytes"], 8 * 1024 ** 3)
        self.assertIsNone(requested_memory("4G", 8, 2)["allocation_bytes"])

    def test_queue_names_preserve_separators_and_keep_job_and_user_scope(self):
        calls = []

        def runner(argv):
            calls.append(argv)
            return ("123_7|{user}|RUNNING|1:00|8|1|node1|simulation | phase 2 |\n"
                    "123_8|{user}|RUNNING|1:00|8|1|node1|other task\n"
                    "123_7|another-user|RUNNING|1:00|8|1|node1|foreign job\n").format(user=identity())

        rows = SlurmBackend(runner).jobs("123_7")
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["job_name"], "simulation | phase 2 |")
        self.assertEqual(rows[0]["job_id"], "123_7")
        self.assertEqual(len(calls), 1)
        for argument in ("--local", "--array", "--user=" + identity(), "--jobs=123_7"):
            self.assertIn(argument, calls[0])
        self.assertIn("--format=%i|%u|%T|%M|%C|%D|%R|%j", calls[0])
        with self.assertRaises(JobmonError):
            SlurmBackend(lambda argv: "123_7|user|RUNNING").jobs("123_7")

    def test_jobs_text_escapes_name_controls_and_json_preserves_the_name(self):
        row = {"job_id": "123_7", "state": "RUNNING", "elapsed": "1:00", "cpus": "8", "nodes": "1",
               "reason_or_nodes": "node1", "job_name": "simulation\tphase\x1b[31m"}
        with patch.object(SlurmBackend, "jobs", return_value=[row]), \
                contextlib.redirect_stdout(io.StringIO()) as output:
            jobmon.execute("jobs")
        lines = output.getvalue().splitlines()
        self.assertEqual(len(lines), 2)
        self.assertTrue(lines[0].endswith("\tJOB NAME"))
        self.assertEqual(len(lines[1].split("\t")), 7)
        self.assertTrue(lines[1].endswith("\tsimulation\\tphase\\x1b[31m"))
        self.assertNotIn("\x1b", output.getvalue())
        with patch.object(SlurmBackend, "jobs", return_value=[row]), \
                contextlib.redirect_stdout(io.StringIO()) as output:
            jobmon.execute("jobs", as_json=True)
        self.assertEqual(json.loads(output.getvalue())["jobs"][0]["job_name"], row["job_name"])

    def test_watch_header_prefers_queue_then_latest_accounting_and_saved_names(self):
        queue = {"job_name": "queue simulation", "state": "RUNNING", "elapsed": "1:00", "cpus": "8",
                 "nodes": "1", "reason_or_nodes": "node1"}
        report = {"job_id": "123_7", "scheduler_snapshot_at": "2026-09-03T00:00:00+00:00", "errors": [],
                  "scheduler": {"queue": [queue], "live_steps": [], "errors": [], "accounting": [
                      {"job_name": "older accounting", "submit": "2026-09-01T00:00:00", "state": "COMPLETED",
                       "exit_code": "0:0"},
                      {"job_name": "newer accounting", "submit": "2026-09-02T00:00:00", "state": "COMPLETED",
                       "exit_code": "0:0"}]},
                  "runs": [{"metadata": {"job_name": "older saved", "created_at": "2026-09-01T00:00:00+00:00"}},
                           {"metadata": {"job_name": "newer saved", "created_at": "2026-09-02T00:00:00+00:00"}}]}

        def title():
            return jobmon.watch_header(report, dt.timezone.utc)[0]

        self.assertTrue(title().startswith("Job 123_7 (queue simulation):"))
        report["scheduler"]["queue"] = []
        self.assertTrue(title().startswith("Job 123_7 (newer accounting):"))
        report["scheduler"]["accounting"] = []
        self.assertTrue(title().startswith("Job 123_7 (newer saved):"))
        report["runs"][-1]["metadata"]["job_name"] = "simulation\tα\x1b[2J"
        self.assertIn("(simulation\\tα\\x1b[2J)", title())
        self.assertNotIn("\x1b", title())
        for run in report["runs"]:
            run["metadata"].pop("job_name")
        self.assertTrue(title().startswith("Job 123_7 (name unavailable):"))

    def test_unreadable_memory_request_has_no_invented_scope(self):
        # The trailing letter of an unparseable request is not a per-CPU scope.
        for value in ("abc", "n", "c", "?c", "", None):
            with self.subTest(value=value):
                request = requested_memory(value, 8, 2)
                self.assertIsNone(request["bytes"])
                self.assertIsNone(request["allocation_bytes"])
                self.assertEqual(request["scope"], "unavailable")
        self.assertEqual(requested_memory("2Gc", 8, 2)["scope"], "per_cpu")

    def test_report_numbers_use_readable_units_and_never_unit_an_unavailable_value(self):
        for value, text in ((None, "unavailable"), (0, "0 B"), (1023, "1023 B"), (1024, "1.00 KiB"),
                            (38832472064.0, "36.17 GiB"), (5 * 1024 ** 5, "5120.00 TiB")):
            self.assertEqual(jobmon.display_bytes(value), text)
        self.assertEqual(jobmon.display_cpu(15.78, 6.31), "15.8% (6.3 cores)")
        self.assertEqual(jobmon.display_cpu(None, 2.0), "2.0 cores")
        self.assertEqual(jobmon.display_cpu(None, None, "-"), "-")
        parent = {"JobID": "1", "JobIDRaw": "1", "AllocCPUS": "8", "ElapsedRaw": "100", "TotalCPU": "00:10:00",
                  "ReqMem": "4Gn", "NNodes": "1", "AllocTRES": "mem=4G", "State": "COMPLETED",
                  "ExitCode": "0:0", "Start": "s"}
        accounting = normalize_allocation(parent, [{"JobID": "1.batch", "JobIDRaw": "1.batch", "MaxRSS": "1024M"},
                                                   {"JobID": "1.0", "JobIDRaw": "1.0", "MaxRSS": ""}])
        text = jobmon.format_report({
            "job_id": "1", "generated_at": "t", "scheduler_snapshot_at": "t", "runs": [], "errors": [],
            "history_reason": None,
            "scheduler": {"queue": [], "accounting": [accounting], "live_steps": [], "errors": []}})
        self.assertIn("Requested memory: 4.00 GiB (per_node); allocated memory: 4.00 GiB", text)
        self.assertIn("1.batch MaxRSS: 1.00 GiB", text)
        self.assertIn("1.0 MaxRSS: unavailable (maximum task within step)", text)
        self.assertNotIn("bytes", text)

    def test_elapsed_uses_slurm_layout_and_start_or_falls_back_to_monitoring(self):
        for seconds, text in ((0, "0:00"), (217, "3:37"), (26837, "7:27:17"), (93784, "1-02:03:04"),
                              (-1, "0:00"), (-61, "-"), (None, "-")):
            self.assertEqual(jobmon.display_elapsed(seconds), text)
        snapshot = dt.datetime(2026, 10, 8, 2, 26, 15, tzinfo=dt.timezone.utc)
        start = snapshot.timestamp() - 26837

        def run(created):
            return {"metadata": {"created_at": None if created is None else
                                 dt.datetime.fromtimestamp(created, dt.timezone.utc).isoformat()}}

        def report(runs, state="RUNNING"):
            return {"runs": runs, "scheduler_snapshot_at": snapshot.isoformat(),
                    "scheduler": {"queue": [{"state": state, "elapsed": "7:27:17"}]}}

        current = run(start + 2)
        self.assertEqual(jobmon.run_start(report([current]), current), ("slurm", start))
        # A start after the run began belongs to a later, requeued allocation.
        early = run(start - 3600)
        self.assertEqual(jobmon.run_start(report([early]), early), ("monitoring", start - 3600))
        older, newer = run(start - 7200), run(start + 2)
        self.assertEqual(jobmon.run_start(report([older, newer]), older), ("monitoring", start - 7200))
        self.assertEqual(jobmon.run_start(report([current], "PENDING"), current), ("monitoring", start + 2))
        missing = run(None)
        self.assertEqual(jobmon.run_start(report([missing]), missing), (None, None))

    def test_watch_clock_uses_the_workstation_offset_or_this_hosts_zone(self):
        moment = dt.datetime(2026, 9, 14, 0, 0, 30, tzinfo=dt.timezone.utc).timestamp()
        seoul = dt.timezone(dt.timedelta(minutes=540))
        self.assertEqual(jobmon.clock_time(moment, seoul), "09:00:30")
        self.assertEqual(jobmon.display_zone(seoul), "your workstation's time, UTC+09:00")
        self.assertEqual(jobmon.display_zone(dt.timezone(dt.timedelta(minutes=-330))),
                         "your workstation's time, UTC-05:30")
        previous = os.environ.get("TZ")
        try:
            os.environ["TZ"] = "KST-9"
            time.tzset()
            self.assertEqual(jobmon.clock_time(moment), "09:00:30")
            self.assertEqual(jobmon.display_zone(None), "the cluster's local time, KST UTC+09:00")
        finally:
            if previous is None:
                os.environ.pop("TZ", None)
            else:
                os.environ["TZ"] = previous
            time.tzset()
        self.assertEqual(jobmon.cpu_cell(101.25, 40.5), "101.2%")
        self.assertEqual(jobmon.cpu_cell(None, 3.6), "3.6 cores")
        self.assertEqual(jobmon.cpu_cell(None, None), "-")

    def test_parent_usage_is_not_added_to_steps(self):
        parent = {"JobID": "123", "JobIDRaw": "123", "TotalCPU": "00:01:40", "AllocCPUS": "8",
                  "ElapsedRaw": "100", "NNodes": "2", "ReqMem": "4Gn", "AllocTRES": "cpu=8,mem=8G"}
        steps = [{"JobID": "123.batch", "JobIDRaw": "123.batch", "TotalCPU": "00:01:40", "MaxRSS": "2048M"}]
        report = normalize_allocation(parent, steps)
        self.assertEqual(report["total_cpu_seconds"], 100)
        self.assertEqual(report["cpu_efficiency"]["value"], 12.5)
        self.assertEqual(report["step_max_rss"][0]["bytes"], 2 * 1024 ** 3)
        parent["TotalCPU"] = "00:00:00"
        self.assertIsNone(normalize_allocation(parent, steps)["cpu_efficiency"]["value"])

    def test_bounded_array_accounting_and_requeues(self):
        calls = []
        rows = [
            {"JobID": "123_7", "JobIDRaw": "130", "Submit": "2026-09-01T10:00:00", "TotalCPU": "00:00:10",
             "JobName": "first simulation"},
            {"JobID": "123_7.batch", "JobIDRaw": "130.batch", "Submit": "2026-09-01T10:00:00", "MaxRSS": "1G"},
            {"JobID": "123_7", "JobIDRaw": "130", "Submit": "2026-09-02T10:00:00", "TotalCPU": "00:00:20",
             "JobName": "restart | simulation |"},
            {"JobID": "123_7.batch", "JobIDRaw": "130.batch", "Submit": "2026-09-02T10:00:00", "MaxRSS": "2G"},
            {"JobID": "123_8", "JobIDRaw": "131", "Submit": "2026-09-02T10:00:00"},
            {"JobID": "123_7", "JobIDRaw": "999", "User": "another-user", "UID": "999999"},
        ]

        def runner(argv):
            calls.append(argv)
            if "--helpformat" in argv:
                return " ".join(SlurmBackend.ACCOUNT_FIELDS)
            fields = next(arg.split("=", 1)[1] for arg in argv if arg.startswith("--format=")).split(",")
            fields = [field.split("%", 1)[0] for field in fields]
            normalized = [dict({"User": identity(), "UID": str(os.getuid())}, **row) for row in rows]
            return "\n".join("|".join(row.get(field, "") for field in fields) for row in normalized)

        reports = SlurmBackend(runner).accounting("123_7", days=7)
        self.assertEqual(len(reports), 2)
        self.assertEqual(reports[0]["job_id_raw"], "130")
        self.assertEqual(len(reports[0]["steps"]), 1)
        self.assertEqual(reports[1]["steps"][0]["MaxRSS"], "2G")
        self.assertEqual([run["job_name"] for run in reports], ["first simulation", "restart | simulation |"])
        query = calls[1]
        for expected in ("--jobs=123_7", "--duplicates", "--endtime=now", "--user=" + identity()):
            self.assertIn(expected, query)
        self.assertTrue(any(value.startswith("--starttime=") for value in query))
        self.assertNotIn("-X", query)

    def test_accounting_without_the_optional_job_name_field_remains_available(self):
        calls = []

        def runner(argv):
            calls.append(argv)
            if "--helpformat" in argv:
                return "JobIDRaw JobID User UID State"
            return "123|123|{}|{}|COMPLETED\n".format(identity(), os.getuid())

        rows = SlurmBackend(runner).accounting("123")
        self.assertEqual(len(rows), 1)
        self.assertIsNone(rows[0]["job_name"])
        self.assertFalse(any("JobName" in argument for argument in calls[-1]))

    def test_unavailable_capability_does_not_guess_fields(self):
        calls = []

        def runner(argv):
            calls.append(argv)
            return "JobID State"

        with self.assertRaises(JobmonError):
            SlurmBackend(runner).accounting("123")
        self.assertEqual(len(calls), 1)

    def test_disabled_accounting_storage_is_named_as_a_site_capability(self):
        def runner(argv):
            if argv[0] == "sacct":
                raise JobmonError("sacct exited 1: Slurm accounting storage is disabled")
            return "JobID MaxRSS"

        with self.assertRaises(JobmonError) as caught:
            SlurmBackend(runner).accounting("123456")
        message = str(caught.exception)
        self.assertIn("accounting storage is disabled", message.lower())
        self.assertIn("opt-in collector", message)
        self.assertNotIn("exited 1", message)

        with patch.object(slurm_backend.shutil, "which", lambda command: "/usr/bin/" + command):
            report = SlurmBackend(runner).doctor()
        self.assertEqual(report["accounting_storage"], "disabled")
        self.assertNotIn("sacct", report["fields"])

    def test_report_guidance_points_at_the_site_not_the_job_id(self):
        class Backend:
            def snapshot(self, *args):
                return {"ownership_verified": False, "queue": [], "accounting": [], "live_steps": [],
                        "errors": [slurm_backend.ACCOUNTING_DISABLED_GUIDANCE]}

        class NoHistory:
            errors = []

            def __init__(self, *args, **kwargs):
                pass

            def refresh(self):
                return []

        with patch.object(jobmon, "SavedHistory", NoHistory):
            with self.assertRaises(JobmonError) as caught:
                jobmon.build_report("123456", backend=Backend())
        message = str(caught.exception)
        self.assertIn("opt-in collector", message)
        self.assertNotIn("--days", message)

    def test_doctor_checks_actual_accounting_after_successful_help(self):
        for failure, verdict in ((None, "available"),
                                 ("Slurm accounting storage is disabled", "disabled"),
                                 ("database connection timed out", "unknown")):
            calls = []

            def runner(argv):
                calls.append(argv)
                if "--helpformat" in argv:
                    return "JobID User UID"
                if "--version" in argv:
                    return "slurm synthetic-version"
                if failure:
                    raise JobmonError(failure)
                return ""  # No jobs is still a successful storage query.

            with self.subTest(verdict=verdict), \
                    patch.object(slurm_backend.shutil, "which", lambda cmd: "/usr/bin/" + cmd):
                report = SlurmBackend(runner).doctor()
                self.assertEqual(report["accounting_storage"], verdict)
                probes = [call for call in calls if any(arg.startswith("--starttime=") for arg in call)]
                self.assertEqual(len(probes), 1)
                probe = probes[0]
                self.assertEqual(probe[0], "sacct")
                self.assertIn("--local", probe)
                self.assertIn("--user=" + identity(), probe)
                self.assertIn("--format=JobID%256", probe)
                times = {arg.split("=", 1)[0]: dt.datetime.fromisoformat(arg.split("=", 1)[1])
                         for arg in probe if arg.startswith(("--starttime=", "--endtime="))}
                self.assertEqual((times["--endtime"] - times["--starttime"]).total_seconds(), 60)
                if failure:
                    self.assertIn(failure, report["errors"])


class StorageTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name) / "state"
        environment = {key: value for key, value in os.environ.items() if not key.startswith(("JOBMON_", "SLURM_"))}
        environment.update({"JOBMON_DATA_DIR": str(self.root), "SLURM_JOB_ID": "130",
                     "SLURM_ARRAY_JOB_ID": "123", "SLURM_ARRAY_TASK_ID": "7", "SLURMD_NODENAME": "node1",
                     "SLURM_JOB_NUM_NODES": "1", "SLURM_JOB_NODELIST": "node1", "SLURM_JOB_CPUS_PER_NODE": "8"})
        self.env = patch.dict(os.environ, environment, clear=True)
        self.env.start()
        collect.initialize_storage_probe(self.root, allow_allocation=True)
        self.run = collect.initialize(runner=lambda argv: "node1\n")
        self.metadata = collect.load_run(self.run)

    def tearDown(self):
        self.env.stop()
        self.temp.cleanup()

    def record(self, node="node1", tick=0, memory=100, final=False, cpu_delta=None, gpu_util=0):
        return {"schema_version": 1, "job_id": "123_7", "job_id_raw": "130", "run_id": self.run.name,
                "node": node, "stream_id": node + "--test",
                "timestamp": (dt.datetime(2026, 9, 14, tzinfo=dt.timezone.utc)
                              + dt.timedelta(seconds=tick)).isoformat(),
                "monotonic_seconds": tick, "interval_seconds": 30, "final": final, "errors": [],
                "metrics": {"scope": "job_cgroup_v2", "memory_bytes": memory, "memory_peak_bytes": memory,
                            "allocated_cpus": 8, "cpu_delta_seconds": cpu_delta,
                            "cpu_interval_seconds": 30 if cpu_delta is not None else None},
                "gpus": [{"uuid": "GPU-abcd", "scope": "shared_device_context", "device_utilization_percent": gpu_util,
                          "memory_used_bytes": 20, "unavailable_fields": []}]}

    def append(self, value, node="node1", tail="\n"):
        path = self.run / (node + "--test.jsonl")
        with collect.private_open(path, os.O_WRONLY | os.O_APPEND | os.O_CREAT, "a") as stream:
            stream.write((json.dumps(value) if not isinstance(value, str) else value) + tail)

    def test_private_metadata_and_unique_runs(self):
        self.assertEqual(self.metadata["job_id"], "123_7")
        self.assertEqual(self.metadata["job_id_raw"], "130")
        self.assertEqual(self.root.stat().st_mode & 0o777, 0o700)
        self.assertEqual((self.run / "metadata.json").stat().st_mode & 0o777, 0o600)
        another = collect.initialize(runner=lambda argv: "node1\n")
        self.assertNotEqual(another, self.run)

    def test_saved_job_name_is_available_offline_and_older_metadata_remains_readable(self):
        with patch.dict(os.environ, {"SLURM_JOB_NAME": "synthetic simulation"}):
            directory = collect.initialize(runner=lambda argv: "node1\n")
        metadata = collect.load_run(directory)
        self.assertEqual(metadata["job_name"], "synthetic simulation")
        run = jobmon.RunHistory(directory, metadata).summary()
        report = self.report_for(run)
        self.assertTrue(jobmon.watch_header(report, dt.timezone.utc)[0].startswith(
            "Job 123_7 (synthetic simulation):"))
        del metadata["job_name"]
        (directory / "metadata.json").write_text(json.dumps(metadata))
        older = collect.load_run(directory)
        self.assertNotIn("job_name", older)
        report = self.report_for(jobmon.RunHistory(directory, older).summary())
        self.assertIn("(name unavailable)", jobmon.watch_header(report, dt.timezone.utc)[0])

    def test_storage_probe_and_absolute_configuration_are_required(self):
        status = collect.verify_storage_probe(self.root)
        self.assertTrue(status["verified"])
        self.assertEqual(self.metadata["storage_probe"]["path"], str(self.root))
        with self.assertRaisesRegex(JobmonError, "outside a Slurm allocation"):
            collect.initialize_storage_probe(self.root)
        (self.root / collect.STORAGE_PROBE_FILE).unlink()
        with self.assertRaisesRegex(JobmonError, "hpc-jstorage-init"):
            collect.initialize(runner=lambda argv: "node1\n")
        with patch.dict(os.environ, {"JOBMON_DATA_DIR": "relative/jobmon"}):
            with self.assertRaisesRegex(JobmonError, "absolute path"):
                collect.data_root()

    def test_unavailable_storage_and_symlinks_rejected(self):
        self.root.chmod(0o755)
        with self.assertRaises(JobmonError):
            collect.load_run(self.run)
        self.root.chmod(0o700)
        (self.run / "bad.jsonl").symlink_to(self.run / "metadata.json")
        with self.assertRaises(OSError):
            collect.private_open(self.run / "bad.jsonl", os.O_RDONLY)

    def test_partial_tail_and_incremental_reading(self):
        self.append(self.record(tick=0))
        self.append(self.record(tick=30, cpu_delta=30, final=True), tail="")
        history = jobmon.RunHistory(self.run, self.metadata)
        first = history.summary()
        self.assertEqual(first["nodes"]["node1"]["samples"], 1)
        self.assertEqual(len(first["partial_final_lines"]), 1)
        self.append("", tail="\n")
        second = history.summary()
        self.assertEqual(second["nodes"]["node1"]["samples"], 2)
        self.assertEqual(second["partial_final_lines"], [])
        self.assertEqual(second["cpu_percent_covered_intervals"], 12.5)
        self.assertEqual(history.summary()["nodes"]["node1"]["samples"], 2)
        self.assertEqual(second["gpus"][0]["device_utilization_percent"], 0)
        self.assertIsNone(second["gpus"][0]["job_utilization_percent"])

    def test_concurrent_memory_not_sum_of_independent_peaks(self):
        self.metadata["expected_nodes"] = ["node1", "node2"]
        self.metadata["expected_node_count"] = 2
        for node, values in (("node1", (100, 10)), ("node2", (10, 100))):
            self.append(self.record(node, 0, values[0]), node)
            self.append(self.record(node, 30, values[1], True, 30), node)
        result = jobmon.RunHistory(self.run, self.metadata).summary()
        self.assertEqual(result["concurrent_memory_peak_bytes"], 110)
        self.assertEqual(result["sum_of_independent_node_memory_peaks_bytes"], 200)
        self.assertEqual(result["cpu_percent_covered_intervals"], 12.5)
        self.assertTrue(result["all_expected_collectors_final"])

    def test_missing_nodes_and_gaps_never_become_zero(self):
        self.metadata["expected_nodes"] = ["node1", "node2"]
        self.metadata["expected_node_count"] = 2
        self.append(self.record(tick=0))
        self.append(self.record(tick=120))
        result = jobmon.RunHistory(self.run, self.metadata).summary()
        self.assertEqual(result["missing_nodes"], ["node2"])
        self.assertEqual(result["nodes"]["node1"]["gaps"], 1)
        self.assertIsNone(result["concurrent_memory_peak_bytes"])
        self.assertIsNone(result["cpu_percent_covered_intervals"])
        self.assertEqual(result["gpus"][0]["covered_device_seconds"], 0)

    def test_incomplete_node_layout_keeps_readings_without_claiming_completeness(self):
        with patch.dict(os.environ, {"SLURM_JOB_NUM_NODES": "2", "SLURM_JOB_CPUS_PER_NODE": "8(x2)"}):
            run = collect.initialize(runner=lambda argv: "node1\n")
        metadata = collect.load_run(run)
        self.assertEqual(metadata["expected_nodes"], ["node1"])
        self.assertEqual(metadata["expected_node_count"], 2)
        record = self.record(final=True)
        record["run_id"] = run.name
        collect.write_json(run / "node1--test.jsonl", record)
        result = next(saved for saved in jobmon.SavedHistory("123_7").refresh() if saved["run_id"] == run.name)
        self.assertEqual(result["nodes"]["node1"]["memory_peak_bytes"], 100)
        self.assertEqual(result["missing_node_count"], 1)
        self.assertFalse(result["all_expected_collectors_final"])
        self.assertIsNone(result["concurrent_memory_peak_bytes"])
        self.assertEqual(result["aligned_memory_sample_count"], 0)
        self.assertIn("expected-node layout", result["unavailable_metrics"]["concurrent_memory_peak_bytes"])
        self.assertIn("Memory across nodes: concurrent peak unavailable", jobmon.format_report(self.report_for(result)))

    def test_unknown_node_names_cannot_establish_completeness_from_sample_count(self):
        self.metadata["expected_nodes"] = []
        self.metadata["expected_node_count"] = 2
        for node in ("node1", "node2"):
            self.append(self.record(node=node, final=True), node)
        result = jobmon.RunHistory(self.run, self.metadata).summary()
        self.assertEqual(result["missing_node_count"], 0)
        self.assertFalse(result["all_expected_collectors_final"])
        self.assertIsNone(result["concurrent_memory_peak_bytes"])
        self.assertEqual(result["sum_of_independent_node_memory_peaks_bytes"], 200)
        self.assertIn("expected-node layout", result["unavailable_metrics"]["concurrent_memory_peak_bytes"])

    def test_long_multinode_streams_stay_aligned(self):
        self.metadata["expected_nodes"] = ["node1", "node2"]
        self.metadata["expected_node_count"] = 2
        for node in self.metadata["expected_nodes"]:
            path = self.run / (node + "--test.jsonl")
            with collect.private_open(path, os.O_WRONLY | os.O_CREAT, "w") as stream:
                for index in range(5200):
                    row = self.record(node, index * 30, 100 if index == 5100 else 10)
                    stream.write(json.dumps(row) + "\n")
        result = jobmon.RunHistory(self.run, self.metadata).summary()
        self.assertEqual(result["concurrent_memory_peak_bytes"], 200)
        self.assertEqual(result["aligned_memory_sample_count"], 5200)

    def test_delayed_node_samples_recover_the_concurrent_peak(self):
        self.metadata["expected_nodes"] = ["node1", "node2", "node3"]
        self.metadata["expected_node_count"] = 3
        history = jobmon.RunHistory(self.run, self.metadata)
        for node in self.metadata["expected_nodes"]:
            self.append(self.record(node, 0, 100), node)
        self.assertEqual(history.summary()["concurrent_memory_peak_bytes"], 300)
        for node in ("node1", "node2"):
            self.append(self.record(node, 30, 1000), node)
            self.append(self.record(node, 60, 100), node)
        self.assertEqual(history.summary()["concurrent_memory_peak_bytes"], 300)
        for tick, memory in ((30, 1000), (60, 100)):
            self.append(self.record("node3", tick, memory), "node3")
        incremental = history.summary()
        fresh = jobmon.RunHistory(self.run, self.metadata).summary()
        self.assertEqual(incremental["concurrent_memory_peak_bytes"], 3000)
        self.assertEqual(incremental["concurrent_memory_peak_bytes"], fresh["concurrent_memory_peak_bytes"])
        self.assertEqual(incremental["aligned_memory_sample_count"], fresh["aligned_memory_sample_count"])
        self.assertFalse(incremental["concurrent_memory_alignment_incomplete"])
        self.assertEqual(history.summary()["aligned_memory_sample_count"], incremental["aligned_memory_sample_count"])

    def test_delayed_samples_can_correct_an_uncommitted_peak_downward(self):
        self.metadata["expected_nodes"] = ["node1", "node2"]
        self.metadata["expected_node_count"] = 2
        history = jobmon.RunHistory(self.run, self.metadata)
        self.append(self.record("node1", 0, 0), "node1")
        self.append(self.record("node2", 0, 1000), "node2")
        history.summary()
        # This apparent alignment uses node2's old reading within the 15 s tolerance.
        self.append(self.record("node1", 15, 1000), "node1")
        self.assertEqual(history.summary()["concurrent_memory_peak_bytes"], 2000)
        self.append(self.record("node2", 10, 0), "node2")
        result = history.summary()
        fresh = jobmon.RunHistory(self.run, self.metadata).summary()
        self.assertEqual(result["concurrent_memory_peak_bytes"], 1000)
        self.assertEqual(result["concurrent_memory_peak_bytes"], fresh["concurrent_memory_peak_bytes"])
        self.assertEqual(result["aligned_memory_sample_count"], fresh["aligned_memory_sample_count"])
        self.assertFalse(result["concurrent_memory_alignment_incomplete"])

    def test_memory_alignment_buffer_is_bounded_and_reports_excessive_delay(self):
        self.metadata["expected_nodes"] = ["node1", "node2"]
        self.metadata["expected_node_count"] = 2
        history = jobmon.RunHistory(self.run, self.metadata)
        for node in self.metadata["expected_nodes"]:
            self.append(self.record(node, 0, 100), node)
        history.summary()
        for index in range(1, jobmon.MEMORY_ALIGNMENT_BUFFER_SAMPLES + 3):
            self.append(self.record("node1", index * 30, 1000 if index == 1 else 100), "node1")
        result = history.summary()
        self.assertLessEqual(len(history.pending_memory["node1"]), jobmon.MEMORY_ALIGNMENT_BUFFER_SAMPLES)
        self.assertTrue(result["concurrent_memory_alignment_incomplete"])
        self.assertIn("concurrent memory alignment incomplete", result["coverage"])
        self.assertTrue(any("pending samples per node" in error for error in result["errors"]))
        for index in range(1, jobmon.MEMORY_ALIGNMENT_BUFFER_SAMPLES + 3):
            self.append(self.record("node2", index * 30, 1000 if index == 1 else 100), "node2")
        self.assertTrue(history.summary()["concurrent_memory_alignment_incomplete"])
        fresh = jobmon.RunHistory(self.run, self.metadata).summary()
        self.assertEqual(fresh["concurrent_memory_peak_bytes"], 2000)
        self.assertFalse(fresh["concurrent_memory_alignment_incomplete"])

    def test_memory_timestamp_regression_is_reported_after_committing(self):
        history = jobmon.RunHistory(self.run, self.metadata)
        self.append(self.record(tick=30, memory=100))
        history.summary()
        regressed = self.record(tick=60, memory=1000)
        regressed["timestamp"] = self.record(tick=10)["timestamp"]
        self.append(regressed)
        result = history.summary()
        self.assertTrue(result["concurrent_memory_alignment_incomplete"])
        self.assertTrue(any("committed time frontier" in error for error in result["errors"]))

    def test_malformed_and_wrong_identity_samples_are_reported(self):
        self.append("{broken}")
        wrong = self.record()
        wrong["job_id"] = "999"
        self.append(wrong)
        self.append(self.record())
        result = jobmon.RunHistory(self.run, self.metadata).summary()
        self.assertEqual(result["nodes"]["node1"]["samples"], 1)
        self.assertEqual(len(result["errors"]), 2)

    def test_malformed_record_fields_do_not_mutate_aggregates(self):
        self.append(self.record())
        history = jobmon.RunHistory(self.run, self.metadata)
        history.summary()
        bad_records = []
        for scope in ([], {}, None, 1):
            record = self.record(tick=30, memory=9999, cpu_delta=240)
            record["metrics"]["scope"] = scope
            bad_records.append(record)
        record = self.record(tick=30, memory=9999, cpu_delta=240)
        record["gpus"][0]["unavailable_fields"] = [{}]
        bad_records.append(record)
        record = self.record(tick=30, memory=9999, cpu_delta=240)
        record["metrics"]["memory_bytes"] = 10 ** 400
        bad_records.append(record)
        for record in bad_records:
            self.append(record)
        result = history.summary()
        self.assertEqual(result["nodes"]["node1"]["samples"], 1)
        self.assertEqual(result["nodes"]["node1"]["memory_peak_bytes"], 100)
        self.assertEqual(result["concurrent_memory_peak_bytes"], 100)
        self.assertIsNone(result["cpu_percent_covered_intervals"])
        self.assertEqual(result["gpus"][0]["covered_device_seconds"], 0)
        self.assertTrue(result["errors"])
        self.assertTrue(all("invalid record" in error for error in result["errors"]))
        # A valid sample at the rejected records' tick must still be accepted.
        self.append(self.record(tick=30, memory=200, cpu_delta=30))
        result = history.summary()
        self.assertEqual(result["nodes"]["node1"]["samples"], 2)
        self.assertEqual(result["nodes"]["node1"]["cpu_seconds_covered"], 30)
        self.assertEqual(result["concurrent_memory_peak_bytes"], 200)
        self.assertEqual(result["cpu_percent_covered_intervals"], 12.5)
        self.assertEqual(result["gpus"][0]["covered_device_seconds"], 30)

    def test_gpu_query_delays_do_not_shift_cpu_memory_sample_times(self):
        with patch.dict(os.environ, {"SLURM_JOB_NUM_NODES": "2", "SLURM_JOB_CPUS_PER_NODE": "8(x2)"}):
            run = collect.initialize(runner=lambda argv: "node1\nnode2\n")
        elapsed = [0]
        epoch = dt.datetime(2026, 9, 14, tzinfo=dt.timezone.utc)
        metrics = {"scope": "job_cgroup_v2", "cpu_seconds": 0, "memory_bytes": 1000,
                   "memory_peak_bytes": 1000, "errors": []}

        def gpu_sample():
            if os.environ["SLURMD_NODENAME"] == "node1":
                elapsed[0] += 10
            return [], []

        with patch.object(collect, "discover_cgroup", return_value=(Path("/synthetic/cgroup"), None)), \
                patch.object(collect, "cgroup_sample", side_effect=lambda path: dict(metrics)), \
                patch.object(collect, "NvidiaSampler") as sampler, \
                patch.object(collect.time, "monotonic", side_effect=lambda: 100 + elapsed[0]), \
                patch.object(collect, "utc_now", side_effect=lambda:
                             (epoch + dt.timedelta(seconds=elapsed[0])).isoformat()):
            sampler.return_value.sample.side_effect = gpu_sample
            for node in ("node1", "node2"):
                elapsed[0] = 0
                with patch.dict(os.environ, {"SLURMD_NODENAME": node}):
                    collect.collect(run, interval=1, once=True)
        records = [json.loads(path.read_text()) for path in run.glob("*.jsonl")]
        self.assertEqual(len(records), 2)
        for record in records:
            self.assertEqual(record["timestamp"], epoch.isoformat())
            self.assertEqual(record["monotonic_seconds"], 100)
        result = jobmon.RunHistory(run, collect.load_run(run)).summary()
        self.assertEqual(result["concurrent_memory_peak_bytes"], 2000)
        self.assertTrue(result["all_expected_collectors_final"])

    def test_collect_once_missing_cgroup_is_unavailable(self):
        with patch.object(collect, "discover_cgroup", return_value=(None, "no readable job cgroup")):
            collect.collect(self.run, once=True)
        record = json.loads(next(self.run.glob("*.jsonl")).read_text())
        self.assertIsNone(record["metrics"]["memory_bytes"])
        self.assertIsNone(record["metrics"]["cpu_seconds"])
        self.assertTrue(record["final"])
        self.assertEqual(record["metrics"]["scope"], "unavailable")
        # The diagnostic keeps the cgroup reason and names the opt-in that would apply.
        joined = "; ".join(record["errors"])
        self.assertIn("no readable job cgroup", joined)
        self.assertIn("JOBMON_PROCESS_TREE=1", joined)

    def test_duplicate_collector_lock(self):
        import fcntl
        with collect.private_open(self.run / "node1.lock", os.O_RDWR | os.O_CREAT, "r+") as lock:
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            with self.assertRaisesRegex(JobmonError, "already running"):
                collect.collect(self.run, once=True)

    def test_watch_queries_scheduler_only_once(self):
        self.append(self.record())
        calls = []

        class Backend:
            def snapshot(self, *args):
                calls.append(args)
                return {"ownership_verified": False, "queue": [], "accounting": [],
                        "live_steps": [], "errors": ["scheduler offline"]}

        with patch.object(jobmon, "SlurmBackend", Backend), patch.object(jobmon.time, "sleep"), \
                contextlib.redirect_stdout(io.StringIO()) as output:
            jobmon.execute("watch", "123_7", as_json=True, count=3)
        self.assertEqual(len(calls), 1)
        self.assertEqual(len(output.getvalue().splitlines()), 3)
        self.assertEqual(json.loads(output.getvalue().splitlines()[0])["ownership_source"], "private_saved_metadata")

    def test_kernel_high_water_mark_is_reported_beside_the_sampled_peak(self):
        for tick in (0, 30):
            record = self.record(tick=tick, memory=1000)
            record["metrics"]["memory_peak_bytes"] = 8000
            self.append(record)
        run = jobmon.RunHistory(self.run, self.metadata).summary()
        self.assertEqual(run["nodes"]["node1"]["memory_peak_bytes"], 1000)
        self.assertEqual(run["nodes"]["node1"]["cgroup_memory_peak_bytes"], 8000)
        self.assertEqual(run["sum_of_independent_node_memory_peaks_bytes"], 1000)
        self.assertEqual(run["sum_of_independent_node_cgroup_memory_peaks_bytes"], 8000)
        self.assertNotIn("cgroup_memory_peak_bytes", run["unavailable_metrics"])
        text = jobmon.format_report({
            "job_id": "123_7", "generated_at": "t", "scheduler_snapshot_at": "t",
            "scheduler": {"queue": [], "accounting": [], "live_steps": [], "errors": []},
            "runs": [run], "errors": [], "history_reason": None})
        self.assertIn("sampled peak 1000 B; kernel high-water 7.81 KiB", text)
        self.assertIn("Kernel high-water scope: kernel memory.peak high-water mark per node", text)
        # One node: the run-level sums would only repeat the node's own readings.
        self.assertNotIn("Sum of independent", text)

    def test_missing_kernel_high_water_mark_is_named_not_zero(self):
        for tick in (0, 30):
            record = self.record(tick=tick, memory=1000)
            record["metrics"]["memory_peak_bytes"] = None
            self.append(record)
        run = jobmon.RunHistory(self.run, self.metadata).summary()
        self.assertIsNone(run["nodes"]["node1"]["cgroup_memory_peak_bytes"])
        self.assertIsNone(run["sum_of_independent_node_cgroup_memory_peaks_bytes"])
        self.assertIn("5.19", run["unavailable_metrics"]["cgroup_memory_peak_bytes"])

    def report_for(self, run, queue=()):
        return {"job_id": "123_7", "generated_at": "t", "scheduler_snapshot_at": "t",
                "scheduler": {"queue": list(queue), "accounting": [], "live_steps": [], "errors": []},
                "runs": [run], "errors": [], "history_reason": None}

    def process_tree_record(self, tick, cpu_delta=None, final=False):
        record = self.record(tick=tick, memory=38832472064, cpu_delta=cpu_delta, final=final)
        record["metrics"].update(scope="process_tree_pgid", memory_peak_bytes=None)
        record["gpus"] = []
        return record

    def test_latest_interval_cpu_and_memory_are_kept_beside_the_average(self):
        history = jobmon.RunHistory(self.run, self.metadata)
        self.append(self.record(tick=0, memory=100))
        self.append(self.record(tick=30, memory=200, cpu_delta=30))
        self.append(self.record(tick=60, memory=300, cpu_delta=90))
        node = history.summary()["nodes"]["node1"]
        self.assertEqual(node["latest_interval_busy_cores"], 3.0)
        self.assertEqual(node["latest_interval_cpu_percent"], 37.5)
        self.assertEqual(node["latest_memory_bytes"], 300)
        self.assertEqual(jobmon.average_cpu(node), (25.0, 2.0))
        text = jobmon.format_report(self.report_for(history.summary()))
        self.assertIn("CPU: last interval 37.5% (3.0 cores); average 25.0% (2.0 cores)", text)
        self.assertIn("Memory: latest 300 B; sampled peak 300 B", text)
        # A gap leaves the newest interval unavailable, never zero, and keeps the average.
        late = self.record(tick=150, memory=300, cpu_delta=30)
        late["metrics"]["cpu_interval_seconds"] = 90
        self.append(late)
        node = history.summary()["nodes"]["node1"]
        self.assertIsNone(node["latest_interval_busy_cores"])
        self.assertIsNone(node["latest_interval_cpu_percent"])
        self.assertEqual(jobmon.average_cpu(node), (25.0, 2.0))

    def test_single_node_report_states_each_reading_once(self):
        self.append(self.process_tree_record(0))
        self.append(self.process_tree_record(30, cpu_delta=60))
        run = jobmon.RunHistory(self.run, self.metadata).summary()
        running = [{"job_id": "123_7", "user": "u", "state": "RUNNING", "elapsed": "0:30",
                    "cpus": "8", "nodes": "1", "reason_or_nodes": "node1"}]
        text = jobmon.format_report(self.report_for(run, running))
        self.assertEqual(text.count("Not measured:"), 1)
        self.assertIn("GPU (no GPU samples; collection is off unless JOBMON_GPU_UUIDS is set", text)
        self.assertIn("kernel memory peak (process-tree sampling has no kernel high-water mark", text)
        for repeated in ("GPU: unavailable", "Sum of independent", "kernel high-water unavailable",
                         "Kernel high-water scope", "5.19", "GPU averages", "bytes", "Completion:"):
            self.assertNotIn(repeated, text)
        self.assertIn("Memory: latest 36.17 GiB; sampled peak 36.17 GiB", text)
        # Once the job has left the queue, a missing completion record is worth stating.
        self.assertIn("Completion: unavailable", jobmon.format_report(self.report_for(run)))

    def watch(self, sleep, count, clock, snapshot_tick=65, **options):
        # Slurm reports 1:00 elapsed at the snapshot, so the job started at tick 5.
        snapshot = dt.datetime(2026, 9, 14, tzinfo=dt.timezone.utc) + dt.timedelta(seconds=snapshot_tick)

        class Backend:
            def snapshot(self, *args):
                return {"ownership_verified": True, "accounting": [], "live_steps": [],
                        "errors": ["scheduler note"],
                        "queue": [{"job_id": "123_7", "user": "u", "state": "RUNNING", "elapsed": "1:00",
                                   "cpus": "8", "nodes": "1", "reason_or_nodes": "node1",
                                   "job_name": "synthetic-simulation"}]}

        sleeps = []

        def fake_sleep(seconds):
            sleeps.append(seconds)
            clock[0] += seconds
            sleep(len(sleeps))

        with patch.object(jobmon, "SlurmBackend", Backend), patch.object(jobmon.time, "sleep", fake_sleep), \
                patch.object(jobmon.time, "time", lambda: clock[0]), \
                patch.object(jobmon, "utc_now", lambda: snapshot.isoformat()), \
                contextlib.redirect_stdout(io.StringIO()) as output:
            jobmon.execute("watch", "123_7", interval=5, count=count, utc_offset_minutes=0, **options)
        return output.getvalue(), sleeps

    def test_text_watch_states_context_once_and_prints_rows_only_for_new_samples(self):
        base = dt.datetime(2026, 9, 14, tzinfo=dt.timezone.utc).timestamp()
        self.append(self.record(tick=0))
        self.append(self.record(tick=30, cpu_delta=30))
        text, sleeps = self.watch(
            lambda calls: calls == 3 and self.append(self.record(tick=60, cpu_delta=60)), 6, [base + 35])
        self.assertEqual(len(sleeps), 5)
        for once in ("Scheduler at 00:01:05", "elapsed 1:00", "not refreshed while watching",
                     "Job 123_7 (synthetic-simulation): watching saved samples",
                     "Diagnostic: scheduler note", "  Memory: job cgroup", "  Coverage: ", "CPU LAST 30 s",
                     "CLOCK is your workstation's time, UTC+00:00", "as in squeue's TIME column"):
            self.assertEqual(text.count(once), 1, once)
        lines = text.splitlines()
        table = lines[lines.index(next(line for line in lines if line.startswith("CLOCK "))):]
        # ELAPSED counts from Slurm's start (tick 5); the MAX columns keep the highest so far.
        self.assertEqual(table, [
            "CLOCK        ELAPSED  SAMPLES  CPU LAST 30 s    CPU AVG    CPU MAX       MEMORY   MEMORY MAX",
            "00:00:30        0:25        2          12.5%      12.5%      12.5%        100 B        100 B",
            "00:01:00        0:55        3          25.0%      18.8%      25.0%        100 B        100 B"])
        self.assertNotIn("no new sample", text)
        self.assertTrue(text.isascii())

    def test_text_watch_warns_once_when_samples_stop_but_not_after_the_final_sample(self):
        base = dt.datetime(2026, 9, 14, tzinfo=dt.timezone.utc).timestamp()
        self.append(self.record(tick=0))
        self.append(self.record(tick=30, cpu_delta=30))
        # --count bounds refreshes, so a stalled collector cannot hold the watch open.
        # 70 s after the last sample is within the limit; the second refresh, at 80 s, is not.
        text, sleeps = self.watch(lambda calls: None, 6, [base + 100])
        self.assertEqual(len(sleeps), 5)
        self.assertEqual(text.count("no new sample for"), 1)
        self.assertIn("no new sample for 80 s (expected every 30 s)", text)

        self.append(self.record(tick=60, cpu_delta=30, final=True))
        collect.write_json(self.run / "completion.json", {
            "schema_version": 1, "job_id": "123_7", "run_id": self.run.name, "timestamp": "t",
            "workload_exit_code": 0, "scope": "batch wrapper"})
        text, _ = self.watch(lambda calls: None, 6, [base + 600])
        self.assertNotIn("no new sample", text)
        self.assertEqual(text.count("Workload finished successfully (exit code 0)."), 1)
        self.assertTrue([line for line in text.splitlines() if line.startswith("00:01:00")][0].endswith("  final"))

    def test_watch_completion_names_nonzero_exit_and_summarizes_sampled_usage_once(self):
        base = dt.datetime(2026, 9, 14, tzinfo=dt.timezone.utc).timestamp()
        self.append(self.record(tick=0, memory=4096))
        self.append(self.record(tick=30, memory=100, cpu_delta=30, final=True))
        collect.finish(self.run, 1)
        text, sleeps = self.watch(lambda calls: None, 4, [base + 35])
        self.assertEqual(len(sleeps), 3)  # Default watches keep following later runs.
        for once in ("Workload finished with exit code 1 (nonzero).", "Final samples: 1/1 node recorded.",
                     "CPU average (sampled intervals): 12.5% of allocated cores",
                     "Sampled memory peak: 4.00 KiB", "Check application logs; run hpc-jreport 123_7",
                     "Watch remains open; press Ctrl-C to stop viewing."):
            self.assertEqual(text.count(once), 1, once)
        self.assertNotIn("all expected collectors final:", text)
        self.assertNotIn("Watch finished for", text)

    def test_watch_completion_keeps_unavailable_usage_unavailable(self):
        base = dt.datetime(2026, 9, 14, tzinfo=dt.timezone.utc).timestamp()
        self.append(self.record(memory=None, final=True))
        collect.finish(self.run, 0)
        text, _ = self.watch(lambda calls: None, 1, [base + 5])
        self.assertIn("CPU average (sampled intervals): unavailable", text)
        self.assertIn("Sampled memory peak: unavailable", text)
        self.assertNotIn("unavailable of allocated cores", text)
        self.assertNotIn("0.0%", text)

    def test_watch_updates_final_node_counts_and_exits_after_delayed_samples(self):
        base = dt.datetime(2026, 9, 14, tzinfo=dt.timezone.utc).timestamp()
        self.metadata.update(expected_nodes=["node1", "node2"], expected_node_count=2,
                             allocated_cpus_by_node={"node1": 8, "node2": 8})
        (self.run / "metadata.json").write_text(json.dumps(self.metadata))
        self.append(self.record(tick=0))
        self.append(self.record(tick=30, cpu_delta=30, final=True))
        self.append(self.record(node="node2", tick=0, memory=200), "node2")
        collect.finish(self.run, 0)

        def append_final(calls):
            if calls == 1:
                self.append(self.record(node="node2", tick=30, memory=100, cpu_delta=60, final=True), "node2")

        text, sleeps = self.watch(append_final, 4, [base + 35], exit_on_completion=True)
        self.assertEqual(len(sleeps), 1)
        for once in ("Workload finished successfully (exit code 0).",
                     "Final samples: 1/2 nodes; waiting for final samples.",
                     "Final samples: 2/2 nodes recorded.",
                     "CPU average (sampled intervals): 18.8% of allocated cores",
                     "Sampled memory peak on node1: 100 B", "Sampled memory peak on node2: 200 B",
                     "Watch finished for the latest saved run."):
            self.assertEqual(text.count(once), 1, once)
        self.assertNotIn("Watch remains open", text)

    def test_exit_on_completion_waits_for_wrapper_exit_and_final_samples(self):
        base = dt.datetime(2026, 9, 14, tzinfo=dt.timezone.utc).timestamp()
        self.append(self.record(tick=0))
        collect.finish(self.run, 0)
        text, sleeps = self.watch(lambda calls: None, 3, [base + 5], exit_on_completion=True)
        self.assertEqual(len(sleeps), 2)
        self.assertIn("Final samples: 0/1 node; waiting for final samples.", text)
        self.assertNotIn("Watch finished for", text)
        (self.run / "completion.json").unlink()
        self.append(self.record(tick=30, cpu_delta=30, final=True))
        text, sleeps = self.watch(lambda calls: None, 3, [base + 35], exit_on_completion=True)
        self.assertEqual(len(sleeps), 2)
        self.assertNotIn("Watch finished for", text)
        collect.finish(self.run, 0)
        text, sleeps = self.watch(lambda calls: None, 3, [base + 35], exit_on_completion=True)
        self.assertEqual(sleeps, [])
        self.assertIn("Watch finished for the latest saved run.", text)

    def test_exit_on_completion_requires_verified_node_layout(self):
        base = dt.datetime(2026, 9, 14, tzinfo=dt.timezone.utc).timestamp()
        self.append(self.record(final=True))
        collect.finish(self.run, 0)
        self.metadata["expected_nodes"] = []
        for total in (1, None):
            with self.subTest(total=total):
                self.metadata["expected_node_count"] = total
                (self.run / "metadata.json").write_text(json.dumps(self.metadata))
                text, sleeps = self.watch(lambda calls: None, 3, [base + 5], exit_on_completion=True)
                self.assertEqual(len(sleeps), 2)
                counts = "1/1 node" if total == 1 else "1/unknown nodes"
                self.assertIn("Final samples: " + counts + "; expected node layout incomplete.", text)
                self.assertNotIn("Watch finished for", text)

    def test_exit_on_completion_follows_newest_run_instead_of_an_older_completed_run(self):
        base = dt.datetime(2026, 9, 14, tzinfo=dt.timezone.utc)
        self.metadata["created_at"] = base.isoformat()
        (self.run / "metadata.json").write_text(json.dumps(self.metadata))
        self.append(self.record(final=True))
        collect.finish(self.run, 0)
        with patch.object(collect, "utc_now", return_value=(base + dt.timedelta(seconds=60)).isoformat()):
            new_run = collect.initialize(runner=lambda argv: "node1\n")
        record = self.record(tick=60)
        record["run_id"] = new_run.name
        collect.write_json(new_run / "node1--test.jsonl", record)

        def finish_new_run(calls):
            if calls == 1:
                record = self.record(tick=90, cpu_delta=30, final=True)
                record["run_id"] = new_run.name
                with collect.private_open(new_run / "node1--test.jsonl", os.O_WRONLY | os.O_APPEND, "a") as stream:
                    stream.write(json.dumps(record) + "\n")
                collect.finish(new_run, 1)

        build_report = jobmon.build_report

        def newest_first(*args, **kwargs):
            report, history = build_report(*args, **kwargs)
            # Put the old completed run last: iteration order cannot choose the target.
            report["runs"].sort(key=lambda run: run["metadata"]["created_at"], reverse=True)
            return report, history

        with patch.object(jobmon, "build_report", newest_first):
            text, sleeps = self.watch(finish_new_run, 4, [base.timestamp() + 65], exit_on_completion=True)
        self.assertEqual(len(sleeps), 1)
        self.assertIn("run " + new_run.name + ": Workload finished with exit code 1 (nonzero).", text)
        self.assertIn("Watch finished for the latest saved run.", text)

    def test_json_watch_exits_after_final_samples_without_changing_report_format(self):
        base = dt.datetime(2026, 9, 14, tzinfo=dt.timezone.utc).timestamp()
        self.append(self.record(tick=0))
        collect.finish(self.run, 1)
        text, sleeps = self.watch(
            lambda calls: calls == 1 and self.append(self.record(tick=30, cpu_delta=30, final=True)),
            4, [base + 5], exit_on_completion=True, as_json=True)
        self.assertEqual(len(sleeps), 1)
        snapshots = [json.loads(line) for line in text.splitlines()]
        self.assertEqual(len(snapshots), 2)
        self.assertFalse(snapshots[0]["runs"][0]["all_expected_collectors_final"])
        self.assertTrue(snapshots[1]["runs"][0]["all_expected_collectors_final"])
        self.assertEqual(snapshots[1]["runs"][0]["completion"]["workload_exit_code"], 1)
        self.assertNotIn("exit_on_completion", snapshots[1])

    def test_cli_exit_on_completion_keeps_viewer_status_separate_from_workload_status(self):
        self.append(self.record(final=True))
        collect.finish(self.run, 1)
        scheduler = {"ownership_verified": False, "queue": [], "accounting": [], "live_steps": [], "errors": []}
        with patch.object(SlurmBackend, "snapshot", return_value=scheduler) as snapshot, \
                patch.object(jobmon.time, "sleep") as sleep, contextlib.redirect_stdout(io.StringIO()) as output:
            result = jobmon.main(["watch", "123_7", "--json", "--exit-on-completion"])
        self.assertEqual(result, 0)
        self.assertEqual(snapshot.call_count, 1)
        sleep.assert_not_called()
        self.assertEqual(len(output.getvalue().splitlines()), 1)
        self.assertTrue(json.loads(output.getvalue())["runs"][0]["all_expected_collectors_final"])

    def test_watch_maxima_hold_after_cpu_and_memory_drop(self):
        base = dt.datetime(2026, 9, 14, tzinfo=dt.timezone.utc).timestamp()
        self.append(self.record(tick=0, memory=100))
        self.append(self.record(tick=30, memory=4096, cpu_delta=60))
        self.append(self.record(tick=60, memory=100, cpu_delta=30))
        node = jobmon.RunHistory(self.run, self.metadata).summary()["nodes"]["node1"]
        self.assertEqual((node["max_interval_cpu_percent"], node["max_interval_busy_cores"]), (25.0, 2.0))
        self.assertEqual(node["memory_peak_bytes"], 4096)
        text, _ = self.watch(lambda calls: None, 1, [base + 61])
        row = [line for line in text.splitlines() if line.startswith("00:01:00")][0]
        self.assertEqual(row.split(), ["00:01:00", "0:55", "3", "12.5%", "18.8%", "25.0%", "100", "B", "4.00", "KiB"])
        report = jobmon.format_report(self.report_for(jobmon.RunHistory(self.run, self.metadata).summary()))
        self.assertIn("busiest interval 25.0% (2.0 cores)", report)

    def test_invalid_cpu_layout_is_recorded_without_losing_the_run(self):
        with patch.dict(os.environ, {"SLURM_JOB_CPUS_PER_NODE": "8(x999999)"}):
            run = collect.initialize(runner=lambda argv: "node1\n")
        metadata = collect.load_run(run)
        self.assertEqual(metadata["allocated_cpus_by_node"], {})
        self.assertTrue(any("repetition" in error for error in metadata["errors"]))

    def test_unusable_cpu_counts_are_never_written_as_unreadable_metadata(self):
        # initialize must not store a layout that load_run would reject forever.
        for layout in ("0", "8,4", "8(x2)"):
            with self.subTest(layout=layout), patch.dict(os.environ, {"SLURM_JOB_CPUS_PER_NODE": layout}):
                run = collect.initialize(runner=lambda argv: "node1\n")
                metadata = collect.load_run(run)
                self.assertEqual(metadata["allocated_cpus_by_node"], {})
                self.assertTrue(any("CPU" in error for error in metadata["errors"]), metadata["errors"])

    def test_stop_marker_poll_is_backed_off_but_still_bounded(self):
        waits = []

        class Stub:
            def __init__(self):
                self.stopped = False

            def is_set(self):
                return self.stopped

            def wait(self, timeout=None):
                waits.append(timeout)
                self.stopped = True
                return True

        collect.wait_for_next_sample(self.run, Stub(), collect.time.monotonic() + 3600)
        self.assertEqual(waits, [collect.STOP_POLL_SECONDS])
        self.assertGreater(collect.STOP_POLL_SECONDS, 1)
        # A short interval must still bound the wait by its own remaining time.
        waits.clear()
        collect.wait_for_next_sample(self.run, Stub(), collect.time.monotonic() + 0.25)
        self.assertLessEqual(waits[0], 0.25)

    def test_process_tree_samples_reach_the_report_with_their_own_scope(self):
        for tick in (0, 30):
            record = self.record(tick=tick, memory=4096, cpu_delta=15)
            record["metrics"]["scope"] = "process_tree_pgid"
            record["metrics"]["memory_peak_bytes"] = None
            self.append(record)
        run = jobmon.RunHistory(self.run, self.metadata).summary()
        self.assertEqual(run["nodes"]["node1"]["memory_peak_bytes"], 4096)
        self.assertIsNone(run["nodes"]["node1"]["cgroup_memory_peak_bytes"])
        self.assertIsNotNone(run["cpu_percent_covered_intervals"])
        self.assertIn("process group", run["memory_scope"])
        self.assertNotIn("cgroup charges", run["memory_scope"])
        # The scope, not the kernel version, is why there is no high-water mark.
        self.assertIn("process-tree sampling has no kernel high-water mark",
                      run["unavailable_metrics"]["cgroup_memory_peak_bytes"])
        self.assertNotIn("5.19", run["unavailable_metrics"]["cgroup_memory_peak_bytes"])

    def test_archive_contains_only_private_relative_job_files(self):
        self.append(self.record())
        result = subprocess.run([sys.executable, str(ROOT / "jobmon/jobmon.py"), "_transport"],
                                input=b'{"operation":"fetch","job_id":"123_7"}', capture_output=True, timeout=10)
        self.assertEqual(result.returncode, 0, result.stderr)
        with tarfile.open(fileobj=io.BytesIO(result.stdout), mode="r:gz") as archive:
            members = archive.getmembers()
            self.assertTrue(any(member.name.endswith(".jsonl") for member in members))
            for member in members:
                self.assertTrue(member.name == "123_7" or member.name.startswith("123_7/"))
                self.assertNotIn("..", Path(member.name).parts)
                self.assertEqual(member.mode, 0o700 if member.isdir() else 0o600)
                self.assertFalse(member.issym())

    def test_unrelated_storage_entry_is_ignored_and_never_blocks_a_fetch(self):
        self.append(self.record())
        # Shared storage leaves stubs like .nfs00000001 beside the run directories.
        for name in (".nfs00000001", "notes.txt"):
            (self.root / "123_7" / name).write_text("")
        history = jobmon.SavedHistory("123_7")
        runs = history.refresh()
        self.assertEqual(len(runs), 1)
        self.assertEqual(history.errors, [])
        self.assertEqual(history.ignored, [".nfs00000001", "notes.txt"])

        class Backend:
            def snapshot(self, *args):
                return {"ownership_verified": False, "queue": [], "accounting": [],
                        "live_steps": [], "errors": ["scheduler offline"]}

        report, _ = jobmon.build_report("123_7", backend=Backend())
        self.assertEqual(report["ignored_storage_entries"], [".nfs00000001", "notes.txt"])
        self.assertIn("Ignored non-run entries in saved storage: .nfs00000001, notes.txt",
                      jobmon.format_report(report))
        result = subprocess.run([sys.executable, str(ROOT / "jobmon/jobmon.py"), "_transport"],
                                input=b'{"operation":"fetch","job_id":"123_7"}', capture_output=True, timeout=10)
        self.assertEqual(result.returncode, 0, result.stderr)
        with tarfile.open(fileobj=io.BytesIO(result.stdout), mode="r:gz") as archive:
            names = {Path(member.name).name for member in archive.getmembers()}
        self.assertNotIn("notes.txt", names)
        self.assertIn("metadata.json", names)

    def test_symlinked_run_directory_is_still_refused(self):
        self.append(self.record())
        (self.root / "123_7" / "linked-run").symlink_to(self.run)
        history = jobmon.SavedHistory("123_7")
        history.refresh()
        self.assertEqual(history.ignored, [])
        self.assertTrue(any("real directory" in error for error in history.errors), history.errors)
        with self.assertRaises(JobmonError):
            jobmon.export_history("123_7")

    def test_real_wrapper_preserves_stdin_and_failed_workload_status(self):
        env = dict(os.environ, HOME=self.temp.name, PATH=os.defpath, JOBMON_PYTHON=sys.executable,
                   JOBMON_COLLECTOR=str(ROOT / "jobmon/collect.py"), JOBMON_INTERVAL="1")
        result = subprocess.run(["/bin/bash", str(ROOT / "examples/monitored-job.sh"), "/bin/sh", "-c",
                                 'read -r value; printf "input=%s\\n" "$value"; exit 7'],
                                input="scientific-input\n", capture_output=True, text=True, env=env, timeout=15)
        self.assertEqual(result.returncode, 7, result.stderr)
        self.assertIn("input=scientific-input", result.stdout)
        completions = list(self.root.glob("*/*/completion.json"))
        self.assertEqual(len(completions), 1)
        self.assertEqual(json.loads(completions[0].read_text())["workload_exit_code"], 7)
        records = [json.loads(line) for path in completions[0].parent.glob("*.jsonl")
                   for line in path.read_text().splitlines()]
        self.assertGreaterEqual(len(records), 2)
        self.assertTrue(records[-1]["final"])

    def test_closed_stdin_is_preserved_with_and_without_monitoring(self):
        workload = ("import errno, os, sys\n"
                    "try:\n"
                    "    os.fstat(0)\n"
                    "except OSError as exc:\n"
                    "    if exc.errno != errno.EBADF: raise\n"
                    "    print('stdin-closed')\n"
                    "    sys.exit(7)\n"
                    "sys.exit(99)\n")
        for monitoring in (True, False):
            collector = ROOT / "jobmon/collect.py" if monitoring else Path(self.temp.name) / "missing.py"
            env = dict(os.environ, HOME=self.temp.name, PATH=os.defpath, JOBMON_PYTHON=sys.executable,
                       JOBMON_COLLECTOR=str(collector), JOBMON_INTERVAL="1")
            with self.subTest(monitoring=monitoring):
                result = subprocess.run(["/bin/bash", "-c", 'exec 0<&-; exec "$@"', "closed-stdin",
                                         "/bin/bash", str(ROOT / "examples/monitored-job.sh"), sys.executable,
                                         "-c", workload], env=env, capture_output=True, text=True, timeout=15)
                self.assertEqual(result.returncode, 7, result.stderr)
                self.assertEqual(result.stdout, "stdin-closed\n")
        completion = next(self.root.glob("*/*/completion.json"))
        self.assertEqual(json.loads(completion.read_text())["workload_exit_code"], 7)

    def test_wrapper_sigterm_records_interruption(self):
        import select
        env = dict(os.environ, HOME=self.temp.name, PATH=os.defpath, JOBMON_PYTHON=sys.executable,
                   JOBMON_COLLECTOR=str(ROOT / "jobmon/collect.py"), JOBMON_INTERVAL="1")
        process = subprocess.Popen(["/bin/bash", str(ROOT / "examples/monitored-job.sh"), sys.executable,
                                    "-c", "import time; print('workload-ready', flush=True); time.sleep(30)"],
                                   stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, env=env)
        try:
            self.assertTrue(select.select([process.stdout], [], [], 10)[0], "workload never started")
            self.assertEqual(process.stdout.readline().strip(), "workload-ready")
            process.terminate()
            _, error = process.communicate(timeout=10)
            self.assertEqual(process.returncode, 143, error)
            completion = next(self.root.glob("*/*/completion.json"))
            self.assertEqual(json.loads(completion.read_text())["workload_exit_code"], 143)
        finally:
            if process.poll() is None:
                process.kill()
                process.wait()

    def signal_workload(self, source, *args, monitoring=True):
        script = Path(self.temp.name) / "signal-workload.py"
        script.write_text(source)
        collector = ROOT / "jobmon/collect.py" if monitoring else script.parent / "missing.py"
        env = dict(os.environ, HOME=self.temp.name, PATH=os.defpath, JOBMON_PYTHON=sys.executable,
                   JOBMON_COLLECTOR=str(collector),
                   JOBMON_INTERVAL="1")
        process = subprocess.Popen(["bash", str(ROOT / "examples/monitored-job.sh"), sys.executable,
                                    str(script), *args], env=env, stdout=subprocess.PIPE,
                                   stderr=subprocess.PIPE, text=True, start_new_session=True)

        def cleanup():
            # This test owns the isolated group, including any leaked descendants.
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            process.wait()
            process.stdout.close()
            process.stderr.close()

        self.addCleanup(cleanup)
        self.assertTrue(select.select([process.stdout], [], [], 10)[0], "workload never started")
        payload = process.stdout.readline()
        self.assertTrue(payload, "wrapper exited before starting its workload")
        return process, json.loads(payload)

    def assert_process_stopped(self, pid):
        try:
            state = (Path("/proc") / str(pid) / "stat").read_text().rpartition(") ")[2].split()[0]
        except FileNotFoundError:
            return
        self.assertIn(state, ("Z", "X"), "workload process {} is still running".format(pid))

    def test_wrapper_interrupt_and_hangup_reach_workload_with_and_without_monitoring(self):
        source = ("import json,os,signal,time\n"
                  "print(json.dumps({'pid':os.getpid(), 'group':os.getpgrp(), "
                  "'ignores_int':signal.getsignal(signal.SIGINT)==signal.SIG_IGN}), flush=True)\n"
                  "time.sleep(60)\n")
        for signum, status, monitoring in ((signal.SIGINT, 130, True), (signal.SIGINT, 130, False),
                                          (signal.SIGHUP, 129, True), (signal.SIGHUP, 129, False)):
            with self.subTest(signum=signum, monitoring=monitoring):
                completed_before = set(self.root.glob("*/*/completion.json"))
                process, payload = self.signal_workload(source, monitoring=monitoring)
                self.assertFalse(payload["ignores_int"])
                self.assertEqual(payload["group"], os.getpgid(process.pid))
                process.send_signal(signum)
                self.assertEqual(process.wait(timeout=10), status)
                self.assert_process_stopped(payload["pid"])
                process.communicate(timeout=2)
                if monitoring:
                    completions = set(self.root.glob("*/*/completion.json")) - completed_before
                    self.assertEqual(len(completions), 1)
                    completion = completions.pop()
                    self.assertEqual(json.loads(completion.read_text())["workload_exit_code"], status)
                    records = [json.loads(line) for path in completion.parent.glob("*.jsonl")
                               for line in path.read_text().splitlines()]
                    self.assertTrue(records[-1]["final"])

    def test_wrapper_term_waits_for_descendant_shutdown_after_parent_exits(self):
        marker = Path(self.temp.name) / "child-finished"
        child = ("import signal,sys,time\n"
                 "from pathlib import Path\n"
                 "def finish(signum, frame):\n"
                 "    time.sleep(0.4)\n"
                 "    Path(sys.argv[1]).write_text('checkpoint finished')\n"
                 "    sys.exit(0)\n"
                 "signal.signal(signal.SIGTERM, finish)\n"
                 "print('child-ready', flush=True)\n"
                 "time.sleep(60)\n")
        source = ("import json,os,subprocess,sys,time\n"
                  "child=subprocess.Popen([sys.executable,'-c'," + repr(child) + ",sys.argv[1]], "
                  "stdout=subprocess.PIPE, text=True)\n"
                  "child.stdout.readline()\n"
                  "print(json.dumps({'pid':os.getpid(),'child':child.pid}), flush=True)\n"
                  "time.sleep(60)\n")
        process, payload = self.signal_workload(source, str(marker))
        process.terminate()
        self.assertEqual(process.wait(timeout=10), 143)
        self.assertTrue(marker.is_file(), "wrapper exited before its descendant finished shutdown")
        self.assert_process_stopped(payload["pid"])
        self.assert_process_stopped(payload["child"])
        completion = next(self.root.glob("*/*/completion.json"))
        self.assertEqual(json.loads(completion.read_text())["workload_exit_code"], 143)
        process.communicate(timeout=2)

    def test_wrapper_forwards_another_signal_while_waiting(self):
        source = ("import json,os,signal,time\n"
                  "signal.signal(signal.SIGINT, lambda s,f: print('interrupt-received', flush=True))\n"
                  "print(json.dumps({'pid':os.getpid()}), flush=True)\n"
                  "time.sleep(60)\n")
        process, payload = self.signal_workload(source, monitoring=False)
        process.send_signal(signal.SIGINT)
        self.assertTrue(select.select([process.stdout], [], [], 5)[0], "interrupt was not forwarded")
        self.assertEqual(process.stdout.readline().strip(), "interrupt-received")
        self.assertIsNone(process.poll(), "wrapper must wait for a workload that handles SIGINT")
        process.terminate()
        self.assertEqual(process.wait(timeout=5), 130)
        self.assert_process_stopped(payload["pid"])
        process.communicate(timeout=2)

    def test_signal_during_monitor_cleanup_preserves_finished_workload_status(self):
        collector = Path(self.temp.name) / "cleanup-collector.py"
        finished = Path(self.temp.name) / "finished-status"
        collector.write_text(
            "import os,sys,time\nfrom pathlib import Path\n"
            "if sys.argv[1]=='init': print(os.environ['TEST_RUN_DIR'])\n"
            "elif sys.argv[1]=='stop':\n"
            "    print('cleanup-ready', flush=True)\n"
            "    time.sleep(0.4)\n"
            "elif sys.argv[1]=='finish':\n"
            "    Path(os.environ['TEST_FINISHED_STATUS']).write_text(sys.argv[-1])\n")
        env = dict(os.environ, HOME=self.temp.name, JOBMON_PYTHON=sys.executable,
                   JOBMON_COLLECTOR=str(collector), TEST_RUN_DIR=str(self.run),
                   TEST_FINISHED_STATUS=str(finished))
        process = subprocess.Popen(["bash", str(ROOT / "examples/monitored-job.sh"),
                                    "sh", "-c", "exit 7"], env=env, stdout=subprocess.PIPE,
                                   stderr=subprocess.PIPE, text=True)
        try:
            self.assertTrue(select.select([process.stdout], [], [], 5)[0], "cleanup never started")
            self.assertEqual(process.stdout.readline().strip(), "cleanup-ready")
            process.terminate()
            _, error = process.communicate(timeout=5)
            self.assertEqual(process.returncode, 7, error)
            self.assertEqual(finished.read_text(), "7")
        finally:
            if process.poll() is None:
                process.kill()
                process.wait()
            process.stdout.close()
            process.stderr.close()


class CgroupAndGpuTests(unittest.TestCase):
    def test_discovers_job_ancestor_and_refuses_wrong_job_or_v1(self):
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory)
            mount = base / "cgroup"
            job_path = mount / "slurm/uid_1000/job_130"
            (job_path / "step_batch/task_0").mkdir(parents=True)
            proc = base / "proc"
            proc.mkdir()
            (proc / "cgroup").write_text("0::/slurm/uid_1000/job_130/step_batch/task_0\n")
            (proc / "mountinfo").write_text("1 0 0:1 / {} rw - cgroup2 cgroup rw\n".format(mount))
            self.assertEqual(collect.discover_cgroup("130", proc), (job_path, None))
            self.assertIsNone(collect.discover_cgroup("13", proc)[0])
            (proc / "cgroup").write_text("2:cpu:/slurm/uid_1000/job_130\n")
            self.assertIn("v1", collect.discover_cgroup("130", proc)[1])

    def test_numeric_gpu_ordinals_are_not_attribution(self):
        sampler = collect.NvidiaSampler(env={"CUDA_VISIBLE_DEVICES": "0", "SLURM_JOB_GPUS": "1"})
        records, errors = sampler.sample()
        self.assertEqual(records, [])
        self.assertIn("no verified GPU UUID", errors[0])

    def test_unset_gpu_mapping_is_off_without_gpus_and_explained_with_them(self):
        def no_nvidia(argv):
            raise AssertionError("GPU collection is off; nvidia-smi must not run")

        for env in ({}, {"SLURM_GPUS_ON_NODE": "0", "CUDA_VISIBLE_DEVICES": "NoDevFiles"}):
            with self.subTest(env=env):
                sampler = collect.NvidiaSampler(no_nvidia, env)
                self.assertFalse(sampler.enabled)
                self.assertEqual(sampler.sample(), ([], []))
        for env in ({"SLURM_GPUS_ON_NODE": "2"}, {"CUDA_VISIBLE_DEVICES": "0"}, {"SLURM_JOB_GPUS": "1,3"}):
            with self.subTest(env=env):
                records, errors = collect.NvidiaSampler(no_nvidia, env).sample()
                self.assertEqual(records, [])
                self.assertIn("no verified GPU UUID", errors[0])

    def test_gpu_zero_unavailable_and_mig_context(self):
        gpu_uuid = "GPU-aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"

        def runner(argv):
            if "--help-query-gpu" in argv:
                return "\n".join('"' + field + '"' for field in collect.NvidiaSampler.FIELDS)
            if "-L" in argv:
                return "GPU 0: Test\n MIG 1g device"
            return gpu_uuid + ", 0, 0, 1024, [N/A], [Not Supported]\n"

        sampler = collect.NvidiaSampler(runner, {"JOBMON_GPU_UUIDS": gpu_uuid, "JOBMON_GPU_EXCLUSIVE": "1"})
        records, errors = sampler.sample()
        self.assertEqual(errors, [])
        self.assertEqual(records[0]["device_utilization_percent"], 0)
        self.assertEqual(records[0]["memory_used_bytes"], 0)
        self.assertIsNone(records[0]["job_utilization_percent"])
        self.assertIsNone(records[0]["power_w"])
        self.assertEqual(records[0]["scope"], "shared_device_context")


class TransportTests(unittest.TestCase):
    def test_strict_transport(self):
        request = jobmon.transport_request(io.StringIO('{"operation":"report","job_id":"123_7","json":true}'))
        self.assertTrue(request["as_json"])
        watch = jobmon.transport_request(io.StringIO('{"operation":"watch","job_id":"1","utc_offset_minutes":-330}'))
        self.assertEqual(watch["utc_offset_minutes"], -330)
        storage = jobmon.transport_request(io.StringIO('{"operation":"storage-init","json":true}'))
        self.assertEqual(storage, {"operation": "storage-init", "as_json": True})
        for enabled in (True, False):
            watch = jobmon.transport_request(io.StringIO(json.dumps({
                "operation": "watch", "job_id": "123_7", "exit_on_completion": enabled})))
            self.assertIs(watch["exit_on_completion"], enabled)
        for value in ({"operation": "shell"}, {"operation": "show", "job_id": "1; id"},
                      {"operation": "doctor", "command": "id"}, {"operation": "watch", "job_id": "1", "interval": True},
                      {"operation": "watch", "job_id": "1", "interval": 0},
                      {"operation": "report", "job_id": "1", "json": "true"},
                      {"operation": "doctor", "days": 1}, {"operation": "jobs", "job_id": "1"},
                      {"operation": "fetch", "job_id": "1", "json": True},
                      {"operation": "watch", "job_id": "1", "utc_offset_minutes": 1440},
                      {"operation": "watch", "job_id": "1", "utc_offset_minutes": True},
                      {"operation": "watch", "job_id": "1", "utc_offset_minutes": "+0900"},
                      {"operation": "report", "job_id": "1", "utc_offset_minutes": 540},
                      {"operation": "watch", "job_id": "1", "exit_on_completion": "true"},
                      {"operation": "watch", "job_id": "1", "exit_on_completion": 1},
                      {"operation": "watch", "job_id": "1", "exit_on_completion": None},
                      {"operation": "report", "job_id": "1", "exit_on_completion": True},
                      {"operation": "jobs", "exit_on_completion": False}):
            with self.subTest(value=value), self.assertRaises(JobmonError):
                jobmon.transport_request(io.StringIO(json.dumps(value)))

    def test_zsh_arguments_are_stdin_json_and_exit_status_preserved(self):
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory)
            fake = base / "ssh"
            capture = base / "capture.json"
            fake.write_text("#!" + sys.executable + "\nimport json,os,sys\n"
                            "with open(os.environ['SSH_CAPTURE'],'w') as f: "
                            "json.dump({'argv':sys.argv[1:],'stdin':sys.stdin.read()}, f)\n"
                            "sys.exit(int(os.environ.get('SSH_STATUS', '0')))\n")
            fake.chmod(0o700)
            env = dict(os.environ, PATH=str(base) + os.pathsep + os.environ["PATH"],
                       SSH_CAPTURE=str(capture), HPC_JOBMON_HOST="test-alias")
            script = 'source "$1"; hpc-jreport 123_7 --json --days 7'
            result = subprocess.run(["zsh", "-f", "-c", script, "test", str(ROOT / "shell/jobmon.zsh")],
                                    env=env, capture_output=True, text=True)
            self.assertEqual(result.returncode, 0, result.stderr)
            sent = json.loads(capture.read_text())
            self.assertEqual(json.loads(sent["stdin"]),
                             {"operation": "report", "job_id": "123_7", "json": True, "days": 7})
            self.assertIn('$HOME/.local/libexec/jobmon/jobmon.py', sent["argv"][-1])
            self.assertNotIn("123_7", sent["argv"][-1])
            self.assertIn("-T", sent["argv"])
            capture.unlink()
            storage = subprocess.run(["zsh", "-f", "-c", 'source "$1"; hpc-jstorage-init --json',
                                     "test", str(ROOT / "shell/jobmon.zsh")], env=env, capture_output=True, text=True)
            self.assertEqual(storage.returncode, 0, storage.stderr)
            self.assertEqual(json.loads(json.loads(capture.read_text())["stdin"]),
                             {"operation": "storage-init", "json": True})
            env["SSH_STATUS"] = "255"
            result = subprocess.run(["zsh", "-f", "-c", script, "test", str(ROOT / "shell/jobmon.zsh")],
                                    env=env, capture_output=True)
            self.assertEqual(result.returncode, 255)
            capture.unlink()
            result = subprocess.run(["zsh", "-f", "-c", 'source "$1"; hpc-job "123; id"', "test",
                                     str(ROOT / "shell/jobmon.zsh")], env=env, capture_output=True)
            self.assertEqual(result.returncode, 2)
            self.assertFalse(capture.exists())
            result = subprocess.run(["zsh", "-f", "-c", 'source "$1"; hpc-jobs --days 1', "test",
                                     str(ROOT / "shell/jobmon.zsh")], env=env, capture_output=True)
            self.assertEqual(result.returncode, 2)
            self.assertFalse(capture.exists())

    def zsh_environment(self, base):
        capture = base / "ssh-calls.jsonl"
        (base / "bin").mkdir()
        fake = base / "bin/ssh"
        fake.write_text("#!" + sys.executable + "\nimport json,os,sys\n"
                        "call = {'args': sys.argv[1:], 'stdin': sys.stdin.read()}\n"
                        "with open(os.environ['SSH_CAPTURE'],'a') as f: f.write(json.dumps(call)+'\\n')\n")
        fake.chmod(0o700)
        (base / "home").mkdir()
        env = {key: value for key, value in os.environ.items() if not key.startswith("HPC_JOBMON_")}
        env.update(PATH=str(base / "bin") + os.pathsep + os.environ["PATH"], SSH_CAPTURE=str(capture),
                   HOME=str(base / "home"))
        return env, capture

    def last_call(self, capture):
        return json.loads(capture.read_text().splitlines()[-1])

    def last_request(self, capture):
        return json.loads(self.last_call(capture)["stdin"])

    def test_exit_on_completion_is_sent_as_json_and_rejected_for_other_operations(self):
        with tempfile.TemporaryDirectory() as directory:
            env, capture = self.zsh_environment(Path(directory))
            env["HPC_JOBMON_HOST"] = "test-alias"
            helpers = str(ROOT / "shell/jobmon.zsh")
            result = subprocess.run([
                "zsh", "-f", "-c", 'source "$1"; hpc-jwatch 123_7 --exit-on-completion --json --count 2',
                "test", helpers], env=env, capture_output=True, text=True, timeout=15)
            self.assertEqual(result.returncode, 0, result.stderr)
            request = self.last_request(capture)
            self.assertIs(request["exit_on_completion"], True)
            self.assertIs(request["json"], True)
            self.assertEqual(request["count"], 2)
            self.assertNotIn("exit_on_completion", self.last_call(capture)["args"][-1])
            calls = capture.read_text()
            for command in ("hpc-jreport 123_7", "hpc-job 123_7", "hpc-jobs", "hpc-jdoctor", "hpc-jstorage-init"):
                with self.subTest(command=command):
                    result = subprocess.run([
                        "zsh", "-f", "-c", 'source "$1"; ' + command + " --exit-on-completion", "test", helpers],
                        env=env, capture_output=True, text=True, timeout=15)
                    self.assertEqual(result.returncode, 2)
                    self.assertIn("not valid for", result.stderr)
                    self.assertEqual(capture.read_text(), calls)

    def test_ssh_destination_may_be_user_at_host_but_never_options(self):
        with tempfile.TemporaryDirectory() as directory:
            env, capture = self.zsh_environment(Path(directory))
            helpers = str(ROOT / "shell/jobmon.zsh")
            for value in (None, "", "login-host -p 2222", "-oProxyCommand=x", "ssh://user@login-host"):
                with self.subTest(value=value):
                    run_env = dict(env) if value is None else dict(env, HPC_JOBMON_HOST=value)
                    result = subprocess.run(["zsh", "-f", "-c", 'source "$1"; hpc-jobs', "test", helpers],
                                            env=run_env, capture_output=True, text=True, timeout=15,
                                    stdin=subprocess.DEVNULL)
                    self.assertEqual(result.returncode, 2)
                    self.assertIn("alias or user@host", result.stderr)
                    self.assertIn("~/.ssh/config", result.stderr)
                    self.assertFalse(capture.exists())
            result = subprocess.run(["zsh", "-f", "-c", 'source "$1"; hpc-jobs', "test", helpers],
                                    env=dict(env, HPC_JOBMON_HOST="user@login-host"),
                                    capture_output=True, text=True, timeout=15,
                                    stdin=subprocess.DEVNULL)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertIn("user@login-host", self.last_call(capture)["args"])

    def test_jdeploy_finds_the_clone_it_was_sourced_from(self):
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory)
            env, capture = self.zsh_environment(base)
            clone = base / "anywhere/my-clone"
            for name in ("jobmon", "shell", "examples"):
                shutil.copytree(ROOT / name, clone / name, ignore=shutil.ignore_patterns("__pycache__"))
            # A dotfile manager typically links the helpers into place.
            module = base / "home/.zsh/jobmon.zsh"
            module.parent.mkdir()
            module.symlink_to(clone / "shell/jobmon.zsh")
            env["HPC_JOBMON_HOST"] = "test-alias"
            script = 'source "$1"; hpc-jdeploy'
            result = subprocess.run(["zsh", "-f", "-c", script, "test", str(module)],
                                    env=env, capture_output=True, text=True, timeout=15,
                                    stdin=subprocess.DEVNULL)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertIn("Preview only", result.stdout)
            self.assertIn(str(clone.resolve() / "jobmon/jobmon.py"), result.stdout)
            self.assertIn("test-alias", self.last_call(capture)["args"])
            # An explicit HPC_JOBMON_TOOLS_DIR still takes precedence.
            result = subprocess.run(["zsh", "-f", "-c", script, "test", str(module)],
                                    env=dict(env, HPC_JOBMON_TOOLS_DIR=str(base / "missing")),
                                    capture_output=True, text=True, timeout=15,
                                    stdin=subprocess.DEVNULL)
            self.assertEqual(result.returncode, 2)
            self.assertIn("set HPC_JOBMON_TOOLS_DIR", result.stderr)

    def test_jwatch_sends_the_workstation_utc_offset(self):
        with tempfile.TemporaryDirectory() as directory:
            env, capture = self.zsh_environment(Path(directory))
            env["HPC_JOBMON_HOST"] = "test-alias"
            helpers = str(ROOT / "shell/jobmon.zsh")
            for zone, minutes in (("KST-9", 540), ("UTC", 0), ("IST-5:30", 330), ("EST5", -300)):
                with self.subTest(zone=zone):
                    result = subprocess.run(["zsh", "-f", "-c", 'source "$1"; hpc-jwatch 123_7 --count 1', "test",
                                             helpers], env=dict(env, TZ=zone), capture_output=True, text=True,
                                            timeout=15, stdin=subprocess.DEVNULL)
                    self.assertEqual(result.returncode, 0, result.stderr)
                    self.assertEqual(self.last_request(capture), {"operation": "watch", "job_id": "123_7", "count": 1,
                                               "utc_offset_minutes": minutes})
            result = subprocess.run(["zsh", "-f", "-c", 'source "$1"; hpc-jreport 123_7', "test", helpers],
                                    env=dict(env, TZ="KST-9"), capture_output=True, text=True, timeout=15,
                                    stdin=subprocess.DEVNULL)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertNotIn("utc_offset_minutes", self.last_request(capture))

    def job_table(self, *rows):
        return "JOB ID\tSTATE\tELAPSED\tCPUS\tNODES\tREASON / NODELIST\tJOB NAME\n" + "".join(
            "{}\t{}\t1:00\t4\t1\tnode1\t{}\n".format(row[0], row[1], row[2] if len(row) > 2 else "job-" + row[0])
            for row in rows)

    def pick(self, table, keys=(), args=(), lines=24, command="hpc-jwatch"):
        """Run a job picker in a pseudo-terminal; return (screen output, exit status, requests)."""
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory)
            capture = base / "requests.jsonl"
            fake = base / "ssh"
            fake.write_text("#!" + sys.executable + "\nimport json,os,sys\n"
                            "request = json.loads(sys.stdin.read())\n"
                            "with open(os.environ['SSH_CAPTURE'],'a') as f: f.write(json.dumps(request)+'\\n')\n"
                            "sys.stdout.write(os.environ['JOBS_TABLE'] if request['operation'] == 'jobs'\n"
                            "                 else 'watching ' + request['job_id'] + '\\n')\n")
            fake.chmod(0o700)
            env = {key: value for key, value in os.environ.items() if not key.startswith("HPC_JOBMON_")}
            env.update(PATH=str(base) + os.pathsep + os.environ["PATH"], SSH_CAPTURE=str(capture), TZ="UTC",
                       JOBS_TABLE=table, HPC_JOBMON_HOST="test-alias", TERM="xterm", COLUMNS="100",
                       LINES=str(lines))
            if command == "hpc-live":
                plotter = base / "tools/jobmon/live.py"
                plotter.parent.mkdir(parents=True)
                plotter.write_text("import sys\nprint('viewed ' + sys.argv[1])\n")
                env["HPC_JOBMON_TOOLS_DIR"] = str(plotter.parent.parent)
            pid, fd = pty.fork()
            if pid == 0:
                try:
                    # Report terminal settings afterwards: the menu must restore signal keys and echo.
                    script = ('source "$1"; shift; ' + command + ' "$@"; rc=$?; '
                              'print -r -- "terminal:" $(stty -a </dev/tty | tr " " "\\n" '
                              '| grep -xE -- "-?(isig|echo)"); exit $rc')
                    os.execvpe("zsh", ["zsh", "-f", "-c", script, "test", str(ROOT / "shell/jobmon.zsh"), *args], env)
                except OSError as exc:
                    os.write(2, ("Cannot start test shell zsh: {}\n".format(exc)).encode("utf-8", errors="replace"))
                finally:
                    # An exec failure must never return this child to the unittest runner.
                    os._exit(127)
            output, deadline, pending = b"", time.time() + 10, list(keys)
            try:
                while time.time() < deadline:
                    finished, status = os.waitpid(pid, os.WNOHANG)
                    if finished:
                        break
                    # Each key is sent once the menu has been drawn, then redrawn.
                    if pending and output.count(b"q or Esc: cancel") > len(keys) - len(pending):
                        os.write(fd, pending.pop(0))
                    if select.select([fd], [], [], 0.05)[0]:
                        try:
                            output += os.read(fd, 65536)
                        except OSError:
                            pass
                else:
                    os.kill(pid, signal.SIGKILL)
                    os.waitpid(pid, 0)
                    self.fail("hpc-jwatch did not finish; output: {!r}".format(output[-500:]))
                while select.select([fd], [], [], 0.05)[0]:
                    try:
                        chunk = os.read(fd, 65536)
                    except OSError:
                        break
                    if not chunk:
                        break
                    output += chunk
            finally:
                os.close(fd)
            requests = [json.loads(line) for line in capture.read_text().splitlines()] if capture.exists() else []
            return output.decode(errors="replace"), os.WEXITSTATUS(status), requests

    def test_jwatch_picker_child_exits_when_shell_start_fails(self):
        with patch.object(os, "execvpe", side_effect=FileNotFoundError("synthetic missing shell")):
            screen, status, requests = self.pick(self.job_table(("900_0", "RUNNING")))
        self.assertEqual(status, 127, screen)
        self.assertIn("Cannot start test shell zsh: synthetic missing shell", screen)
        self.assertNotIn("Traceback", screen)
        self.assertEqual(requests, [])

    def test_live_picker_uses_keys_and_passes_the_selected_id(self):
        table = self.job_table(("900_2", "PENDING"), ("900_0", "RUNNING", "first simulation"),
                               ("900_1", "RUNNING", "second simulation"))
        for keys, chosen in (([b"j", b"k", b"j", b"\n"], "900_1"),
                             ([b"\x1b[B", b"\x1b[A", b"\r"], "900_0")):
            with self.subTest(keys=keys):
                screen, status, requests = self.pick(table, keys, command="hpc-live")
                self.assertEqual(status, 0, screen)
                self.assertIn("Choose a running job to view", screen)
                self.assertIn("Enter: view", screen)
                self.assertIn("second simulation", screen)
                self.assertIn("viewed " + chosen, screen)
                self.assertIn("terminal: isig echo", screen)
                self.assertEqual(requests, [{"operation": "jobs"}])

    def test_live_picker_cancel_and_single_or_no_running_jobs(self):
        table = self.job_table(("900_0", "RUNNING"), ("900_1", "RUNNING"))
        for key in (b"q", b"\x1b", b"\x03"):
            with self.subTest(key=key):
                screen, status, requests = self.pick(table, [key], command="hpc-live")
                self.assertEqual(status, 130, screen)
                self.assertEqual(requests, [{"operation": "jobs"}])
                self.assertIn("terminal: isig echo", screen)
        screen, status, requests = self.pick(self.job_table(("900_0", "RUNNING")), command="hpc-live")
        self.assertEqual(status, 0, screen)
        self.assertNotIn("Choose a running job", screen)
        self.assertIn("viewed 900_0", screen)
        screen, status, requests = self.pick(self.job_table(("900_0", "PENDING")), command="hpc-live")
        self.assertEqual(status, 1, screen)
        self.assertIn("no running jobs to view", screen)
        self.assertEqual(requests, [{"operation": "jobs"}])

    def test_jwatch_without_an_id_picks_a_running_job_with_keys(self):
        table = self.job_table(("900_2", "PENDING"), ("900_0", "RUNNING"), ("900_1", "RUNNING"),
                               ("900_3", "PENDING"), ("900_4", "COMPLETING"))
        for keys, chosen in (([b"j", b"\n"], "900_1"), ([b"\x1b[B", b"\x1b[B", b"\x1b[A", b"\r"], "900_1"),
                             ([b"k", b"\n"], "900_0"), ([b"j", b"j", b"j", b"\n"], "900_4"),
                             ([b"\x1bOB", b"\n"], "900_1")):
            with self.subTest(keys=keys):
                screen, status, requests = self.pick(table, keys, ("--interval", "30"))
                self.assertEqual(status, 0, screen)
                self.assertEqual(requests, [{"operation": "jobs"},
                                            {"operation": "watch", "job_id": chosen, "interval": 30,
                                             "utc_offset_minutes": 0}])
                first = screen[:screen.index("q or Esc: cancel")]
                self.assertIn("Choose a running job to watch (1/3; 2 queued or not running)", first)
                self.assertIn("\x1b[7m> 900_0 ", first)
                self.assertNotIn("900_2", first)
                # The menu is erased and the cursor shown again before watching starts.
                after = screen[screen.rindex("q or Esc: cancel"):]
                self.assertIn("\x1b[J\x1b[?25h", after)
                self.assertIn("watching " + chosen, after)
                self.assertIn("terminal: isig echo", after)

    def test_jwatch_picker_cancels_without_watching(self):
        table = self.job_table(("900_0", "RUNNING"), ("900_1", "RUNNING"))
        for key in (b"q", b"\x1b", b"\x03"):
            with self.subTest(key=key):
                screen, status, requests = self.pick(table, [key])
                self.assertEqual(status, 130, screen)
                self.assertEqual(requests, [{"operation": "jobs"}])
                after = screen[screen.rindex("q or Esc: cancel"):]
                self.assertIn("\x1b[J\x1b[?25h", after)
                # Ctrl-C is read as a key while the menu is open, and signal keys come back.
                self.assertNotIn("^C", after)
                self.assertIn("terminal: isig echo", after)

    def test_jwatch_picker_shows_names_and_selects_by_id(self):
        long_name = "second simulation with a long descriptive name"
        table = self.job_table(("900_0", "RUNNING", "first simulation"), ("900_1", "RUNNING", long_name))
        screen, status, requests = self.pick(table, [b"j", b"\n"])
        self.assertEqual(status, 0, screen)
        self.assertIn("NAME", screen)
        self.assertIn("first simulation", screen)
        self.assertIn(long_name[:20], screen)
        self.assertNotIn(long_name, screen)
        self.assertEqual(requests[-1]["job_id"], "900_1")

    def test_jwatch_picker_accepts_an_older_jobs_table_without_names(self):
        table = self.job_table(("900_0", "RUNNING"), ("900_1", "RUNNING"))
        older = "\n".join(line.rsplit("\t", 1)[0] for line in table.splitlines()) + "\n"
        screen, status, requests = self.pick(older, [b"\n"])
        self.assertEqual(status, 0, screen)
        self.assertIn("unavailable", screen)
        self.assertEqual(requests[-1]["job_id"], "900_0")

    def test_jwatch_with_one_running_job_watches_it_without_a_menu(self):
        screen, status, requests = self.pick(self.job_table(("900_0", "RUNNING"), ("900_1", "PENDING")))
        self.assertEqual(status, 0, screen)
        self.assertNotIn("Choose a running job", screen)
        self.assertEqual(requests[-1], {"operation": "watch", "job_id": "900_0", "utc_offset_minutes": 0})

    def test_jwatch_with_no_running_jobs_says_so(self):
        screen, status, requests = self.pick(self.job_table(("900_0", "PENDING"), ("900_1", "PENDING")))
        self.assertEqual(status, 1, screen)
        self.assertIn("no running jobs to watch (2 queued or not running)", screen)
        self.assertEqual(requests, [{"operation": "jobs"}])

    def test_jwatch_picker_scrolls_a_list_taller_than_the_terminal(self):
        table = self.job_table(*(("900_{}".format(index), "RUNNING") for index in range(12)))
        screen, status, requests = self.pick(table, [b"j"] * 6 + [b"\n"], lines=8)
        self.assertEqual(status, 0, screen)
        self.assertEqual(requests[-1]["job_id"], "900_6")
        last = screen[:screen.rindex("q or Esc: cancel")]
        last = last[last.rindex("Choose a running job"):]
        self.assertIn("(7/12)", last)
        shown = [line for line in last.splitlines() if "900_" in line]
        self.assertEqual(len(shown), 4)
        self.assertIn("\x1b[7m> 900_6 ", last)

    def test_jwatch_without_an_id_outside_a_terminal_does_not_connect(self):
        with tempfile.TemporaryDirectory() as directory:
            env, capture = self.zsh_environment(Path(directory))
            env["HPC_JOBMON_HOST"] = "test-alias"
            for args in ("", " --count 1"):
                result = subprocess.run(["zsh", "-f", "-c", 'source "$1"; hpc-jwatch' + args, "test",
                                         str(ROOT / "shell/jobmon.zsh")], env=env, capture_output=True,
                                        text=True, timeout=15, stdin=subprocess.DEVNULL)
                self.assertEqual(result.returncode, 2)
                self.assertIn("expected a job ID", result.stderr)
                self.assertFalse(capture.exists())

    def test_standalone_fetch_preserves_callers_umask(self):
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory)
            fake = base / "ssh"
            fake.write_text("#!" + sys.executable + "\nimport sys\nsys.stdout.buffer.write(b'archive')\n")
            fake.chmod(0o700)
            destination = base / "downloads"
            env = dict(os.environ, PATH=str(base) + os.pathsep + os.environ["PATH"], HPC_JOBMON_HOST="test-alias")
            script = 'source "$1"; umask 022; hpc-jfetch 123 "$2"; umask'
            result = subprocess.run(["zsh", "-f", "-c", script, "test",
                                     str(ROOT / "shell/jobmon.zsh"), str(destination)],
                                    env=env, capture_output=True, text=True)
            self.assertEqual(result.returncode, 0, result.stderr)
            archive, mask = result.stdout.splitlines()
            self.assertEqual(mask, "022")
            self.assertEqual(Path(archive).read_bytes(), b"archive")

    def test_default_sampling_interval_is_one_minute(self):
        # The wrapper, the collector, and the example env.sh must agree, or a copied
        # env.sh silently pins a different default.
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory)
            log, run_dir = base / "collector.jsonl", base / "run"
            stub = base / "collect.py"
            stub.write_text("import json, os, pathlib, sys\n"
                            "with open(os.environ['STUB_LOG'], 'a') as f: f.write(json.dumps(sys.argv[1:]) + '\\n')\n"
                            "run = pathlib.Path(os.environ['STUB_RUN'])\n"
                            "if sys.argv[1] == 'init':\n"
                            "    run.mkdir(exist_ok=True); print(run)\n"
                            "elif sys.argv[1] == 'run':\n"
                            "    (run / 'stub.ready.json').write_text('{}')\n")
            env = {key: value for key, value in os.environ.items() if not key.startswith(("JOBMON_", "SLURM_"))}
            env.update(HOME=str(base), PATH=os.defpath, JOBMON_PYTHON=sys.executable, JOBMON_COLLECTOR=str(stub),
                       STUB_LOG=str(log), STUB_RUN=str(run_dir))
            result = subprocess.run(["/bin/bash", str(ROOT / "examples/monitored-job.sh"), "true"], env=env,
                                    capture_output=True, text=True, timeout=15, stdin=subprocess.DEVNULL)
            self.assertEqual(result.returncode, 0, result.stderr)
            started = [call for call in map(json.loads, log.read_text().splitlines()) if call[0] == "run"]
            self.assertEqual(len(started), 1, log.read_text())
            self.assertEqual(started[0][started[0].index("--interval") + 1], "60")
            sourced = subprocess.run(["sh", "-c", '. "$1"; printf %s "$JOBMON_INTERVAL"', "sh",
                                      str(ROOT / "examples/env.sh")], env=env, capture_output=True, text=True,
                                     timeout=15, stdin=subprocess.DEVNULL)
            self.assertEqual(sourced.stdout, "60", sourced.stderr)
        calls = []
        with patch.object(collect, "collect", lambda *args: calls.append(args)), \
                patch.object(collect.signal, "signal"):
            self.assertEqual(collect.main(["run", "--run-dir", "unused"]), 0)
        self.assertEqual(calls[0][1], 60)

    def test_monitor_failure_preserves_workload_exit_code(self):
        with tempfile.TemporaryDirectory() as directory:
            env = dict(os.environ, JOBMON_COLLECTOR=str(Path(directory) / "missing.py"), JOBMON_PYTHON=sys.executable)
            for code in (0, 7):
                result = subprocess.run(["bash", str(ROOT / "examples/monitored-job.sh"),
                                         "bash", "-c", "exit " + str(code)],
                                        env=env, capture_output=True, text=True, timeout=10)
                self.assertEqual(result.returncode, code, result.stderr)

    def test_deployment_preview_and_apply_with_simulated_ssh(self):
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory)
            remote = base / "remote"
            remote.mkdir()
            fake = base / "ssh"
            fake.write_text("#!" + sys.executable + "\nimport os,subprocess,sys\n"
                            "command = sys.argv[-1].replace('$HOME', '${REMOTE_TEST_HOME}')\n"
                            "sys.exit(subprocess.run(['/bin/sh','-c',command]).returncode)\n")
            fake.chmod(0o700)
            env = dict(os.environ, PATH=str(base) + os.pathsep + os.environ["PATH"], REMOTE_TEST_HOME=str(remote))
            command = ["bash", str(ROOT / "shell/deploy-jobmon.sh"), "test-alias"]
            result = subprocess.run(command, capture_output=True, text=True, env=env, timeout=10)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertIn("Preview only", result.stdout)
            self.assertIn("examples/monitored-job.sh", result.stdout)
            self.assertIn("Wrapper link:", result.stdout)
            self.assertFalse((remote / ".local").exists())
            result = subprocess.run(command + ["--apply"], capture_output=True, text=True, env=env, timeout=10)
            self.assertEqual(result.returncode, 0, result.stderr)
            destination = remote / ".local/libexec/jobmon"
            self.assertEqual(sorted(path.name for path in destination.iterdir()),
                             ["collect.py", "current", "jobmon.py", "releases", "slurm_backend.py"])
            self.assertTrue((destination / "current").is_symlink())
            release = (destination / "current").resolve()
            wrapper = remote / ".local/bin/monitored-job.sh"
            self.assertTrue(wrapper.is_symlink())
            self.assertEqual(wrapper.resolve(), release / "monitored-job.sh")
            self.assertEqual(wrapper.read_bytes(), (ROOT / "examples/monitored-job.sh").read_bytes())
            self.assertEqual(wrapper.stat().st_mode & 0o777, 0o700)
            self.assertIn("monitored-job.sh", (release / "SHA256SUMS").read_text())
            self.assertEqual(len(list((destination / "releases").iterdir())), 1)
            for name in ("jobmon.py", "slurm_backend.py", "collect.py"):
                path = destination / name
                self.assertTrue(path.is_symlink())
                self.assertEqual(path.read_bytes(), (ROOT / "jobmon" / name).read_bytes())
                self.assertEqual((release / name).stat().st_mode & 0o777, 0o600)
            active_before = os.readlink(destination / "current")
            config = remote / ".config/jobmon/env.sh"
            config.parent.mkdir(parents=True)
            config.write_text("export JOBMON_PYTHON=/bin/false\n")
            failed = subprocess.run(command + ["--apply"], capture_output=True, text=True, env=env, timeout=10)
            self.assertNotEqual(failed.returncode, 0)
            self.assertEqual(os.readlink(destination / "current"), active_before)
            self.assertEqual(wrapper.resolve(), release / "monitored-job.sh")


def write_stat(directory, pid, pgrp, utime=0, stime=0, cutime=0, cstime=0, rss=0, comm="python3"):
    """Write a synthetic /proc/<pid>/stat with proc(5) field positions."""
    entry = directory / str(pid)
    entry.mkdir(exist_ok=True)
    fields = (["R", "1", str(pgrp), "1", "0", "0", "0", "0", "0", "0", "0"]      # fields 3-13
              + [str(utime), str(stime), str(cutime), str(cstime)]               # fields 14-17
              + ["20", "0", "1", "0", "0", "0"]                                  # fields 18-23
              + [str(rss)])                                                      # field 24
    (entry / "stat").write_text("{} ({}) {}\n".format(pid, comm, " ".join(fields)))


class ProcessTreeTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.proc = Path(self.temp.name)
        self.ticks = os.sysconf("SC_CLK_TCK")
        self.page = os.sysconf("SC_PAGE_SIZE")

    def tearDown(self):
        self.temp.cleanup()

    def test_group_cpu_and_memory_are_summed_and_scoped(self):
        write_stat(self.proc, 100, 100, utime=self.ticks, stime=self.ticks, rss=10)
        write_stat(self.proc, 101, 100, utime=2 * self.ticks, rss=5)
        write_stat(self.proc, 200, 200, utime=99 * self.ticks, rss=1000)
        write_stat(self.proc, 102, 100, utime=7 * self.ticks, rss=7)
        sample = collect.process_tree_sample(100, self.proc, exclude=(102,))
        self.assertEqual(sample["scope"], "process_tree_pgid")
        self.assertEqual(sample["counted_processes"], 2)
        self.assertEqual(sample["cpu_seconds"], 4.0)
        self.assertEqual(sample["memory_bytes"], 15 * self.page)
        self.assertIsNone(sample["memory_peak_bytes"])
        self.assertEqual(sample["errors"], [])

    def test_reaped_children_keep_the_total_monotonic(self):
        write_stat(self.proc, 100, 100, utime=self.ticks, cutime=3 * self.ticks, cstime=self.ticks)
        self.assertEqual(collect.process_tree_sample(100, self.proc)["cpu_seconds"], 5.0)

    def test_command_with_spaces_and_parentheses_is_parsed(self):
        write_stat(self.proc, 100, 100, utime=self.ticks, rss=3, comm="my (odd) app")
        group, ticks, pages, command = collect.read_process_stat(100, self.proc)
        self.assertEqual((group, ticks, pages), (100, self.ticks, 3))
        self.assertEqual(command, "my (odd) app")

    def test_separate_step_launcher_is_reported_not_silently_undercounted(self):
        write_stat(self.proc, 100, 100, utime=self.ticks, comm="srun")
        sample = collect.process_tree_sample(100, self.proc)
        self.assertIsNotNone(sample["cpu_seconds"])
        self.assertTrue(any("srun" in error and "understate" in error for error in sample["errors"]))

    def test_empty_group_is_unavailable_rather_than_zero(self):
        write_stat(self.proc, 200, 200, utime=50 * self.ticks, rss=9)
        sample = collect.process_tree_sample(100, self.proc)
        self.assertIsNone(sample["cpu_seconds"])
        self.assertIsNone(sample["memory_bytes"])
        self.assertEqual(sample["counted_processes"], 0)
        self.assertIn("nothing is measurable", sample["errors"][0])

    def test_vanished_or_unparsable_entries_do_not_abort_the_sample(self):
        write_stat(self.proc, 100, 100, utime=self.ticks)
        (self.proc / "101").mkdir()
        (self.proc / "not-a-pid").mkdir()
        sample = collect.process_tree_sample(100, self.proc)
        self.assertEqual(sample["cpu_seconds"], 1.0)
        self.assertEqual(sample["counted_processes"], 1)

    def test_process_tree_scope_is_opt_in(self):
        group, reason = collect.discover_process_group({})
        self.assertIsNone(group)
        self.assertIn("JOBMON_PROCESS_TREE=1", reason)
        group, reason = collect.discover_process_group({"JOBMON_PROCESS_TREE": "1"})
        self.assertEqual(group, os.getpgid(0))
        self.assertIsNone(reason)


if __name__ == "__main__":
    unittest.main()
