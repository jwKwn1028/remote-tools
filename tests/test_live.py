"""Full-history streaming, local dashboard lifecycle, and SSH transport tests."""

import contextlib
import io
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from unittest.mock import patch
from urllib.error import HTTPError
from urllib.request import Request, urlopen
import zipfile

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "jobmon"))

import collect
import jobmon
import live
from slurm_backend import JobmonError, identity
from test_plot import JOB_ID, RUN_ID, STREAM, metadata, sample


def events(rows=None, job_id=JOB_ID):
    rows = rows if rows is not None else [sample(0), sample(30, 60), sample(60, 120)]
    raw_id = job_id.split("_", 1)[0]
    rows = [dict(row, job_id=job_id, job_id_raw=raw_id) for row in rows]
    meta = dict(metadata(), job_id=job_id, job_id_raw=raw_id)
    history = jobmon.RunHistory(Path(RUN_ID), meta)
    result = [{"event": "run", "metadata": meta}]
    for row in rows:
        history.consume(row, row["stream_id"] + ".jsonl")
        result.append({"event": "sample", "run_id": RUN_ID, "node": row["node"],
                       "point": jobmon.sample_point(history, row), "raw_sample": json.dumps(row)})
    result.append({"event": "status", "generated_at": "2026-09-01T00:01:00+00:00",
                   "scheduler_snapshot_at": "2026-09-01T00:01:00+00:00",
                   "scheduler": {"queue": [{"state": "RUNNING"}], "errors": []}, "runs": []})
    return [dict(event, job_id=job_id, schema_version=1) for event in result]


class LiveStateTests(unittest.TestCase):
    def state(self):
        state = live.LiveState(JOB_ID)
        for event in events():
            state.apply(event)
        return state

    def test_initial_data_starts_at_run_beginning_and_new_samples_append(self):
        state = self.state()
        initial = state.snapshot()
        points = initial["runs"][0]["nodes"][0]["points"]
        self.assertEqual(len(points), 3)
        self.assertEqual(points[0]["time"], jobmon.timestamp_seconds(sample(0)["timestamp"]) * 1000)
        self.assertEqual([point["cpu"] for point in points], [None, 50, 100])
        self.assertEqual(json.loads(points[0]["raw_sample"]), sample(0))
        self.assertEqual(initial["runs"][0]["metadata"], metadata())
        state.apply(events([sample(90, 60)])[1])
        updated = state.snapshot()["runs"][0]["nodes"][0]["points"]
        self.assertEqual(len(updated), 4)
        self.assertEqual(updated[0], points[0])
        self.assertEqual(updated[-1]["cpu"], 50)

    def test_gaps_and_unavailable_values_remain_explicit_and_late_samples_sort(self):
        state = live.LiveState(JOB_ID)
        for event in events([sample(0), sample(90, duration=90, memory=None)]):
            state.apply(event)
        points = state.snapshot()["runs"][0]["nodes"][0]["points"]
        self.assertTrue(points[-1]["gap"])
        self.assertIsNone(points[-1]["cpu"])
        self.assertIsNone(points[-1]["memory"])
        state.apply(events([sample(30, 60)])[1])
        points = state.snapshot()["runs"][0]["nodes"][0]["points"]
        self.assertEqual([point["time"] for point in points], sorted(point["time"] for point in points))
        self.assertFalse(points[1]["gap"])
        self.assertTrue(points[2]["gap"])

    def test_runtime_uses_slurm_start_and_advances_only_with_saved_samples(self):
        state = live.LiveState(JOB_ID)
        packets = events()
        packets[0]["metadata"]["created_at"] = "2026-09-01T00:00:02+00:00"
        packets[-1]["scheduler"]["queue"][0]["elapsed"] = "2:40:10"
        for event in packets:
            state.apply(event)
        runtime = state.snapshot()["runs"][0]["runtime"]
        self.assertEqual((runtime["source"], runtime["seconds"], runtime["display"]),
                         ("slurm", 9610, "2:40:10"))
        # File refreshes can advance while no new collector sample is saved.
        status = dict(packets[-1], scheduler=None, generated_at="2026-09-01T00:02:00+00:00")
        state.apply(status)
        self.assertEqual(state.snapshot()["runs"][0]["runtime"], runtime)
        state.apply(events([sample(90, 60)])[1])
        updated = state.snapshot()["runs"][0]["runtime"]
        self.assertEqual((updated["seconds"], updated["display"]), (9640, "2:40:40"))
        state.disconnected("synthetic connection lost")
        self.assertEqual(state.snapshot()["runs"][0]["runtime"], updated)

    def test_runtime_keeps_requeues_separate_from_the_initial_slurm_snapshot(self):
        state = live.LiveState(JOB_ID)
        packets = events()
        packets[0]["metadata"]["created_at"] = "2026-09-01T00:00:02+00:00"
        packets[-1]["scheduler"]["queue"][0]["elapsed"] = "5:00"
        for event in packets:
            state.apply(event)
        second = "20260901T000500Z-r1-abcdef012345"
        meta = dict(metadata(second), created_at="2026-09-01T00:05:00+00:00")
        state.apply(dict(packets[0], metadata=meta))
        point = dict(packets[1]["point"], time=packets[1]["point"]["time"] + 330000)
        state.apply(dict(packets[1], run_id=second, point=point,
                         raw_sample=json.dumps(sample(330, run_id=second))))
        runs = state.snapshot()["runs"]
        # The initial snapshot still belongs to the older allocation.
        self.assertEqual((runs[0]["runtime"]["source"], runs[0]["runtime"]["seconds"]), ("slurm", 300))
        self.assertEqual((runs[1]["runtime"]["source"], runs[1]["runtime"]["seconds"]), ("monitoring", 30))

    def test_invalid_foreign_or_nonfinite_events_do_not_change_data(self):
        state = self.state()
        before = state.snapshot()
        for key, value in (("job_id", "999"), ("point", {"time": float("nan")}),
                           ("raw_sample", json.dumps(dict(sample(0), job_id_raw="999")))):
            event = events()[1]
            event[key] = value
            with self.subTest(key=key), self.assertRaises(JobmonError):
                state.apply(event)
        self.assertEqual(state.snapshot(), before)

    def test_wrapper_completion_is_validated_per_run_and_retained_after_disconnect(self):
        state = self.state()
        status = dict(events()[-1], scheduler=None)
        completion = {"job_id": JOB_ID, "run_id": RUN_ID, "workload_exit_code": 0}

        def finish(value, run_id=RUN_ID):
            state.apply(dict(status, runs=[{"run_id": run_id, "completion": value, "errors": []}]))

        for invalid in ([], {}, dict(completion, job_id="999"), dict(completion, run_id="foreign-run"),
                        dict(completion, workload_exit_code=True), dict(completion, workload_exit_code="0")):
            with self.subTest(invalid=invalid):
                finish(invalid)
                self.assertIsNone(state.snapshot()["runs"][0]["exit_code"])
        # A final collector sample is not a wrapper completion record.
        state.apply(events([dict(sample(90, 60), final=True)])[1])
        self.assertIsNone(state.snapshot()["runs"][0]["exit_code"])
        for code in (0, 1, 137):
            finish(dict(completion, workload_exit_code=code))
            self.assertEqual(state.snapshot()["runs"][0]["exit_code"], code)
        state.disconnected("synthetic connection lost")
        self.assertEqual(state.snapshot()["runs"][0]["exit_code"], 137)
        second = "20260902T000000Z-r1-abcdef012345"
        state.apply(dict(events()[0], metadata=metadata(second)))
        runs = state.snapshot()["runs"]
        self.assertEqual(runs[0]["exit_code"], 137)
        self.assertIsNone(runs[1]["exit_code"])
        self.assertEqual(state.snapshot()["scheduler"]["queue"][0]["state"], "RUNNING")

    def test_disconnected_stream_retains_history_and_stale_stream_is_labelled(self):
        state = self.state()
        before = state.snapshot()["runs"]
        state.last_received -= 60
        self.assertEqual(state.snapshot()["connection"], "Waiting for remote stream")
        state.disconnected("synthetic connection lost")
        self.assertEqual(state.snapshot()["runs"], before)
        self.assertIn("synthetic connection lost", state.snapshot()["diagnostics"])


class MultiJobStateTests(unittest.TestCase):
    def test_each_job_notifies_the_shared_stream_and_retains_its_own_history(self):
        state = live.MultiJobState([JOB_ID, "456_2"])
        for job in state.jobs:
            for event in events(job_id=job.job_id):
                before = state.version
                job.apply(event)
                self.assertEqual(state.version, before + 1)
        initial = state.snapshot()
        self.assertEqual([job["job_id"] for job in initial["jobs"]], [JOB_ID, "456_2"])
        state.jobs[1].apply(events([sample(90, 60)], job_id="456_2")[1])
        updated = state.snapshot()
        self.assertEqual(updated["jobs"][0], initial["jobs"][0])
        self.assertEqual(len(updated["jobs"][1]["runs"][0]["nodes"][0]["points"]), 4)
        version = state.version
        with self.assertRaises(JobmonError):
            state.jobs[0].apply(events(job_id="456_2")[1])
        self.assertEqual(state.version, version)
        state.jobs[1].disconnected("synthetic connection lost")
        disconnected = state.snapshot()
        self.assertEqual(disconnected["jobs"][0], initial["jobs"][0])
        self.assertEqual(disconnected["jobs"][1]["runs"], updated["jobs"][1]["runs"])
        self.assertIn("synthetic connection lost", disconnected["jobs"][1]["diagnostics"])
        disconnected["jobs"][0]["runs"].clear()
        self.assertTrue(state.snapshot()["jobs"][0]["runs"])

    def test_one_job_preserves_the_existing_snapshot_shape(self):
        state = live.MultiJobState([JOB_ID])
        for event in events():
            state.jobs[0].apply(event)
        self.assertEqual(state.snapshot(), state.jobs[0].snapshot())
        self.assertNotIn("jobs", state.snapshot())

    def test_all_ids_are_validated_before_any_connection_or_server(self):
        for ids in ([], [JOB_ID, JOB_ID], [JOB_ID, "../invalid"]):
            with self.subTest(ids=ids), self.assertRaises(JobmonError):
                live.MultiJobState(ids)
        with patch.object(live, "SSHSource") as source, patch.object(live, "DashboardServer") as server, \
                contextlib.redirect_stderr(io.StringIO()):
            self.assertEqual(live.main([JOB_ID, "../invalid", "--host", "test-alias"]), 2)
        source.assert_not_called()
        server.assert_not_called()


class RemoteStreamTests(unittest.TestCase):
    def test_initial_history_then_only_appends_with_one_scheduler_snapshot(self):
        with tempfile.TemporaryDirectory() as folder:
            root = collect.private_directory(Path(folder) / "state", create=True)
            job = collect.private_directory(root / JOB_ID, create=True)
            run = collect.private_directory(job / RUN_ID, create=True)
            meta = dict(metadata(), uid=os.getuid(), user=identity())
            collect.write_json(run / "metadata.json", meta)
            path = run / (STREAM + ".jsonl")
            rows = [sample(0), sample(30, 60), sample(60, 120)]
            with collect.private_open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, "w") as stream:
                for row in rows:
                    stream.write(json.dumps(row) + "\n")
            sleeps = []

            def append(_):
                with collect.private_open(path, os.O_WRONLY | os.O_APPEND, "a") as stream:
                    if not sleeps:
                        stream.write(json.dumps(sample(90, 60)) + "\n")
                        stream.write(json.dumps(sample(120, 120))[:30])
                    else:
                        stream.write(json.dumps(sample(120, 120))[30:] + "\n")
                sleeps.append(True)

            scheduler = {"accounting": [], "queue": [], "errors": [], "ownership_verified": True}
            with patch.dict(os.environ, JOBMON_DATA_DIR=str(root)), \
                    patch.object(jobmon.SlurmBackend, "snapshot", return_value=scheduler) as snapshot, \
                    patch.object(jobmon.time, "sleep", side_effect=append), \
                    contextlib.redirect_stdout(io.StringIO()) as output:
                jobmon.stream_history(JOB_ID, interval=5, count=3)
            packets = [json.loads(line) for line in output.getvalue().splitlines()]
            statuses = [index for index, event in enumerate(packets) if event["event"] == "status"]
            initial = [event for event in packets[:statuses[0]] if event["event"] == "sample"]
            self.assertEqual(len(initial), 3)
            self.assertEqual([json.loads(event["raw_sample"]) for event in initial], rows)
            self.assertEqual(initial[0]["point"]["time"], jobmon.timestamp_seconds(sample(0)["timestamp"]) * 1000)
            all_samples = [event for event in packets if event["event"] == "sample"]
            self.assertEqual(len(all_samples), 5)
            self.assertEqual([event["point"]["samples"] for event in all_samples], [1, 2, 3, 4, 5])
            snapshot.assert_called_once()
            self.assertEqual(sum(event.get("scheduler") is not None for event in packets), 1)

    def test_live_request_is_validated_before_scheduler_or_storage(self):
        request = jobmon.transport_request(io.StringIO(json.dumps(
            {"operation": "live", "job_id": JOB_ID, "interval": 2, "count": 3})))
        self.assertEqual(request["operation"], "live")
        for invalid in ({"operation": "live", "job_id": "../bad"},
                        {"operation": "live", "job_id": JOB_ID, "interval": 0},
                        {"operation": "live", "job_id": JOB_ID, "shell": "ignored"}):
            with self.subTest(invalid=invalid), self.assertRaises(JobmonError):
                jobmon.transport_request(io.StringIO(json.dumps(invalid)))


class LocalServerTests(unittest.TestCase):
    def setUp(self):
        self.state = live.LiveState(JOB_ID)
        for event in events():
            self.state.apply(event)
        self.server = live.DashboardServer(self.state)
        self.thread = threading.Thread(target=self.server.serve_forever, kwargs={"poll_interval": 0.05}, daemon=True)
        self.thread.start()
        self.addCleanup(self.close)

    def close(self):
        self.server.stopping.set()
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=3)

    def test_page_and_data_are_private_local_no_store_and_theme_matches(self):
        with urlopen(self.server.url, timeout=3) as reply:
            html = reply.read().decode()
            self.assertIn("#292d3e", html)
            self.assertIn("JetBrains Mono", html)
            self.assertIn('new EventSource("events")', html)
            self.assertEqual(reply.headers["Cache-Control"], "no-store")
            self.assertIn("frame-ancestors 'none'", reply.headers["Content-Security-Policy"])
        with urlopen(self.server.url + "data", timeout=3) as reply:
            self.assertEqual(json.load(reply)["runs"][0]["nodes"][0]["points"][0]["cpu"], None)
        for request in (Request(self.server.origin + "/"), Request(self.server.url, headers={"Host": "foreign"}),
                        Request(self.server.url, headers={"Origin": "https://foreign.invalid"})):
            with self.subTest(request=request.full_url), self.assertRaises(HTTPError) as failure:
                urlopen(request, timeout=3)
            self.assertIn(failure.exception.code, (403, 404))

    def test_sse_delivers_history_and_new_samples_without_reopening_ssh(self):
        response = urlopen(self.server.url + "events", timeout=4)
        self.addCleanup(response.close)

        def update():
            for _ in range(10):
                line = response.readline().decode()
                if line.startswith("data: "):
                    return json.loads(line[6:])
            self.fail("No SSE data")

        before = update()
        self.assertEqual(len(before["runs"][0]["nodes"][0]["points"]), 3)
        self.state.apply(events([sample(90, 60)])[1])
        after = update()
        self.assertEqual(len(after["runs"][0]["nodes"][0]["points"]), 4)
        self.assertEqual(after["runs"][0]["nodes"][0]["points"][0], before["runs"][0]["nodes"][0]["points"][0])

    def test_closing_last_tab_stops_server_but_another_tab_keeps_it_alive(self):
        first = urlopen(self.server.url + "events", timeout=4)
        second = urlopen(self.server.url + "events", timeout=4)
        first.readline()
        second.readline()
        self.assertEqual(self.server.sessions, 2)
        first.close()
        deadline = time.monotonic() + 6
        while self.server.sessions > 1 and time.monotonic() < deadline:
            time.sleep(0.05)
        self.assertEqual(self.server.sessions, 1)
        self.assertFalse(self.server.stopping.is_set())
        second.close()
        self.assertTrue(self.server.stopping.wait(timeout=3))
        self.thread.join(timeout=3)
        self.assertFalse(self.thread.is_alive())

    def test_reload_within_close_grace_preserves_server_and_history(self):
        first = urlopen(self.server.url + "events", timeout=4)
        first.readline()
        before = self.state.snapshot()["runs"]
        first.close()
        deadline = time.monotonic() + 3
        while self.server.sessions and time.monotonic() < deadline:
            time.sleep(0.01)
        self.assertEqual(self.server.sessions, 0)
        self.assertFalse(self.server.stopping.is_set())
        second = urlopen(self.server.url + "events", timeout=4)
        self.addCleanup(second.close)
        second.readline()
        # A live replacement tab must outlast the shortened close grace.
        time.sleep(0.7)
        self.assertEqual(self.server.sessions, 1)
        self.assertFalse(self.server.stopping.is_set())
        self.assertEqual(self.state.snapshot()["runs"], before)

    def test_last_tab_grace_expires_after_half_a_second(self):
        with self.server.lifecycle:
            self.server.idle_since = time.monotonic() - 0.6
        self.server.service_actions()
        self.assertTrue(self.server.stopping.is_set())

    def test_one_event_stream_updates_all_jobs_and_stops_after_browser_close(self):
        group = live.MultiJobState([JOB_ID, "456_2"])
        for job in group.jobs:
            for event in events(job_id=job.job_id):
                job.apply(event)
        self.server.state = group
        response = urlopen(self.server.url + "events", timeout=4)
        self.addCleanup(response.close)

        def update():
            for _ in range(20):
                line = response.readline().decode()
                if line.startswith("data: "):
                    return json.loads(line[6:])
            self.fail("No multi-job SSE data")

        initial = update()
        self.assertEqual(len(initial["jobs"]), 2)
        group.jobs[1].apply(events([sample(90, 60)], job_id="456_2")[1])
        updated = update()
        self.assertEqual(updated["jobs"][0], initial["jobs"][0])
        self.assertEqual(len(updated["jobs"][1]["runs"][0]["nodes"][0]["points"]), 4)
        self.assertEqual(self.server.sessions, 1)
        response.close()
        self.assertTrue(self.server.stopping.wait(timeout=3))


class SSHAndShellTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.base = Path(self.temp.name)

    def zip_ssh(self, packets, status=0, delay=0):
        fixture = self.base / "zip-events.jsonl"
        fixture.write_text("".join(json.dumps(packet) + "\n" for packet in packets))
        capture = self.base / "zip-request.json"
        fake = self.base / "ssh"
        fake.write_text("#!" + sys.executable + "\nimport json,os,pathlib,sys,time\n"
                        "pathlib.Path(os.environ['CAPTURE']).write_text(json.dumps({'args':sys.argv[1:],"
                        "'request':json.loads(sys.stdin.read())}))\n"
                        "sys.stdout.write(pathlib.Path(os.environ['EVENTS']).read_text());sys.stdout.flush()\n"
                        "time.sleep(" + str(delay) + ")\nsys.exit(" + str(status) + ")\n")
        fake.chmod(0o700)
        env = {key: value for key, value in os.environ.items() if not key.startswith("HPC_JOBMON_")}
        env.update(PATH=str(self.base) + os.pathsep + os.environ["PATH"], CAPTURE=str(capture), EVENTS=str(fixture),
                   HPC_JOBMON_HOST="test-alias")
        return env, capture

    def test_report_zip_shell_downloads_a_complete_snapshot_and_both_charts(self):
        rows = [sample(0), sample(30, 0, memory=None), sample(60, 120)]
        rows[0]["large_counter"] = 2 ** 60 + 1
        packets = events(rows)
        packets[0]["metadata"]["job_name"] = "synthetic simulation α"
        packets[-1]["runs"] = [{"run_id": RUN_ID, "completion": {
            "job_id": JOB_ID, "run_id": RUN_ID, "workload_exit_code": 0}, "errors": []}]
        env, capture = self.zip_ssh(packets)
        destination = self.base / "saved exports"
        result = subprocess.run(["zsh", "-f", "-c",
                                 'source "$1"; hpc-jreport 123_7 --zip --output-dir "$2" --days 90',
                                 "test", str(ROOT / "shell/jobmon.zsh"), str(destination)],
                                env=env, stdin=subprocess.DEVNULL, capture_output=True, text=True, timeout=10)
        self.assertEqual(result.returncode, 0, result.stderr)
        paths = [Path(line) for line in result.stdout.splitlines()]
        self.assertEqual(len(paths), 2)
        self.assertNotIn("http://", result.stdout)
        for path, chart in zip(paths, ("cpu", "memory")):
            self.assertTrue(path.name.startswith("jobmon-123_7-synthetic-simulation-α-" + RUN_ID + "-node1-" + chart))
            with zipfile.ZipFile(path) as package:
                saved = json.loads(package.read("data.json"))
                self.assertEqual(saved["chart"], chart)
                self.assertEqual(saved["workload_exit_code"], 0)
                self.assertEqual(len(saved["points"]), 3)
                self.assertTrue(saved["raw_samples_complete"])
                self.assertFalse(any("Remote stream ended" in item for item in saved["diagnostics"]))
                raw = [json.loads(line) for line in package.read("samples.jsonl").splitlines()]
                self.assertEqual(raw, rows)
                self.assertEqual(raw[0]["large_counter"], 2 ** 60 + 1)
        request = json.loads(capture.read_text())
        self.assertEqual(request["request"], {"operation": "live", "job_id": JOB_ID,
                                             "interval": 5, "count": 1, "days": 90})
        self.assertEqual(request["args"][-1], live.REMOTE_COMMAND)

    def test_zip_mode_never_opens_a_browser_or_local_server(self):
        env, _ = self.zip_ssh(events())
        with patch.dict(os.environ, env), patch.object(live, "DashboardServer") as server, \
                patch.object(live.webbrowser, "open") as browser, contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(live.main([JOB_ID, "--host", "test-alias", "--zip",
                                        "--output-dir", str(self.base / "zip")]), 0)
        server.assert_not_called()
        browser.assert_not_called()

    def test_zip_mode_rejects_failed_partial_and_timed_out_snapshots(self):
        for label, packets, status, delay in (("failed", events(), 23, 0),
                                              ("partial", events()[:-1], 0, 0),
                                              ("empty", [events()[-1]], 0, 0),
                                              ("timeout", [], 0, 10)):
            with self.subTest(label=label):
                env, _ = self.zip_ssh(packets, status, delay)
                destination = self.base / label
                with patch.dict(os.environ, env), patch.object(live, "SNAPSHOT_TIMEOUT", 0.2), \
                        contextlib.redirect_stderr(io.StringIO()) as errors:
                    result = live.main([JOB_ID, "--host", "test-alias", "--zip", "--output-dir", str(destination)])
                self.assertEqual(result, 2, errors.getvalue())
                self.assertFalse(destination.exists())
                self.assertNotIn("Traceback", errors.getvalue())

    def test_zip_shell_rejects_invalid_options_before_connecting(self):
        env, capture = self.zip_ssh(events())
        for args in ("123_7 --zip --json", "123_7 --zip --days 0", "123_7 --zip --days 3651",
                     "123_7 --zip --output-dir", "../invalid --zip", "123_7 --zip --interval 2"):
            with self.subTest(args=args):
                result = subprocess.run(["zsh", "-f", "-c", 'source "$1"; hpc-jreport ' + args,
                                         "test", str(ROOT / "shell/jobmon.zsh")],
                                        env=env, capture_output=True, text=True, timeout=10)
                self.assertEqual(result.returncode, 2, result.stderr)
                self.assertFalse(capture.exists())
                self.assertFalse(result.stdout)

    def test_single_fixed_ssh_command_json_stdin_and_clean_shutdown(self):
        capture = self.base / "request.json"
        fixture = self.base / "events.jsonl"
        rows = [sample(0), sample(30, 60), sample(60, 120)]
        # A valid saved line may grow when its original JSON is nested in an event.
        rows[0]["extra"] = "\\" * (jobmon.MAX_LINE // 3)
        self.assertLess(len(json.dumps(rows[0])), jobmon.MAX_LINE)
        fixture.write_text("".join(json.dumps(event) + "\n" for event in events(rows)))
        fake = self.base / "ssh"
        fake.write_text("#!" + sys.executable + "\nimport json,os,pathlib,sys,time\n"
                        "pathlib.Path(os.environ['CAPTURE']).write_text(json.dumps({'args':sys.argv[1:],"
                        "'request':json.loads(sys.stdin.read())}))\n"
                        "sys.stdout.write(pathlib.Path(os.environ['EVENTS']).read_text());sys.stdout.flush()\n"
                        "time.sleep(60)\n")
        fake.chmod(0o700)
        state = live.LiveState(JOB_ID, 2)
        source = live.SSHSource(state, "test-alias")
        with patch.dict(os.environ, PATH=str(self.base) + os.pathsep + os.environ["PATH"],
                        CAPTURE=str(capture), EVENTS=str(fixture)):
            source.start()
        self.addCleanup(source.close)
        deadline = time.monotonic() + 4
        while state.connection != "Live" and time.monotonic() < deadline:
            time.sleep(0.01)
        self.assertEqual(len(state.snapshot()["runs"][0]["nodes"][0]["points"]), 3)
        original = state.snapshot()["runs"][0]["nodes"][0]["points"][0]["raw_sample"]
        self.assertEqual(json.loads(original)["extra"], rows[0]["extra"])
        request = json.loads(capture.read_text())
        self.assertEqual(request["request"], {"operation": "live", "job_id": JOB_ID, "interval": 2})
        self.assertEqual(request["args"][-1], live.REMOTE_COMMAND)
        self.assertNotIn(JOB_ID, request["args"][-1])
        source.close()
        self.assertIsNotNone(source.process.poll())

    def test_invalid_hosts_are_rejected_before_launch(self):
        for host in ("", "-option", "ssh://foreign", "host; command", "host --option"):
            with self.subTest(host=host), self.assertRaises(JobmonError):
                live.SSHSource(live.LiveState(JOB_ID), host)

    def test_browser_launch_cannot_block_http_serving(self):
        serving, opened = threading.Event(), threading.Event()

        def browser(_):
            if serving.wait(timeout=2):
                opened.set()
            return True

        def serve(**kwargs):
            serving.set()
            self.assertTrue(opened.wait(timeout=2), "Browser launch blocked the server")

        with patch.object(live, "DashboardServer") as server, patch.object(live, "SSHSource") as source, \
                patch.object(live.webbrowser, "open", side_effect=browser), \
                contextlib.redirect_stdout(io.StringIO()):
            server.return_value.serve_forever.side_effect = serve
            self.assertEqual(live.main([JOB_ID, "--host", "test-alias"]), 0)
            source.return_value.close.assert_called_once()

    def test_main_starts_and_closes_every_job_and_isolates_a_failed_stream(self):
        with patch.object(live, "DashboardServer") as server, patch.object(live, "SSHSource") as source, \
                contextlib.redirect_stdout(io.StringIO()):
            readers = []

            def reader(state, host):
                from unittest.mock import Mock
                item = Mock(state=state)
                if not readers:
                    item.start.side_effect = OSError("synthetic SSH launch failure")
                readers.append(item)
                return item

            source.side_effect = reader
            self.assertEqual(live.main([JOB_ID, "456_2", "--host", "test-alias", "--no-browser"]), 0)
            self.assertEqual([item.state.job_id for item in readers], [JOB_ID, "456_2"])
            for item in readers:
                item.start.assert_called_once()
                item.close.assert_called_once()
            group = server.call_args[0][0]
            snapshots = group.snapshot()["jobs"]
            self.assertIn("synthetic SSH launch failure", snapshots[0]["diagnostics"][0])
            self.assertEqual(snapshots[1]["connection"], "Connecting")
            server.return_value.serve_forever.assert_called_once()

    def test_without_an_id_collects_all_started_jobs_noninteractively(self):
        script = self.base / "clone/jobmon/live.py"
        script.parent.mkdir(parents=True)
        script.write_text("import json,sys\nprint(json.dumps(sys.argv[1:]))\n")
        fake = self.base / "ssh"
        fake.write_text("#!" + sys.executable + "\nimport json,os,sys\n"
                        "with open(os.environ['CAPTURE'],'a') as f: f.write(sys.stdin.read())\n"
                        "sys.stdout.write(os.environ['JOBS_TABLE'])\n")
        fake.chmod(0o700)
        env = {key: value for key, value in os.environ.items() if not key.startswith("HPC_JOBMON_")}
        capture = self.base / "requests.jsonl"
        env.update(PATH=str(self.base) + os.pathsep + os.environ["PATH"], HPC_JOBMON_HOST="test-alias",
                   HPC_JOBMON_TOOLS_DIR=str(script.parent.parent), CAPTURE=str(capture))
        for rows, ids, status in (
                ([("900_0", "RUNNING"), ("900_1", "COMPLETING"), ("900_2", "PENDING")], ["900_0", "900_1"], 0),
                ([("900_0", "RUNNING"), ("900_0", "RUNNING")], ["900_0"], 0),
                ([("900_2", "PENDING")], [], 1),
                ([("900_0", "RUNNING"), ("../invalid", "RUNNING")], [], 2)):
            with self.subTest(rows=rows):
                table = "JOB ID\tSTATE\n" + "".join("\t".join(row) + "\n" for row in rows)
                result = subprocess.run(["zsh", "-f", "-c", 'source "$1"; hpc-live --no-browser',
                                         "test", str(ROOT / "shell/jobmon.zsh")],
                                        env=dict(env, JOBS_TABLE=table), stdin=subprocess.DEVNULL,
                                        capture_output=True, text=True, timeout=10)
                self.assertEqual(result.returncode, status, result.stderr)
                self.assertNotIn("Choose a running job", result.stderr)
                if ids:
                    self.assertEqual(json.loads(result.stdout),
                                     ids + ["--host", "test-alias", "--interval", "5", "--port", "0", "--no-browser"])
                else:
                    self.assertFalse(result.stdout)
        requests = [json.loads(line) for line in capture.read_text().splitlines()]
        self.assertEqual(requests, [{"operation": "jobs"}] * 4)

    def test_shell_name_options_and_clone_resolution_without_a_server(self):
        script = self.base / "clone/jobmon/live.py"
        script.parent.mkdir(parents=True)
        script.write_text("import json,sys\nprint(json.dumps(sys.argv[1:]))\n")
        env = {key: value for key, value in os.environ.items() if not key.startswith("HPC_JOBMON_")}
        env.update(HPC_JOBMON_HOST="test-alias", HPC_JOBMON_TOOLS_DIR=str(script.parent.parent))
        result = subprocess.run(["zsh", "-f", "-c", 'source "$1"; hpc-live 123_7 --interval 2 --port 0 --no-browser',
                                 "test", str(ROOT / "shell/jobmon.zsh")],
                                env=env, capture_output=True, text=True, timeout=10)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(json.loads(result.stdout),
                         [JOB_ID, "--host", "test-alias", "--interval", "2", "--port", "0", "--no-browser"])
        for args in ("--interval 0", "--interval 86401", "--port 65536", "--port", '"1;bad"', "123 --invalid"):
            command = 'source "$1"; hpc-live ' + args
            result = subprocess.run(["zsh", "-f", "-c", command, "test", str(ROOT / "shell/jobmon.zsh")],
                                    env=env, capture_output=True, text=True, timeout=10, stdin=subprocess.DEVNULL)
            self.assertEqual(result.returncode, 2, (args, result.stderr))
            self.assertFalse(result.stdout)


if __name__ == "__main__":
    unittest.main()

