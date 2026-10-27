#!/usr/bin/env python3
"""Local browser dashboard for per-job saved-sample JSON-over-SSH streams."""

import argparse
import bisect
from collections import deque
from concurrent.futures import ThreadPoolExecutor
import copy
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import math
import re
import secrets
import subprocess
import sys
import threading
import time
from urllib.parse import urlsplit
import webbrowser

from collect import SCHEMA_VERSION, checked_name
from jobmon import MAX_LINE, MEMORY_SCOPE_NOTES
from plot import metadata_for_plot, render_html, with_runtime, write_exports
from slurm_backend import JobmonError, validate_job_id

# As in _jobmon_ssh: user arguments travel only in JSON on stdin.
REMOTE_COMMAND = ('if [ -r "$HOME/.config/jobmon/env.sh" ]; then . "$HOME/.config/jobmon/env.sh" || exit; fi; '
                  'exec "${JOBMON_PYTHON:-python3}" "$HOME/.local/libexec/jobmon/jobmon.py" _transport')
# Allow for JSON escaping when raw samples are nested inside events.
MAX_EVENT_LINE = 4 * MAX_LINE
SNAPSHOT_TIMEOUT = 120


class LiveState:
    def __init__(self, job_id, interval=5, group=None):
        self.job_id = validate_job_id(job_id)
        self.interval = interval
        self.group = group
        self.changed = group.changed if group is not None else threading.Condition()
        self.version = 0
        self.runs = {}
        self.errors = deque(maxlen=100)
        self.scheduler = None
        self.scheduler_snapshot_at = None
        self.connection = "Connecting"
        self.updated_at = None
        self.last_received = time.monotonic()

    def apply(self, event):
        if (not isinstance(event, dict) or event.get("schema_version") != SCHEMA_VERSION
                or event.get("job_id") != self.job_id):
            raise JobmonError("Live event identity does not match the requested job.")
        with self.changed:
            kind = event.get("event")
            if kind == "run":
                meta = event.get("metadata")
                run_id = checked_name(meta.get("run_id")) if isinstance(meta, dict) else None
                meta = metadata_for_plot(meta, self.job_id, run_id)
                self.runs.setdefault(run_id, {"run_id": run_id, "job_name": meta.get("job_name"),
                    "metadata": copy.deepcopy(meta),
                    "expected_nodes": meta["expected_nodes"], "expected_node_count": meta.get("expected_node_count"),
                    "exit_code": None, "nodes": {}})
            elif kind == "sample":
                self._sample(event)
            elif kind == "status":
                if event.get("scheduler") is not None:
                    self.scheduler = event["scheduler"]
                    self.scheduler_snapshot_at = event.get("scheduler_snapshot_at")
                self.updated_at = event.get("generated_at")
                self.connection = "Live"
                for run in event.get("runs", []):
                    saved = self.runs.get(run.get("run_id"))
                    if saved is None:
                        continue
                    completion = run.get("completion")
                    if completion is not None:
                        if (isinstance(completion, dict) and completion.get("job_id") == self.job_id
                                and completion.get("run_id") == saved["run_id"]
                                and type(completion.get("workload_exit_code")) is int):
                            saved["exit_code"] = completion["workload_exit_code"]
                        else:
                            self._error("Ignoring invalid wrapper completion for run " + saved["run_id"])
                    for error in run.get("errors", []):
                        self._error(error)
                for error in event.get("errors", []):
                    self._error(error)
                if event.get("history_reason"):
                    self._error("No saved history yet; the submitted job must start the opt-in collector.")
                if self.scheduler:
                    for error in self.scheduler.get("errors", []):
                        self._error(error)
            else:
                raise JobmonError("Unknown live stream event.")
            self.last_received = time.monotonic()
            self._notify()

    def _notify(self):
        self.version += 1
        if self.group is not None:
            self.group.version += 1
        self.changed.notify_all()

    def _sample(self, event):
        run = self.runs.get(event.get("run_id"))
        if run is None:
            raise JobmonError("Sample arrived before its run metadata.")
        node = checked_name(event.get("node"))
        if run["expected_nodes"] and node not in run["expected_nodes"]:
            raise JobmonError("Sample names an unexpected allocation node.")
        point = copy.deepcopy(event.get("point"))
        if not isinstance(point, dict):
            raise JobmonError("Malformed live sample.")
        for key in ("time", "interval", "cpu", "average", "maximum", "memory", "memory_peak"):
            if key not in point:
                raise JobmonError("Missing live measurement: " + key)
            value = point.get(key)
            if value is None and key not in ("time", "interval"):
                continue
            if type(value) not in (int, float) or not math.isfinite(value) or value < 0:
                raise JobmonError("Invalid live measurement: " + key)
        if (not 1 <= point["interval"] <= 86400 or type(point.get("samples")) is not int or point["samples"] < 1
                or not isinstance(point.get("final"), bool) or not isinstance(point.get("scope"), str)
                or not isinstance(point.get("stream_id"), str) or not point["stream_id"].startswith(node + "--")):
            raise JobmonError("Invalid live sample scope or stream.")
        raw = event.get("raw_sample")
        if raw is not None:
            if not isinstance(raw, str):
                raise JobmonError("Raw live sample must be JSON text.")
            original = json.loads(raw)
            if (not isinstance(original, dict) or original.get("job_id") != self.job_id
                    or original.get("schema_version") != SCHEMA_VERSION
                    or original.get("job_id_raw") != run["metadata"]["job_id_raw"]
                    or original.get("run_id") != run["run_id"] or original.get("node") != node
                    or original.get("stream_id") != point["stream_id"]):
                raise JobmonError("Raw live sample identity mismatch.")
            point["raw_sample"] = raw
        group = run["nodes"].setdefault(node, {"node": node, "scopes": [], "points": []})
        if point["scope"] not in group["scopes"]:
            group["scopes"].append(point["scope"])
        points = group["points"]
        position = len(points) if not points or point["time"] >= points[-1]["time"] else bisect.bisect_right(
            [row["time"] for row in points], point["time"])
        points.insert(position, point)
        for index in range(position, min(position + 2, len(points))):
            current = points[index]
            old = points[index - 1] if index else None
            current["gap"] = (old is None or old["stream_id"] != current["stream_id"]
                              or old["scope"] != current["scope"]
                              or current["time"] - old["time"] > current["interval"] * 1500)

    def _error(self, value):
        message = str(value)[:1000]
        if message not in self.errors:
            self.errors.append(message)

    def disconnected(self, message):
        with self.changed:
            self.connection = message
            self._error(message)
            self._notify()

    def snapshot(self):
        with self.changed:
            connection = self.connection
            if connection == "Live" and time.monotonic() - self.last_received > max(30, self.interval * 3):
                connection = "Waiting for remote stream"
            runs = [dict(run, nodes=[run["nodes"][node] for node in sorted(run["nodes"])])
                    for _, run in sorted(self.runs.items())]
            return copy.deepcopy(with_runtime({"job_id": self.job_id, "runs": runs, "scope_notes": MEMORY_SCOPE_NOTES,
                "diagnostics": list(self.errors), "live": True, "connection": connection,
                "updated_at": self.updated_at, "scheduler": self.scheduler,
                "scheduler_snapshot_at": self.scheduler_snapshot_at}))


class MultiJobState:
    def __init__(self, job_ids, interval=5):
        ids = [validate_job_id(job_id) for job_id in job_ids]
        if not ids or len(ids) != len(set(ids)):
            raise JobmonError("Choose at least one job; duplicate job IDs are not allowed.")
        self.changed = threading.Condition()
        self.version = 0
        self.jobs = [LiveState(job_id, interval, group=self) for job_id in ids]

    def snapshot(self):
        with self.changed:
            if len(self.jobs) == 1:
                return self.jobs[0].snapshot()
            return {"live": True, "jobs": [dict(job.snapshot(), version=job.version) for job in self.jobs]}


class SSHSource:
    def __init__(self, state, host, count=None, days=30):
        if not re.fullmatch(r"[A-Za-z0-9_.@:][A-Za-z0-9_.@:-]*", host or ""):
            raise JobmonError("Use a valid SSH config alias or user@host.")
        self.state = state
        self.host = host
        self.count = count
        self.days = days
        self.received_status = False
        self.failure = None
        self.process = None
        self.thread = None
        self.stopping = threading.Event()

    def start(self):
        self.process = subprocess.Popen(
            ["ssh", "-T", "-o", "BatchMode=yes", "-o", "ConnectTimeout=10", "-o", "ConnectionAttempts=1",
             "-o", "ServerAliveInterval=15", "-o", "ServerAliveCountMax=2", "--", self.host, REMOTE_COMMAND],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            universal_newlines=True, bufsize=1, start_new_session=True)
        request = {"operation": "live", "job_id": self.state.job_id, "interval": self.state.interval}
        if self.count is not None:
            request["count"] = self.count
        if self.days != 30:
            request["days"] = self.days
        self.process.stdin.write(json.dumps(request) + "\n")
        self.process.stdin.close()
        self.thread = threading.Thread(target=self._read, daemon=True)
        self.thread.start()

    def _read(self):
        diagnostics = deque(maxlen=20)

        def errors():
            for line in self.process.stderr:
                diagnostics.append(line.strip()[:1000])

        stderr = threading.Thread(target=errors, daemon=True)
        stderr.start()
        try:
            while not self.stopping.is_set():
                line = self.process.stdout.readline(MAX_EVENT_LINE + 1)
                if not line:
                    break
                if len(line) > MAX_EVENT_LINE or not line.endswith("\n"):
                    raise JobmonError("Oversized or unfinished live event.")
                event = json.loads(line)
                self.state.apply(event)
                if event.get("event") == "status":
                    self.received_status = True
            status = self.process.wait()
            stderr.join(timeout=1)
            if not self.stopping.is_set() and (self.count is None or status != 0):
                message = "Remote stream ended (SSH exit {})".format(status)
                if diagnostics:
                    message += ": " + "; ".join(diagnostics)
                if any("Unsupported transport operation" in line for line in diagnostics):
                    message += ". Update the reporter with hpc-jdeploy --apply."
                self.state.disconnected(message)
                if status != 0:
                    self.failure = message
        except (OSError, ValueError, TypeError, KeyError, AttributeError, OverflowError, JobmonError) as exc:
            if not self.stopping.is_set():
                self.failure = "Remote stream error: " + str(exc)
                self.state.disconnected(self.failure)
                try:
                    self.process.terminate()
                except ProcessLookupError:
                    pass

    def close(self):
        self.stopping.set()
        if self.process is not None:
            if self.process.poll() is None:
                self.process.terminate()
                try:
                    self.process.wait(timeout=3)
                except subprocess.TimeoutExpired:
                    self.process.kill()
                    self.process.wait()
            if self.thread is not None:
                self.thread.join(timeout=3)
            for stream in (self.process.stdin, self.process.stdout, self.process.stderr):
                try:
                    stream.close()
                except OSError:
                    pass


class DashboardServer(ThreadingHTTPServer):
    daemon_threads = True
    close_grace = 0.5
    session_tick = 0.25

    def __init__(self, state, port=0):
        super().__init__(("127.0.0.1", port), DashboardHandler)
        self.state = state
        self.token = secrets.token_urlsafe(24)
        self.origin = "http://127.0.0.1:{}".format(self.server_port)
        self.url = self.origin + "/" + self.token + "/"
        self.sessions = 0
        self.idle_since = None
        self.lifecycle = threading.Lock()
        self.stopping = threading.Event()

    def service_actions(self):
        with self.lifecycle:
            expired = (not self.sessions and self.idle_since is not None
                       and time.monotonic() - self.idle_since >= self.close_grace)
        if expired and not self.stopping.is_set():
            self.stopping.set()
            threading.Thread(target=self.shutdown, daemon=True).start()


class DashboardHandler(BaseHTTPRequestHandler):
    server_version = "jobmon-live"
    sys_version = ""
    timeout = 10

    def log_message(self, fmt, *args):
        pass

    def send_headers(self, status, content_type, length=None):
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("Referrer-Policy", "no-referrer")
        self.send_header("Cross-Origin-Resource-Policy", "same-origin")
        self.send_header("Content-Security-Policy", "default-src 'none'; script-src 'unsafe-inline'; "
                         "style-src 'unsafe-inline'; connect-src 'self'; img-src data: blob:; "
                         "frame-ancestors 'none'; base-uri 'none'; form-action 'none'")
        if length is not None:
            self.send_header("Content-Length", str(length))
        self.end_headers()

    def reply(self, status, body, content_type="text/plain; charset=utf-8"):
        encoded = body.encode("utf-8")
        self.send_headers(status, content_type, len(encoded))
        self.wfile.write(encoded)

    def do_GET(self):
        server = self.server
        expected_host = server.origin.split("//", 1)[1]
        if self.headers.get("Host") != expected_host or self.headers.get("Origin", server.origin) != server.origin:
            self.reply(403, "Local origin required.")
            return
        prefix = "/" + server.token + "/"
        path = urlsplit(self.path).path
        if not path.startswith(prefix):
            self.reply(404, "Not found.")
            return
        endpoint = path[len(prefix):]
        try:
            if endpoint == "":
                self.reply(200, render_html(server.state.snapshot()), "text/html; charset=utf-8")
            elif endpoint == "data":
                self.reply(200, json.dumps(server.state.snapshot(), allow_nan=False), "application/json")
            elif endpoint == "events":
                self.events()
            else:
                self.reply(404, "Not found.")
        except (BrokenPipeError, ConnectionResetError, TimeoutError):
            pass

    def events(self):
        server = self.server
        with server.lifecycle:
            server.sessions += 1
            server.idle_since = None
        version = -1
        try:
            self.send_headers(200, "text/event-stream")
            # Reconnect sooner than the close grace expires after a brief drop.
            self.wfile.write(b"retry: 100\n\n")
            while not server.stopping.is_set():
                with server.state.changed:
                    if version == server.state.version:
                        server.state.changed.wait(timeout=server.session_tick)
                    changed = version != server.state.version
                    version = server.state.version
                    payload = json.dumps(server.state.snapshot(), allow_nan=False) if changed else None
                self.wfile.write(("data: " + payload + "\n\n" if payload else ": heartbeat\n\n").encode())
                self.wfile.flush()
                # Coalesce an initial history burst rather than repainting per sample.
                server.stopping.wait(server.session_tick)
        finally:
            with server.lifecycle:
                server.sessions -= 1
                if not server.sessions:
                    server.idle_since = time.monotonic()


def open_browser(url):
    # Browser launchers may block until closed; serve HTTP independently.
    try:
        if not webbrowser.open(url):
            print("Open the printed URL in your browser.", file=sys.stderr)
    except (OSError, webbrowser.Error):
        print("Open the printed URL in your browser.", file=sys.stderr)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("job_id", nargs="+", metavar="JOB_ID")
    parser.add_argument("--host", required=True)
    parser.add_argument("--interval", type=int, default=5, help="remote saved-file refresh seconds")
    parser.add_argument("--port", type=int, default=0, help="local port; 0 chooses a free port")
    parser.add_argument("--no-browser", action="store_true", help="print the URL without opening it")
    parser.add_argument("--zip", action="store_true", help="save CPU and memory ZIPs and exit")
    parser.add_argument("--output-dir", default="downloads", help="private ZIP output directory")
    parser.add_argument("--days", type=int, default=30, help="accounting lookback for ZIP export")
    args = parser.parse_args(argv)
    sources, server = [], None
    try:
        if not 1 <= args.interval <= 86400 or not 0 <= args.port <= 65535:
            raise JobmonError("Interval must be 1–86400 seconds and port must be 0–65535.")
        if not 1 <= args.days <= 3650 or (args.zip and len(args.job_id) != 1):
            raise JobmonError("ZIP export needs one job ID; days must be 1–3650.")
        state = MultiJobState(args.job_id, args.interval)
        if args.zip:
            source = SSHSource(state.jobs[0], args.host, count=1, days=args.days)
            sources.append(source)
            source.start()
            source.thread.join(timeout=SNAPSHOT_TIMEOUT)
            if source.thread.is_alive():
                raise JobmonError("Timed out waiting for a complete saved-history snapshot; retry the export.")
            if source.failure or not source.received_status:
                raise JobmonError(source.failure or "Remote stream ended before a complete history snapshot arrived.")
            for path in write_exports(state.snapshot(), args.output_dir):
                print(path, flush=True)
            return 0
        sources = [SSHSource(job, args.host) for job in state.jobs]
        server = DashboardServer(state, args.port)
        for source in sources:
            try:
                source.start()
            except OSError as exc:
                if len(sources) == 1:
                    raise
                source.state.disconnected("Could not start remote stream: " + str(exc))
        print("hpc-live: " + server.url, flush=True)
        print("Close the last dashboard browser tab or press Ctrl-C to stop viewing.", flush=True)
        if not args.no_browser:
            threading.Thread(target=open_browser, args=(server.url,), daemon=True).start()
        server.serve_forever(poll_interval=0.1)
        return 0
    except KeyboardInterrupt:
        return 130
    except (OSError, JobmonError, ValueError) as exc:
        print(("hpc-jreport: " if args.zip else "hpc-live: ") + str(exc), file=sys.stderr)
        return 2
    finally:
        if server is not None:
            server.stopping.set()
            server.server_close()
        if sources:
            with ThreadPoolExecutor(max_workers=min(32, len(sources))) as closing:
                list(closing.map(lambda source: source.close(), sources))


if __name__ == "__main__":
    sys.exit(main())

