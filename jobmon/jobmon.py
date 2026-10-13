#!/usr/bin/env python3
"""Personal Slurm reports and saved history, with a JSON-over-SSH interface."""

import argparse
import bisect
from contextlib import ExitStack
import datetime as dt
import heapq
import json
import os
from pathlib import Path
import sys
import tarfile
import time

from collect import (PROCESS_TREE_SCOPE, SCHEMA_VERSION, data_root, initialize_storage_probe, load_run,
                     private_directory, private_open, read_json, utc_now,
                     verify_storage_probe)
from slurm_backend import (JobmonError, SlurmBackend, accounting_disabled, cpu_seconds, number,
                           validate_job_id)

MAX_LINE = 1024 * 1024
# Bound a fast node's pending readings while another node's file is delayed.
MEMORY_ALIGNMENT_BUFFER_SAMPLES = 128
# Scopes that carry per-node memory readings, with the wording each one earns.
MEMORY_SCOPE_NOTES = {
    "job_cgroup_v2": "job cgroup charges including cache; not process RSS; independent peaks are not concurrent",
    "process_tree_pgid": "summed process RSS within the batch process group; shared pages are counted once per "
                         "process; processes leaving the group or exiting between samples are not counted",
}


def timestamp_seconds(value):
    try:
        return dt.datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()
    except (TypeError, ValueError, AttributeError):
        return None


class RunHistory:
    """Incremental JSONL reader and streaming aggregates, bounded by nodes/devices."""

    def __init__(self, directory, metadata):
        self.directory = directory
        self.metadata = metadata
        self.cursors = {}
        self.nodes = {}
        self.gpus = {}
        self.latest_memory = {}
        self.pending_memory = {}
        self.memory_watermark = None
        self.memory_sequence = 0
        self.memory_alignment_incomplete = False
        self.concurrent_memory_peak = None
        self.aligned_memory_samples = 0
        self.errors = set(metadata.get("errors", []))
        expected = metadata["expected_nodes"]
        count = metadata.get("expected_node_count")
        self.node_layout_complete = bool(expected) and (count is None or count == len(expected))
        if not self.node_layout_complete:
            self.errors.add("Incomplete expected-node layout; concurrent memory totals and collector completeness "
                            "are unavailable, but per-node readings are retained.")
        self.partial_lines = set()
        self.stream_last = {}
        self.cpu_numerator = 0.0
        self.cpu_denominator = 0.0
        self.cpu_missing_denominator_nodes = set()

    def consume(self, record, stream_name):
        identity = ("job_id", "job_id_raw", "run_id")
        if not isinstance(record, dict) or any(record.get(k) != self.metadata[k] for k in identity):
            raise ValueError("sample job/run identity mismatch")
        node = record.get("node")
        if (record.get("schema_version") != SCHEMA_VERSION or not isinstance(node, str)
                or not isinstance(record.get("stream_id"), str)
                or stream_name != node + "--" + record.get("stream_id", "").split("--")[-1] + ".jsonl"):
            raise ValueError("sample schema or stream identity mismatch")
        if self.metadata["expected_nodes"] and node not in self.metadata["expected_nodes"]:
            raise ValueError("unexpected allocation node")
        when = timestamp_seconds(record.get("timestamp"))
        metrics = record.get("metrics")
        if not isinstance(metrics, dict) or not isinstance(metrics.get("scope"), str):
            raise ValueError("sample metrics must contain a string scope")
        scope = metrics["scope"]
        if not isinstance(record.get("final", False), bool):
            raise ValueError("sample final flag must be a boolean")
        gpu_rows = record.get("gpus", [])
        if not isinstance(gpu_rows, list) or not isinstance(record.get("errors", []), list):
            raise ValueError("GPU records and collection errors must be lists")
        for gpu in gpu_rows:
            if (not isinstance(gpu, dict) or not isinstance(gpu.get("uuid"), str)
                    or not isinstance(gpu.get("scope"), str)
                    or not isinstance(gpu.get("unavailable_fields", []), list)
                    or any(not isinstance(field, str) for field in gpu.get("unavailable_fields", []))):
                raise ValueError("invalid GPU record")
        # Finish all parsing that can reject a record before touching aggregates.
        try:
            tick = number(record.get("monotonic_seconds"))
            interval = number(record.get("interval_seconds"))
            cpus = number(metrics.get("allocated_cpus"))
            delta = number(metrics.get("cpu_delta_seconds"))
            duration = number(metrics.get("cpu_interval_seconds"))
            memory_value = number(metrics.get("memory_bytes"))
            kernel_peak = number(metrics.get("memory_peak_bytes"))
            gpu_values = [(number(gpu.get("memory_used_bytes")), number(gpu.get("device_utilization_percent")))
                          for gpu in gpu_rows]
        except OverflowError as exc:
            raise ValueError("sample numeric value is too large") from exc
        if when is None or tick is None or not interval:
            raise ValueError("sample timestamp or interval is invalid")
        previous = self.stream_last.get(stream_name)
        if previous and tick <= number(previous["monotonic_seconds"]):
            raise ValueError("non-increasing monotonic sample time")
        state = self.nodes.setdefault(node, {"samples": 0, "first": when, "last": when,
                     "largest_gap_seconds": 0, "gaps": 0, "memory_peak_bytes": None,
                     "cgroup_memory_peak_bytes": None, "cpu_seconds_covered": 0.0,
                     "cpu_seconds_available": False, "cpu_wall_seconds_covered": 0.0,
                     "allocated_cpus": None, "final": False, "scope": scope,
                     "interval_seconds": interval, "latest_interval_cpu_percent": None,
                     "latest_interval_busy_cores": None, "latest_memory_bytes": None,
                     "max_interval_cpu_percent": None, "max_interval_busy_cores": None})
        if state["samples"] and when > state["last"]:
            gap = when - state["last"]
            state["largest_gap_seconds"] = max(gap, state["largest_gap_seconds"])
            if gap > interval * 1.5:
                state["gaps"] += 1
        state["samples"] += 1
        state["first"] = min(state["first"], when)
        newest = when >= state["last"]
        if newest:
            state["last"] = when
            state["final"] = bool(record.get("final"))
            state["scope"] = scope
        state["allocated_cpus"] = cpus
        valid_interval = delta is not None and duration and duration <= interval * 1.5
        if newest:
            # The newest interval alone, so a ramp-up is not hidden by the running average.
            # A first sample, counter reset, or gap leaves it unavailable rather than zero.
            state["interval_seconds"] = interval
            state["latest_interval_busy_cores"] = delta / duration if valid_interval else None
            state["latest_interval_cpu_percent"] = (100 * delta / (duration * cpus)
                                                    if valid_interval and cpus else None)
            state["latest_memory_bytes"] = memory_value if scope in MEMORY_SCOPE_NOTES else None
        if valid_interval:
            state["cpu_seconds_available"] = True
            state["cpu_seconds_covered"] += delta
            state["cpu_wall_seconds_covered"] += duration
            # The busiest single interval so far, the CPU counterpart of a memory peak.
            state["max_interval_busy_cores"] = max(delta / duration, state["max_interval_busy_cores"] or 0)
            if cpus:
                state["max_interval_cpu_percent"] = max(100 * delta / (duration * cpus),
                                                        state["max_interval_cpu_percent"] or 0)
            if cpus:
                self.cpu_numerator += delta
                self.cpu_denominator += duration * cpus
            else:
                self.cpu_missing_denominator_nodes.add(node)
        if scope in MEMORY_SCOPE_NOTES:
            for value, target in ((memory_value, "memory_peak_bytes"), (kernel_peak, "cgroup_memory_peak_bytes")):
                if value is not None:
                    state[target] = max(value, state[target] or 0)
        current = memory_value if scope in MEMORY_SCOPE_NOTES else None
        self.record_memory(node, when, current, interval, stream_name)
        for gpu, (memory, _) in zip(gpu_rows, gpu_values):
            key = (node, gpu["uuid"], gpu.get("scope"))
            summary = self.gpus.setdefault(key, {"node": node, "uuid": gpu["uuid"], "scope": gpu.get("scope"),
                    "weighted_utilization": 0.0, "covered_device_seconds": 0.0,
                    "memory_peak_bytes": None, "unavailable_fields": set()})
            summary["unavailable_fields"].update(gpu.get("unavailable_fields", []))
            if memory is not None:
                summary["memory_peak_bytes"] = max(memory, summary["memory_peak_bytes"] or 0)
            if previous:
                duration = tick - number(previous["monotonic_seconds"])
                old = next((g for g in previous.get("gpus", [])
                            if g["uuid"] == gpu["uuid"] and g.get("scope") == gpu.get("scope")), None)
                utilization = number(old.get("device_utilization_percent")) if old else None
                if 0 < duration <= interval * 1.5 and utilization is not None:
                    summary["weighted_utilization"] += utilization * duration
                    summary["covered_device_seconds"] += duration
        self.stream_last[stream_name] = record
        for error in record.get("errors", []):
            if len(self.errors) < 100:
                self.errors.add(str(error))

    def record_memory(self, node, when, current, interval, stream_name):
        expected = self.metadata["expected_nodes"]
        if not self.node_layout_complete:
            return
        if current is not None:
            if self.memory_watermark is not None and when <= self.memory_watermark:
                self.memory_alignment_incomplete = True
                self.errors.add("Concurrent memory alignment incomplete: a sample arrived behind the committed "
                                "time frontier; start a fresh report to recompute saved history.")
            else:
                self.memory_sequence += 1
                entry = (when, stream_name, self.memory_sequence, node, current, interval)
                bisect.insort(self.pending_memory.setdefault(node, []), entry)
        if all(n in self.nodes for n in expected):
            # Only commit times every node has reached. The remaining tail can
            # be recomputed when delayed records arrive, including revised peaks.
            watermark = min(self.nodes[n]["last"] for n in expected)
            ready = []
            for pending in self.pending_memory.values():
                split = 0
                while split < len(pending) and pending[split][0] <= watermark:
                    split += 1
                ready.extend(pending[:split])
                del pending[:split]
            self.concurrent_memory_peak, self.aligned_memory_samples = self.align_memory(
                ready, self.latest_memory, self.concurrent_memory_peak, self.aligned_memory_samples)
            self.memory_watermark = watermark
        for pending in self.pending_memory.values():
            if len(pending) > MEMORY_ALIGNMENT_BUFFER_SAMPLES:
                del pending[:-MEMORY_ALIGNMENT_BUFFER_SAMPLES]
                self.memory_alignment_incomplete = True
                self.errors.add("Concurrent memory alignment incomplete: delayed nodes exceeded the {} pending "
                                "samples per node; start a fresh report to recompute saved history.".format(
                                    MEMORY_ALIGNMENT_BUFFER_SAMPLES))

    def align_memory(self, entries, latest, peak, samples):
        if not self.node_layout_complete:
            return None, 0
        expected = self.metadata["expected_nodes"]
        for when, _, _, node, current, interval in sorted(entries):
            latest[node] = (when, current, interval)
            if all(n in latest for n in expected):
                readings = [latest[n] for n in expected]
                tolerance = min(row[2] for row in readings) / 2
                if max(row[0] for row in readings) - min(row[0] for row in readings) <= tolerance:
                    peak = max(sum(row[1] for row in readings), peak or 0)
                    samples += 1
        return peak, samples

    def refresh(self):
        # Merge streams chronologically with one pending record per stream. This
        # keeps both memory use and cross-node peak alignment independent of job length.
        pending = []
        with ExitStack() as opened:
            for path in sorted(self.directory.glob("*.jsonl")):
                try:
                    stream = opened.enter_context(private_open(path, os.O_RDONLY))
                    info = os.fstat(stream.fileno())
                    inode = (info.st_dev, info.st_ino)
                    old_inode, offset = self.cursors.get(path.name, (inode, 0))
                    if old_inode != inode or info.st_size < offset:
                        self.errors.add(path.name + ": stream replaced or truncated; earlier aggregates "
                                        "retained, new content skipped")
                        continue
                    stream.seek(offset)
                    self._queue_next(stream, path.name, inode, pending)
                except (OSError, JobmonError, UnicodeError) as exc:
                    self.errors.add(path.name + ": " + str(exc))
            while pending:
                _, name, record, stream, inode = heapq.heappop(pending)
                try:
                    self.consume(record, name)
                except (ValueError, TypeError, KeyError, AttributeError) as exc:
                    if len(self.errors) < 100:
                        self.errors.add(name + ": invalid record: " + str(exc))
                self._queue_next(stream, name, inode, pending)

    def _queue_next(self, stream, name, inode, pending):
        while True:
            start = stream.tell()
            try:
                line = stream.readline(MAX_LINE + 1)
                if not line:
                    self.cursors[name] = (inode, start)
                    return
                if len(line) > MAX_LINE:
                    self.errors.add(name + ": oversized record; remaining stream skipped")
                    self.cursors[name] = (inode, start)
                    return
                if not line.endswith("\n"):
                    self.partial_lines.add(name)
                    self.cursors[name] = (inode, start)
                    return
                self.partial_lines.discard(name)
                self.cursors[name] = (inode, stream.tell())
                record = json.loads(line)
                when = timestamp_seconds(record.get("timestamp")) if isinstance(record, dict) else None
                heapq.heappush(pending, (when or 0, name, record, stream, inode))
                return
            except (OSError, UnicodeError) as exc:
                self.errors.add(name + ": unreadable record: " + str(exc))
                self.cursors[name] = (inode, start)
                return
            except (ValueError, TypeError) as exc:
                if len(self.errors) < 100:
                    self.errors.add(name + ": malformed JSON record: " + str(exc))

    def summary(self):
        self.refresh()
        # Preview the uncommitted tail without making its peaks permanent. A
        # later append from a slow node may change its chronological alignment.
        pending = [entry for entries in self.pending_memory.values() for entry in entries]
        concurrent_peak, aligned_samples = self.align_memory(
            pending, dict(self.latest_memory), self.concurrent_memory_peak, self.aligned_memory_samples)
        completion = None
        try:
            completion = read_json(self.directory / "completion.json")
            if (completion.get("job_id") != self.metadata["job_id"]
                    or completion.get("run_id") != self.metadata["run_id"]
                    or type(completion.get("workload_exit_code")) is not int):
                completion = None
                raise JobmonError("invalid completion identity or exit status")
        except FileNotFoundError:
            pass
        except (OSError, JobmonError, ValueError) as exc:
            self.errors.add("completion.json: " + str(exc))
        expected = self.metadata["expected_nodes"]
        missing = sorted(set(expected) - set(self.nodes))
        latest = max((node["last"] for node in self.nodes.values()), default=None)
        gpus = []
        for source in self.gpus.values():
            gpu = dict(source)
            weighted = gpu.pop("weighted_utilization")
            covered = gpu["covered_device_seconds"]
            gpu["device_utilization_percent"] = weighted / covered if covered else None
            gpu["job_utilization_percent"] = (gpu["device_utilization_percent"]
                                              if gpu["scope"] == "exclusive_device_user_verified" else None)
            gpu["unavailable_fields"] = sorted(gpu["unavailable_fields"])
            gpus.append(gpu)
        peaks = [state["memory_peak_bytes"] for state in self.nodes.values() if state["memory_peak_bytes"] is not None]
        kernel_peaks = [state["cgroup_memory_peak_bytes"] for state in self.nodes.values()
                        if state["cgroup_memory_peak_bytes"] is not None]
        observed = sorted({state["scope"] for state in self.nodes.values()
                           if state["scope"] in MEMORY_SCOPE_NOTES})
        memory_scope = "; ".join(MEMORY_SCOPE_NOTES[scope] for scope in observed) or (
            "no readable memory scope; see collection diagnostics")
        unavailable_metrics = {}
        if not self.cpu_denominator:
            unavailable_metrics["cpu_percent_covered_intervals"] = (
                "no valid CPU deltas with allocated-core denominators")
        if concurrent_peak is None:
            unavailable_metrics["concurrent_memory_peak_bytes"] = (
                "complete expected-node layout unavailable" if not self.node_layout_complete
                else "no aligned readable memory samples from every expected node")
        if not kernel_peaks and observed and all(scope == PROCESS_TREE_SCOPE for scope in observed):
            unavailable_metrics["cgroup_memory_peak_bytes"] = (
                "process-tree sampling has no kernel high-water mark; that needs the job cgroup's memory.peak")
        elif not kernel_peaks:
            unavailable_metrics["cgroup_memory_peak_bytes"] = (
                "no readable cgroup memory.peak; kernel high-water marks need cgroup v2 with memory.peak (Linux 5.19+)")
        if not gpus:
            unavailable_metrics["gpus"] = ("no GPU samples; collection is off unless JOBMON_GPU_UUIDS is set, "
                                           "otherwise see collection diagnostics")
        return {"run_id": self.metadata["run_id"], "metadata": self.metadata, "nodes": self.nodes,
                "latest_sample_timestamp": (dt.datetime.fromtimestamp(latest, dt.timezone.utc).isoformat()
                                            if latest else None),
                "sample_age_seconds": max(0, time.time() - latest) if latest else None,
                "expected_nodes": expected, "missing_nodes": missing,
                "missing_node_count": max(
                    0, (self.metadata.get("expected_node_count") or len(expected)) - len(self.nodes)),
                "cpu_percent_covered_intervals": (100 * self.cpu_numerator / self.cpu_denominator
                                                  if self.cpu_denominator else None),
                "cpu_covered_core_seconds": self.cpu_denominator,
                "cpu_missing_denominator_nodes": sorted(self.cpu_missing_denominator_nodes),
                "concurrent_memory_peak_bytes": concurrent_peak,
                "aligned_memory_sample_count": aligned_samples,
                "concurrent_memory_alignment_incomplete": self.memory_alignment_incomplete,
                "concurrent_memory_alignment_buffer_samples_per_node": MEMORY_ALIGNMENT_BUFFER_SAMPLES,
                "sum_of_independent_node_memory_peaks_bytes": sum(peaks) if peaks else None,
                "sum_of_independent_node_cgroup_memory_peaks_bytes": sum(kernel_peaks) if kernel_peaks else None,
                "memory_scope": memory_scope,
                "cgroup_memory_peak_scope": "kernel memory.peak high-water mark per node; exact rather than sampled, "
                                            "lifetime-cumulative, and never concurrent across nodes",
                "gpus": gpus, "completion": completion,
                "all_expected_collectors_final": (self.node_layout_complete and not missing
                                                  and all(n["final"] for n in self.nodes.values())),
                "coverage": "sampled intervals only; no extrapolation to the full job"
                            + ("; GPU averages use covered device-time" if gpus else "")
                            + ("; concurrent memory alignment incomplete" if self.memory_alignment_incomplete else ""),
                "unavailable_metrics": unavailable_metrics,
                "partial_final_lines": sorted(self.partial_lines), "errors": sorted(self.errors),
                "source": "personal_collector"}


class SavedHistory:
    def __init__(self, job_id, root=None):
        self.job_id = validate_job_id(job_id)
        self.root = Path(root or data_root())
        self.runs = {}
        self.errors = []
        self.ignored = []

    def refresh(self):
        if not self.root.exists():
            return []
        private_directory(self.root)
        directory = self.root / self.job_id
        if not directory.exists():
            return []
        private_directory(directory)
        for path in sorted(directory.iterdir()):
            if path.name in self.runs:
                continue
            # Only a directory can be a run. An unrelated entry, such as the
            # .nfs stub shared storage leaves behind, is not saved history and
            # must not block a fetch of the runs beside it. A symlinked
            # directory still reaches load_run, which rejects it.
            if not path.is_dir():
                if path.name not in self.ignored:
                    self.ignored.append(path.name)
                continue
            try:
                metadata = load_run(path, self.root, self.job_id)
                self.runs[path.name] = RunHistory(path, metadata)
            except (OSError, JobmonError, ValueError, TypeError) as exc:
                message = str(exc)
                if message not in self.errors:
                    self.errors.append(message)
        return [run.summary() for run in self.runs.values()]


def build_report(job_id, days=30, live=False, backend=None):
    scheduler = (backend or SlurmBackend()).snapshot(job_id, days, live)
    canonical = {r["job_id"] for r in scheduler["accounting"]}
    stored_id = next(iter(canonical)) if len(canonical) == 1 else job_id
    history = SavedHistory(stored_id)
    runs = history.refresh()
    if not scheduler["ownership_verified"] and not runs:
        diagnostics = scheduler["errors"] + history.errors
        # A disabled accounting database is a site limitation; widening --days cannot help.
        guidance = ("Completed jobs have no scheduler record here; use the opt-in collector for future jobs."
                    if any(accounting_disabled(entry) for entry in diagnostics)
                    else "Check the ID, --days, and storage.")
        raise JobmonError("Cannot verify an owned job or private saved run for {}. {} {}".format(
            job_id, guidance, "; ".join(diagnostics)))
    report = {"schema_version": SCHEMA_VERSION, "generated_at": utc_now(), "job_id": job_id,
              "storage_job_id": stored_id, "scheduler_snapshot_at": utc_now(), "scheduler": scheduler,
              "runs": runs, "errors": history.errors,
              "ignored_storage_entries": history.ignored,
              "ownership_source": "scheduler" if scheduler["ownership_verified"] else "private_saved_metadata",
              "history_reason": None if runs else "unavailable: no saved collector history"}
    return report, history


def display_value(value, suffix=""):
    return "unavailable" if value is None else "{:.2f}{}".format(value, suffix)


def display_scalar(value):
    return "unavailable" if value is None or value == "" else str(value)


def display_bytes(value, missing="unavailable"):
    # Binary units for reading; JSON output keeps exact byte counts.
    if value is None:
        return missing
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if value < 1024 or unit == "TiB":
            return "{:.0f} B".format(value) if unit == "B" else "{:.2f} {}".format(value, unit)
        value /= 1024


def display_cpu(percent, cores, missing="unavailable"):
    if percent is None and cores is None:
        return missing
    if percent is None:
        return "{:.1f} cores".format(cores)
    if cores is None:
        return "{:.1f}%".format(percent)
    return "{:.1f}% ({:.1f} cores)".format(percent, cores)


def average_cpu(state):
    """Covered-interval average for one node, as (percent of allocated cores, busy cores)."""
    wall = state.get("cpu_wall_seconds_covered")
    if not state.get("cpu_seconds_available") or not wall:
        return None, None
    cores = state["cpu_seconds_covered"] / wall
    return (100 * cores / state["allocated_cpus"] if state.get("allocated_cpus") else None), cores


METRIC_LABELS = {"cpu_percent_covered_intervals": "CPU percentage",
                 "concurrent_memory_peak_bytes": "concurrent memory peak",
                 "cgroup_memory_peak_bytes": "kernel memory peak", "gpus": "GPU"}


def multi_node(run):
    return ((run["metadata"].get("expected_node_count") or 0) > 1
            or len(run["expected_nodes"]) > 1 or len(run["nodes"]) > 1)


def format_report(report):
    lines = ["Job {}  |  {}".format(report["job_id"], report["generated_at"]),
             "Scheduler snapshot: " + report["scheduler_snapshot_at"]]
    scheduler = report["scheduler"]
    for queue in scheduler["queue"]:
        lines.append("Queue: {state}; elapsed {elapsed}; {cpus} CPUs, {nodes} nodes; {reason_or_nodes}".format(**queue))
    for run in scheduler["accounting"]:
        lines.append("Accounting: {} (raw {}), {}; start {}; exit {}".format(
            run["job_id"], run["job_id_raw"], display_scalar(run["state"]),
            display_scalar(run["start"]), display_scalar(run["exit_code"])))
        lines.append("  Elapsed {} s; CPUs {}; CPU consumed {} s; efficiency {}".format(
            display_scalar(run["elapsed_seconds"]), display_scalar(run["allocated_cpus"]),
            display_scalar(run["total_cpu_seconds"]), display_value(run["cpu_efficiency"]["value"], "%")))
        if run["cpu_efficiency"].get("reason"):
            lines.append("  CPU: " + run["cpu_efficiency"]["reason"])
        lines.append("  Requested memory: {} ({}); allocated memory: {}".format(
            display_bytes(run["requested_memory"]["bytes"]), run["requested_memory"]["scope"],
            display_bytes(run["allocated_memory_bytes"])))
        for step in run["step_max_rss"]:
            lines.append("  {} MaxRSS: {} (maximum task within step)".format(
                step["job_id"], display_bytes(step["bytes"])))
        lines.extend("  Note: " + caveat for caveat in run["caveats"])
    if scheduler["live_steps"]:
        lines.append("Live step accounting (AveRSS/MaxRSS retain Slurm task/step scope):")
        lines.extend("  " + json.dumps(row, sort_keys=True) for row in scheduler["live_steps"])
    if report["history_reason"]:
        lines.append(report["history_reason"])
    for run in report["runs"]:
        lines.append("Run {}: {} node(s), missing {}; latest {} (age {} s)".format(
            run["run_id"], len(run["nodes"]), run["missing_nodes"] or run["missing_node_count"],
            run["latest_sample_timestamp"] or "unavailable", display_value(run["sample_age_seconds"])))
        lines.append("  CPU {} of allocated cores over {:.0f} sampled core-seconds".format(
            display_value(run["cpu_percent_covered_intervals"], "%"), run["cpu_covered_core_seconds"]))
        # On one node the concurrent peak, the sum of node peaks, and the node's own
        # peak are the same reading; only several nodes make them differ.
        if multi_node(run):
            lines.append("  Memory across nodes: concurrent peak {}; sum of independent node peaks {}".format(
                display_bytes(run["concurrent_memory_peak_bytes"]),
                display_bytes(run["sum_of_independent_node_memory_peaks_bytes"])))
            if run["sum_of_independent_node_cgroup_memory_peaks_bytes"] is not None:
                lines.append("  Kernel high-water marks summed across nodes: {}".format(
                    display_bytes(run["sum_of_independent_node_cgroup_memory_peaks_bytes"])))
        lines.append("  Memory scope: " + run["memory_scope"])
        if any(state["cgroup_memory_peak_bytes"] is not None for state in run["nodes"].values()):
            lines.append("  Kernel high-water scope: " + run["cgroup_memory_peak_scope"])
        for node, state in sorted(run["nodes"].items()):
            lines.append("  {}: {} samples; scope {}; gaps {}{}".format(
                node, state["samples"], state["scope"], state["gaps"], "; final" if state["final"] else ""))
            lines.append("    CPU: last interval {}; average {}; busiest interval {}".format(
                display_cpu(state["latest_interval_cpu_percent"], state["latest_interval_busy_cores"]),
                display_cpu(*average_cpu(state)),
                display_cpu(state["max_interval_cpu_percent"], state["max_interval_busy_cores"])))
            memory = "    Memory: latest {}; sampled peak {}".format(
                display_bytes(state["latest_memory_bytes"]), display_bytes(state["memory_peak_bytes"]))
            if state["cgroup_memory_peak_bytes"] is not None:
                memory += "; kernel high-water {}".format(display_bytes(state["cgroup_memory_peak_bytes"]))
            lines.append(memory)
        for gpu in run["gpus"]:
            lines.append("  GPU {} on {}: device {}; job {}; {}; covered {} device-seconds".format(
                gpu["uuid"], gpu["node"], display_value(gpu["device_utilization_percent"], "%"),
                display_value(gpu["job_utilization_percent"], "%"), gpu["scope"],
                display_value(gpu["covered_device_seconds"])))
        # While the job is still queued or running, a missing completion is expected.
        if run["completion"] or not scheduler["queue"]:
            lines.append("  Completion: {}; all expected collectors final: {}".format(
                run["completion"]["workload_exit_code"] if run["completion"] else "unavailable",
                run["all_expected_collectors_final"]))
        lines.append("  Coverage: " + run["coverage"])
        if run["unavailable_metrics"]:
            lines.append("  Not measured: " + "; ".join(
                "{} ({})".format(METRIC_LABELS.get(metric, metric), reason)
                for metric, reason in run["unavailable_metrics"].items()))
        if run["partial_final_lines"]:
            lines.append("  Incomplete JSONL tails: " + ", ".join(run["partial_final_lines"]))
        lines.extend("  Collection: " + error for error in run["errors"])
    if report.get("ignored_storage_entries"):
        lines.append("Ignored non-run entries in saved storage: "
                     + ", ".join(report["ignored_storage_entries"]))
    lines.extend("Diagnostic: " + error for error in scheduler["errors"] + report["errors"])
    return "\n".join(lines)


# A node is stale once it misses this many sampling intervals, with a floor that
# absorbs shared-filesystem delays on short intervals.
STALE_FACTOR = 2.5
STALE_MARGIN_SECONDS = 30
# Gaps that hold for a whole run, so a watch states them once instead of per row.
STRUCTURAL_GAPS = ("cgroup_memory_peak_bytes", "gpus")


def clock_time(seconds, tz=None):
    # tz None means this host's local zone; a fixed offset comes from the workstation.
    return dt.datetime.fromtimestamp(seconds, tz).strftime("%H:%M:%S") if seconds else "--:--:--"


def display_zone(tz):
    now = dt.datetime.now(tz) if tz else dt.datetime.now().astimezone()
    minutes = int(now.utcoffset().total_seconds() // 60)
    offset = "UTC{}{:02d}:{:02d}".format("+" if minutes >= 0 else "-", abs(minutes) // 60, abs(minutes) % 60)
    if tz:
        return "your workstation's time, " + offset
    name = now.tzname()
    named = name and not name.startswith(("UTC", "+", "-"))
    return "the cluster's local time, " + (name + " " + offset if named else offset)


def display_elapsed(seconds):
    """Slurm's elapsed-time layout, as squeue prints it: M:SS, H:MM:SS, or D-HH:MM:SS."""
    # A second or two before the start is clock skew between hosts, not a real gap.
    if seconds is None or seconds < -60:
        return "-"
    days, rest = divmod(int(max(0, seconds)), 86400)
    hours, rest = divmod(rest, 3600)
    minutes, secs = divmod(rest, 60)
    if days:
        return "{}-{:02d}:{:02d}:{:02d}".format(days, hours, minutes, secs)
    if hours:
        return "{}:{:02d}:{:02d}".format(hours, minutes, secs)
    return "{}:{:02d}".format(minutes, secs)


def run_start(report, run):
    """Where a run's ELAPSED column counts from: ("slurm" or "monitoring", epoch seconds)."""
    created = timestamp_seconds(run["metadata"].get("created_at"))
    newest = max((timestamp_seconds(other["metadata"].get("created_at")) or 0 for other in report["runs"]),
                 default=0)
    snapshot = timestamp_seconds(report["scheduler_snapshot_at"])
    if created is not None and created >= newest and snapshot is not None:
        for queue in report["scheduler"]["queue"]:
            elapsed = cpu_seconds(queue.get("elapsed"))
            # Slurm's start is authoritative for the running allocation. A start after this
            # run began belongs to a later allocation of a requeued job, so it is not used.
            if (queue.get("state") in ("RUNNING", "COMPLETING") and elapsed is not None
                    and snapshot - elapsed <= created + 60):
                return "slurm", snapshot - elapsed
    return ("monitoring", created) if created is not None else (None, None)


def cpu_cell(percent, cores):
    if percent is not None:
        return "{:.1f}%".format(percent)
    return "-" if cores is None else "{:.1f} cores".format(cores)


def watch_header(report, tz):
    scheduler = report["scheduler"]
    at = clock_time(timestamp_seconds(report["scheduler_snapshot_at"]), tz)
    lines = ["Job {}: watching saved samples; CLOCK is {}; Ctrl-C stops viewing".format(
        report["job_id"], display_zone(tz))]
    # Watch reads Slurm once, so say when, rather than repeat a frozen elapsed time.
    for queue in scheduler["queue"]:
        lines.append("Scheduler at {}: {state}, elapsed {elapsed}, {cpus} CPUs on {nodes} node(s), "
                     "{reason_or_nodes}; not refreshed while watching".format(at, **queue))
    for run in scheduler["accounting"]:
        lines.append("Accounting at {}: {}, exit {}; not refreshed while watching".format(
            at, display_scalar(run["state"]), display_scalar(run["exit_code"])))
    if not scheduler["queue"] and not scheduler["accounting"]:
        lines.append("Scheduler at {}: no queue or accounting record".format(at))
    if report.get("ignored_storage_entries"):
        lines.append("Ignored non-run entries in saved storage: "
                     + ", ".join(report["ignored_storage_entries"]))
    lines.extend("Diagnostic: " + error for error in scheduler["errors"] + report["errors"])
    return lines


class WatchLayout:
    """One run's table: column widths and the origin of its ELAPSED column."""

    def __init__(self, report, run, tz):
        self.multi = multi_node(run)
        self.source, self.start = run_start(report, run)
        intervals = sorted({state["interval_seconds"] for state in run["nodes"].values()})
        self.cpu_label = "CPU LAST {:g} s".format(intervals[0]) if len(intervals) == 1 else "CPU LAST"
        self.cpu_width = max(9, len(self.cpu_label))
        self.tz = tz

    def columns(self, clock, node, values, suffix=""):
        prefix = "{:<8}  {:<12}  ".format(clock, node) if self.multi else "{:<8}  ".format(clock)
        body = "{:>10}  {:>7}  {:>{width}}  {:>9}  {:>9}  {:>11}  {:>11}".format(*values, width=self.cpu_width)
        return (prefix + body + suffix).rstrip()

    def header(self, run):
        cpus = run["metadata"]["allocated_cpus_by_node"]
        nodes = ", ".join("{} ({} CPUs)".format(node, display_scalar(cpus.get(node)))
                          for node in sorted(set(run["expected_nodes"]) | set(run["nodes"]))) or "no nodes yet"
        intervals = sorted({state["interval_seconds"] for state in run["nodes"].values()})
        scopes = sorted({state["scope"] for state in run["nodes"].values() if state["scope"]})
        lines = ["", "Run {}: {}; sampled every {}; scope {}".format(
            run["run_id"], nodes, ", ".join("{:g} s".format(value) for value in intervals) or "unknown",
            ", ".join(scopes) or "unknown")]
        if run["missing_nodes"]:
            lines.append("  Missing nodes: " + ", ".join(run["missing_nodes"]))
        lines.append("  Memory: " + run["memory_scope"])
        lines.append("  Coverage: " + run["coverage"])
        gaps = [(METRIC_LABELS[metric], run["unavailable_metrics"][metric])
                for metric in STRUCTURAL_GAPS if metric in run["unavailable_metrics"]]
        if gaps:
            lines.append("  Not measured: " + "; ".join("{} ({})".format(label, reason) for label, reason in gaps))
        lines.extend("  Collection: " + error for error in run["errors"])
        if self.source == "slurm":
            lines.append("  ELAPSED: job run time from Slurm, as in squeue's TIME column")
        elif self.source == "monitoring":
            lines.append("  ELAPSED: time since monitoring started at {}; Slurm has no running record "
                         "for this run".format(clock_time(self.start, self.tz)))
        lines.append("  CLOCK is when each sample was taken. CPU is % of allocated cores; MAX columns are the "
                     "highest interval and sampled memory so far")
        lines.append("")
        lines.append(self.columns("CLOCK", "NODE", ("ELAPSED", "SAMPLES", self.cpu_label, "CPU AVG", "CPU MAX",
                                                    "MEMORY", "MEMORY MAX")))
        return lines

    def row(self, node, state):
        elapsed = display_elapsed(state["last"] - self.start) if self.start is not None else "-"
        average_percent, average_cores = average_cpu(state)
        return self.columns(clock_time(state["last"], self.tz), node, (
            elapsed, state["samples"],
            cpu_cell(state["latest_interval_cpu_percent"], state["latest_interval_busy_cores"]),
            cpu_cell(average_percent, average_cores),
            cpu_cell(state["max_interval_cpu_percent"], state["max_interval_busy_cores"]),
            display_bytes(state["latest_memory_bytes"], "-"),
            display_bytes(state["memory_peak_bytes"], "-")), "  final" if state["final"] else "")


class WatchView:
    """Text watch: state fixed context once, then a row only when a sample arrives."""

    def __init__(self, tz=None):
        self.tz = tz
        self.started = False
        self.runs = {}
        self.shown = {}
        self.stale = set()
        self.errors = set()
        self.completed = set()

    def update(self, report):
        lines = []
        if not self.started:
            self.started = True
            lines.extend(watch_header(report, self.tz))
            self.errors.update(report["scheduler"]["errors"] + report["errors"])
        now = time.time()
        stamp = clock_time(now, self.tz)
        for run in report["runs"]:
            run_id = run["run_id"]
            if run_id not in self.runs:
                self.runs[run_id] = WatchLayout(report, run, self.tz)
                lines.extend(self.runs[run_id].header(run))
                self.errors.update(run["errors"])
            layout = self.runs[run_id]
            for node, state in sorted(run["nodes"].items()):
                key = (run_id, node)
                if state["samples"] > self.shown.get(key, 0):
                    self.shown[key] = state["samples"]
                    self.stale.discard(key)
                    lines.append(layout.row(node, state))
                    continue
                interval = state["interval_seconds"]
                age = now - state["last"]
                if (not state["final"] and key not in self.stale
                        and age > max(STALE_FACTOR * interval, interval + STALE_MARGIN_SECONDS)):
                    self.stale.add(key)
                    lines.append("{}  {}no new sample for {:.0f} s (expected every {:g} s)".format(
                        stamp, node + ": " if layout.multi else "", age, interval))
            for error in run["errors"]:
                if error not in self.errors:
                    self.errors.add(error)
                    lines.append("{}  collection: {}".format(stamp, error))
            if run["completion"] and run_id not in self.completed:
                self.completed.add(run_id)
                lines.append("{}  workload exit {} (batch wrapper); all expected collectors final: {}".format(
                    stamp, run["completion"]["workload_exit_code"], run["all_expected_collectors_final"]))
        for error in report["errors"]:
            if error not in self.errors:
                self.errors.add(error)
                lines.append("{}  diagnostic: {}".format(stamp, error))
        return lines


def emit(value, as_json=False):
    if as_json:
        print(json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False), flush=True)
    elif "scheduler" in value:
        print(format_report(value), flush=True)
    else:
        print(json.dumps(value, indent=2, sort_keys=True), flush=True)


def export_history(job_id):
    # Validate private ownership before emitting even the archive header.
    history = SavedHistory(job_id)
    history.refresh()
    if not history.runs or history.errors:
        raise JobmonError("No verified saved history to fetch. " + "; ".join(history.errors))
    with tarfile.open(fileobj=sys.stdout.buffer, mode="w|gz") as archive:
        top = tarfile.TarInfo(job_id)
        top.type, top.mode = tarfile.DIRTYPE, 0o700
        archive.addfile(top)
        for run_id, run in sorted(history.runs.items()):
            folder = tarfile.TarInfo(job_id + "/" + run_id)
            folder.type, folder.mode = tarfile.DIRTYPE, 0o700
            archive.addfile(folder)
            for path in sorted(run.directory.iterdir()):
                if path.name not in ("metadata.json", "completion.json", "stop.json") and path.suffix != ".jsonl":
                    continue
                with private_open(path, os.O_RDONLY) as stream:
                    # Snapshot size: appends during fetch are excluded, a partial final line is OK.
                    info = tarfile.TarInfo("{}/{}/{}".format(job_id, run_id, path.name))
                    info.size = os.fstat(stream.fileno()).st_size
                    info.mode = 0o600
                    archive.addfile(info, stream.buffer)


def execute(operation, job_id=None, as_json=False, interval=5, count=None, days=30, utc_offset_minutes=None):
    if operation == "storage-init":
        emit({"schema_version": SCHEMA_VERSION,
              "storage_probe": initialize_storage_probe()}, as_json)
        return
    if operation == "doctor":
        result = SlurmBackend().doctor()
        wrapper = Path.home() / ".local/bin/monitored-job.sh"
        wrapper_exists = wrapper.is_file()
        wrapper_executable = wrapper_exists and os.access(str(wrapper), os.X_OK)
        result["batch_wrapper"] = {"path": str(wrapper), "exists": wrapper_exists,
                                   "executable": wrapper_executable}
        if not wrapper_executable:
            result["errors"].append(
                "Batch wrapper is missing or not executable: {}. "
                "Run hpc-jdeploy --apply from the workstation.".format(wrapper))
        root = data_root()
        result.update({"python_version": sys.version.split()[0], "data_directory": str(root),
                       "storage_exists": root.exists(), "storage_readable": os.access(str(root), os.R_OK),
                       "storage_shared_between_nodes": ("unverified until the login probe is seen "
                                                        "from an allocation node"),
                       "allocation": bool(os.environ.get("SLURM_JOB_ID")),
                       "collection": ("cgroup v2 requires a verified job ancestor; "
                                      "GPU collection requires verified UUIDs")})
        if root.exists():
            try:
                private_directory(root)
                result["storage_probe"] = verify_storage_probe(root)
                result["storage_shared_between_nodes"] = (
                    "login probe present; collector initialization verifies allocation-node visibility")
            except JobmonError as exc:
                result["storage_probe"] = {"verified": False, "reason": str(exc)}
                result["errors"].append(str(exc))
        else:
            result["storage_probe"] = {
                "verified": False, "reason": "storage directory is absent; run hpc-jstorage-init"}
        if os.environ.get("SLURM_JOB_ID"):
            from collect import allocation_job_id, discover_cgroup
            _, raw = allocation_job_id()
            cgroup, reason = discover_cgroup(raw)
            result["cgroup"] = {"path": str(cgroup) if cgroup else None, "reason": reason}
        emit(result, as_json)
        return
    if operation == "jobs":
        rows = SlurmBackend().jobs()
        if as_json:
            emit({"schema_version": SCHEMA_VERSION, "jobs": rows}, True)
        else:
            print("JOB ID\tSTATE\tELAPSED\tCPUS\tNODES\tREASON / NODELIST")
            for row in rows:
                print("{job_id}\t{state}\t{elapsed}\t{cpus}\t{nodes}\t{reason_or_nodes}".format(**row))
        return
    validate_job_id(job_id)
    if operation == "fetch":
        export_history(job_id)
        return
    report, history = build_report(job_id, days, live=operation == "show")
    if operation != "watch":
        emit(report, as_json)
        return
    if not report["runs"]:
        emit(report, as_json)
        raise JobmonError("No collector history to watch. Use show/report for an on-demand scheduler snapshot.")
    # The workstation helper sends its UTC offset; direct use keeps this host's zone.
    zone = None if utc_offset_minutes is None else dt.timezone(dt.timedelta(minutes=utc_offset_minutes))
    view = None if as_json else WatchView(zone)
    displayed = 0
    while True:
        if view is None:
            emit(report, True)
        else:
            lines = view.update(report)
            if lines:
                print("\n".join(lines), flush=True)
        # --count bounds refreshes, not new samples, so it ends even if collection stops.
        displayed += 1
        if count is not None and displayed >= count:
            return
        time.sleep(interval)
        # This loop performs filesystem reads only. A new report explicitly refreshes Slurm.
        report["generated_at"] = utc_now()
        report["runs"] = history.refresh()


def transport_request(stream):
    data = stream.read(8193)
    if len(data) > 8192:
        raise JobmonError("Transport request exceeds 8192 bytes.")
    try:
        request = json.loads(data)
    except ValueError as exc:
        raise JobmonError("Transport expects one JSON object on stdin.") from exc
    if not isinstance(request, dict):
        raise JobmonError("Transport expects a JSON object.")
    operation = request.get("operation")
    allowed = {
        "doctor": {"operation", "json"},
        "storage-init": {"operation", "json"},
        "jobs": {"operation", "json"},
        "show": {"operation", "job_id", "json", "days"},
        "watch": {"operation", "job_id", "json", "days", "interval", "count", "utc_offset_minutes"},
        "report": {"operation", "job_id", "json", "days"},
        "fetch": {"operation", "job_id"},
    }
    if operation not in allowed:
        raise JobmonError("Unsupported transport operation.")
    unsupported = set(request) - allowed[operation]
    if unsupported:
        raise JobmonError("Unsupported fields for {}: {}.".format(operation, ", ".join(sorted(unsupported))))
    if operation in ("show", "watch", "report", "fetch"):
        validate_job_id(request.get("job_id"))
    if "json" in request and not isinstance(request["json"], bool):
        raise JobmonError("json must be a boolean.")
    for name, low, high in (("interval", 1, 86400), ("count", 1, 1000000), ("days", 1, 3650),
                            ("utc_offset_minutes", -1439, 1439)):
        if name in request and (type(request[name]) is not int or not low <= request[name] <= high):
            raise JobmonError("{} must be an integer between {} and {}.".format(name, low, high))
    request["as_json"] = request.pop("json", False)
    return request


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    subs = parser.add_subparsers(dest="operation", required=True)
    subs.add_parser("_transport", help="private JSON-over-SSH entry point")
    for name in ("doctor", "storage-init", "jobs", "show", "watch", "report"):
        command = subs.add_parser(name)
        command.add_argument("--json", dest="as_json", action="store_true")
        if name in ("show", "watch", "report"):
            command.add_argument("job_id")
            command.add_argument("--days", type=int, default=30)
        if name == "watch":
            command.add_argument("--interval", type=int, default=5)
            command.add_argument("--count", type=int, help="stop after this many snapshots")
    args = vars(parser.parse_args(argv))
    try:
        if args["operation"] == "_transport":
            args = transport_request(sys.stdin)
        for key, low, high in (("interval", 1, 86400), ("count", 1, 1000000), ("days", 1, 3650)):
            if args.get(key) is not None and not low <= args[key] <= high:
                raise JobmonError("{} must be between {} and {}.".format(key, low, high))
        execute(**args)
    except BrokenPipeError:
        return 0
    except KeyboardInterrupt:
        return 130
    except (JobmonError, OSError, ValueError) as exc:
        print("jobmon: " + str(exc), file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
