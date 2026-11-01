#!/usr/bin/env python3
"""Standalone HTML plots from private jobmon archives."""

import argparse
import datetime as dt
import json
import math
import os
from pathlib import Path
import sys
import tarfile
import tempfile
import unicodedata
import xml.etree.ElementTree as ET
import zipfile

from collect import SCHEMA_VERSION, checked_name, private_directory, private_open
from jobmon import (MAX_LINE, MEMORY_SCOPE_NOTES, RunHistory, display_elapsed, run_start,
                    sample_point, timestamp_seconds)
from slurm_backend import JobmonError, validate_job_id

MAX_ARCHIVE_BYTES = 256 * 1024 * 1024
MAX_MEMBERS = 10000
CHART_WINDOW_MS = 24 * 3600 * 1000
EXPORT_SPEC = {
    "series": {
        "cpu": [["cpu", "Last interval", "#7aa2f7"], ["average", "Average", "#ff9e64"],
                ["maximum", "Maximum", "#9ece6a"]],
        "memory": [["memory", "Current", "#7aa2f7"], ["memory_peak", "Sampled maximum", "#ff9e64"]]},
    "columns": ["timestamp_utc", "elapsed_seconds", "cpu_percent", "cpu_average_percent", "cpu_maximum_percent",
                "memory_gib", "memory_peak_gib", "sample_count", "final", "gap", "scope"],
    "units": {"time": "Unix milliseconds (UTC)", "elapsed_seconds": "seconds since first collected sample",
              "cpu": "percent of allocated cores", "average": "percent of allocated cores",
              "maximum": "percent of allocated cores", "memory": "GiB", "memory_peak": "GiB",
              "raw_sample_memory": "bytes"}}


def filename_part(value, max_bytes):
    normalized = unicodedata.normalize("NFKC", value or "")
    safe, separator = [], False
    for character in normalized:
        if unicodedata.category(character)[0] in "LMN" or character in "_.-":
            safe.append(character)
            separator = False
        elif not separator:
            safe.append("-")
            separator = True
    name = "".join(safe).strip("._-")
    name = name.encode("utf-8")[:max_bytes].decode("utf-8", errors="ignore").rstrip("._-")
    return name or "unnamed"


def iso_time(milliseconds):
    seconds, rest = divmod(int(milliseconds), 1000)
    value = dt.datetime.fromtimestamp(seconds, dt.timezone.utc).replace(microsecond=rest * 1000)
    return value.isoformat(timespec="milliseconds").replace("+00:00", "Z")


def csv_cell(value):
    if value is None:
        return ""
    if isinstance(value, bool):
        text = "true" if value else "false"
    elif isinstance(value, float) and value.is_integer():
        text = str(int(value))
    else:
        text = str(value)
    return '"' + text.replace('"', '""') + '"'


def svg_chart(chart, points, axis="elapsed"):
    """SVG renderer using the browser's series."""
    root = ET.Element("svg", {"xmlns": "http://www.w3.org/2000/svg", "id": chart, "width": "1000", "height": "280",
                              "viewBox": "0 0 1000 280", "role": "img",
                              "aria-label": "CPU history" if chart == "cpu" else "Memory history"})

    def add(tag, attributes, text=None):
        child = ET.SubElement(root, tag, {key: str(value) for key, value in attributes.items()})
        if text is not None:
            child.text = str(text)
        return child

    add("rect", {"width": 1000, "height": 280, "fill": "#1d202f"})
    add("style", {}, "text{font:11px ui-monospace,monospace;fill:#a9b1d6}")
    specs = EXPORT_SPEC["series"][chart]
    if not points:
        add("text", {"x": 500, "y": 120, "text-anchor": "middle"}, "Waiting for saved samples")
        return ET.tostring(root, encoding="unicode")
    first, last = points[0]["time"], points[-1]["time"]
    duration = max(1000, last - first)
    maximum = max([100 if chart == "cpu" else 0]
                  + [point[key] for point in points for key, _, _ in specs if point[key] is not None])
    maximum = 100 if chart == "cpu" and maximum == 100 else maximum * 1.1 if maximum > 0 else 1

    def x(when):
        return 74 + (when - first) / duration * 896

    def y(value):
        return 236 - value / maximum * 220

    for tick in range(5):
        value, px = maximum * tick / 4, 74 + 896 * tick / 4
        py = y(value)
        add("line", {"x1": 74, "x2": 970, "y1": py, "y2": py, "stroke": "#343a55"})
        label = csv_cell(float("{:.3g}".format(value))).strip('"') + ("%" if chart == "cpu" else "")
        add("text", {"x": 64, "y": py + 4, "text-anchor": "end"}, label)
        stamp = first + (last - first) * tick / 4
        if axis == "elapsed":
            seconds = int(max(0, (stamp - first) / 1000) + 0.5)
            label = "{}:{:02d}:{:02d}".format(seconds // 3600, seconds // 60 % 60, seconds % 60)
        else:
            local = dt.datetime.fromtimestamp(int(stamp) / 1000).astimezone()
            label = local.strftime("%H:%M:%S" if last - first < 86400000 else "%Y-%m-%d %H:%M:%S")
        add("text", {"x": px, "y": 264, "text-anchor": "start" if tick == 0 else "end" if tick == 4 else "middle"},
            label)
    if chart == "cpu":
        add("line", {"x1": 74, "x2": 970, "y1": y(100), "y2": y(100), "stroke": "#737aa2",
                     "stroke-dasharray": "5 4", "data-reference": "100-percent"})
        if maximum > 100:
            add("text", {"x": 964, "y": max(30, y(100) - 6), "text-anchor": "end"}, "100%")
    for key, _, color in specs:
        path, pen = [], False
        for point in points:
            if point["gap"] or point[key] is None:
                pen = False
            if point[key] is None:
                continue
            path.append(("L" if pen else "M") + "{:.2f},{:.2f}".format(x(point["time"]), y(point[key]))
                        + ("" if pen else "l0,0"))
            pen = True
        add("path", {"d": "".join(path), "fill": "none", "stroke": color,
                     "stroke-width": 2, "stroke-linecap": "round"})
    if not any(point[key] is not None for point in points for key, _, _ in specs):
        add("text", {"x": 500, "y": 120, "text-anchor": "middle"}, "No available measurements")
    return ET.tostring(root, encoding="unicode")


def export_files(data, run, node, chart, captured=None, axis="elapsed", timezone_name=None):
    validate_job_id(data["job_id"])
    if not node["points"]:
        raise JobmonError("No saved samples to export.")
    captured = captured or dt.datetime.now(dt.timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")
    points = [{key: value for key, value in point.items() if key != "raw_sample"} for point in node["points"]]
    raw = [point["raw_sample"] for point in node["points"] if isinstance(point.get("raw_sample"), str)]
    snapshot = {"schema_version": 1, "exported_at": captured, "job_id": data["job_id"], "job_name": run.get("job_name"),
                "run_id": run["run_id"], "node": node["node"], "chart": chart, "time_axis": axis,
                "timezone": timezone_name or dt.datetime.now().astimezone().strftime("%Z (UTC%z)"),
                "run_metadata": run.get("metadata"), "workload_exit_code": run["exit_code"],
                "diagnostics": data["diagnostics"], "scope_notes": data["scope_notes"], "units": EXPORT_SPEC["units"],
                "raw_sample_count": len(raw), "raw_samples_complete": len(raw) == len(points), "points": points}
    rows = [",".join(EXPORT_SPEC["columns"])]
    for point in points:
        scope = point["scope"] if point["scope"] in data["scope_notes"] else "unavailable"
        rows.append(",".join(csv_cell(value) for value in (
            iso_time(point["time"]), (point["time"] - points[0]["time"]) / 1000, point["cpu"], point["average"],
            point["maximum"], point["memory"], point["memory_peak"], point["samples"], point["final"],
            point["gap"], scope)))
    files = [("chart.svg", svg_chart(chart, points, axis)), ("data.csv", "\r\n".join(rows) + "\r\n"),
             ("data.json", json.dumps(snapshot, ensure_ascii=False, allow_nan=False, indent=2) + "\n")]
    if raw:
        files.append(("samples.jsonl", "\n".join(raw) + "\n"))
    stamp = captured.replace("-", "").replace(":", "").replace(".", "")
    filename = "jobmon-{}-{}-{}-{}-{}-{}.zip".format(
        data["job_id"], filename_part(run.get("job_name"), 64), filename_part(run["run_id"], 48),
        filename_part(node["node"], 32), chart, stamp)
    return filename, files


def write_exports(data, destination, selections=None, axis="elapsed"):
    validate_job_id(data["job_id"])
    if selections is None:
        selections = [(run, node) for run in data["runs"] for node in run["nodes"] if node["points"]]
    if not selections:
        raise JobmonError("No saved samples to export; the job must have used the opt-in collector.")
    directory = private_directory(destination, create=True)
    outputs = []
    for run, node in selections:
        for chart in EXPORT_SPEC["series"]:
            filename, files = export_files(data, run, node, chart, axis=axis)
            path = directory / filename
            fd, temporary = tempfile.mkstemp(prefix=".jobmon-", suffix=".zip.partial", dir=str(directory))
            try:
                with os.fdopen(fd, "wb") as stream, \
                        zipfile.ZipFile(stream, "w", compression=zipfile.ZIP_STORED) as bundle:
                    for name, contents in files:
                        info = zipfile.ZipInfo(name, dt.datetime.now().timetuple()[:6])
                        info.create_system = 3
                        info.external_attr = 0o100600 << 16
                        bundle.writestr(info, contents.encode("utf-8"))
                os.link(temporary, str(path))
            finally:
                os.unlink(temporary)
            outputs.append(path)
    return outputs


def warning(errors, message):
    if len(errors) < 100:
        errors.add(str(message)[:500])


def metadata_for_plot(value, job_id, run_id):
    # Check local ownership; preserve cluster UID/user.
    if (not isinstance(value, dict) or value.get("schema_version") != SCHEMA_VERSION
            or value.get("job_id") != job_id or value.get("run_id") != run_id):
        raise JobmonError("Archive metadata does not match the requested job/run.")
    validate_job_id(value.get("job_id_raw"))
    nodes = value.get("expected_nodes")
    cpus = value.get("allocated_cpus_by_node")
    if (not isinstance(nodes, list) or not isinstance(cpus, dict)
            or not isinstance(value.get("errors", []), list)
            or any(not isinstance(error, str) for error in value.get("errors", []))):
        raise JobmonError("Malformed allocation metadata in archive.")
    for node in nodes:
        checked_name(node)
    for node, count in cpus.items():
        checked_name(node)
        if type(count) is not int or count < 1:
            raise JobmonError("Invalid CPU allocation in archive metadata.")
    count = value.get("expected_node_count")
    if (len(nodes) != len(set(nodes))
            or (count is not None and (type(count) is not int or count < max(1, len(nodes))))
            or (value.get("job_name") is not None and not isinstance(value["job_name"], str))):
        raise JobmonError("Invalid node layout or job name in archive metadata.")
    return value


def read_archive(job_id, path):
    validate_job_id(job_id)
    runs, seen, errors = {}, set(), set()
    total = 0
    # Read members without extracting.
    with private_open(path, os.O_RDONLY) as local, tarfile.open(fileobj=local.buffer, mode="r|gz") as archive:
        for index, member in enumerate(archive):
            total += member.size
            if member.size < 0 or index >= MAX_MEMBERS or total > MAX_ARCHIVE_BYTES:
                raise JobmonError("Archive exceeds the plot limit (256 MiB or 10000 members).")
            parts = member.name.rstrip("/").split("/")
            if (not parts or parts[0] != job_id or len(parts) > 3
                    or any(part in ("", ".", "..") for part in parts)):
                raise JobmonError("Unexpected archive path: " + member.name)
            if len(parts) > 1:
                checked_name(parts[1])
            if member.name in seen:
                raise JobmonError("Duplicate archive entry: " + member.name)
            seen.add(member.name)
            if member.isdir() and len(parts) <= 2:
                continue
            if not member.isfile() or len(parts) != 3:
                raise JobmonError("Only regular job-history files are accepted: " + member.name)
            filename = parts[2]
            if filename not in ("metadata.json", "completion.json", "stop.json") and not filename.endswith(".jsonl"):
                raise JobmonError("Unexpected history file: " + member.name)
            run = runs.setdefault(parts[1], {"records": [], "metadata": None, "completion": None})
            with archive.extractfile(member) as stream:
                if not filename.endswith(".jsonl"):
                    if member.size > MAX_LINE:
                        raise JobmonError("Oversized history metadata: " + member.name)
                    if filename == "stop.json":
                        stream.read()
                        continue
                    try:
                        run[filename.split(".")[0]] = json.load(stream)
                    except (ValueError, UnicodeError) as exc:
                        if filename == "metadata.json":
                            raise JobmonError("Unreadable run metadata: " + member.name) from exc
                        warning(errors, member.name + ": unreadable completion record")
                    continue
                line_number = 0
                while True:
                    line = stream.readline(MAX_LINE + 1)
                    if not line:
                        break
                    line_number += 1
                    if len(line) > MAX_LINE:
                        raise JobmonError("Sample line exceeds 1 MiB: " + member.name)
                    if not line.endswith(b"\n"):
                        warning(errors, member.name + ": unfinished final line skipped; fetch again for updates")
                        break
                    try:
                        record = json.loads(line)
                        when = timestamp_seconds(record.get("timestamp")) if isinstance(record, dict) else None
                        if when is None:
                            raise ValueError("missing or invalid timestamp")
                        run["records"].append((when, filename, record))
                    except (ValueError, UnicodeError) as exc:
                        warning(errors, "{}:{}: {}".format(member.name, line_number, exc))
    return plot_data(job_id, runs, errors)


def plot_data(job_id, runs, errors):
    result = []
    for run_id, saved in sorted(runs.items()):
        metadata = metadata_for_plot(saved["metadata"], job_id, run_id)
        history = RunHistory(Path(run_id), metadata)
        nodes, previous = {}, {}
        for when, filename, record in sorted(saved["records"], key=lambda row: (row[0], row[1])):
            try:
                history.consume(record, filename)
            except (ValueError, TypeError, OverflowError) as exc:
                warning(errors, run_id + "/" + filename + ": " + str(exc))
                continue
            node = record["node"]
            state = history.nodes[node]
            group = nodes.setdefault(node, {"node": node, "scopes": [], "points": []})
            scope = record["metrics"]["scope"]
            if scope not in group["scopes"]:
                group["scopes"].append(scope)
            old = previous.get(node)
            gap = (old is None or old[0] != record["stream_id"] or old[2] != scope
                   or when - old[1] > state["interval_seconds"] * 1.5)
            point = dict(sample_point(history, record), gap=gap)
            point["raw_sample"] = json.dumps(record, separators=(",", ":"))
            group["points"].append(point)
            previous[node] = (record["stream_id"], when, scope)
        for error in history.errors:
            warning(errors, run_id + ": " + str(error))
        completion = saved["completion"]
        if completion is not None and (
                not isinstance(completion, dict) or completion.get("job_id") != job_id
                or completion.get("run_id") != run_id or type(completion.get("workload_exit_code")) is not int):
            warning(errors, run_id + ": invalid completion record ignored")
            completion = None
        result.append({"run_id": run_id, "job_name": metadata.get("job_name"),
                       "metadata": metadata,
                       "expected_nodes": metadata["expected_nodes"],
                       "expected_node_count": metadata.get("expected_node_count"),
                       "exit_code": completion["workload_exit_code"] if completion else None,
                       "nodes": [nodes[node] for node in sorted(nodes)]})
    if not any(node["points"] for run in result for node in run["nodes"]):
        raise JobmonError("No valid saved samples to plot. " + "; ".join(sorted(errors)[:5]))
    return with_runtime({"job_id": job_id, "runs": result, "diagnostics": sorted(errors),
                         "scope_notes": MEMORY_SCOPE_NOTES})


def completion_result(job_id, run, queue_status, latest=True):
    """Match the page's completionResult evidence rules."""
    if run is not None and type(run.get("exit_code")) is int:
        code = run["exit_code"]
        detail = "Batch wrapper result for the selected run. Use hpc-jreport {} for final Slurm state.".format(job_id)
        if code == 0:
            return {"kind": "finished", "label": "finished", "message": "Workload finished (exit 0).",
                    "detail": detail}
        return {"kind": "failed", "label": "exit {}".format(code), "detail": detail,
                "message": "Workload exited with code {}. Check the job's output/error logs.".format(code)}
    if queue_status is None or queue_status["state"] is not None or not latest:
        return None
    metadata = (run or {}).get("metadata") or {}
    checked = timestamp_seconds(queue_status["checked_at"])
    created = timestamp_seconds(metadata.get("created_at"))
    if checked is None or (created is not None and created > checked):
        return None
    detail = ("Attached monitoring cannot record the workload's exit code." if metadata.get("attachment")
              else "No workload exit code was recorded by this monitor.")
    return {"kind": "unknown", "label": "left queue", "message": "Job left the Slurm queue.", "detail": detail}


def with_runtime(data):
    """Elapsed through saved samples, never wall-clock now."""
    report = dict(data, scheduler=data.get("scheduler") or {"queue": []},
                  scheduler_snapshot_at=data.get("scheduler_snapshot_at"))
    snapshot = timestamp_seconds(report["scheduler_snapshot_at"])
    if snapshot is not None:
        # Ignore runs created after this snapshot.
        report["runs"] = [run for run in data["runs"]
                          if (timestamp_seconds(run["metadata"].get("created_at")) or 0) <= snapshot + 60]
    runs = []
    for run in data["runs"]:
        source, start = run_start(report, run)
        created = timestamp_seconds(run["metadata"].get("created_at"))
        # Later requeues cannot inherit the initial start.
        if source == "slurm" and created > snapshot + 60:
            source, start = "monitoring", created
        bounds = [(node["points"][0]["time"], node["points"][-1]["time"])
                  for node in run["nodes"] if node["points"]]
        first = min((first for first, _ in bounds), default=None)
        last = max((last for _, last in bounds), default=None)
        if start is None and first is not None:
            source, start = "samples", first / 1000
        seconds = (last / 1000 - start) if last is not None and start is not None else None
        if seconds is not None:
            seconds = max(0, seconds) if math.isfinite(seconds) and seconds >= -60 else None
        runs.append(dict(run, runtime={"source": source, "seconds": seconds,
                                      "display": display_elapsed(seconds), "sampled_at": last}))
    return dict(data, runs=runs)


HTML = """<!doctype html>
<html lang="en">
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<meta http-equiv="Content-Security-Policy" content="default-src 'none'; script-src 'unsafe-inline';
style-src 'unsafe-inline'; connect-src 'self'; img-src data: blob:">
<title>Job history</title>
<style>
:root{color-scheme:dark;--bg:#292d3e;--bg-dark:#1d202f;--bg-high:#363d5c;--line:#343a55;
--fg:#c0caf5;--fg-dim:#a9b1d6;--muted:#737aa2;--edge:#414868;--focus:#2e3c64;
--blue:#7aa2f7;--cyan:#7dcfff;--magenta:#bb9af7;--orange:#ff9e64;--green:#9ece6a;--red:#f7768e}
*{box-sizing:border-box}body{font:13px/1.45 "JetBrains Mono",ui-monospace,monospace;
color:var(--fg);background:var(--bg);margin:0}
::selection{background:#3d59a1}header{display:flex;align-items:center;gap:18px;padding:12px 18px;
background:var(--bg-dark);border-bottom:1px solid var(--edge);box-shadow:inset 0 2px #29a4bd}
header h1{font-size:14px;color:var(--blue);margin:0}#title{overflow-wrap:anywhere;min-width:0}
#runtime{font-size:12px;color:var(--cyan);white-space:nowrap;font-variant-numeric:tabular-nums}
#connection{margin-left:auto;color:var(--muted);font-size:12px}#connection.live{color:var(--green)}
#job-tabs{display:flex;gap:6px;padding:8px 18px;background:var(--bg-dark);
border-bottom:1px solid var(--line);overflow-x:auto}#job-tabs[hidden]{display:none}
#job-tabs button{white-space:nowrap;flex-shrink:0;max-width:320px;overflow:hidden;text-overflow:ellipsis}
#job-tabs button[aria-selected="true"]{background:var(--focus);border-color:var(--blue);color:var(--blue)}
#job-tabs button::before{content:"";display:inline-block;width:6px;height:6px;margin-right:8px;
border-radius:50%;background:var(--muted)}#job-tabs button.live::before{background:var(--green)}
#job-tabs button.warning::before{background:var(--orange)}
#job-tabs button.unknown::before{background:var(--orange)}
#job-tabs button.finished::before{background:var(--green)}#job-tabs button.failed::before{background:var(--red)}
main{display:grid;grid-template-columns:260px minmax(0,1fr);min-height:calc(100vh - 45px)}
main.sidebar-collapsed{grid-template-columns:minmax(0,1fr)}
aside{padding:18px;background:var(--bg-dark);border-right:1px solid var(--line);overflow-wrap:anywhere}
aside h2{font-size:11px;text-transform:uppercase;letter-spacing:.1em;color:var(--magenta);margin:0 0 18px}
.charts{padding:20px;min-width:0}p{line-height:1.6}.controls{display:grid;gap:16px}
#completion{padding:12px 14px;border:1px solid var(--green);border-left-width:3px;border-radius:5px;
background:var(--bg-dark);color:var(--green);white-space:pre-wrap;margin:0 0 18px}
#completion.failed{border-color:var(--red);color:var(--red)}
#completion.unknown{border-color:var(--orange);color:var(--orange)}
label{display:grid;gap:6px;color:var(--fg-dim);font-size:12px}
select,button{font:inherit;padding:6px 8px;border:1px solid var(--edge);border-radius:5px;
background:var(--bg-high);color:var(--fg-dim);min-width:0;max-width:100%}
button{cursor:pointer}button:hover{color:var(--fg);border-color:var(--blue)}
button:focus-visible,select:focus-visible{outline:2px solid var(--cyan);outline-offset:2px}
#sidebar-toggle{display:grid;place-items:center;padding:6px;flex-shrink:0}
#sidebar-toggle svg{width:18px;height:18px}
section{background:var(--bg-dark);border:1px solid var(--line);border-radius:5px;padding:16px;margin-bottom:18px}
h2{font-size:13px;color:var(--blue);margin:0 0 12px;font-weight:600}
svg{width:100%;height:auto;display:block;touch-action:pan-y}
output{display:block;min-height:3.5em;font-size:12px;color:var(--fg-dim);white-space:pre-wrap}
small{font-size:11px;color:var(--muted)}.legend{font-size:12px;display:flex;gap:18px;flex-wrap:wrap;margin-bottom:10px}
.legend span::before{content:"";display:inline-block;width:18px;height:3px;background:var(--color);
vertical-align:middle;margin-right:6px}details{margin-top:20px}summary{cursor:pointer;color:var(--magenta)}
pre{white-space:pre-wrap;overflow-wrap:anywhere;font:11px/1.5 ui-monospace,monospace;color:var(--fg-dim)}
#scheduler{font-size:11px;color:var(--muted)}#window{font-size:12px;color:var(--fg-dim);margin:0 0 12px}
@media(max-width:760px){
main{grid-template-columns:1fr}aside{border-right:0;border-bottom:1px solid var(--line)}
.controls{grid-template-columns:repeat(3,minmax(0,1fr))}.charts{padding:12px}header{gap:12px;flex-wrap:wrap}}
</style>
<header><button id="sidebar-toggle" type="button" aria-controls="sidebar" aria-expanded="true"
aria-label="Toggle sidebar" title="Toggle sidebar">
<svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8"
aria-hidden="true" focusable="false"><rect x="3" y="3" width="18" height="18" rx="2"/>
<path d="M9 3v18"/></svg></button>
<h1 id="title">Job history</h1><span id="runtime">Elapsed unavailable</span>
<span id="connection" role="status" aria-live="polite">Connecting</span></header>
<div id="job-tabs" role="tablist" aria-label="Jobs" hidden></div><div id="job-panel">
<main><aside id="sidebar"><h2>Saved history</h2>
<div class="controls">
<label>Run<select id="run"></select></label>
<label>Node<select id="node"></select></label>
<label>Time axis<select id="axis"><option value="elapsed">Time since first sample</option>
<option value="clock">Local date/time</option></select></label>
</div>
<p id="context"></p>
<p id="scheduler"></p><small id="scope"></small>
<details><summary>Diagnostics</summary><pre id="diagnostics"></pre></details>
</aside><div class="charts">
<p id="completion" role="status" aria-live="polite" hidden></p>
<p id="window" hidden></p>
<section><h2>CPU · % of allocated cores</h2><div class="legend" id="cpu-legend"></div>
<svg id="cpu" width="1000" height="280" viewBox="0 0 1000 280" role="img" aria-label="CPU history"></svg>
<output id="cpu-reading">Hover or tap to inspect.</output>
<button data-save="cpu">Save CPU image + data</button></section>
<section><h2>Memory · GiB</h2><div class="legend" id="memory-legend"></div>
<svg id="memory" width="1000" height="280" viewBox="0 0 1000 280" role="img" aria-label="Memory history"></svg>
<output id="memory-reading">Hover or tap to inspect.</output>
<button data-save="memory">Save memory image + data</button></section>
<p id="export-status" role="status" aria-live="polite"></p>
<p><small id="notice">Saved archive snapshot. Hover or tap to inspect readings.
CPU averages cover sampled intervals only.
Memory peaks are sampled per node; independent node peaks are never a concurrent total.
Wrapper exit codes do not establish final Slurm state. All assets stay local.</small></p>
</div></main></div>
<script type="application/json" id="data">__JOBMON_DATA__</script>
<script>
"use strict";
let data=JSON.parse(document.getElementById("data").textContent);
let jobs=[], selectedJob=null, discoveryError="";
const jobViews=new Map();
const byId=id=>document.getElementById(id), ns="http://www.w3.org/2000/svg";
const exportSpec=__JOBMON_EXPORT_SPEC__;
const series=exportSpec.series;
function element(tag,attrs,text){
 const el=document.createElementNS(ns,tag);
 for(const [key,value] of Object.entries(attrs||{}))el.setAttribute(key,value);
 if(text!==undefined)el.textContent=text;
 return el;
}
function option(select,value,text){
 const el=document.createElement("option");el.value=value;el.textContent=text;select.appendChild(el);
}
function elapsedText(milliseconds){
 const seconds=Math.max(0,Math.round(milliseconds/1000));
 return Math.floor(seconds/3600)+":"+String(Math.floor(seconds/60)%60).padStart(2,"0")+":"+
        String(seconds%60).padStart(2,"0");
}
function labelTime(time,first){
 if(byId("axis").value==="clock")return new Date(time).toLocaleString();
 return elapsedText(time-first);
}
const WINDOW_MS=__JOBMON_WINDOW_MS__;
let inspect=null, windowEnd=null;
function chartSpan(points){
 const first=points[0].time,last=points[points.length-1].time;
 if(last-first<=WINDOW_MS)return [first,last];
 const end=windowEnd===null?last:Math.max(first+WINDOW_MS,Math.min(last,windowEnd));
 return [end-WINDOW_MS,end];
}
function firstAtOrAfter(points,time){
 let low=0,high=points.length;
 while(low<high){const middle=Math.floor((low+high)/2);if(points[middle].time<time)low=middle+1;else high=middle;}
 return low;
}
function nearestIndex(points,time){
 let low=Math.min(points.length-1,firstAtOrAfter(points,time));
 if(low>0 && Math.abs(points[low-1].time-time)<Math.abs(points[low].time-time))low--;
 return low;
}
function readingText(id,points,point){
 return labelTime(point.time,points[0].time)+(point.final?" · final sample":"")+"\\n"+
  series[id].map(([key,label])=>label+": "+(point[key]===null?"unavailable":point[key].toFixed(2)+
             (id==="cpu"?"%":" GiB"))).join("   ");
}
function chart(id,points,svg=byId(id),span=null){
 const specs=series[id], left=74,right=970,top=16,bottom=236;
 svg.replaceChildren();svg.onpointermove=null;svg.onpointerleave=null;
 svg.appendChild(element("rect",{width:1000,height:280,fill:"#1d202f"}));
 const style=element("style",{},"text{font:11px ui-monospace,monospace;fill:#a9b1d6}");
 svg.appendChild(style);
 if(!points.length){
  svg.appendChild(element("text",{x:500,y:120,"text-anchor":"middle"},"Waiting for saved samples"));return;
 }
 const first=points[0].time,last=points[points.length-1].time,[start,end]=span||chartSpan(points);
 const range=Math.max(1000,end-start), windowed=start>first||end<last;
 // Whole-run scale: scrolling must not rescale.
 let maximum=id==="cpu"?100:0;
 for(const point of points)for(const [key] of specs)if(point[key]!==null)maximum=Math.max(maximum,point[key]);
 maximum=id==="cpu" && maximum===100?100:maximum>0?maximum*1.1:1;
 const x=time=>left+(time-start)/range*(right-left), y=value=>bottom-value/maximum*(bottom-top);
 for(let tick=0;tick<=4;tick++){
  const value=maximum*tick/4, py=y(value), px=left+(right-left)*tick/4;
  svg.appendChild(element("line",{x1:left,x2:right,y1:py,y2:py,stroke:"#343a55"}));
  const label=Number(value.toPrecision(3))+(id==="cpu"?"%":"");
  svg.appendChild(element("text",{x:left-10,y:py+4,"text-anchor":"end"},label));
  const stamp=start+(end-start)*tick/4;
  let text=labelTime(stamp,first);
  if(byId("axis").value==="clock" && end-start<86400000)text=new Date(stamp).toLocaleTimeString();
  svg.appendChild(element("text",{x:px,y:bottom+28,"text-anchor":tick===0?"start":tick===4?"end":"middle"},text));
 }
 if(id==="cpu"){
  const py=y(100);
  svg.appendChild(element("line",{x1:left,x2:right,y1:py,y2:py,stroke:"#737aa2",
   "stroke-dasharray":"5 4","data-reference":"100-percent"}));
  if(maximum>100)svg.appendChild(element("text",{x:right-6,y:Math.max(top+14,py-6),
   "text-anchor":"end"},"100%"));
 }
 if(windowed){
  const clip=element("clipPath",{id:id+"-plot"});
  clip.appendChild(element("rect",{x:left,y:top-4,width:right-left,height:bottom-top+8}));
  svg.appendChild(clip);
 }
 const shown=windowed?points.slice(Math.max(0,firstAtOrAfter(points,start)-1),
                                   firstAtOrAfter(points,end+1)+1):points;
 for(const [key,,color] of specs){
  let path="",pen=false;
  for(const point of shown){
   if(point.gap || point[key]===null)pen=false;
   if(point[key]===null)continue;
   path+=(pen?"L":"M")+x(point.time).toFixed(2)+","+y(point[key]).toFixed(2)+(pen?"":"l0,0");
   pen=true;
  }
  svg.appendChild(element("path",{d:path,fill:"none",stroke:color,"stroke-width":2,"stroke-linecap":"round",
   ...(windowed?{"clip-path":"url(#"+id+"-plot)"}:{})}));
 }
 if(!points.some(point=>specs.some(([key])=>point[key]!==null)))
  svg.appendChild(element("text",{x:500,y:120,"text-anchor":"middle"},"No available measurements"));
 const cursor=element("line",{x1:left,x2:left,y1:top,y2:bottom,stroke:"#737aa2",
                             "stroke-dasharray":"3 3",visibility:"hidden"});
 svg.appendChild(cursor);
 const place=time=>{
  const px=Math.max(left,Math.min(right,x(time)));
  cursor.setAttribute("x1",px);cursor.setAttribute("x2",px);cursor.setAttribute("visibility","visible");
 };
 const rest=()=>{
  if(inspect===null){cursor.setAttribute("visibility","hidden");return;}
  place(inspect);byId(id+"-reading").textContent=readingText(id,points,points[nearestIndex(points,inspect)]);
 };
 if(inspect!==null)place(inspect);
 svg.onpointermove=event=>{
  if(!points.length)return;
  const relative=(event.clientX-svg.getBoundingClientRect().left)/svg.getBoundingClientRect().width*1000;
  const point=points[nearestIndex(points,start+(relative-left)/(right-left)*range)];
  place(point.time);byId(id+"-reading").textContent=readingText(id,points,point);
 };
 svg.onpointerleave=rest;
}
function windowText(points){
 if(!points.length)return "";
 const first=points[0].time,last=points[points.length-1].time;
 if(last-first<=WINDOW_MS)return "";
 const total=elapsedText(last-first), [start,end]=chartSpan(points);
 if(inspect===null)return "Showing the newest 24 hours of "+total+". h starts inspecting; Ctrl-h jumps back.";
 return "Showing "+labelTime(start,first)+" to "+labelTime(end,first)+" of "+total+
  ". h/l move one sample, H/L a step, Ctrl-h/Ctrl-l a tick. Esc returns to the newest.";
}
function clearInspect(){inspect=null;windowEnd=null;}
function pinWindow(points){
 const [start,end]=chartSpan(points);
 windowEnd=inspect<start?inspect+WINDOW_MS:inspect>end?inspect:end;
}
function moveInspect(points,direction,fraction){
 if(!points.length)return;
 if(inspect===null){
  inspect=points[points.length-1].time;
  if(!fraction){pinWindow(points);return;}
 }
 const [start,end]=chartSpan(points), index=nearestIndex(points,inspect);
 let target=index+direction;
 if(fraction){
  const landed=nearestIndex(points,inspect+direction*(end-start)*fraction);
  target=direction<0?Math.min(landed,target):Math.max(landed,target);
 }
 inspect=points[Math.min(points.length-1,Math.max(0,target))].time;
 pinWindow(points);
}
function draw(){
 const run=data.runs[Number(byId("run").value)], node=run?.nodes[Number(byId("node").value)];
 const name=run?.job_name||jobName(data);
 byId("title").textContent="Job "+data.job_id+(name?" ("+name+")":"");
 const result=completionResult(data,run), completion=byId("completion");
 completion.hidden=!result;completion.className=result?.kind||"";
 const message=result?.message||"";
 if(completion.textContent!==message)completion.textContent=message;
 const runtime=run?.runtime, labels={slurm:"Elapsed",monitoring:"Monitored",samples:"Sample span"};
 byId("runtime").textContent=runtime?.seconds!=null?labels[runtime.source]+" "+runtime.display:"Elapsed unavailable";
 const origins={slurm:"Job start inferred from the initial Slurm snapshot",
  monitoring:"Monitoring start; full job elapsed time unavailable",
  samples:"First saved sample; full job elapsed time unavailable"};
 byId("runtime").title=runtime?.seconds!=null?origins[runtime.source]+"; through latest saved sample at "+
  new Date(runtime.sampled_at).toLocaleString():"Waiting for a saved sample with a usable start time.";
 const connection=byId("connection");
 connection.textContent=data.live?(data.connection||"Connecting"):"Saved snapshot";
 connection.className=data.live && data.connection==="Live"?"live":"";
 byId("context").textContent=run?"Run "+run.run_id+" · "+run.nodes.length+"/"+
  (run.expected_node_count||run.expected_nodes.length||"?")+" nodes with samples"+
  (run.exit_code===null?" · No wrapper completion record":" · Wrapper exit: "+run.exit_code):
  "Connecting to saved history…";
 const missing=run?.expected_nodes.filter(name=>!run.nodes.some(node=>node.node===name))||[];
 if(missing.length)byId("context").textContent+=" · Missing: "+missing.join(", ");
 byId("scope").textContent=node?node.scopes.map(scope=>scope+": "+
  (data.scope_notes[scope]||"CPU/memory unavailable; see diagnostics")).join("; "):"No saved samples yet.";
 const queue=data.scheduler?.queue?.[0], snapshot=data.scheduler_snapshot_at, status=data.queue_status;
 byId("scheduler").textContent=status?"Slurm queue: "+(status.state||"job not present")+
  " at "+new Date(status.checked_at).toLocaleTimeString():queue?"Initial Slurm snapshot: "+queue.state+
  (snapshot?" at "+new Date(snapshot).toLocaleTimeString():""):"";
 byId("diagnostics").textContent=[...data.diagnostics,...(discoveryError?[discoveryError]:[])].join("\\n")||
  "No recorded diagnostics.";
 if(data.live)byId("notice").textContent="Live saved samples. Hover or tap for values, or press h and l. "+
  "CPU averages cover sampled intervals only. Missing readings remain gaps. Memory peaks stay per node. "+
  "Closing the last browser tab stops viewing, while workload collection continues.";
 document.title=byId("title").textContent;
 const points=node?node.points:[], summary=windowText(points);
 byId("window").textContent=summary;byId("window").hidden=!summary;
 for(const id of Object.keys(series)){
  chart(id,points);
  byId(id+"-reading").textContent=inspect!==null && points.length?
   readingText(id,points,points[nearestIndex(points,inspect)]):"Hover or tap to inspect.";
  document.querySelector('[data-save="'+id+'"]').disabled=!points.length;
 }
}
function selectRun(preferredNode){
 const run=data.runs[Number(byId("run").value)];
 byId("node").replaceChildren();
 run?.nodes.forEach((node,index)=>option(byId("node"),index,node.node));
 const selected=run?.nodes.findIndex(node=>node.node===preferredNode);
 if(selected>=0)byId("node").value=selected;
 draw();
}
function jobName(job){
 return job.scheduler?.queue?.[0]?.job_name||[...job.runs].reverse().find(run=>run.job_name)?.job_name||"";
}
function completionResult(job,run){
 if(Number.isInteger(run?.exit_code)){
  const code=run.exit_code, detail="\\nBatch wrapper result for the selected run. "+
   "Use hpc-jreport "+job.job_id+" for final Slurm state.";
  return code===0?{kind:"finished",label:"finished",message:"Workload finished (exit 0)."+detail}:
   {kind:"failed",label:"exit "+code,message:"Workload exited with code "+code+
    ". Check the job's output/error logs."+detail};
 }
 const status=job.queue_status;
 if(!status || status.state!==null || run!==job.runs[job.runs.length-1])return null;
 const checked=Date.parse(status.checked_at), created=Date.parse(run?.metadata?.created_at);
 if(!Number.isFinite(checked) || (Number.isFinite(created) && created>checked))return null;
 const detail=run?.metadata?.attachment?"Attached monitoring cannot record the workload's exit code.":
  "No workload exit code was recorded by this monitor.";
 return {kind:"unknown",label:"left queue",message:"Job left the Slurm queue.\\n"+detail};
}
function rememberJob(){
 if(!jobs.length || !data.job_id)return;
 const run=data.runs[Number(byId("run").value)];
 jobViews.set(data.job_id,{run:run?.run_id,node:run?.nodes[Number(byId("node").value)]?.node,
  following:!run || run===data.runs[data.runs.length-1],axis:byId("axis").value,
  exportStatus:byId("export-status").textContent});
}
function showJob(){
 data=jobs.find(job=>job.job_id===selectedJob);
 const view=jobViews.get(selectedJob);
 byId("run").replaceChildren();
 data.runs.forEach((run,index)=>option(byId("run"),index,run.run_id));
 let index=view && !view.following?data.runs.findIndex(run=>run.run_id===view.run):data.runs.length-1;
 if(index<0)index=data.runs.length-1;
 byId("run").value=index;byId("axis").value=view?.axis||"elapsed";
 byId("export-status").textContent=view?.exportStatus||"";
 selectRun(view?.node);
}
function selectJob(jobId){
 if(selectedJob===jobId)return;
 rememberJob();selectedJob=jobId;clearInspect();renderJobTabs();showJob();
}
function renderJobTabs(){
 const tabs=byId("job-tabs"), panel=byId("job-panel");
 tabs.hidden=jobs.length<2;
 if(jobs.length>1){
  panel.setAttribute("role","tabpanel");panel.setAttribute("aria-labelledby","job-tab-"+selectedJob);
 }else{panel.removeAttribute("role");panel.removeAttribute("aria-labelledby");}
 for(const button of Array.from(tabs.children))
  if(!jobs.some(job=>job.job_id===button.dataset.jobId))button.remove();
 for(const job of jobs){
  const id="job-tab-"+job.job_id;
  let button=byId(id);
  if(!button){
   button=document.createElement("button");button.id=id;button.type="button";button.dataset.jobId=job.job_id;
   button.setAttribute("role","tab");button.setAttribute("aria-controls","job-panel");
   button.onclick=()=>selectJob(job.job_id);
   button.onkeydown=event=>{
    const index=jobs.findIndex(item=>item.job_id===job.job_id);
    const moves={ArrowRight:(index+1)%jobs.length,ArrowLeft:(index+jobs.length-1)%jobs.length,
                 Home:0,End:jobs.length-1};
    if(!Object.hasOwn(moves,event.key))return;
    event.preventDefault();selectJob(jobs[moves[event.key]].job_id);
    byId("job-tab-"+selectedJob).focus();
   };
   tabs.appendChild(button);
  }
  const name=jobName(job), selected=job.job_id===selectedJob;
  const result=completionResult(job,job.runs[job.runs.length-1]);
  button.textContent=job.job_id+(result?" · "+result.label:"")+(name?" ("+name+")":"");
  button.title=button.textContent+" · "+(job.connection||"Saved snapshot");
  button.setAttribute("aria-selected",String(selected));button.tabIndex=selected?0:-1;
  button.className=result?result.kind:job.connection==="Live"?"live":job.connection==="Connecting"?"":"warning";
 }
}
function update(next){
 const previous=data, previousError=discoveryError, hadSelection=selectedJob!==null;
 discoveryError=next.discovery_error||"";
 rememberJob();jobs=next.jobs||[next];
 if(!jobs.some(job=>job.job_id===selectedJob))selectedJob=jobs[0].job_id;
 renderJobTabs();
 const selected=jobs.find(job=>job.job_id===selectedJob);
 if(hadSelection && previous.job_id===selectedJob && selected.version!==undefined &&
    previous.version===selected.version && previous.connection===selected.connection &&
    previousError===discoveryError)return;
 showJob();
}
byId("run").onchange=()=>{clearInspect();selectRun();};
byId("node").onchange=()=>{clearInspect();draw();};byId("axis").onchange=draw;
const inspectMoves={h:[-1,0],l:[1,0],H:[-1,1/20],L:[1,1/20]}, inspectJumps={h:[-1,1/4],l:[1,1/4]};
window.addEventListener("keydown",event=>{
 if(event.altKey || event.metaKey)return;
 if(event.key==="Escape"){if(inspect!==null){clearInspect();draw();}return;}
 const move=event.ctrlKey?(event.shiftKey?null:inspectJumps[event.key]):inspectMoves[event.key];
 if(!move)return;
 // Claim Ctrl-h and Ctrl-l from the browser.
 event.preventDefault();
 const run=data.runs[Number(byId("run").value)], node=run?.nodes[Number(byId("node").value)];
 moveInspect(node?node.points:[],move[0],move[1]);draw();
});
function toggleSidebar(){
 const sidebar=byId("sidebar"), expanded=sidebar.hidden;
 sidebar.hidden=!expanded;
 sidebar.parentElement.classList.toggle("sidebar-collapsed",!expanded);
 const button=byId("sidebar-toggle");
 button.setAttribute("aria-expanded",String(expanded));
}
byId("sidebar-toggle").onclick=toggleSidebar;
for(const [id,specs] of Object.entries(series))for(const [,label,color] of specs){
 const item=document.createElement("span");item.textContent=label;item.style.setProperty("--color",color);
 byId(id+"-legend").appendChild(item);
}
function crc32(bytes){
 let crc=0xffffffff;
 for(const byte of bytes){
  crc^=byte;
  for(let bit=0;bit<8;bit++)crc=(crc>>>1)^((crc&1)?0xedb88320:0);
 }
 return (crc^0xffffffff)>>>0;
}
function zipFiles(files){
 const encoder=new TextEncoder(), parts=[], directory=[];
 let offset=0, directorySize=0;
 const now=new Date(), year=Math.min(2107,Math.max(1980,now.getFullYear()));
 const date=((year-1980)<<9)|((now.getMonth()+1)<<5)|now.getDate();
 const time=(now.getHours()<<11)|(now.getMinutes()<<5)|(now.getSeconds()>>1);
 for(const [name,text] of files){
  const filename=encoder.encode(name), bytes=encoder.encode(text), crc=crc32(bytes);
  if(bytes.length>0xffffffff || offset+bytes.length+30+filename.length>0xffffffff)
   throw new Error("Export exceeds the ZIP size limit.");
  const local=new Uint8Array(30+filename.length), entry=new Uint8Array(46+filename.length);
  const l=new DataView(local.buffer), c=new DataView(entry.buffer);
  l.setUint32(0,0x04034b50,true);l.setUint16(4,20,true);l.setUint16(6,0x800,true);
  l.setUint16(10,time,true);l.setUint16(12,date,true);l.setUint32(14,crc,true);
  l.setUint32(18,bytes.length,true);l.setUint32(22,bytes.length,true);l.setUint16(26,filename.length,true);
  local.set(filename,30);
  c.setUint32(0,0x02014b50,true);c.setUint16(4,0x314,true);c.setUint16(6,20,true);c.setUint16(8,0x800,true);
  c.setUint16(12,time,true);c.setUint16(14,date,true);c.setUint32(16,crc,true);
  c.setUint32(20,bytes.length,true);c.setUint32(24,bytes.length,true);c.setUint16(28,filename.length,true);
  c.setUint32(38,(0o100600<<16)>>>0,true);c.setUint32(42,offset,true);entry.set(filename,46);
  parts.push(local,bytes);directory.push(entry);offset+=local.length+bytes.length;directorySize+=entry.length;
 }
 const end=new Uint8Array(22), e=new DataView(end.buffer);
 e.setUint32(0,0x06054b50,true);e.setUint16(8,files.length,true);e.setUint16(10,files.length,true);
 e.setUint32(12,directorySize,true);e.setUint32(16,offset,true);
 return new Blob([...parts,...directory,end],{type:"application/zip"});
}
function filenamePart(value,maxBytes){
 const normalized=String(value||"").normalize("NFKC").replace(/[^\\p{L}\\p{M}\\p{N}_.-]+/gu,"-")
  .replace(/^[._-]+|[._-]+$/g,"");
 const encoder=new TextEncoder();let result="",size=0;
 for(const character of normalized){
  const bytes=encoder.encode(character).length;
  if(size+bytes>maxBytes)break;
  result+=character;size+=bytes;
 }
 return result.replace(/[._-]+$/g,"")||"unnamed";
}
function exportFiles(id){
 const run=data.runs[Number(byId("run").value)], node=run?.nodes[Number(byId("node").value)];
 if(!node?.points.length)throw new Error("No saved samples to export.");
 const captured=new Date().toISOString(), first=node.points[0].time;
 const raw=node.points.filter(point=>typeof point.raw_sample==="string").map(point=>point.raw_sample);
 const points=node.points.map(({raw_sample,...point})=>point);
 const snapshot={schema_version:1,exported_at:captured,job_id:data.job_id,job_name:run.job_name,
  run_id:run.run_id,node:node.node,chart:id,time_axis:byId("axis").value,
  timezone:Intl.DateTimeFormat().resolvedOptions().timeZone,run_metadata:run.metadata||null,
  workload_exit_code:run.exit_code,diagnostics:data.diagnostics,scope_notes:data.scope_notes,
  units:exportSpec.units,
  raw_sample_count:raw.length,raw_samples_complete:raw.length===points.length,points:points};
 const columns=exportSpec.columns;
 const cell=value=>value===null || value===undefined?"":'"'+String(value).replace(/"/g,'""')+'"';
 const csv=[columns.join(",")];
 for(const point of points){
  const scope=Object.hasOwn(data.scope_notes,point.scope)?point.scope:"unavailable";
  csv.push([new Date(point.time).toISOString(),(point.time-first)/1000,point.cpu,point.average,point.maximum,
   point.memory,point.memory_peak,point.samples,point.final,point.gap,scope].map(cell).join(","));
 }
 const svg=byId(id).cloneNode(true);svg.setAttribute("xmlns",ns);
 if(points[points.length-1].time-first>WINDOW_MS)chart(id,points,svg,[first,points[points.length-1].time]);
 svg.querySelector("[visibility]")?.remove();
 const files=[["chart.svg",new XMLSerializer().serializeToString(svg)],
  ["data.csv",csv.join("\\r\\n")+"\\r\\n"],["data.json",JSON.stringify(snapshot,null,2)+"\\n"]];
 if(raw.length)files.push(["samples.jsonl",raw.join("\\n")+"\\n"]);
 const stamp=captured.replace(/[-:.]/g,"");
 const jobName=filenamePart(run.job_name,64), runName=filenamePart(run.run_id,48), nodeName=filenamePart(node.node,32);
 const filename="jobmon-"+data.job_id+"-"+jobName+"-"+runName+"-"+nodeName+"-"+id+"-"+stamp+".zip";
 return {files,filename,complete:snapshot.raw_samples_complete};
}
for(const button of document.querySelectorAll("[data-save]"))button.onclick=()=>{
 try{
  // Capture synchronously, before a live update.
  const result=exportFiles(button.dataset.save), url=URL.createObjectURL(zipFiles(result.files));
  const link=document.createElement("a");link.href=url;link.download=result.filename;link.click();
  setTimeout(()=>URL.revokeObjectURL(url),1000);
  byId("export-status").textContent=result.complete?"Saved image + CSV, JSON, and original JSONL samples.":
   "Saved image + CSV/JSON. For original JSONL samples, update the reporter with hpc-jdeploy --apply.";
 }catch(error){byId("export-status").textContent="Export failed: "+error.message;}
};
update(data);
if(data.live){
 const events=new EventSource("events");
 events.onmessage=event=>{try{update(JSON.parse(event.data));}catch(error){
  byId("connection").textContent="Invalid dashboard update";}};
 events.onerror=()=>{byId("connection").textContent="Local connection interrupted; retrying";};
 window.addEventListener("pagehide",()=>events.close());
}
</script>
</html>
""".replace("__JOBMON_EXPORT_SPEC__", json.dumps(EXPORT_SPEC, separators=(",", ":"))).replace(
    "__JOBMON_WINDOW_MS__", str(CHART_WINDOW_MS))


def render_html(data):
    # Escape </script>; use textContent when rendering.
    payload = json.dumps(data, ensure_ascii=True, allow_nan=False).replace("<", "\\u003c")
    payload = payload.replace(">", "\\u003e").replace("&", "\\u0026")
    return HTML.replace("__JOBMON_DATA__", payload)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("job_id")
    parser.add_argument("archive", type=Path)
    parser.add_argument("--output", type=Path, help="new HTML file; defaults to the archive basename")
    args = parser.parse_args(argv)
    try:
        data = read_archive(args.job_id, args.archive)
        name = args.archive.name
        output = args.output or args.archive.with_name(
            (name[:-7] if name.endswith(".tar.gz") else name) + ".html")
        with private_open(output, os.O_WRONLY | os.O_CREAT | os.O_EXCL, "w") as stream:
            stream.write(render_html(data))
        for message in data["diagnostics"]:
            print("jobmon plot: " + message, file=sys.stderr)
        print(output)
        return 0
    except (OSError, JobmonError, tarfile.TarError, ValueError, RecursionError) as exc:
        print("jobmon plot: " + str(exc), file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())

