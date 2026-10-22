#!/usr/bin/env python3
"""Allocation-local collection and private storage; no scheduler polling."""

import argparse
import datetime as dt
import fcntl
import json
import os
from pathlib import Path, PurePosixPath
import re
import signal
import socket
import stat
import sys
import threading
import time
import uuid

from slurm_backend import JobmonError, identity, number, run_command, validate_job_id

SCHEMA_VERSION = 1
STOP_POLL_SECONDS = 5
SAFE_NAME = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,199}\Z")
STORAGE_PROBE_FILE = ".shared-storage-probe.json"


def utc_now():
    return dt.datetime.now(dt.timezone.utc).isoformat(timespec="milliseconds")


def data_root():
    configured = os.environ.get("JOBMON_DATA_DIR")
    if configured:
        root = Path(configured).expanduser()
        if not root.is_absolute():
            raise JobmonError("JOBMON_DATA_DIR must be an absolute path shared by login and compute nodes.")
        return root
    state = Path(os.environ.get("XDG_STATE_HOME") or str(Path.home() / ".local/state")).expanduser()
    if not state.is_absolute():
        raise JobmonError("XDG_STATE_HOME must be absolute when JOBMON_DATA_DIR is unset.")
    return state / "jobmon"


def checked_name(value):
    if not isinstance(value, str) or not SAFE_NAME.fullmatch(value) or value in (".", ".."):
        raise JobmonError("Invalid storage component: {!r}".format(value))
    return value


def private_directory(path, create=False):
    path = Path(path).absolute()
    if create:
        path.mkdir(mode=0o700, parents=True, exist_ok=True)
    try:
        info = path.lstat()
    except OSError as exc:
        raise JobmonError("Cannot access private storage {}: {}".format(path, exc)) from exc
    if not stat.S_ISDIR(info.st_mode) or info.st_uid != os.getuid() or stat.S_IMODE(info.st_mode) & 0o077:
        raise JobmonError("Storage must be a real directory owned by you with mode 700: {}".format(path))
    return path


def private_open(path, flags, mode="r"):
    fd = os.open(str(path), flags | os.O_NOFOLLOW | os.O_NONBLOCK, 0o600)
    try:
        info = os.fstat(fd)
        if not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid() or stat.S_IMODE(info.st_mode) & 0o077:
            raise JobmonError("Refusing non-private or non-regular file: {}".format(path))
        return os.fdopen(fd, mode, encoding="utf-8")
    except BaseException:
        os.close(fd)
        raise


def write_json(path, value):
    with private_open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, "w") as stream:
        json.dump(value, stream, separators=(",", ":"), allow_nan=False)
        stream.write("\n")


def read_json(path):
    with private_open(path, os.O_RDONLY) as stream:
        result = json.load(stream)
    if not isinstance(result, dict):
        raise JobmonError("Expected a JSON object in {}".format(path))
    return result


def verify_storage_probe(root=None):
    """Verify that a probe created on the login node is visible here."""
    root = private_directory(root or data_root())
    path = root / STORAGE_PROBE_FILE
    try:
        probe = read_json(path)
    except FileNotFoundError as exc:
        raise JobmonError(
            "Shared-storage probe is missing; run hpc-jstorage-init from the workstation before monitored jobs."
        ) from exc
    token = probe.get("token")
    if (probe.get("schema_version") != SCHEMA_VERSION
            or probe.get("kind") != "jobmon_shared_storage_probe"
            or probe.get("uid") != os.getuid()
            or probe.get("user") != identity()
            or probe.get("configured_path") != str(root)
            or not isinstance(token, str)
            or not re.fullmatch(r"[0-9a-f]{32}", token)):
        raise JobmonError("Shared-storage probe is malformed or belongs to another path/account.")
    return {"verified": True, "path": str(root), "probe_file": str(path),
            "created_at": probe.get("created_at"), "created_on": probe.get("created_on")}


def initialize_storage_probe(root=None, allow_allocation=False):
    """Create the login-side marker later required from allocation nodes."""
    if os.environ.get("SLURM_JOB_ID") and not allow_allocation:
        raise JobmonError("Initialize shared storage from the login node, outside a Slurm allocation.")
    root = private_directory(root or data_root(), create=True)
    path = root / STORAGE_PROBE_FILE
    if path.exists():
        result = verify_storage_probe(root)
        result["created"] = False
        return result
    probe = {"schema_version": SCHEMA_VERSION, "kind": "jobmon_shared_storage_probe",
             "uid": os.getuid(), "user": identity(), "configured_path": str(root),
             "token": uuid.uuid4().hex, "created_at": utc_now(), "created_on": socket.gethostname()}
    try:
        write_json(path, probe)
    except FileExistsError:
        result = verify_storage_probe(root)
        result["created"] = False
        return result
    result = verify_storage_probe(root)
    result["created"] = True
    return result


def allocation_job_id(env=None):
    env = os.environ if env is None else env
    raw = env.get("SLURM_JOB_ID", "")
    validate_job_id(raw)
    if "_" in raw or env.get("SLURM_HET_SIZE") or env.get("SLURM_HET_GROUP"):
        raise JobmonError("Heterogeneous allocations are unsupported; SLURM_JOB_ID must be numeric.")
    if env.get("SLURM_ARRAY_JOB_ID") and env.get("SLURM_ARRAY_TASK_ID"):
        return validate_job_id(env["SLURM_ARRAY_JOB_ID"] + "_" + env["SLURM_ARRAY_TASK_ID"]), raw
    return raw, raw


def expand_cpu_counts(value):
    counts = []
    for part in (value or "").split(","):
        match = re.fullmatch(r"([0-9]+)(?:\(x([0-9]+)\))?", part)
        if not match:
            return []
        repeats = int(match.group(2) or "1")
        if repeats > 100000:
            raise JobmonError("Invalid SLURM_JOB_CPUS_PER_NODE repetition count.")
        counts.extend([int(match.group(1))] * repeats)
    return counts


def initialize(root=None, runner=run_command):
    job_id, raw = allocation_job_id()
    root = private_directory(root or data_root(), create=True)
    probe = verify_storage_probe(root)
    job_dir = private_directory(root / job_id, create=True)
    run_id = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-r{}-{}".format(
        os.environ.get("SLURM_RESTART_COUNT", "0"), uuid.uuid4().hex[:12])
    checked_name(run_id)
    run_dir = private_directory(job_dir / run_id, create=True)
    errors = []
    nodes = []
    if os.environ.get("SLURM_JOB_NODELIST"):
        try:
            nodes = runner(["scontrol", "show", "hostnames", os.environ["SLURM_JOB_NODELIST"]]).split()
            for node in nodes:
                checked_name(node)
        except JobmonError as exc:
            errors.append(str(exc))
    count = number(os.environ.get("SLURM_JOB_NUM_NODES") or os.environ.get("SLURM_NNODES"))
    if not nodes and count == 1:
        nodes = [os.environ.get("SLURMD_NODENAME") or socket.gethostname()]
    if count and len(nodes) < count:
        errors.append("Incomplete expected-node layout: Slurm declares {} nodes but only {} hostnames are known; "
                      "concurrent memory totals and collector completeness are unavailable.".format(
                          int(count), len(nodes)))
    try:
        cpus = expand_cpu_counts(os.environ.get("SLURM_JOB_CPUS_PER_NODE"))
    except JobmonError as exc:
        # Match the sibling hostname lookup: record the failure, keep collecting.
        cpus = []
        errors.append(str(exc))
    # Store only a layout load_run accepts. A short or zero-CPU map would make
    # the whole run permanently unreadable instead of losing its denominators.
    cpu_map = {}
    if nodes and len(nodes) == len(cpus) and all(count >= 1 for count in cpus):
        cpu_map = dict(zip(nodes, cpus))
    elif cpus:
        errors.append("SLURM_JOB_CPUS_PER_NODE does not give every allocation node at least one CPU; "
                      "per-node CPU denominators are unavailable.")
    metadata = {"schema_version": SCHEMA_VERSION, "job_id": job_id, "job_id_raw": raw,
                "job_name": os.environ.get("SLURM_JOB_NAME") or None,
                "run_id": run_id, "uid": os.getuid(), "user": identity(), "created_at": utc_now(),
                "restart_count": os.environ.get("SLURM_RESTART_COUNT", "0"),
                "cluster": os.environ.get("SLURM_CLUSTER_NAME"), "expected_nodes": nodes,
                "expected_node_count": int(count) if count else len(nodes) or None,
                "allocated_cpus_by_node": cpu_map, "errors": errors,
                "storage_visibility": "login-created probe verified from allocation node",
                "storage_probe": probe}
    write_json(run_dir / "metadata.json", metadata)
    return run_dir


def load_run(run_dir, root=None, job_id=None):
    run_dir = Path(run_dir).absolute()
    root = private_directory(root or data_root())
    job_id = validate_job_id(job_id or run_dir.parent.name)
    expected = root / job_id / checked_name(run_dir.name)
    if run_dir != expected:
        raise JobmonError("Run directory must be an immediate child of the job's private storage directory.")
    private_directory(expected.parent)
    private_directory(expected)
    metadata = read_json(expected / "metadata.json")
    if (metadata.get("schema_version") != SCHEMA_VERSION or metadata.get("uid") != os.getuid()
            or metadata.get("user") != identity() or metadata.get("job_id") != job_id
            or metadata.get("run_id") != expected.name):
        raise JobmonError("Stored run identity or ownership does not match: {}".format(expected))
    validate_job_id(metadata.get("job_id_raw"))
    nodes = metadata.get("expected_nodes")
    cpus = metadata.get("allocated_cpus_by_node")
    if (not isinstance(nodes, list) or not isinstance(cpus, dict)
            or not isinstance(metadata.get("errors", []), list)):
        raise JobmonError("Malformed allocation metadata: " + str(expected))
    for node in nodes:
        checked_name(node)
    if len(nodes) != len(set(nodes)):
        raise JobmonError("Duplicate expected nodes in allocation metadata.")
    count = metadata.get("expected_node_count")
    if count is not None and (type(count) is not int or count < 1 or count < len(nodes)):
        raise JobmonError("Invalid expected node count in allocation metadata.")
    if any(type(value) is not int or value < 1 for value in cpus.values()):
        raise JobmonError("Invalid per-node CPU allocation metadata.")
    return metadata


def unescape_mount(value):
    return re.sub(r"\\([0-7]{3})", lambda match: chr(int(match.group(1), 8)), value)


def discover_cgroup(raw_job_id, proc=Path("/proc/self")):
    """Select only a visible job_<raw ID> ancestor of this process's cgroup."""
    validate_job_id(raw_job_id)
    try:
        lines = (proc / "cgroup").read_text().splitlines()
        memberships = [line.split(":", 2)[2] for line in lines if line.startswith("0::")]
        if not memberships:
            return None, "cgroup v1 or no unified membership; v1 collection is unsupported"
        member = PurePosixPath(memberships[0])
        boundaries = [(i, re.fullmatch(r"job_([0-9]+)(?:\.scope)?", part)) for i, part in enumerate(member.parts)]
        boundaries = [(i, match) for i, match in boundaries if match]
        if not boundaries or boundaries[-1][1].group(1) != raw_job_id:
            return None, "no verified job-level cgroup ancestor; collector-only and user scopes are not measured"
        job_path = PurePosixPath(*member.parts[:boundaries[-1][0] + 1])
        for line in (proc / "mountinfo").read_text().splitlines():
            left, separator, right = line.partition(" - ")
            if not separator or right.split()[0] != "cgroup2":
                continue
            fields = left.split()
            mount_root = PurePosixPath(unescape_mount(fields[3]))
            mount_point = Path(unescape_mount(fields[4]))
            try:
                relative = job_path.relative_to(mount_root)
            except ValueError:
                continue
            candidate = mount_point / str(relative)
            if candidate.is_dir() and candidate.resolve() == candidate.absolute():
                return candidate, None
        return None, "job boundary is outside visible cgroup mounts (possibly a cgroup namespace)"
    except (OSError, ValueError, IndexError) as exc:
        return None, "cgroup discovery failed: {}".format(exc)


PROCESS_TREE_SCOPE = "process_tree_pgid"
# Launchers whose real work runs in a separate Slurm step, outside this group.
SEPARATE_STEP_LAUNCHERS = ("srun", "mpirun", "mpiexec", "orterun")


def discover_process_group(env=None):
    """Opt-in process-group scope for sites where Slurm creates no job cgroup."""
    env = os.environ if env is None else env
    if env.get("JOBMON_PROCESS_TREE") != "1":
        return None, ("process-tree sampling is off; set JOBMON_PROCESS_TREE=1 only where "
                      "Slurm creates no job cgroup and the workload shares the batch process group")
    try:
        return os.getpgid(0), None
    except OSError as exc:
        return None, "process group unavailable: {}".format(exc)


def read_process_stat(pid, proc=Path("/proc")):
    """Return (process group, CPU ticks, RSS pages, command) from /proc/<pid>/stat.

    The command field is parenthesized and may itself contain spaces and
    parentheses, so every numeric field is taken after its final ')'.
    """
    content = (proc / str(pid) / "stat").read_text()
    end = content.rindex(")")
    command = content[content.index("(") + 1:end]
    # This slice starts at field 3 (state), so proc(5) field N is at index N - 3.
    fields = content[end + 2:].split()
    # utime, stime, cutime, cstime: reaped descendants keep the total monotonic.
    ticks = sum(int(fields[index]) for index in (11, 12, 13, 14))
    return int(fields[2]), ticks, int(fields[21]), command


def process_tree_sample(pgid, proc=Path("/proc"), exclude=()):
    """Sum CPU and RSS across one process group; an approximation, not a cgroup."""
    result = {"scope": PROCESS_TREE_SCOPE, "cpu_seconds": None, "memory_bytes": None,
              "memory_peak_bytes": None, "counted_processes": 0, "errors": []}
    try:
        pids = sorted(int(entry.name) for entry in proc.iterdir() if entry.name.isdigit())
    except OSError as exc:
        result["errors"].append("/proc is unreadable: {}".format(exc))
        return result
    ticks = 0
    pages = 0
    launchers = set()
    for pid in pids:
        if pid in exclude:
            continue
        try:
            group, process_ticks, process_pages, command = read_process_stat(pid, proc)
        except (OSError, ValueError, IndexError):
            continue  # The process exited between listing and reading; skip it.
        if group != pgid:
            continue
        result["counted_processes"] += 1
        ticks += process_ticks
        pages += process_pages
        if command in SEPARATE_STEP_LAUNCHERS:
            launchers.add(command)
    if not result["counted_processes"]:
        result["errors"].append(
            "no processes in process group {}; the workload does not share the collector's "
            "group, so nothing is measurable".format(pgid))
        return result
    result["cpu_seconds"] = ticks / os.sysconf("SC_CLK_TCK")
    result["memory_bytes"] = pages * os.sysconf("SC_PAGE_SIZE")
    if launchers:
        result["errors"].append(
            "process group contains {}; work launched into a separate Slurm step is not "
            "counted and these totals understate the job".format(", ".join(sorted(launchers))))
    return result


def cgroup_sample(path):
    result = {"scope": "job_cgroup_v2", "cpu_seconds": None, "memory_bytes": None,
              "memory_peak_bytes": None, "errors": []}
    try:
        cpu = dict(line.split() for line in (path / "cpu.stat").read_text().splitlines())
        result["cpu_seconds"] = int(cpu["usage_usec"]) / 1000000
    except (OSError, ValueError, KeyError) as exc:
        result["errors"].append("cpu.stat: {}".format(exc))
    for filename, key in (("memory.current", "memory_bytes"), ("memory.peak", "memory_peak_bytes")):
        try:
            result[key] = int((path / filename).read_text())
        except (OSError, ValueError) as exc:
            result["errors"].append("{}: {}".format(filename, exc))
    return result


def gpus_allocated(env):
    """True when Slurm or CUDA shows GPUs on this node; never which ones are the job's."""
    if number(env.get("SLURM_GPUS_ON_NODE")):
        return True
    for name in ("SLURM_JOB_GPUS", "SLURM_STEP_GPUS", "CUDA_VISIBLE_DEVICES"):
        value = (env.get(name) or "").strip()
        if value and value not in ("NoDevFiles", "-1"):
            return True
    return False


class NvidiaSampler:
    FIELDS = ("uuid", "utilization.gpu", "memory.used", "memory.total", "temperature.gpu", "power.draw")

    def __init__(self, runner=run_command, env=None):
        self.run = runner
        env = os.environ if env is None else env
        self.uuids = [x.strip() for x in env.get("JOBMON_GPU_UUIDS", "").split(",") if x.strip()]
        self.fields = []
        self.reason = None
        self.enabled = True
        self.exclusive = False
        if not self.uuids:
            # An unset mapping turns GPU collection off. Only an allocation that holds
            # GPUs needs a per-sample reminder of how to attribute them.
            if gpus_allocated(env):
                self.reason = ("no verified GPU UUID mapping; set JOBMON_GPU_UUIDS per node after "
                               "checking allocation ownership")
            else:
                self.enabled = False
            return
        full_uuid = r"GPU-[0-9a-fA-F]{8}(?:-[0-9a-fA-F]{4}){3}-[0-9a-fA-F]{12}"
        if len(set(self.uuids)) != len(self.uuids) or any(not re.fullmatch(full_uuid, value) for value in self.uuids):
            self.reason = "GPU mapping must contain unique full GPU UUIDs; numeric ordinals and MIG IDs are unsupported"
            return
        try:
            supported = self.run(["nvidia-smi", "--help-query-gpu"])
            self.fields = [field for field in self.FIELDS if re.search(r'"' + re.escape(field) + r'"', supported)]
            if "uuid" not in self.fields:
                self.reason = "nvidia-smi does not advertise the UUID query field"
                return
            listing = self.run(["nvidia-smi", "-L"])
            self.exclusive = env.get("JOBMON_GPU_EXCLUSIVE") == "1" and "MIG" not in listing
        except JobmonError as exc:
            self.reason = str(exc)

    def sample(self):
        if not self.enabled:
            return [], []
        if self.reason:
            return [], [self.reason]
        records, errors = [], []
        try:
            output = self.run(["nvidia-smi", "--id=" + ",".join(self.uuids),
                               "--query-gpu=" + ",".join(self.fields), "--format=csv,noheader,nounits"])
            for line in output.splitlines():
                values = [value.strip() for value in line.split(",")]
                if len(values) != len(self.fields):
                    errors.append("malformed NVIDIA CSV record")
                    continue
                row = dict(zip(self.fields, values))
                if row["uuid"] not in self.uuids:
                    errors.append("NVIDIA returned an unrequested UUID")
                    continue
                record = {"uuid": row["uuid"],
                          "scope": "exclusive_device_user_verified" if self.exclusive else "shared_device_context",
                          "device_utilization_percent": number(row.get("utilization.gpu")),
                          "memory_used_bytes": None, "memory_total_bytes": None,
                          "temperature_c": number(row.get("temperature.gpu")), "power_w": number(row.get("power.draw"))}
                for source, target in (("memory.used", "memory_used_bytes"), ("memory.total", "memory_total_bytes")):
                    value = number(row.get(source))
                    record[target] = int(value * 1024 ** 2) if value is not None else None
                record["job_utilization_percent"] = record["device_utilization_percent"] if self.exclusive else None
                record["reason"] = None if self.exclusive else (
                    "device totals are context; shared/MPS/MIG job utilization is unavailable")
                record["unavailable_fields"] = [field for field in self.FIELDS[1:] if number(row.get(field)) is None]
                records.append(record)
            missing = set(self.uuids) - {row["uuid"] for row in records}
            if missing:
                errors.append("missing GPU UUIDs: " + ", ".join(sorted(missing)))
        except JobmonError as exc:
            errors.append(str(exc))
        return records, errors


def sample_metrics(cgroup, cgroup_error, cpus, previous, tick, process_group=None):
    if cgroup:
        metrics = cgroup_sample(cgroup)
    elif process_group is not None:
        # Never measure the collector itself; skipping it also drops its reaped children.
        metrics = process_tree_sample(process_group, exclude=(os.getpid(),))
    else:
        metrics = {"scope": "unavailable", "cpu_seconds": None, "memory_bytes": None,
                   "memory_peak_bytes": None, "errors": [cgroup_error]}
    metrics.update({"allocated_cpus": cpus, "cpu_delta_seconds": None,
                    "cpu_interval_seconds": None, "cpu_percent": None})
    if not previous or metrics["cpu_seconds"] is None or previous[1] is None:
        return metrics
    delta, duration = metrics["cpu_seconds"] - previous[1], tick - previous[0]
    if delta < 0 or duration <= 0:
        metrics["errors"].append("CPU counter reset or invalid monotonic interval")
        return metrics
    metrics["cpu_delta_seconds"] = delta
    metrics["cpu_interval_seconds"] = duration
    if cpus:
        metrics["cpu_percent"] = 100 * delta / (duration * cpus)
    return metrics


def sample_record(metadata, job_id, raw_job_id, node, stream_id, interval,
                  tick, timestamp, metrics, gpus, gpu_errors, final):
    return {"schema_version": SCHEMA_VERSION, "job_id": job_id, "job_id_raw": raw_job_id,
            "run_id": metadata["run_id"], "node": node, "stream_id": stream_id,
            "timestamp": timestamp, "monotonic_seconds": tick, "interval_seconds": interval,
            "metrics": metrics, "gpus": gpus, "errors": metrics["errors"] + gpu_errors,
            "final": final}


def stop_requested(run_dir, stop):
    if stop.is_set():
        return True
    if (run_dir / "stop.json").exists():
        stop.set()
        return True
    return False


def wait_for_next_sample(run_dir, stop, deadline, poll=STOP_POLL_SECONDS):
    """Wait responsively so a stop marker still produces one final sample.

    Signals set the event directly, so only the shared-storage marker is polled;
    a short interval still bounds each wait by its own remaining time.
    """
    while time.monotonic() < deadline and not stop.is_set():
        stop.wait(min(poll, max(0, deadline - time.monotonic())))
        stop_requested(run_dir, stop)


def collect(run_dir, interval=60, once=False, stop=None):
    if not 1 <= interval <= 86400:
        raise JobmonError("Collection interval must be between 1 and 86400 seconds.")
    metadata = load_run(run_dir)
    job_id, raw = allocation_job_id()
    if job_id != metadata["job_id"] or raw != metadata["job_id_raw"]:
        raise JobmonError("Collector allocation does not match run metadata.")
    node = checked_name(os.environ.get("SLURMD_NODENAME") or socket.gethostname())
    if metadata["expected_nodes"] and node not in metadata["expected_nodes"]:
        raise JobmonError("Collector node is not an expected allocation node.")
    run_dir = Path(run_dir)
    stop = stop or threading.Event()
    cgroup, cgroup_error = discover_cgroup(raw)
    process_group = None
    if not cgroup:
        process_group, group_error = discover_process_group()
        if process_group is None:
            cgroup_error = "; ".join(part for part in (cgroup_error, group_error) if part)
    gpu = NvidiaSampler()
    cpus = metadata["allocated_cpus_by_node"].get(node)
    lock = private_open(run_dir / (node + ".lock"), os.O_RDWR | os.O_CREAT, "r+")
    try:
        try:
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise JobmonError("A collector is already running on node " + node) from exc
        # A restarted collector gets a separate stream; counters are never spliced.
        stream_id = node + "--" + uuid.uuid4().hex[:12]
        with private_open(run_dir / (stream_id + ".jsonl"), os.O_WRONLY | os.O_CREAT | os.O_EXCL, "w") as stream:
            previous = None
            while True:
                tick = time.monotonic()
                # CPU/memory alignment uses capture time, before a possibly slow GPU query.
                timestamp = utc_now()
                metrics = sample_metrics(cgroup, cgroup_error, cpus, previous, tick, process_group)
                gpus, gpu_errors = gpu.sample()
                record = sample_record(metadata, job_id, raw, node, stream_id, interval,
                                       tick, timestamp, metrics, gpus, gpu_errors, stop.is_set() or once)
                stream.write(json.dumps(record, separators=(",", ":"), allow_nan=False) + "\n")
                stream.flush()
                if previous is None:
                    write_json(run_dir / (stream_id + ".ready.json"), {"node": node, "timestamp": record["timestamp"]})
                previous = (tick, metrics["cpu_seconds"])
                if stop.is_set() or once:
                    break
                if stop_requested(run_dir, stop):
                    continue
                # Check the stop marker promptly without collecting or calling Slurm.
                wait_for_next_sample(run_dir, stop, tick + interval)
    finally:
        lock.close()


def finish(run_dir, exit_code):
    metadata = load_run(run_dir)
    job_id, raw = allocation_job_id()
    if (job_id, raw) != (metadata["job_id"], metadata["job_id_raw"]):
        raise JobmonError("Completion allocation does not match run metadata.")
    write_json(Path(run_dir) / "completion.json", {"schema_version": SCHEMA_VERSION,
               "job_id": job_id, "run_id": metadata["run_id"], "timestamp": utc_now(),
               "workload_exit_code": exit_code, "scope": "batch wrapper; reconcile final state with sacct"})


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    subs = parser.add_subparsers(dest="command", required=True)
    subs.add_parser("init", help="create one shared run directory inside the allocation")
    sample = subs.add_parser("run", help="run one collector on this allocation node")
    sample.add_argument("--run-dir", required=True)
    sample.add_argument("--interval", type=float, default=60)
    sample.add_argument("--once", action="store_true")
    for name in ("stop", "finish"):
        command = subs.add_parser(name)
        command.add_argument("--run-dir", required=True)
        if name == "finish":
            command.add_argument("--exit-code", type=int, required=True)
    args = parser.parse_args(argv)
    try:
        if args.command == "init":
            print(initialize())
        elif args.command == "run":
            stop = threading.Event()
            for signum in (signal.SIGTERM, signal.SIGINT, signal.SIGHUP):
                signal.signal(signum, lambda _signum, _frame: stop.set())
            collect(args.run_dir, args.interval, args.once, stop)
        elif args.command == "stop":
            load_run(args.run_dir)
            try:
                write_json(Path(args.run_dir) / "stop.json", {"timestamp": utc_now()})
            except FileExistsError:
                pass
        elif args.command == "finish":
            finish(args.run_dir, args.exit_code)
    except (JobmonError, OSError, ValueError) as exc:
        print("jobmon collector: " + str(exc), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
