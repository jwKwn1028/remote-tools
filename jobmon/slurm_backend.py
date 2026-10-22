"""Bounded, read-only Slurm queries. Python 3.8+, standard library only."""

import datetime as dt
import os
import pwd
import re
import shutil
import subprocess


class JobmonError(Exception):
    """An actionable error suitable for the command line."""


ACCOUNTING_DISABLED_GUIDANCE = (
    "Slurm accounting storage is disabled on this cluster, so sacct cannot describe any job, "
    "running or completed. squeue and sstat still report live jobs; completed-job usage is "
    "retained only by the opt-in collector.")


def accounting_disabled(message):
    """True when Slurm reports that accounting storage is turned off site-wide."""
    return "accounting storage is disabled" in str(message).lower()


def validate_job_id(value):
    if not isinstance(value, str) or not re.fullmatch(r"[1-9][0-9]*(?:_[0-9]+)?", value):
        raise JobmonError("Use an allocation ID or array task ID (123456 or 123456_7); "
                          "steps, array ranges, and heterogeneous components are unsupported.")
    if len(value) > 40:
        raise JobmonError("Job ID is too long.")
    return value


def identity():
    return pwd.getpwuid(os.getuid()).pw_name


def run_command(argv, timeout=15):
    env = dict(os.environ, LC_ALL="C", SLURM_BITSTR_LEN="256")
    try:
        result = subprocess.run(argv, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                universal_newlines=True, timeout=timeout, env=env)
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise JobmonError("{}: {}".format(argv[0], exc)) from exc
    if result.returncode:
        raise JobmonError("{} exited {}: {}".format(
            argv[0], result.returncode, result.stderr.strip()[:2000]))
    return result.stdout


def number(value):
    try:
        parsed = float(value)
        return parsed if 0 <= parsed < float("inf") else None
    except (ValueError, TypeError):
        return None


def cpu_seconds(value):
    if not value or not re.fullmatch(r"(?:[0-9]+-)?[0-9]+(?::[0-9]+){0,2}(?:\.[0-9]+)?", value):
        return None
    day, clock = value.split("-", 1) if "-" in value else ("0", value)
    total = 0.0
    for component in clock.split(":"):
        total = total * 60 + float(component)
    return int(day) * 86400 + total


def memory_bytes(value, default_unit="M"):
    match = re.fullmatch(r"([0-9]+(?:\.[0-9]+)?)([KMGTPE]?)(?:i?B)?", value or "", re.I)
    if not match:
        return None
    unit = (match.group(2) or default_unit).upper()
    return int(float(match.group(1)) * 1024 ** ("BKMGTPE".index(unit)))


def requested_memory(value, cpus, nodes):
    match = re.fullmatch(r"(.+?)([cn])?", value or "")
    amount = memory_bytes(match.group(1)) if match else None
    # An unreadable request has no per-CPU/per-node scope either. The trailing
    # letter of an unparseable value must never be reported as that scope.
    if amount is None:
        return {"bytes": None, "scope": "unavailable", "allocation_bytes": None}
    suffix = match.group(2)
    multiplier = cpus if suffix == "c" else nodes if suffix == "n" else None
    return {"bytes": amount, "scope": {"c": "per_cpu", "n": "per_node"}.get(suffix, "unspecified"),
            "allocation_bytes": int(amount * multiplier) if amount and multiplier else None}


def parse_rows(output, fields, free_last=False):
    rows = []
    for line in output.splitlines():
        if not line.strip():
            continue
        # Query job names last so an embedded separator stays in the name.
        cells = line.split("|", len(fields) - 1) if free_last else line.split("|")
        if not free_last and len(cells) == len(fields) + 1 and cells[-1] == "":
            cells.pop()
        if len(cells) != len(fields):
            raise JobmonError("Malformed Slurm row: expected {} fields, received {}.".format(len(fields), len(cells)))
        rows.append(dict(zip(fields, (cell.strip() for cell in cells))))
    return rows


def unavailable(reason, **extra):
    return dict({"value": None, "reason": reason}, **extra)


def normalize_allocation(parent, steps):
    cpus = number(parent.get("AllocCPUS"))
    elapsed = number(parent.get("ElapsedRaw"))
    consumed = cpu_seconds(parent.get("TotalCPU"))
    request = requested_memory(parent.get("ReqMem"), cpus, number(parent.get("NNodes")))
    tres = dict(item.split("=", 1) for item in parent.get("AllocTRES", "").split(",") if "=" in item)
    allocation_memory = memory_bytes(tres.get("mem")) or request["allocation_bytes"]
    efficiency = unavailable("TotalCPU, elapsed time, or allocated CPUs unavailable", unit="percent")
    if consumed is not None and elapsed and cpus:
        if consumed > 0:
            efficiency = {"value": 100 * consumed / (elapsed * cpus), "unit": "percent", "reason": None}
        else:
            efficiency = unavailable("Slurm returned zero TotalCPU; missing accounting cannot be excluded",
                                     unit="percent")
    rss = [{"job_id": row.get("JobID"), "job_id_raw": row.get("JobIDRaw"),
            "bytes": memory_bytes(row.get("MaxRSS"))} for row in steps]
    return {"job_id": parent.get("JobID"), "job_id_raw": parent.get("JobIDRaw"),
            "job_name": parent.get("JobName") or None,
            "submit": parent.get("Submit"), "start": parent.get("Start"), "end": parent.get("End"),
            "state": parent.get("State"), "exit_code": parent.get("ExitCode"),
            "allocated_cpus": cpus, "allocated_nodes": number(parent.get("NNodes")),
            "elapsed_seconds": elapsed, "total_cpu_seconds": consumed,
            "cpu_efficiency": efficiency, "requested_memory": request,
            "allocated_memory_bytes": allocation_memory,
            "step_max_rss": rss, "memory_scope": "maximum task RSS within each step; not whole-job memory",
            "source": "sacct", "raw": parent, "steps": steps,
            "caveats": ["CPU efficiency assumes a fixed allocation and uses only the parent TotalCPU.",
                        "Accounting can be delayed or incomplete; signal termination may omit child CPU time."]}


class SlurmBackend:
    ACCOUNT_FIELDS = ("JobIDRaw", "JobID", "User", "UID", "Submit", "Start", "End", "State",
                      "ElapsedRaw", "AllocCPUS", "NNodes", "TotalCPU", "ReqMem", "MaxRSS",
                      "AllocTRES", "ExitCode", "NodeList", "Restarts", "DBIndex", "JobName")
    LIVE_FIELDS = ("JobID", "NTasks", "AveCPU", "AveRSS", "MaxRSS")

    def __init__(self, runner=run_command):
        self.run = runner
        self.user = identity()
        self.uid = os.getuid()
        self._fields = {}

    def fields(self, command):
        if command not in self._fields:
            self._fields[command] = set(re.findall(r"\b[A-Za-z][A-Za-z0-9]*\b", self.run([command, "--helpformat"])))
        return self._fields[command]

    def doctor(self):
        commands = {cmd: shutil.which(cmd) for cmd in ("python3", "squeue", "sacct", "sstat", "scontrol", "nvidia-smi")}
        result = {"user": self.user, "commands": commands, "fields": {}, "errors": []}
        for cmd in ("squeue", "sacct"):
            if not commands[cmd]:
                result["errors"].append(cmd + " unavailable; associated scheduler data cannot be queried")
        for cmd in ("sacct", "sstat"):
            if commands[cmd]:
                try:
                    result["fields"][cmd] = sorted(self.fields(cmd))
                    result[cmd + "_version"] = self.run([cmd, "--version"]).strip()
                except JobmonError as exc:
                    result["errors"].append(str(exc))
        accounting_available = False
        if "sacct" in result["fields"]:
            # Help/version exit before Slurm checks storage. Exercise the actual
            # query path for this user on this cluster, within one fixed minute.
            end = dt.datetime.now()
            start = end - dt.timedelta(minutes=1)
            try:
                self.run(["sacct", "--local", "--user=" + self.user,
                          "--starttime=" + start.strftime("%Y-%m-%dT%H:%M:%S"),
                          "--endtime=" + end.strftime("%Y-%m-%dT%H:%M:%S"),
                          "--noheader", "--parsable2", "--format=JobID%256"])
                accounting_available = True
            except JobmonError as exc:
                result["errors"].append(str(exc))
        result["accounting_storage"] = (
            "disabled" if any(accounting_disabled(error) for error in result["errors"])
            else "available" if accounting_available else "unknown")
        if result["accounting_storage"] == "disabled":
            result["errors"].append(ACCOUNTING_DISABLED_GUIDANCE)
        return result

    def jobs(self, job_id=None):
        args = ["squeue", "--local", "--array", "--noheader", "--user=" + self.user,
                "--format=%i|%u|%T|%M|%C|%D|%R|%j"]
        if job_id:
            args.append("--jobs=" + validate_job_id(job_id))
        rows = parse_rows(self.run(args), ("job_id", "user", "state", "elapsed", "cpus", "nodes",
                                          "reason_or_nodes", "job_name"), free_last=True)
        rows = [r for r in rows if r["user"] == self.user]
        return [r for r in rows if r["job_id"] == job_id] if job_id else rows

    def accounting(self, job_id, days=30):
        validate_job_id(job_id)
        if isinstance(days, bool) or not isinstance(days, int) or not 1 <= days <= 3650:
            raise JobmonError("History days must be an integer between 1 and 3650.")
        # A site without slurmdbd fails every sacct call; report the capability, not the job.
        try:
            return self._accounting(job_id, days)
        except JobmonError as exc:
            if accounting_disabled(exc):
                raise JobmonError(ACCOUNTING_DISABLED_GUIDANCE) from exc
            raise

    def _accounting(self, job_id, days):
        supported = self.fields("sacct")
        if not {"JobID", "JobIDRaw", "User", "UID"}.issubset(supported):
            raise JobmonError("sacct lacks fields required to verify job identity and ownership.")
        fields = [f for f in self.ACCOUNT_FIELDS if f in supported]
        start = (dt.datetime.now() - dt.timedelta(days=days)).strftime("%Y-%m-%dT%H:%M:%S")
        args = ["sacct", "--local", "--duplicates", "--jobs=" + job_id, "--user=" + self.user,
                "--starttime=" + start, "--endtime=now", "--noheader", "--parsable2", "--units=M",
                "--format=" + ",".join(f + "%256" for f in fields)]
        rows = parse_rows(self.run(args), fields, free_last=fields[-1] == "JobName")
        parents = [r for r in rows if "." not in r["JobID"] and
                   (r["JobID"] == job_id or ("_" not in job_id and r["JobIDRaw"] == job_id)) and
                   r["User"] == self.user and r["UID"] == str(self.uid)]
        result = []
        for parent in parents:
            siblings = [p for p in parents if p["JobIDRaw"] == parent["JobIDRaw"]]
            steps = []
            for row in rows:
                if row["JobIDRaw"].partition(".")[0] != parent["JobIDRaw"] or "." not in row["JobIDRaw"]:
                    continue
                if row["UID"] not in ("", str(self.uid)) or row["User"] not in ("", self.user):
                    continue
                # Old Slurm versions may omit step Submit; never merge ambiguous requeues.
                if len(siblings) > 1:
                    matching = [p for p in siblings if row.get("Submit") and p.get("Submit") == row["Submit"]]
                    if len(matching) != 1 or matching[0] is not parent:
                        continue
                steps.append(row)
            result.append(normalize_allocation(parent, steps))
        return result

    def live(self, job_id):
        supported = self.fields("sstat")
        if "JobID" not in supported:
            raise JobmonError("sstat does not advertise JobID.")
        fields = [f for f in self.LIVE_FIELDS if f in supported]
        args = ["sstat", "--jobs=" + validate_job_id(job_id), "--allsteps", "--noheader", "--parsable2",
                "--format=" + ",".join(f + "%256" for f in fields)]
        rows = parse_rows(self.run(args), fields)
        return rows

    def snapshot(self, job_id, days=30, live=False):
        validate_job_id(job_id)
        result = {"job_id": job_id, "queue": [], "accounting": [], "live_steps": [], "errors": []}
        for name, query in (("queue", lambda: self.jobs(job_id)),
                            ("accounting", lambda: self.accounting(job_id, days))):
            try:
                result[name] = query()
            except JobmonError as exc:
                result["errors"].append(str(exc))
        result["ownership_verified"] = bool(result["queue"] or result["accounting"])
        if live and result["queue"] and result["queue"][0]["state"] in ("RUNNING", "COMPLETING"):
            try:
                result["live_steps"] = self.live(job_id)
            except JobmonError as exc:
                result["errors"].append(str(exc))
        return result
