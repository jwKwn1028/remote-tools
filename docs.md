# jobmon user guide

Jobmon combines Slurm reports with optional allocation-local CPU, memory, and GPU
history. It needs Python 3.8+ and the standard library; no administrator access
or Python packages are required. Slurm remains authoritative for job state.

[Setup](#setup) · [Commands](#commands) · [Configuration](#configure-the-remote-environment) ·
[Collection](#add-persistent-collection) · [Watch](#watch-saved-history) ·
[Archives](#fetch-and-read-offline) · [Live dashboard](#live-browser-dashboard) ·
[Measurements](#what-measurements-mean) ·
[Troubleshooting](#troubleshooting)

## Setup

The cluster needs an existing Python 3.8+ interpreter, `squeue`, and `sacct`;
`sstat` is optional. Completed-job accounting requires site accounting storage.
Deployment uses ordinary tools including Bash, `tar`, `mktemp`, `sha256sum`,
`cmp`, and `readlink`, plus SSH for workstation installation.

If Python or Slurm needs modules or custom paths,
[configure the remote environment](#configure-the-remote-environment) first.
Then, on your workstation:

```zsh
cd path/to/your/clone
export HPC_JOBMON_HOST=your-existing-ssh-alias
source shell/jobmon.zsh
hpc-jdeploy          # preview
hpc-jdeploy --apply  # install the verified release
hpc-jdoctor
hpc-jobs
```

For future shells, source the canonical helper from your Zsh configuration.
A dotfile manager should install a thin module that reads private settings and
sources this file, rather than copying its functions. Keep real hostnames,
usernames, and storage paths in private configuration.

### SSH destination and authentication

`HPC_JOBMON_HOST` accepts an SSH config alias or `user@login-host`. Allowed
characters are letters, digits, and `_ . @ : -`; the value cannot start with
`-`. Put ports, jump hosts, and identity files in `~/.ssh/config`, rather than
passing SSH options or URLs as the destination.

Helpers use `BatchMode=yes`: password, passphrase, and two-factor prompts fail
with status 255. Load a key into `ssh-agent`, or, if site policy permits, share
an interactive connection:

```text
Host your-existing-ssh-alias
    HostName login-host
    User your-cluster-username
    ControlMaster auto
    ControlPath ~/.ssh/cm-%C
    ControlPersist 8h
```

Log in once with `ssh your-existing-ssh-alias` and complete authentication.
Use `ssh -O check your-existing-ssh-alias` to check the shared connection or
`ssh -O exit your-existing-ssh-alias` to close it.

### Install directly on the cluster

For a web-portal shell or a cluster without workstation SSH, clone or copy the
repository there and run:

```sh
bash shell/deploy-jobmon.sh --local          # preview
bash shell/deploy-jobmon.sh --local --apply
```

This installs into the current cluster account without SSH, using the same
release checks and paths. Later workstation deployments upgrade it in place.
For direct CLI use, create and source the remote environment described below,
then choose a subcommand:

```sh
. "$HOME/.config/jobmon/env.sh"
"$JOBMON_PYTHON" "$HOME/.local/libexec/jobmon/jobmon.py" jobs
"$JOBMON_PYTHON" "$HOME/.local/libexec/jobmon/jobmon.py" report 123456 --json
```

Other subcommands include `doctor`, `storage-init`, `show`, and `watch`.
[examples/bashrc.fragment](examples/bashrc.fragment) provides an optional
interactive `jobmon` function.

### Deployment and upgrades

Repeat `hpc-jdeploy`, review the preview, then run `hpc-jdeploy --apply` and
`hpc-jdoctor` after updates. Deployment includes `jobmon.py`, `slurm_backend.py`,
`collect.py`, and [examples/monitored-job.sh](examples/monitored-job.sh).
It verifies `SHA256SUMS`, byte-compiles Python, and checks wrapper Bash syntax
before atomically switching `~/.local/libexec/jobmon/current` to a
content-addressed directory under `releases/`. Failed preflight leaves the
active release unchanged.

Stable Python links target `current/FILENAME`; the wrapper link is
`~/.local/bin/monitored-job.sh -> ../libexec/jobmon/current/monitored-job.sh`.
Identical manual copies can migrate to managed links. Unrelated files,
directories, and symlinks are refused; review conflicts and move them aside
before retrying. Existing bin permissions are preserved; the wrapper is mode 700.
The wrapper and Python files must also be accessible on compute nodes.

Environment and shell startup files remain manual configuration. Deployment
starts no collectors and changes no submitted jobs.

## Commands

Run workstation commands after sourcing `shell/jobmon.zsh`:

| Command | Purpose |
| --- | --- |
| `hpc-jobs` | Your current queue, including job names. |
| `hpc-job 123456` | Scheduler/history report, plus optional live `sstat` steps. |
| `hpc-jreport 123456` | Accounting and saved history; rerun for final Slurm status. |
| `hpc-jreport 123456 --days 90 --json` | Older accounting or machine-readable output. |
| `hpc-jwatch 123456 --interval 5` | Follow saved samples; Ctrl-C stops viewing. |
| `hpc-jwatch` | Choose a running job by ID and name. |
| `hpc-jwatch 123456 --exit-on-completion` | Stop after wrapper completion and all final samples. |
| `hpc-jfetch 123456 ./downloads` | Save a private history archive locally. |
| `hpc-live` | Choose a running job and open its live CPU/memory dashboard. |
| `hpc-live 123456` | View a running or completed job from its earliest saved samples. |
| `hpc-jstorage-init` | Create the login-side shared-storage probe once. |
| `hpc-jdoctor` | Check interpreter, Slurm, wrapper, storage, and probe. |

Use exact array task IDs such as `123456_7`. Step IDs, array ranges, and
heterogeneous allocations are rejected as arguments; step records remain in
reports. Accounting searches default to 30 days; `--days N` accepts 1–3650.
Scheduler queries are read-only, bounded, timeout-limited, and scoped to your
authenticated account on the local cluster. Failures retain available saved
history with diagnostics.

Doctor reports accounting storage as `available`, `disabled`, or `unknown`
using a bounded one-minute accounting query, rather than help/version output.
Without accounting storage, completed jobs depend on previously collected
history; live queue reports still work.

Submit jobs normally with `sbatch`. Reporting and watching never start or stop
collection. No helper implicitly deploys, submits jobs, starts a login-node
daemon, or deletes history.

## Configure the remote environment

On the cluster, create `~/.config/jobmon/env.sh` using
[examples/env.sh](examples/env.sh) as a template. Minimal CPU-only settings:

```sh
export JOBMON_PYTHON="${JOBMON_PYTHON:-python3}"
export JOBMON_DATA_DIR="${JOBMON_DATA_DIR:-${XDG_STATE_HOME:-$HOME/.local/state}/jobmon}"
export JOBMON_INTERVAL="${JOBMON_INTERVAL:-60}"
```

Keep the directory mode 700 and file mode 600; check syntax with
`sh -n "$HOME/.config/jobmon/env.sh"`. The file must be quiet and POSIX-compatible,
with no output or collector startup. Put any required quiet site-module setup
before the exports. An existing environment's Python can be invoked directly;
`JOBMON_PYTHON` is one executable name or absolute path, without arguments, and
must work on login and compute nodes.

The SSH helper, deployment preflight, wrapper, and optional interactive function
source this file. Source it yourself before direct Python commands.
The `${VARIABLE:-default}` form preserves per-job overrides.

| Remote setting | Default | Meaning |
| --- | --- | --- |
| `JOBMON_PYTHON` | `python3` | Existing Python 3.8+ executable. |
| `JOBMON_DATA_DIR` | `${XDG_STATE_HOME:-$HOME/.local/state}/jobmon` | Absolute private directory shared at the same path on login and all compute nodes. |
| `JOBMON_INTERVAL` | `60` | Sampling seconds, 1–86400; fractions accepted. |
| `JOBMON_GPU_UUIDS` | Unset | Unique, comma-separated full `GPU-…` UUIDs verified for this allocation and node. |
| `JOBMON_GPU_EXCLUSIVE` | Disabled | Assert verified exclusive GPU use without sharing, MPS, or MIG. |
| `JOBMON_ALLOW_OVERLAP` | `0` | Enable site-validated multi-node collection. |
| `JOBMON_PROCESS_TREE` | Disabled | Enable approximate process-group sampling where no job cgroup exists. |
| `JOBMON_COLLECTOR` | `$HOME/.local/libexec/jobmon/collect.py` | Advanced wrapper-only override; must exist on every relevant node. |

The three switches enable behavior only for the exact value `1`; use `0` to
disable them. Other unset or empty settings use their defaults.
`JOBMON_INTERVAL` is fixed when a job starts and differs from watch refresh
`--interval`; direct collector invocations use their own `--interval`.

The data directory must be shared, owned by you, and private: directories 700,
files 600, with no symlinks for trusted state. Relative paths are rejected;
`XDG_STATE_HOME` must also be absolute when used as the fallback. After
deployment, run these on the workstation:

```zsh
hpc-jstorage-init
hpc-jdoctor
```

Initialization writes a login-side probe that every allocation must see;
collectors also check visibility of the run directory. This detects missing
shared storage, without certifying filesystem behavior. A failed probe disables
monitoring while allowing the workload to run. Node-local spooling is unsupported.

Start with one short CPU-only job and leave GPU settings unset. Set GPU UUIDs
per allocation and node after verification, rather than retaining an old list in
global configuration. The wrapper supplies `JOBMON_RUN_DIR`; Slurm supplies
`SLURM_*` and `SLURMD_NODENAME`.

Workstation settings belong in your private Zsh configuration:

| Local setting | Meaning |
| --- | --- |
| `HPC_JOBMON_HOST` | Required SSH destination; no default. |
| `HPC_JOBMON_TOOLS_DIR` | Optional clone path for deployment/plotting; normally inferred from the sourced helper, resolving symlinks. |

History range, JSON output, watch refresh/count, and completion stopping are
command options, rather than remote environment settings.

## Add persistent collection

A submitted batch script must launch the workload through the deployed wrapper
to save samples. A job submitted without it still has scheduler reports.
Keep existing Slurm directives and the application's required launcher:

```bash
#!/usr/bin/env bash
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=8
#SBATCH --time=00:30:00

set -euo pipefail
bash "$HOME/.local/bin/monitored-job.sh" srun your-application its-arguments
```

Submit with `sbatch example.sbatch`. Do not add `srun` solely for monitoring.
With a verified job cgroup, retain the existing launcher. The process-group
fallback instead requires a direct workload, without separate `srun`/MPI steps.

For a multi-command script, define a persistent `RESULT_DIR`, then re-enter the
script once through the wrapper. Place this before environment setup and work:

```bash
mkdir -p "$RESULT_DIR"
if [[ ${JOBMON_WRAPPED:-0} != 1 ]]; then
  exec > "$RESULT_DIR/job.out" 2> "$RESULT_DIR/job.err"
  export JOBMON_WRAPPED=1
  exec bash "$HOME/.local/bin/monitored-job.sh" bash "${BASH_SOURCE[0]}"
fi
unset JOBMON_WRAPPED

# Environment setup, workload, and postprocessing follow.
```

Redirecting first retains monitor diagnostics. Use another marker name if
`JOBMON_WRAPPED` conflicts with your script, and do not export it at submission.
The long-running work must remain the wrapper's child. Slurm copies scripts at
submission: changes require a new submission and cannot retrofit pending or
running jobs. Each array task collects separately.

The wrapper creates a unique run with a restart label, waits up to ten seconds
for the first sample, and runs the workload even if monitoring fails. It
preserves arguments, stdin (including closed stdin), and workload exit status.
One collector per node uses an advisory lock and a separate, flushed JSONL
stream per collector start; the shared filesystem must support advisory locks.
Never start collectors on the login node.

SIGTERM, SIGINT, and SIGHUP reach the launcher and visible local descendants.
The wrapper waits for shutdown without cutting off checkpoint handlers, forwards
further signals to survivors, and reports the first interruption as 143, 130,
or 129 respectively. Detached/reparented processes fall outside that tree; use
the workload's launcher to manage remote processes. SIGKILL and node loss cannot
guarantee final records. Monitoring failures and interruptions during bounded
monitor cleanup preserve the workload's recorded result.

Multi-node collection stays disabled until you validate the site's overlapping
steps and binding. With `JOBMON_ALLOW_OVERLAP=1`, the wrapper launches one
`srun --overlap` monitor task per node, requesting one CPU and 64 MiB per node.
Without opt-in, the workload runs with a missing-coverage diagnostic.

## Watch saved history

Watch takes one scheduler snapshot over one SSH connection, then incrementally
reads saved samples. It does not keep polling Slurm. Rerun `hpc-jreport` for
updated accounting. Without saved history, watch prints a one-shot report and
diagnostic. Ctrl-C stops the viewer while collection continues.

Without an ID, an interactive picker lists running/completing jobs by ID and
shortened name: use arrows or `j`/`k`, Enter to select, and `q`, Esc, or Ctrl-C
to cancel (status 130). One running job is selected automatically; other jobs
are counted but excluded. Options apply to the selection. The picker adds one
SSH connection; noninteractive use requires an explicit ID.

The title shows `Job 123456 (simulation)`. Names come from the initial queue,
then latest accounting, then newest named saved run; new runs record
`SLURM_JOB_NAME`. Older history remains readable, with `name unavailable` when
needed. Names stay fixed during a watch and text output escapes control characters.

Text watch prints its snapshot, diagnostics, and run scope once, then a row only
when a new sample arrives. A five-second refresh stays quiet between 60-second
samples. New collection errors appear once.

| Column | Meaning |
| --- | --- |
| `CLOCK` | Sample capture time using the workstation's UTC offset at startup; direct cluster use shows cluster local time. |
| `ELAPSED` | Runtime inferred from the initial Slurm snapshot; falls back to monitoring start when unavailable, as labelled. |
| `CPU LAST / AVG / MAX` | Latest interval, covered-interval average, and busiest interval, as percentages of allocated cores. |
| `MEMORY / MEMORY MAX` | Current and largest sampled memory readings. |
| `NODE` | Added for multi-node runs. |

A stall produces one warning after `max(2.5 × interval, interval + 30 s)`.
Final samples are marked `final` and stop stall warnings for that node.
Wrapper completion reports exit code 0 as success, nonzero exits with a log
hint, and final-node counts that update as delayed samples arrive. Unknown or
incomplete node layouts remain explicit. Once all expected final samples arrive,
watch prints sampled CPU average and memory peaks once; multi-node peaks stay
separate per node.

Watch remains open for later requeued runs by default. `--exit-on-completion`
stops when the newest visible saved run has a wrapper completion record and
all expected nodes' final samples, including if already complete at startup.
Older runs cannot stop a newer active run. Missing finals or unknown layouts
keep it open; the option neither waits for future requeues nor establishes final
Slurm state. Viewer exit status is 0 even when the workload exited nonzero.

`--count N` limits refreshes, including those without new samples.
`--json` emits a complete JSON snapshot on every refresh; completion stopping
emits the final snapshot before closing, without extra text.

## Fetch and read offline

`hpc-jfetch 123456 ./downloads` writes a private `.tar.gz` without extracting it.
Failed transfers retain a `.partial` file and the SSH/remote exit status.
Live archives may end in an unfinished JSONL record, which readers skip or await.

```sh
umask 077
mkdir -m 700 saved-history
tar -xzf downloads/jobmon-123456.EXAMPLE.tar.gz -C saved-history
JOBMON_DATA_DIR="$PWD/saved-history" python3 jobmon/jobmon.py report 123456 --json
```

Offline reports retain samples when scheduler commands are unavailable. Ownership,
permissions, and metadata are checked; saved username and UID must match the
reading account. If accounts differ, inspect raw JSON/JSONL or report on the
cluster, preserving original metadata. Use the exact fetched array task ID.

Unrelated entries beside run directories, including `.nfs` files, are skipped
and listed under `ignored_storage_entries`; symlinked run directories are refused.
Keep archives in ignored `downloads/` or outside the repository. There is no
automatic telemetry deletion; removing the wrapper from future scripts stops
future collection.

### Live browser dashboard

```zsh
hpc-live                        # same j/k, arrows, Enter picker as hpc-jwatch
hpc-live 123456 --interval 5
hpc-live 123456 --no-browser     # print the local URL for manual opening
```

The command opens a local browser dashboard with a TokyoNight Storm theme.
It loads the complete saved history from the beginning of each run, then appends
new readings over one SSH connection. Samples from before the command started
are included; time before collection began stays unmeasured. Slurm is queried
once, while subsequent refreshes only read saved files.

Deploy the updated reporter once with `hpc-jdeploy` and `hpc-jdeploy --apply`.
The dashboard needs an existing local `python3` (3.8+), without extra packages.
Its server binds only to loopback on a free port; `--port N` selects a port.
Close the last dashboard tab or press Ctrl-C to stop viewing; collection continues.
Tab closure has a 0.5-second grace period; normal CLI return takes about 1–2 seconds,
including disconnect detection and SSH cleanup. Reloading within the grace keeps it open.
`--interval` changes file refreshes (default 5 seconds), not the job's sampling rate.

Choose a run and node, switch between local clock time and time since the first
sample, and hover or tap for values. CPU shows latest, average, and maximum usage;
memory shows current and sampled peak GiB. Gaps remain gaps, process-group scope
stays labelled, and nodes/requeues stay separate. New data updates automatically;
disconnections keep existing plots visible with a diagnostic. Use an explicit ID
for completed jobs. Each **Save image + data** button downloads one ZIP for the
selected run/node: `chart.svg`, `data.csv`, `data.json` (units and run metadata),
and `samples.jsonl` (original collector records). Image and data share one snapshot;
missing values stay blank/null. Update the reporter to include original JSONL samples;
older reporters still export the plotted values as CSV/JSON. Keep exports private.
ZIP filenames include the job ID and sanitized job name, followed by run, node,
chart, and export time. Long names are shortened; JSON metadata retains full names.

To plot an existing archive without SSH:

```sh
python3 jobmon/plot.py 123456 ./downloads/jobmon-123456.EXAMPLE.tar.gz
```

Plotting preserves cluster-account metadata and works when local/remote usernames
or UIDs differ. Unlike the offline report reader, it does not require them to match.

## What measurements mean

Unavailable or stale data never becomes guessed zero usage. Reports retain raw
scheduler records, distinct allocations and collected runs, scope, coverage,
missing nodes, gaps, final status, and errors. Malformed samples are skipped with
diagnostics without discarding valid records. Saved runs are not automatically
matched to accounting restarts: compare raw IDs, submit/start times, and restart
labels for requeues. Wrapper completion and final samples do not prove final
scheduler state.

### Scheduler accounting

CPU efficiency is parent `TotalCPU / (ElapsedRaw × AllocCPUS)`, assuming a fixed
allocation. Parent and step totals are never added. Zero `TotalCPU` is ambiguous,
and accounting can arrive late or omit CPU time after signals.

`MaxRSS` is a task maximum within a step, rather than whole-job memory.
Requests retain per-CPU/per-node scope; missing or unreadable allocation data
does not produce an invented percentage or scope.

### CPU and memory sampling

Preferred collection uses a visible cgroup v2 `job_<raw job ID>` ancestor found
through `/proc`. Cgroup v1 is unsupported. Without a verified boundary, CPU and
memory stay unavailable unless the process-group approximation is enabled.
Cgroup memory includes cache charges and is not RSS.

`JOBMON_PROCESS_TREE=1` enables `process_tree_pgid` where no job cgroup exists.
It sums the collector's process group, excluding itself, and includes reaped
children's CPU time. Separate Slurm/MPI steps, `setsid`/`setpgid`, orphaned
processes, and short-lived unsampled work can escape it; summed RSS double-counts
shared pages. GNU `timeout` creates a new group unless given `--foreground`.
Detected launchers produce an understatement warning; an empty group is
unavailable, rather than zero. This scope is always labelled as approximate.

CPU averages use allocated core-time over covered intervals. First samples,
counter resets, and gaps longer than 1.5 sampling intervals leave the latest
interval unavailable and are excluded from averages and maxima. Without core
enforcement (for example `TaskPlugin=task/none`), real use can exceed 100%.
Reports also show busy-core counts. Text uses binary memory units; JSON keeps bytes.

The **sampled memory peak** is the largest observed reading and misses spikes
between samples. A shorter interval improves resolution at the cost of more
records. The separate **kernel high-water mark**, cgroup `memory.peak`, captures
the lifetime peak independently of sampling, requires Linux 5.19+ with cgroup v2,
and remains unavailable where unsupported. Process-group sampling has no kernel
mark. Neither substitutes for the other.

### Multi-node totals

Concurrent memory needs valid readings from every expected node within half
the shortest sampling interval, a complete node layout matching the declared
count, and synchronized clocks. Independent node peaks and kernel high-water
marks are never summed as concurrent usage. Incomplete layouts retain per-node
readings, but concurrent totals are unavailable and
`all_expected_collectors_final` remains false.

Timestamps precede GPU queries, so slow GPU reads do not shift CPU/memory alignment.
Delayed samples may revise concurrent peaks. If the watch's bounded alignment buffer
overflows or data arrives behind a committed time range,
`concurrent_memory_alignment_incomplete` is set with a diagnostic. A fresh report
recomputes alignment from saved files; independent node peaks are unaffected.

### GPU scope

GPU sampling requires existing `nvidia-smi` and verified full `GPU-…` UUIDs
for each allocation/node. Numeric ordinals and MIG UUIDs are unsupported.
Without mapping, GPU sampling is off; visible allocations get a mapping diagnostic.

Measurements default to shared-device context, with job utilization unavailable.
Set `JOBMON_GPU_EXCLUSIVE=1` only after verifying exclusive use without sharing,
MPS, or MIG; detected MIG still prevents exclusive attribution. Averages cover
observed device-time, rather than claiming full-job coverage. Utilization measures
device activity, not theoretical FLOPS.

## Troubleshooting

| Symptom | Action |
| --- | --- |
| SSH status 255 or permission denied | Load an agent key or [share an authenticated connection](#ssh-destination-and-authentication). |
| Cannot find deployment script | Source the helper from the clone, or set `HPC_JOBMON_TOOLS_DIR`. |
| Deployment preflight fails | Check quiet `env.sh`, interpreter, checksums, and Bash syntax; the previous release stays active. |
| Deployment path conflict | Review the unrelated file/link and move it aside; keep custom wrappers at another path. |
| Doctor finds no executable wrapper | Resolve path conflicts, then preview and apply deployment again. |
| Missing storage probe | Check the shared data path; run `hpc-jstorage-init` on the workstation or direct `storage-init` on the login node. |
| Workload runs without history | Read batch stderr for probe, collector-path, interpreter, or compute-node module errors. |
| No verified cgroup | Check site plugins and cgroup visibility; consider the labelled process-group fallback only where appropriate. |
| Process-group CPU is too low | Keep the workload in the batch process group; separate launchers/groups escape measurement. |
| Accounting storage disabled | Completed jobs require saved history; a larger `--days` cannot help. |
| Older job missing | Increase `--days` if accounting storage is available. |
| Fetch left a `.partial` file | Diagnose the SSH/remote failure before removing it. |

Inspect site plugins with
`scontrol show config | grep -iE 'ProctrackType|TaskPlugin|JobAcctGatherType'`.
Sites using `proctrack/pgid` with `task/none` create no job cgroup and do not
enforce allocated core limits.

For rollback, choose a retained release under
`~/.local/libexec/jobmon/releases/`, verify its `SHA256SUMS`, create a temporary
relative symlink to `releases/HASH`, and replace `current` with `mv -fT`.
Choose a release containing `monitored-job.sh`; older Python-only releases leave
the managed wrapper link dangling.

## Development and validation

See [AGENTS.md](AGENTS.md) for the repository map and contribution contracts.
Run:

```sh
python3 -m unittest discover -s tests -v
zsh -n shell/jobmon.zsh
bash -n shell/deploy-jobmon.sh
bash -n examples/monitored-job.sh
bash -n examples/env.sh
bash -n examples/bashrc.fragment
shellcheck shell/deploy-jobmon.sh examples/monitored-job.sh examples/env.sh examples/bashrc.fragment
ruff check jobmon tests
```

Tests use synthetic fixtures and fake SSH; no cluster is required. Local tests
need Bash and Zsh, with Ruff and ShellCheck for linting. CI checks Python 3.8 and
a current release. External dotfile tests run only when `REMOTE_HELPERS_FILE`
and `REMOTE_BASHRC_FILE` are supplied. Compatibility and publication tests guard
the Python floor and private-data leaks; keep local configuration and archives
out of commits.

References: [Slurm accounting](https://slurm.schedmd.com/sacct.html),
[Slurm step sharing](https://slurm.schedmd.com/srun.html#OPT_overlap),
[kernel cgroup v2](https://docs.kernel.org/admin-guide/cgroup-v2.html), and
[NVIDIA queries](https://docs.nvidia.com/deploy/nvidia-smi/index.html).
