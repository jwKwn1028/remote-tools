# jobmon user guide

Optional Slurm CPU/memory/GPU history; Python 3.8+, standard library only.

[Setup](#setup) · [Commands](#commands) ·
[Configuration](#configure-the-remote-environment) ·
[Collection](#add-persistent-collection) · [Watch](#watch-saved-history) ·
[Archives](#fetch-and-read-offline) · [ZIP exports](#export-graphs-and-data) ·
[Dashboard](#live-browser-dashboard) · [Measurements](#what-measurements-mean) ·
[Troubleshooting](#troubleshooting)

## Setup

Cluster: Python 3.8+, `squeue`, `sacct` (optional `sstat`); site accounting for
completed jobs. Deployment: Bash, `tar`, `mktemp`, `sha256sum`, `cmp`, `readlink`.

[Configure custom Python/Slurm/storage](#configure-the-remote-environment) first.
On the workstation:

```zsh
cd path/to/your/clone
export HPC_JOBMON_HOST=your-existing-ssh-alias
source shell/jobmon.zsh
hpc-jdeploy          # preview
hpc-jdeploy --apply  # install
hpc-jstorage-init
hpc-jdoctor
hpc-jobs
```

Source the helper from private Zsh configuration; source again after updates.

### SSH destination and authentication

`HPC_JOBMON_HOST`: SSH alias or `user@login-host`; ports/keys/jumps belong in
`~/.ssh/config`. `BatchMode=yes` disables passwords (SSH 255).
Use an agent key or permitted shared connection:

```text
Host your-existing-ssh-alias
    HostName login-host
    User your-cluster-username
    ControlMaster auto
    ControlPath ~/.ssh/cm-%C
    ControlPersist 8h
```

Authenticate with `ssh your-existing-ssh-alias`; `ssh -O check`/`ssh -O exit`
with the alias checks/closes the connection. Keep settings private.

### Install directly on the cluster

From a clone/copy in the cluster account:

```sh
bash shell/deploy-jobmon.sh --local
bash shell/deploy-jobmon.sh --local --apply
. "$HOME/.config/jobmon/env.sh"
"$JOBMON_PYTHON" "$HOME/.local/libexec/jobmon/jobmon.py" report 123456 --json
```

`--local` runs identical checks without SSH. See `jobmon.py --help` and
[examples/bashrc.fragment](examples/bashrc.fragment) for direct/Bash commands.

### Deployment and upgrades

After remote changes, repeat preview/apply/doctor. Before atomically switching
`~/.local/libexec/jobmon/current`, deployment verifies the three Python files and
[wrapper](examples/monitored-job.sh) against `SHA256SUMS`, compiles Python, and
validates Bash. Failure preserves the active content-addressed release.

Python links follow `current/`; the mode-700 wrapper is
`~/.local/bin/monitored-job.sh`. Identical copies migrate; unrelated entries are
refused and bin permissions preserved. Compute nodes must see installed files.
Environment/startup files remain manual; deployment starts no jobs/collectors.

## Commands

| Command | Purpose |
| --- | --- |
| `hpc-jobs` | Queue and job names. |
| `hpc-job 123456` | Scheduler/history report with available `sstat` steps. |
| `hpc-jreport 123456` | Fresh accounting and saved-history summary. |
| `hpc-jreport 123456 --days 90 --json` | Longer lookback; JSON output. |
| `hpc-jreport 123456 --zip` | Download graphs and data. |
| `hpc-jwatch` / `hpc-jwatch 123456` | Pick a running job or watch an ID. |
| `hpc-jattach` / `hpc-jattach 123456` | Pick an unmonitored job or attach to an ID. |
| `hpc-jattach -a` | Attach all eligible unmonitored jobs. |
| `hpc-live` / `hpc-live 123456` | Dashboard for running jobs or one ID. |
| `hpc-jfetch 123456 ./downloads` | Download private history. |
| `hpc-jstorage-init` | Initialize the shared-storage probe. |
| `hpc-jdoctor` | Check Python, Slurm, wrapper, storage, and accounting. |

Use exact IDs, including array tasks (`123456_7`); steps, ranges, and heterogeneous
allocations are unsupported. `--days` is accounting lookback (1–3650, default 30),
not retention. With accounting disabled, completed jobs need saved history;
`--days` cannot recover final Slurm state.

Queries are read-only, bounded, timed, and account/job-scoped. Collection,
deployment, and deletion are never implicit.

| Timing | Default | Set with |
| --- | --- | --- |
| Wrapper samples | 60 s | `JOBMON_INTERVAL` |
| Attached samples | 60 s | `hpc-jattach --interval` |
| Viewer refresh | 5 s | `hpc-jwatch --interval` / `hpc-live --interval` |
| Attached batch/stop checks | 5 s | Fixed between samples |

Viewers read saved data without creating samples or reconfiguring collectors.

## Configure the remote environment

Create `~/.config/jobmon/env.sh` from [examples/env.sh](examples/env.sh):

```sh
export JOBMON_PYTHON="${JOBMON_PYTHON:-python3}"
export JOBMON_DATA_DIR="${JOBMON_DATA_DIR:-${XDG_STATE_HOME:-$HOME/.local/state}/jobmon}"
export JOBMON_INTERVAL="${JOBMON_INTERVAL:-60}"
```

Use directories 700/files 600, POSIX syntax, quiet module loading, and no collector
startup; check with `sh -n`. Default expressions preserve overrides.
Helpers/wrapper source the file; direct Python use must source it first.

| Remote setting | Default / meaning |
| --- | --- |
| `JOBMON_PYTHON` | `python3`; one executable, no arguments; available on login/compute nodes. |
| `JOBMON_DATA_DIR` | `${XDG_STATE_HOME:-$HOME/.local/state}/jobmon`; shared storage. |
| `JOBMON_INTERVAL` | `60` s; 1–86400, fractions allowed; fixed at job start. |
| `JOBMON_PROCESS_TREE` | Disabled; `1` enables approximate process-group sampling. |
| `JOBMON_ALLOW_OVERLAP` | `0`; `1` enables site-validated multi-node collection. |
| `JOBMON_GPU_UUIDS` | Unset; verified unique full `GPU-…` UUIDs per allocation/node. |
| `JOBMON_GPU_EXCLUSIVE` | Disabled; `1` asserts verified exclusive use without sharing/MPS/MIG. |
| `JOBMON_COLLECTOR` | `$HOME/.local/libexec/jobmon/collect.py`; wrapper-only override. |

Only exact `1` enables switches. Never reuse GPU mappings across jobs.
`SLURM_*`, `SLURMD_NODENAME`, and `JOBMON_RUN_DIR` are supplied at runtime.

Storage: absolute, owned, identical login/compute paths; directories 700, files
600, no trusted-state symlinks. `XDG_STATE_HOME` must be absolute.
Allocations verify the login probe/run directory; failure disables monitoring
while work continues. Advisory locks are required; no node-local spooling.

Workstation settings: `HPC_JOBMON_HOST` and optional `HPC_JOBMON_TOOLS_DIR`
(clone path, normally inferred from the helper).

## Add persistent collection

Preserve Slurm directives and launchers; wrap the workload:

```bash
#!/usr/bin/env bash
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=8
#SBATCH --time=00:30:00

set -euo pipefail
bash "$HOME/.local/bin/monitored-job.sh" srun your-application its-arguments
```

Do not add `srun` solely for monitoring. Process-group sampling requires work
directly in the batch group; separate `srun`/MPI steps are excluded.

For multiple commands, define persistent `RESULT_DIR` and wrap the script:

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

Put work below the block; redirect first for diagnostics. Never export the
marker at submission. Script edits cannot change submitted jobs.
Arrays/requeues have separate saved runs.

The wrapper waits up to ten seconds for a sample, then runs work regardless.
It preserves arguments, stdin, exit status, and SIGTERM/SIGINT/SIGHUP forwarding
to the launcher/visible descendants, waiting for checkpoint handlers. Launchers manage
detached/reparented processes; SIGKILL/node loss may prevent final records.

Multi-node collection needs validated step sharing/binding.
`JOBMON_ALLOW_OVERLAP=1` starts one `srun --overlap` collector per node
(one CPU, 64 MiB); otherwise multi-node coverage is missing.

## Watch saved history

### Attach to a running job

`hpc-jattach` lists unmonitored `RUNNING`, single-node jobs using the picker below.
Held locks/fresh non-final samples exclude active collectors across hosts;
old samples alone do not. Eligibility is rechecked at launch.

`-a` tries all eligible jobs, continuing after failures; any failure returns
nonzero, no candidates succeeds. Collection persists until the verified batch
process exits. `--interval`: 1–86400 seconds; defaults above.

Job cgroups are preferred; process groups need `JOBMON_PROCESS_TREE=1`.
Legacy Slurm/array allocation IDs are handled. Earlier samples, workload exits,
and GPU data are unavailable. Identity/launch failures show the private log
path and last output.

### Follow samples

`hpc-jwatch` queries Slurm once, then reads saved files over one SSH connection.
`hpc-jreport` refreshes accounting. Missing history prints a snapshot/diagnostic.

The picker lists `RUNNING`/`COMPLETING` jobs with names: `j`/`k` or arrows,
Enter to select; `q`/Esc/Ctrl-C cancels (130). One job auto-selects;
noninteractive use requires an ID.

| Column | Meaning |
| --- | --- |
| `CLOCK` | Capture time at the workstation's startup UTC offset; direct cluster use follows local time. |
| `ELAPSED` | From Slurm's inferred start, or labelled monitoring-start fallback. |
| `CPU LAST / AVG / MAX` | Latest interval, covered average, busiest interval; % of allocated cores. |
| `MEMORY / MEMORY MAX` | Current and largest sampled memory. |
| `NODE` | Multiple nodes only. |

New samples add rows; stalls warn after `max(2.5 × interval, interval + 30 s)`.
Finals stop warnings. Wrapper completion shows exit code, final-node coverage,
and sampled summary after all expected finals; node peaks stay separate.

Watch follows requeues. `--exit-on-completion` needs the newest wrapper result
and all expected finals; incomplete layouts/finals keep it open.
Viewer status remains 0 for workload failure. `--count N` bounds refreshes;
`--json` emits full snapshots. Ctrl-C stops only viewing.

## Fetch and read offline

`hpc-jfetch` downloads private `.tar.gz` history without extraction. Failure
retains `.partial` and remote/SSH status; incomplete JSONL tails are skipped/awaited.

```sh
umask 077
mkdir -m 700 saved-history
tar -xzf downloads/jobmon-123456.EXAMPLE.tar.gz -C saved-history
JOBMON_DATA_DIR="$PWD/saved-history" python3 jobmon/jobmon.py report 123456 --json
```

Offline reports require matching saved UID/username, ownership, and permissions;
otherwise report remotely or plot across accounts. `.nfs` stubs are skipped,
symlinked runs refused. Exports belong in ignored/outside paths;
history is never auto-deleted.

### Export graphs and data

```zsh
hpc-jreport 123456 --zip
hpc-jreport 123456 --zip --output-dir ./exports --days 90
```

`--zip` saves CPU/memory ZIPs per run/node to `./downloads/` and exits without a
browser/text report. `--json` is separate. Directories must be owned, private,
and non-symlink (700); ZIPs/entries use 600. No overwrites or partial ZIPs.

CLI/dashboard filenames include job ID/name, run, node, chart, and time;
metadata retains full names. Each ZIP contains `chart.svg`, `data.csv`,
`data.json` (units/metadata), and original `samples.jsonl` when available.
Image/data share a snapshot, preserving raw integers and missing values.
The default axis starts at the first sample; CLI clock labels use local
24-hour time. Saved samples and a current reporter are required for JSONL.

### Live browser dashboard

```zsh
hpc-live                        # all running jobs in one page
hpc-live 123456 --interval 5
hpc-live 123456 --no-browser     # print the URL
```

Full history updates without reloading. Each job's SSH stream queries Slurm once,
then reads files; one shared queue check runs every 30 seconds.

Without an ID, tabs include running/completing jobs, add new jobs, and retain
ended jobs. Click or use focused arrows/Home/End; run/node/axis choices persist.
One job hides tabs; an explicit ID shows only that job's history.

Axis: **Time since first sample** or local date/time. Hover/tap for values;
toggle the sidebar for full-width graphs. CPU marks 100% but permits higher use;
memory is GiB. **Elapsed** stops at the latest sample; **Monitored**/**Sample span**
label fallbacks. **Save image + data** exports the selected run/node ZIP.

Banners: green/red for wrapper success/exit code; amber for queue departure with
unknown outcome, cleared on reappearance. Tabs mark the latest run.
Failed queries, final samples, silence, or disconnection alone never prove completion.

While viewing, each newly ended run sends one popup: job name and **Elapsed**
through the latest sample. Attached jobs notify on queue departure with unknown
outcome; old completions do not replay. `notify-send` uses your desktop theme,
requests persistence until clicked, and reports delivery failures in diagnostics.
Failed sends retry on the next status update.

Workstation: Python 3.8+, current reporter. Server: loopback-only, random port
unless `--port N`. Last-tab closure has a 0.5-second grace (CLI return: 1–2 seconds).
Ctrl-C stops viewing; collection continues.

Standalone plotting without SSH:

```sh
python3 jobmon/plot.py 123456 ./downloads/jobmon-123456.EXAMPLE.tar.gz
```

## What measurements mean

Slurm determines job state; wrapper exits/finals cannot prove scheduler outcome.
Missing/stale usage stays unavailable; malformed records are diagnosed.
Match separate requeues to accounting using raw IDs/times.

### Scheduler accounting

CPU efficiency: parent `TotalCPU / (ElapsedRaw × AllocCPUS)`, fixed allocation;
never sum parent/steps. Zero CPU is ambiguous; accounting may lag/omit CPU after
signals. `MaxRSS`: maximum task RSS per step. Memory requests retain CPU/node scope.

### CPU and memory sampling

Preferred collection uses a verified cgroup v2 `job_<raw job ID>` ancestor;
v1 is unsupported. Cgroup memory includes cache.

Without job cgroups, `JOBMON_PROCESS_TREE=1` enables approximate `process_tree_pgid`
sampling. It includes reaped-child CPU but may miss separate Slurm/MPI steps,
`setsid`/`setpgid`, orphans, and short-lived work. Shared RSS counts per process.
GNU `timeout` needs `--foreground`; an empty group is unavailable, not zero.

CPU averages cover sampled allocated core-time. First samples, resets, and gaps
over 1.5 intervals are excluded from averages/maxima; latest CPU is unavailable.
Unenforced cores permit >100% use. Memory: binary text units, JSON bytes, graph GiB.

Sampled memory peaks miss between-sample spikes. Kernel `memory.peak` requires
Linux 5.19+ and cgroup v2; process-group sampling has no kernel high-water mark.

### Multi-node totals

Concurrent memory needs all expected nodes within half the shortest interval,
complete layout, and synchronized clocks. Independent node/kernel peaks are
not totals. Incomplete layouts disable totals/`all_expected_collectors_final`,
retaining per-node data.

Delayed samples can revise peaks. Buffer overflow/late arrivals set
`concurrent_memory_alignment_incomplete`; a fresh report recomputes from files.
Capture timestamps precede GPU queries to preserve alignment.

### GPU scope

Sampling needs `nvidia-smi` and verified full `GPU-…` UUIDs per allocation/node;
no ordinals/MIG UUIDs. Missing mappings disable it, diagnosing visible allocations.

Results describe shared devices unless `JOBMON_GPU_EXCLUSIVE=1` asserts exclusive
use without sharing/MPS/MIG; detected MIG still blocks attribution. Averages cover
sampled device-time. Utilization measures activity, not FLOPS.

## Troubleshooting

| Symptom | Action |
| --- | --- |
| SSH 255 | Load an agent key or [share authentication](#ssh-destination-and-authentication). |
| Missing helper/tools | Source `shell/jobmon.zsh` or set `HPC_JOBMON_TOOLS_DIR`. |
| Preflight failure | Check quiet `env.sh`, Python, checksums, and Bash syntax. |
| Deployment path conflict | Move unrelated entries aside; preview/apply. |
| Missing storage probe | Check shared paths; run `hpc-jstorage-init` and doctor. |
| No history | Check batch stderr/attachment logs for collector, module, or probe errors. |
| No cgroup / low process-group CPU | Verify plugins and launcher/group boundaries. |
| Accounting disabled / old records missing | Use history / increase `--days` where accounting works. |
| ZIP directory refused | Use an owned, non-symlink mode-700 directory. |
| Fetch left `.partial` | Diagnose transfer before removing it. |

Inspect plugins with
`scontrol show config | grep -iE 'ProctrackType|TaskPlugin|JobAcctGatherType'`.
`proctrack/pgid` with `task/none` creates no job cgroup or core enforcement.

Rollback: verify retained `SHA256SUMS`, then atomically replace `current` via a
temporary relative `releases/HASH` link and `mv -fT`. The managed wrapper needs
a release containing `monitored-job.sh`.

## Development and validation

[Validation commands](AGENTS.md#validation) use synthetic fixtures/fake SSH.
Local tools: Bash, Zsh, Ruff, ShellCheck. CI covers Python 3.8 and a current release.
External dotfile tests require `REMOTE_HELPERS_FILE`/`REMOTE_BASHRC_FILE`.
Keep private settings, history, and generated exports out of commits.

References: [Slurm accounting](https://slurm.schedmd.com/sacct.html),
[step sharing](https://slurm.schedmd.com/srun.html#OPT_overlap),
[cgroup v2](https://docs.kernel.org/admin-guide/cgroup-v2.html),
[NVIDIA queries](https://docs.nvidia.com/deploy/nvidia-smi/index.html).
