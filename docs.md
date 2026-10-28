# jobmon user guide

Optional CPU/memory/GPU history for Slurm, using Python 3.8+ and the standard
library. No administrator access or packages required.

[Setup](#setup) · [Commands](#commands) ·
[Configuration](#configure-the-remote-environment) ·
[Collection](#add-persistent-collection) · [Watch](#watch-saved-history) ·
[Archives](#fetch-and-read-offline) · [ZIP exports](#export-graphs-and-data) ·
[Dashboard](#live-browser-dashboard) · [Measurements](#what-measurements-mean) ·
[Troubleshooting](#troubleshooting)

## Setup

The cluster needs Python 3.8+, `squeue`, and `sacct`; `sstat` is optional.
Completed-job accounting needs site accounting storage. Deployment uses Bash,
`tar`, `mktemp`, `sha256sum`, `cmp`, and `readlink`.

[Configure the remote environment](#configure-the-remote-environment) first if
Python, Slurm, or shared storage needs custom settings. On the workstation:

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

Source the canonical helper from private Zsh configuration and again after updates.

### SSH destination and authentication

`HPC_JOBMON_HOST` accepts an SSH alias or `user@login-host`. Put ports, keys, and
jump hosts in `~/.ssh/config`. `BatchMode=yes` prevents password prompts (SSH 255);
use an agent key or, where permitted, a shared authenticated connection:

```text
Host your-existing-ssh-alias
    HostName login-host
    User your-cluster-username
    ControlMaster auto
    ControlPath ~/.ssh/cm-%C
    ControlPersist 8h
```

Log in once with `ssh your-existing-ssh-alias`. Use `ssh -O check` or `ssh -O exit`
with that alias to inspect or close the connection. Keep real settings private.

### Install directly on the cluster

From a clone/copy in the cluster account:

```sh
bash shell/deploy-jobmon.sh --local
bash shell/deploy-jobmon.sh --local --apply
. "$HOME/.config/jobmon/env.sh"
"$JOBMON_PYTHON" "$HOME/.local/libexec/jobmon/jobmon.py" report 123456 --json
```

`--local` uses the same checks without SSH. See `jobmon.py --help` for direct
commands and [examples/bashrc.fragment](examples/bashrc.fragment) for a Bash helper.

### Deployment and upgrades

After remote updates, repeat preview, apply, and doctor. Deployment verifies the
three Python files and [wrapper](examples/monitored-job.sh) against `SHA256SUMS`,
compiles Python, and checks Bash before atomically switching
`~/.local/libexec/jobmon/current` to a content-addressed release. Failed preflight
preserves the active release.

Stable Python links follow `current/`; the mode-700 wrapper is
`~/.local/bin/monitored-job.sh`. Identical copies can migrate; unrelated entries
are refused and bin permissions preserved. Compute nodes must see installed files.
Environment/startup files remain manual; deployment starts no collection or jobs.

## Commands

| Command | Purpose |
| --- | --- |
| `hpc-jobs` | Your queue and job names. |
| `hpc-job 123456` | Scheduler/history report with available `sstat` steps. |
| `hpc-jreport 123456` | Fresh accounting and saved-history summary. |
| `hpc-jreport 123456 --days 90 --json` | Longer accounting lookback; JSON output. |
| `hpc-jreport 123456 --zip` | Download graphs and data. |
| `hpc-jwatch` / `hpc-jwatch 123456` | Pick a running job or watch an explicit ID. |
| `hpc-jattach` / `hpc-jattach 123456` | Pick an unmonitored job or attach to an ID. |
| `hpc-jattach -a` | Attach to all eligible unmonitored jobs. |
| `hpc-live` / `hpc-live 123456` | Browser dashboard for running jobs or one ID. |
| `hpc-jfetch 123456 ./downloads` | Download a private history archive. |
| `hpc-jstorage-init` | Initialize the shared-storage probe. |
| `hpc-jdoctor` | Check Python, Slurm, wrapper, storage, and accounting. |

Use exact IDs, including array tasks (`123456_7`); steps, ranges, and heterogeneous
allocations are unsupported. `--days` accepts 1–3650 (default 30), affecting
accounting lookback, not history retention.

Queries are read-only, bounded, timed, and scoped to your account/job. Collection,
deployment, and deletion are never implicit. With accounting disabled, completed
jobs need saved history; `--days` cannot recover final Slurm state.

| Timing | Default | Set with |
| --- | --- | --- |
| Wrapper samples | 60 s | `JOBMON_INTERVAL` |
| Attached samples | 60 s | `hpc-jattach --interval` |
| Viewer refresh | 5 s | `hpc-jwatch --interval` / `hpc-live --interval` |
| Attached batch/stop checks | 5 s | Fixed between samples |

Refreshing reads saved data; it creates no samples or changes to running collectors.

## Configure the remote environment

Create `~/.config/jobmon/env.sh` from [examples/env.sh](examples/env.sh):

```sh
export JOBMON_PYTHON="${JOBMON_PYTHON:-python3}"
export JOBMON_DATA_DIR="${JOBMON_DATA_DIR:-${XDG_STATE_HOME:-$HOME/.local/state}/jobmon}"
export JOBMON_INTERVAL="${JOBMON_INTERVAL:-60}"
```

Use directory mode 700 and file mode 600. Keep POSIX syntax, no output or collector
startup (`sh -n` checks syntax). Load modules quietly before exports.
`${VARIABLE:-default}` preserves overrides. Helpers and the wrapper source this
file; source it yourself for direct Python commands.

| Remote setting | Default / meaning |
| --- | --- |
| `JOBMON_PYTHON` | `python3`; one executable, no arguments; must work on login/compute nodes. |
| `JOBMON_DATA_DIR` | `${XDG_STATE_HOME:-$HOME/.local/state}/jobmon`; shared storage. |
| `JOBMON_INTERVAL` | `60` seconds; 1–86400, fractions allowed; fixed at job start. |
| `JOBMON_PROCESS_TREE` | Disabled; `1` enables approximate process-group sampling. |
| `JOBMON_ALLOW_OVERLAP` | `0`; `1` enables site-validated multi-node collection. |
| `JOBMON_GPU_UUIDS` | Unset; verified unique full `GPU-…` UUIDs per allocation/node. |
| `JOBMON_GPU_EXCLUSIVE` | Disabled; `1` asserts verified exclusive use without sharing/MPS/MIG. |
| `JOBMON_COLLECTOR` | `$HOME/.local/libexec/jobmon/collect.py`; wrapper-only override. |

Only exact `1` enables switches. Never reuse GPU mappings across jobs.
`SLURM_*`, `SLURMD_NODENAME`, and `JOBMON_RUN_DIR` are supplied at runtime.

Storage must be absolute, owned by you, and shared at the same path on login and
compute nodes: directories 700, files 600, no trusted-state symlinks.
`XDG_STATE_HOME` must also be absolute. Allocations must see the login-side probe
and run directory; probe failure disables collection while work continues.
Advisory locks are required; node-local spooling is unsupported.

Local settings: `HPC_JOBMON_HOST` and optional `HPC_JOBMON_TOOLS_DIR` (clone path,
normally inferred from the sourced helper).

## Add persistent collection

Preserve existing Slurm directives and application launchers; wrap the workload:

```bash
#!/usr/bin/env bash
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=8
#SBATCH --time=00:30:00

set -euo pipefail
bash "$HOME/.local/bin/monitored-job.sh" srun your-application its-arguments
```

Do not add `srun` solely for monitoring. Process-group sampling needs the workload
directly in the batch group, excluding separate `srun`/MPI steps.

For multiple commands, define a persistent `RESULT_DIR` and wrap the whole script:

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

Put work beneath this block; redirecting first retains diagnostics. Do not export
the marker at submission. Slurm copies submitted scripts, so later edits do not
affect pending/running jobs. Arrays and requeues have separate saved runs.

The wrapper waits up to ten seconds for a sample, then runs work even if monitoring
fails. It preserves arguments, stdin, status, and SIGTERM/SIGINT/SIGHUP forwarding
to the launcher and visible descendants, waiting for checkpoint handlers.
Detached/reparented processes need launcher management; SIGKILL/node loss can
prevent final records.

Multi-node collection requires validated step sharing and binding.
`JOBMON_ALLOW_OVERLAP=1` starts one `srun --overlap` collector per node (one CPU,
64 MiB). Without opt-in, multi-node coverage is missing.

## Watch saved history

### Attach to a running job

`hpc-jattach` starts collection for your unmonitored `RUNNING`, single-node jobs.
Its picker uses the keys below. Held locks or recent non-final samples exclude
active collectors, including when locks are not visible across hosts; old samples
alone do not exclude jobs. Eligibility is rechecked at launch.

`-a` attempts all eligible jobs, continues after failures, and returns nonzero if
any fail. An empty list succeeds. Collection continues after the command returns,
until the verified original batch process exits. `--interval` sets sampling
(1–86400 seconds); defaults are above. Job cgroups are preferred; process-group
sampling requires `JOBMON_PROCESS_TREE=1`. Legacy Slurm and numeric array-allocation
IDs are handled.

Earlier samples, workload exit codes, and GPUs are unavailable when attaching.
Ambiguous identity or unsupported launching fails explicitly; errors include the
private log path and last output.

### Follow samples

`hpc-jwatch` reads Slurm once, then saved files over one SSH connection.
`hpc-jreport` refreshes accounting. Without history, watch prints a snapshot and
diagnostic. Ctrl-C stops viewing while collection continues.

The picker shows `RUNNING`/`COMPLETING` jobs: `j`/`k` or arrows, Enter to select,
`q`/Esc/Ctrl-C to cancel (130). One job selects automatically; noninteractive use
requires an ID. Job names appear when available.

| Column | Meaning |
| --- | --- |
| `CLOCK` | Capture time in the workstation's startup UTC offset; direct cluster use follows local time. |
| `ELAPSED` | Time from Slurm's inferred start, or labelled monitoring-start fallback. |
| `CPU LAST / AVG / MAX` | Latest interval, covered-interval average, busiest interval; % of allocated cores. |
| `MEMORY / MEMORY MAX` | Current and largest sampled memory. |
| `NODE` | Shown for multiple nodes. |

Rows appear only for new samples. Stalls warn after
`max(2.5 × interval, interval + 30 s)`; final samples stop warnings.
Wrapper completion shows exit code, final-node coverage, and a sampled summary
after all expected finals; node peaks remain separate.

Watch follows requeues. `--exit-on-completion` exits after the newest wrapper
completion and all expected finals; unknown layouts/missing finals keep it open.
Viewer status is 0 even for workload failure. `--count N` bounds refreshes;
`--json` emits a full snapshot each refresh, including completion.

## Fetch and read offline

`hpc-jfetch` downloads a private `.tar.gz` without extraction. Failures retain
`.partial` and the remote/SSH status; incomplete JSONL tails are skipped or awaited.

```sh
umask 077
mkdir -m 700 saved-history
tar -xzf downloads/jobmon-123456.EXAMPLE.tar.gz -C saved-history
JOBMON_DATA_DIR="$PWD/saved-history" python3 jobmon/jobmon.py report 123456 --json
```

Offline reports require matching saved UID/username, ownership, and permissions.
Otherwise report on the cluster or use the plotter, which preserves cluster
metadata across accounts. `.nfs` stubs are skipped; symlinked run directories
are refused. Keep exports ignored or outside the repository. History is never
deleted automatically.

### Export graphs and data

```zsh
hpc-jreport 123456 --zip
hpc-jreport 123456 --zip --output-dir ./exports --days 90
```

`--zip` snapshots saved history, saves CPU/memory ZIPs per run/node, and exits
without a browser or text report. Output defaults to `./downloads/`.
Directories use 700; ZIPs/entries use 600. Existing paths must be owned and
private; files are never overwritten and failures leave no partial ZIP.
`--json` is separate.

CLI/dashboard filenames include job ID/name, run, node, chart, and time; shortened
names remain complete in metadata. ZIPs contain `chart.svg`, `data.csv`,
`data.json` (units/metadata), and original `samples.jsonl` when available.
Image and data share a snapshot, preserving raw integers and missing values.
Graphs default to time since first sample; CLI clock labels use local 24-hour
time. Update older reporters for JSONL. Saved samples are required.

### Live browser dashboard

```zsh
hpc-live                        # all running jobs in one page
hpc-live 123456 --interval 5
hpc-live 123456 --no-browser     # print the URL
```

The dashboard loads full saved history and updates without reloading over one
SSH stream per job. Each stream queries Slurm once, then reads files; a shared
queue check refreshes job presence every 30 seconds, including with an explicit ID.

Without an ID, running/completing jobs appear as tabs; queue checks every
30 seconds add newly started jobs and retain finished tabs. Click or use focused
arrows/Home/End. Run/node/axis choices persist while all jobs update. One job
hides the tabs; an explicit ID shows only that job, including completed jobs.

The default axis is **Time since first sample**, with local date/time available.
Hover/tap for values and toggle the sidebar for full-width graphs. CPU marks 100%
and allows higher readings; memory is GiB. **Elapsed** ends at the latest sample
and stays fixed through gaps; **Monitored**/**Sample span** label fallback times.

Green success/red exit-code banners report wrapper completion; tabs mark the
latest run. Without a wrapper result (including attached jobs), leaving the queue
shows an amber message with final state/exit code unavailable. Queue failures,
final samples, silence, and disconnection alone never establish completion.
Reappearing jobs clear the queue message. **Save image + data** exports the
selected run/node's ZIP.

The workstation needs Python 3.8+ and the current remote reporter. The server is
loopback-only; `--port N` selects a port, otherwise one is chosen. Closing the last
browser tab has a 0.5-second grace (normal CLI return: 1–2 seconds).
Ctrl-C stops viewing; collection continues.

For a standalone plot without SSH:

```sh
python3 jobmon/plot.py 123456 ./downloads/jobmon-123456.EXAMPLE.tar.gz
```

## What measurements mean

Slurm determines job state. Wrapper exits/final samples cannot establish final
scheduler state; missing/stale usage remains unavailable. Malformed records are
diagnosed. Requeues stay separate; compare raw IDs/times when matching saved
runs to accounting restarts.

### Scheduler accounting

CPU efficiency is parent `TotalCPU / (ElapsedRaw × AllocCPUS)` for a fixed
allocation; parent and step totals are never added. Zero CPU time is ambiguous;
accounting can be delayed or omit CPU after signals. `MaxRSS` is maximum task RSS
within a step. Memory requests retain per-CPU/per-node scope.

### CPU and memory sampling

Preferred collection uses a verified cgroup v2 `job_<raw job ID>` ancestor;
v1 is unsupported. Cgroup memory includes cache, not just RSS.

Without job cgroups, `JOBMON_PROCESS_TREE=1` enables the approximate
`process_tree_pgid` scope. It includes reaped-child CPU but can miss separate
Slurm/MPI steps, `setsid`/`setpgid`, orphaned processes, and short-lived work.
Shared RSS pages count per process. GNU `timeout` needs `--foreground` to retain
the group; an empty group is unavailable, not zero.

CPU averages cover sampled allocated core-time. First samples, counter resets,
and gaps over 1.5 intervals are excluded from CPU averages/maxima and leave the
latest interval unavailable. Use may exceed 100% without core enforcement.
Text memory uses binary units; report JSON uses bytes, graph exports GiB.

Sampled memory peaks miss between-sample spikes. Kernel `memory.peak` needs
Linux 5.19+ and cgroup v2; process-group sampling has no kernel high-water mark.

### Multi-node totals

Concurrent memory needs every expected node within half the shortest interval,
a complete layout, and synchronized clocks. Independent node/kernel peaks are
never concurrent totals. Incomplete layouts retain per-node data but disable
concurrent totals and `all_expected_collectors_final`.

Delayed samples can revise peaks. Alignment-buffer overflow/late arrivals set
`concurrent_memory_alignment_incomplete`; a fresh report recomputes from files.
Capture timestamps precede GPU queries to preserve CPU/memory alignment.

### GPU scope

Sampling needs `nvidia-smi` and verified full `GPU-…` UUIDs per allocation/node;
ordinals and MIG UUIDs are unsupported. Without mappings, it is off, with a
diagnostic for visible GPU allocations.

Results describe shared devices unless `JOBMON_GPU_EXCLUSIVE=1` asserts verified
exclusive use without sharing/MPS/MIG; detected MIG still blocks attribution.
Averages cover sampled device-time. Utilization measures activity, not FLOPS.

## Troubleshooting

| Symptom | Action |
| --- | --- |
| SSH 255 | Load an agent key or [share authentication](#ssh-destination-and-authentication). |
| Missing helper/tools | Source `shell/jobmon.zsh` from the clone or set `HPC_JOBMON_TOOLS_DIR`. |
| Preflight failure | Check quiet `env.sh`, Python, checksums, and Bash syntax. |
| Deployment path conflict | Move unrelated entries aside, then preview/apply. |
| Missing storage probe | Check shared paths, run `hpc-jstorage-init`, then doctor. |
| No history | Check batch stderr or attachment startup logs for collector/module/probe errors. |
| No cgroup / low process-group CPU | Verify plugins and launcher/group boundaries. |
| Accounting disabled / old records missing | Use saved history / increase `--days` where accounting works. |
| ZIP directory refused | Use an owned, non-symlink directory with mode 700. |
| Fetch left `.partial` | Diagnose the transfer before removing it. |

Inspect plugins with
`scontrol show config | grep -iE 'ProctrackType|TaskPlugin|JobAcctGatherType'`.
`proctrack/pgid` with `task/none` creates no job cgroup or core enforcement.

Rollback: verify a retained release's `SHA256SUMS`, then replace `current` with a
temporary relative link to `releases/HASH` using `mv -fT`. Keep a release containing
`monitored-job.sh` for the managed wrapper link.

## Development and validation

See [validation commands](AGENTS.md#validation). Tests use synthetic fixtures/fake
SSH; local checks need Bash, Zsh, Ruff, and ShellCheck. CI covers Python 3.8 and a
current release. External dotfile tests require `REMOTE_HELPERS_FILE` and
`REMOTE_BASHRC_FILE`. Keep private settings, history, and generated exports out
of commits.

References: [Slurm accounting](https://slurm.schedmd.com/sacct.html),
[step sharing](https://slurm.schedmd.com/srun.html#OPT_overlap),
[cgroup v2](https://docs.kernel.org/admin-guide/cgroup-v2.html),
[NVIDIA queries](https://docs.nvidia.com/deploy/nvidia-smi/index.html).
