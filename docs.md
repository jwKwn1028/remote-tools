# jobmon user guide

Jobmon adds optional CPU, memory, and GPU history to Slurm reports. It uses
Python 3.8+ and the standard library; no administrator access or packages are
required. Slurm is authoritative for job state; missing usage stays unavailable.

[Setup](#setup) · [Commands](#commands) ·
[Configuration](#configure-the-remote-environment) ·
[Collection](#add-persistent-collection) · [Watch](#watch-saved-history) ·
[Archives](#fetch-and-read-offline) · [ZIP exports](#export-graphs-and-data) ·
[Dashboard](#live-browser-dashboard) · [Measurements](#what-measurements-mean) ·
[Troubleshooting](#troubleshooting)

## Setup

The cluster needs Python 3.8+, `squeue`, and `sacct`; `sstat` is optional.
Completed-job accounting requires site accounting storage. Deployment also uses
Bash, `tar`, `mktemp`, `sha256sum`, `cmp`, and `readlink`.

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

Source this helper from your private Zsh configuration for future shells, and
source it again after local updates. Dotfile managers should source the canonical
file instead of copying functions. Keep real hostnames and paths private.

### SSH destination and authentication

`HPC_JOBMON_HOST` accepts an SSH alias or `user@login-host`. Put ports, jump
hosts, and identity files in `~/.ssh/config`. Helpers use `BatchMode=yes`, so
interactive authentication prompts fail with status 255. Use an agent key or,
where site policy permits, share an authenticated connection:

```text
Host your-existing-ssh-alias
    HostName login-host
    User your-cluster-username
    ControlMaster auto
    ControlPath ~/.ssh/cm-%C
    ControlPersist 8h
```

Log in once with `ssh your-existing-ssh-alias`. Use `ssh -O check` or
`ssh -O exit` with the same alias to check or close the shared connection.

### Install directly on the cluster

Clone/copy the repository to a portal shell or cluster account, then run:

```sh
bash shell/deploy-jobmon.sh --local
bash shell/deploy-jobmon.sh --local --apply
. "$HOME/.config/jobmon/env.sh"
"$JOBMON_PYTHON" "$HOME/.local/libexec/jobmon/jobmon.py" report 123456 --json
```

This uses the same installation checks without workstation SSH. Direct CLI
subcommands include `jobs`, `doctor`, `storage-init`, `show`, `report`, and `watch`.
An optional interactive function is in
[examples/bashrc.fragment](examples/bashrc.fragment).

### Deployment and upgrades

Repeat preview, apply, and doctor after remote code updates. Deployment verifies
`SHA256SUMS` for the three remote Python files and
[monitored-job.sh](examples/monitored-job.sh), compiles Python, and checks Bash
syntax before atomically switching `~/.local/libexec/jobmon/current` to a
content-addressed `releases/` directory. Failed preflight preserves the active
release.

Stable Python links follow `current/`; the mode-700 wrapper is
`~/.local/bin/monitored-job.sh`. Identical manual copies can migrate; unrelated
files or links are refused. Existing bin permissions are preserved. Sources and
wrapper must be accessible on compute nodes. Environment/startup files remain
manual; deployment starts no collection and changes no submitted jobs.

## Commands

| Command | Purpose |
| --- | --- |
| `hpc-jobs` | Your queue, including names. |
| `hpc-job 123456` | Scheduler/history report with available live `sstat` steps. |
| `hpc-jreport 123456` | Fresh accounting and saved-history summary. |
| `hpc-jreport 123456 --days 90 --json` | Longer accounting lookback; JSON output. |
| `hpc-jreport 123456 --zip` | Download dashboard-format graphs and data. |
| `hpc-jwatch` | Choose a running job and watch saved samples. |
| `hpc-jwatch 123456 --interval 5` | Watch an explicit job. |
| `hpc-live` | All running jobs in one browser dashboard. |
| `hpc-live 123456` | Dashboard for one running or completed job. |
| `hpc-jfetch 123456 ./downloads` | Download a private history archive. |
| `hpc-jstorage-init` | Initialize the shared-storage probe. |
| `hpc-jdoctor` | Check Python, Slurm, wrapper, storage, and accounting. |

Use exact IDs, including array tasks such as `123456_7`. Step IDs, array ranges,
and heterogeneous allocations are rejected. `--days` accepts 1–3650, defaults
to 30, and affects accounting lookback, not saved-data retention.

Queries are read-only, bounded, timed, and scoped to your account/job. Helpers
never implicitly submit jobs, deploy, start login-node collectors, or delete
history. Doctor reports accounting as `available`, `disabled`, or `unknown`.
With accounting disabled, completed jobs require saved history and final Slurm
state may be unavailable; increasing `--days` cannot help.

## Configure the remote environment

Create `~/.config/jobmon/env.sh` on the cluster from
[examples/env.sh](examples/env.sh). Minimal CPU-only settings:

```sh
export JOBMON_PYTHON="${JOBMON_PYTHON:-python3}"
export JOBMON_DATA_DIR="${JOBMON_DATA_DIR:-${XDG_STATE_HOME:-$HOME/.local/state}/jobmon}"
export JOBMON_INTERVAL="${JOBMON_INTERVAL:-60}"
```

Use mode 700 for the directory and 600 for the file. Keep it quiet,
POSIX-compatible, and free of collector startup; check with `sh -n`. Load any
required site modules quietly before the exports. The `${VARIABLE:-default}`
form preserves per-job overrides. Helpers and the wrapper source this file;
source it yourself for direct Python commands.

| Remote setting | Default / meaning |
| --- | --- |
| `JOBMON_PYTHON` | `python3`; one existing executable/path, without arguments, working on login and compute nodes. |
| `JOBMON_DATA_DIR` | `${XDG_STATE_HOME:-$HOME/.local/state}/jobmon`; shared history storage. |
| `JOBMON_INTERVAL` | `60` seconds; 1–86400, fractions allowed; fixed at job start. |
| `JOBMON_PROCESS_TREE` | Disabled; `1` enables the approximate process-group fallback. |
| `JOBMON_ALLOW_OVERLAP` | `0`; `1` enables site-validated multi-node collection. |
| `JOBMON_GPU_UUIDS` | Unset; verified, unique, comma-separated full `GPU-…` UUIDs for this allocation/node. |
| `JOBMON_GPU_EXCLUSIVE` | Disabled; `1` asserts verified exclusive use without sharing, MPS, or MIG. |
| `JOBMON_COLLECTOR` | `$HOME/.local/libexec/jobmon/collect.py`; advanced wrapper-only override. |

Only the exact value `1` enables switches. GPU mappings belong to each verified
allocation/node, not a reusable global list. Slurm and the wrapper supply
`SLURM_*`, `SLURMD_NODENAME`, and `JOBMON_RUN_DIR`.

Storage must be absolute, shared at the same path on login/compute nodes, and
owned by you: directories 700, files 600, no trusted-state symlinks.
`XDG_STATE_HOME` must also be absolute. `hpc-jstorage-init` writes a
login-side probe that allocations must see; collectors also check run-directory
visibility. A failed probe disables monitoring while preserving the workload.
Shared storage must support advisory locks; node-local spooling is unsupported.

Local settings are `HPC_JOBMON_HOST` and optional `HPC_JOBMON_TOOLS_DIR`
(clone path, normally inferred from the sourced helper). CLI `--interval` changes
viewer refresh, not sampling; direct collector calls use their own interval.

## Add persistent collection

Submit normally with `sbatch`, but launch the workload through the wrapper to
save samples. Preserve your Slurm directives and required application launcher:

```bash
#!/usr/bin/env bash
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=8
#SBATCH --time=00:30:00

set -euo pipefail
bash "$HOME/.local/bin/monitored-job.sh" srun your-application its-arguments
```

Do not add `srun` solely for monitoring. With process-group sampling, run the
workload directly instead of through separate `srun`/MPI steps.

For a multi-command script, define a persistent `RESULT_DIR` and put this before
environment setup and work:

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

Keep the work beneath the wrapper; redirecting first retains its diagnostics.
Do not export the marker at submission. Slurm copies scripts when submitted:
edits cannot retrofit pending/running jobs. Array tasks and requeues have separate
saved runs.

The wrapper waits up to ten seconds for the first sample, then runs the workload
even if monitoring fails. It preserves arguments, stdin, signals, and workload
status. SIGTERM/SIGINT/SIGHUP reach the launcher and visible local descendants;
shutdown waits preserve checkpoint handlers. Detached/reparented processes need
launcher management. SIGKILL or node loss can prevent final records.

Multi-node collection requires validated overlapping-step behavior and binding.
`JOBMON_ALLOW_OVERLAP=1` starts one `srun --overlap` collector per node, requesting
one CPU and 64 MiB each. Without opt-in, the workload runs with missing coverage.

## Watch saved history

`hpc-jwatch` reads Slurm once, then follows saved files over one SSH connection.
Rerun `hpc-jreport` for fresh accounting. Without history, watch prints a one-shot
report and diagnostic. Ctrl-C stops viewing while collection continues.

The no-ID picker includes `RUNNING`/`COMPLETING` jobs: arrows or `j`/`k` to move,
Enter to select, `q`/Esc/Ctrl-C to cancel (130). One job selects automatically;
noninteractive use requires an ID. The title includes the job name when known.

| Column | Meaning |
| --- | --- |
| `CLOCK` | Sample capture time in the workstation's startup UTC offset; direct cluster use follows its local time. |
| `ELAPSED` | Time from the initial Slurm start estimate, or labelled monitoring-start fallback. |
| `CPU LAST / AVG / MAX` | Latest interval, covered-interval average, and busiest interval; % of allocated cores. |
| `MEMORY / MEMORY MAX` | Current and largest sampled memory. |
| `NODE` | Included for multiple nodes. |

Text rows appear only for new samples. Stalls warn after
`max(2.5 × interval, interval + 30 s)`; final samples stop that node's warnings.
Wrapper completion shows exit code, final-node coverage, and a sampled summary
once all expected finals arrive. Node peaks remain separate.

Watch stays open for requeues. `--exit-on-completion` exits after the newest run's
wrapper completion and all expected node finals; unknown layouts or missing
finals keep it open. Viewer exit status is 0 even for a nonzero workload exit.
`--count N` limits refreshes; `--json` emits a full snapshot each refresh,
including the last one before completion exit.

## Fetch and read offline

`hpc-jfetch` downloads a private `.tar.gz` without extraction. Failed transfers
retain `.partial` and the remote/SSH status; unfinished JSONL tails are skipped
or awaited.

```sh
umask 077
mkdir -m 700 saved-history
tar -xzf downloads/jobmon-123456.EXAMPLE.tar.gz -C saved-history
JOBMON_DATA_DIR="$PWD/saved-history" python3 jobmon/jobmon.py report 123456 --json
```

Offline reports check ownership, permissions, and metadata; saved UID/username
must match the reading account. Otherwise report on the cluster or use the
plotter below, which preserves cluster metadata without requiring account matches.
Unrelated entries such as `.nfs` files are diagnosed and skipped; symlinked run
directories are refused. Keep downloads ignored or outside the repository.
History is never deleted automatically.

### Export graphs and data

```zsh
hpc-jreport 123456 --zip
hpc-jreport 123456 --zip --output-dir ./exports --days 90
```

`--zip` takes one saved-history snapshot and exits without a browser or text
report. It saves CPU and memory ZIPs for every run/node in `./downloads/`, or
`--output-dir`. Existing directories must be private and owned by you; new ones
use 700. ZIPs and entries use 600; existing files survive and failed writes leave
no partial ZIP. `--json` is a separate mode.

CLI and dashboard exports share filenames containing job ID, sanitized job name,
run, node, chart, and time. Long names shorten; full names remain in metadata.
Each ZIP contains `chart.svg`, `data.csv`, `data.json` (units/run metadata), and
`samples.jsonl` when original records are available. Image and data share a
snapshot, preserve raw integers, and retain missing values as blank/null.
CLI clock labels use local 24-hour time. Older reporters export CSV/JSON;
update the reporter for original JSONL. ZIP graphs default to time since first
sample. Saved samples are required even when
accounting is disabled.

### Live browser dashboard

```zsh
hpc-live                        # all running jobs in one page
hpc-live 123456 --interval 5
hpc-live 123456 --no-browser     # print the URL
```

The local TokyoNight Storm dashboard loads each run from its earliest saved
sample, then updates in place over one SSH sample stream per job. Each stream
reads Slurm once, then refreshes saved files.

Without an ID, current `RUNNING`/`COMPLETING` jobs appear as in-page tabs. Click
or use focused-tab arrows/Home/End. Tabs preserve run, node, and time-axis choices
while all jobs update. Your queue is checked every 30 seconds; newly running jobs
appear without a page reload, and finished tabs remain available. Pending jobs
appear after they start. One job hides the tab bar. An explicit ID monitors only
that job, including completed jobs.

The default axis is **Time since first sample**; local date/time is also available.
Hover/tap for values. Toggle the
sidebar to hide/restore controls; graphs fill the available width. CPU includes a 100% reference and allows
higher readings; memory is GiB. **Elapsed** ends at the latest sample and stays
fixed through gaps; **Monitored** and **Sample span** label fallback start times.

A green **Workload finished (exit 0)** or red exit-code banner reports wrapper
completion; tabs mark the latest run. Graphs/exports remain available and new
requeues clear the message. Final samples, silence, or disconnection alone do not
establish completion. Use **Save image + data** for the selected run/node's ZIP.

The workstation needs Python 3.8+ and the current remote reporter. The server is
loopback-only: `--port N` selects a port, otherwise one is chosen. Closing the last
browser tab has a 0.5-second grace; normal CLI return takes about 1–2 seconds.
Ctrl-C also stops viewing, while collection continues. Viewer `--interval`
defaults to 5 seconds and does not change sampling.

For a standalone plot without SSH:

```sh
python3 jobmon/plot.py 123456 ./downloads/jobmon-123456.EXAMPLE.tar.gz
```

## What measurements mean

Saved data supplements Slurm. Wrapper exits and final collector samples do not
establish final scheduler state. Missing/stale usage stays unavailable, malformed
records are diagnosed, and requeues remain separate. Saved runs are not
automatically matched to accounting restarts: compare raw IDs and times.

### Scheduler accounting

CPU efficiency is parent `TotalCPU / (ElapsedRaw × AllocCPUS)` for a fixed
allocation; parent and step totals are never added. Zero CPU time is ambiguous;
accounting may arrive late or omit CPU after signals. `MaxRSS` is the maximum
task within a step. Memory requests retain per-CPU/per-node scope.

### CPU and memory sampling

Preferred collection uses a verified cgroup v2 `job_<raw job ID>` ancestor.
Cgroup v1 is unsupported. Cgroup memory includes cache charges, not just RSS.

`JOBMON_PROCESS_TREE=1` enables the labelled `process_tree_pgid` approximation
where no job cgroup exists. It includes reaped children's CPU but can miss
separate Slurm/MPI steps, `setsid`/`setpgid`, orphaned processes, and short-lived
work between samples; shared RSS pages count per process. GNU `timeout` needs
`--foreground` to retain the group. An empty group is unavailable, not zero.

CPU averages cover sampled allocated core-time. First samples, counter resets,
and gaps beyond 1.5 intervals leave the latest CPU unavailable and are excluded
from averages/maxima. Use can exceed 100% without core enforcement.
Text memory uses binary units; JSON uses bytes, except graph exports use GiB
with units recorded.

Sampled memory peaks miss between-sample spikes. The separate cgroup
`memory.peak` kernel high-water mark needs Linux 5.19+ and cgroup v2;
process-group sampling has no kernel mark.

### Multi-node totals

Concurrent memory requires every expected node within half the shortest sample
interval, a complete layout, and synchronized clocks. Independent node/kernel
peaks are never summed as concurrent usage. Incomplete layouts keep per-node
data, but concurrent totals are unavailable and `all_expected_collectors_final`
stays false.

Delayed samples may revise concurrent peaks. Overflow/late arrivals in watch's
bounded alignment buffer set `concurrent_memory_alignment_incomplete`; a fresh
report recomputes from files. Timestamps precede GPU queries to avoid GPU delays
shifting CPU/memory alignment.

### GPU scope

GPU sampling needs `nvidia-smi` and verified full `GPU-…` UUIDs per allocation/node.
Ordinals and MIG UUIDs are unsupported. Without mapping it is off, with a
diagnostic for visible GPU allocations.

Default results describe the shared device. `JOBMON_GPU_EXCLUSIVE=1` permits job
attribution only after verified exclusive use without sharing, MPS, or MIG;
detected MIG still blocks it. Averages cover sampled device-time. Utilization is
device activity, not theoretical FLOPS.

## Troubleshooting

| Symptom | Action |
| --- | --- |
| SSH 255 / authentication failure | Load an agent key or [share authentication](#ssh-destination-and-authentication). |
| Helper command missing | Source `shell/jobmon.zsh` again. |
| Deployment files not found | Source from the clone or set `HPC_JOBMON_TOOLS_DIR`. |
| Preflight fails | Check quiet `env.sh`, Python, checksums, and Bash syntax; the active release is preserved. |
| Deployment/wrapper path conflict | Move the unrelated entry aside, then preview/apply again. |
| Storage probe missing | Check the shared path, run `hpc-jstorage-init`, then doctor. |
| Workload has no history | Check batch stderr for probe, collector, interpreter, or module errors. |
| No cgroup / low process-group CPU | Verify site plugins and workload launcher/group boundaries. |
| Accounting disabled | Saved history is needed; `--days` cannot restore accounting. |
| Older accounting missing | Increase `--days` where accounting is available. |
| ZIP destination refused | Use an owned, non-symlink directory with mode 700. |
| Fetch left `.partial` | Diagnose the transfer failure before removing it. |

Inspect plugins with
`scontrol show config | grep -iE 'ProctrackType|TaskPlugin|JobAcctGatherType'`.
`proctrack/pgid` with `task/none` creates no job cgroup or core enforcement.

For rollback, verify a retained release's `SHA256SUMS`, create a temporary relative
link to `releases/HASH`, then replace `current` with `mv -fT`. Choose a release
containing `monitored-job.sh` to keep the managed wrapper link valid.

## Development and validation

Run the [validation commands](AGENTS.md#validation). Tests use synthetic fixtures
and fake SSH; local checks need Bash, Zsh, Ruff, and ShellCheck. CI covers Python
3.8 and a current release. External dotfile tests require `REMOTE_HELPERS_FILE`
and `REMOTE_BASHRC_FILE`. Keep private settings, archives, and generated exports
out of commits.

References: [Slurm accounting](https://slurm.schedmd.com/sacct.html),
[step sharing](https://slurm.schedmd.com/srun.html#OPT_overlap),
[cgroup v2](https://docs.kernel.org/admin-guide/cgroup-v2.html),
[NVIDIA queries](https://docs.nvidia.com/deploy/nvidia-smi/index.html).
