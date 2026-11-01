# jobmon user guide

Optional Slurm CPU/memory/GPU history. Python 3.8+, standard library only.

[Setup](#setup) · [Commands](#commands) ·
[Configuration](#configure-the-remote-environment) ·
[Collection](#add-persistent-collection) · [Watch](#watch) ·
[Archives](#archives-and-exports) · [Live view](#live-view) ·
[Measurements](#what-measurements-mean) · [Troubleshooting](#troubleshooting)

## Setup

Cluster: Python 3.8+, `squeue`, `sacct`; optional `sstat`.
[Configure Python](#configure-the-remote-environment) first if it needs modules or a custom path.

```zsh
cd path/to/your/clone
export HPC_JOBMON_HOST=your-existing-ssh-alias  # or user@login-host
source shell/jobmon.zsh
hpc-jdeploy          # preview
hpc-jdeploy --apply  # install; repeat after updates
hpc-jstorage-init
hpc-jdoctor
```

Source the helper from private Zsh config; keep ports, keys, and jump hosts in
`~/.ssh/config`. Batch-mode SSH needs agent keys or a permitted shared connection
(password/2FA logins fail with status 255). Authenticate once with
`ssh your-existing-ssh-alias` to share:

```text
Host your-existing-ssh-alias
    HostName login-host
    User your-cluster-username
    ControlMaster auto
    ControlPath ~/.ssh/cm-%C
    ControlPersist 8h
```

For cluster-local installation, run `bash shell/deploy-jobmon.sh --local --apply`;
see [examples/bashrc.fragment](examples/bashrc.fragment) for direct commands.

Deployment checks checksums and Python/Bash syntax before switching releases.
Failures preserve the active release; unrelated files are refused. It starts no
jobs or collectors.

## Commands

| Command | Purpose |
| --- | --- |
| `hpc-jobs` | Queue and job names. |
| `hpc-job 123456` | Scheduler and history report. |
| `hpc-jreport 123456` | Fresh accounting and saved-history summary. |
| `hpc-jreport 123456 --days 90 --json` | Longer lookback; JSON output. |
| `hpc-jreport 123456 --zip` | Export graphs and data. |
| `hpc-jwatch` / `hpc-jwatch 123456` | Pick a running job or watch an ID. |
| `hpc-jattach` / `hpc-jattach 123456` | Pick an unmonitored job or attach to an ID. |
| `hpc-jattach -a` | Attach all eligible unmonitored jobs. |
| `hpc-live` / `hpc-live 123456` | Live charts for running jobs or one ID. |
| `hpc-live --web` | Browser charts. |
| `hpc-jfetch 123456 ./downloads` | Download private history. |
| `hpc-jstorage-init` | Initialize the shared-storage probe. |
| `hpc-jdoctor` | Check Python, Slurm, wrapper, storage, and accounting. |

Use exact IDs, including array tasks (`123456_7`). `--days` is accounting lookback
(1–3650, default 30), not retention.

| Timing | Default | Set with |
| --- | --- | --- |
| Wrapper samples | 60 s | `JOBMON_INTERVAL` |
| Attached samples | 60 s | `hpc-jattach --interval` |
| Viewer refresh | 5 s | `hpc-jwatch --interval` / `hpc-live --interval` |

## Configure the remote environment

Remote config: copy [examples/env.sh](examples/env.sh) to `~/.config/jobmon/env.sh`
(file 600, directory 700); keep it POSIX and silent.

| Setting | Default / meaning |
| --- | --- |
| `JOBMON_PYTHON` | `python3`; one executable on login and compute nodes. |
| `JOBMON_DATA_DIR` | `${XDG_STATE_HOME:-$HOME/.local/state}/jobmon`; shared storage. |
| `JOBMON_INTERVAL` | `60` s; 1–86400; fixed at job start. |
| `JOBMON_PROCESS_TREE` | `1` enables approximate process-group sampling. |
| `JOBMON_ALLOW_OVERLAP` | `1` enables multi-node collection after site validation of `srun --overlap`. |
| `JOBMON_GPU_UUIDS` | Verified full `GPU-…` UUIDs per allocation and node. |
| `JOBMON_GPU_EXCLUSIVE` | `1` asserts exclusive use without sharing, MPS, or MIG. |
| `JOBMON_COLLECTOR` | Wrapper-only collector path override. |

Switches require `1`. Storage must be absolute, private, shared at the same path on
login/compute nodes, and support advisory locks. Storage failure disables monitoring;
the workload continues.

Set `HPC_JOBMON_TOOLS_DIR` only when sourcing helpers outside a clone.

## Add persistent collection

Keep the workload's launcher and Slurm directives:

```bash
#!/usr/bin/env bash
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=8
#SBATCH --time=00:30:00

set -euo pipefail
bash "$HOME/.local/bin/monitored-job.sh" srun your-application its-arguments
```

To wrap a script, put this before the work; set `RESULT_DIR` to persistent storage:

```bash
mkdir -p "$RESULT_DIR"
if [[ ${JOBMON_WRAPPED:-0} != 1 ]]; then
  exec > "$RESULT_DIR/job.out" 2> "$RESULT_DIR/job.err"
  export JOBMON_WRAPPED=1
  exec bash "$HOME/.local/bin/monitored-job.sh" bash "${BASH_SOURCE[0]}"
fi
unset JOBMON_WRAPPED
```

Monitoring failures leave the workload's arguments, stdin, signals, and exit status
intact. Keep the existing launcher; process-group sampling excludes separate
`srun`/MPI steps.

## Watch

`hpc-jattach` collects running, single-node, unmonitored jobs; `-a` tries all eligible
jobs. Earlier samples and workload exit status remain unavailable.

`hpc-jwatch` adds a row per sample and warns on stalls. No ID opens a picker
(`j`/`k`, Enter, `q`) or selects the only running job.

| Column | Meaning |
| --- | --- |
| `CLOCK` | Capture time (workstation UTC offset). |
| `ELAPSED` | Slurm elapsed, or labelled monitoring-start fallback. |
| `CPU LAST / AVG / MAX` | Latest, covered average, and busiest interval; % of allocated cores. |
| `MEMORY / MEMORY MAX` | Current and largest sampled memory. |

`--exit-on-completion` waits for wrapper exit and expected final samples;
`--count N` bounds refreshes; `--json` emits snapshots. Quitting leaves collection running.

## Archives and exports

`hpc-jfetch` saves private `.tar.gz` files; failed transfers retain `.partial`.
Read offline as the same user:

```sh
umask 077
mkdir -m 700 saved-history
tar -xzf downloads/jobmon-123456.EXAMPLE.tar.gz -C saved-history
JOBMON_DATA_DIR="$PWD/saved-history" python3 jobmon/jobmon.py report 123456 --json
```

Or plot without unpacking:

```sh
python3 jobmon/plot.py 123456 ./downloads/jobmon-123456.EXAMPLE.tar.gz
```

`hpc-jreport 123456 --zip [--output-dir DIR] [--days N]` writes CPU/memory ZIPs per
run/node (default `./downloads`, mode 700): `chart.svg`, `data.csv`, `data.json`, and
available `samples.jsonl`. History is never auto-deleted.

## Live view

```zsh
hpc-live                      # all running jobs, as tabs
hpc-live 123456 --text        # character charts
hpc-live 123456 --web         # browser; also --port N and --no-browser
```

Interactive terminal: at least 50x16. Kitty-compatible terminals (e.g. Ghostty) use
pixel charts; others, tmux, and screen use character charts.

Charts follow the newest 24 hours; inspection reaches older data. Scale and exports
cover the full run.

| Key | Action |
| --- | --- |
| `h` / `l` | Previous/next sample; first press selects newest. |
| `H` / `L` | Move 5% of the visible span. |
| Ctrl-h / Ctrl-l | Jump 25% of the visible span (up to 6 h). |
| Esc | Return to the newest day. |
| Tab / Shift-Tab | Next or previous job. * |
| `r` / `R`, `n` / `N` | Next or previous run or node. * |
| Home / End | First or last sample. * |
| `a` | Elapsed or clock axis. * |
| `s` | ZIPs for selected job/run/node/axis. * |
| `d` | Scope notes and diagnostics. * |
| `q` / Ctrl-C | Quit. * |

\* Terminal-only keys. Browser controls use tabs, menus, hover, and buttons.
Closing its last tab stops viewing; `--port`/`--no-browser` require `--web`.

Wrapper banners show exit codes; "left queue" leaves the outcome unknown. Each run
ending while viewed sends one `notify-send` popup.

## What measurements mean

Slurm determines job state; wrapper exits and final samples do not. Missing data
and gaps stay unavailable.

- **Accounting:** efficiency is parent `TotalCPU / (ElapsedRaw × AllocCPUS)`.
  Zero CPU is ambiguous; accounting can lag. `MaxRSS` is a step's largest task.
  Disabled accounting leaves completed jobs dependent on saved history.
- **Cgroup sampling:** preferred; requires a cgroup v2 job ancestor. Memory includes cache.
- **Process-group sampling** (`process_tree_pgid`): fallback without a job cgroup.
  Misses separate Slurm/MPI steps, `setsid`/`setpgid`, orphans, and short-lived work;
  counts shared pages per process. GNU `timeout` needs `--foreground`.
- **CPU:** sampled core-time only; gaps over 1.5 intervals excluded. Over 100% means
  core limits aren't enforced.
- **Memory peaks:** samples miss spikes; kernel `memory.peak` needs Linux 5.19+ and cgroup v2.
- **Multi-node:** concurrent memory needs all expected nodes within half the shortest
  interval; independent peaks are not totals.
- **GPU:** `nvidia-smi` and verified full UUIDs required. Measurements remain shared-device
  unless `JOBMON_GPU_EXCLUSIVE=1`; MIG prevents exclusive attribution.

## Troubleshooting

| Symptom | Action |
| --- | --- |
| SSH 255 | Load an agent key or [share a connection](#setup). |
| Missing helper or tools | Source `shell/jobmon.zsh` or set `HPC_JOBMON_TOOLS_DIR`. |
| Deployment fails | Check `env.sh` is quiet and Python works; move unrelated files aside. |
| Missing storage probe | Check shared paths; run `hpc-jstorage-init` and `hpc-jdoctor`. |
| No history | Check batch stderr and attachment logs. |
| No cgroup, or low process-group CPU | Check plugins and launcher boundaries. |
| Old records missing | Increase `--days` where accounting works. |
| ZIP directory refused | Use an owned, non-symlink, mode-700 directory. |
| Fetch left `.partial` | Diagnose the transfer before removing it. |

Inspect plugins with
`scontrol show config | grep -iE 'ProctrackType|TaskPlugin|JobAcctGatherType'`.

To roll back, verify a retained release's `SHA256SUMS`, then replace `current` with a
relative `releases/HASH` link using `mv -fT`.

## Development

[Validation](AGENTS.md#validation) uses synthetic data and fake SSH; keep private
settings, history, and exports out of commits.
