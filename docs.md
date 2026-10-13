# jobmon user guide

Personal Slurm monitoring using **Python 3.8+ and the standard library**.
Neither `jobstats` nor `seff` is required. There are no pip, Conda, system-package,
Prometheus, or administrator setup steps.

This guide covers first-time setup, normal daily use, persistent collection,
measurement semantics, and validation. Live cluster behavior still depends on
site Slurm, cgroup, GPU, and shared-filesystem configuration.

The initial rollout described here is CPU-only. Leave `JOBMON_GPU_UUIDS` and
`JOBMON_GPU_EXCLUSIVE` unset. The GPU sections document a later, explicit opt-in
after device ownership and site behavior have been verified.

The repository helper is the canonical workstation interface. A dotfile manager
should install a small module that sets private local values and sources
`shell/jobmon.zsh`; do not maintain a copied implementation:

```zsh
cd path/to/your/clone
export HPC_JOBMON_HOST=your-existing-ssh-alias
source shell/jobmon.zsh
hpc-jdeploy            # preview
hpc-jdeploy --apply    # verified, versioned activation of Python tools and wrapper
# Create ~/.config/jobmon/env.sh as described below, then:
hpc-jstorage-init      # one-time login-side shared-storage marker
hpc-jdoctor
```

Set the host through your local environment or private dotfile data. Do not
commit a real SSH alias, username, cluster address, or storage path to this
repository.

## Start with scheduler reports

On your workstation, set the SSH destination: an alias from `~/.ssh/config`, or
`user@login-host` (see [Choosing the SSH destination](#choosing-the-ssh-destination)):

```zsh
cd path/to/your/clone
export HPC_JOBMON_HOST=your-existing-ssh-alias
source shell/jobmon.zsh

# Read-only preview: shows host, four sources, release directory, and wrapper link.
bash shell/deploy-jobmon.sh
# Validate, compile, and activate a versioned release on that host.
bash shell/deploy-jobmon.sh --apply

hpc-jstorage-init
hpc-jdoctor
hpc-jobs
hpc-job 123456
hpc-jreport 123456 --json
hpc-jreport 123456_7 --days 90
```

The remote requirements for scheduler reports are `squeue`, `sacct`, and an
existing Python 3.8+ interpreter. `sacct` additionally requires the site to run
accounting storage; where it is disabled, completed jobs have no scheduler
record at all and only the opt-in collector retains their usage.
`sstat` adds an optional live step snapshot.
Verified deployment also expects standard workstation/cluster tools `ssh`,
`tar`, `mktemp`, and `sha256sum`, plus remote Bash, `cmp`, and `readlink`.
Installing on the cluster itself with `--local` needs no `ssh`.
Commands and accounting fields are checked at runtime; unavailable data carries
diagnostics. Scheduler failures do not prevent reading your private saved history.
No helper installs or deploys anything implicitly.

### Choosing the SSH destination

`HPC_JOBMON_HOST`, or the installer's first argument, is passed to `ssh` as the
destination. It may be an alias from `~/.ssh/config`, or a plain
`user@login-host` when the default port and your key already work; no alias is
required. Only letters, digits, and `_ . @ : -` are accepted, and the value may
not begin with `-`, so ports, SSH options, and `ssh://` URLs cannot be passed
through it. Put a non-default port, a jump host, or a specific identity file in a
`Host` block of `~/.ssh/config` and use that block's name.

### Password and two-factor logins

The helpers and the installer run `ssh` with `BatchMode=yes`, so they never stop
at a password, passphrase, or one-time-code prompt. Such a login fails at once
with SSH exit status 255. A passphrase-protected key loaded into `ssh-agent`
satisfies batch mode. Where the site requires an interactive second factor, let
one interactive login carry the others by sharing its connection. In
`~/.ssh/config`, with placeholder values:

```
Host your-existing-ssh-alias
    HostName login-host
    User your-cluster-username
    ControlMaster auto
    ControlPath ~/.ssh/cm-%C
    ControlPersist 8h
```

Log in once with `ssh your-existing-ssh-alias` and complete the prompts. While
that master connection lasts, `hpc-*` commands reuse it without prompting.
`ssh -O check your-existing-ssh-alias` reports whether it is active, and
`ssh -O exit your-existing-ssh-alias` closes it. Check your site's policy first;
some centers limit or forbid persistent connections.

### Installing from the cluster itself

Without SSH from a workstation, for example from a web-portal shell, copy or
clone this repository onto the cluster and run the same installer there:

```sh
cd path/to/your/clone
bash shell/deploy-jobmon.sh --local          # read-only preview
bash shell/deploy-jobmon.sh --local --apply  # verified, versioned activation
```

`--local` installs into the account running it and makes no SSH connection. It
applies the same checksum, compilation, wrapper-syntax, and refusal checks and
builds the same `~/.local/libexec/jobmon` release and wrapper link, so a later
`hpc-jdeploy --apply` from a workstation upgrades it in place. Run it on the
cluster, not on a workstation. Afterwards use the direct cluster commands shown
under [Day-to-day workflow](#day-to-day-workflow), or the `jobmon` function from
`examples/bashrc.fragment`. Do not copy the files into place by hand: deployment
refuses to replace an unrelated file where a managed link belongs, so a hand-made
copy blocks later upgrades.

## Day-to-day workflow

The workstation helper is the normal interface. Source it from your Zsh startup
configuration once, or source it in a terminal before use:

```zsh
export HPC_JOBMON_HOST=your-existing-ssh-alias
source path/to/your/clone/shell/jobmon.zsh
```

If a dotfile manager already installs a module that sources this file, do not add
it to `PATH` or source a second copy. After applying that module, start a new
shell once and confirm that the functions are available:

```zsh
exec zsh
type hpc-jobs
type hpc-jstorage-init
```

Before submitting work, an occasional `hpc-jdoctor` confirms the interpreter,
Slurm commands, executable batch wrapper, private storage, and login-side storage
probe. It also reports
`accounting_storage` as `available`, `disabled`, or `unknown`; a `disabled`
verdict means `sacct` cannot describe any job at this site, so completed-job
reports depend entirely on collected samples. The check makes one timeout-limited
accounting query for your account on the local cluster, over a fixed one-minute
window. Help and version output alone do not establish storage availability;
query failures other than explicit disabled-storage errors leave it `unknown`.
`hpc-jobs` is a
quick view of your current queue:

```zsh
hpc-jdoctor
hpc-jobs
```

Submit jobs normally with `sbatch`. Use `examples/monitored-job.sh` inside the
batch script only for jobs whose sampled history you want to retain. After Slurm
prints the job ID, run the monitoring commands on the workstation:

```zsh
hpc-job 123456                         # one current scheduler/history report
hpc-jwatch 123456 --interval 5         # follow saved samples; Ctrl-C stops viewing
hpc-jwatch                             # choose one of your running jobs, then watch it
hpc-jreport 123456                     # final human-readable report
hpc-jreport 123456 --days 90 --json    # machine-readable or older accounting
hpc-jfetch 123456 ./downloads          # optional private local archive
```

Nothing else normally needs to be started manually on the cluster. The batch
wrapper starts and stops the collector inside the allocation. Do not start a
collector on the login node. If a running job was submitted without the wrapper,
`hpc-job` can still report scheduler data, but `hpc-jwatch` will have no
persistent allocation-local CPU or memory samples.

Use the exact array task ID, such as `123456_7`, when applicable. Watching never
starts or stops collection; the batch wrapper owns the collectors. Re-run
`hpc-jreport` after completion to refresh final Slurm accounting.

When this bundle changes, preview and apply another deployment. The installer
verifies checksums and byte-compilation before switching the single `current`
release link, so the three Python entry points and batch wrapper select the same
release:

```zsh
hpc-jdeploy
hpc-jdeploy --apply
hpc-jdoctor
```

Older content-addressed releases remain under the remote
`~/.local/libexec/jobmon/releases/` directory for manual inspection or rollback.
The tool never submits jobs, deletes history, or runs a login-node daemon.

If Python or Slurm requires a site module, adapt `examples/env.sh` into remote
`~/.config/jobmon/env.sh`. This file must be quiet and work in a noninteractive
POSIX shell. `JOBMON_PYTHON` selects a single interpreter path, without arguments.
The transport, batch wrapper, and optional `examples/bashrc.fragment` source it.
Creating the environment file and editing startup files remain manual steps;
deployment installs the batch wrapper automatically.

For direct use on the cluster:

```sh
python3 "$HOME/.local/libexec/jobmon/jobmon.py" storage-init
python3 "$HOME/.local/libexec/jobmon/jobmon.py" doctor
python3 "$HOME/.local/libexec/jobmon/jobmon.py" jobs
python3 "$HOME/.local/libexec/jobmon/jobmon.py" show 123456
python3 "$HOME/.local/libexec/jobmon/jobmon.py" report 123456 --json
```

`show` includes an on-demand `sstat` snapshot for a running owned job. `report`
combines recorded history and accounting. History searches default to 30 days;
use `--days N` (1–3650) for older jobs. Requests are limited to your current local
cluster and authenticated Unix account. Array task IDs retain their identity;
step IDs, array ranges, and heterogeneous allocations are intentionally rejected
as command arguments. Step records remain visible inside allocation reports.

## Configure the remote environment

Create this file **on the cluster login node**, at `~/.config/jobmon/env.sh`.
The deployment helper does not create or copy it. From a cluster terminal:

```bash
mkdir -p "$HOME/.config/jobmon"
chmod 700 "$HOME/.config/jobmon"
micro "$HOME/.config/jobmon/env.sh"
```

Use another editor if `micro` is unavailable. For the first CPU-only test, save:

```sh
#!/bin/sh
# Use an existing Python 3.8+ interpreter.
export JOBMON_PYTHON="${JOBMON_PYTHON:-python3}"

# This absolute directory must be shared by the login node and every compute node.
export JOBMON_DATA_DIR="${JOBMON_DATA_DIR:-${XDG_STATE_HOME:-$HOME/.local/state}/jobmon}"

# Sampling period used by the batch wrapper, in seconds.
export JOBMON_INTERVAL="${JOBMON_INTERVAL:-30}"
```

The default data directory is appropriate only if that location is shared.
Otherwise, replace it with an absolute site shared-storage path. Relative
`JOBMON_DATA_DIR` values are rejected instead of being resolved differently on
login and compute nodes.
The `${VARIABLE:-default}` form preserves an existing per-job override.
Use `export` so Python and collector subprocesses receive the settings.

Check the existing interpreter on the cluster:

```bash
command -v python3
python3 --version
```

You can replace the `python3` default in the file with the absolute executable
path returned above, provided that path also exists on the compute nodes.
An existing Conda/Miniforge environment's Python can be invoked directly without
interactive activation. If site modules are required, put the minimum quiet,
noninteractive-compatible setup before the exports. No package installation is
required. Avoid Bash-only syntax: SSH also sources this file from a POSIX shell.

After saving the file, check its syntax, permissions, and interpreter. The final
command assumes the three jobmon Python files have already been deployed:

```bash
chmod 600 "$HOME/.config/jobmon/env.sh"
sh -n "$HOME/.config/jobmon/env.sh"
. "$HOME/.config/jobmon/env.sh"
"$JOBMON_PYTHON" --version
"$JOBMON_PYTHON" "$HOME/.local/libexec/jobmon/jobmon.py" storage-init
"$JOBMON_PYTHON" "$HOME/.local/libexec/jobmon/jobmon.py" doctor
```

Alternatively, back on the workstation run `hpc-jstorage-init` once and then
`hpc-jdoctor`. Storage initialization creates a private marker on the login node.
Every monitored allocation must see that marker before it creates a run, which
detects a node-local path. Other allocation nodes must also see the batch node's
new run directory before their collectors can start. A missing probe disables
monitoring but does not change the scientific workload's exit status.

Keep `env.sh` quiet: no `echo`, watchers, or collector startup commands. Source
it explicitly before direct Python commands; the SSH helper, interactive
`jobmon` function, deployment preflight, and batch wrapper source it themselves.

### All supported remote jobmon settings

The eight jobmon-specific inputs currently read by the remote shell integration
and collector are listed below. Defaults apply when a setting is unset or empty,
except the three switches, which enable their behavior only for the exact value
`1`; use `0` to leave them disabled.

| Setting                | Default                                        | Meaning and supported values                                                                                                                                                                                                                                                                                            |
| ---------------------- | ---------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `JOBMON_PYTHON`        | `python3`                                      | Existing Python 3.8+ interpreter, as one executable name or absolute path. No command-line arguments. Used by shell helpers and the batch wrapper.                                                                                                                                                                      |
| `JOBMON_DATA_DIR`      | `${XDG_STATE_HOME:-$HOME/.local/state}/jobmon` | Absolute private shared history directory visible at the same path on login/compute nodes. Relative configured paths are rejected. Directories must be owned by you and mode 700.                                                                                                                                       |
| `JOBMON_INTERVAL`      | `30`                                           | Collection period in seconds, between 1 and 86400; fractions are accepted. The batch wrapper passes it to `collect.py run --interval`. It does not change watch refreshes, and direct collector invocations use their own `--interval` argument.                                                                        |
| `JOBMON_GPU_UUIDS`     | Unset; GPU collection off                      | Comma-separated, unique, full `GPU-…` UUIDs that you have verified belong to the allocation on that node. Numeric ordinals and MIG UUIDs are unsupported.                                                                                                                                                               |
| `JOBMON_GPU_EXCLUSIVE` | Disabled                                       | `1` asserts verified exclusive use without sharing/MPS/MIG; supported device utilization may then be attributed to the job. `0` keeps shared-device context. Detected MIG prevents exclusive attribution even when set to `1`.                                                                                          |
| `JOBMON_ALLOW_OVERLAP` | `0`                                            | `1` enables the example wrapper's multi-node `srun --overlap` monitor step after site validation. With `0`, a multi-node workload still runs but collection is skipped with a diagnostic. Single-node collection is unaffected.                                                                                         |
| `JOBMON_PROCESS_TREE`  | Disabled                                       | `1` enables approximate process-group sampling where Slurm creates no job cgroup. Scope is the collector's own process group, so the workload must run directly in the batch script; `srun`/`mpirun` steps and anything calling `setsid`/`setpgid` are not counted. `0` keeps CPU and memory unavailable with a reason. |
| `JOBMON_COLLECTOR`     | `$HOME/.local/libexec/jobmon/collect.py`       | Optional collector script path used by the batch wrapper for init/run/stop/finish. It must exist on every relevant node. It does not relocate the SSH report entry point.                                                                                                                                               |

The expanded `remote-tools/examples/env.sh` template includes all eight inputs,
with the advanced overrides commented out. Leave the GPU settings unset for
your first CPU-only test. GPU UUIDs generally differ by node and allocation;
set them in a validated per-node launch environment rather than keeping an old
job's GPU list in a global configuration file.

`XDG_STATE_HOME` is an existing system setting used only as a storage fallback
when `JOBMON_DATA_DIR` is absent. Prefer configuring `JOBMON_DATA_DIR` directly.
Do not set `JOBMON_RUN_DIR` in this file: the wrapper creates and exports it for
each run. Slurm supplies `SLURM_*` and `SLURMD_NODENAME`; do not invent those
values to enable collection outside an allocation.

### Local helper settings and command options

These settings belong in your **workstation's zsh environment**, not the
cluster's `env.sh`:

| Setting                | Default                                          | Meaning                                                                                                                                                       |
| ---------------------- | ------------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `HPC_JOBMON_HOST`      | Unset by the canonical helper                    | SSH destination for job monitoring and deployment: an `~/.ssh/config` alias or `user@login-host`. Set it in a private local configuration or managed shell module. |
| `HPC_JOBMON_TOOLS_DIR` | The clone containing the sourced `jobmon.zsh`    | Repository used by `hpc-jdeploy`. Set it only when the helpers are not sourced from a clone. Reporting does not need this directory.                          |

Watch refresh rate, history range, JSON output, watch refresh count, and stopping
after a collected run finishes are
command options, not `env.sh` toggles: use `hpc-jwatch JOB_ID --interval 5`,
`hpc-jreport JOB_ID --days 90 --json`, or
`hpc-jwatch JOB_ID --count 3`. To stop after the newest saved run has a workload
exit and all expected final samples, use `hpc-jwatch JOB_ID --exit-on-completion`.

## Add persistent collection to a batch script

### Submission requirement

Jobmon does not attach a collector when `hpc-job` or `hpc-jwatch` is run. Those
commands only read scheduler information and collector files that already exist.
For persistent CPU and memory samples, the submitted batch allocation must start
the workload through `monitored-job.sh`.

`hpc-jdeploy --apply` installs the Python entry points and this wrapper as a
single verified release. The cluster-side executable symlink is:

```text
~/.local/bin/monitored-job.sh -> ../libexec/jobmon/current/monitored-job.sh
```

For an existing installation, rerun `hpc-jdeploy`, review the preview, then run
`hpc-jdeploy --apply` and `hpc-jdoctor`. No separate `scp` step is needed. The
wrapper must be accessible from the allocation nodes along with the Python
files. Installing it starts no collector and changes no submitted job scripts.

An existing byte-identical manual copy is migrated to this managed symlink.
Deployment refuses to overwrite a modified file, directory, or unrelated
symlink at that path; review and move it aside yourself before retrying. Keep
local wrapper customizations at a separate path. Existing `~/.local/bin`
permissions are preserved; the installed wrapper has mode 700.

The same rule protects the `~/.local/libexec/jobmon/*.py` entry points: a
byte-identical manual copy is migrated to the managed symlink, and anything else
found at those paths is reported and left alone. On a first installation
`current` is activated before the entry points are linked, so an interrupted
deployment never leaves links pointing at a release that does not exist.
Managed Python links must have the relative target `current/FILENAME`, and
`current` must point directly to a release under `releases/`. Other symlink
targets, including dangling unrelated links, are refused before activation.

For a simple batch script with one workload command, keep the existing Slurm
directives and launcher, but place the wrapper around the command:

```bash
#!/usr/bin/env bash
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=8
#SBATCH --time=00:30:00

set -euo pipefail

# Keep the launcher your application already requires. Do not add srun solely
# for jobmon.
bash "$HOME/.local/bin/monitored-job.sh" \
  srun your-application its-arguments
```

Submit it normally:

```bash
sbatch example.sbatch
```

For a complex script that performs environment setup, creates per-job log
directories, runs several commands, or records postprocessing results, wrap the
remainder of the script once by re-entering the same script. Place this block
after the persistent log directory exists but before environment setup or
scientific work begins:

```bash
mkdir -p "$RESULT_DIR"

# Redirect before jobmon starts so collector diagnostics are retained too.
if [[ ${JOBMON_WRAPPED:-0} != 1 ]]; then
  exec > "$RESULT_DIR/job.out" 2> "$RESULT_DIR/job.err"
  export JOBMON_WRAPPED=1
  exec bash "$HOME/.local/bin/monitored-job.sh" \
    bash "${BASH_SOURCE[0]}"
fi
unset JOBMON_WRAPPED

# Environment setup and all workload/postprocessing commands follow here.
```

The first invocation creates the logs and replaces itself with the wrapper. The
wrapper starts the allocation-local collector and invokes the same batch script
again. The inherited marker prevents recursion, while the wrapper retains the
final script exit status and stops the collector during cleanup.

Use a project-specific marker name if the script already uses `JOBMON_WRAPPED`.
Do not pass that marker with `sbatch`, and do not place the wrapper around only a
short setup command—the long-running workload must remain its child. Slurm copies
the batch script when it is submitted, so editing the source file cannot add
monitoring to an already running or pending job; submit a new job after making
this change.

For a job array, each task starts its own collector and history. Query each task
with its exact ID, such as `123456_7`. Once a task is running, use these commands
on the workstation, not inside the allocation:

```zsh
hpc-job 123456_7
hpc-jwatch 123456_7 --interval 5
```

If the cluster exposes a verified job cgroup, retain the application's existing
`srun` or MPI launcher. If no job cgroup exists and
`JOBMON_PROCESS_TREE=1` is deliberately enabled, the workload must instead run
directly under the wrapper: separate `srun`/MPI steps and programs that create a
new process group are outside that approximation and will be reported as
uncounted.

Choose an absolute private storage directory **shared by login and every compute node**.
Set `JOBMON_DATA_DIR` in the remote environment file. Its default is
`${XDG_STATE_HOME:-$HOME/.local/state}/jobmon`, which is useful only if that path
is shared. Run `hpc-jstorage-init` from the workstation before the first monitored
job. The login-created probe and allocation-side checks catch missing visibility;
they do not benchmark or certify the filesystem. Node-local spooling/persistence
is not implemented.

The wrapper creates a unique run record (including a requeue/restart label),
starts collection, waits up to ten seconds for the first sample, then runs and
waits for your workload. It preserves stdin, including a deliberately closed
file descriptor 0, arguments, and the workload exit status.
Monitoring initialization, sampling, or cleanup failures do not change
that status. The workload retains normal SIGINT handling and stays in the batch
process group, including when monitoring fails. SIGTERM, SIGINT, and SIGHUP are
forwarded to the launcher and its visible local descendants; the wrapper waits
for them to stop before recording completion. Further signals reach surviving
processes during that wait, and the wrapper reports the first interruption as
143, 130, or 129 respectively. No forced shutdown timeout cuts short a workload's
checkpoint handler. Processes already detached and reparented are outside that
tree; use a launcher such as `srun` to manage remote processes. SIGKILL and node
loss cannot guarantee cleanup or completion records. Once the workload finishes,
interruptions during the bounded monitor shutdown preserve its recorded exit
status.

Collection defaults to 30 seconds; set `JOBMON_INTERVAL` to change it. There is
one collector per node, protected by an advisory lock, with a separate JSONL
stream for every collector start. Samples are flushed as they are written.
The shared filesystem must support advisory locks. Files are mode 600 and
directories mode 700; existing broadly readable storage is rejected.

For multiple nodes, the example requires explicit `JOBMON_ALLOW_OVERLAP=1` after
testing site support. It launches one monitor task per allocated node using
`srun --overlap`, one CPU per task and 64 MiB per node. Adapt this launcher to
site policy and verify application/GPU binding. Without opt-in, the scientific
workload runs and the report marks the missing collector coverage. There is no
separate queued monitor job or persistent login-node collection daemon.

## Watch and fetch history

```zsh
hpc-jwatch 123456 --interval 5
hpc-jwatch --interval 5                 # no ID: choose from your running jobs
hpc-jwatch 123456 --interval 5 --json   # newline-delimited JSON snapshots
hpc-jwatch 123456 --exit-on-completion # stop after workload exit and final samples
hpc-jfetch 123456 ./downloads
```

Without a job ID, `hpc-jwatch` lists your running jobs and lets you choose one:
`j`/`k` or the arrow keys move, Enter watches the highlighted job, and `q`, Esc,
or Ctrl-C cancel with status 130. Options such as `--interval` still apply to
the chosen job. Only running jobs are listed, since a pending job has no samples
yet; the title counts the others. With exactly one running job, it is watched
without a menu. The menu is drawn on the terminal and erased before watching
starts, a long list scrolls, and the terminal's settings are restored on every
exit. Outside an interactive terminal, such as in a script, a job ID is still
required and nothing connects without one. Choosing costs one extra SSH
connection for the job list.

Watch uses one SSH connection, takes one scheduler snapshot, and then reads
new sample records incrementally. Ctrl-C stops only the viewer. Run a new
`hpc-jreport` to refresh final accounting. A job without saved history gets a
one-shot report and a diagnostic instead of a scheduler polling loop.

The text view states everything that cannot change during the watch once: the
scheduler snapshot and the time it was taken, site diagnostics, and for each run
its nodes, sampling interval, measurement scope, and what is not measured. After
that it prints a row only when a node delivers a new sample, so a 5-second
refresh against 30-second sampling stays quiet in between. For example:

```text
CLOCK        ELAPSED  SAMPLES  CPU LAST 30 s    CPU AVG    CPU MAX       MEMORY   MEMORY MAX
11:25:29     7:26:31      894         101.2%      43.4%     101.2%    27.37 GiB    27.37 GiB
11:25:59     7:27:01      895         101.8%      43.5%     101.8%    27.37 GiB    27.37 GiB
11:26:29     7:27:31      896          98.0%      43.5%     101.8%     2.01 GiB    27.37 GiB
11:27:46  no new sample for 77 s (expected every 30 s)
```

- `CLOCK` is when each sample was taken, in your workstation's time zone:
  `hpc-jwatch` sends its UTC offset, so the times match your clock even when the
  cluster runs in another zone. Run directly on the cluster, watch uses the
  cluster's local time. The header names the zone either way.
- `ELAPSED` is the job's run time at that sample, laid out like squeue's `TIME`
  column, which is also elapsed time rather than a time of day. It counts from
  the start Slurm implies at the scheduler snapshot (snapshot time minus Slurm's
  elapsed). When Slurm has no running record for the run, for example after the
  job ends, it counts from when monitoring started instead, and the run header
  says so.
- CPU columns are a percentage of the node's allocated cores. `CPU LAST` covers
  only the newest sampling interval, so a ramp-up shows at once; `CPU AVG` covers
  every sampled interval so far; `CPU MAX` is the busiest single interval so
  far. Busy-core counts appear in `hpc-jreport` and `hpc-job`.
- `MEMORY` is the newest sampled reading and `MEMORY MAX` the largest sampled
  reading so far, so a peak stays visible after memory drops.

Multi-node runs add a `NODE` column. A node that misses about two and a half
sampling intervals, at least 30 seconds past its interval, produces one warning
per stall rather than a growing age on every refresh. A final sample is marked
`final` and produces no further stall warnings for that node. When the wrapper
records completion, the watch distinguishes a successful workload exit (`0`)
from a nonzero exit and points to application logs when needed. It reports
final samples as node counts, such as `1/2 nodes; waiting for final samples`, and
updates the count as delayed samples arrive. An incomplete expected-node layout
is named explicitly, even if the number of observed final samples matches the
declared node count.

Once all expected nodes have final samples, the watch prints the CPU average
over sampled intervals and the sampled memory peak once. For multiple nodes,
memory peaks are listed separately per node; they are not a concurrent total.
Unavailable measurements remain unavailable. A synthetic completion example:

```text
11:28:02  Workload finished with exit code 1 (nonzero).
11:28:02  Final samples: 1/1 node recorded.
          CPU average (sampled intervals): 43.5% of allocated cores
          Sampled memory peak: 27.37 GiB
          Check application logs; run hpc-jreport 123456 for final Slurm status.
          Watch remains open; press Ctrl-C to stop viewing.
```

The workload exit comes from the batch wrapper; final Slurm status still needs
a fresh `hpc-jreport`. Watching stays open by default so later requeued runs can
appear. Add `--exit-on-completion` to stop when the newest run currently visible
in saved history has both a wrapper completion record and final samples from
every expected node. Older completed runs cannot stop a watch of a newer active
run. The option also stops immediately if that newest saved run is already
complete when watching starts; it does not wait for a future requeue and does
not establish a final scheduler state.

Missing final samples or an unknown node layout leave the watch open. `--count N`
still stops after N refreshes, whether or not samples arrived, so it can bound
the wait; Ctrl-C also stops viewing. A normal automatic stop returns viewer exit
status `0`, including when the recorded workload exit is nonzero. New collection
errors are printed when they first appear.

`--json` keeps the same report format: every refresh emits one complete JSON
snapshot, for scripts that parse the stream. `--exit-on-completion` also works
with it, emitting the final snapshot before closing without extra text.

Only run directories are read as saved history. An unrelated entry beside them,
such as the `.nfs` stub shared storage leaves behind when an open file is
deleted, is listed under `ignored_storage_entries` and skipped; it does not stop
a report or a fetch. A symlink where a run directory belongs is still refused.

Fetch writes a private `.tar.gz` archive to the required local directory. It
does not extract files automatically. Failed transfers preserve a `.partial`
file and the SSH/remote exit status. Live archives can end with a partial JSONL
line; readers wait for or ignore that unfinished record. To inspect offline:

```sh
umask 077
mkdir -m 700 saved-history
tar -xzf downloads/jobmon-123456.EXAMPLE.tar.gz -C saved-history
JOBMON_DATA_DIR="$PWD/saved-history" python3 jobmon/jobmon.py report 123456 --json
```

Offline reads verify directory/file ownership and saved job/run metadata.
They report unavailable scheduler queries without discarding the samples.
An array fetched with `123456_7` must be read using the same task ID.
The report reader currently requires the metadata username and UID to match
the reading account. If your local and remote accounts differ, inspect the
archived JSON/JSONL files directly or generate reports on the cluster; keep
the original ownership metadata intact.

## Troubleshooting

- If every `hpc-*` command fails at once with SSH exit status 255 and
  `Permission denied`, the login needs a password or second factor, which batch
  mode never prompts for. Load your key into `ssh-agent`, or share one interactive
  login as described in [Password and two-factor logins](#password-and-two-factor-logins).
- If `hpc-jdeploy` reports that it cannot find `shell/deploy-jobmon.sh`, the
  helpers were not sourced from a clone of this repository. Source
  `shell/jobmon.zsh` from your clone, or set `HPC_JOBMON_TOOLS_DIR` to it.
- If `hpc-jdeploy --apply` fails during checksum, Python compilation, or wrapper
  Bash syntax validation, the old
  `current` release remains active. Check that `env.sh` is quiet and that
  `JOBMON_PYTHON` works in a noninteractive POSIX shell.
- To roll back, log in to the cluster, choose a hash shown by
  `ls ~/.local/libexec/jobmon/releases`, verify its `SHA256SUMS`, create a new
  relative symlink to `releases/HASH`, and replace `current` with `mv -fT`.
  The Python entry points and wrapper link follow `current`. Choose a release
  containing `monitored-job.sh` to retain the managed wrapper; older Python-only
  releases leave that wrapper link dangling.
- If `hpc-jdoctor` reports a missing or non-executable batch wrapper, rerun
  `hpc-jdeploy --apply`. Its `batch_wrapper` result includes the path and
  existence/executable checks. Resolve any wrapper path conflict as described
  in the submission section before retrying.
- If `hpc-jdoctor` reports a missing storage probe, confirm that
  `JOBMON_DATA_DIR` is the intended absolute shared path and rerun
  `hpc-jstorage-init` from the workstation, not from an allocation.
- If a monitored workload runs but no history appears, read the batch stderr.
  The wrapper deliberately continues after monitoring setup fails. Typical
  causes are an invisible storage probe, an unreadable collector, or a site
  module unavailable on compute nodes.
- If CPU or memory samples say that no verified job cgroup is visible, the site
  may use cgroup v1, a cgroup namespace, or a path shape this collector does not
  support. Scheduler accounting reports still work. Check the site's plugins with
  `scontrol show config | grep -iE 'ProctrackType|TaskPlugin|JobAcctGatherType'`.
  If `ProctrackType` is `proctrack/pgid` and `TaskPlugin` is `task/none`, Slurm
  creates no job cgroup in any version and no cgroup reader can help; set
  `JOBMON_PROCESS_TREE=1` for approximate process-group sampling instead. Note
  that `task/none` also means allocated cores are not enforced, so a job can use
  more cores than it requested.
- If process-tree samples report far less CPU than the job is using, the work is
  leaving the batch process group. Run the workload directly in the batch script
  rather than through `srun`, and avoid wrappers such as GNU `timeout` that place
  the child in a new process group.
- If a report says that Slurm accounting storage is disabled, the site runs
  without an accounting database. `sacct` then fails for every job, so completed
  jobs cannot be reported from the scheduler and a larger `--days` cannot help.
  `hpc-jdoctor` shows this as `"accounting_storage": "disabled"`. `hpc-jobs`,
  queued/running reports, and `sstat` live steps are unaffected; wrap future
  jobs with the collector to retain usage after they finish.
- If `hpc-jreport` cannot find an older completed job, increase `--days`. If
  Slurm accounting is unavailable but private history exists, the report keeps
  the saved samples and includes the scheduler error.
- If `hpc-jfetch` fails, its `.partial` file is retained deliberately. Remove it
  only after diagnosing the SSH or remote error.

## What measurements mean

- CPU accounting efficiency is parent `TotalCPU / (ElapsedRaw × AllocCPUS)`.
  Parent and child CPU totals are never added. The percentage assumes a fixed
  allocation; zero `TotalCPU` is treated as ambiguous accounting, not proof of
  an idle job. Accounting may arrive late or omit CPU time after signals.
- Accounting `MaxRSS` is a maximum associated with a task inside a step, not
  whole-job memory. Memory requests preserve per-CPU/per-node semantics; reports
  do not invent a percentage when allocation data is missing. A request Slurm
  reports in an unreadable form has scope `unavailable`, never a per-CPU or
  per-node label guessed from its trailing character.
- Where Slurm creates no job cgroup at all, `JOBMON_PROCESS_TREE=1` enables a
  fallback that sums `utime`, `stime`, `cutime`, and `cstime` from `/proc` across
  the collector's own process group, plus each member's RSS. It is labelled
  `process_tree_pgid` and is an **approximation**, never presented as a cgroup
  measurement. Reaped children are counted through the parent's `cutime`, so the
  total stays monotonic, but four accuracy limits remain: work in a
  separate Slurm step (`srun`, `mpirun`), any process that calls `setsid` or
  `setpgid` (GNU `timeout` does this unless given `--foreground`), processes
  orphaned to `init`, and summed RSS double-counts pages shared between
  processes. A detected launcher in the group is reported as an explicit
  understatement warning rather than silently lowering the number. The collector
  excludes itself. An empty group reports unavailable, never zero.
- Collection uses a visible cgroup v2 `job_<raw job ID>` ancestor of the
  collector, discovered from `/proc`. If no such boundary is visible, CPU/memory
  remain unavailable with a reason unless `JOBMON_PROCESS_TREE=1` explicitly
  enables the process-group approximation described above. Cgroup v1 collection
  is unsupported. Cgroup memory charges include cache and are not RSS.
- Two memory peaks are reported per node and never mixed. The _sampled peak_ is
  the largest `memory.current` an interval happened to observe, so a spike
  between samples is invisible to it. The _kernel high-water mark_ is the
  cgroup's own `memory.peak`, which is exact and independent of the sampling
  interval, but is lifetime-cumulative and cannot be aligned across nodes, so it
  is never used for a concurrent total. `memory.peak` needs cgroup v2 on Linux
  5.19 or newer; where it is absent the mark reports as unavailable with that
  reason rather than falling back to the sampled value. Process-tree sampling
  has no kernel high-water mark at all, and says so instead.
- Concurrent memory peaks require a valid reading from every expected node,
  within half the shortest sampling interval. The expected node list must be
  known and match the declared node count, when available. An incomplete layout
  retains per-node readings, but concurrent totals remain unavailable and
  `all_expected_collectors_final` stays false, even if every known collector
  recorded its final sample. Independent node peaks are
  reported separately, and on a one-node run, where all of these are the same
  reading, the text report shows only the node's own. Clock synchronization is
  assumed. CPU percentages use allocated core-time over covered intervals; gaps
  are not filled with zeros.
- Sample timestamps mark the start of CPU and memory capture, before optional
  GPU queries. A slow GPU query does not shift the CPU/memory timestamp used for
  cross-node alignment.
- Watches retain up to 128 pending memory readings per node to align delayed
  file appends. Recent concurrent peaks can be revised as delayed samples arrive;
  completed time ranges remain accumulated without rereading the full history.
  Exceeding the buffer, or receiving timestamps behind an already committed time
  range, marks `concurrent_memory_alignment_incomplete` and adds a diagnostic.
  Start a fresh report to recompute those alignments from the saved files. This
  affects concurrent totals; independent node peaks remain available.
- Each node reports CPU for its newest interval, the average over all sampled
  intervals, and its busiest single interval, as a percentage of allocated cores
  and as busy cores. The newest interval is unavailable, not zero, after a first
  sample, a counter reset, or a gap longer than one and a half sampling
  intervals; such intervals never count toward the average or the maximum. Where
  Slurm does not enforce core limits (`TaskPlugin` is `task/none`, common on
  process-tree sites), an interval can exceed 100% of allocated cores: that is
  real use beyond the allocation, not a measurement error. Text output shows
  memory in binary units (KiB, MiB, GiB); JSON output keeps exact bytes.
- GPU sampling requires existing `nvidia-smi` and explicitly verified full
  UUIDs in `JOBMON_GPU_UUIDS`, comma-separated. Numeric CUDA/Slurm ordinals are
  never mapped by assumption. Without a mapping, GPU collection is off and the
  report lists GPU under "Not measured". Only an allocation that Slurm or CUDA
  shows holding GPUs also records a collection diagnostic explaining how to map
  them. In multi-node jobs, provide UUIDs in a node-specific
  launch environment; different nodes generally have different UUIDs.
- GPU measurements default to shared-device context, with job utilization
  unavailable. Set `JOBMON_GPU_EXCLUSIVE=1` only after verifying exclusive use
  and absence of sharing/MPS/MIG. Detected MIG keeps measurements as context.
  MIG UUID sampling is unsupported. GPU averages use covered device-time;
  utilization is device activity, not a percentage of theoretical FLOPS.
- Between samples the collector checks the `stop.json` marker every few seconds
  rather than every second, so a long job on shared storage does not generate
  steady metadata traffic. Signals are unaffected: `SIGTERM`, `SIGINT`, and
  `SIGHUP` interrupt the wait immediately and still produce one final sample.

Each JSON report includes raw scheduler records, distinct allocation records,
separate collected runs, data sources, scope labels, missing nodes, gaps,
latest timestamps, final-sample status, both the sampled and kernel high-water
memory peaks per node, and collection errors. Saved runs are
not automatically merged with a particular accounting restart: inspect raw
IDs, submit/start timestamps, and restart labels when reconciling requeues.
Missing final samples, stale data, and a wrapper completion marker do not by
themselves establish a job's final scheduler state. Full-run GPU coverage is
not asserted because the expected device set must be verified on each node.
Malformed sample records are skipped with diagnostics before changing any
aggregates; valid records in the same run remain readable.

## Files and validation

The deployed program sources are `jobmon/jobmon.py`, `jobmon/slurm_backend.py`,
`jobmon/collect.py`, and `examples/monitored-job.sh`. The installer packages those
four files with `SHA256SUMS`, validates the archive remotely, byte-compiles the
Python files with the configured interpreter, checks the wrapper with `bash -n`,
and activates a content-addressed directory through `current`. Stable
`~/.local/libexec/jobmon/*.py` and `~/.local/bin/monitored-job.sh` symlinks preserve
the command paths. A failed
preflight leaves the previously active `current` release unchanged.

`shell/jobmon.zsh` stays local; `shell/deploy-jobmon.sh` is the explicit installer,
run from a workstation over SSH or on the cluster with `--local`.
`examples/env.sh` and `examples/bashrc.fragment` remain opt-in manual
configuration; `examples/monitored-job.sh` is included in deployment. No real host aliases,
usernames, telemetry, or storage paths are baked into sources. Keep fetched
archives under `downloads/` or outside the repository; the provided `.gitignore`
excludes that directory and common private local configuration files.

```sh
python3 -m unittest discover -s tests -v
zsh -n shell/jobmon.zsh
bash -n shell/deploy-jobmon.sh
bash -n examples/monitored-job.sh
shellcheck shell/deploy-jobmon.sh examples/monitored-job.sh examples/env.sh examples/bashrc.fragment
ruff check jobmon tests
```

`.github/workflows/checks.yml` runs the same checks on push and pull request,
with the unit suite on both the supported Python floor and a current release.
The unit suite also exercises the workstation helpers, so local test runs need
Bash and Zsh. Each Python CI job installs Zsh before running those transport and
terminal tests; the separate shell job's installed tools are not shared with it.

Tests use synthetic accounting, cgroup/GPU fixtures, temporary private storage,
and a fake SSH executable. They require no cluster connection. The optional
`test_shell_integration.py` checks external shell integrations only when their
paths are supplied through `REMOTE_HELPERS_FILE` and `REMOTE_BASHRC_FILE`; those
tests skip otherwise. `test_public_release.py` guards public documentation
against machine-specific home paths, identity values, credentials, and stale
generated copies; it also covers the license and CI workflow, and never walks
`.git`, whose configuration holds the committer's address.
`test_compatibility.py` enforces the Python 3.8 floor without needing a 3.8
interpreter: it parses every source at that feature version and rejects PEP 604
unions, subscripted builtin generics, and standard-library APIs added later.
Start rollout with `storage-init`, `doctor`, then one short CPU-only job. Check
multi-node launches, signals, storage errors, and retention separately on your
site. Validate GPU behavior only when deliberately expanding beyond the current
CPU-only scope. There is no automatic telemetry deletion. Remove the wrapper
from future batch scripts to stop collection; preserve or archive existing
samples as you choose.

If a dotfile manager owns your shell configuration, keep this application bundle
outside its source tree. Install a thin module that obtains `HPC_JOBMON_HOST`
from private local data and sources, or links to, the canonical
`shell/jobmon.zsh`; do not duplicate its functions. `hpc-jdeploy` finds the clone
from the sourced file, resolving a symlinked module, so `HPC_JOBMON_TOOLS_DIR` is
needed only when the helpers come from somewhere else. Keep
machine-specific SSH aliases, usernames, cluster addresses, storage paths, and
private template data in ignored local configuration.

Reference definitions: [Slurm accounting](https://slurm.schedmd.com/sacct.html),
[Slurm step sharing](https://slurm.schedmd.com/srun.html#OPT_overlap),
[kernel cgroup v2](https://docs.kernel.org/admin-guide/cgroup-v2.html), and
[NVIDIA queries](https://docs.nvidia.com/deploy/nvidia-smi/index.html).
