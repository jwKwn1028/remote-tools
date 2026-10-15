#!/bin/sh
# Remote ~/.config/jobmon/env.sh. Keep machine-specific values private.
# Create/edit this on the cluster; chmod 600 the file and chmod 700 its directory.
# Use POSIX shell syntax, produce no output, and start no watchers or collectors.
# Load existing site modules quietly here only if needed; install no packages.
# Defaults preserve settings already exported for a particular job.

# 1. Existing Python 3.8+: one executable name/path, without extra arguments.
# Replace python3 with a verified absolute path when needed on compute nodes.
export JOBMON_PYTHON="${JOBMON_PYTHON:-python3}"

# 2. Absolute private storage path shared by login and ALL allocation nodes.
# Relative JOBMON_DATA_DIR values are rejected. Replace the fallback if needed.
export JOBMON_DATA_DIR="${JOBMON_DATA_DIR:-${XDG_STATE_HOME:-$HOME/.local/state}/jobmon}"

# 3. Batch-wrapper sampling interval: 1 to 86400 seconds (fractions allowed).
# Direct collect.py calls use --interval; this does not change watch refreshes.
export JOBMON_INTERVAL="${JOBMON_INTERVAL:-60}"

# 4. Optional GPU mapping: unique full UUIDs, verified for THIS allocation/node.
# Unset means no GPU samples. Numeric ordinals and MIG UUIDs are unsupported.
# For multiple nodes, supply the correct mapping in each node's launch environment.
# Do not keep a previous job's GPU list in this global file.
# export JOBMON_GPU_UUIDS=GPU-aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee

# 5. GPU attribution switch: default disabled; only the exact value 1 enables it.
# Set 1 only after verifying exclusive use without sharing, MPS, or MIG.
# Detected MIG prevents exclusive attribution even with this switch enabled.
# export JOBMON_GPU_EXCLUSIVE=1

# 6. Multi-node example-launcher switch: default 0 (disabled); set 1 only after
# site validation of the concurrent srun --overlap step. Single-node runs ignore it.
# export JOBMON_ALLOW_OVERLAP=1

# 7. Process-tree fallback switch: default disabled; only the exact value 1
# enables it. Set 1 ONLY where Slurm creates no job cgroup (ProctrackType is not
# proctrack/cgroup and TaskPlugin is not task/cgroup). Results are an
# approximation scoped to the batch process group, not cgroup measurements: the
# workload must run directly in the batch script, because srun/mpirun steps and
# anything calling setsid/setpgid leave the group uncounted.
# export JOBMON_PROCESS_TREE=1

# 8. Advanced: collector script path used by the batch wrapper, on every node.
# This does not change the SSH reporter's fixed installation path.
# export JOBMON_COLLECTOR="${JOBMON_COLLECTOR:-$HOME/.local/libexec/jobmon/collect.py}"

# JOBMON_RUN_DIR and SLURM_* are runtime-provided values, not configuration.
# HPC_JOBMON_HOST and HPC_JOBMON_TOOLS_DIR belong in local workstation zsh.
# Watch --interval, report --days, --json, and watch --count are CLI options.
