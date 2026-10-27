#!/bin/sh
# Remote ~/.config/jobmon/env.sh: private file (600), directory (700).
# Use POSIX syntax; no output, watchers, collectors, or package installs.
# Load existing site modules quietly if needed. Defaults preserve exported values.

# Python 3.8+: one executable, no arguments; verify it on compute nodes.
export JOBMON_PYTHON="${JOBMON_PYTHON:-python3}"

# Absolute private storage shared by login and all allocation nodes.
export JOBMON_DATA_DIR="${JOBMON_DATA_DIR:-${XDG_STATE_HOME:-$HOME/.local/state}/jobmon}"

# Wrapper sampling: 1-86400 seconds, fractions allowed; separate from watch refreshes.
# Direct collect.py calls use --interval.
export JOBMON_INTERVAL="${JOBMON_INTERVAL:-60}"

# Optional GPU mapping: unique full UUIDs verified per allocation/node.
# Unset disables GPU samples; numeric ordinals and MIG UUIDs are unsupported.
# Supply per-node mappings at launch; never retain a previous job's list here.
# export JOBMON_GPU_UUIDS=GPU-aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee

# GPU attribution: only 1 enables it; requires exclusive use without sharing/MPS/MIG.
# Detected MIG blocks attribution even with this enabled.
# export JOBMON_GPU_EXCLUSIVE=1

# Multi-node launcher: default 0; set 1 after site validation of srun --overlap.
# Ignored on single-node runs.
# export JOBMON_ALLOW_OVERLAP=1

# Process-tree fallback: only 1 enables it; requires no Slurm job cgroup
# (neither proctrack/cgroup nor task/cgroup). Approximate batch-group measurements:
# run workloads directly in the batch script; srun/mpirun and setsid/setpgid
# can leave the group uncounted.
# export JOBMON_PROCESS_TREE=1

# Collector path on every node; the SSH reporter's installation path stays fixed.
# export JOBMON_COLLECTOR="${JOBMON_COLLECTOR:-$HOME/.local/libexec/jobmon/collect.py}"

# JOBMON_RUN_DIR and SLURM_* are supplied at runtime.
# HPC_JOBMON_HOST and HPC_JOBMON_TOOLS_DIR belong in workstation zsh.
# Watch/report options belong on the command line.
