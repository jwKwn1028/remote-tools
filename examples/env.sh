#!/bin/sh
# ~/.config/jobmon/env.sh (600): POSIX and silent; see docs.md.
# Load site modules quietly; start no background work.

export JOBMON_PYTHON="${JOBMON_PYTHON:-python3}"
export JOBMON_DATA_DIR="${JOBMON_DATA_DIR:-${XDG_STATE_HOME:-$HOME/.local/state}/jobmon}"
export JOBMON_INTERVAL="${JOBMON_INTERVAL:-60}"

# Per allocation and node; never keep a previous job's list here.
# export JOBMON_GPU_UUIDS=GPU-aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee
# export JOBMON_GPU_EXCLUSIVE=1
# export JOBMON_ALLOW_OVERLAP=1
# export JOBMON_PROCESS_TREE=1
# export JOBMON_COLLECTOR="${JOBMON_COLLECTOR:-$HOME/.local/libexec/jobmon/collect.py}"
