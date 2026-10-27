#!/usr/bin/env bash
# Invoke after allocation/module setup in your sbatch script:
#   bash /path/to/monitored-job.sh srun your-application its-arguments
# Preserve your workload's existing launcher and resource/binding options.
# Trap handlers below are invoked indirectly by Bash.
# shellcheck disable=SC2317
set -euo pipefail

if (( $# == 0 )); then
    printf 'Usage: monitored-job.sh WORKLOAD_COMMAND [ARGUMENT ...]\n' >&2
    exit 2
fi
if [[ -r "$HOME/.config/jobmon/env.sh" ]]; then
    # shellcheck source=/dev/null
    . "$HOME/.config/jobmon/env.sh" || printf 'jobmon: optional environment setup failed; continuing.\n' >&2
fi
jobmon_python=${JOBMON_PYTHON:-python3}
jobmon_collector=${JOBMON_COLLECTOR:-$HOME/.local/libexec/jobmon/collect.py}
jobmon_run_dir=''
jobmon_monitor_pid=''
jobmon_workload_pid=''
jobmon_workload_start=''
jobmon_launching=0
jobmon_workload_status=0
jobmon_cleaned=0
declare -a jobmon_signal_pids=()
declare -A jobmon_signal_starts=()

# Track /proc identities to avoid signaling recycled PIDs; preserve the batch group.
jobmon_read_process() {
    local pid=$1 info
    local -a fields
    IFS= read -r info 2>/dev/null <"/proc/$pid/stat" || return 1
    info=${info##*) }
    read -r -a fields <<<"$info"
    (( ${#fields[@]} >= 20 )) || return 1
    jobmon_proc_state=${fields[0]}
    jobmon_proc_parent=${fields[1]}
    jobmon_proc_start=${fields[19]}
}

jobmon_forward_signal() {
    local signum=$1 pid
    for pid in "${jobmon_signal_pids[@]}"; do
        if jobmon_read_process "$pid" && [[ $jobmon_proc_start == "${jobmon_signal_starts[$pid]}" ]]; then
            kill -s "$signum" "$pid" 2>/dev/null || true
        fi
    done
}

jobmon_workload_tree_alive() {
    local pid
    for pid in "${jobmon_signal_pids[@]}"; do
        if jobmon_read_process "$pid" && [[ $jobmon_proc_start == "${jobmon_signal_starts[$pid]}" &&
                $jobmon_proc_state != Z && $jobmon_proc_state != X ]]; then
            return 0
        fi
    done
    return 1
}

jobmon_cleanup() {
    local application_status=$1 attempts=0
    (( jobmon_cleaned == 0 )) || return 0
    jobmon_cleaned=1
    # Cleanup interruptions must preserve the workload's final exit status.
    trap '' TERM INT HUP
    # Every monitoring operation is guarded so failures cannot mask workload status.
    if [[ -n $jobmon_run_dir ]]; then
        "$jobmon_python" "$jobmon_collector" stop --run-dir "$jobmon_run_dir" || true
    fi
    if [[ -n $jobmon_monitor_pid ]]; then
        while kill -0 "$jobmon_monitor_pid" 2>/dev/null && (( attempts < 20 )); do
            sleep 1
            attempts=$((attempts + 1))
        done
        kill -TERM "$jobmon_monitor_pid" 2>/dev/null || true
        attempts=0
        while kill -0 "$jobmon_monitor_pid" 2>/dev/null && (( attempts < 5 )); do
            sleep 1
            attempts=$((attempts + 1))
        done
        kill -KILL "$jobmon_monitor_pid" 2>/dev/null || true
        wait "$jobmon_monitor_pid" 2>/dev/null || true
    fi
    if [[ -n $jobmon_run_dir ]]; then
        "$jobmon_python" "$jobmon_collector" finish --run-dir "$jobmon_run_dir" --exit-code "$application_status" || true
    fi
    return 0
}

jobmon_signal() {
    local signum=$1 exit_status=$2 path pid index
    local -a pending=() child_pids=()
    local -A children=() starts=()
    trap '' TERM INT HUP
    # A trap can run between spawning the asynchronous command and saving $!.
    if [[ -z $jobmon_workload_pid && $jobmon_launching == 1 ]]; then
        jobmon_workload_pid=${!:-}
    fi
    if [[ -n $jobmon_workload_pid ]]; then
        # Snapshot descendants before signaling parents that could exit and orphan them.
        for path in /proc/[0-9]*/stat; do
            pid=${path%/stat}
            pid=${pid##*/}
            if jobmon_read_process "$pid"; then
                children[$jobmon_proc_parent]="${children[$jobmon_proc_parent]:-} $pid"
                starts[$pid]=$jobmon_proc_start
            fi
        done
        if [[ -z $jobmon_workload_start ]] && jobmon_read_process "$jobmon_workload_pid" &&
                [[ $jobmon_proc_parent == "$$" && $jobmon_proc_start == "${starts[$jobmon_workload_pid]:-}" ]]; then
            jobmon_workload_start=$jobmon_proc_start
        fi
        if [[ -n $jobmon_workload_start && ${starts[$jobmon_workload_pid]:-} == "$jobmon_workload_start" ]]; then
            pending=("$jobmon_workload_pid")
            for (( index = 0; index < ${#pending[@]}; index++ )); do
                pid=${pending[index]}
                read -r -a child_pids <<<"${children[$pid]:-}"
                pending+=("${child_pids[@]}")
                jobmon_signal_starts[$pid]=${starts[$pid]}
            done
            # Leaves first, so parents can reap children during graceful shutdown.
            for (( index = ${#pending[@]} - 1; index >= 0; index-- )); do
                jobmon_signal_pids+=("${pending[index]}")
            done
        fi
        # Further interruptions still reach surviving processes while we wait.
        trap 'jobmon_forward_signal TERM' TERM
        trap 'jobmon_forward_signal INT' INT
        trap 'jobmon_forward_signal HUP' HUP
        jobmon_forward_signal "$signum"
        wait "$jobmon_workload_pid" 2>/dev/null || true
        while jobmon_workload_tree_alive; do
            sleep 0.2
        done
        wait "$jobmon_workload_pid" 2>/dev/null || true
        jobmon_workload_pid=''
    fi
    exit "$exit_status"
}
trap 'jobmon_cleanup "$?"' EXIT
trap 'jobmon_signal TERM 143' TERM
trap 'jobmon_signal INT 130' INT
trap 'jobmon_signal HUP 129' HUP

if jobmon_run_dir=$("$jobmon_python" "$jobmon_collector" init); then
    export JOBMON_RUN_DIR=$jobmon_run_dir
    if [[ ${SLURM_JOB_NUM_NODES:-${SLURM_NNODES:-1}} == 1 ]]; then
        "$jobmon_python" "$jobmon_collector" run --run-dir "$jobmon_run_dir" \
            --interval "${JOBMON_INTERVAL:-60}" &
        jobmon_monitor_pid=$!
    elif [[ ${JOBMON_ALLOW_OVERLAP:-0} == 1 ]]; then
        # Opt in only after site validation. Overlap may expose a different GPU set.
        srun --overlap --nodes="${SLURM_JOB_NUM_NODES:-$SLURM_NNODES}" \
            --ntasks="${SLURM_JOB_NUM_NODES:-$SLURM_NNODES}" --ntasks-per-node=1 \
            --cpus-per-task=1 --mem=64M \
            "$jobmon_python" "$jobmon_collector" run --run-dir "$jobmon_run_dir" \
            --interval "${JOBMON_INTERVAL:-60}" &
        jobmon_monitor_pid=$!
    else
        printf 'jobmon: multi-node collection needs a site-validated launcher; set JOBMON_ALLOW_OVERLAP=1 after validation.\n' >&2
    fi
else
    jobmon_run_dir=''
    printf 'jobmon: monitoring initialization failed; workload continues.\n' >&2
fi

if [[ -n $jobmon_monitor_pid ]]; then
    # Give collectors a bounded opportunity to persist their first sample.
    shopt -s nullglob
    jobmon_ready_attempts=0
    jobmon_expected_nodes=${SLURM_JOB_NUM_NODES:-${SLURM_NNODES:-1}}
    while kill -0 "$jobmon_monitor_pid" 2>/dev/null && (( jobmon_ready_attempts < 10 )); do
        jobmon_ready_files=("$jobmon_run_dir/"*.ready.json)
        (( ${#jobmon_ready_files[@]} >= jobmon_expected_nodes )) && break
        sleep 1
        jobmon_ready_attempts=$((jobmon_ready_attempts + 1))
    done
    jobmon_ready_files=("$jobmon_run_dir/"*.ready.json)
    if (( ${#jobmon_ready_files[@]} < jobmon_expected_nodes )); then
        printf 'jobmon: not all collectors became ready; workload continues with incomplete monitoring.\n' >&2
    fi
fi

# Preserve an input deck, or a caller's deliberately closed fd 0.
# Bash otherwise redirects asynchronous stdin and ignores SIGINT.
if { exec {jobmon_input_fd}<&0; } 2>/dev/null; then
    jobmon_launching=1
    (trap - INT QUIT; exec "$@") <&"$jobmon_input_fd" &
else
    jobmon_launching=1
    (trap - INT QUIT; exec "$@") <&- &
fi
jobmon_workload_pid=$!
jobmon_launching=0
if jobmon_read_process "$jobmon_workload_pid"; then
    jobmon_workload_start=$jobmon_proc_start
fi
if [[ -n ${jobmon_input_fd:-} ]]; then
    exec {jobmon_input_fd}<&-
fi
wait "$jobmon_workload_pid" || jobmon_workload_status=$?
jobmon_workload_pid=''
exit "$jobmon_workload_status"
