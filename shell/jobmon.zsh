# Source this file from zsh. Set HPC_JOBMON_HOST to an SSH destination: an alias
# from ~/.ssh/config, or user@host when the default port and key already work.
# A dotfile manager may provide it, but this public source must not hard-code it.

# Record this bundle's location while it is being sourced, so hpc-jdeploy finds
# the installer and local plotter from any clone. :A resolves symlinked modules.
() {
  emulate -L zsh
  typeset -g _jobmon_bundle_dir=${${(%):-%x}:A:h:h}
}

_jobmon_host() {
  emulate -L zsh
  local host=${HPC_JOBMON_HOST:-}
  if [[ -z $host || $host == -* || $host == *[^A-Za-z0-9_.@:-]* ]]; then
    print -u2 -- 'jobmon: set HPC_JOBMON_HOST to an SSH destination: an ~/.ssh/config alias or user@host.'
    print -u2 -- 'jobmon: ports, jump hosts, and identity files belong in ~/.ssh/config, not in this value.'
    return 2
  fi
  REPLY=$host
}

_jobmon_ssh() {
  emulate -L zsh
  local REPLY
  _jobmon_host || return
  # This command is constant: HOME, interpreter, and environment resolve remotely.
  local remote_command='if [ -r "$HOME/.config/jobmon/env.sh" ]; then . "$HOME/.config/jobmon/env.sh" || exit; fi; exec "${JOBMON_PYTHON:-python3}" "$HOME/.local/libexec/jobmon/jobmon.py" _transport'
  command ssh -T -o BatchMode=yes -o ConnectTimeout=10 -o ConnectionAttempts=1 -- "$REPLY" "$remote_command"
}

_jobmon_request() {
  emulate -L zsh
  local operation=$1 job_id='' request option value
  shift
  case $operation in
    show|watch|report|fetch)
      job_id=${1:-}
      local job_pattern='^[1-9][0-9]*(_[0-9]+)?$'
      if [[ ! $job_id =~ $job_pattern || ${#job_id} -gt 40 ]]; then
        print -u2 -- 'jobmon: expected a job ID such as 123456 or 123456_7.'
        return 2
      fi
      shift
      request='{"operation":"'${operation}'","job_id":"'${job_id}'"'
      ;;
    jobs|doctor|storage-init) request='{"operation":"'${operation}'"' ;;
    *) print -u2 -- 'jobmon: unsupported operation'; return 2 ;;
  esac
  while (( $# )); do
    option=$1
    shift
    case $option in
      --json) request+=',"json":true' ;;
      --exit-on-completion)
        if [[ $operation != watch ]]; then
          print -u2 -- "jobmon: $option is not valid for $operation."
          return 2
        fi
        request+=',"exit_on_completion":true'
        ;;
      --days|--interval|--count)
        if [[ $option == --days && $operation != (show|watch|report) ]] ||
           [[ $option != --days && $operation != watch ]]; then
          print -u2 -- "jobmon: $option is not valid for $operation."
          return 2
        fi
        value=${1:-}
        local integer_pattern='^[1-9][0-9]{0,6}$'
        if [[ ! $value =~ $integer_pattern ]] ||
           [[ $option == --days && $value -gt 3650 ]] ||
           [[ $option == --interval && $value -gt 86400 ]] ||
           [[ $option == --count && $value -gt 1000000 ]]; then
          print -u2 -- "jobmon: invalid positive integer for $option."
          return 2
        fi
        shift
        request+=',"'${option#--}'":'${value}
        ;;
      *) print -u2 -- "jobmon: unsupported option: $option"; return 2 ;;
    esac
  done
  if [[ $operation == watch ]]; then
    # Watch prints clock times; send this workstation's UTC offset so they match its
    # clock even when the cluster runs in another zone.
    local zone=${(%):-%D{%z}}
    if [[ $zone == [+-][0-9][0-9][0-9][0-9] ]]; then
      request+=',"utc_offset_minutes":'$(( ${zone[1]}1 * (10#${zone[2,3]} * 60 + 10#${zone[4,5]}) ))
    fi
  fi
  request+='}'
  print -r -- "$request" | _jobmon_ssh
}

# Choose one of your running jobs on the terminal and return its ID in REPLY.
# Keys are read from and the menu drawn on /dev/tty, so piped output stays clean.
_jobmon_choose_job() {
  emulate -L zsh
  local action=${1:-watch}
  local job_pattern='^[1-9][0-9]*(_[0-9]+)?$' output line key rest frame terminal=
  local -a fields ids rows
  local -i others=0 count selected=1 top=1 visible drawn=0 i width
  if [[ ! -t 0 ]]; then
    print -u2 -- 'jobmon: expected a job ID such as 123456 or 123456_7.'
    return 2
  fi
  output=$(_jobmon_request jobs) || return
  for line in ${(f)output}; do
    fields=("${(@ps:\t:)line}")
    [[ $fields[1] == 'JOB ID' ]] && continue
    # Only a started job can have samples to watch; the others are counted, not listed.
    if [[ $fields[2] == (RUNNING|COMPLETING) && $fields[1] =~ $job_pattern ]]; then
      ids+=("$fields[1]")
      rows+=("$(printf '%-16s %-20.20s %-11s %11s %5s %5s  %s' \
        "$fields[1]" "${fields[7]:-unavailable}" "${(@)fields[2,6]}")")
    else
      (( ++others ))
    fi
  done
  count=${#ids}
  if (( count == 0 )); then
    if (( others )); then
      print -u2 -- "jobmon: no running jobs to $action ($others queued or not running)."
    else
      print -u2 -- "jobmon: no running jobs to $action."
    fi
    return 1
  fi
  if (( count == 1 )); then
    REPLY=$ids[1]
    return 0
  fi
  visible=$(( count < ${LINES:-24} - 4 ? count : ${LINES:-24} - 4 ))
  (( visible < 1 )) && visible=1
  width=$(( ${COLUMNS:-80} - 3 ))
  # While the menu is open, Ctrl-C is read as a key, so it cancels like q however
  # early it is pressed, and keys typed ahead are not echoed. The local trap covers
  # an external SIGINT; emulate -L restores any TRAPINT of your own afterwards.
  TRAPINT() { return 130 }
  {
    terminal=$(command stty -g </dev/tty) && command stty -isig -echo </dev/tty
    print -rn -- $'\e[?25l' >/dev/tty
    while true; do
      (( selected < top )) && top=selected
      (( selected >= top + visible )) && top=$(( selected - visible + 1 ))
      frame=$'\r'
      (( drawn > 1 )) && frame+=$'\e['$(( drawn - 1 ))'A'
      frame+=$'\e[J'"Choose a running job to $action ($selected/$count"
      (( others )) && frame+="; $others queued or not running"
      frame+=$')\n'"  ${$(printf '%-16s %-20.20s %-11s %11s %5s %5s  %s' 'JOB ID' NAME STATE ELAPSED CPUS NODES NODELIST)[1,width]}"$'\n'
      for (( i = top; i < top + visible; i++ )); do
        if (( i == selected )); then
          frame+=$'\e[7m> '"${rows[i][1,width]}"$'\e[0m\n'
        else
          frame+="  ${rows[i][1,width]}"$'\n'
        fi
      done
      frame+="j/k or arrows: move   Enter: $action   q or Esc: cancel"
      print -rn -- "$frame" >/dev/tty
      drawn=$(( visible + 3 ))
      read -sk 1 key || return 130
      case $key in
        j) (( selected < count )) && (( ++selected )) ;;
        k) (( selected > 1 )) && (( --selected )) ;;
        $'\e')
          # An arrow key is Esc plus two bytes; Esc alone cancels.
          rest=''
          read -sk 2 -t 0.05 rest
          case $rest in
            '[A'|OA) (( selected > 1 )) && (( --selected )) ;;
            '[B'|OB) (( selected < count )) && (( ++selected )) ;;
            '') return 130 ;;
          esac
          ;;
        $'\n'|$'\r') REPLY=$ids[selected]; return 0 ;;
        q|$'\x03') return 130 ;;
      esac
    done
  } always {
    # Clear the menu and restore the cursor and terminal on every exit.
    frame=$'\r'
    (( drawn > 1 )) && frame+=$'\e['$(( drawn - 1 ))'A'
    print -rn -- "$frame"$'\e[J\e[?25h' >/dev/tty
    [[ -n $terminal ]] && command stty "$terminal" </dev/tty
  }
}

hpc-jdoctor() { emulate -L zsh; _jobmon_request doctor "$@"; }
hpc-jstorage-init() { emulate -L zsh; _jobmon_request storage-init "$@"; }
hpc-jobs() { emulate -L zsh; _jobmon_request jobs "$@"; }
hpc-job() { emulate -L zsh; _jobmon_request show "$@"; }
hpc-jwatch() {
  emulate -L zsh
  # Without a job ID, choose one of your running jobs; options still follow.
  if (( ! $# )) || [[ $1 == -* ]]; then
    local REPLY
    _jobmon_choose_job || return
    set -- "$REPLY" "$@"
  fi
  _jobmon_request watch "$@"
}
hpc-jreport() { emulate -L zsh; _jobmon_request report "$@"; }

hpc-live() {
  emulate -L zsh
  local job_id='' interval=5 port=0 host REPLY option value
  local tools_dir=${HPC_JOBMON_TOOLS_DIR:-${_jobmon_bundle_dir:-}}
  local -a options
  while (( $# )); do
    case $1 in
      -h|--help)
        print -r -- 'Usage: hpc-live [JOB_ID] [--interval SECONDS] [--port PORT] [--no-browser]'
        print -r -- 'Without an ID, choose a running job. Opens a local browser dashboard of live saved samples.'
        return 0
        ;;
      --interval|--port)
        option=$1
        value=${2:-}
        local integer_pattern='^(0|[1-9][0-9]{0,5})$'
        if [[ ! $value =~ $integer_pattern ]] ||
           [[ $option == --interval && ( $value -lt 1 || $value -gt 86400 ) ]] ||
           [[ $option == --port && $value -gt 65535 ]]; then
          print -u2 -- "jobmon: invalid value for $option."
          return 2
        fi
        if [[ $option == --interval ]]; then interval=$value; else port=$value; fi
        shift 2
        ;;
      --no-browser) options+=(--no-browser); shift ;;
      *)
        local job_pattern='^[1-9][0-9]*(_[0-9]+)?$'
        if [[ -n $job_id || ! $1 =~ $job_pattern || ${#1} -gt 40 ]]; then
          print -u2 -- 'Usage: hpc-live [JOB_ID] [--interval SECONDS] [--port PORT] [--no-browser]'
          return 2
        fi
        job_id=$1
        shift
        ;;
    esac
  done
  if [[ ! -r $tools_dir/jobmon/live.py ]]; then
    print -u2 -- 'jobmon: cannot find jobmon/live.py; source helpers from the clone or set HPC_JOBMON_TOOLS_DIR.'
    return 2
  fi
  if ! command -v python3 >/dev/null; then
    print -u2 -- 'jobmon: hpc-live needs an existing local Python 3.8+ interpreter named python3.'
    return 2
  fi
  if [[ -z $job_id ]]; then
    _jobmon_choose_job view || return
    job_id=$REPLY
  fi
  _jobmon_host || return
  host=$REPLY
  command python3 "$tools_dir/jobmon/live.py" "$job_id" --host "$host" --interval "$interval" --port "$port" "${options[@]}"
}

hpc-jdeploy() {
  emulate -L zsh
  local REPLY
  local tools_dir=${HPC_JOBMON_TOOLS_DIR:-${_jobmon_bundle_dir:-}}
  if (( $# > 1 )) || { (( $# == 1 )) && [[ $1 != --apply ]]; }; then
    print -u2 -- 'Usage: hpc-jdeploy [--apply] (default: read-only deployment preview)'
    return 2
  fi
  _jobmon_host || return
  if [[ ! -r $tools_dir/shell/deploy-jobmon.sh ]]; then
    print -u2 -- 'jobmon: cannot find shell/deploy-jobmon.sh; set HPC_JOBMON_TOOLS_DIR to your copy of this repository.'
    return 2
  fi
  command bash "$tools_dir/shell/deploy-jobmon.sh" "$REPLY" "$@"
}

hpc-jfetch() (
  emulate -L zsh
  if (( $# != 2 )); then
    print -u2 -- 'Usage: hpc-jfetch JOB_ID LOCAL_DIRECTORY'
    return 2
  fi
  local job_id=$1 destination=$2 partial result
  local job_pattern='^[1-9][0-9]*(_[0-9]+)?$'
  if [[ ! $job_id =~ $job_pattern || ${#job_id} -gt 40 || -z $destination ]]; then
    print -u2 -- 'jobmon: invalid job ID or empty destination.'
    return 2
  fi
  _jobmon_host || return
  umask 077
  command mkdir -p -- "$destination" || return
  partial=$(command mktemp "${destination%/}/jobmon-${job_id}.XXXXXXXX.tar.gz.partial") || return
  _jobmon_request fetch "$job_id" > "$partial"
  result=$?
  if (( result )); then
    print -u2 -- "jobmon: fetch failed ($result); incomplete archive retained at $partial"
    return $result
  fi
  command mv -- "$partial" "${partial%.partial}" || return
  print -r -- "${partial%.partial}"
)
