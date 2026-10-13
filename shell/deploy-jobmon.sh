#!/usr/bin/env bash
# Preview by default; --apply installs the Python tools and inert batch wrapper,
# over SSH or, with --local, into the current account on this machine.
set -euo pipefail

apply=0
install_here=0
destination=''
for argument in "$@"; do
    case "$argument" in
        --apply) apply=1 ;;
        --local) install_here=1 ;;
        --help|-h)
            printf 'Usage: bash shell/deploy-jobmon.sh [SSH_DESTINATION | --local] [--apply]\n'
            exit 0 ;;
        -*) printf 'Unknown option: %s\n' "$argument" >&2; exit 2 ;;
        *) destination=$argument ;;
    esac
done
host=''
if (( install_here )); then
    if [[ -n $destination ]]; then
        printf 'Use either an SSH destination or --local, not both.\n' >&2
        exit 2
    fi
else
    host=${destination:-${HPC_JOBMON_HOST:-}}
    if [[ -z $host || $host == -* || $host == *[!A-Za-z0-9_.@:-]* ]]; then
        printf 'Set HPC_JOBMON_HOST or supply an SSH destination: an ~/.ssh/config alias or user@host.\n' >&2
        printf 'Without SSH from a workstation, run this on the cluster itself with --local.\n' >&2
        exit 2
    fi
fi
source_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
sources=("$source_dir/jobmon/jobmon.py" "$source_dir/jobmon/slurm_backend.py"
    "$source_dir/jobmon/collect.py" "$source_dir/examples/monitored-job.sh")
release_files=("${sources[@]##*/}")
for source_file in "${sources[@]}"; do
    [[ -f "$source_file" ]] || { printf 'Missing source: %s\n' "$source_file" >&2; exit 2; }
done
command -v sha256sum >/dev/null || { printf 'sha256sum is required for verified deployment.\n' >&2; exit 2; }
ssh_options=(-T -o BatchMode=yes -o ConnectTimeout=10 -o ConnectionAttempts=1)
# Run one fixed POSIX script in the target account: over SSH, or here for --local.
# Both paths execute the identical script, so verification and refusals match.
run_target() {
    if (( install_here )); then
        sh -c "$1"
    else
        ssh "${ssh_options[@]}" -- "$host" "$1"
    fi
}
# shellcheck disable=SC2016
remote_local=$(run_target 'printf "%s\n" "$HOME/.local"')
printf 'Host: %s\nDestination: %s\nWrapper link: %s\nSources:\n' \
    "${host:-this machine (--local)}" "$remote_local/libexec/jobmon" "$remote_local/bin/monitored-job.sh"
printf '  %s\n' "${sources[@]}"
if (( ! apply )); then
    printf 'Preview only. Add --apply to copy these files.\n'
    exit 0
fi
# Stage the fixed source set with a checksum manifest. The remote side validates
# and compiles the complete release before atomically switching `current`.
local_staging=$(mktemp -d)
cleanup_local() {
    rm -f -- "$local_staging/jobmon.py" "$local_staging/slurm_backend.py" \
        "$local_staging/collect.py" "$local_staging/monitored-job.sh" "$local_staging/SHA256SUMS"
    rmdir -- "$local_staging"
}
trap cleanup_local EXIT
trap 'exit 129' HUP
trap 'exit 130' INT
trap 'exit 143' TERM
for source_file in "${sources[@]}"; do
    cp -- "$source_file" "$local_staging/${source_file##*/}"
done
(
    cd -- "$local_staging"
    sha256sum "${release_files[@]}" > SHA256SUMS
)

# Arguments never become remote shell code. Only the fixed source set and its
# checksum manifest are sent.
# shellcheck disable=SC2016
install_script='
set -eu
umask 077
base="$HOME/.local/libexec/jobmon"
releases="$base/releases"
bin_dir="$HOME/.local/bin"
wrapper="$bin_dir/monitored-job.sh"
wrapper_target="../libexec/jobmon/current/monitored-job.sh"
command -v sha256sum >/dev/null || { printf "jobmon deploy: remote sha256sum is required.\n" >&2; exit 1; }
command -v bash >/dev/null || { printf "jobmon deploy: remote bash is required.\n" >&2; exit 1; }
mkdir -p "$releases"
chmod 700 "$base" "$releases"
staging=$(mktemp -d "$releases/.deploy.XXXXXXXX")
next_link=""
cleanup_remote() {
    if [ -n "$next_link" ]; then
        rm -f -- "$next_link"
    fi
    case "$staging" in
        "$releases"/.deploy.*) rm -rf -- "$staging" ;;
    esac
}
trap cleanup_remote EXIT
trap "exit 129" HUP
trap "exit 130" INT
trap "exit 143" TERM
tar -xf - -C "$staging" -- jobmon.py slurm_backend.py collect.py monitored-job.sh SHA256SUMS
for file in jobmon.py slurm_backend.py collect.py; do
    chmod 600 "$staging/$file"
done
chmod 600 "$staging/SHA256SUMS"
chmod 700 "$staging/monitored-job.sh"
(cd "$staging" && sha256sum -c SHA256SUMS)
bash -n "$staging/monitored-job.sh"

# Refuse an unrelated script, directory, or symlink before changing current.
# A byte-identical manual install can be migrated to the managed link.
if [ -L "$wrapper" ]; then
    [ "$(readlink "$wrapper")" = "$wrapper_target" ] || {
        printf "jobmon deploy: refusing unrelated wrapper symlink: %s\n" "$wrapper" >&2; exit 1;
    }
elif [ -e "$wrapper" ]; then
    if [ ! -f "$wrapper" ] || ! cmp -s "$staging/monitored-job.sh" "$wrapper"; then
        printf "jobmon deploy: refusing to overwrite existing wrapper: %s; move it aside after review.\n" "$wrapper" >&2
        exit 1
    fi
fi
# Apply the wrapper policy to the Python entry points too: refuse an unrelated
# directory or file, and migrate only a byte-identical manual copy.
for file in current slurm_backend.py collect.py jobmon.py; do
    target="$base/$file"
    if [ -L "$target" ]; then
        link_target=$(readlink "$target")
        if [ "$file" = current ]; then
            case "$link_target" in
                releases/*) release_name=${link_target#releases/} ;;
                *) release_name="" ;;
            esac
            case "$release_name" in
                ""|.|..|*[!A-Za-z0-9_.-]*)
                    printf "jobmon deploy: refusing unrelated current symlink: %s\n" "$target" >&2; exit 1 ;;
            esac
        elif [ "$link_target" != "current/$file" ]; then
            printf "jobmon deploy: refusing unrelated entry point symlink: %s\n" "$target" >&2; exit 1
        fi
    elif [ -d "$target" ]; then
        printf "jobmon deploy: refusing to replace directory: %s\n" "$target" >&2; exit 1
    elif [ -e "$target" ]; then
        if [ "$file" = current ] || [ ! -f "$target" ] || ! cmp -s "$staging/$file" "$target"; then
            printf "jobmon deploy: refusing to overwrite unrelated file: %s; move it aside after review.\n" "$target" >&2
            exit 1
        fi
    fi
done
mkdir -p "$bin_dir"

if [ -r "$HOME/.config/jobmon/env.sh" ]; then
    . "$HOME/.config/jobmon/env.sh" || exit
fi
python_bin=${JOBMON_PYTHON:-python3}
PYTHONPYCACHEPREFIX="$staging/.pycache" "$python_bin" -m py_compile \
    "$staging/jobmon.py" "$staging/slurm_backend.py" "$staging/collect.py"
rm -rf -- "$staging/.pycache"

release_id=$(sha256sum "$staging/SHA256SUMS" | awk '\''{print $1}'\'')
release="$releases/$release_id"
if [ -e "$release" ]; then
    [ -d "$release" ] || { printf "jobmon deploy: release target is not a directory.\n" >&2; exit 1; }
    cmp -s "$staging/SHA256SUMS" "$release/SHA256SUMS" || {
        printf "jobmon deploy: existing release %s holds a different manifest.\n" "$release_id" >&2
        exit 1
    }
    (cd "$release" && sha256sum -c SHA256SUMS)
else
    mv -- "$staging" "$release"
    staging=""
fi

# Keep existing bin directory permissions; only the installed wrapper is private.
chmod 700 "$release/monitored-job.sh"
# On a first install the entry points below would dangle until current exists,
# so point current at the verified release before creating them. The switch at
# the end remains the single atomic step that activates an upgrade.
if [ ! -e "$base/current" ]; then
    next_link="$base/.current.$$"
    ln -s "releases/$release_id" "$next_link"
    mv -fT -- "$next_link" "$base/current"
    next_link=""
fi
# Set up stable entry points first, then switch the complete release with current.
for file in slurm_backend.py collect.py jobmon.py; do
    next_link="$base/.${file}.link.$$"
    ln -s "current/$file" "$next_link"
    mv -fT -- "$next_link" "$base/$file"
    next_link=""
done
next_link="$bin_dir/.monitored-job.sh.link.$$"
ln -s "$wrapper_target" "$next_link"
mv -fT -- "$next_link" "$wrapper"
next_link=""
next_link="$base/.current.$$"
ln -s "releases/$release_id" "$next_link"
mv -fT -- "$next_link" "$base/current"
next_link=""
(cd "$base/current" && sha256sum -c SHA256SUMS)
printf "Activated jobmon release %s\n" "$release_id"
printf "Installed wrapper: %s\n" "$wrapper"
'
tar -C "$local_staging" -cf - -- "${release_files[@]}" SHA256SUMS | run_target "$install_script"
if (( install_here )); then
    printf 'Installed verified release. Run jobmon.py storage-init, then doctor (see docs.md).\n'
else
    printf 'Installed verified release. Run hpc-jstorage-init, then hpc-jdoctor.\n'
fi
