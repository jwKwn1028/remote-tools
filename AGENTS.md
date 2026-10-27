# Repository instructions

## Purpose

`jobmon` is an independent, minimal Slurm job monitor for personal
use. It combines bounded `squeue`/`sacct` queries with optional allocation-local
CPU, memory, and GPU samples. Keep it deployable on Python 3.8+ with only the
standard library and ordinary cluster command-line tools.

Read `README.md` for the short setup path and `docs.md` for behavior,
configuration, limitations, and daily usage.

## Repository map

- `jobmon/jobmon.py`: CLI, JSON-over-SSH transport, reports, watches, archives,
  and saved-history aggregation.
- `jobmon/slurm_backend.py`: validated job identities, bounded Slurm commands,
  and scheduler output parsing/normalization.
- `jobmon/collect.py`: allocation-local cgroup/GPU sampling and private history
  storage. It does not poll the scheduler.
- `jobmon/plot.py`: workstation-only archive reader and standalone HTML/SVG time
  plots; reuses report validation and CPU calculations, never extracts archives.
- `jobmon/live.py`: loopback browser dashboard with one SSH sample stream per job;
  uses the plot renderer and shuts down after the last browser tab closes.
- `shell/jobmon.zsh`: workstation commands such as `hpc-jobs`, `hpc-jreport`,
  `hpc-jfetch`, `hpc-live`, and `hpc-jdeploy`.
- `shell/deploy-jobmon.sh`: explicit, checksum-verified, content-addressed remote
  deployment of the three Python source files and `examples/monitored-job.sh`,
  with a stable wrapper symlink under `~/.local/bin`. `--local` runs the identical
  install script in the current account, for clusters without workstation SSH.
- `examples/`: remote environment and bashrc fragments, plus the deployed batch
  wrapper (collection remains opt-in through the submitted job script).
- `tests/`: synthetic unit, transport, wrapper, deployment, compatibility, and
  publication safety tests. Tests must not require a live cluster.
- `.github/workflows/checks.yml`: the validation commands below, run on push and
  pull request across the supported Python floor and a current release.
- `ruff.toml`: the lint gate's line length and rule selection, so CI and a local
  run agree without command-line flags.
- `LICENSE`: MIT.

## Behavioral contracts

- Treat Slurm as authoritative for job state. Saved samples are supplemental;
  missing or stale samples must never be converted into guessed zero usage.
- Keep scheduler queries read-only, bounded, timeout-limited, and scoped to the
  current user and requested job.
- Validate job IDs before any scheduler, storage, archive, or transport action.
- Send operation arguments as validated JSON on standard input. Never interpolate
  user arguments into remote shell commands.
- Keep monitoring opt-in. Do not submit jobs, install packages, start login-node
  daemons, or delete telemetry implicitly.
- Preserve workload standard input, signals, and exit status. Monitoring failure
  must not replace a scientific workload's result.
- Require private storage ownership and permissions, reject symlinks for trusted
  state, and retain the login/compute shared-storage probe.
- Attribute GPU utilization only with verified full GPU UUIDs and explicit
  exclusive-use opt-in. Numeric ordinals, sharing, MPS, and MIG must not be
  presented as job-exclusive measurements.
- Keep multi-node collection disabled until the site's overlapping-step behavior
  has been validated.
- Prefer the job cgroup for CPU and memory. Process-group sampling is an opt-in
  approximation for sites with no job cgroup: keep its distinct scope label, keep
  its uncounted cases reported, and never let it be read as a cgroup measurement.
- Deployment remains preview-only by default. Applied deployments must verify the
  complete fixed source set before atomically changing the active release.
- Include the batch wrapper in release checksums and validate its Bash syntax.
  Refuse to overwrite an unrelated wrapper file or symlink; an identical manual
  copy may be migrated to the managed link. Preserve existing bin permissions.

## Compatibility and style

- Preserve Python 3.8 compatibility: avoid `match`, PEP 604 unions, built-in
  generic annotations, and APIs introduced after 3.8.
  `tests/test_compatibility.py` enforces this without a 3.8 interpreter; extend
  it when a new class of incompatibility becomes possible.
- Use the Python standard library only unless the project requirements explicitly
  change.
- Keep `examples/env.sh` POSIX-compatible, quiet, and safe in a noninteractive
  shell. Keep Bash- and Zsh-specific code in their existing files.
- Prefer explicit failures with actionable diagnostics. Unavailable cluster
  capabilities should degrade to reported diagnostics where safe.
- Keep `README.md` minimal. Put setup detail, operational explanations,
  measurement semantics, and troubleshooting in `docs.md`.
- Follow existing formatting and naming rather than introducing a framework or
  packaging layer for this small source bundle. `ruff.toml` holds the style gate:
  keep lines within its limit and leave import blocks ordered by hand, since
  `ruff`'s isort rewrites them and misreads `jobmon/` as an installed package.

## Public-repository hygiene

- Never commit real usernames, home directories, SSH aliases, hostnames, cluster
  addresses, email addresses, storage locations, credentials, keys, or captured
  job history.
- Use `$HOME`, `your-existing-ssh-alias`, and clearly synthetic values in examples.
- Keep fetched archives in ignored `downloads/` or outside the repository.
- Do not add `README.pdf`; generated copies can retain information removed from
  Markdown. Update `README.md` and `docs.md` directly.
- Keep external dotfile tests opt-in through `REMOTE_HELPERS_FILE` and
  `REMOTE_BASHRC_FILE`. Do not inspect files outside the repository by default.
- Extend `tests/test_public_release.py` when adding a new class of publishable
  artifact or sensitive local configuration. It scans Markdown, Python, shell,
  YAML, and `LICENSE`, and deliberately skips `.git` and tool caches.

## Validation

Run the checks relevant to every changed layer. Before handing off a broad change,
run the complete set:

```sh
python3 -m unittest discover -s tests -v
zsh -n shell/jobmon.zsh
bash -n shell/deploy-jobmon.sh
bash -n examples/monitored-job.sh
bash -n examples/env.sh
bash -n examples/bashrc.fragment
shellcheck shell/deploy-jobmon.sh examples/monitored-job.sh examples/env.sh examples/bashrc.fragment
ruff check jobmon tests
```

When another supported Python version is installed, run the unit suite with it as
well; CI already covers the floor and a current release. Use synthetic fixtures
and fake SSH commands for regressions. Do not deploy to or modify a real cluster
unless the user explicitly requests it.

## Change checklist

1. Identify whether the change affects the workstation, login node, compute node,
   or saved-data format.
2. Preserve the contracts above and add a focused regression test.
3. Update `docs.md` for user-visible behavior; update `README.md` only when the
   quick start or project summary changes.
4. Run the relevant Python and shell checks.
5. Recheck public documentation and examples for local or sensitive information.
