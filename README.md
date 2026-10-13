# jobmon

Personal Slurm monitoring with Python 3.8+ and the standard library:
`squeue`/`sacct` reports and optional allocation-local CPU, memory, and GPU samples.
No administrator access or package installation required.

An independent, minimal version of Princeton University's
[Jobstats](https://github.com/PrincetonUniversity/jobstats).

## Quick start

```zsh
cd path/to/your/clone
export HPC_JOBMON_HOST=your-existing-ssh-alias
source shell/jobmon.zsh
hpc-jdeploy          # preview
hpc-jdeploy --apply  # deploy
hpc-jobs             # list your jobs
```

If remote Python needs modules or a custom path, configure
[env.sh](docs.md#configure-the-remote-environment) before deploying.

[User guide](docs.md) · [MIT license](LICENSE)
