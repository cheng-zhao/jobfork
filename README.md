# jobfork

![GitHub](https://img.shields.io/github/license/cheng-zhao/jobfork.svg)

## Table of Contents

- [Introduction](#introduction)
- [Compilation](#compilation)
- [Getting started](#getting-started)
   - [Job list file](#job-list-file)
   - [OpenMP scheduler](#openmp-scheduler)
   - [MPI scheduler](#mpi-scheduler)
   - [Standard output/error](#standard-outputerror)
   - [Restart file](#restart-file)

## Introduction

jobfork is a simple tool written in C, for running a list of independent commands in parallel, with either OpenMP or MPI schedulers. It launches a pending command in the pool (job list file) whenever a scheduler worker is available, while keeping the scheduling overhead small.

To avoid nested parallelism and resource oversubscription, it is generally recommended not to run OpenMP-parallelised jobs with the OpenMP scheduler, or MPI-parallelised jobs with the MPI scheduler. A common use case is to run OpenMP-parallelised jobs with the MPI scheduler, where each MPI task runs one OpenMP job with multiple threads.

jobfork captures all the standard outputs and errors of the jobs, and prepends identifiers with different symbols and ANSI colours. Jobs that return non-zero statuses, and unfinished jobs upon termination, are saved to a restart file.

Apart from the reliance on `omp.h` and `mpi.h`, which are required for the OpenMP and MPI schedulers respectively, jobfork is compliant with the ISO C99 and IEEE POSIX.1-2008 standards. It is written by Cheng Zhao (&#36213;&#25104;), and is distributed under the [MIT license](LICENSE.txt).

If you use this tool in research that results in publications, please consider citing the following paper:

> Zhao et al., 2021, [The completed SDSS-IV extended Baryon Oscillation Spectroscopic Survey: 1000 multi-tracer mock catalogues with redshift evolution and systematics for galaxies and quasars of the final data release](https://doi.org/10.1093/mnras/stab510), *MNRAS*, 503(1):1149&ndash;1173 ([arXiv:2007.08997](https://arxiv.org/abs/2007.08997))

<sub>[\[TOC\]](#table-of-contents)</sub>

## Compilation

The building of jobfork is based on the make utility. In addition to a C compiler supporting the C99 and POSIX.1-2008 standards, the OpenMP scheduler requires OpenMP compiler support, while the MPI scheduler requires an MPI implementation (such as [Open MPI](https://www.open-mpi.org/)) and the corresponding compiler wrapper (such as `mpicc`). On some supercomputers, the MPI environment has to be configured first (for instance, with the `module load` command).

By default, jobfork can be compiled with both schedulers using the command
```bash
make
```
Upon successful completion of the compilation process, two executables `jobfork_omp` and `jobfork_mpi`, are generated for the OpenMP and MPI schedulers, respectively.

Alternatively, the two components can be compiled individually with the following commands
```bash
make omp    # compile the OpenMP scheduler
make mpi    # compile the MPI scheduler
```
In this case only the corresponding executable is generated for each of the compilation command.

<sub>[\[TOC\]](#table-of-contents)</sub>

## Getting started

### Job list file

Before calling the jobfork executables, one has to prepare a job list file, which is a plain text file containing all the jobs to be run. Each line of the file should be one valid shell command. Leading whitespace is removed, and empty lines and lines starting with `#` (which can be reset in [jobfork.h](jobfork.h#L57)) are ignored by the scheduler.

Note that currently the job list file must be a real file on disk, and cannot be supplied through standard input or a pipe. The commands are executed with `/bin/sh -c` by default, where the shell path can be customised with [`JOBFORK_SHELL`](jobfork.h#L52). In addition, each input line must be shorter than the [`CMD_BUF`](jobfork.h#L56) limit, which is 2048 characters by default.

As an example, we create a job list file called `joblist.txt`, with the contents as follows:
```console
$ cat joblist.txt
echo "Hello World!"
uname
non-existent-command
```

There are three commands in total, in which the last one is invalid and is expected to fail eventually.

<sub>[\[TOC\]](#table-of-contents)</sub>

### OpenMP scheduler

To run jobfork with OpenMP, the number of threads to be used should be specified via the environment variable `OMP_NUM_THREADS`. Then the `jobfork_omp` executable can be simply called with the job list file as its only argument.

Below is an example for running the jobs in `joblist.txt` in parallel with two OpenMP threads:
```console
$ export OMP_NUM_THREADS=2
$ ./jobfork_omp joblist.txt
3 jobs are found in the list file.
Parallelizing jobs with OpenMP: 2 threads.
-> Allocating command to thread 0 (job index: 0):
   echo "Hello World!"
-> Allocating command to thread 1 (job index: 0):
   uname
[0-0] Hello World!
-> Allocating command to thread 0 (job index: 1):
   non-existent-command
[1-0] Linux
<0-1> sh: 1: non-existent-command: command not found
Error: unable to finish command on task 0 (job index: 1).
Restart file saved (1 unfinished jobs):
  joblist.txt.rst
```

Here, the first two commands are assigned to the two OpenMP threads with thread IDs `0` and `1`, respectively. They are the first commands run on their threads, hence are both identified by the job index `0`. Once a thread becomes available, it is assigned the next pending command. The exact assignment and output order can therefore vary between runs.

<sub>[\[TOC\]](#table-of-contents)</sub>

### MPI scheduler

The `jobfork_mpi` executable can be called in the standard way for running MPI programs, with a MPI process manager such as `mpirun`. The number of MPI tasks can then be specified via command line options of the process manager.

Task 0 acts as the manager and maintains the job pool, but it also executes jobs itself. Thus all MPI tasks can be used for running jobs. For instance, the commands in `joblist.txt` can be run with two MPI tasks using
```console
$ mpirun -n 2 ./jobfork_mpi joblist.txt
3 jobs are found in the list file.
Parallelizing jobs with MPI: 2 tasks.
-> Allocating command to task 0 (job index: 0):
   echo "Hello World!"
-> Allocating command to task 1 (job index: 0):
   uname
[0-0] Hello World!
-> Allocating command to task 0 (job index: 1):
   non-existent-command
[1-0] Linux
<0-1> sh: 1: non-existent-command: not found
Error: unable to finish command on task 0 (job index: 1).
Restart file saved (1 unfinished jobs):
  joblist.txt.rst
```

As for the OpenMP scheduler, the actual assignment and output order depend on the completion time of individual jobs.

<sub>[\[TOC\]](#table-of-contents)</sub>

### Standard output/error

The standard outputs and errors of each jobs are reprocessed by jobfork, and prepended a unique identifier, which consists of two numbers connected by the `-` symbol. The first number is the OpenMP thread ID or MPI task ID, while the second is the local index of the job run by this thread or task. The identifiers for standard outputs are surrounded by pairs of square brackets (`[]`) with green ANSI colour, while for standard errors they are red angle brackets (`<>`).

<sub>[\[TOC\]](#table-of-contents)</sub>

### Restart file

If not all jobs finish successfully (recognised by 0 exit codes), jobfork creates a restart file containing all failed commands. If jobfork receives a termination signal (such as being cancelled by the user with the `CTRL`-`C` keys, or more specifically, a `SIGHUP`, `SIGINT`, `SIGTERM`, or `SIGQUIT` signal), it also records all jobs that have not finished before the termination. The restart file is in the same format as the job list file, hence can be run with jobfork later.

The name of the restart file is hard-coded, with the suffix `.rst` appended to the name of the job list file. If the file already exists, then jobfork will overwrite it silently. If all jobs are recognised as successful, no restart file is created or modified.

In our example job list file, there is a nonexistent command `non-existent-command`, which cannot be executed by shell. As a failed job, it is recorded in the restart file for both the OpenMP and MPI schedulers:
```console
$ cat joblist.txt.rst
# Unfinished jobs
non-existent-command
```

<sub>[\[TOC\]](#table-of-contents)</sub>
