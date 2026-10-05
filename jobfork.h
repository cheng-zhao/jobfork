/*******************************************************************************
* 
* jobfork: C tool for running multiple jobs in parallel.
*
* Github repository:
*       https://github.com/cheng-zhao/jobfork
*
* Copyright (c) 2020 Cheng Zhao <zhaocheng03@gmail.com>
* 
* Permission is hereby granted, free of charge, to any person obtaining a copy
* of this software and associated documentation files (the "Software"), to deal
* in the Software without restriction, including without limitation the rights
* to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
* copies of the Software, and to permit persons to whom the Software is
* furnished to do so, subject to the following conditions:
* 
* The above copyright notice and this permission notice shall be included in all
* copies or substantial portions of the Software.
* 
* THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
* IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
* FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
* AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
* LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
* OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
* SOFTWARE.
*
*******************************************************************************/

#ifndef JOBFORK_H
#define JOBFORK_H

#ifndef _POSIX_C_SOURCE
#define _POSIX_C_SOURCE 200809L
#endif

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>
#include <signal.h>
#include <sys/types.h>
#include <time.h>

/* Prefer the MPI scheduler if both modes are enabled. */
#ifdef CMD_MPI
#define FLUSH_STDOUT
#undef CMD_OMP
#endif

#ifndef JOBFORK_SHELL
#define JOBFORK_SHELL "/bin/sh"
#endif

#define CMD_BUF 2048            /* maximum number of bytes for commands */
#define COMMENT '#'             /* comment symbol for the job list file */
#define JOB_START 1
#define JOB_FAIL  2
#define JOB_DONE  3
#define PIPE_READ_MAX 16        /* maximum number of reads per call */

#ifndef IDLE_MSEC
#define IDLE_MSEC 10            /* idle sleep between job loop checks */
#endif

extern int term;                /* handled stop signal */

/*============================================================================*\
                           Definition of error codes
\*============================================================================*/
#define ERR_MEMORY      101     /* failed to allocate memory      */
#define ERR_FILE        102     /* failed to read or write a file */
#define ERR_PIPE        103     /* failed to allocate pipe        */
#define ERR_FORK        104     /* failed to create a process     */
#define ERR_REDIR       105     /* failed to redirect I/O         */
#define ERR_EXEC        106     /* failed to execute the job      */
#define ERR_STRING      107     /* string length exceeding limits */
#define ERR_ARG         108     /* invalid argument               */
#define ERR_CMD         109     /* invalid command                */
#define ERR_SIG         110     /* signal handling error          */
#define ERR_MPI         111     /* MPI error                      */
#define ERR_OTHER       119     /* unknown errors                 */

/*============================================================================*\
                                   Shortcuts
\*============================================================================*/
#define MSG_ERR(...)            \
  fprintf(stderr, "\x1B[31;1mError:\x1B[0m " __VA_ARGS__)
#define MSG_CHILD_STDOUT(...)   \
  printf("\x1B[32;1m[%d-%d]\x1B[0m %s", __VA_ARGS__)
#define MSG_CHILD_STDERR(...)   \
  fprintf(stderr, "\x1B[31;1m<%d-%d>\x1B[0m %s", __VA_ARGS__)

#ifdef CMD_MPI
#define MPI_CHECK(func) {                                       \
  int _err = (func);                                            \
  if (_err != MPI_SUCCESS) {                                    \
    MSG_ERR("MPI call failed: %s (error %d).\n", #func, _err);  \
    MPI_Abort(MPI_COMM_WORLD, _err);                            \
    _Exit(ERR_MPI);                                             \
  }                                                             \
}
#endif

/*============================================================================*\
                            Definition of data types
\*============================================================================*/
typedef struct {
  pid_t pid;
  int fd[2];                    /* POSIX file descriptor of stdout and stderr */
  char line[2][CMD_BUF];        /* incomplete output lines */
  int used[2];
  int stopped;
  int failed;
} CHILD_INFO;

struct cmd_status {
  char fname_rst[CMD_BUF];              /* restart file for unfinished jobs */
  int num;
  int len;
  char *cmd;
  char *status;
};
extern struct cmd_status cstat;

/*============================================================================*\
                            Definition of functions
\*============================================================================*/

/******************************************************************************
Function `read_jobs`:
  Read commands (one per non-empty line) from a job list file.
Arguments:
  * `fname`:    path to the job list file.
Return:
  Zero on success; non-zero on error.
******************************************************************************/
int read_jobs(const char *fname);

/******************************************************************************
Function `save_jobs`:
  Write unfinished commands to a restart file.
Return:
  Zero on success; non-zero on error.
******************************************************************************/
int save_jobs(void);

/******************************************************************************
Function `create_child`:
  Create a child process for a shell command.
Arguments:
  * `cmd`:      the command to be executed;
  * `ci`:       child information.
Return:
  Zero on success; non-zero on error.
******************************************************************************/
int create_child(const char *cmd, CHILD_INFO *ci);

/******************************************************************************
Function `close_child`:
  Read available child output, check its status, and stop it if requested.
Arguments:
  * `ci`:       child information;
  * `task`:     MPI task or OpenMP thread number;
  * `idx`:      job index for this task or thread;
  * `stop`:     non-zero for terminating the job.
Return:
  JOB_START if job is running; JOB_DONE on success; JOB_FAIL on error.
******************************************************************************/
int close_child(CHILD_INFO *ci, const int task, const int idx, const int stop);

/******************************************************************************
Function `check_signal`:
  Check if a termination signal has been received.
Return:
  Zero if no stop is requested; otherwise the detected signal number.
******************************************************************************/
int check_signal(void);

/******************************************************************************
Function `check_jobs`:
  Check for a stop request and save the restart file if needed.
Return:
  Zero if no stop is requested; otherwise the stop signal number.
******************************************************************************/
int check_jobs(void);

/******************************************************************************
Function `idle_wait`:
  Sleep shortly to avoid busy-waiting between job loop checks.
******************************************************************************/
void idle_wait(void);

#ifdef CMD_MPI
/******************************************************************************
Function `mpi_manager`:
  Schedule commands and execute task-private jobs simultaneously.
Arguments:
  * `tasknum`:  total number of MPI tasks.
******************************************************************************/
void mpi_manager(const int tasknum);

/******************************************************************************
Function `mpi_worker`:
  Run assigned jobs and report completion to the manager.
******************************************************************************/
void mpi_worker(void);
#endif

#endif
