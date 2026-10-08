#include "jobfork.h"
#include <sys/wait.h>
#include <fcntl.h>

#define PIPE_READ 0
#define PIPE_WRITE 1

extern char **environ;


/******************************************************************************
Function `create_child`:
  Create a child process for a shell command.
Arguments:
  * `cmd`:      the command to be executed;
  * `ci`:       child information.
Return:
  Zero on success; non-zero on error.
******************************************************************************/
int create_child(const char *cmd, CHILD_INFO *ci) {
  int pipe_stdout[2] = {-1, -1};
  int pipe_stderr[2] = {-1, -1};
  char *argp[] = {"sh", "-c", (char *) cmd, NULL};

  if (pipe(pipe_stdout) < 0) {
    MSG_ERR("failed to allocate pipe for child stdout on job:\n   %s\n", cmd);
    return ERR_PIPE;
  }
  if (pipe(pipe_stderr) < 0) {
    close(pipe_stdout[PIPE_READ]);
    close(pipe_stdout[PIPE_WRITE]);
    MSG_ERR("failed to allocate pipe for child stderr on job:\n   %s\n", cmd);
    return ERR_PIPE;
  }

  if (fcntl(pipe_stdout[PIPE_READ], F_SETFL, O_NONBLOCK) == -1 ||
      fcntl(pipe_stderr[PIPE_READ], F_SETFL, O_NONBLOCK) == -1 ||
      fcntl(pipe_stdout[PIPE_READ], F_SETFD, FD_CLOEXEC) == -1 ||
      fcntl(pipe_stderr[PIPE_READ], F_SETFD, FD_CLOEXEC) == -1) {
    close(pipe_stdout[PIPE_READ]);
    close(pipe_stdout[PIPE_WRITE]);
    close(pipe_stderr[PIPE_READ]);
    close(pipe_stderr[PIPE_WRITE]);
    MSG_ERR("failed to set child output pipe on job:\n   %s\n", cmd);
    return ERR_PIPE;
  }

  pid_t pid = fork();
  if (pid < 0) {
    close(pipe_stdout[PIPE_READ]);
    close(pipe_stdout[PIPE_WRITE]);
    close(pipe_stderr[PIPE_READ]);
    close(pipe_stderr[PIPE_WRITE]);
    MSG_ERR("failed to create a new process for job:\n"
        "    %s\n", cmd);
    return ERR_FORK;
  }
  else if (pid == 0) {          /* child process */
    /* Restore default stop-signal handling and create a process group. */
    struct sigaction sa = {0};
    sa.sa_handler = SIG_DFL;
    if (sigemptyset(&sa.sa_mask) ||
        sigaction(SIGHUP, &sa, NULL) || sigaction(SIGINT, &sa, NULL) ||
        sigaction(SIGTERM, &sa, NULL) || sigaction(SIGQUIT, &sa, NULL)) {
      _exit(ERR_SIG);
    }
    setpgid(0, 0);
    if (sigprocmask(SIG_SETMASK, &sa.sa_mask, NULL)) _exit(ERR_SIG);

    if (dup2(pipe_stdout[PIPE_WRITE], STDOUT_FILENO) == -1 ||
        dup2(pipe_stderr[PIPE_WRITE], STDERR_FILENO) == -1) _exit(ERR_REDIR);

    close(pipe_stdout[PIPE_READ]);
    close(pipe_stdout[PIPE_WRITE]);
    close(pipe_stderr[PIPE_READ]);
    close(pipe_stderr[PIPE_WRITE]);

    /* Group for the command and its descendants to be stopped together. */
    execve(JOBFORK_SHELL, argp, environ);
    _exit(ERR_EXEC);
  }

  /* Also set the process group from the parent to avoid fork/exec race. */
  if (setpgid(pid, pid) && errno != EACCES && errno != ESRCH) {
    MSG_ERR("failed to set process group for child %ld: %s\n",
        (long) pid, strerror(errno));
  }
  close(pipe_stdout[PIPE_WRITE]);
  close(pipe_stderr[PIPE_WRITE]);

  memset(ci, 0, sizeof(*ci));
  ci->pid = pid;
  ci->fd[0] = pipe_stdout[PIPE_READ];
  ci->fd[1] = pipe_stderr[PIPE_READ];

  return 0;
}


/******************************************************************************
Function `print_line`:
  Print one output line.
Arguments:
  * `ci`:       child information;
  * `stream`:   0 for stdout, 1 for stderr;
  * `task`:     MPI task or OpenMP thread number;
  * `idx`:      job index for this task or thread.
******************************************************************************/
static void print_line(CHILD_INFO *ci, const int stream, const int task,
    const int idx) {
  ci->line[stream][ci->used[stream]] = '\0';
  if (stream) {
    MSG_CHILD_STDERR(task, idx, ci->line[stream]);
#ifdef FLUSH_STDOUT
    fflush(stderr);
#endif
  }
  else {
    MSG_CHILD_STDOUT(task, idx, ci->line[stream]);
#ifdef FLUSH_STDOUT
    fflush(stdout);
#endif
  }
  ci->used[stream] = 0;
}


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
int close_child(CHILD_INFO *ci, const int task, const int idx, const int stop) {
  siginfo_t info = {0};
  int waiterr = 0;
  if (!ci->stopped) {
    /* Check for failure even if children still hold the output pipes. */
    if (!stop && waitid(P_PID, ci->pid, &info, WEXITED | WNOHANG | WNOWAIT)) {
      waiterr = errno;
      info.si_pid = 0;
    }
    /* Stop the whole group on cancellation or an unsuccessful shell exit. */
    if (stop || (info.si_pid &&
        (info.si_code != CLD_EXITED || info.si_status))) {
      ci->stopped = ci->failed = 1;
      if (kill(-ci->pid, SIGKILL) && errno == ESRCH) kill(ci->pid, SIGKILL);
    }
  }

  char buf[CMD_BUF];
  for (int i = 0; i < 2; i++) {
    /* Limit reads per call to avoid blocking MPI and signal checks. */
    for (int j = 0; ci->fd[i] >= 0 && j < PIPE_READ_MAX; j++) {
      ssize_t n = read(ci->fd[i], buf, sizeof(buf));
      if (n < 0 && (errno == EINTR || errno == EAGAIN || errno == EWOULDBLOCK))
        break;

      if (n <= 0) {
        if (n < 0) ci->failed = 1;
        if (ci->used[i]) print_line(ci, i, task, idx);
        close(ci->fd[i]);
        ci->fd[i] = -1;
        break;
      }
      for (ssize_t k = 0; k < n; k++) {
        ci->line[i][ci->used[i]++] = buf[k];
        if (buf[k] == '\n' || ci->used[i] == CMD_BUF - 1)
          print_line(ci, i, task, idx);
      }
    }
  }

  if (ci->fd[0] >= 0 || ci->fd[1] >= 0) return JOB_START;
  if (!ci->stopped && !info.si_pid && (!waiterr || waiterr == EINTR))
    return JOB_START;

  /* Close the child only after stdout and stderr are closed. */
  int status;
  pid_t pid = waiterr ? -1 : waitpid(ci->pid, &status, WNOHANG);
  if (!pid || (!waiterr && pid < 0 && errno == EINTR)) return JOB_START;

  if (pid < 0 || ci->failed || !WIFEXITED(status) || WEXITSTATUS(status)) {
    if (pid < 0) {
      MSG_ERR("waitpid error of job index %d on task %d: %s.\n", idx, task,
          strerror(waiterr ? waiterr : errno));
    }
    else if (WIFEXITED(status)) {
      MSG_ERR("exit code of job index %d on task %d: %d%s.\n", idx, task,
          WEXITSTATUS(status), ci->failed && !WEXITSTATUS(status) ?
          " (internal stop or I/O failure)" : "");
    }
    else if (WIFSIGNALED(status)) {
      MSG_ERR("termination signal of job index %d on task %d: %d.\n",
          idx, task, WTERMSIG(status));
    }
    else {
      MSG_ERR("unexpected wait status of job index %d on task %d: %d.\n",
          idx, task, status);
    }
    return JOB_FAIL;
  }

  return JOB_DONE;
}

