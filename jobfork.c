#include "jobfork.h"

#ifdef CMD_MPI
#include <mpi.h>
#define CLEAN_UP(ret) {                                 \
  free(cstat.status);                                   \
  free(cstat.cmd);                                      \
  if (MPI_Finalize() != MPI_SUCCESS) return ERR_MPI;    \
  return (ret);                                         \
}
#else
#ifdef CMD_OMP
#include <omp.h>
#endif
#define CLEAN_UP(ret) {                                 \
  free(cstat.status);                                   \
  free(cstat.cmd);                                      \
  return (ret);                                         \
}
#endif

int term;
struct cmd_status cstat;
static int save_err;            /* restart file error on termination */
static const int stop_signals[] = {SIGHUP, SIGINT, SIGTERM, SIGQUIT};


/******************************************************************************
Function `catch_signal`:
  Record a caught stop signal by setting its disposition to SIG_IGN.
Arguments:
  * `sig`:      received signal number.
******************************************************************************/
static void catch_signal(const int sig) {
  const int err = errno;
  struct sigaction sa = {0};
  sa.sa_handler = SIG_IGN;
  sigemptyset(&sa.sa_mask);
  sigaction(sig, &sa, NULL);
  errno = err;
}


int main(int argc, char *argv[]) {
  if (argc != 2) {
    fprintf(stderr, "Usage: %s job_list\n"
        "joblist: a text file with each line being a job (command)\n",
        argv[0]);
    return ERR_ARG;
  }

  /* Handle stop signals. */
  sigset_t stop_sigs;
  if (sigemptyset(&stop_sigs)) return ERR_SIG;
  for (size_t i = 0; i < sizeof(stop_signals) / sizeof(*stop_signals); i++) {
    if (sigaddset(&stop_sigs, stop_signals[i])) return ERR_SIG;
  }

  struct sigaction sa = {0};
  sa.sa_handler = catch_signal;
  sa.sa_flags = SA_RESTART;
  sa.sa_mask = stop_sigs;
  for (size_t i = 0; i < sizeof(stop_signals) / sizeof(*stop_signals); i++) {
    if (sigaction(stop_signals[i], &sa, NULL)) return ERR_SIG;
  }

  /* MPI and the created threads keep these signals blocked. */
  if (pthread_sigmask(SIG_BLOCK, &stop_sigs, NULL)) return ERR_SIG;

  /* MPI initialization. */
#ifdef CMD_MPI
  int myrank, tasknum;
  if (MPI_Init(&argc, &argv) != MPI_SUCCESS) return ERR_MPI;
  MPI_CHECK(MPI_Comm_set_errhandler(MPI_COMM_WORLD, MPI_ERRORS_RETURN));
  MPI_CHECK(MPI_Comm_rank(MPI_COMM_WORLD, &myrank));
  MPI_CHECK(MPI_Comm_size(MPI_COMM_WORLD, &tasknum));
#endif

  /* Read jobs by the manager. */
  int ret = 0;
#ifdef CMD_MPI
  if (myrank == 0) {
#endif
    ret = read_jobs(argv[1]);
    if (ret == 0) {
      printf("%d jobs are found in the list file.\n", cstat.num);
      ret = snprintf(cstat.fname_rst, CMD_BUF, "%s.rst", argv[1]);
      if (ret < 0 || ret >= CMD_BUF) {
        MSG_ERR("the filename is too long to create the restart file.\n");
        ret = ERR_STRING;
      }
      else ret = 0;
    }
    if (ret == 0) {
      if (!(cstat.status = calloc(cstat.num, sizeof(char)))) {
        MSG_ERR("failed to allocate memory for handling jobs.\n");
        ret = ERR_MEMORY;
      }
    }

#ifdef CMD_MPI
  }

  /* Synchronize jobs. */
  MPI_CHECK(MPI_Bcast(&ret, 1, MPI_INT, 0, MPI_COMM_WORLD));
  if (ret) CLEAN_UP(ret);

  MPI_CHECK(MPI_Bcast(&cstat.len, 1, MPI_INT, 0, MPI_COMM_WORLD));
  MPI_CHECK(MPI_Bcast(&cstat.num, 1, MPI_INT, 0, MPI_COMM_WORLD));

  if (myrank != 0) {
    if (!(cstat.cmd = calloc(cstat.num, cstat.len))) ret = ERR_MEMORY;
  }
  int err;
  MPI_CHECK(MPI_Allreduce(&ret, &err, 1, MPI_INT, MPI_MAX, MPI_COMM_WORLD));
  if (err) CLEAN_UP(err);

  MPI_CHECK(MPI_Bcast(cstat.cmd, cstat.num * cstat.len,
      MPI_CHAR, 0, MPI_COMM_WORLD));

  /* Signal handling by manager. */
  if (myrank == 0) {
    printf("Parallelizing jobs with MPI: %d tasks.\n", tasknum);
    fflush(stdout);
    mpi_manager(tasknum);       /* manager runs jobs as well */
  }
  else mpi_worker();

  check_signal();
  MPI_CHECK(MPI_Allreduce(&term, &err, 1, MPI_INT, MPI_MAX, MPI_COMM_WORLD));
  term = err;
#else
  if (ret) CLEAN_UP(ret);

  CHILD_INFO ci;
  char *cmd;
  int id = 0;

#ifdef CMD_OMP
  printf("Parallelizing jobs with OpenMP: %d threads.\n",
      omp_get_max_threads());
#pragma omp parallel for private(ci,cmd) firstprivate(id) schedule(dynamic)
#endif
  for (int i = 0; i < cstat.num; i++) {
    int stop, worker = 0;
    int job_status = JOB_START;
#ifdef CMD_OMP
    worker = omp_get_thread_num();
#pragma omp critical
#endif
    {
      if (!(stop = check_jobs())) {
        cmd = cstat.cmd + (size_t) i * cstat.len;
        printf("-> Allocating command to thread %d (job index: %d):\n   %s\n",
            worker, id, cmd);
        cstat.status[i] = JOB_START;
        if (create_child(cmd, &ci)) job_status = JOB_FAIL;
      }
    }
    if (stop) continue;

    while (job_status == JOB_START) {
#ifdef CMD_OMP
#pragma omp critical
#endif
      { stop = check_jobs(); }
      job_status = close_child(&ci, worker, id, stop);
      if (job_status == JOB_START) idle_wait();
    }

#ifdef CMD_OMP
#pragma omp critical
#endif
    { cstat.status[i] = job_status; }
    id++;
  }

#endif

#ifdef CMD_MPI
  if (myrank == 0) {
#endif
    check_jobs();

    if (save_err) ret = save_err;
    else if (term) ret = 128 + term;
    else {
      for (int i = 0; i < cstat.num; i++) {
        if (cstat.status[i] != JOB_DONE) {
          ret = ERR_CMD;
          break;
        }
      }
      if (ret && save_jobs()) ret = ERR_FILE;
    }

#ifdef CMD_MPI
  }

  MPI_CHECK(MPI_Bcast(&ret, 1, MPI_INT, 0, MPI_COMM_WORLD));
#endif

  CLEAN_UP(ret);
}


/******************************************************************************
Function `check_signal`:
  Check if a termination signal has been received.
Return:
  Zero if no stop is requested; otherwise the detected signal number.
******************************************************************************/
int check_signal(void) {
  if (term) return term;

  sigset_t pending;
  if (sigpending(&pending)) {
    MSG_ERR("failed to inspect pending signals: %s\n", strerror(errno));
    term = SIGTERM;
    return term;
  }

  for (size_t i = 0; i < sizeof(stop_signals) / sizeof(*stop_signals); i++) {
    if (sigismember(&pending, stop_signals[i]) == 1) {
      term = stop_signals[i];
      break;
    }

    /* A handler on another thread may have already consumed the signal. */
    struct sigaction sa;
    if (sigaction(stop_signals[i], NULL, &sa)) {
      MSG_ERR("failed to inspect signal disposition: %s\n", strerror(errno));
      term = SIGTERM;
      break;
    }
    if (sa.sa_handler == SIG_IGN) {
      term = stop_signals[i];
      break;
    }
  }
  return term;
}


/******************************************************************************
Function `check_jobs`:
  Check for a stop request and save the restart file if needed.
Return:
  Zero if no stop is requested; otherwise the stop signal number.
******************************************************************************/
int check_jobs(void) {
  static int saved;
  if (check_signal() && !saved) {
    saved = 1;          /* prevent retrying failed save */
    save_err = save_jobs();
  }
  return term;
}


/******************************************************************************
Function `idle_wait`:
  Sleep shortly to avoid busy-waiting between job loop checks.
******************************************************************************/
void idle_wait(void) {
  const struct timespec delay = {IDLE_MSEC / 1000,
      (IDLE_MSEC % 1000) * 1000000L};
  nanosleep(&delay, NULL);
}
