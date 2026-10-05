#include "jobfork.h"
#include <mpi.h>

#define TAG_JOB_INDEX   0
#define TAG_JOB_RESULT  1       /* {job index, job status} */
#define TAG_STOP        2
#define TAG_SIGNAL      3

typedef struct {
  int ready;            /* non-zero if the worker is ready for a new job */
  int count;            /* number of job assigned */
  int jobid;            /* current job index */
  int report[2];        /* job index and completion status */
  int signal;           /* worker stop notice */
  MPI_Request result, command, notice, stop;
} WORKER;


/******************************************************************************
Function `mpi_manager`:
  Schedule commands and execute task-private jobs simultaneously.
Arguments:
  * `tasknum`:  total number of MPI tasks.
******************************************************************************/
void mpi_manager(const int tasknum) {
  WORKER *worker = calloc(tasknum, sizeof(WORKER));
  if (!worker) {
    MSG_ERR("failed to allocate memory for the manager.\n");
    MPI_Abort(MPI_COMM_WORLD, ERR_MEMORY);
    _Exit(ERR_MEMORY);
  }

  /* Send jobs to free workers. */
  for (int i = 0; i < tasknum; i++) {
    worker[i].ready = 1;
    worker[i].jobid = -1;
    worker[i].result = worker[i].command = MPI_REQUEST_NULL;
    worker[i].notice = worker[i].stop = MPI_REQUEST_NULL;
    if (i) {            /* receive stop signals from workers */
      MPI_CHECK(MPI_Irecv(&worker[i].signal, 1, MPI_INT, i, TAG_SIGNAL,
          MPI_COMM_WORLD, &worker[i].notice))
    }
  }

  CHILD_INFO ci;
  int next, stopping, stop_sig;
  next = stopping = stop_sig = 0;
  for (;;) {
    int progress = 0, ready = 0;

    /* Collect worker stop notices and job reports. */
    for (int i = 1; i < tasknum; i++) {
      int received;
      if (worker[i].notice != MPI_REQUEST_NULL) {
        MPI_CHECK(MPI_Test(&worker[i].notice, &received, MPI_STATUS_IGNORE));
        if (received && worker[i].signal && !term) term = worker[i].signal;
      }

      if (worker[i].result != MPI_REQUEST_NULL) {
        MPI_CHECK(MPI_Test(&worker[i].result, &received, MPI_STATUS_IGNORE));
        if (received) {
          int idx = worker[i].report[0];
          if (idx != worker[i].jobid || idx < 0 || idx >= cstat.num) {
            MSG_ERR("unexpected job index from task %d: %d.\n", i, idx);
            MPI_Abort(MPI_COMM_WORLD, ERR_MPI);
            _Exit(ERR_MPI);
          }
          cstat.status[idx] = worker[i].report[1];
          worker[i].ready = progress = 1;
        }
      }
    }

    /* Send a stop request to all workers. */
    check_jobs();       /* save before broadcasting termination signal */
    if (term && !stopping) {
      stop_sig = term;
      for (int i = 1; i < tasknum; i++) {
        MPI_CHECK(MPI_Isend(&stop_sig, 1, MPI_INT, i, TAG_STOP,
            MPI_COMM_WORLD, &worker[i].stop));
      }
      stopping = 1;
    }

    /* Check the job run locally by the manager. */
    if (!worker[0].ready) {
      int status = close_child(&ci, 0, worker[0].count - 1, term);
      if (status != JOB_START) {
        cstat.status[worker[0].jobid] = status;
        worker[0].ready = progress = 1;
      }
    }

    /* Assign pending jobs to available workers. */
    for (int i = 0; i < tasknum; i++) {
      if (!worker[i].ready) continue;
      int sent;
      MPI_CHECK(MPI_Test(&worker[i].command, &sent, MPI_STATUS_IGNORE));
      if (!term && next < cstat.num && sent) {
        worker[i].jobid = next++;
        cstat.status[worker[i].jobid] = JOB_START;
        char *cmd = cstat.cmd + (size_t) worker[i].jobid * cstat.len;
        printf("-> Allocating command to task %d (job index: %d):\n   %s\n",
            i, worker[i].count, cmd);
        fflush(stdout);

        worker[i].count++;
        worker[i].ready = 0;
        if (i == 0) {
          if (create_child(cmd, &ci)) {
            cstat.status[worker[i].jobid] = JOB_FAIL;
            worker[i].ready = 1;
          }
        }
        else {
          MPI_CHECK(MPI_Irecv(worker[i].report, 2, MPI_INT, i, TAG_JOB_RESULT,
              MPI_COMM_WORLD, &worker[i].result));
          MPI_CHECK(MPI_Isend(&worker[i].jobid, 1, MPI_INT, i, TAG_JOB_INDEX,
              MPI_COMM_WORLD, &worker[i].command));
        }
        progress = 1;
      }
      ready += worker[i].ready;
    }

    if (ready == tasknum && (term || next == cstat.num)) break;
    if (!progress) idle_wait();
  }

  /* Send negative job index to ask all workers to exit. */
  for (int i = 1; i < tasknum; i++) {
    if (!stopping) {
      MPI_CHECK(MPI_Isend(&stop_sig, 1, MPI_INT, i, TAG_STOP,
          MPI_COMM_WORLD, &worker[i].stop));
    }
    MPI_CHECK(MPI_Wait(&worker[i].command, MPI_STATUS_IGNORE));
    worker[i].jobid = -1;
    MPI_CHECK(MPI_Isend(&worker[i].jobid, 1, MPI_INT, i, TAG_JOB_INDEX,
        MPI_COMM_WORLD, &worker[i].command));
  }
  for (int i = 1; i < tasknum; i++) {
    MPI_CHECK(MPI_Wait(&worker[i].command, MPI_STATUS_IGNORE));
    MPI_CHECK(MPI_Wait(&worker[i].stop, MPI_STATUS_IGNORE));
    MPI_CHECK(MPI_Wait(&worker[i].notice, MPI_STATUS_IGNORE));
    if (worker[i].signal && !term) term = worker[i].signal;
  }

  free(worker);
}


/******************************************************************************
Function `mpi_worker`:
  Run assigned jobs and report completion to the manager.
******************************************************************************/
void mpi_worker(void) {
  CHILD_INFO ci;
  int myrank, idx, cnt, stop_sig, sig_code, report[2];
  int active, done, notified;
  cnt = active = done = notified = 0;
  stop_sig = sig_code = 0;
  MPI_Request jobid, stop, result, notice;
  result = notice = MPI_REQUEST_NULL;

  MPI_CHECK(MPI_Comm_rank(MPI_COMM_WORLD, &myrank));
  MPI_CHECK(MPI_Irecv(&stop_sig, 1, MPI_INT, 0, TAG_STOP,
      MPI_COMM_WORLD, &stop));
  MPI_CHECK(MPI_Irecv(&idx, 1, MPI_INT, 0, TAG_JOB_INDEX,
      MPI_COMM_WORLD, &jobid));

  for (;;) {
    int received, finished, progress;
    finished = progress = 0;

    check_signal();
    if (stop != MPI_REQUEST_NULL) {
      MPI_CHECK(MPI_Test(&stop, &received, MPI_STATUS_IGNORE));
      if (received && stop_sig && !term) term = stop_sig;
    }

    if ((term || done) && !notified) {
      sig_code = term;
      MPI_CHECK(MPI_Isend(&sig_code, 1, MPI_INT, 0, TAG_SIGNAL,
          MPI_COMM_WORLD, &notice));
      notified = 1;
    }

    MPI_CHECK(MPI_Test(&result, &received, MPI_STATUS_IGNORE));
    if (!active && !done && result == MPI_REQUEST_NULL) {
      MPI_CHECK(MPI_Test(&jobid, &received, MPI_STATUS_IGNORE));
      if (received) {
        if (idx < 0) done = 1;
        else {
          report[0] = idx;
          if (term || create_child(cstat.cmd + (size_t) idx * cstat.len, &ci)) {
            report[1] = JOB_FAIL;
            finished = 1;
          }
          else active = 1;
        }
        progress = 1;
      }
    }

    if (active) {
      int status = close_child(&ci, myrank, cnt, term);
      if (status != JOB_START) {
        report[1] = status;
        active = 0;
        finished = 1;
      }
    }

    if (finished) {
      MPI_CHECK(MPI_Irecv(&idx, 1, MPI_INT, 0, TAG_JOB_INDEX,
          MPI_COMM_WORLD, &jobid));
      MPI_CHECK(MPI_Isend(report, 2, MPI_INT, 0, TAG_JOB_RESULT,
          MPI_COMM_WORLD, &result));
      cnt++;
      progress = 1;
    }

    MPI_CHECK(MPI_Test(&notice, &received, MPI_STATUS_IGNORE));
    if (done && stop == MPI_REQUEST_NULL && notified &&
        notice == MPI_REQUEST_NULL && result == MPI_REQUEST_NULL) break;
    if (!progress) idle_wait();
  }
}

