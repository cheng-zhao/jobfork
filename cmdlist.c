#include "jobfork.h"
#include <ctype.h>
#include <fcntl.h>
#include <limits.h>

#define SAVE_ERROR_CLEAN_UP(tmp, fd, err) {                             \
  const int _err = (err);                                               \
  if ((fd) >= 0) close(fd);                                             \
  unlink(tmp);                                                          \
  MSG_ERR("failed to save restart file `%s': %s\n",                     \
      cstat.fname_rst, strerror(_err));                                 \
  return ERR_FILE;                                                      \
}

/******************************************************************************
Function `read_jobs`:
  Read commands (one per non-empty line) from a job list file.
Arguments:
  * `fname`:    path to the job list file.
Return:
  Zero on success; non-zero on error.
******************************************************************************/
int read_jobs(const char *fname) {
  FILE *fp;
  if (!(fp = fopen(fname, "r"))) {
    MSG_ERR("cannot open the job list file `%s'.\n", fname);
    return ERR_FILE;
  }

  /* count the number and maximum length of commands */
  int n = 0;
  int maxlen = 1;
  char *buf, line[CMD_BUF + 2];         /* extra space for CRLF */
  while (fgets(line, sizeof(line), fp) != NULL) {
    /* Exclude long lines. */
    size_t len = strlen(line);
    if (len && line[len - 1] == '\n') len--;
    if (len && line[len - 1] == '\r') len--;
    if (len >= CMD_BUF) {
      MSG_ERR("command length exceeds %d bytes.\n", CMD_BUF - 1);
      fclose(fp);
      return ERR_STRING;
    }
    buf = line;
    while (isspace((unsigned char) *buf)) buf++;
    if (*buf && *buf != COMMENT) {
      int len = (int) strlen(buf) + 1;
      if (maxlen < len) maxlen = len;
      if (n >= INT_MAX / maxlen) {
        fclose(fp);
        return ERR_STRING;
      }
      n++;
    }
  }

  if (ferror(fp)) {
    MSG_ERR("failed to read file `%s'.\n", fname);
    fclose(fp);
    return ERR_FILE;
  }
  if (n < 1) {
    MSG_ERR("no job found in file `%s'.\n", fname);
    fclose(fp);
    return ERR_FILE;
  }

  cstat.num = n;
  cstat.len = maxlen;
  cstat.cmd = calloc((size_t) n * maxlen, sizeof(char));
  if (!cstat.cmd) {
    MSG_ERR("failed to allocate memory for the commands.\n");
    fclose(fp);
    return ERR_MEMORY;
  }

  /* read commands into array */
  if (fseek(fp, 0, SEEK_SET)) {
    MSG_ERR("failed to read file `%s'.\n", fname);
    fclose(fp);
    return ERR_FILE;
  }
  n = 0;
  while (fgets(line, sizeof(line), fp) != NULL) {
    buf = line;
    while (isspace((unsigned char) *buf)) buf++;
    if (*buf && *buf != COMMENT) {
      if (n >= cstat.num || strlen(buf) >= (size_t) maxlen) {
        MSG_ERR("file changed while reading: `%s'.\n", fname);
        fclose(fp);
        return ERR_FILE;
      }
      buf[strcspn(buf, "\r\n")] = '\0';
      strncpy(cstat.cmd + (size_t) n * maxlen, buf, maxlen);
      n++;
    }
  }

  if (ferror(fp) || n != cstat.num) {
    MSG_ERR("unexpected error while reading file: `%s'.\n", fname);
    fclose(fp);
    return ERR_FILE;
  }

  fclose(fp);
  return 0;
}


/******************************************************************************
Function `write_all`:
  Write all requested bytes and retry interrupted writes.
Arguments:
  * `fd`:       POSIX-compatible file descriptor;
  * `buf`:      starting address of contents to be written;
  * `len`:      number of bytes to be written.
Return:
  Zero on success; non-zero on error.
******************************************************************************/
static int write_all(const int fd, const char *buf, size_t len) {
  while (len) {
    ssize_t n = write(fd, buf, len);
    if (n < 0 && errno == EINTR) continue;
    if (n <= 0) {
      if (!n) errno = EIO;
      return ERR_FILE;
    }
    buf += n;
    len -= n;
  }
  return 0;
}


/******************************************************************************
Function `save_jobs`:
  Write unfinished commands to a restart file.
Return:
  Zero on success; non-zero on error.
******************************************************************************/
int save_jobs(void) {
  if (!cstat.cmd || !cstat.status) return ERR_FILE;
  int cnt = 0;
  for (int i = 0; i < cstat.num; i++) {
    if (cstat.status[i] != JOB_DONE) cnt++;
  }
  if (!cnt) return 0;

  /* Write to a temporary file, and then rename it to the restart file. */
  char tmp[CMD_BUF + 8];
  snprintf(tmp, sizeof(tmp), "%s.XXXXXX", cstat.fname_rst);
  int fd = mkstemp(tmp);
  if (fd < 0) {
    MSG_ERR("failed to save restart file `%s': %s\n",
        cstat.fname_rst, strerror(errno));
    return ERR_FILE;
  }

  const char header[] = "# Unfinished jobs\n";
  if (write_all(fd, header, sizeof(header) - 1))
    SAVE_ERROR_CLEAN_UP(tmp, fd, errno);

  for (int i = 0; i < cstat.num; i++) {
    if (cstat.status[i] != JOB_DONE) {
      const char *cmd = cstat.cmd + (size_t) i * cstat.len;
      if (write_all(fd, cmd, strlen(cmd)) || write_all(fd, "\n", 1))
        SAVE_ERROR_CLEAN_UP(tmp, fd, errno);
    }
  }

  if (close(fd) || rename(tmp, cstat.fname_rst))
    SAVE_ERROR_CLEAN_UP(tmp, -1, errno);

  printf("Restart file saved (%d unfinished jobs):\n  %s\n",
      cnt, cstat.fname_rst);
  fflush(stdout);
  return 0;
}
