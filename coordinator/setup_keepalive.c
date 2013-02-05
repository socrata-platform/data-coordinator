#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/tcp.h>

#include <stdlib.h>
#include <string.h>
#include <errno.h>

static char* my_strerror(int errnum) {
  int strerror_result;
  char* buf;
  size_t size = 128;
  while(1) {
    buf = malloc(size);
    strerror_result = strerror_r(errnum, buf, size);
    if(strerror_result == 0) break;
    free(buf);
    if(errno == EINVAL) return strdup("Unknown error");
    else if(errno == ERANGE) size *= 2;
    else return strdup("Unknown error from strerror_r");
  }
  return buf;
}

char* platform_setup_keepalive(int fd, int keepidle, int keepintvl, int keepcnt) {
  int num;

  num = 1;
  if(setsockopt(fd, SOL_SOCKET, SO_KEEPALIVE, &num, sizeof(num)) == -1)
    return my_strerror(errno);

  num = keepidle;
  if(setsockopt(fd, SOL_TCP, TCP_KEEPIDLE, &num, sizeof(num)) == -1)
    return my_strerror(errno);

  num = keepintvl;
  if(setsockopt(fd, SOL_TCP, TCP_KEEPINTVL, &num, sizeof(num)) == -1)
    return my_strerror(errno);

  num = keepcnt;
  if(setsockopt(fd, SOL_TCP, TCP_KEEPCNT, &num, sizeof(num)) == -1)
    return my_strerror(errno);

  return NULL;
}

void platform_error_free(void* ptr) {
  free(ptr);
}
