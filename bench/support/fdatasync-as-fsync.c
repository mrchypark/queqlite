#define _GNU_SOURCE

#include <errno.h>
#include <fcntl.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

static _Atomic unsigned long long intercept_count = 0;

int fdatasync(int fd) {
    atomic_fetch_add_explicit(&intercept_count, 1, memory_order_relaxed);
    return fsync(fd);
}

__attribute__((destructor)) static void write_intercept_count(void) {
    int saved_errno = errno;
    const char *path = getenv("RHIZA_FDATASYNC_COUNT_FILE");
    if (path == NULL || path[0] == '\0') {
        errno = saved_errno;
        return;
    }

    int fd = open(path, O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0644);
    if (fd < 0) {
        errno = saved_errno;
        return;
    }
    char buffer[64];
    int length = snprintf(
        buffer,
        sizeof(buffer),
        "%llu\n",
        atomic_load_explicit(&intercept_count, memory_order_relaxed)
    );
    if (length > 0 && (size_t)length < sizeof(buffer)) {
        const char *cursor = buffer;
        size_t remaining = (size_t)length;
        while (remaining > 0) {
            ssize_t written = write(fd, cursor, remaining);
            if (written < 0) {
                if (errno == EINTR) {
                    continue;
                }
                break;
            }
            cursor += written;
            remaining -= (size_t)written;
        }
        (void)fsync(fd);
    }
    (void)close(fd);
    errno = saved_errno;
}
