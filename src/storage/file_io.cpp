/*
 * SpeedSQL - Cross-platform file I/O
 */

#include "speedsql_internal.h"

#ifdef _WIN32

/* Windows implementation */
void mutex_init(mutex_t* m) {
    InitializeCriticalSection(m);
}

void mutex_destroy(mutex_t* m) {
    DeleteCriticalSection(m);
}

void mutex_lock(mutex_t* m) {
    EnterCriticalSection(m);
}

void mutex_unlock(mutex_t* m) {
    LeaveCriticalSection(m);
}

void rwlock_init(rwlock_t* rw) {
    InitializeSRWLock(rw);
}

void rwlock_destroy(rwlock_t* rw) {
    (void)rw;  /* SRWLock doesn't need destruction */
}

void rwlock_rdlock(rwlock_t* rw) {
    AcquireSRWLockShared(rw);
}

void rwlock_wrlock(rwlock_t* rw) {
    AcquireSRWLockExclusive(rw);
}

void rwlock_unlock(rwlock_t* rw) {
    /* Note: In real implementation, we need to track if shared or exclusive */
    ReleaseSRWLockExclusive(rw);
}

int file_open(file_t* f, const char* path, int flags) {
    if (!f || !path) return SPEEDSQL_MISUSE;

    memset(f, 0, sizeof(*f));
    f->path = sdb_strdup(path);
    if (!f->path) return SPEEDSQL_NOMEM;

    rwlock_init(&f->lock);

    DWORD access = 0;
    DWORD share = FILE_SHARE_READ;
    DWORD creation = 0;

    if (flags & 1) {  /* Read-write */
        access = GENERIC_READ | GENERIC_WRITE;
    } else {
        access = GENERIC_READ;
        f->readonly = true;
    }

    if (flags & 2) {  /* Create */
        creation = OPEN_ALWAYS;
    } else {
        creation = OPEN_EXISTING;
    }

    f->handle = CreateFileA(
        path,
        access,
        share,
        NULL,
        creation,
        FILE_ATTRIBUTE_NORMAL | FILE_FLAG_RANDOM_ACCESS,
        NULL
    );

    if (f->handle == INVALID_HANDLE_VALUE) {
        sdb_free(f->path);
        f->path = nullptr;
        return SPEEDSQL_CANTOPEN;
    }

    /* Get file size */
    LARGE_INTEGER size;
    if (GetFileSizeEx(f->handle, &size)) {
        f->size = size.QuadPart;
    }

    return SPEEDSQL_OK;
}

int file_close(file_t* f) {
    if (!f) return SPEEDSQL_MISUSE;

    if (f->handle != INVALID_HANDLE_VALUE) {
        CloseHandle(f->handle);
        f->handle = INVALID_HANDLE_VALUE;
    }

    if (f->path) {
        sdb_free(f->path);
        f->path = nullptr;
    }

    rwlock_destroy(&f->lock);
    return SPEEDSQL_OK;
}

int file_read(file_t* f, uint64_t offset, void* buf, size_t len) {
    if (!f || f->handle == INVALID_HANDLE_VALUE) return SPEEDSQL_MISUSE;

    rwlock_rdlock(&f->lock);

    OVERLAPPED ov = {0};
    ov.Offset = (DWORD)(offset & 0xFFFFFFFF);
    ov.OffsetHigh = (DWORD)(offset >> 32);

    DWORD read = 0;
    BOOL ok = ReadFile(f->handle, buf, (DWORD)len, &read, &ov);

    rwlock_unlock(&f->lock);

    if (!ok || read != len) {
        return SPEEDSQL_IOERR;
    }

    return SPEEDSQL_OK;
}

int file_write(file_t* f, uint64_t offset, const void* buf, size_t len) {
    if (!f || f->handle == INVALID_HANDLE_VALUE) return SPEEDSQL_MISUSE;
    if (f->readonly) return SPEEDSQL_READONLY;

    rwlock_wrlock(&f->lock);

    OVERLAPPED ov = {0};
    ov.Offset = (DWORD)(offset & 0xFFFFFFFF);
    ov.OffsetHigh = (DWORD)(offset >> 32);

    DWORD written = 0;
    BOOL ok = WriteFile(f->handle, buf, (DWORD)len, &written, &ov);

    if (ok && offset + len > f->size) {
        f->size = offset + len;
    }

    rwlock_unlock(&f->lock);

    if (!ok || written != len) {
        return SPEEDSQL_IOERR;
    }

    return SPEEDSQL_OK;
}

int file_sync(file_t* f) {
    if (!f || f->handle == INVALID_HANDLE_VALUE) return SPEEDSQL_MISUSE;

    if (!FlushFileBuffers(f->handle)) {
        return SPEEDSQL_IOERR;
    }

    return SPEEDSQL_OK;
}

int file_truncate(file_t* f, uint64_t size) {
    if (!f || f->handle == INVALID_HANDLE_VALUE) return SPEEDSQL_MISUSE;
    if (f->readonly) return SPEEDSQL_READONLY;

    LARGE_INTEGER li;
    li.QuadPart = size;

    if (!SetFilePointerEx(f->handle, li, NULL, FILE_BEGIN)) {
        return SPEEDSQL_IOERR;
    }

    if (!SetEndOfFile(f->handle)) {
        return SPEEDSQL_IOERR;
    }

    f->size = size;
    return SPEEDSQL_OK;
}

int file_size(file_t* f, uint64_t* size) {
    if (!f || !size) return SPEEDSQL_MISUSE;
    *size = f->size;
    return SPEEDSQL_OK;
}

uint64_t get_timestamp_us(void) {
    LARGE_INTEGER freq, counter;
    QueryPerformanceFrequency(&freq);
    QueryPerformanceCounter(&counter);
    return (uint64_t)(counter.QuadPart * 1000000 / freq.QuadPart);
}

#else

/* POSIX implementation */
#include <sys/stat.h>
#include <sys/mman.h>
#include <errno.h>
#include <time.h>

void mutex_init(mutex_t* m) {
    pthread_mutex_init(m, NULL);
}

void mutex_destroy(mutex_t* m) {
    pthread_mutex_destroy(m);
}

void mutex_lock(mutex_t* m) {
    pthread_mutex_lock(m);
}

void mutex_unlock(mutex_t* m) {
    pthread_mutex_unlock(m);
}

void rwlock_init(rwlock_t* rw) {
    pthread_rwlock_init(rw, NULL);
}

void rwlock_destroy(rwlock_t* rw) {
    pthread_rwlock_destroy(rw);
}

void rwlock_rdlock(rwlock_t* rw) {
    pthread_rwlock_rdlock(rw);
}

void rwlock_wrlock(rwlock_t* rw) {
    pthread_rwlock_wrlock(rw);
}

void rwlock_unlock(rwlock_t* rw) {
    pthread_rwlock_unlock(rw);
}

int file_open(file_t* f, const char* path, int flags) {
    if (!f || !path) return SPEEDSQL_MISUSE;

    memset(f, 0, sizeof(*f));
    f->path = sdb_strdup(path);
    if (!f->path) return SPEEDSQL_NOMEM;

    rwlock_init(&f->lock);

    int oflags = 0;
    if (flags & 1) {
        oflags = O_RDWR;
    } else {
        oflags = O_RDONLY;
        f->readonly = true;
    }

    if (flags & 2) {
        oflags |= O_CREAT;
    }

    f->handle = open(path, oflags, 0644);
    if (f->handle == INVALID_FILE_HANDLE) {
        sdb_free(f->path);
        f->path = nullptr;
        return SPEEDSQL_CANTOPEN;
    }

    /* Get file size */
    struct stat st;
    if (fstat(f->handle, &st) == 0) {
        f->size = st.st_size;
    }

    return SPEEDSQL_OK;
}

int file_close(file_t* f) {
    if (!f) return SPEEDSQL_MISUSE;

    if (f->handle != INVALID_FILE_HANDLE) {
        close(f->handle);
        f->handle = INVALID_FILE_HANDLE;
    }

    if (f->path) {
        sdb_free(f->path);
        f->path = nullptr;
    }

    rwlock_destroy(&f->lock);
    return SPEEDSQL_OK;
}

int file_read(file_t* f, uint64_t offset, void* buf, size_t len) {
    if (!f || f->handle == INVALID_FILE_HANDLE) return SPEEDSQL_MISUSE;

    rwlock_rdlock(&f->lock);

    ssize_t n = pread(f->handle, buf, len, offset);

    rwlock_unlock(&f->lock);

    if (n != (ssize_t)len) {
        return SPEEDSQL_IOERR;
    }

    return SPEEDSQL_OK;
}

int file_write(file_t* f, uint64_t offset, const void* buf, size_t len) {
    if (!f || f->handle == INVALID_FILE_HANDLE) return SPEEDSQL_MISUSE;
    if (f->readonly) return SPEEDSQL_READONLY;

    rwlock_wrlock(&f->lock);

    ssize_t n = pwrite(f->handle, buf, len, offset);

    if (n > 0 && offset + len > f->size) {
        f->size = offset + len;
    }

    rwlock_unlock(&f->lock);

    if (n != (ssize_t)len) {
        return SPEEDSQL_IOERR;
    }

    return SPEEDSQL_OK;
}

int file_sync(file_t* f) {
    if (!f || f->handle == INVALID_FILE_HANDLE) return SPEEDSQL_MISUSE;

    if (fsync(f->handle) != 0) {
        return SPEEDSQL_IOERR;
    }

    return SPEEDSQL_OK;
}

int file_truncate(file_t* f, uint64_t size) {
    if (!f || f->handle == INVALID_FILE_HANDLE) return SPEEDSQL_MISUSE;
    if (f->readonly) return SPEEDSQL_READONLY;

    if (ftruncate(f->handle, size) != 0) {
        return SPEEDSQL_IOERR;
    }

    f->size = size;
    return SPEEDSQL_OK;
}

int file_size(file_t* f, uint64_t* size) {
    if (!f || !size) return SPEEDSQL_MISUSE;
    *size = f->size;
    return SPEEDSQL_OK;
}

uint64_t get_timestamp_us(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000 + ts.tv_nsec / 1000;
}

#endif
