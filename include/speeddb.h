/*
 * SpeedDB - Ultra-fast file-based local database
 * Copyright (c) 2024-2025
 *
 * Main public API header
 */

#ifndef SPEEDDB_H
#define SPEEDDB_H

#include <stdint.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Version info */
#define SPEEDDB_VERSION_MAJOR 0
#define SPEEDDB_VERSION_MINOR 1
#define SPEEDDB_VERSION_PATCH 0
#define SPEEDDB_VERSION "0.1.0"

/* Export macros */
#ifdef _WIN32
    #ifdef SPEEDDB_EXPORTS
        #define SPEEDDB_API __declspec(dllexport)
    #else
        #define SPEEDDB_API __declspec(dllimport)
    #endif
#else
    #define SPEEDDB_API __attribute__((visibility("default")))
#endif

/* Forward declarations */
typedef struct speeddb speeddb;
typedef struct speeddb_stmt speeddb_stmt;
typedef struct speeddb_value speeddb_value;

/* Result codes */
typedef enum {
    SPEEDDB_OK = 0,
    SPEEDDB_ERROR = 1,
    SPEEDDB_BUSY = 2,
    SPEEDDB_LOCKED = 3,
    SPEEDDB_NOMEM = 4,
    SPEEDDB_READONLY = 5,
    SPEEDDB_IOERR = 6,
    SPEEDDB_CORRUPT = 7,
    SPEEDDB_NOTFOUND = 8,
    SPEEDDB_FULL = 9,
    SPEEDDB_CANTOPEN = 10,
    SPEEDDB_CONSTRAINT = 11,
    SPEEDDB_MISMATCH = 12,
    SPEEDDB_MISUSE = 13,
    SPEEDDB_RANGE = 14,
    SPEEDDB_ROW = 100,
    SPEEDDB_DONE = 101
} speeddb_result;

/* Data types */
typedef enum {
    SPEEDDB_TYPE_NULL = 0,
    SPEEDDB_TYPE_INT = 1,
    SPEEDDB_TYPE_FLOAT = 2,
    SPEEDDB_TYPE_TEXT = 3,
    SPEEDDB_TYPE_BLOB = 4,
    SPEEDDB_TYPE_JSON = 5,
    SPEEDDB_TYPE_VECTOR = 6
} speeddb_type;

/* Open flags */
typedef enum {
    SPEEDDB_OPEN_READONLY    = 0x00000001,
    SPEEDDB_OPEN_READWRITE   = 0x00000002,
    SPEEDDB_OPEN_CREATE      = 0x00000004,
    SPEEDDB_OPEN_MEMORY      = 0x00000008,
    SPEEDDB_OPEN_NOMUTEX     = 0x00000010,
    SPEEDDB_OPEN_FULLMUTEX   = 0x00000020,
    SPEEDDB_OPEN_WAL         = 0x00000040
} speeddb_open_flags;

/* ============================================================================
 * Database Connection API
 * ============================================================================ */

/* Open a database connection */
SPEEDDB_API int speeddb_open(
    const char* filename,
    speeddb** db
);

/* Open with flags */
SPEEDDB_API int speeddb_open_v2(
    const char* filename,
    speeddb** db,
    int flags,
    const char* vfs
);

/* Close database connection */
SPEEDDB_API int speeddb_close(speeddb* db);

/* Get last error message */
SPEEDDB_API const char* speeddb_errmsg(speeddb* db);

/* Get last error code */
SPEEDDB_API int speeddb_errcode(speeddb* db);

/* ============================================================================
 * SQL Execution API
 * ============================================================================ */

/* Execute SQL directly (no result) */
SPEEDDB_API int speeddb_exec(
    speeddb* db,
    const char* sql,
    int (*callback)(void*, int, char**, char**),
    void* arg,
    char** errmsg
);

/* Prepare a statement */
SPEEDDB_API int speeddb_prepare(
    speeddb* db,
    const char* sql,
    int sql_len,
    speeddb_stmt** stmt,
    const char** tail
);

/* Step through results */
SPEEDDB_API int speeddb_step(speeddb_stmt* stmt);

/* Reset statement for re-execution */
SPEEDDB_API int speeddb_reset(speeddb_stmt* stmt);

/* Finalize (destroy) statement */
SPEEDDB_API int speeddb_finalize(speeddb_stmt* stmt);

/* ============================================================================
 * Parameter Binding API
 * ============================================================================ */

SPEEDDB_API int speeddb_bind_null(speeddb_stmt* stmt, int idx);
SPEEDDB_API int speeddb_bind_int(speeddb_stmt* stmt, int idx, int value);
SPEEDDB_API int speeddb_bind_int64(speeddb_stmt* stmt, int idx, int64_t value);
SPEEDDB_API int speeddb_bind_double(speeddb_stmt* stmt, int idx, double value);
SPEEDDB_API int speeddb_bind_text(speeddb_stmt* stmt, int idx, const char* value, int len, void(*destructor)(void*));
SPEEDDB_API int speeddb_bind_blob(speeddb_stmt* stmt, int idx, const void* value, int len, void(*destructor)(void*));
SPEEDDB_API int speeddb_bind_json(speeddb_stmt* stmt, int idx, const char* json, int len);
SPEEDDB_API int speeddb_bind_vector(speeddb_stmt* stmt, int idx, const float* vec, int dimensions);

/* ============================================================================
 * Column Access API
 * ============================================================================ */

SPEEDDB_API int speeddb_column_count(speeddb_stmt* stmt);
SPEEDDB_API const char* speeddb_column_name(speeddb_stmt* stmt, int col);
SPEEDDB_API int speeddb_column_type(speeddb_stmt* stmt, int col);

SPEEDDB_API int speeddb_column_int(speeddb_stmt* stmt, int col);
SPEEDDB_API int64_t speeddb_column_int64(speeddb_stmt* stmt, int col);
SPEEDDB_API double speeddb_column_double(speeddb_stmt* stmt, int col);
SPEEDDB_API const unsigned char* speeddb_column_text(speeddb_stmt* stmt, int col);
SPEEDDB_API const void* speeddb_column_blob(speeddb_stmt* stmt, int col);
SPEEDDB_API int speeddb_column_bytes(speeddb_stmt* stmt, int col);
SPEEDDB_API const char* speeddb_column_json(speeddb_stmt* stmt, int col);
SPEEDDB_API const float* speeddb_column_vector(speeddb_stmt* stmt, int col, int* dimensions);

/* ============================================================================
 * Transaction API
 * ============================================================================ */

SPEEDDB_API int speeddb_begin(speeddb* db);
SPEEDDB_API int speeddb_commit(speeddb* db);
SPEEDDB_API int speeddb_rollback(speeddb* db);

/* ============================================================================
 * Utility API
 * ============================================================================ */

/* Get number of rows changed by last statement */
SPEEDDB_API int speeddb_changes(speeddb* db);

/* Get total rows changed since connection opened */
SPEEDDB_API int64_t speeddb_total_changes(speeddb* db);

/* Get last inserted rowid */
SPEEDDB_API int64_t speeddb_last_insert_rowid(speeddb* db);

/* Memory management */
SPEEDDB_API void speeddb_free(void* ptr);

/* ============================================================================
 * Modern Features API
 * ============================================================================ */

/* JSON path query */
SPEEDDB_API int speeddb_json_extract(
    speeddb* db,
    const char* json,
    const char* path,
    char** result
);

/* Vector similarity search */
SPEEDDB_API int speeddb_vector_search(
    speeddb* db,
    const char* table,
    const char* column,
    const float* query_vector,
    int dimensions,
    int top_k,
    speeddb_stmt** result
);

/* Full-text search */
SPEEDDB_API int speeddb_fts_search(
    speeddb* db,
    const char* table,
    const char* query,
    speeddb_stmt** result
);

#ifdef __cplusplus
}
#endif

#endif /* SPEEDDB_H */
