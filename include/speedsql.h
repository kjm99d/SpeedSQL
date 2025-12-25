/*
 * SpeedSQL - Ultra-fast file-based local database
 * Copyright (c) 2024-2025
 *
 * Main public API header
 */

#ifndef SPEEDSQL_H
#define SPEEDSQL_H

#include <stdint.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Version info */
#define SPEEDSQL_VERSION_MAJOR 0
#define SPEEDSQL_VERSION_MINOR 1
#define SPEEDSQL_VERSION_PATCH 0
#define SPEEDSQL_VERSION "0.1.0"

/* Export macros */
#ifdef _WIN32
    #ifdef SPEEDSQL_STATIC
        #define SPEEDSQL_API
    #elif defined(SPEEDSQL_EXPORTS)
        #define SPEEDSQL_API __declspec(dllexport)
    #else
        #define SPEEDSQL_API __declspec(dllimport)
    #endif
#else
    #define SPEEDSQL_API __attribute__((visibility("default")))
#endif

/* Forward declarations */
typedef struct speedsql speedsql;
typedef struct speedsql_stmt speedsql_stmt;
typedef struct speedsql_value speedsql_value;

/* Result codes */
typedef enum {
    SPEEDSQL_OK = 0,
    SPEEDSQL_ERROR = 1,
    SPEEDSQL_BUSY = 2,
    SPEEDSQL_LOCKED = 3,
    SPEEDSQL_NOMEM = 4,
    SPEEDSQL_READONLY = 5,
    SPEEDSQL_IOERR = 6,
    SPEEDSQL_CORRUPT = 7,
    SPEEDSQL_NOTFOUND = 8,
    SPEEDSQL_FULL = 9,
    SPEEDSQL_CANTOPEN = 10,
    SPEEDSQL_CONSTRAINT = 11,
    SPEEDSQL_MISMATCH = 12,
    SPEEDSQL_MISUSE = 13,
    SPEEDSQL_RANGE = 14,
    SPEEDSQL_ROW = 100,
    SPEEDSQL_DONE = 101
} speedsql_result;

/* Data types */
typedef enum {
    SPEEDSQL_TYPE_NULL = 0,
    SPEEDSQL_TYPE_INT = 1,
    SPEEDSQL_TYPE_FLOAT = 2,
    SPEEDSQL_TYPE_TEXT = 3,
    SPEEDSQL_TYPE_BLOB = 4,
    SPEEDSQL_TYPE_JSON = 5,
    SPEEDSQL_TYPE_VECTOR = 6
} speedsql_type;

/* Open flags */
typedef enum {
    SPEEDSQL_OPEN_READONLY    = 0x00000001,
    SPEEDSQL_OPEN_READWRITE   = 0x00000002,
    SPEEDSQL_OPEN_CREATE      = 0x00000004,
    SPEEDSQL_OPEN_MEMORY      = 0x00000008,
    SPEEDSQL_OPEN_NOMUTEX     = 0x00000010,
    SPEEDSQL_OPEN_FULLMUTEX   = 0x00000020,
    SPEEDSQL_OPEN_WAL         = 0x00000040
} speedsql_open_flags;

/* ============================================================================
 * Database Connection API
 * ============================================================================ */

/* Open a database connection */
SPEEDSQL_API int speedsql_open(
    const char* filename,
    speedsql** db
);

/* Open with flags */
SPEEDSQL_API int speedsql_open_v2(
    const char* filename,
    speedsql** db,
    int flags,
    const char* vfs
);

/* Close database connection */
SPEEDSQL_API int speedsql_close(speedsql* db);

/* Get last error message */
SPEEDSQL_API const char* speedsql_errmsg(speedsql* db);

/* Get last error code */
SPEEDSQL_API int speedsql_errcode(speedsql* db);

/* ============================================================================
 * SQL Execution API
 * ============================================================================ */

/* Execute SQL directly (no result) */
SPEEDSQL_API int speedsql_exec(
    speedsql* db,
    const char* sql,
    int (*callback)(void*, int, char**, char**),
    void* arg,
    char** errmsg
);

/* Prepare a statement */
SPEEDSQL_API int speedsql_prepare(
    speedsql* db,
    const char* sql,
    int sql_len,
    speedsql_stmt** stmt,
    const char** tail
);

/* Step through results */
SPEEDSQL_API int speedsql_step(speedsql_stmt* stmt);

/* Reset statement for re-execution */
SPEEDSQL_API int speedsql_reset(speedsql_stmt* stmt);

/* Finalize (destroy) statement */
SPEEDSQL_API int speedsql_finalize(speedsql_stmt* stmt);

/* ============================================================================
 * Parameter Binding API
 * ============================================================================ */

SPEEDSQL_API int speedsql_bind_null(speedsql_stmt* stmt, int idx);
SPEEDSQL_API int speedsql_bind_int(speedsql_stmt* stmt, int idx, int value);
SPEEDSQL_API int speedsql_bind_int64(speedsql_stmt* stmt, int idx, int64_t value);
SPEEDSQL_API int speedsql_bind_double(speedsql_stmt* stmt, int idx, double value);
SPEEDSQL_API int speedsql_bind_text(speedsql_stmt* stmt, int idx, const char* value, int len, void(*destructor)(void*));
SPEEDSQL_API int speedsql_bind_blob(speedsql_stmt* stmt, int idx, const void* value, int len, void(*destructor)(void*));
SPEEDSQL_API int speedsql_bind_json(speedsql_stmt* stmt, int idx, const char* json, int len);
SPEEDSQL_API int speedsql_bind_vector(speedsql_stmt* stmt, int idx, const float* vec, int dimensions);

/* ============================================================================
 * Column Access API
 * ============================================================================ */

SPEEDSQL_API int speedsql_column_count(speedsql_stmt* stmt);
SPEEDSQL_API const char* speedsql_column_name(speedsql_stmt* stmt, int col);
SPEEDSQL_API int speedsql_column_type(speedsql_stmt* stmt, int col);

SPEEDSQL_API int speedsql_column_int(speedsql_stmt* stmt, int col);
SPEEDSQL_API int64_t speedsql_column_int64(speedsql_stmt* stmt, int col);
SPEEDSQL_API double speedsql_column_double(speedsql_stmt* stmt, int col);
SPEEDSQL_API const unsigned char* speedsql_column_text(speedsql_stmt* stmt, int col);
SPEEDSQL_API const void* speedsql_column_blob(speedsql_stmt* stmt, int col);
SPEEDSQL_API int speedsql_column_bytes(speedsql_stmt* stmt, int col);
SPEEDSQL_API const char* speedsql_column_json(speedsql_stmt* stmt, int col);
SPEEDSQL_API const float* speedsql_column_vector(speedsql_stmt* stmt, int col, int* dimensions);

/* ============================================================================
 * Transaction API
 * ============================================================================ */

SPEEDSQL_API int speedsql_begin(speedsql* db);
SPEEDSQL_API int speedsql_commit(speedsql* db);
SPEEDSQL_API int speedsql_rollback(speedsql* db);

/* ============================================================================
 * Utility API
 * ============================================================================ */

/* Get number of rows changed by last statement */
SPEEDSQL_API int speedsql_changes(speedsql* db);

/* Get total rows changed since connection opened */
SPEEDSQL_API int64_t speedsql_total_changes(speedsql* db);

/* Get last inserted rowid */
SPEEDSQL_API int64_t speedsql_last_insert_rowid(speedsql* db);

/* Memory management */
SPEEDSQL_API void speedsql_free(void* ptr);

/* ============================================================================
 * Modern Features API
 * ============================================================================ */

/* JSON path query */
SPEEDSQL_API int speedsql_json_extract(
    speedsql* db,
    const char* json,
    const char* path,
    char** result
);

/* Vector similarity search */
SPEEDSQL_API int speedsql_vector_search(
    speedsql* db,
    const char* table,
    const char* column,
    const float* query_vector,
    int dimensions,
    int top_k,
    speedsql_stmt** result
);

/* Full-text search */
SPEEDSQL_API int speedsql_fts_search(
    speedsql* db,
    const char* table,
    const char* query,
    speedsql_stmt** result
);

#ifdef __cplusplus
}
#endif

#endif /* SPEEDSQL_H */
