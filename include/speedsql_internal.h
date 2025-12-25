/*
 * SpeedSQL - Internal structures and functions
 * Not for public use
 */

#ifndef SPEEDSQL_INTERNAL_H
#define SPEEDSQL_INTERNAL_H

#include "speedsql.h"
#include "speedsql_types.h"
#include "speedsql_crypto.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#ifdef _WIN32
    #include <windows.h>
    typedef HANDLE file_handle_t;
    typedef CRITICAL_SECTION mutex_t;
    typedef CONDITION_VARIABLE cond_t;
    typedef SRWLOCK rwlock_t;
    #define INVALID_FILE_HANDLE INVALID_HANDLE_VALUE
#else
    #include <pthread.h>
    #include <fcntl.h>
    #include <unistd.h>
    typedef int file_handle_t;
    typedef pthread_mutex_t mutex_t;
    typedef pthread_cond_t cond_t;
    typedef pthread_rwlock_t rwlock_t;
    #define INVALID_FILE_HANDLE (-1)
#endif

/* ============================================================================
 * Memory Allocator
 * ============================================================================ */

void* sdb_malloc(size_t size);
void* sdb_realloc(void* ptr, size_t size);
void* sdb_calloc(size_t count, size_t size);
void sdb_free(void* ptr);
char* sdb_strdup(const char* str);

/* ============================================================================
 * Thread Synchronization
 * ============================================================================ */

void mutex_init(mutex_t* m);
void mutex_destroy(mutex_t* m);
void mutex_lock(mutex_t* m);
void mutex_unlock(mutex_t* m);

void rwlock_init(rwlock_t* rw);
void rwlock_destroy(rwlock_t* rw);
void rwlock_rdlock(rwlock_t* rw);
void rwlock_wrlock(rwlock_t* rw);
void rwlock_unlock(rwlock_t* rw);

/* ============================================================================
 * File I/O
 * ============================================================================ */

typedef struct {
    file_handle_t handle;
    char* path;
    uint64_t size;
    bool readonly;
    rwlock_t lock;
} file_t;

int file_open(file_t* f, const char* path, int flags);
int file_close(file_t* f);
int file_read(file_t* f, uint64_t offset, void* buf, size_t len);
int file_write(file_t* f, uint64_t offset, const void* buf, size_t len);
int file_sync(file_t* f);
int file_truncate(file_t* f, uint64_t size);
int file_size(file_t* f, uint64_t* size);

/* ============================================================================
 * Buffer Pool / Page Cache
 * ============================================================================ */

typedef struct buffer_page {
    page_id_t page_id;           /* Page number */
    uint8_t* data;               /* Page data */
    buffer_state_t state;        /* Current state */
    uint32_t pin_count;          /* Number of pins */
    uint64_t last_access;        /* For LRU eviction */
    struct buffer_page* hash_next; /* Hash chain */
    struct buffer_page* lru_prev;  /* LRU list */
    struct buffer_page* lru_next;  /* LRU list */
} buffer_page_t;

typedef struct {
    buffer_page_t** hash_table;  /* Hash table for page lookup */
    size_t hash_size;            /* Hash table size */
    buffer_page_t* lru_head;     /* LRU list head (most recent) */
    buffer_page_t* lru_tail;     /* LRU list tail (least recent) */
    buffer_page_t* free_list;    /* Free page list */
    size_t page_count;           /* Total pages in pool */
    size_t used_count;           /* Pages currently in use */
    uint64_t page_size;          /* Page size */
    uint64_t hits;               /* Cache hits */
    uint64_t misses;             /* Cache misses */
    mutex_t lock;                /* Pool lock */

    /* Encryption support */
    struct speedsql_cipher_ctx* cipher_ctx; /* Cipher context (if encrypted) */
    speedsql_cipher_t cipher_id;            /* Current cipher algorithm */
    uint8_t* crypt_buffer;                  /* Temporary buffer for encrypt/decrypt */
} buffer_pool_t;

int buffer_pool_init(buffer_pool_t* pool, size_t cache_size, uint32_t page_size);
void buffer_pool_destroy(buffer_pool_t* pool);
buffer_page_t* buffer_pool_get(buffer_pool_t* pool, file_t* file, page_id_t page_id);
void buffer_pool_unpin(buffer_pool_t* pool, buffer_page_t* page, bool dirty);
int buffer_pool_flush(buffer_pool_t* pool, file_t* file);
buffer_page_t* buffer_pool_new_page(buffer_pool_t* pool, file_t* file, page_id_t* page_id);
int buffer_pool_invalidate_dirty(buffer_pool_t* pool, file_t* file);
int buffer_pool_set_encryption(buffer_pool_t* pool, struct speedsql_cipher_ctx* ctx, speedsql_cipher_t cipher_id);

/* ============================================================================
 * B+Tree Index
 * ============================================================================ */

typedef struct btree {
    page_id_t root_page;
    uint32_t key_size;
    uint32_t value_size;
    compare_func_t compare;
    buffer_pool_t* pool;
    file_t* file;
    rwlock_t lock;
} btree_t;

typedef struct btree_cursor {
    btree_t* tree;
    page_id_t current_page;
    uint16_t current_slot;
    bool valid;
    bool at_end;
} btree_cursor_t;

int btree_create(btree_t* tree, buffer_pool_t* pool, file_t* file, compare_func_t cmp);
int btree_open(btree_t* tree, buffer_pool_t* pool, file_t* file, page_id_t root, compare_func_t cmp);
void btree_close(btree_t* tree);
int btree_insert(btree_t* tree, const value_t* key, const value_t* value);
int btree_delete(btree_t* tree, const value_t* key);
int btree_find(btree_t* tree, const value_t* key, value_t* value);

/* Cursor operations */
int btree_cursor_init(btree_cursor_t* cursor, btree_t* tree);
int btree_cursor_first(btree_cursor_t* cursor);
int btree_cursor_last(btree_cursor_t* cursor);
int btree_cursor_seek(btree_cursor_t* cursor, const value_t* key);
int btree_cursor_next(btree_cursor_t* cursor);
int btree_cursor_prev(btree_cursor_t* cursor);
int btree_cursor_key(btree_cursor_t* cursor, value_t* key);
int btree_cursor_value(btree_cursor_t* cursor, value_t* value);
void btree_cursor_close(btree_cursor_t* cursor);

/* ============================================================================
 * Write-Ahead Log (WAL)
 * ============================================================================ */

typedef struct {
    file_t file;
    uint64_t current_lsn;        /* Log sequence number */
    uint64_t checkpoint_lsn;     /* Last checkpoint LSN */
    uint8_t* buffer;             /* Write buffer */
    size_t buffer_size;
    size_t buffer_pos;
    mutex_t lock;
} wal_t;

int wal_init(wal_t* wal, const char* path);
void wal_close(wal_t* wal);
int wal_write(wal_t* wal, txn_id_t txn, page_id_t page, const void* before, const void* after, size_t size);
int wal_commit(wal_t* wal, txn_id_t txn);
int wal_rollback(wal_t* wal, txn_id_t txn);
int wal_checkpoint(wal_t* wal, buffer_pool_t* pool, file_t* db_file);
int wal_recover(wal_t* wal, buffer_pool_t* pool, file_t* db_file);
int wal_savepoint(wal_t* wal, txn_id_t txn, uint64_t* lsn_out);
int wal_release_savepoint(wal_t* wal, txn_id_t txn);
int wal_rollback_to_savepoint(wal_t* wal, txn_id_t txn, uint64_t savepoint_lsn);

/* ============================================================================
 * Database Connection Structure
 * ============================================================================ */

/* Forward declare cipher context */
struct speedsql_cipher_ctx;

struct speedsql {
    file_t db_file;              /* Main database file */
    wal_t* wal;                  /* Write-ahead log */
    buffer_pool_t* buffer_pool;  /* Page cache */

    db_header_t header;          /* Database header */

    /* Schema cache */
    table_def_t* tables;
    size_t table_count;
    index_def_t* indices;
    size_t index_count;

    /* Transaction state */
    txn_id_t current_txn;
    txn_state_t txn_state;

    /* Savepoint stack */
    struct savepoint_entry {
        char name[64];               /* Savepoint name */
        uint64_t wal_lsn;            /* WAL LSN at savepoint */
        int64_t last_rowid_saved;    /* Last rowid at savepoint */
        uint64_t total_changes_saved;/* Total changes at savepoint */
    } savepoints[32];                /* Max 32 nested savepoints */
    int savepoint_count;             /* Current savepoint count */

    /* Error state */
    int errcode;
    char errmsg[512];

    /* Configuration */
    uint32_t flags;
    size_t cache_size;

    /* Encryption */
    struct speedsql_cipher_ctx* cipher_ctx;  /* Cipher context */
    speedsql_cipher_t cipher_id;             /* Current cipher */
    bool encrypted;                          /* Is database encrypted */

    /* Statistics */
    uint64_t total_changes;
    int64_t last_rowid;

    /* Thread safety */
    mutex_t lock;
    rwlock_t schema_lock;
};

/* ============================================================================
 * Prepared Statement Structure
 * ============================================================================ */

/* SQL operation types */
typedef enum {
    SQL_SELECT,
    SQL_INSERT,
    SQL_UPDATE,
    SQL_DELETE,
    SQL_CREATE_TABLE,
    SQL_DROP_TABLE,
    SQL_CREATE_INDEX,
    SQL_DROP_INDEX,
    SQL_BEGIN,
    SQL_COMMIT,
    SQL_ROLLBACK,
    SQL_SAVEPOINT,
    SQL_RELEASE,
    SQL_ROLLBACK_TO
} sql_op_t;

/* Expression node types */
typedef enum {
    EXPR_LITERAL,
    EXPR_COLUMN,
    EXPR_BINARY_OP,
    EXPR_UNARY_OP,
    EXPR_FUNCTION,
    EXPR_SUBQUERY,
    EXPR_PARAMETER
} expr_type_t;

/* Forward declare expression */
typedef struct expr expr_t;

struct expr {
    expr_type_t type;
    union {
        value_t literal;
        struct {
            char* table;
            char* column;
            int index;
        } column_ref;
        struct {
            int op;
            expr_t* left;
            expr_t* right;
        } binary;
        struct {
            int op;
            expr_t* operand;
        } unary;
        struct {
            char* name;
            expr_t** args;
            int arg_count;
        } function;
        int param_index;
    } data;
};

/* Select column */
typedef struct {
    expr_t* expr;
    char* alias;
} select_col_t;

/* Table reference */
typedef struct {
    char* name;
    char* alias;
    table_def_t* def;
} table_ref_t;

/* ORDER BY clause */
typedef struct {
    expr_t* expr;
    bool desc;
} order_by_t;

/* JOIN type */
typedef enum {
    JOIN_INNER = 0,
    JOIN_LEFT = 1,
    JOIN_RIGHT = 2,
    JOIN_CROSS = 3
} join_type_t;

/* JOIN clause */
typedef struct {
    join_type_t type;
    char* table_name;
    char* table_alias;
    expr_t* on_condition;
} join_clause_t;

/* Parsed statement */
typedef struct {
    sql_op_t op;

    /* SELECT */
    select_col_t* columns;
    int column_count;
    table_ref_t* tables;
    int table_count;
    join_clause_t* joins;
    int join_count;
    expr_t* where;
    expr_t** group_by;
    int group_by_count;
    expr_t* having;
    order_by_t* order_by;
    int order_by_count;
    int64_t limit;
    int64_t offset;

    /* INSERT */
    char** insert_columns;
    int insert_column_count;
    value_t** insert_values;
    int insert_row_count;

    /* UPDATE */
    char** update_columns;
    expr_t** update_exprs;
    int update_count;

    /* CREATE TABLE */
    table_def_t* new_table;

    /* CREATE INDEX */
    index_def_t* new_index;

    /* SAVEPOINT / RELEASE / ROLLBACK TO */
    char* savepoint_name;
} parsed_stmt_t;

/* Query execution plan node types */
typedef enum {
    PLAN_SCAN,
    PLAN_INDEX_SCAN,
    PLAN_FILTER,
    PLAN_PROJECT,
    PLAN_SORT,
    PLAN_LIMIT,
    PLAN_JOIN,
    PLAN_AGGREGATE,
    PLAN_INSERT,
    PLAN_UPDATE,
    PLAN_DELETE
} plan_node_type_t;

typedef struct plan_node plan_node_t;

struct plan_node {
    plan_node_type_t type;
    plan_node_t* child;
    plan_node_t* right;          /* For joins */

    /* Node-specific data */
    union {
        struct {
            table_def_t* table;
            btree_cursor_t cursor;
        } scan;
        struct {
            index_def_t* index;
            table_def_t* table;   /* Table to lookup rows from */
            btree_cursor_t cursor;
            value_t* start_key;
            value_t* end_key;
        } index_scan;
        struct {
            expr_t* predicate;
        } filter;
        struct {
            expr_t** expressions;
            int expr_count;
        } project;
        struct {
            order_by_t* order;
            int order_count;
            value_t** buffer;
            int buffer_size;
            int current;
        } sort;
        struct {
            int64_t limit;
            int64_t offset;
            int64_t count;
        } limit_offset;
    } data;
};

struct speedsql_stmt {
    speedsql* db;                /* Parent connection */
    char* sql;                   /* Original SQL */
    parsed_stmt_t* parsed;       /* Parsed statement */
    plan_node_t* plan;           /* Execution plan */

    /* Bound parameters */
    value_t* params;
    int param_count;

    /* Result set */
    value_t* current_row;
    int column_count;
    char** column_names;

    /* State */
    bool executed;
    bool has_row;
    int step_count;
};

/* ============================================================================
 * SQL Parser & Lexer
 * ============================================================================ */

typedef enum {
    TOK_EOF = 0,
    TOK_SEMICOLON,
    TOK_COMMA,
    TOK_LPAREN,
    TOK_RPAREN,
    TOK_DOT,
    TOK_STAR,
    TOK_PLUS,
    TOK_MINUS,
    TOK_SLASH,
    TOK_PERCENT,
    TOK_EQ,
    TOK_NE,
    TOK_LT,
    TOK_LE,
    TOK_GT,
    TOK_GE,
    TOK_AND,
    TOK_OR,
    TOK_NOT,
    TOK_SELECT,
    TOK_FROM,
    TOK_WHERE,
    TOK_ORDER,
    TOK_BY,
    TOK_ASC,
    TOK_DESC,
    TOK_LIMIT,
    TOK_OFFSET,
    TOK_INSERT,
    TOK_INTO,
    TOK_VALUES,
    TOK_UPDATE,
    TOK_SET,
    TOK_DELETE,
    TOK_CREATE,
    TOK_DROP,
    TOK_TABLE,
    TOK_INDEX,
    TOK_ON,
    TOK_PRIMARY,
    TOK_KEY,
    TOK_UNIQUE,
    TOK_NULL,
    TOK_DEFAULT,
    TOK_BEGIN,
    TOK_COMMIT,
    TOK_ROLLBACK,
    TOK_SAVEPOINT,
    TOK_RELEASE,
    TOK_TO,
    TOK_TRANSACTION,
    TOK_GROUP,
    TOK_HAVING,
    TOK_JOIN,
    TOK_LEFT,
    TOK_RIGHT,
    TOK_INNER,
    TOK_OUTER,
    TOK_AS,
    TOK_IN,
    TOK_BETWEEN,
    TOK_LIKE,
    TOK_IS,
    TOK_INTEGER,
    TOK_FLOAT,
    TOK_STRING,
    TOK_IDENT,
    TOK_PARAM,
    TOK_ERROR
} token_type_t;

typedef struct {
    token_type_t type;
    const char* start;
    int length;
    int line;
    union {
        int64_t int_val;
        double float_val;
    } value;
} token_t;

typedef struct {
    const char* start;
    const char* current;
    int line;
} lexer_t;

void lexer_init(lexer_t* lexer, const char* source);
token_t lexer_next(lexer_t* lexer);
token_t lexer_peek(lexer_t* lexer);

typedef struct {
    lexer_t lexer;
    token_t current;
    token_t previous;
    speedsql* db;
    char error[256];
    bool had_error;
} parser_t;

void parser_init(parser_t* parser, speedsql* db, const char* sql);
parsed_stmt_t* parser_parse(parser_t* parser);
void parsed_stmt_free(parsed_stmt_t* stmt);

/* ============================================================================
 * Query Optimizer & Executor
 * ============================================================================ */

plan_node_t* optimizer_plan(speedsql* db, parsed_stmt_t* stmt);
void plan_free(plan_node_t* plan);

int executor_init(speedsql_stmt* stmt);
int executor_step(speedsql_stmt* stmt);
void executor_reset(speedsql_stmt* stmt);

/* ============================================================================
 * Value Operations
 * ============================================================================ */

void value_init_null(value_t* v);
void value_init_int(value_t* v, int64_t i);
void value_init_float(value_t* v, double f);
void value_init_text(value_t* v, const char* s, int len);
void value_init_blob(value_t* v, const void* data, int len);
void value_copy(value_t* dst, const value_t* src);
void value_free(value_t* v);
int value_compare(const value_t* a, const value_t* b);
uint64_t value_hash(const value_t* v);

/* ============================================================================
 * Utility Functions
 * ============================================================================ */

uint32_t crc32(const void* data, size_t len);
uint64_t xxhash64(const void* data, size_t len);
uint64_t get_timestamp_us(void);

/* Set error on connection */
void sdb_set_error(speedsql* db, int code, const char* fmt, ...);

#endif /* SPEEDSQL_INTERNAL_H */
