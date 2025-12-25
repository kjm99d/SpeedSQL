/*
 * SpeedSQL - Internal type definitions
 */

#ifndef SPEEDSQL_TYPES_H
#define SPEEDSQL_TYPES_H

#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>

/* Page size - 16KB for better large data performance (vs SQLite's 4KB) */
#define SPEEDSQL_PAGE_SIZE 16384
#define SPEEDSQL_MAX_PAGE_SIZE 65536
#define SPEEDSQL_MIN_PAGE_SIZE 4096

/* Cache configuration */
#define SPEEDSQL_DEFAULT_CACHE_SIZE (256 * 1024 * 1024)  /* 256MB default cache */
#define SPEEDSQL_MAX_CACHE_SIZE (8ULL * 1024 * 1024 * 1024)  /* 8GB max */

/* Limits */
#define SPEEDSQL_MAX_SQL_LENGTH (1024 * 1024)  /* 1MB SQL */
#define SPEEDSQL_MAX_COLUMNS 32767
#define SPEEDSQL_MAX_VARIABLE_NUMBER 999
#define SPEEDSQL_MAX_COMPOUND_SELECT 500
#define SPEEDSQL_MAX_EXPR_DEPTH 1000

/* Page types */
typedef enum {
    PAGE_TYPE_FREE = 0,
    PAGE_TYPE_BTREE_INTERNAL = 1,
    PAGE_TYPE_BTREE_LEAF = 2,
    PAGE_TYPE_OVERFLOW = 3,
    PAGE_TYPE_FREELIST = 4,
    PAGE_TYPE_SCHEMA = 5,
    PAGE_TYPE_WAL = 6
} page_type_t;

/* Page ID type - 64-bit for large file support */
typedef uint64_t page_id_t;
#define INVALID_PAGE_ID ((page_id_t)-1)

/* Row ID type */
typedef int64_t rowid_t;
#define INVALID_ROWID ((rowid_t)-1)

/* Transaction ID */
typedef uint64_t txn_id_t;

/* File header structure (first page of database file) */
typedef struct {
    char magic[16];              /* "SpeedSQL format1" */
    uint32_t version;            /* File format version */
    uint32_t page_size;          /* Page size in bytes */
    uint64_t page_count;         /* Total pages in file */
    uint64_t freelist_head;      /* First page of freelist */
    uint64_t freelist_count;     /* Number of free pages */
    uint64_t schema_root;        /* Root page of schema table */
    uint64_t txn_id;             /* Current transaction ID */
    uint32_t checksum;           /* Header checksum */
    uint8_t reserved[4012];      /* Reserved for future use */
} db_header_t;

/* Page header - common to all page types */
typedef struct {
    uint8_t page_type;           /* Page type */
    uint8_t flags;               /* Page flags */
    uint16_t cell_count;         /* Number of cells in page */
    uint32_t free_start;         /* Start of free space */
    uint32_t free_end;           /* End of free space */
    page_id_t right_ptr;         /* Right sibling (internal) or overflow */
    uint64_t txn_id;             /* Last modifying transaction */
    uint32_t checksum;           /* Page checksum */
} page_header_t;

/* Value type enum (for internal use) */
typedef enum {
    VAL_NULL = 0,
    VAL_INT = 1,
    VAL_FLOAT = 2,
    VAL_TEXT = 3,
    VAL_BLOB = 4,
    VAL_JSON = 5,
    VAL_VECTOR = 6
} value_type_t;

/* Value storage structure */
typedef struct {
    uint8_t type;                /* value_type_t / speedsql_type */
    uint32_t size;               /* Size in bytes */
    union {
        int64_t i;               /* Integer value (VAL_INT) */
        double f;                /* Float value (VAL_FLOAT) */
        struct {
            char* data;
            uint32_t len;
        } text;                  /* Text/JSON */
        struct {
            uint8_t* data;
            uint32_t len;
        } blob;                  /* Blob */
        struct {
            float* data;
            uint32_t dimensions;
        } vec;                   /* Vector */
    } data;
} value_t;

/* Column definition */
typedef struct {
    char* name;                  /* Column name */
    uint8_t type;                /* Data type */
    uint8_t flags;               /* NOT NULL, UNIQUE, etc. */
    char* default_value;         /* Default value expression */
    char* collation;             /* Collation name */
} column_def_t;

/* Column flags */
#define COL_FLAG_NOT_NULL    0x01
#define COL_FLAG_UNIQUE      0x02
#define COL_FLAG_PRIMARY_KEY 0x04
#define COL_FLAG_AUTOINCREMENT 0x08
#define COL_FLAG_INDEXED     0x10

/* Forward declare btree_t for table_def */
struct btree;

/* Table definition */
typedef struct {
    char* name;                  /* Table name */
    uint32_t column_count;       /* Number of columns */
    column_def_t* columns;       /* Column definitions */
    page_id_t root_page;         /* Root page of B+tree */
    struct btree* data_tree;     /* In-memory B+tree handle */
    uint64_t row_count;          /* Estimated row count */
    uint8_t flags;               /* Table flags */
} table_def_t;

/* Index definition */
typedef struct {
    char* name;                  /* Index name */
    char* table_name;            /* Table this index belongs to */
    uint32_t column_count;       /* Number of columns in index */
    uint32_t* column_indices;    /* Column indices */
    page_id_t root_page;         /* Root page of B+tree */
    struct btree* index_tree;    /* In-memory B+tree handle */
    uint8_t flags;               /* UNIQUE, etc. */
} index_def_t;

/* Index flags */
#define IDX_FLAG_UNIQUE      0x01
#define IDX_FLAG_PRIMARY     0x02

/* Lock modes */
typedef enum {
    SDB_LOCK_NONE = 0,
    SDB_LOCK_SHARED = 1,
    SDB_LOCK_RESERVED = 2,
    SDB_LOCK_PENDING = 3,
    SDB_LOCK_EXCLUSIVE = 4
} lock_mode_t;

/* Transaction states */
typedef enum {
    TXN_NONE = 0,
    TXN_READ = 1,
    TXN_WRITE = 2
} txn_state_t;

/* Buffer pool page state */
typedef enum {
    BUF_INVALID = 0,
    BUF_CLEAN = 1,
    BUF_DIRTY = 2,
    BUF_PINNED = 3
} buffer_state_t;

/* Comparison result */
typedef int (*compare_func_t)(const value_t*, const value_t*);

/* Hash function type */
typedef uint64_t (*hash_func_t)(const void* data, size_t len);

#endif /* SPEEDSQL_TYPES_H */
