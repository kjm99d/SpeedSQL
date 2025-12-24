/*
 * SpeedDB - Internal type definitions
 */

#ifndef SPEEDDB_TYPES_H
#define SPEEDDB_TYPES_H

#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>

/* Page size - 16KB for better large data performance (vs SQLite's 4KB) */
#define SPEEDDB_PAGE_SIZE 16384
#define SPEEDDB_MAX_PAGE_SIZE 65536
#define SPEEDDB_MIN_PAGE_SIZE 4096

/* Cache configuration */
#define SPEEDDB_DEFAULT_CACHE_SIZE (256 * 1024 * 1024)  /* 256MB default cache */
#define SPEEDDB_MAX_CACHE_SIZE (8ULL * 1024 * 1024 * 1024)  /* 8GB max */

/* Limits */
#define SPEEDDB_MAX_SQL_LENGTH (1024 * 1024)  /* 1MB SQL */
#define SPEEDDB_MAX_COLUMNS 32767
#define SPEEDDB_MAX_VARIABLE_NUMBER 999
#define SPEEDDB_MAX_COMPOUND_SELECT 500
#define SPEEDDB_MAX_EXPR_DEPTH 1000

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
    char magic[16];              /* "SpeedDB format 1" */
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

/* Value storage structure */
typedef struct {
    uint8_t type;                /* speeddb_type */
    uint32_t size;               /* Size in bytes */
    union {
        int64_t i64;             /* Integer value */
        double f64;              /* Float value */
        struct {
            char* data;
            uint32_t len;
        } str;                   /* Text/Blob/JSON */
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

/* Table definition */
typedef struct {
    char* name;                  /* Table name */
    uint32_t column_count;       /* Number of columns */
    column_def_t* columns;       /* Column definitions */
    page_id_t root_page;         /* Root page of B+tree */
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
    uint8_t flags;               /* UNIQUE, etc. */
} index_def_t;

/* Index flags */
#define IDX_FLAG_UNIQUE      0x01
#define IDX_FLAG_PRIMARY     0x02

/* Lock modes */
typedef enum {
    LOCK_NONE = 0,
    LOCK_SHARED = 1,
    LOCK_RESERVED = 2,
    LOCK_PENDING = 3,
    LOCK_EXCLUSIVE = 4
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

#endif /* SPEEDDB_TYPES_H */
