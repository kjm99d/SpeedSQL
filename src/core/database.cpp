/*
 * SpeedDB - Database connection implementation
 */

#include "speeddb_internal.h"
#include <stdarg.h>

static const char DB_MAGIC[] = "SpeedDB format 1";
static const uint32_t DB_VERSION = 1;

/* Memory allocation wrappers */
void* sdb_malloc(size_t size) {
    return malloc(size);
}

void* sdb_realloc(void* ptr, size_t size) {
    return realloc(ptr, size);
}

void* sdb_calloc(size_t count, size_t size) {
    return calloc(count, size);
}

void sdb_free(void* ptr) {
    free(ptr);
}

char* sdb_strdup(const char* str) {
    if (!str) return nullptr;
    size_t len = strlen(str) + 1;
    char* dup = (char*)sdb_malloc(len);
    if (dup) memcpy(dup, str, len);
    return dup;
}

/* Error handling */
void sdb_set_error(speeddb* db, int code, const char* fmt, ...) {
    if (!db) return;
    db->errcode = code;

    va_list args;
    va_start(args, fmt);
    vsnprintf(db->errmsg, sizeof(db->errmsg), fmt, args);
    va_end(args);
}

/* Initialize a new database file */
static int init_new_database(speeddb* db) {
    /* Initialize header */
    memset(&db->header, 0, sizeof(db->header));
    memcpy(db->header.magic, DB_MAGIC, sizeof(DB_MAGIC));
    db->header.version = DB_VERSION;
    db->header.page_size = SPEEDDB_PAGE_SIZE;
    db->header.page_count = 1;  /* Just the header page */
    db->header.freelist_head = INVALID_PAGE_ID;
    db->header.freelist_count = 0;
    db->header.schema_root = INVALID_PAGE_ID;
    db->header.txn_id = 1;

    /* Calculate checksum */
    db->header.checksum = crc32(&db->header, offsetof(db_header_t, checksum));

    /* Write header to file */
    uint8_t page[SPEEDDB_PAGE_SIZE] = {0};
    memcpy(page, &db->header, sizeof(db->header));

    int rc = file_write(&db->db_file, 0, page, SPEEDDB_PAGE_SIZE);
    if (rc != SPEEDDB_OK) {
        sdb_set_error(db, SPEEDDB_IOERR, "Failed to write database header");
        return SPEEDDB_IOERR;
    }

    rc = file_sync(&db->db_file);
    if (rc != SPEEDDB_OK) {
        sdb_set_error(db, SPEEDDB_IOERR, "Failed to sync database file");
        return SPEEDDB_IOERR;
    }

    return SPEEDDB_OK;
}

/* Read and validate database header */
static int read_database_header(speeddb* db) {
    uint8_t page[SPEEDDB_PAGE_SIZE];

    int rc = file_read(&db->db_file, 0, page, SPEEDDB_PAGE_SIZE);
    if (rc != SPEEDDB_OK) {
        sdb_set_error(db, SPEEDDB_IOERR, "Failed to read database header");
        return SPEEDDB_IOERR;
    }

    memcpy(&db->header, page, sizeof(db->header));

    /* Validate magic */
    if (memcmp(db->header.magic, DB_MAGIC, sizeof(DB_MAGIC)) != 0) {
        sdb_set_error(db, SPEEDDB_CORRUPT, "Invalid database file format");
        return SPEEDDB_CORRUPT;
    }

    /* Validate version */
    if (db->header.version > DB_VERSION) {
        sdb_set_error(db, SPEEDDB_CORRUPT, "Database version %u not supported", db->header.version);
        return SPEEDDB_CORRUPT;
    }

    /* Validate checksum */
    uint32_t expected = crc32(&db->header, offsetof(db_header_t, checksum));
    if (db->header.checksum != expected) {
        sdb_set_error(db, SPEEDDB_CORRUPT, "Database header checksum mismatch");
        return SPEEDDB_CORRUPT;
    }

    return SPEEDDB_OK;
}

/* Public API: Open database */
SPEEDDB_API int speeddb_open(const char* filename, speeddb** db_out) {
    return speeddb_open_v2(filename, db_out,
        SPEEDDB_OPEN_READWRITE | SPEEDDB_OPEN_CREATE, nullptr);
}

SPEEDDB_API int speeddb_open_v2(const char* filename, speeddb** db_out,
                                 int flags, const char* vfs) {
    (void)vfs;  /* Reserved for future VFS support */

    if (!filename || !db_out) {
        return SPEEDDB_MISUSE;
    }

    *db_out = nullptr;

    /* Allocate connection structure */
    speeddb* db = (speeddb*)sdb_calloc(1, sizeof(speeddb));
    if (!db) {
        return SPEEDDB_NOMEM;
    }

    /* Initialize synchronization */
    mutex_init(&db->lock);
    rwlock_init(&db->schema_lock);

    db->flags = flags;
    db->cache_size = SPEEDDB_DEFAULT_CACHE_SIZE;
    db->errcode = SPEEDDB_OK;
    db->errmsg[0] = '\0';

    /* Open database file */
    int file_flags = 0;
    if (flags & SPEEDDB_OPEN_READONLY) {
        file_flags = 0;  /* Read only */
    } else if (flags & SPEEDDB_OPEN_READWRITE) {
        file_flags = 1;  /* Read-write */
    }
    if (flags & SPEEDDB_OPEN_CREATE) {
        file_flags |= 2;  /* Create if not exists */
    }

    int rc = file_open(&db->db_file, filename, file_flags);
    if (rc != SPEEDDB_OK) {
        sdb_set_error(db, SPEEDDB_CANTOPEN, "Cannot open database file: %s", filename);
        mutex_destroy(&db->lock);
        rwlock_destroy(&db->schema_lock);
        sdb_free(db);
        return SPEEDDB_CANTOPEN;
    }

    /* Check if this is a new database */
    uint64_t file_size;
    file_size(&db->db_file, &file_size);

    if (file_size == 0) {
        /* New database - initialize */
        rc = init_new_database(db);
        if (rc != SPEEDDB_OK) {
            file_close(&db->db_file);
            mutex_destroy(&db->lock);
            rwlock_destroy(&db->schema_lock);
            sdb_free(db);
            return rc;
        }
    } else {
        /* Existing database - read header */
        rc = read_database_header(db);
        if (rc != SPEEDDB_OK) {
            file_close(&db->db_file);
            mutex_destroy(&db->lock);
            rwlock_destroy(&db->schema_lock);
            sdb_free(db);
            return rc;
        }
    }

    /* Initialize buffer pool */
    db->buffer_pool = (buffer_pool_t*)sdb_malloc(sizeof(buffer_pool_t));
    if (!db->buffer_pool) {
        file_close(&db->db_file);
        mutex_destroy(&db->lock);
        rwlock_destroy(&db->schema_lock);
        sdb_free(db);
        return SPEEDDB_NOMEM;
    }

    rc = buffer_pool_init(db->buffer_pool, db->cache_size, db->header.page_size);
    if (rc != SPEEDDB_OK) {
        file_close(&db->db_file);
        mutex_destroy(&db->lock);
        rwlock_destroy(&db->schema_lock);
        sdb_free(db->buffer_pool);
        sdb_free(db);
        return rc;
    }

    /* Initialize WAL if enabled */
    if (flags & SPEEDDB_OPEN_WAL) {
        db->wal = (wal_t*)sdb_malloc(sizeof(wal_t));
        if (db->wal) {
            char wal_path[1024];
            snprintf(wal_path, sizeof(wal_path), "%s-wal", filename);
            rc = wal_init(db->wal, wal_path);
            if (rc != SPEEDDB_OK) {
                sdb_free(db->wal);
                db->wal = nullptr;
                /* Continue without WAL */
            }
        }
    }

    *db_out = db;
    return SPEEDDB_OK;
}

SPEEDDB_API int speeddb_close(speeddb* db) {
    if (!db) return SPEEDDB_MISUSE;

    /* Flush buffer pool */
    if (db->buffer_pool) {
        buffer_pool_flush(db->buffer_pool, &db->db_file);
        buffer_pool_destroy(db->buffer_pool);
        sdb_free(db->buffer_pool);
    }

    /* Close WAL */
    if (db->wal) {
        wal_close(db->wal);
        sdb_free(db->wal);
    }

    /* Free schema cache */
    if (db->tables) {
        for (size_t i = 0; i < db->table_count; i++) {
            sdb_free(db->tables[i].name);
            for (uint32_t j = 0; j < db->tables[i].column_count; j++) {
                sdb_free(db->tables[i].columns[j].name);
                sdb_free(db->tables[i].columns[j].default_value);
                sdb_free(db->tables[i].columns[j].collation);
            }
            sdb_free(db->tables[i].columns);
        }
        sdb_free(db->tables);
    }

    if (db->indices) {
        for (size_t i = 0; i < db->index_count; i++) {
            sdb_free(db->indices[i].name);
            sdb_free(db->indices[i].table_name);
            sdb_free(db->indices[i].column_indices);
        }
        sdb_free(db->indices);
    }

    /* Close database file */
    file_close(&db->db_file);

    /* Destroy synchronization */
    mutex_destroy(&db->lock);
    rwlock_destroy(&db->schema_lock);

    sdb_free(db);
    return SPEEDDB_OK;
}

SPEEDDB_API const char* speeddb_errmsg(speeddb* db) {
    if (!db) return "Invalid database handle";
    return db->errmsg[0] ? db->errmsg : "No error";
}

SPEEDDB_API int speeddb_errcode(speeddb* db) {
    if (!db) return SPEEDDB_MISUSE;
    return db->errcode;
}

/* Transaction API */
SPEEDDB_API int speeddb_begin(speeddb* db) {
    if (!db) return SPEEDDB_MISUSE;

    mutex_lock(&db->lock);

    if (db->txn_state != TXN_NONE) {
        mutex_unlock(&db->lock);
        sdb_set_error(db, SPEEDDB_MISUSE, "Transaction already in progress");
        return SPEEDDB_MISUSE;
    }

    db->current_txn = ++db->header.txn_id;
    db->txn_state = TXN_READ;  /* Upgrade to write on first write */

    mutex_unlock(&db->lock);
    return SPEEDDB_OK;
}

SPEEDDB_API int speeddb_commit(speeddb* db) {
    if (!db) return SPEEDDB_MISUSE;

    mutex_lock(&db->lock);

    if (db->txn_state == TXN_NONE) {
        mutex_unlock(&db->lock);
        return SPEEDDB_OK;  /* No transaction, nothing to do */
    }

    int rc = SPEEDDB_OK;

    /* Commit WAL if enabled */
    if (db->wal && db->txn_state == TXN_WRITE) {
        rc = wal_commit(db->wal, db->current_txn);
        if (rc != SPEEDDB_OK) {
            mutex_unlock(&db->lock);
            return rc;
        }
    }

    /* Flush dirty pages */
    if (db->txn_state == TXN_WRITE) {
        rc = buffer_pool_flush(db->buffer_pool, &db->db_file);
    }

    db->txn_state = TXN_NONE;
    db->current_txn = 0;

    mutex_unlock(&db->lock);
    return rc;
}

SPEEDDB_API int speeddb_rollback(speeddb* db) {
    if (!db) return SPEEDDB_MISUSE;

    mutex_lock(&db->lock);

    if (db->txn_state == TXN_NONE) {
        mutex_unlock(&db->lock);
        return SPEEDDB_OK;
    }

    /* Rollback WAL if enabled */
    if (db->wal && db->txn_state == TXN_WRITE) {
        wal_rollback(db->wal, db->current_txn);
    }

    /* TODO: Invalidate dirty pages in buffer pool */

    db->txn_state = TXN_NONE;
    db->current_txn = 0;

    mutex_unlock(&db->lock);
    return SPEEDDB_OK;
}

/* Utility functions */
SPEEDDB_API int speeddb_changes(speeddb* db) {
    if (!db) return 0;
    return (int)(db->total_changes & 0x7FFFFFFF);
}

SPEEDDB_API int64_t speeddb_total_changes(speeddb* db) {
    if (!db) return 0;
    return db->total_changes;
}

SPEEDDB_API int64_t speeddb_last_insert_rowid(speeddb* db) {
    if (!db) return 0;
    return db->last_rowid;
}

SPEEDDB_API void speeddb_free(void* ptr) {
    sdb_free(ptr);
}
