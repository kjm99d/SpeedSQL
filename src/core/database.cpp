/*
 * SpeedSQL - Database connection implementation
 */

#include "speedsql_internal.h"
#include <stdarg.h>

static const char DB_MAGIC[] = "SpeedSQL format 1";
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
void sdb_set_error(speedsql* db, int code, const char* fmt, ...) {
    if (!db) return;
    db->errcode = code;

    va_list args;
    va_start(args, fmt);
    vsnprintf(db->errmsg, sizeof(db->errmsg), fmt, args);
    va_end(args);
}

/* Initialize a new database file */
static int init_new_database(speedsql* db) {
    /* Initialize header */
    memset(&db->header, 0, sizeof(db->header));
    memcpy(db->header.magic, DB_MAGIC, sizeof(DB_MAGIC));
    db->header.version = DB_VERSION;
    db->header.page_size = SPEEDSQL_PAGE_SIZE;
    db->header.page_count = 1;  /* Just the header page */
    db->header.freelist_head = INVALID_PAGE_ID;
    db->header.freelist_count = 0;
    db->header.schema_root = INVALID_PAGE_ID;
    db->header.txn_id = 1;

    /* Calculate checksum */
    db->header.checksum = crc32(&db->header, offsetof(db_header_t, checksum));

    /* Write header to file */
    uint8_t page[SPEEDSQL_PAGE_SIZE] = {0};
    memcpy(page, &db->header, sizeof(db->header));

    int rc = file_write(&db->db_file, 0, page, SPEEDSQL_PAGE_SIZE);
    if (rc != SPEEDSQL_OK) {
        sdb_set_error(db, SPEEDSQL_IOERR, "Failed to write database header");
        return SPEEDSQL_IOERR;
    }

    rc = file_sync(&db->db_file);
    if (rc != SPEEDSQL_OK) {
        sdb_set_error(db, SPEEDSQL_IOERR, "Failed to sync database file");
        return SPEEDSQL_IOERR;
    }

    return SPEEDSQL_OK;
}

/* ============================================================================
 * Schema Persistence
 * ============================================================================ */

/* Schema page layout:
 * - page_type (1 byte) = PAGE_TYPE_SCHEMA
 * - table_count (2 bytes)
 * - index_count (2 bytes)
 * - For each table:
 *   - name_len (2 bytes) + name
 *   - column_count (2 bytes)
 *   - root_page (8 bytes)
 *   - For each column:
 *     - name_len (2 bytes) + name
 *     - type (1 byte)
 *     - flags (1 byte)
 * - For each index:
 *   - name_len (2 bytes) + name
 *   - table_name_len (2 bytes) + table_name
 *   - column_count (4 bytes)
 *   - root_page (8 bytes)
 *   - flags (1 byte)
 *   - column_indices (4 bytes each)
 */

static int save_schema(speedsql* db) {
    if (!db || (db->flags & SPEEDSQL_OPEN_MEMORY)) {
        return SPEEDSQL_OK;  /* No-op for memory databases */
    }

    /* Allocate schema page buffer */
    uint8_t* page = (uint8_t*)sdb_calloc(1, SPEEDSQL_PAGE_SIZE);
    if (!page) return SPEEDSQL_NOMEM;

    uint8_t* ptr = page;
    uint8_t* end = page + SPEEDSQL_PAGE_SIZE;

    /* Page type */
    *ptr++ = PAGE_TYPE_SCHEMA;

    /* Table count */
    *(uint16_t*)ptr = (uint16_t)db->table_count;
    ptr += 2;

    /* Index count */
    *(uint16_t*)ptr = (uint16_t)db->index_count;
    ptr += 2;

    /* Write tables */
    for (size_t t = 0; t < db->table_count && ptr < end - 256; t++) {
        table_def_t* tbl = &db->tables[t];

        /* Table name */
        uint16_t name_len = (uint16_t)strlen(tbl->name);
        *(uint16_t*)ptr = name_len;
        ptr += 2;
        memcpy(ptr, tbl->name, name_len);
        ptr += name_len;

        /* Column count */
        *(uint16_t*)ptr = (uint16_t)tbl->column_count;
        ptr += 2;

        /* Root page */
        *(page_id_t*)ptr = tbl->root_page;
        ptr += sizeof(page_id_t);

        /* Table flags */
        *ptr++ = tbl->flags;

        /* Write columns */
        for (uint32_t c = 0; c < tbl->column_count && ptr < end - 64; c++) {
            column_def_t* col = &tbl->columns[c];

            /* Column name */
            uint16_t col_name_len = (uint16_t)strlen(col->name);
            *(uint16_t*)ptr = col_name_len;
            ptr += 2;
            memcpy(ptr, col->name, col_name_len);
            ptr += col_name_len;

            /* Type and flags */
            *ptr++ = col->type;
            *ptr++ = col->flags;
        }
    }

    /* Write indices */
    for (size_t i = 0; i < db->index_count && ptr < end - 256; i++) {
        index_def_t* idx = &db->indices[i];

        /* Index name */
        uint16_t name_len = (uint16_t)strlen(idx->name);
        *(uint16_t*)ptr = name_len;
        ptr += 2;
        memcpy(ptr, idx->name, name_len);
        ptr += name_len;

        /* Table name */
        uint16_t tbl_name_len = (uint16_t)strlen(idx->table_name);
        *(uint16_t*)ptr = tbl_name_len;
        ptr += 2;
        memcpy(ptr, idx->table_name, tbl_name_len);
        ptr += tbl_name_len;

        /* Column count */
        *(uint32_t*)ptr = idx->column_count;
        ptr += 4;

        /* Root page */
        *(page_id_t*)ptr = idx->root_page;
        ptr += sizeof(page_id_t);

        /* Flags */
        *ptr++ = idx->flags;

        /* Column indices */
        for (uint32_t c = 0; c < idx->column_count && ptr < end - 8; c++) {
            *(uint32_t*)ptr = idx->column_indices[c];
            ptr += 4;
        }
    }

    /* Write to schema page (page 1, after header page) */
    int rc = file_write(&db->db_file, SPEEDSQL_PAGE_SIZE, page, SPEEDSQL_PAGE_SIZE);
    sdb_free(page);

    if (rc != SPEEDSQL_OK) {
        return rc;
    }

    /* Update header to indicate schema exists */
    db->header.schema_root = 1;  /* Schema is on page 1 */
    db->header.page_count = db->header.page_count < 2 ? 2 : db->header.page_count;
    db->header.checksum = crc32(&db->header, offsetof(db_header_t, checksum));

    /* Write updated header */
    uint8_t header_page[SPEEDSQL_PAGE_SIZE] = {0};
    memcpy(header_page, &db->header, sizeof(db->header));
    rc = file_write(&db->db_file, 0, header_page, SPEEDSQL_PAGE_SIZE);

    return rc;
}

static int load_schema(speedsql* db) {
    if (!db || (db->flags & SPEEDSQL_OPEN_MEMORY)) {
        return SPEEDSQL_OK;
    }

    if (db->header.schema_root == INVALID_PAGE_ID) {
        return SPEEDSQL_OK;  /* No schema stored */
    }

    /* Read schema page */
    uint8_t* page = (uint8_t*)sdb_malloc(SPEEDSQL_PAGE_SIZE);
    if (!page) return SPEEDSQL_NOMEM;

    int rc = file_read(&db->db_file, SPEEDSQL_PAGE_SIZE, page, SPEEDSQL_PAGE_SIZE);
    if (rc != SPEEDSQL_OK) {
        sdb_free(page);
        return rc;
    }

    uint8_t* ptr = page;
    uint8_t* end = page + SPEEDSQL_PAGE_SIZE;

    /* Check page type */
    if (*ptr++ != PAGE_TYPE_SCHEMA) {
        sdb_free(page);
        return SPEEDSQL_OK;  /* Not a schema page, skip */
    }

    /* Table count */
    uint16_t table_count = *(uint16_t*)ptr;
    ptr += 2;

    /* Index count */
    uint16_t index_count = *(uint16_t*)ptr;
    ptr += 2;

    /* Read tables */
    if (table_count > 0) {
        db->tables = (table_def_t*)sdb_calloc(table_count, sizeof(table_def_t));
        if (!db->tables) {
            sdb_free(page);
            return SPEEDSQL_NOMEM;
        }
    }

    for (uint16_t t = 0; t < table_count && ptr < end - 16; t++) {
        table_def_t* tbl = &db->tables[db->table_count];

        /* Table name */
        uint16_t name_len = *(uint16_t*)ptr;
        ptr += 2;
        if (ptr + name_len > end) break;
        tbl->name = (char*)sdb_malloc(name_len + 1);
        if (tbl->name) {
            memcpy(tbl->name, ptr, name_len);
            tbl->name[name_len] = '\0';
        }
        ptr += name_len;

        /* Column count */
        uint16_t col_count = *(uint16_t*)ptr;
        ptr += 2;
        tbl->column_count = col_count;

        /* Root page */
        tbl->root_page = *(page_id_t*)ptr;
        ptr += sizeof(page_id_t);

        /* Table flags */
        tbl->flags = *ptr++;

        /* Allocate columns */
        if (col_count > 0) {
            tbl->columns = (column_def_t*)sdb_calloc(col_count, sizeof(column_def_t));
        }

        /* Read columns */
        for (uint32_t c = 0; c < col_count && ptr < end - 8 && tbl->columns; c++) {
            column_def_t* col = &tbl->columns[c];

            /* Column name */
            uint16_t col_name_len = *(uint16_t*)ptr;
            ptr += 2;
            if (ptr + col_name_len > end) break;
            col->name = (char*)sdb_malloc(col_name_len + 1);
            if (col->name) {
                memcpy(col->name, ptr, col_name_len);
                col->name[col_name_len] = '\0';
            }
            ptr += col_name_len;

            /* Type and flags */
            col->type = *ptr++;
            col->flags = *ptr++;
            col->default_value = nullptr;
            col->collation = nullptr;
        }

        /* Re-create B+Tree for table data if root page exists */
        if (tbl->root_page != INVALID_PAGE_ID) {
            tbl->data_tree = (struct btree*)sdb_malloc(sizeof(btree_t));
            if (tbl->data_tree) {
                btree_open((btree_t*)tbl->data_tree, db->buffer_pool,
                           &db->db_file, tbl->root_page, value_compare);
            }
        }

        db->table_count++;
    }

    /* Read indices */
    if (index_count > 0) {
        db->indices = (index_def_t*)sdb_calloc(index_count, sizeof(index_def_t));
        if (!db->indices) {
            sdb_free(page);
            return SPEEDSQL_NOMEM;
        }
    }

    for (uint16_t i = 0; i < index_count && ptr < end - 32; i++) {
        index_def_t* idx = &db->indices[db->index_count];

        /* Index name */
        uint16_t name_len = *(uint16_t*)ptr;
        ptr += 2;
        if (ptr + name_len > end) break;
        idx->name = (char*)sdb_malloc(name_len + 1);
        if (idx->name) {
            memcpy(idx->name, ptr, name_len);
            idx->name[name_len] = '\0';
        }
        ptr += name_len;

        /* Table name */
        uint16_t tbl_name_len = *(uint16_t*)ptr;
        ptr += 2;
        if (ptr + tbl_name_len > end) break;
        idx->table_name = (char*)sdb_malloc(tbl_name_len + 1);
        if (idx->table_name) {
            memcpy(idx->table_name, ptr, tbl_name_len);
            idx->table_name[tbl_name_len] = '\0';
        }
        ptr += tbl_name_len;

        /* Column count */
        idx->column_count = *(uint32_t*)ptr;
        ptr += 4;

        /* Root page */
        idx->root_page = *(page_id_t*)ptr;
        ptr += sizeof(page_id_t);

        /* Flags */
        idx->flags = *ptr++;

        /* Column indices */
        if (idx->column_count > 0) {
            idx->column_indices = (uint32_t*)sdb_malloc(idx->column_count * sizeof(uint32_t));
            if (idx->column_indices) {
                for (uint32_t c = 0; c < idx->column_count && ptr < end - 4; c++) {
                    idx->column_indices[c] = *(uint32_t*)ptr;
                    ptr += 4;
                }
            }
        }

        db->index_count++;
    }

    sdb_free(page);
    return SPEEDSQL_OK;
}

/* Read and validate database header */
static int read_database_header(speedsql* db) {
    uint8_t page[SPEEDSQL_PAGE_SIZE];

    int rc = file_read(&db->db_file, 0, page, SPEEDSQL_PAGE_SIZE);
    if (rc != SPEEDSQL_OK) {
        sdb_set_error(db, SPEEDSQL_IOERR, "Failed to read database header");
        return SPEEDSQL_IOERR;
    }

    memcpy(&db->header, page, sizeof(db->header));

    /* Validate magic */
    if (memcmp(db->header.magic, DB_MAGIC, sizeof(DB_MAGIC)) != 0) {
        sdb_set_error(db, SPEEDSQL_CORRUPT, "Invalid database file format");
        return SPEEDSQL_CORRUPT;
    }

    /* Validate version */
    if (db->header.version > DB_VERSION) {
        sdb_set_error(db, SPEEDSQL_CORRUPT, "Database version %u not supported", db->header.version);
        return SPEEDSQL_CORRUPT;
    }

    /* Validate checksum */
    uint32_t expected = crc32(&db->header, offsetof(db_header_t, checksum));
    if (db->header.checksum != expected) {
        sdb_set_error(db, SPEEDSQL_CORRUPT, "Database header checksum mismatch");
        return SPEEDSQL_CORRUPT;
    }

    return SPEEDSQL_OK;
}

/* Public API: Open database */
SPEEDSQL_API int speedsql_open(const char* filename, speedsql** db_out) {
    return speedsql_open_v2(filename, db_out,
        SPEEDSQL_OPEN_READWRITE | SPEEDSQL_OPEN_CREATE, nullptr);
}

SPEEDSQL_API int speedsql_open_v2(const char* filename, speedsql** db_out,
                                 int flags, const char* vfs) {
    (void)vfs;  /* Reserved for future VFS support */

    if (!filename || !db_out) {
        return SPEEDSQL_MISUSE;
    }

    *db_out = nullptr;

    /* Check for in-memory database */
    bool is_memory = (strcmp(filename, ":memory:") == 0 ||
                      strcmp(filename, "") == 0);

    /* Allocate connection structure */
    speedsql* db = (speedsql*)sdb_calloc(1, sizeof(speedsql));
    if (!db) {
        return SPEEDSQL_NOMEM;
    }

    /* Initialize synchronization */
    mutex_init(&db->lock);
    rwlock_init(&db->schema_lock);

    db->flags = flags | (is_memory ? SPEEDSQL_OPEN_MEMORY : 0);
    db->cache_size = SPEEDSQL_DEFAULT_CACHE_SIZE;
    db->errcode = SPEEDSQL_OK;
    db->errmsg[0] = '\0';

    int rc = SPEEDSQL_OK;

    if (is_memory) {
        /* In-memory database - no file operations */
        memset(&db->db_file, 0, sizeof(db->db_file));
        db->db_file.handle = INVALID_FILE_HANDLE;
        db->db_file.path = sdb_strdup(":memory:");
        rwlock_init(&db->db_file.lock);

        /* Initialize header directly */
        memset(&db->header, 0, sizeof(db->header));
        memcpy(db->header.magic, DB_MAGIC, sizeof(DB_MAGIC));
        db->header.version = DB_VERSION;
        db->header.page_size = SPEEDSQL_PAGE_SIZE;
        db->header.page_count = 1;
        db->header.freelist_head = INVALID_PAGE_ID;
        db->header.freelist_count = 0;
        db->header.schema_root = INVALID_PAGE_ID;
        db->header.txn_id = 1;
        db->header.checksum = crc32(&db->header, offsetof(db_header_t, checksum));
    } else {
        /* Open database file */
        int file_flags = 0;
        if (flags & SPEEDSQL_OPEN_READONLY) {
            file_flags = 0;  /* Read only */
        } else if (flags & SPEEDSQL_OPEN_READWRITE) {
            file_flags = 1;  /* Read-write */
        }
        if (flags & SPEEDSQL_OPEN_CREATE) {
            file_flags |= 2;  /* Create if not exists */
        }

        rc = file_open(&db->db_file, filename, file_flags);
        if (rc != SPEEDSQL_OK) {
            sdb_set_error(db, SPEEDSQL_CANTOPEN, "Cannot open database file: %s", filename);
            mutex_destroy(&db->lock);
            rwlock_destroy(&db->schema_lock);
            sdb_free(db);
            return SPEEDSQL_CANTOPEN;
        }

        /* Check if this is a new database */
        uint64_t db_file_size;
        file_size(&db->db_file, &db_file_size);

        if (db_file_size == 0) {
            /* New database - initialize */
            rc = init_new_database(db);
            if (rc != SPEEDSQL_OK) {
                file_close(&db->db_file);
                mutex_destroy(&db->lock);
                rwlock_destroy(&db->schema_lock);
                sdb_free(db);
                return rc;
            }
        } else {
            /* Existing database - read header */
            rc = read_database_header(db);
            if (rc != SPEEDSQL_OK) {
                file_close(&db->db_file);
                mutex_destroy(&db->lock);
                rwlock_destroy(&db->schema_lock);
                sdb_free(db);
                return rc;
            }
        }
    }

    /* Initialize buffer pool */
    db->buffer_pool = (buffer_pool_t*)sdb_malloc(sizeof(buffer_pool_t));
    if (!db->buffer_pool) {
        if (!is_memory) file_close(&db->db_file);
        mutex_destroy(&db->lock);
        rwlock_destroy(&db->schema_lock);
        sdb_free(db);
        return SPEEDSQL_NOMEM;
    }

    rc = buffer_pool_init(db->buffer_pool, db->cache_size, db->header.page_size);
    if (rc != SPEEDSQL_OK) {
        if (!is_memory) file_close(&db->db_file);
        mutex_destroy(&db->lock);
        rwlock_destroy(&db->schema_lock);
        sdb_free(db->buffer_pool);
        sdb_free(db);
        return rc;
    }

    /* Initialize WAL if enabled (not for memory databases) */
    if ((flags & SPEEDSQL_OPEN_WAL) && !is_memory) {
        db->wal = (wal_t*)sdb_malloc(sizeof(wal_t));
        if (db->wal) {
            char wal_path[1024];
            snprintf(wal_path, sizeof(wal_path), "%s-wal", filename);
            rc = wal_init(db->wal, wal_path);
            if (rc != SPEEDSQL_OK) {
                sdb_free(db->wal);
                db->wal = nullptr;
                /* Continue without WAL */
            }
        }
    }

    /* Load schema from file if exists */
    if (!is_memory) {
        rc = load_schema(db);
        if (rc != SPEEDSQL_OK) {
            /* Non-fatal - just continue without schema */
        }
    }

    *db_out = db;
    return SPEEDSQL_OK;
}

SPEEDSQL_API int speedsql_close(speedsql* db) {
    if (!db) return SPEEDSQL_MISUSE;

    /* Save schema before closing (if file database) */
    if (db->table_count > 0 || db->index_count > 0) {
        save_schema(db);
    }

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
            /* Free B+Tree for table data */
            if (db->tables[i].data_tree) {
                btree_close((btree_t*)db->tables[i].data_tree);
                sdb_free(db->tables[i].data_tree);
            }
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
    return SPEEDSQL_OK;
}

SPEEDSQL_API const char* speedsql_errmsg(speedsql* db) {
    if (!db) return "Invalid database handle";
    return db->errmsg[0] ? db->errmsg : "No error";
}

SPEEDSQL_API int speedsql_errcode(speedsql* db) {
    if (!db) return SPEEDSQL_MISUSE;
    return db->errcode;
}

/* Transaction API */
SPEEDSQL_API int speedsql_begin(speedsql* db) {
    if (!db) return SPEEDSQL_MISUSE;

    mutex_lock(&db->lock);

    if (db->txn_state != TXN_NONE) {
        mutex_unlock(&db->lock);
        sdb_set_error(db, SPEEDSQL_MISUSE, "Transaction already in progress");
        return SPEEDSQL_MISUSE;
    }

    db->current_txn = ++db->header.txn_id;
    db->txn_state = TXN_READ;  /* Upgrade to write on first write */

    mutex_unlock(&db->lock);
    return SPEEDSQL_OK;
}

SPEEDSQL_API int speedsql_commit(speedsql* db) {
    if (!db) return SPEEDSQL_MISUSE;

    mutex_lock(&db->lock);

    if (db->txn_state == TXN_NONE) {
        mutex_unlock(&db->lock);
        return SPEEDSQL_OK;  /* No transaction, nothing to do */
    }

    int rc = SPEEDSQL_OK;

    /* Commit WAL if enabled */
    if (db->wal && db->txn_state == TXN_WRITE) {
        rc = wal_commit(db->wal, db->current_txn);
        if (rc != SPEEDSQL_OK) {
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

SPEEDSQL_API int speedsql_rollback(speedsql* db) {
    if (!db) return SPEEDSQL_MISUSE;

    mutex_lock(&db->lock);

    if (db->txn_state == TXN_NONE) {
        mutex_unlock(&db->lock);
        return SPEEDSQL_OK;
    }

    /* Rollback WAL if enabled */
    if (db->wal && db->txn_state == TXN_WRITE) {
        wal_rollback(db->wal, db->current_txn);
    }

    /* Invalidate dirty pages in buffer pool */
    if (db->buffer_pool) {
        buffer_pool_invalidate_dirty(db->buffer_pool, &db->db_file);
    }

    db->txn_state = TXN_NONE;
    db->current_txn = 0;
    db->savepoint_count = 0;  /* Clear all savepoints */

    mutex_unlock(&db->lock);
    return SPEEDSQL_OK;
}

/* Savepoint support */
SPEEDSQL_API int speedsql_savepoint(speedsql* db, const char* name) {
    if (!db || !name) return SPEEDSQL_MISUSE;

    mutex_lock(&db->lock);

    /* Must be in a transaction */
    if (db->txn_state == TXN_NONE) {
        mutex_unlock(&db->lock);
        sdb_set_error(db, SPEEDSQL_MISUSE, "No transaction in progress");
        return SPEEDSQL_MISUSE;
    }

    /* Check savepoint limit */
    if (db->savepoint_count >= 32) {
        mutex_unlock(&db->lock);
        sdb_set_error(db, SPEEDSQL_FULL, "Maximum savepoint depth reached (32)");
        return SPEEDSQL_FULL;
    }

    /* Check for duplicate name */
    for (int i = 0; i < db->savepoint_count; i++) {
        if (strcmp(db->savepoints[i].name, name) == 0) {
            mutex_unlock(&db->lock);
            sdb_set_error(db, SPEEDSQL_CONSTRAINT, "Savepoint '%s' already exists", name);
            return SPEEDSQL_CONSTRAINT;
        }
    }

    /* Create savepoint entry */
    auto* sp = &db->savepoints[db->savepoint_count];
    strncpy(sp->name, name, sizeof(sp->name) - 1);
    sp->name[sizeof(sp->name) - 1] = '\0';
    sp->last_rowid_saved = db->last_rowid;
    sp->total_changes_saved = db->total_changes;

    /* Record in WAL */
    if (db->wal) {
        int rc = wal_savepoint(db->wal, db->current_txn, &sp->wal_lsn);
        if (rc != SPEEDSQL_OK) {
            mutex_unlock(&db->lock);
            return rc;
        }
    } else {
        sp->wal_lsn = 0;
    }

    db->savepoint_count++;

    mutex_unlock(&db->lock);
    return SPEEDSQL_OK;
}

SPEEDSQL_API int speedsql_release(speedsql* db, const char* name) {
    if (!db || !name) return SPEEDSQL_MISUSE;

    mutex_lock(&db->lock);

    /* Find the savepoint */
    int found_idx = -1;
    for (int i = db->savepoint_count - 1; i >= 0; i--) {
        if (strcmp(db->savepoints[i].name, name) == 0) {
            found_idx = i;
            break;
        }
    }

    if (found_idx < 0) {
        mutex_unlock(&db->lock);
        sdb_set_error(db, SPEEDSQL_NOTFOUND, "Savepoint '%s' not found", name);
        return SPEEDSQL_NOTFOUND;
    }

    /* Record in WAL */
    if (db->wal) {
        wal_release_savepoint(db->wal, db->current_txn);
    }

    /* Remove this savepoint and all savepoints after it */
    db->savepoint_count = found_idx;

    mutex_unlock(&db->lock);
    return SPEEDSQL_OK;
}

SPEEDSQL_API int speedsql_rollback_to(speedsql* db, const char* name) {
    if (!db || !name) return SPEEDSQL_MISUSE;

    mutex_lock(&db->lock);

    /* Find the savepoint */
    int found_idx = -1;
    for (int i = db->savepoint_count - 1; i >= 0; i--) {
        if (strcmp(db->savepoints[i].name, name) == 0) {
            found_idx = i;
            break;
        }
    }

    if (found_idx < 0) {
        mutex_unlock(&db->lock);
        sdb_set_error(db, SPEEDSQL_NOTFOUND, "Savepoint '%s' not found", name);
        return SPEEDSQL_NOTFOUND;
    }

    auto* sp = &db->savepoints[found_idx];

    /* Record in WAL */
    if (db->wal) {
        wal_rollback_to_savepoint(db->wal, db->current_txn, sp->wal_lsn);
    }

    /* Invalidate dirty pages (simplified - in production, would selectively undo) */
    if (db->buffer_pool) {
        buffer_pool_invalidate_dirty(db->buffer_pool, &db->db_file);
    }

    /* Restore state */
    db->last_rowid = sp->last_rowid_saved;
    db->total_changes = sp->total_changes_saved;

    /* Remove savepoints after the one we're rolling back to, but keep this one */
    db->savepoint_count = found_idx + 1;

    mutex_unlock(&db->lock);
    return SPEEDSQL_OK;
}

/* Utility functions */
SPEEDSQL_API int speedsql_changes(speedsql* db) {
    if (!db) return 0;
    return (int)(db->total_changes & 0x7FFFFFFF);
}

SPEEDSQL_API int64_t speedsql_total_changes(speedsql* db) {
    if (!db) return 0;
    return db->total_changes;
}

SPEEDSQL_API int64_t speedsql_last_insert_rowid(speedsql* db) {
    if (!db) return 0;
    return db->last_rowid;
}

SPEEDSQL_API void speedsql_free(void* ptr) {
    sdb_free(ptr);
}
