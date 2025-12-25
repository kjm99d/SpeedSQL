/*
 * SpeedSQL - Write-Ahead Logging (WAL)
 *
 * Implements crash-safe durability via write-ahead logging.
 * Log records are written before page modifications, enabling
 * recovery by replaying the log after a crash.
 *
 * WAL File Format:
 *   [Header: 64 bytes]
 *   [Log Record]*
 *
 * Log Record Format:
 *   [LSN: 8 bytes]
 *   [TXN ID: 8 bytes]
 *   [Type: 1 byte]
 *   [Page ID: 4 bytes] (for page records)
 *   [Data Length: 4 bytes]
 *   [Before Image: variable]
 *   [After Image: variable]
 *   [Checksum: 4 bytes]
 */

#include "speedsql_internal.h"

/* WAL magic number */
static const uint32_t WAL_MAGIC = 0x57414C31;  /* "WAL1" */
static const uint32_t WAL_VERSION = 1;
static const size_t WAL_HEADER_SIZE = 64;
static const size_t WAL_BUFFER_SIZE = 64 * 1024;  /* 64KB buffer */

/* Log record types */
typedef enum {
    WAL_RECORD_BEGIN = 1,
    WAL_RECORD_COMMIT = 2,
    WAL_RECORD_ROLLBACK = 3,
    WAL_RECORD_PAGE = 4,
    WAL_RECORD_CHECKPOINT = 5
} wal_record_type_t;

/* WAL file header */
typedef struct {
    uint32_t magic;
    uint32_t version;
    uint64_t lsn;               /* Next LSN to write */
    uint64_t checkpoint_lsn;    /* Last checkpoint LSN */
    uint32_t page_size;
    uint32_t checksum;
    uint8_t reserved[32];
} wal_header_t;

/* Log record header */
typedef struct {
    uint64_t lsn;
    uint64_t txn_id;
    uint8_t type;
    uint8_t reserved[3];
    page_id_t page_id;
    uint32_t data_len;
} wal_record_header_t;

/* Calculate header checksum */
static uint32_t wal_header_checksum(const wal_header_t* hdr) {
    return crc32(hdr, offsetof(wal_header_t, checksum));
}

/* Calculate record checksum */
static uint32_t wal_record_checksum(const wal_record_header_t* hdr,
                                     const void* before, const void* after, size_t size) {
    uint32_t crc = crc32(hdr, sizeof(*hdr));
    if (before && size > 0) {
        crc ^= crc32(before, size);
    }
    if (after && size > 0) {
        crc ^= crc32(after, size);
    }
    return crc;
}

/* Flush WAL buffer to disk */
static int wal_flush_buffer(wal_t* wal) {
    if (wal->buffer_pos == 0) {
        return SPEEDSQL_OK;
    }

    /* Calculate write position (after header, at current file position) */
    uint64_t wal_file_sz;
    file_size(&wal->file, &wal_file_sz);

    uint64_t write_pos = wal_file_sz;
    if (write_pos < WAL_HEADER_SIZE) {
        write_pos = WAL_HEADER_SIZE;
    }

    int rc = file_write(&wal->file, write_pos, wal->buffer, wal->buffer_pos);
    if (rc != SPEEDSQL_OK) {
        return rc;
    }

    rc = file_sync(&wal->file);
    if (rc != SPEEDSQL_OK) {
        return rc;
    }

    wal->buffer_pos = 0;
    return SPEEDSQL_OK;
}

/* Write WAL header */
static int wal_write_header(wal_t* wal) {
    wal_header_t hdr = {0};
    hdr.magic = WAL_MAGIC;
    hdr.version = WAL_VERSION;
    hdr.lsn = wal->current_lsn;
    hdr.checkpoint_lsn = wal->checkpoint_lsn;
    hdr.page_size = SPEEDSQL_PAGE_SIZE;
    hdr.checksum = wal_header_checksum(&hdr);

    uint8_t header_buf[WAL_HEADER_SIZE] = {0};
    memcpy(header_buf, &hdr, sizeof(hdr));

    int rc = file_write(&wal->file, 0, header_buf, WAL_HEADER_SIZE);
    if (rc != SPEEDSQL_OK) {
        return rc;
    }

    return file_sync(&wal->file);
}

/* Read WAL header */
static int wal_read_header(wal_t* wal) {
    uint8_t header_buf[WAL_HEADER_SIZE];

    int rc = file_read(&wal->file, 0, header_buf, WAL_HEADER_SIZE);
    if (rc != SPEEDSQL_OK) {
        return rc;
    }

    wal_header_t* hdr = (wal_header_t*)header_buf;

    /* Validate magic */
    if (hdr->magic != WAL_MAGIC) {
        return SPEEDSQL_CORRUPT;
    }

    /* Validate version */
    if (hdr->version > WAL_VERSION) {
        return SPEEDSQL_CORRUPT;
    }

    /* Validate checksum */
    if (hdr->checksum != wal_header_checksum(hdr)) {
        return SPEEDSQL_CORRUPT;
    }

    wal->current_lsn = hdr->lsn;
    wal->checkpoint_lsn = hdr->checkpoint_lsn;

    return SPEEDSQL_OK;
}

/* Append record to buffer (with auto-flush if needed) */
static int wal_append_record(wal_t* wal, const wal_record_header_t* hdr,
                             const void* before, const void* after, size_t data_size) {
    size_t record_size = sizeof(*hdr);
    if (hdr->type == WAL_RECORD_PAGE) {
        record_size += data_size * 2 + sizeof(uint32_t);  /* before + after + checksum */
    } else {
        record_size += sizeof(uint32_t);  /* just checksum */
    }

    /* Flush if record won't fit */
    if (wal->buffer_pos + record_size > wal->buffer_size) {
        int rc = wal_flush_buffer(wal);
        if (rc != SPEEDSQL_OK) {
            return rc;
        }
    }

    /* Write header */
    memcpy(wal->buffer + wal->buffer_pos, hdr, sizeof(*hdr));
    wal->buffer_pos += sizeof(*hdr);

    /* Write data for page records */
    if (hdr->type == WAL_RECORD_PAGE && data_size > 0) {
        if (before) {
            memcpy(wal->buffer + wal->buffer_pos, before, data_size);
        } else {
            memset(wal->buffer + wal->buffer_pos, 0, data_size);
        }
        wal->buffer_pos += data_size;

        if (after) {
            memcpy(wal->buffer + wal->buffer_pos, after, data_size);
        } else {
            memset(wal->buffer + wal->buffer_pos, 0, data_size);
        }
        wal->buffer_pos += data_size;
    }

    /* Write checksum */
    uint32_t checksum = wal_record_checksum(hdr, before, after, data_size);
    memcpy(wal->buffer + wal->buffer_pos, &checksum, sizeof(checksum));
    wal->buffer_pos += sizeof(checksum);

    return SPEEDSQL_OK;
}

/* ============================================================================
 * Public API
 * ============================================================================ */

int wal_init(wal_t* wal, const char* path) {
    if (!wal || !path) {
        return SPEEDSQL_MISUSE;
    }

    memset(wal, 0, sizeof(*wal));
    mutex_init(&wal->lock);

    /* Allocate buffer */
    wal->buffer_size = WAL_BUFFER_SIZE;
    wal->buffer = (uint8_t*)sdb_malloc(wal->buffer_size);
    if (!wal->buffer) {
        mutex_destroy(&wal->lock);
        return SPEEDSQL_NOMEM;
    }
    wal->buffer_pos = 0;

    /* Open or create WAL file */
    int rc = file_open(&wal->file, path, 1 | 2);  /* Read-write, create */
    if (rc != SPEEDSQL_OK) {
        sdb_free(wal->buffer);
        mutex_destroy(&wal->lock);
        return rc;
    }

    /* Check file size */
    uint64_t wal_file_size;
    file_size(&wal->file, &wal_file_size);

    if (wal_file_size == 0) {
        /* New WAL - initialize header */
        wal->current_lsn = 1;
        wal->checkpoint_lsn = 0;
        rc = wal_write_header(wal);
        if (rc != SPEEDSQL_OK) {
            file_close(&wal->file);
            sdb_free(wal->buffer);
            mutex_destroy(&wal->lock);
            return rc;
        }
    } else if (wal_file_size >= WAL_HEADER_SIZE) {
        /* Existing WAL - read header */
        rc = wal_read_header(wal);
        if (rc != SPEEDSQL_OK) {
            file_close(&wal->file);
            sdb_free(wal->buffer);
            mutex_destroy(&wal->lock);
            return rc;
        }
    } else {
        /* Corrupt WAL file - too small */
        file_close(&wal->file);
        sdb_free(wal->buffer);
        mutex_destroy(&wal->lock);
        return SPEEDSQL_CORRUPT;
    }

    return SPEEDSQL_OK;
}

void wal_close(wal_t* wal) {
    if (!wal) return;

    mutex_lock(&wal->lock);

    /* Flush remaining buffer */
    if (wal->buffer_pos > 0) {
        wal_flush_buffer(wal);
    }

    /* Update header with final LSN */
    wal_write_header(wal);

    /* Close file */
    file_close(&wal->file);

    /* Free buffer */
    if (wal->buffer) {
        sdb_free(wal->buffer);
        wal->buffer = nullptr;
    }

    mutex_unlock(&wal->lock);
    mutex_destroy(&wal->lock);
}

int wal_write(wal_t* wal, txn_id_t txn, page_id_t page,
              const void* before, const void* after, size_t size) {
    if (!wal) return SPEEDSQL_MISUSE;

    mutex_lock(&wal->lock);

    /* Create record header */
    wal_record_header_t hdr = {0};
    hdr.lsn = wal->current_lsn++;
    hdr.txn_id = txn;
    hdr.type = WAL_RECORD_PAGE;
    hdr.page_id = page;
    hdr.data_len = (uint32_t)size;

    int rc = wal_append_record(wal, &hdr, before, after, size);

    mutex_unlock(&wal->lock);
    return rc;
}

int wal_commit(wal_t* wal, txn_id_t txn) {
    if (!wal) return SPEEDSQL_MISUSE;

    mutex_lock(&wal->lock);

    /* Create commit record */
    wal_record_header_t hdr = {0};
    hdr.lsn = wal->current_lsn++;
    hdr.txn_id = txn;
    hdr.type = WAL_RECORD_COMMIT;
    hdr.page_id = INVALID_PAGE_ID;
    hdr.data_len = 0;

    int rc = wal_append_record(wal, &hdr, nullptr, nullptr, 0);
    if (rc != SPEEDSQL_OK) {
        mutex_unlock(&wal->lock);
        return rc;
    }

    /* Force flush on commit for durability */
    rc = wal_flush_buffer(wal);

    mutex_unlock(&wal->lock);
    return rc;
}

int wal_rollback(wal_t* wal, txn_id_t txn) {
    if (!wal) return SPEEDSQL_MISUSE;

    mutex_lock(&wal->lock);

    /* Create rollback record */
    wal_record_header_t hdr = {0};
    hdr.lsn = wal->current_lsn++;
    hdr.txn_id = txn;
    hdr.type = WAL_RECORD_ROLLBACK;
    hdr.page_id = INVALID_PAGE_ID;
    hdr.data_len = 0;

    int rc = wal_append_record(wal, &hdr, nullptr, nullptr, 0);
    if (rc != SPEEDSQL_OK) {
        mutex_unlock(&wal->lock);
        return rc;
    }

    rc = wal_flush_buffer(wal);

    mutex_unlock(&wal->lock);
    return rc;
}

/* Transaction state tracking for recovery */
typedef struct {
    txn_id_t txn_id;
    bool committed;
    bool rolled_back;
} txn_status_t;

/* Find or add transaction status */
static txn_status_t* find_or_add_txn(txn_status_t** txns, size_t* count, size_t* capacity, txn_id_t txn_id) {
    /* Search existing */
    for (size_t i = 0; i < *count; i++) {
        if ((*txns)[i].txn_id == txn_id) {
            return &(*txns)[i];
        }
    }

    /* Need to add new */
    if (*count >= *capacity) {
        size_t new_cap = *capacity == 0 ? 16 : *capacity * 2;
        txn_status_t* new_txns = (txn_status_t*)sdb_realloc(*txns, new_cap * sizeof(txn_status_t));
        if (!new_txns) {
            return nullptr;
        }
        *txns = new_txns;
        *capacity = new_cap;
    }

    txn_status_t* status = &(*txns)[*count];
    status->txn_id = txn_id;
    status->committed = false;
    status->rolled_back = false;
    (*count)++;
    return status;
}

int wal_recover(wal_t* wal, buffer_pool_t* pool, file_t* db_file) {
    if (!wal || !pool || !db_file) {
        return SPEEDSQL_MISUSE;
    }

    mutex_lock(&wal->lock);

    uint64_t wal_file_size;
    file_size(&wal->file, &wal_file_size);

    if (wal_file_size <= WAL_HEADER_SIZE) {
        /* Nothing to recover */
        mutex_unlock(&wal->lock);
        return SPEEDSQL_OK;
    }

    /* Track transaction states */
    txn_status_t* txns = nullptr;
    size_t txn_count = 0;
    size_t txn_capacity = 0;

    /* First pass: determine committed transactions */
    uint64_t pos = WAL_HEADER_SIZE;
    uint8_t* record_buf = (uint8_t*)sdb_malloc(SPEEDSQL_PAGE_SIZE * 2 + 64);
    if (!record_buf) {
        mutex_unlock(&wal->lock);
        return SPEEDSQL_NOMEM;
    }

    while (pos < wal_file_size) {
        wal_record_header_t hdr;
        int rc = file_read(&wal->file, pos, &hdr, sizeof(hdr));
        if (rc != SPEEDSQL_OK) {
            break;  /* End of valid records */
        }

        /* Check for valid record (LSN should be reasonable) */
        if (hdr.lsn == 0 || hdr.lsn > wal->current_lsn) {
            break;
        }

        txn_status_t* status = find_or_add_txn(&txns, &txn_count, &txn_capacity, hdr.txn_id);
        if (!status) {
            sdb_free(record_buf);
            sdb_free(txns);
            mutex_unlock(&wal->lock);
            return SPEEDSQL_NOMEM;
        }

        if (hdr.type == WAL_RECORD_COMMIT) {
            status->committed = true;
        } else if (hdr.type == WAL_RECORD_ROLLBACK) {
            status->rolled_back = true;
        }

        /* Calculate record size and skip to next */
        size_t record_size = sizeof(hdr) + sizeof(uint32_t);  /* Header + checksum */
        if (hdr.type == WAL_RECORD_PAGE) {
            record_size += hdr.data_len * 2;  /* Before + after images */
        }
        pos += record_size;
    }

    /* Second pass: apply committed transactions */
    pos = WAL_HEADER_SIZE;
    while (pos < wal_file_size) {
        wal_record_header_t hdr;
        int rc = file_read(&wal->file, pos, &hdr, sizeof(hdr));
        if (rc != SPEEDSQL_OK) {
            break;
        }

        if (hdr.lsn == 0 || hdr.lsn > wal->current_lsn) {
            break;
        }

        /* Check if this transaction was committed */
        bool should_apply = false;
        for (size_t i = 0; i < txn_count; i++) {
            if (txns[i].txn_id == hdr.txn_id && txns[i].committed) {
                should_apply = true;
                break;
            }
        }

        if (hdr.type == WAL_RECORD_PAGE && should_apply) {
            /* Read before and after images */
            size_t data_offset = pos + sizeof(hdr);

            /* Skip before image, read after image */
            uint8_t* after_image = record_buf;
            rc = file_read(&wal->file, data_offset + hdr.data_len, after_image, hdr.data_len);
            if (rc == SPEEDSQL_OK && hdr.page_id != INVALID_PAGE_ID) {
                /* Apply after image to database */
                file_write(db_file, hdr.page_id * pool->page_size, after_image, hdr.data_len);
            }
        }

        /* Skip to next record */
        size_t record_size = sizeof(hdr) + sizeof(uint32_t);
        if (hdr.type == WAL_RECORD_PAGE) {
            record_size += hdr.data_len * 2;
        }
        pos += record_size;
    }

    /* Sync database file */
    file_sync(db_file);

    sdb_free(record_buf);
    sdb_free(txns);

    /* Update checkpoint LSN */
    wal->checkpoint_lsn = wal->current_lsn;
    wal_write_header(wal);

    mutex_unlock(&wal->lock);
    return SPEEDSQL_OK;
}

int wal_checkpoint(wal_t* wal, buffer_pool_t* pool, file_t* db_file) {
    if (!wal || !pool || !db_file) {
        return SPEEDSQL_MISUSE;
    }

    mutex_lock(&wal->lock);

    /* Flush WAL buffer first */
    int rc = wal_flush_buffer(wal);
    if (rc != SPEEDSQL_OK) {
        mutex_unlock(&wal->lock);
        return rc;
    }

    /* Flush all dirty pages from buffer pool */
    rc = buffer_pool_flush(pool, db_file);
    if (rc != SPEEDSQL_OK) {
        mutex_unlock(&wal->lock);
        return rc;
    }

    /* Write checkpoint record */
    wal_record_header_t hdr = {0};
    hdr.lsn = wal->current_lsn++;
    hdr.txn_id = 0;
    hdr.type = WAL_RECORD_CHECKPOINT;
    hdr.page_id = INVALID_PAGE_ID;
    hdr.data_len = 0;

    rc = wal_append_record(wal, &hdr, nullptr, nullptr, 0);
    if (rc != SPEEDSQL_OK) {
        mutex_unlock(&wal->lock);
        return rc;
    }

    rc = wal_flush_buffer(wal);
    if (rc != SPEEDSQL_OK) {
        mutex_unlock(&wal->lock);
        return rc;
    }

    /* Update checkpoint LSN in header */
    wal->checkpoint_lsn = hdr.lsn;
    rc = wal_write_header(wal);
    if (rc != SPEEDSQL_OK) {
        mutex_unlock(&wal->lock);
        return rc;
    }

    /* Truncate WAL file (optional - removes old log records) */
    rc = file_truncate(&wal->file, WAL_HEADER_SIZE);
    if (rc == SPEEDSQL_OK) {
        wal->current_lsn = wal->checkpoint_lsn + 1;
        wal_write_header(wal);
    }

    mutex_unlock(&wal->lock);
    return SPEEDSQL_OK;
}
