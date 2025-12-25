/*
 * SpeedSQL - Buffer Pool / Page Cache
 *
 * High-performance page cache using LRU eviction and concurrent access.
 * This is critical for performance - SQLite's cache is often a bottleneck.
 */

#include "speedsql_internal.h"
#include "speedsql_crypto.h"

/* Forward declarations for encryption functions */
static int encrypt_page(buffer_pool_t* pool, page_id_t page_id,
                        const uint8_t* plaintext, uint8_t* ciphertext);
static int decrypt_page(buffer_pool_t* pool, page_id_t page_id,
                        const uint8_t* ciphertext, uint8_t* plaintext);
static int read_page_encrypted(buffer_pool_t* pool, file_t* file,
                               page_id_t page_id, uint8_t* data);
static int write_page_encrypted(buffer_pool_t* pool, file_t* file,
                                page_id_t page_id, const uint8_t* data);

/* Hash function for page IDs */
static inline size_t page_hash(page_id_t page_id, size_t size) {
    /* Fibonacci hashing for better distribution */
    return (size_t)((page_id * 11400714819323198485ULL) >> (64 - 20)) % size;
}

int buffer_pool_init(buffer_pool_t* pool, size_t cache_size, uint32_t page_size) {
    if (!pool || cache_size == 0 || page_size == 0) {
        return SPEEDSQL_MISUSE;
    }

    memset(pool, 0, sizeof(*pool));

    pool->page_size = page_size;
    pool->page_count = cache_size / page_size;
    if (pool->page_count < 16) {
        pool->page_count = 16;  /* Minimum cache size */
    }

    /* Hash table size - prime number slightly larger than page count */
    pool->hash_size = pool->page_count + pool->page_count / 4;
    if (pool->hash_size < 17) pool->hash_size = 17;

    /* Allocate hash table */
    pool->hash_table = (buffer_page_t**)sdb_calloc(pool->hash_size, sizeof(buffer_page_t*));
    if (!pool->hash_table) {
        return SPEEDSQL_NOMEM;
    }

    /* Pre-allocate all buffer pages */
    for (size_t i = 0; i < pool->page_count; i++) {
        buffer_page_t* page = (buffer_page_t*)sdb_malloc(sizeof(buffer_page_t));
        if (!page) {
            buffer_pool_destroy(pool);
            return SPEEDSQL_NOMEM;
        }

        page->data = (uint8_t*)sdb_malloc(page_size);
        if (!page->data) {
            sdb_free(page);
            buffer_pool_destroy(pool);
            return SPEEDSQL_NOMEM;
        }

        page->page_id = INVALID_PAGE_ID;
        page->state = BUF_INVALID;
        page->pin_count = 0;
        page->last_access = 0;
        page->hash_next = nullptr;
        page->lru_prev = nullptr;
        page->lru_next = pool->free_list;

        if (pool->free_list) {
            pool->free_list->lru_prev = page;
        }
        pool->free_list = page;
    }

    mutex_init(&pool->lock);

    return SPEEDSQL_OK;
}

void buffer_pool_destroy(buffer_pool_t* pool) {
    if (!pool) return;

    /* Pages are in either:
     * 1. Free list (not in use)
     * 2. Hash table + LRU list (in use)
     * We must free each page exactly once.
     */

    /* Free pages in free list (these are NOT in hash table or LRU) */
    buffer_page_t* page = pool->free_list;
    while (page) {
        buffer_page_t* next = page->lru_next;
        sdb_free(page->data);
        sdb_free(page);
        page = next;
    }

    /* Free pages in LRU list (these ARE in hash table too) */
    page = pool->lru_head;
    while (page) {
        buffer_page_t* next = page->lru_next;
        sdb_free(page->data);
        sdb_free(page);
        page = next;
    }

    /* Free the hash table itself (pages already freed above) */
    if (pool->hash_table) {
        sdb_free(pool->hash_table);
    }

    /* Free encryption buffer if allocated */
    if (pool->crypt_buffer) {
        sdb_free(pool->crypt_buffer);
        pool->crypt_buffer = nullptr;
    }

    mutex_destroy(&pool->lock);
}

/* Remove page from hash table */
static void hash_remove(buffer_pool_t* pool, buffer_page_t* page) {
    size_t bucket = page_hash(page->page_id, pool->hash_size);
    buffer_page_t** pp = &pool->hash_table[bucket];

    while (*pp) {
        if (*pp == page) {
            *pp = page->hash_next;
            page->hash_next = nullptr;
            return;
        }
        pp = &(*pp)->hash_next;
    }
}

/* Add page to hash table */
static void hash_insert(buffer_pool_t* pool, buffer_page_t* page) {
    size_t bucket = page_hash(page->page_id, pool->hash_size);
    page->hash_next = pool->hash_table[bucket];
    pool->hash_table[bucket] = page;
}

/* Find page in hash table */
static buffer_page_t* hash_find(buffer_pool_t* pool, page_id_t page_id) {
    size_t bucket = page_hash(page_id, pool->hash_size);
    buffer_page_t* page = pool->hash_table[bucket];

    while (page) {
        if (page->page_id == page_id) {
            return page;
        }
        page = page->hash_next;
    }

    return nullptr;
}

/* Remove page from LRU list */
static void lru_remove(buffer_pool_t* pool, buffer_page_t* page) {
    if (page->lru_prev) {
        page->lru_prev->lru_next = page->lru_next;
    } else {
        pool->lru_head = page->lru_next;
    }

    if (page->lru_next) {
        page->lru_next->lru_prev = page->lru_prev;
    } else {
        pool->lru_tail = page->lru_prev;
    }

    page->lru_prev = nullptr;
    page->lru_next = nullptr;
}

/* Add page to front of LRU list (most recently used) */
static void lru_insert_front(buffer_pool_t* pool, buffer_page_t* page) {
    page->lru_prev = nullptr;
    page->lru_next = pool->lru_head;

    if (pool->lru_head) {
        pool->lru_head->lru_prev = page;
    } else {
        pool->lru_tail = page;
    }

    pool->lru_head = page;
}

/* Get a victim page for eviction */
static buffer_page_t* get_victim(buffer_pool_t* pool, file_t* file) {
    /* First try free list */
    if (pool->free_list) {
        buffer_page_t* page = pool->free_list;
        pool->free_list = page->lru_next;
        if (pool->free_list) {
            pool->free_list->lru_prev = nullptr;
        }
        return page;
    }

    /* Otherwise evict from LRU tail */
    buffer_page_t* page = pool->lru_tail;
    while (page) {
        if (page->pin_count == 0) {
            /* Found victim - remove from LRU and hash */
            lru_remove(pool, page);
            hash_remove(pool, page);

            /* Write back if dirty */
            if (page->state == BUF_DIRTY && file) {
                write_page_encrypted(pool, file, page->page_id, page->data);
            }

            pool->used_count--;
            return page;
        }
        page = page->lru_prev;
    }

    /* All pages pinned - cannot evict */
    return nullptr;
}

buffer_page_t* buffer_pool_get(buffer_pool_t* pool, file_t* file, page_id_t page_id) {
    if (!pool || !file) return nullptr;

    mutex_lock(&pool->lock);

    /* Check if already in cache */
    buffer_page_t* page = hash_find(pool, page_id);
    if (page) {
        /* Cache hit */
        pool->hits++;
        page->pin_count++;
        page->last_access = get_timestamp_us();

        /* Move to front of LRU if not pinned by others */
        if (page->pin_count == 1) {
            lru_remove(pool, page);
            lru_insert_front(pool, page);
        }

        mutex_unlock(&pool->lock);
        return page;
    }

    /* Cache miss */
    pool->misses++;

    /* Get a page to use */
    page = get_victim(pool, file);
    if (!page) {
        mutex_unlock(&pool->lock);
        return nullptr;  /* No pages available */
    }

    /* Read page from disk (with decryption if enabled) */
    page->page_id = page_id;
    int rc = read_page_encrypted(pool, file, page_id, page->data);
    if (rc != SPEEDSQL_OK) {
        /* Read failed - put page back on free list */
        page->page_id = INVALID_PAGE_ID;
        page->lru_next = pool->free_list;
        if (pool->free_list) {
            pool->free_list->lru_prev = page;
        }
        pool->free_list = page;
        mutex_unlock(&pool->lock);
        return nullptr;
    }

    page->state = BUF_CLEAN;
    page->pin_count = 1;
    page->last_access = get_timestamp_us();

    /* Add to hash and LRU */
    hash_insert(pool, page);
    lru_insert_front(pool, page);
    pool->used_count++;

    mutex_unlock(&pool->lock);
    return page;
}

void buffer_pool_unpin(buffer_pool_t* pool, buffer_page_t* page, bool dirty) {
    if (!pool || !page) return;

    mutex_lock(&pool->lock);

    if (page->pin_count > 0) {
        page->pin_count--;
    }

    if (dirty && page->state != BUF_DIRTY) {
        page->state = BUF_DIRTY;
    }

    mutex_unlock(&pool->lock);
}

int buffer_pool_flush(buffer_pool_t* pool, file_t* file) {
    if (!pool || !file) return SPEEDSQL_MISUSE;

    mutex_lock(&pool->lock);

    int rc = SPEEDSQL_OK;

    /* Iterate through all pages in hash table */
    for (size_t i = 0; i < pool->hash_size && rc == SPEEDSQL_OK; i++) {
        buffer_page_t* page = pool->hash_table[i];
        while (page) {
            if (page->state == BUF_DIRTY) {
                rc = write_page_encrypted(pool, file, page->page_id, page->data);
                if (rc == SPEEDSQL_OK) {
                    page->state = BUF_CLEAN;
                }
            }
            page = page->hash_next;
        }
    }

    if (rc == SPEEDSQL_OK) {
        rc = file_sync(file);
    }

    mutex_unlock(&pool->lock);
    return rc;
}

buffer_page_t* buffer_pool_new_page(buffer_pool_t* pool, file_t* file, page_id_t* page_id_out) {
    if (!pool || !file || !page_id_out) return nullptr;

    mutex_lock(&pool->lock);

    /* Calculate new page ID */
    uint64_t current_file_size;
    file_size(file, &current_file_size);
    page_id_t new_page_id = current_file_size / pool->page_size;

    /* Get a page from free list or evict */
    buffer_page_t* page = get_victim(pool, file);
    if (!page) {
        mutex_unlock(&pool->lock);
        return nullptr;
    }

    /* Initialize new page */
    page->page_id = new_page_id;
    memset(page->data, 0, pool->page_size);
    page->state = BUF_DIRTY;
    page->pin_count = 1;
    page->last_access = get_timestamp_us();

    /* Extend file (with encryption if enabled) */
    int rc = write_page_encrypted(pool, file, new_page_id, page->data);
    if (rc != SPEEDSQL_OK) {
        page->page_id = INVALID_PAGE_ID;
        page->state = BUF_INVALID;
        page->lru_next = pool->free_list;
        if (pool->free_list) {
            pool->free_list->lru_prev = page;
        }
        pool->free_list = page;
        mutex_unlock(&pool->lock);
        return nullptr;
    }

    /* Add to hash and LRU */
    hash_insert(pool, page);
    lru_insert_front(pool, page);
    pool->used_count++;

    *page_id_out = new_page_id;

    mutex_unlock(&pool->lock);
    return page;
}

/* Invalidate all dirty pages (for rollback) */
int buffer_pool_invalidate_dirty(buffer_pool_t* pool, file_t* file) {
    if (!pool || !file) return SPEEDSQL_MISUSE;

    mutex_lock(&pool->lock);

    /* Iterate through all pages in hash table */
    for (size_t i = 0; i < pool->hash_size; i++) {
        buffer_page_t* page = pool->hash_table[i];
        buffer_page_t* prev = nullptr;

        while (page) {
            buffer_page_t* next = page->hash_next;

            if (page->state == BUF_DIRTY) {
                /* Remove from hash table */
                if (prev) {
                    prev->hash_next = next;
                } else {
                    pool->hash_table[i] = next;
                }

                /* Remove from LRU list */
                if (page->lru_prev) {
                    page->lru_prev->lru_next = page->lru_next;
                } else {
                    pool->lru_head = page->lru_next;
                }
                if (page->lru_next) {
                    page->lru_next->lru_prev = page->lru_prev;
                } else {
                    pool->lru_tail = page->lru_prev;
                }

                /* Reset page and add to free list */
                page->page_id = INVALID_PAGE_ID;
                page->state = BUF_INVALID;
                page->pin_count = 0;
                page->hash_next = nullptr;
                page->lru_prev = nullptr;
                page->lru_next = pool->free_list;

                if (pool->free_list) {
                    pool->free_list->lru_prev = page;
                }
                pool->free_list = page;
                pool->used_count--;
            } else {
                prev = page;
            }

            page = next;
        }
    }

    mutex_unlock(&pool->lock);
    return SPEEDSQL_OK;
}

/* ============================================================================
 * Page-Level Encryption Support
 * ============================================================================ */

int buffer_pool_set_encryption(buffer_pool_t* pool, struct speedsql_cipher_ctx* ctx, speedsql_cipher_t cipher_id) {
    if (!pool) return SPEEDSQL_MISUSE;

    mutex_lock(&pool->lock);

    pool->cipher_ctx = ctx;
    pool->cipher_id = cipher_id;

    /* Allocate encryption buffer if needed */
    if (ctx && !pool->crypt_buffer) {
        /* Allocate buffer for encryption: page + IV + tag */
        pool->crypt_buffer = (uint8_t*)sdb_malloc(pool->page_size + 32 + 16);
        if (!pool->crypt_buffer) {
            mutex_unlock(&pool->lock);
            return SPEEDSQL_NOMEM;
        }
    }

    mutex_unlock(&pool->lock);
    return SPEEDSQL_OK;
}

/* Encrypt a page before writing to disk */
static int encrypt_page(buffer_pool_t* pool, page_id_t page_id,
                        const uint8_t* plaintext, uint8_t* ciphertext) {
    if (!pool->cipher_ctx) {
        /* No encryption - just copy */
        memcpy(ciphertext, plaintext, pool->page_size);
        return SPEEDSQL_OK;
    }

    const speedsql_cipher_provider_t* provider = speedsql_get_cipher(pool->cipher_id);
    if (!provider || !provider->encrypt) {
        return SPEEDSQL_ERROR;
    }

    /* Generate IV from page ID (deterministic for same page, ensures unique IV) */
    uint8_t iv[24] = {0};  /* Max IV size */
    uint64_t page_id_64 = page_id;
    memcpy(iv, &page_id_64, sizeof(page_id_64));
    /* Add some entropy to the IV */
    iv[8] = 0x53;  /* 'S' for SpeedSQL */
    iv[9] = 0x51;  /* 'Q' */
    iv[10] = 0x4C; /* 'L' */

    /* AAD (Additional Authenticated Data): page ID for integrity */
    uint8_t aad[8];
    memcpy(aad, &page_id_64, sizeof(aad));

    /* Encrypt the page */
    uint8_t tag[16];
    int rc = provider->encrypt(
        pool->cipher_ctx,
        plaintext,
        pool->page_size,
        iv,
        aad,
        sizeof(aad),
        ciphertext,
        tag
    );

    if (rc != SPEEDSQL_OK) {
        return rc;
    }

    /* Append tag to ciphertext (for authenticated ciphers) */
    if (provider->tag_size > 0) {
        memcpy(ciphertext + pool->page_size, tag, provider->tag_size);
    }

    return SPEEDSQL_OK;
}

/* Decrypt a page after reading from disk */
static int decrypt_page(buffer_pool_t* pool, page_id_t page_id,
                        const uint8_t* ciphertext, uint8_t* plaintext) {
    if (!pool->cipher_ctx) {
        /* No encryption - just copy */
        memcpy(plaintext, ciphertext, pool->page_size);
        return SPEEDSQL_OK;
    }

    const speedsql_cipher_provider_t* provider = speedsql_get_cipher(pool->cipher_id);
    if (!provider || !provider->decrypt) {
        return SPEEDSQL_ERROR;
    }

    /* Reconstruct IV from page ID */
    uint8_t iv[24] = {0};
    uint64_t page_id_64 = page_id;
    memcpy(iv, &page_id_64, sizeof(page_id_64));
    iv[8] = 0x53;
    iv[9] = 0x51;
    iv[10] = 0x4C;

    /* AAD: page ID */
    uint8_t aad[8];
    memcpy(aad, &page_id_64, sizeof(aad));

    /* Get tag from after ciphertext */
    const uint8_t* tag = ciphertext + pool->page_size;

    /* Decrypt the page */
    int rc = provider->decrypt(
        pool->cipher_ctx,
        ciphertext,
        pool->page_size,
        iv,
        aad,
        sizeof(aad),
        tag,
        plaintext
    );

    return rc;
}

/* Read page from disk with decryption */
static int read_page_encrypted(buffer_pool_t* pool, file_t* file,
                               page_id_t page_id, uint8_t* data) {
    if (!pool->cipher_ctx) {
        /* No encryption - direct read */
        return file_read(file, page_id * pool->page_size, data, pool->page_size);
    }

    const speedsql_cipher_provider_t* provider = speedsql_get_cipher(pool->cipher_id);
    size_t encrypted_size = pool->page_size;
    if (provider && provider->tag_size > 0) {
        encrypted_size += provider->tag_size;
    }

    /* Read encrypted page into temp buffer */
    int rc = file_read(file, page_id * encrypted_size, pool->crypt_buffer, encrypted_size);
    if (rc != SPEEDSQL_OK) {
        return rc;
    }

    /* Decrypt into destination */
    return decrypt_page(pool, page_id, pool->crypt_buffer, data);
}

/* Write page to disk with encryption */
static int write_page_encrypted(buffer_pool_t* pool, file_t* file,
                                page_id_t page_id, const uint8_t* data) {
    if (!pool->cipher_ctx) {
        /* No encryption - direct write */
        return file_write(file, page_id * pool->page_size, data, pool->page_size);
    }

    const speedsql_cipher_provider_t* provider = speedsql_get_cipher(pool->cipher_id);
    size_t encrypted_size = pool->page_size;
    if (provider && provider->tag_size > 0) {
        encrypted_size += provider->tag_size;
    }

    /* Encrypt into temp buffer */
    int rc = encrypt_page(pool, page_id, data, pool->crypt_buffer);
    if (rc != SPEEDSQL_OK) {
        return rc;
    }

    /* Write encrypted page */
    return file_write(file, page_id * encrypted_size, pool->crypt_buffer, encrypted_size);
}
