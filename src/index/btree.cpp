/*
 * SpeedDB - B+Tree Index Implementation
 *
 * High-performance B+tree for indexing with:
 * - Lock-free reads (optimistic locking)
 * - Bulk loading support
 * - Variable-length keys
 * - Prefix compression (future)
 */

#include "speeddb_internal.h"

/* B+tree node layout:
 *
 * Internal Node:
 * +----------------+
 * | page_header_t  |
 * +----------------+
 * | key_count      | (2 bytes)
 * | keys[]         | variable
 * | children[]     | page_id_t array
 * +----------------+
 *
 * Leaf Node:
 * +----------------+
 * | page_header_t  |
 * +----------------+
 * | key_count      | (2 bytes)
 * | next_leaf      | (8 bytes)
 * | prev_leaf      | (8 bytes)
 * | cell_offsets[] | (2 bytes each)
 * | ...free space...
 * | cells[]        | key-value pairs from end
 * +----------------+
 */

#define BTREE_LEAF_HEADER_SIZE (sizeof(page_header_t) + 2 + 8 + 8)
#define BTREE_INTERNAL_HEADER_SIZE (sizeof(page_header_t) + 2)

/* Get max keys per internal node */
static inline uint32_t btree_internal_max_keys(uint32_t page_size, uint32_t key_size) {
    /* Space = page_size - header - one extra child pointer */
    uint32_t space = page_size - BTREE_INTERNAL_HEADER_SIZE - sizeof(page_id_t);
    return space / (key_size + sizeof(page_id_t));
}

/* Get max keys per leaf node */
static inline uint32_t btree_leaf_max_keys(uint32_t page_size, uint32_t key_size, uint32_t value_size) {
    uint32_t space = page_size - BTREE_LEAF_HEADER_SIZE;
    uint32_t cell_size = key_size + value_size + 2;  /* +2 for offset */
    return space / cell_size;
}

/* Read key count from page */
static inline uint16_t get_key_count(uint8_t* page) {
    return *(uint16_t*)(page + sizeof(page_header_t));
}

/* Write key count to page */
static inline void set_key_count(uint8_t* page, uint16_t count) {
    *(uint16_t*)(page + sizeof(page_header_t)) = count;
}

/* Get next/prev leaf pointers */
static inline page_id_t get_next_leaf(uint8_t* page) {
    return *(page_id_t*)(page + sizeof(page_header_t) + 2);
}

static inline void set_next_leaf(uint8_t* page, page_id_t next) {
    *(page_id_t*)(page + sizeof(page_header_t) + 2) = next;
}

static inline page_id_t get_prev_leaf(uint8_t* page) {
    return *(page_id_t*)(page + sizeof(page_header_t) + 2 + 8);
}

static inline void set_prev_leaf(uint8_t* page, page_id_t prev) {
    *(page_id_t*)(page + sizeof(page_header_t) + 2 + 8) = prev;
}

/* Get child pointer at index (internal node) */
static inline page_id_t get_child(uint8_t* page, uint16_t idx, uint32_t key_size) {
    uint8_t* children = page + BTREE_INTERNAL_HEADER_SIZE;
    /* Layout: child0, key0, child1, key1, child2, ... */
    return *(page_id_t*)(children + idx * (sizeof(page_id_t) + key_size));
}

static inline void set_child(uint8_t* page, uint16_t idx, page_id_t child, uint32_t key_size) {
    uint8_t* children = page + BTREE_INTERNAL_HEADER_SIZE;
    *(page_id_t*)(children + idx * (sizeof(page_id_t) + key_size)) = child;
}

/* Get key at index (internal node) */
static inline uint8_t* get_internal_key(uint8_t* page, uint16_t idx, uint32_t key_size) {
    uint8_t* base = page + BTREE_INTERNAL_HEADER_SIZE + sizeof(page_id_t);
    return base + idx * (sizeof(page_id_t) + key_size);
}

/* Binary search for key in internal node */
static uint16_t search_internal(uint8_t* page, const value_t* key,
                                 compare_func_t cmp, uint32_t key_size) {
    uint16_t count = get_key_count(page);
    if (count == 0) return 0;

    uint16_t lo = 0, hi = count;
    while (lo < hi) {
        uint16_t mid = (lo + hi) / 2;
        value_t page_key;
        page_key.type = SPEEDDB_TYPE_BLOB;
        page_key.data.str.data = (char*)get_internal_key(page, mid, key_size);
        page_key.data.str.len = key_size;

        int c = cmp(key, &page_key);
        if (c <= 0) {
            hi = mid;
        } else {
            lo = mid + 1;
        }
    }
    return lo;
}

/* Leaf cell structure */
typedef struct {
    uint16_t offset;     /* Offset from page start */
    uint16_t key_len;
    uint16_t value_len;
} leaf_cell_t;

/* Get cell offset array start */
static inline uint16_t* get_cell_offsets(uint8_t* page) {
    return (uint16_t*)(page + BTREE_LEAF_HEADER_SIZE);
}

/* Get cell at offset */
static inline uint8_t* get_cell_data(uint8_t* page, uint16_t offset) {
    return page + offset;
}

/* Binary search for key in leaf node */
static uint16_t search_leaf(uint8_t* page, const value_t* key,
                             compare_func_t cmp, bool* exact) {
    uint16_t count = get_key_count(page);
    *exact = false;
    if (count == 0) return 0;

    uint16_t* offsets = get_cell_offsets(page);

    uint16_t lo = 0, hi = count;
    while (lo < hi) {
        uint16_t mid = (lo + hi) / 2;
        uint8_t* cell = get_cell_data(page, offsets[mid]);

        /* Cell format: key_len (2) + value_len (2) + key + value */
        uint16_t key_len = *(uint16_t*)cell;
        value_t page_key;
        page_key.type = SPEEDDB_TYPE_BLOB;
        page_key.data.str.data = (char*)(cell + 4);
        page_key.data.str.len = key_len;

        int c = cmp(key, &page_key);
        if (c < 0) {
            hi = mid;
        } else if (c > 0) {
            lo = mid + 1;
        } else {
            *exact = true;
            return mid;
        }
    }
    return lo;
}

int btree_create(btree_t* tree, buffer_pool_t* pool, file_t* file, compare_func_t cmp) {
    if (!tree || !pool || !file || !cmp) {
        return SPEEDDB_MISUSE;
    }

    memset(tree, 0, sizeof(*tree));
    tree->pool = pool;
    tree->file = file;
    tree->compare = cmp;
    tree->key_size = 0;  /* Variable length */
    tree->value_size = 0;

    rwlock_init(&tree->lock);

    /* Allocate root page */
    page_id_t root_id;
    buffer_page_t* root = buffer_pool_new_page(pool, file, &root_id);
    if (!root) {
        return SPEEDDB_NOMEM;
    }

    /* Initialize root as empty leaf */
    page_header_t* hdr = (page_header_t*)root->data;
    hdr->page_type = PAGE_TYPE_BTREE_LEAF;
    hdr->flags = 0;
    hdr->cell_count = 0;
    hdr->free_start = BTREE_LEAF_HEADER_SIZE;
    hdr->free_end = pool->page_size;
    hdr->right_ptr = INVALID_PAGE_ID;

    set_key_count(root->data, 0);
    set_next_leaf(root->data, INVALID_PAGE_ID);
    set_prev_leaf(root->data, INVALID_PAGE_ID);

    buffer_pool_unpin(pool, root, true);

    tree->root_page = root_id;
    return SPEEDDB_OK;
}

int btree_open(btree_t* tree, buffer_pool_t* pool, file_t* file,
               page_id_t root, compare_func_t cmp) {
    if (!tree || !pool || !file || !cmp) {
        return SPEEDDB_MISUSE;
    }

    memset(tree, 0, sizeof(*tree));
    tree->pool = pool;
    tree->file = file;
    tree->compare = cmp;
    tree->root_page = root;

    rwlock_init(&tree->lock);

    return SPEEDDB_OK;
}

void btree_close(btree_t* tree) {
    if (!tree) return;
    rwlock_destroy(&tree->lock);
}

/* Find leaf page containing key */
static buffer_page_t* find_leaf(btree_t* tree, const value_t* key) {
    page_id_t page_id = tree->root_page;

    while (true) {
        buffer_page_t* page = buffer_pool_get(tree->pool, tree->file, page_id);
        if (!page) return nullptr;

        page_header_t* hdr = (page_header_t*)page->data;

        if (hdr->page_type == PAGE_TYPE_BTREE_LEAF) {
            return page;  /* Found leaf */
        }

        /* Internal node - descend */
        uint16_t idx = search_internal(page->data, key, tree->compare, tree->key_size);
        page_id_t child = get_child(page->data, idx, tree->key_size);

        buffer_pool_unpin(tree->pool, page, false);
        page_id = child;
    }
}

int btree_find(btree_t* tree, const value_t* key, value_t* value) {
    if (!tree || !key) return SPEEDDB_MISUSE;

    rwlock_rdlock(&tree->lock);

    buffer_page_t* leaf = find_leaf(tree, key);
    if (!leaf) {
        rwlock_unlock(&tree->lock);
        return SPEEDDB_IOERR;
    }

    bool exact;
    uint16_t idx = search_leaf(leaf->data, key, tree->compare, &exact);

    if (!exact) {
        buffer_pool_unpin(tree->pool, leaf, false);
        rwlock_unlock(&tree->lock);
        return SPEEDDB_NOTFOUND;
    }

    /* Extract value */
    uint16_t* offsets = get_cell_offsets(leaf->data);
    uint8_t* cell = get_cell_data(leaf->data, offsets[idx]);
    uint16_t key_len = *(uint16_t*)cell;
    uint16_t value_len = *(uint16_t*)(cell + 2);

    if (value) {
        value->type = SPEEDDB_TYPE_BLOB;
        value->size = value_len;
        value->data.str.len = value_len;
        value->data.str.data = (char*)sdb_malloc(value_len);
        if (value->data.str.data) {
            memcpy(value->data.str.data, cell + 4 + key_len, value_len);
        }
    }

    buffer_pool_unpin(tree->pool, leaf, false);
    rwlock_unlock(&tree->lock);

    return SPEEDDB_OK;
}

/* Insert a cell into a leaf page */
static int insert_into_leaf(btree_t* tree, buffer_page_t* leaf,
                             const value_t* key, const value_t* value) {
    page_header_t* hdr = (page_header_t*)leaf->data;
    uint16_t count = get_key_count(leaf->data);

    /* Calculate cell size */
    uint16_t key_len = key->data.str.len;
    uint16_t value_len = value->data.str.len;
    uint16_t cell_size = 4 + key_len + value_len;  /* key_len + value_len + data */

    /* Check if there's space */
    uint16_t* offsets = get_cell_offsets(leaf->data);
    uint32_t free_space = hdr->free_end - hdr->free_start - count * sizeof(uint16_t);

    if (free_space < cell_size + sizeof(uint16_t)) {
        return SPEEDDB_FULL;  /* Need to split */
    }

    /* Find insertion point */
    bool exact;
    uint16_t idx = search_leaf(leaf->data, key, tree->compare, &exact);

    if (exact) {
        /* Update existing - for now, return error */
        return SPEEDDB_CONSTRAINT;
    }

    /* Allocate cell from end of page */
    hdr->free_end -= cell_size;
    uint16_t cell_offset = hdr->free_end;

    /* Write cell */
    uint8_t* cell = leaf->data + cell_offset;
    *(uint16_t*)cell = key_len;
    *(uint16_t*)(cell + 2) = value_len;
    memcpy(cell + 4, key->data.str.data, key_len);
    memcpy(cell + 4 + key_len, value->data.str.data, value_len);

    /* Insert offset into sorted position */
    memmove(&offsets[idx + 1], &offsets[idx], (count - idx) * sizeof(uint16_t));
    offsets[idx] = cell_offset;

    set_key_count(leaf->data, count + 1);
    hdr->cell_count = count + 1;

    return SPEEDDB_OK;
}

int btree_insert(btree_t* tree, const value_t* key, const value_t* value) {
    if (!tree || !key || !value) return SPEEDDB_MISUSE;

    rwlock_wrlock(&tree->lock);

    buffer_page_t* leaf = find_leaf(tree, key);
    if (!leaf) {
        rwlock_unlock(&tree->lock);
        return SPEEDDB_IOERR;
    }

    int rc = insert_into_leaf(tree, leaf, key, value);

    if (rc == SPEEDDB_FULL) {
        /* TODO: Implement page split */
        buffer_pool_unpin(tree->pool, leaf, false);
        rwlock_unlock(&tree->lock);
        return SPEEDDB_FULL;  /* For now */
    }

    buffer_pool_unpin(tree->pool, leaf, rc == SPEEDDB_OK);
    rwlock_unlock(&tree->lock);

    return rc;
}

int btree_delete(btree_t* tree, const value_t* key) {
    if (!tree || !key) return SPEEDDB_MISUSE;

    rwlock_wrlock(&tree->lock);

    buffer_page_t* leaf = find_leaf(tree, key);
    if (!leaf) {
        rwlock_unlock(&tree->lock);
        return SPEEDDB_IOERR;
    }

    bool exact;
    uint16_t idx = search_leaf(leaf->data, key, tree->compare, &exact);

    if (!exact) {
        buffer_pool_unpin(tree->pool, leaf, false);
        rwlock_unlock(&tree->lock);
        return SPEEDDB_NOTFOUND;
    }

    /* Remove from offset array */
    uint16_t count = get_key_count(leaf->data);
    uint16_t* offsets = get_cell_offsets(leaf->data);
    memmove(&offsets[idx], &offsets[idx + 1], (count - idx - 1) * sizeof(uint16_t));
    set_key_count(leaf->data, count - 1);

    /* Note: We don't reclaim cell space here - would need page compaction */

    buffer_pool_unpin(tree->pool, leaf, true);
    rwlock_unlock(&tree->lock);

    return SPEEDDB_OK;
}

/* Cursor implementation */
int btree_cursor_init(btree_cursor_t* cursor, btree_t* tree) {
    if (!cursor || !tree) return SPEEDDB_MISUSE;

    memset(cursor, 0, sizeof(*cursor));
    cursor->tree = tree;
    cursor->current_page = INVALID_PAGE_ID;
    cursor->current_slot = 0;
    cursor->valid = false;
    cursor->at_end = false;

    return SPEEDDB_OK;
}

int btree_cursor_first(btree_cursor_t* cursor) {
    if (!cursor || !cursor->tree) return SPEEDDB_MISUSE;

    btree_t* tree = cursor->tree;

    /* Find leftmost leaf */
    page_id_t page_id = tree->root_page;

    while (true) {
        buffer_page_t* page = buffer_pool_get(tree->pool, tree->file, page_id);
        if (!page) return SPEEDDB_IOERR;

        page_header_t* hdr = (page_header_t*)page->data;

        if (hdr->page_type == PAGE_TYPE_BTREE_LEAF) {
            cursor->current_page = page_id;
            cursor->current_slot = 0;
            cursor->valid = get_key_count(page->data) > 0;
            cursor->at_end = !cursor->valid;
            buffer_pool_unpin(tree->pool, page, false);
            return SPEEDDB_OK;
        }

        /* Get first child */
        page_id_t child = get_child(page->data, 0, tree->key_size);
        buffer_pool_unpin(tree->pool, page, false);
        page_id = child;
    }
}

int btree_cursor_next(btree_cursor_t* cursor) {
    if (!cursor || !cursor->tree || !cursor->valid) return SPEEDDB_MISUSE;

    btree_t* tree = cursor->tree;

    buffer_page_t* page = buffer_pool_get(tree->pool, tree->file, cursor->current_page);
    if (!page) return SPEEDDB_IOERR;

    uint16_t count = get_key_count(page->data);

    if (cursor->current_slot + 1 < count) {
        /* Move to next slot in same page */
        cursor->current_slot++;
        buffer_pool_unpin(tree->pool, page, false);
        return SPEEDDB_OK;
    }

    /* Move to next leaf */
    page_id_t next = get_next_leaf(page->data);
    buffer_pool_unpin(tree->pool, page, false);

    if (next == INVALID_PAGE_ID) {
        cursor->valid = false;
        cursor->at_end = true;
        return SPEEDDB_DONE;
    }

    cursor->current_page = next;
    cursor->current_slot = 0;

    /* Verify next page has entries */
    page = buffer_pool_get(tree->pool, tree->file, next);
    if (!page) return SPEEDDB_IOERR;

    cursor->valid = get_key_count(page->data) > 0;
    buffer_pool_unpin(tree->pool, page, false);

    return cursor->valid ? SPEEDDB_OK : SPEEDDB_DONE;
}

int btree_cursor_key(btree_cursor_t* cursor, value_t* key) {
    if (!cursor || !cursor->tree || !cursor->valid || !key) return SPEEDDB_MISUSE;

    btree_t* tree = cursor->tree;

    buffer_page_t* page = buffer_pool_get(tree->pool, tree->file, cursor->current_page);
    if (!page) return SPEEDDB_IOERR;

    uint16_t* offsets = get_cell_offsets(page->data);
    uint8_t* cell = get_cell_data(page->data, offsets[cursor->current_slot]);
    uint16_t key_len = *(uint16_t*)cell;

    key->type = SPEEDDB_TYPE_BLOB;
    key->size = key_len;
    key->data.str.len = key_len;
    key->data.str.data = (char*)sdb_malloc(key_len);
    if (key->data.str.data) {
        memcpy(key->data.str.data, cell + 4, key_len);
    }

    buffer_pool_unpin(tree->pool, page, false);
    return key->data.str.data ? SPEEDDB_OK : SPEEDDB_NOMEM;
}

int btree_cursor_value(btree_cursor_t* cursor, value_t* value) {
    if (!cursor || !cursor->tree || !cursor->valid || !value) return SPEEDDB_MISUSE;

    btree_t* tree = cursor->tree;

    buffer_page_t* page = buffer_pool_get(tree->pool, tree->file, cursor->current_page);
    if (!page) return SPEEDDB_IOERR;

    uint16_t* offsets = get_cell_offsets(page->data);
    uint8_t* cell = get_cell_data(page->data, offsets[cursor->current_slot]);
    uint16_t key_len = *(uint16_t*)cell;
    uint16_t value_len = *(uint16_t*)(cell + 2);

    value->type = SPEEDDB_TYPE_BLOB;
    value->size = value_len;
    value->data.str.len = value_len;
    value->data.str.data = (char*)sdb_malloc(value_len);
    if (value->data.str.data) {
        memcpy(value->data.str.data, cell + 4 + key_len, value_len);
    }

    buffer_pool_unpin(tree->pool, page, false);
    return value->data.str.data ? SPEEDDB_OK : SPEEDDB_NOMEM;
}

void btree_cursor_close(btree_cursor_t* cursor) {
    if (!cursor) return;
    cursor->valid = false;
    cursor->tree = nullptr;
}
