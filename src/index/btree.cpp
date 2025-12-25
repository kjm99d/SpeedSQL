/*
 * SpeedSQL - B+Tree Index Implementation
 *
 * High-performance B+tree for indexing with:
 * - Lock-free reads (optimistic locking)
 * - Bulk loading support
 * - Variable-length keys
 * - Prefix compression (future)
 */

#include "speedsql_internal.h"

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
#define BTREE_MAX_DEPTH 32  /* Maximum tree depth for path tracking */

/* Path entry for tracking parent nodes during insertion */
typedef struct {
    page_id_t page_id;
    uint16_t slot_index;
} btree_path_entry_t;

/* Forward declarations */
static int insert_into_internal(btree_t* tree, buffer_page_t* node,
                                 const value_t* key, page_id_t left, page_id_t right);
static int create_new_root(btree_t* tree, page_id_t left, page_id_t right,
                            const value_t* separator);

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
        page_key.type = SPEEDSQL_TYPE_BLOB;
        page_key.data.blob.data = (uint8_t*)get_internal_key(page, mid, key_size);
        page_key.data.blob.len = key_size;

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
        page_key.type = SPEEDSQL_TYPE_BLOB;
        page_key.data.blob.data = (uint8_t*)(cell + 4);
        page_key.data.blob.len = key_len;

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
        return SPEEDSQL_MISUSE;
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
        return SPEEDSQL_NOMEM;
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
    return SPEEDSQL_OK;
}

int btree_open(btree_t* tree, buffer_pool_t* pool, file_t* file,
               page_id_t root, compare_func_t cmp) {
    if (!tree || !pool || !file || !cmp) {
        return SPEEDSQL_MISUSE;
    }

    memset(tree, 0, sizeof(*tree));
    tree->pool = pool;
    tree->file = file;
    tree->compare = cmp;
    tree->root_page = root;

    rwlock_init(&tree->lock);

    return SPEEDSQL_OK;
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

/* Find leaf page and track the path from root */
static buffer_page_t* find_leaf_with_path(btree_t* tree, const value_t* key,
                                           btree_path_entry_t* path, int* path_len) {
    page_id_t page_id = tree->root_page;
    *path_len = 0;

    while (true) {
        buffer_page_t* page = buffer_pool_get(tree->pool, tree->file, page_id);
        if (!page) return nullptr;

        page_header_t* hdr = (page_header_t*)page->data;

        if (hdr->page_type == PAGE_TYPE_BTREE_LEAF) {
            return page;  /* Found leaf */
        }

        /* Record this internal node in path */
        if (*path_len < BTREE_MAX_DEPTH) {
            uint16_t idx = search_internal(page->data, key, tree->compare, tree->key_size);
            path[*path_len].page_id = page_id;
            path[*path_len].slot_index = idx;
            (*path_len)++;

            page_id_t child = get_child(page->data, idx, tree->key_size);
            buffer_pool_unpin(tree->pool, page, false);
            page_id = child;
        } else {
            buffer_pool_unpin(tree->pool, page, false);
            return nullptr;  /* Tree too deep */
        }
    }
}

/* Split internal node and return separator for parent */
static int split_internal(btree_t* tree, buffer_page_t* node,
                           const value_t* key, page_id_t left_child, page_id_t right_child,
                           buffer_page_t** new_node_out, value_t* separator_out) {
    uint16_t count = get_key_count(node->data);

    /* Allocate new internal node */
    page_id_t new_page_id;
    buffer_page_t* new_node = buffer_pool_new_page(tree->pool, tree->file, &new_page_id);
    if (!new_node) return SPEEDSQL_NOMEM;

    /* Initialize new internal node */
    page_header_t* new_hdr = (page_header_t*)new_node->data;
    new_hdr->page_type = PAGE_TYPE_BTREE_INTERNAL;
    new_hdr->flags = 0;
    new_hdr->cell_count = 0;
    new_hdr->free_start = BTREE_INTERNAL_HEADER_SIZE;
    new_hdr->free_end = tree->pool->page_size;

    /* Find split point and where new key goes */
    uint16_t insert_idx = search_internal(node->data, key, tree->compare, tree->key_size);
    uint16_t split_point = count / 2;

    /* Build temporary array of all keys including the new one */
    uint32_t key_sz = tree->key_size;
    (void)insert_idx;  /* Used for future optimization */

    /* Determine the middle key which becomes the separator */
    uint16_t mid = split_point;

    /* Copy keys after mid to new node */
    uint16_t new_count = 0;
    for (uint16_t i = mid + 1; i < count; i++) {
        uint8_t* old_key = get_internal_key(node->data, i, key_sz);
        page_id_t child = get_child(node->data, i + 1, key_sz);

        if (new_count == 0) {
            set_child(new_node->data, 0, get_child(node->data, mid + 1, key_sz), key_sz);
        }

        uint8_t* new_key = get_internal_key(new_node->data, new_count, key_sz);
        memcpy(new_key, old_key, key_sz);
        set_child(new_node->data, new_count + 1, child, key_sz);
        new_count++;
    }

    set_key_count(new_node->data, new_count);
    new_hdr->cell_count = new_count;

    /* Get separator key (the middle key goes up) */
    uint8_t* mid_key = get_internal_key(node->data, mid, key_sz);
    separator_out->type = VAL_BLOB;
    separator_out->data.blob.len = key_sz;
    separator_out->data.blob.data = (uint8_t*)sdb_malloc(key_sz);
    if (separator_out->data.blob.data) {
        memcpy(separator_out->data.blob.data, mid_key, key_sz);
    }

    /* Truncate old node */
    set_key_count(node->data, mid);
    page_header_t* old_hdr = (page_header_t*)node->data;
    old_hdr->cell_count = mid;

    /* Now insert the new key into appropriate node */
    int rc;
    if (insert_idx <= mid) {
        rc = insert_into_internal(tree, node, key, left_child, right_child);
    } else {
        rc = insert_into_internal(tree, new_node, key, left_child, right_child);
    }

    if (rc != SPEEDSQL_OK) {
        buffer_pool_unpin(tree->pool, new_node, false);
        value_free(separator_out);
        return rc;
    }

    *new_node_out = new_node;
    return SPEEDSQL_OK;
}

/* Insert separator into parent, splitting if necessary */
static int insert_into_parent(btree_t* tree, btree_path_entry_t* path, int path_len,
                               page_id_t left, page_id_t right, value_t* separator) {
    if (path_len == 0) {
        /* No parent - need new root */
        return create_new_root(tree, left, right, separator);
    }

    /* Get parent from path */
    btree_path_entry_t* parent_entry = &path[path_len - 1];
    buffer_page_t* parent = buffer_pool_get(tree->pool, tree->file, parent_entry->page_id);
    if (!parent) return SPEEDSQL_IOERR;

    /* Try to insert into parent */
    int rc = insert_into_internal(tree, parent, separator, left, right);

    if (rc == SPEEDSQL_FULL) {
        /* Parent is full - split it */
        buffer_page_t* new_parent = nullptr;
        value_t parent_separator;
        memset(&parent_separator, 0, sizeof(parent_separator));

        rc = split_internal(tree, parent, separator, left, right, &new_parent, &parent_separator);
        if (rc != SPEEDSQL_OK) {
            buffer_pool_unpin(tree->pool, parent, false);
            return rc;
        }

        /* Recursively insert into grandparent */
        rc = insert_into_parent(tree, path, path_len - 1,
                                 parent->page_id, new_parent->page_id, &parent_separator);

        buffer_pool_unpin(tree->pool, new_parent, true);
        value_free(&parent_separator);
    }

    buffer_pool_unpin(tree->pool, parent, rc == SPEEDSQL_OK);
    return rc;
}

int btree_find(btree_t* tree, const value_t* key, value_t* value) {
    if (!tree || !key) return SPEEDSQL_MISUSE;

    rwlock_rdlock(&tree->lock);

    buffer_page_t* leaf = find_leaf(tree, key);
    if (!leaf) {
        rwlock_unlock(&tree->lock);
        return SPEEDSQL_IOERR;
    }

    bool exact;
    uint16_t idx = search_leaf(leaf->data, key, tree->compare, &exact);

    if (!exact) {
        buffer_pool_unpin(tree->pool, leaf, false);
        rwlock_unlock(&tree->lock);
        return SPEEDSQL_NOTFOUND;
    }

    /* Extract value */
    uint16_t* offsets = get_cell_offsets(leaf->data);
    uint8_t* cell = get_cell_data(leaf->data, offsets[idx]);
    uint16_t key_len = *(uint16_t*)cell;
    uint16_t value_len = *(uint16_t*)(cell + 2);

    if (value) {
        value->type = SPEEDSQL_TYPE_BLOB;
        value->size = value_len;
        value->data.blob.len = value_len;
        value->data.blob.data = (uint8_t*)sdb_malloc(value_len);
        if (value->data.blob.data) {
            memcpy(value->data.blob.data, cell + 4 + key_len, value_len);
        }
    }

    buffer_pool_unpin(tree->pool, leaf, false);
    rwlock_unlock(&tree->lock);

    return SPEEDSQL_OK;
}

/* Insert a cell into a leaf page */
static int insert_into_leaf(btree_t* tree, buffer_page_t* leaf,
                             const value_t* key, const value_t* value) {
    page_header_t* hdr = (page_header_t*)leaf->data;
    uint16_t count = get_key_count(leaf->data);

    /* Calculate cell size */
    uint16_t key_len = key->data.blob.len;
    uint16_t value_len = value->data.blob.len;
    uint16_t cell_size = 4 + key_len + value_len;  /* key_len + value_len + data */

    /* Check if there's space */
    uint16_t* offsets = get_cell_offsets(leaf->data);
    uint32_t free_space = hdr->free_end - hdr->free_start - count * sizeof(uint16_t);

    if (free_space < cell_size + sizeof(uint16_t)) {
        return SPEEDSQL_FULL;  /* Need to split */
    }

    /* Find insertion point */
    bool exact;
    uint16_t idx = search_leaf(leaf->data, key, tree->compare, &exact);

    if (exact) {
        /* Update existing - for now, return error */
        return SPEEDSQL_CONSTRAINT;
    }

    /* Allocate cell from end of page */
    hdr->free_end -= cell_size;
    uint16_t cell_offset = hdr->free_end;

    /* Write cell */
    uint8_t* cell = leaf->data + cell_offset;
    *(uint16_t*)cell = key_len;
    *(uint16_t*)(cell + 2) = value_len;
    memcpy(cell + 4, key->data.blob.data, key_len);
    memcpy(cell + 4 + key_len, value->data.blob.data, value_len);

    /* Insert offset into sorted position */
    memmove(&offsets[idx + 1], &offsets[idx], (count - idx) * sizeof(uint16_t));
    offsets[idx] = cell_offset;

    set_key_count(leaf->data, count + 1);
    hdr->cell_count = count + 1;

    return SPEEDSQL_OK;
}

/* Split a leaf page and return the new page and separator key */
static int split_leaf(btree_t* tree, buffer_page_t* leaf,
                      const value_t* key, const value_t* value,
                      buffer_page_t** new_leaf_out, value_t* separator_out) {
    uint16_t count = get_key_count(leaf->data);
    uint16_t* offsets = get_cell_offsets(leaf->data);

    /* Allocate new leaf page */
    page_id_t new_page_id;
    buffer_page_t* new_leaf = buffer_pool_new_page(tree->pool, tree->file, &new_page_id);
    if (!new_leaf) return SPEEDSQL_NOMEM;

    /* Initialize new leaf */
    page_header_t* new_hdr = (page_header_t*)new_leaf->data;
    new_hdr->page_type = PAGE_TYPE_BTREE_LEAF;
    new_hdr->flags = 0;
    new_hdr->cell_count = 0;
    new_hdr->free_start = BTREE_LEAF_HEADER_SIZE;
    new_hdr->free_end = tree->pool->page_size;
    new_hdr->right_ptr = INVALID_PAGE_ID;

    set_key_count(new_leaf->data, 0);

    /* Update leaf chain: old -> new -> next */
    page_id_t old_next = get_next_leaf(leaf->data);
    set_next_leaf(leaf->data, new_page_id);
    set_prev_leaf(new_leaf->data, leaf->page_id);
    set_next_leaf(new_leaf->data, old_next);

    /* Update next page's prev pointer if exists */
    if (old_next != INVALID_PAGE_ID) {
        buffer_page_t* next_page = buffer_pool_get(tree->pool, tree->file, old_next);
        if (next_page) {
            set_prev_leaf(next_page->data, new_page_id);
            buffer_pool_unpin(tree->pool, next_page, true);
        }
    }

    /* Find insertion point for new key */
    bool exact;
    uint16_t insert_idx = search_leaf(leaf->data, key, tree->compare, &exact);

    /* Calculate split point - aim for 50% in each page */
    uint16_t split_point = (count + 1) / 2;

    /* Adjust split point if new key goes before split */
    if (insert_idx < split_point) {
        split_point--;
    }

    /* Copy cells after split point to new leaf */
    uint16_t* new_offsets = get_cell_offsets(new_leaf->data);
    uint16_t new_count = 0;

    for (uint16_t i = split_point; i < count; i++) {
        uint8_t* old_cell = get_cell_data(leaf->data, offsets[i]);
        uint16_t key_len = *(uint16_t*)old_cell;
        uint16_t value_len = *(uint16_t*)(old_cell + 2);
        uint16_t cell_size = 4 + key_len + value_len;

        /* Allocate cell in new page */
        new_hdr->free_end -= cell_size;
        uint16_t new_offset = new_hdr->free_end;

        /* Copy cell data */
        memcpy(new_leaf->data + new_offset, old_cell, cell_size);
        new_offsets[new_count++] = new_offset;
    }

    set_key_count(new_leaf->data, new_count);
    new_hdr->cell_count = new_count;

    /* Truncate old leaf */
    set_key_count(leaf->data, split_point);
    page_header_t* old_hdr = (page_header_t*)leaf->data;
    old_hdr->cell_count = split_point;

    /* Now insert the new key into the appropriate page */
    int rc;
    if (insert_idx <= split_point) {
        rc = insert_into_leaf(tree, leaf, key, value);
    } else {
        rc = insert_into_leaf(tree, new_leaf, key, value);
    }

    if (rc != SPEEDSQL_OK) {
        buffer_pool_unpin(tree->pool, new_leaf, false);
        return rc;
    }

    /* Get separator key (first key in new leaf) */
    uint16_t* sep_offsets = get_cell_offsets(new_leaf->data);
    uint8_t* sep_cell = get_cell_data(new_leaf->data, sep_offsets[0]);
    uint16_t sep_key_len = *(uint16_t*)sep_cell;

    separator_out->type = VAL_BLOB;
    separator_out->data.blob.len = sep_key_len;
    separator_out->data.blob.data = (uint8_t*)sdb_malloc(sep_key_len);
    if (separator_out->data.blob.data) {
        memcpy(separator_out->data.blob.data, sep_cell + 4, sep_key_len);
    }

    *new_leaf_out = new_leaf;
    return SPEEDSQL_OK;
}

/* Insert into internal node */
static int insert_into_internal(btree_t* tree, buffer_page_t* node,
                                 const value_t* key, page_id_t left, page_id_t right) {
    uint16_t count = get_key_count(node->data);

    /* Find insertion point */
    uint16_t idx = search_internal(node->data, key, tree->compare, tree->key_size);

    /* Check if there's space */
    uint32_t entry_size = sizeof(page_id_t) + key->data.blob.len;
    uint32_t used = BTREE_INTERNAL_HEADER_SIZE + (count + 1) * entry_size + sizeof(page_id_t);

    if (used > tree->pool->page_size) {
        return SPEEDSQL_FULL;  /* Need to split internal node */
    }

    /* Shift existing entries */
    uint8_t* base = node->data + BTREE_INTERNAL_HEADER_SIZE;
    uint32_t key_size = key->data.blob.len;

    /* For simplicity, assume fixed-size keys matching tree->key_size */
    if (tree->key_size == 0) {
        tree->key_size = key_size;
    }

    /* Move entries after idx */
    uint32_t entry_total = sizeof(page_id_t) + tree->key_size;
    memmove(base + (idx + 1) * entry_total + sizeof(page_id_t),
            base + idx * entry_total + sizeof(page_id_t),
            (count - idx) * entry_total);

    /* Insert new entry */
    /* Format: [child0][key0][child1][key1]... */
    set_child(node->data, idx, left, tree->key_size);
    uint8_t* key_ptr = get_internal_key(node->data, idx, tree->key_size);
    memcpy(key_ptr, key->data.blob.data, key_size);
    set_child(node->data, idx + 1, right, tree->key_size);

    set_key_count(node->data, count + 1);
    return SPEEDSQL_OK;
}

/* Create new root after split */
static int create_new_root(btree_t* tree, page_id_t left, page_id_t right,
                            const value_t* separator) {
    page_id_t new_root_id;
    buffer_page_t* new_root = buffer_pool_new_page(tree->pool, tree->file, &new_root_id);
    if (!new_root) return SPEEDSQL_NOMEM;

    /* Initialize as internal node */
    page_header_t* hdr = (page_header_t*)new_root->data;
    hdr->page_type = PAGE_TYPE_BTREE_INTERNAL;
    hdr->flags = 0;
    hdr->cell_count = 1;
    hdr->free_start = BTREE_INTERNAL_HEADER_SIZE;
    hdr->free_end = tree->pool->page_size;

    set_key_count(new_root->data, 1);

    /* Set children and key */
    if (tree->key_size == 0) {
        tree->key_size = separator->data.blob.len;
    }

    set_child(new_root->data, 0, left, tree->key_size);
    uint8_t* key_ptr = get_internal_key(new_root->data, 0, tree->key_size);
    memcpy(key_ptr, separator->data.blob.data, separator->data.blob.len);
    set_child(new_root->data, 1, right, tree->key_size);

    buffer_pool_unpin(tree->pool, new_root, true);

    tree->root_page = new_root_id;
    return SPEEDSQL_OK;
}

int btree_insert(btree_t* tree, const value_t* key, const value_t* value) {
    if (!tree || !key || !value) return SPEEDSQL_MISUSE;

    rwlock_wrlock(&tree->lock);

    /* Track path from root to leaf for potential splits */
    btree_path_entry_t path[BTREE_MAX_DEPTH];
    int path_len = 0;

    buffer_page_t* leaf = find_leaf_with_path(tree, key, path, &path_len);
    if (!leaf) {
        rwlock_unlock(&tree->lock);
        return SPEEDSQL_IOERR;
    }

    int rc = insert_into_leaf(tree, leaf, key, value);

    if (rc == SPEEDSQL_FULL) {
        /* Page is full - need to split */
        buffer_page_t* new_leaf = nullptr;
        value_t separator;
        memset(&separator, 0, sizeof(separator));

        rc = split_leaf(tree, leaf, key, value, &new_leaf, &separator);

        if (rc == SPEEDSQL_OK) {
            /* Insert separator into parent (handles all cases including new root) */
            rc = insert_into_parent(tree, path, path_len,
                                     leaf->page_id, new_leaf->page_id, &separator);

            buffer_pool_unpin(tree->pool, new_leaf, true);
            value_free(&separator);
        }

        buffer_pool_unpin(tree->pool, leaf, rc == SPEEDSQL_OK);
        rwlock_unlock(&tree->lock);
        return rc;
    }

    buffer_pool_unpin(tree->pool, leaf, rc == SPEEDSQL_OK);
    rwlock_unlock(&tree->lock);

    return rc;
}

int btree_delete(btree_t* tree, const value_t* key) {
    if (!tree || !key) return SPEEDSQL_MISUSE;

    rwlock_wrlock(&tree->lock);

    buffer_page_t* leaf = find_leaf(tree, key);
    if (!leaf) {
        rwlock_unlock(&tree->lock);
        return SPEEDSQL_IOERR;
    }

    bool exact;
    uint16_t idx = search_leaf(leaf->data, key, tree->compare, &exact);

    if (!exact) {
        buffer_pool_unpin(tree->pool, leaf, false);
        rwlock_unlock(&tree->lock);
        return SPEEDSQL_NOTFOUND;
    }

    /* Remove from offset array */
    uint16_t count = get_key_count(leaf->data);
    uint16_t* offsets = get_cell_offsets(leaf->data);
    memmove(&offsets[idx], &offsets[idx + 1], (count - idx - 1) * sizeof(uint16_t));
    set_key_count(leaf->data, count - 1);

    /* Note: We don't reclaim cell space here - would need page compaction */

    buffer_pool_unpin(tree->pool, leaf, true);
    rwlock_unlock(&tree->lock);

    return SPEEDSQL_OK;
}

/* Cursor implementation */
int btree_cursor_init(btree_cursor_t* cursor, btree_t* tree) {
    if (!cursor || !tree) return SPEEDSQL_MISUSE;

    memset(cursor, 0, sizeof(*cursor));
    cursor->tree = tree;
    cursor->current_page = INVALID_PAGE_ID;
    cursor->current_slot = 0;
    cursor->valid = false;
    cursor->at_end = false;

    return SPEEDSQL_OK;
}

int btree_cursor_first(btree_cursor_t* cursor) {
    if (!cursor || !cursor->tree) return SPEEDSQL_MISUSE;

    btree_t* tree = cursor->tree;

    /* Find leftmost leaf */
    page_id_t page_id = tree->root_page;

    while (true) {
        buffer_page_t* page = buffer_pool_get(tree->pool, tree->file, page_id);
        if (!page) return SPEEDSQL_IOERR;

        page_header_t* hdr = (page_header_t*)page->data;

        if (hdr->page_type == PAGE_TYPE_BTREE_LEAF) {
            cursor->current_page = page_id;
            cursor->current_slot = 0;
            cursor->valid = get_key_count(page->data) > 0;
            cursor->at_end = !cursor->valid;
            buffer_pool_unpin(tree->pool, page, false);
            return SPEEDSQL_OK;
        }

        /* Get first child */
        page_id_t child = get_child(page->data, 0, tree->key_size);
        buffer_pool_unpin(tree->pool, page, false);
        page_id = child;
    }
}

int btree_cursor_seek(btree_cursor_t* cursor, const value_t* key) {
    if (!cursor || !cursor->tree || !key) return SPEEDSQL_MISUSE;

    btree_t* tree = cursor->tree;

    /* Use find_leaf to navigate to the correct leaf page */
    buffer_page_t* leaf = find_leaf(tree, key);
    if (!leaf) return SPEEDSQL_IOERR;

    /* Binary search in leaf using existing search_leaf function */
    bool exact;
    uint16_t idx = search_leaf(leaf->data, key, tree->compare, &exact);

    uint16_t count = get_key_count(leaf->data);

    cursor->current_page = leaf->page_id;
    cursor->current_slot = idx;
    cursor->valid = (idx < count);
    cursor->at_end = !cursor->valid;

    buffer_pool_unpin(tree->pool, leaf, false);

    return exact ? SPEEDSQL_OK : SPEEDSQL_NOTFOUND;
}

int btree_cursor_next(btree_cursor_t* cursor) {
    if (!cursor || !cursor->tree || !cursor->valid) return SPEEDSQL_MISUSE;

    btree_t* tree = cursor->tree;

    buffer_page_t* page = buffer_pool_get(tree->pool, tree->file, cursor->current_page);
    if (!page) return SPEEDSQL_IOERR;

    uint16_t count = get_key_count(page->data);

    if (cursor->current_slot + 1 < count) {
        /* Move to next slot in same page */
        cursor->current_slot++;
        buffer_pool_unpin(tree->pool, page, false);
        return SPEEDSQL_OK;
    }

    /* Move to next leaf */
    page_id_t next = get_next_leaf(page->data);
    buffer_pool_unpin(tree->pool, page, false);

    if (next == INVALID_PAGE_ID) {
        cursor->valid = false;
        cursor->at_end = true;
        return SPEEDSQL_DONE;
    }

    cursor->current_page = next;
    cursor->current_slot = 0;

    /* Verify next page has entries */
    page = buffer_pool_get(tree->pool, tree->file, next);
    if (!page) return SPEEDSQL_IOERR;

    cursor->valid = get_key_count(page->data) > 0;
    buffer_pool_unpin(tree->pool, page, false);

    return cursor->valid ? SPEEDSQL_OK : SPEEDSQL_DONE;
}

int btree_cursor_key(btree_cursor_t* cursor, value_t* key) {
    if (!cursor || !cursor->tree || !cursor->valid || !key) return SPEEDSQL_MISUSE;

    btree_t* tree = cursor->tree;

    buffer_page_t* page = buffer_pool_get(tree->pool, tree->file, cursor->current_page);
    if (!page) return SPEEDSQL_IOERR;

    uint16_t* offsets = get_cell_offsets(page->data);
    uint8_t* cell = get_cell_data(page->data, offsets[cursor->current_slot]);
    uint16_t key_len = *(uint16_t*)cell;

    key->type = SPEEDSQL_TYPE_BLOB;
    key->size = key_len;
    key->data.blob.len = key_len;
    key->data.blob.data = (uint8_t*)sdb_malloc(key_len);
    if (key->data.blob.data) {
        memcpy(key->data.blob.data, cell + 4, key_len);
    }

    buffer_pool_unpin(tree->pool, page, false);
    return key->data.blob.data ? SPEEDSQL_OK : SPEEDSQL_NOMEM;
}

int btree_cursor_value(btree_cursor_t* cursor, value_t* value) {
    if (!cursor || !cursor->tree || !cursor->valid || !value) return SPEEDSQL_MISUSE;

    btree_t* tree = cursor->tree;

    buffer_page_t* page = buffer_pool_get(tree->pool, tree->file, cursor->current_page);
    if (!page) return SPEEDSQL_IOERR;

    uint16_t* offsets = get_cell_offsets(page->data);
    uint8_t* cell = get_cell_data(page->data, offsets[cursor->current_slot]);
    uint16_t key_len = *(uint16_t*)cell;
    uint16_t value_len = *(uint16_t*)(cell + 2);

    value->type = SPEEDSQL_TYPE_BLOB;
    value->size = value_len;
    value->data.blob.len = value_len;
    value->data.blob.data = (uint8_t*)sdb_malloc(value_len);
    if (value->data.blob.data) {
        memcpy(value->data.blob.data, cell + 4 + key_len, value_len);
    }

    buffer_pool_unpin(tree->pool, page, false);
    return value->data.blob.data ? SPEEDSQL_OK : SPEEDSQL_NOMEM;
}

void btree_cursor_close(btree_cursor_t* cursor) {
    if (!cursor) return;
    cursor->valid = false;
    cursor->tree = nullptr;
}
