/*
 * SpeedSQL - Value Operations
 *
 * Handles all value type operations following Single Responsibility Principle
 */

#include "speedsql_internal.h"

void value_init_null(value_t* v) {
    if (!v) return;
    memset(v, 0, sizeof(*v));
    v->type = SPEEDSQL_TYPE_NULL;
}

void value_init_int(value_t* v, int64_t i) {
    if (!v) return;
    memset(v, 0, sizeof(*v));
    v->type = SPEEDSQL_TYPE_INT;
    v->size = sizeof(int64_t);
    v->data.i = i;
}

void value_init_float(value_t* v, double f) {
    if (!v) return;
    memset(v, 0, sizeof(*v));
    v->type = SPEEDSQL_TYPE_FLOAT;
    v->size = sizeof(double);
    v->data.f = f;
}

void value_init_text(value_t* v, const char* s, int len) {
    if (!v) return;
    memset(v, 0, sizeof(*v));
    v->type = SPEEDSQL_TYPE_TEXT;

    if (s) {
        if (len < 0) len = (int)strlen(s);
        v->data.text.data = (char*)sdb_malloc(len + 1);
        if (v->data.text.data) {
            memcpy(v->data.text.data, s, len);
            v->data.text.data[len] = '\0';
            v->data.text.len = len;
            v->size = len;
        }
    }
}

void value_init_blob(value_t* v, const void* data, int len) {
    if (!v) return;
    memset(v, 0, sizeof(*v));
    v->type = SPEEDSQL_TYPE_BLOB;

    if (data && len > 0) {
        v->data.blob.data = (uint8_t*)sdb_malloc(len);
        if (v->data.blob.data) {
            memcpy(v->data.blob.data, data, len);
            v->data.blob.len = len;
            v->size = len;
        }
    }
}

void value_copy(value_t* dst, const value_t* src) {
    if (!dst || !src) return;

    memset(dst, 0, sizeof(*dst));
    dst->type = src->type;
    dst->size = src->size;

    switch (src->type) {
        case SPEEDSQL_TYPE_NULL:
            break;

        case SPEEDSQL_TYPE_INT:
            dst->data.i = src->data.i;
            break;

        case SPEEDSQL_TYPE_FLOAT:
            dst->data.f = src->data.f;
            break;

        case SPEEDSQL_TYPE_TEXT:
        case SPEEDSQL_TYPE_JSON:
            if (src->data.text.data && src->data.text.len > 0) {
                dst->data.text.data = (char*)sdb_malloc(src->data.text.len + 1);
                if (dst->data.text.data) {
                    memcpy(dst->data.text.data, src->data.text.data, src->data.text.len);
                    dst->data.text.data[src->data.text.len] = '\0';
                    dst->data.text.len = src->data.text.len;
                }
            }
            break;

        case SPEEDSQL_TYPE_BLOB:
            if (src->data.blob.data && src->data.blob.len > 0) {
                dst->data.blob.data = (uint8_t*)sdb_malloc(src->data.blob.len);
                if (dst->data.blob.data) {
                    memcpy(dst->data.blob.data, src->data.blob.data, src->data.blob.len);
                    dst->data.blob.len = src->data.blob.len;
                }
            }
            break;

        case SPEEDSQL_TYPE_VECTOR:
            if (src->data.vec.data && src->data.vec.dimensions > 0) {
                size_t size = src->data.vec.dimensions * sizeof(float);
                dst->data.vec.data = (float*)sdb_malloc(size);
                if (dst->data.vec.data) {
                    memcpy(dst->data.vec.data, src->data.vec.data, size);
                    dst->data.vec.dimensions = src->data.vec.dimensions;
                }
            }
            break;
    }
}

void value_free(value_t* v) {
    if (!v) return;

    switch (v->type) {
        case SPEEDSQL_TYPE_TEXT:
        case SPEEDSQL_TYPE_JSON:
            sdb_free(v->data.text.data);
            break;

        case SPEEDSQL_TYPE_BLOB:
            sdb_free(v->data.blob.data);
            break;

        case SPEEDSQL_TYPE_VECTOR:
            sdb_free(v->data.vec.data);
            break;

        default:
            break;
    }

    memset(v, 0, sizeof(*v));
}

/* Compare two values - returns <0, 0, or >0 */
int value_compare(const value_t* a, const value_t* b) {
    if (!a && !b) return 0;
    if (!a) return -1;
    if (!b) return 1;

    /* NULL handling: NULL is less than any value */
    if (a->type == SPEEDSQL_TYPE_NULL && b->type == SPEEDSQL_TYPE_NULL) return 0;
    if (a->type == SPEEDSQL_TYPE_NULL) return -1;
    if (b->type == SPEEDSQL_TYPE_NULL) return 1;

    /* Same type comparison */
    if (a->type == b->type) {
        switch (a->type) {
            case SPEEDSQL_TYPE_INT:
                if (a->data.i < b->data.i) return -1;
                if (a->data.i > b->data.i) return 1;
                return 0;

            case SPEEDSQL_TYPE_FLOAT:
                if (a->data.f < b->data.f) return -1;
                if (a->data.f > b->data.f) return 1;
                return 0;

            case SPEEDSQL_TYPE_TEXT:
            case SPEEDSQL_TYPE_JSON: {
                uint32_t len = a->data.text.len < b->data.text.len ?
                          a->data.text.len : b->data.text.len;
                int cmp = memcmp(a->data.text.data, b->data.text.data, len);
                if (cmp != 0) return cmp;
                if (a->data.text.len < b->data.text.len) return -1;
                if (a->data.text.len > b->data.text.len) return 1;
                return 0;
            }

            case SPEEDSQL_TYPE_BLOB: {
                uint32_t len = a->data.blob.len < b->data.blob.len ?
                          a->data.blob.len : b->data.blob.len;
                int cmp = memcmp(a->data.blob.data, b->data.blob.data, len);
                if (cmp != 0) return cmp;
                if (a->data.blob.len < b->data.blob.len) return -1;
                if (a->data.blob.len > b->data.blob.len) return 1;
                return 0;
            }

            default:
                return 0;
        }
    }

    /* Cross-type comparison: numeric types can be compared */
    if ((a->type == SPEEDSQL_TYPE_INT || a->type == SPEEDSQL_TYPE_FLOAT) &&
        (b->type == SPEEDSQL_TYPE_INT || b->type == SPEEDSQL_TYPE_FLOAT)) {

        double da = (a->type == SPEEDSQL_TYPE_INT) ? (double)a->data.i : a->data.f;
        double db = (b->type == SPEEDSQL_TYPE_INT) ? (double)b->data.i : b->data.f;

        if (da < db) return -1;
        if (da > db) return 1;
        return 0;
    }

    /* Different incompatible types - compare by type */
    return (int)a->type - (int)b->type;
}

uint64_t value_hash(const value_t* v) {
    if (!v) return 0;

    switch (v->type) {
        case SPEEDSQL_TYPE_NULL:
            return 0;

        case SPEEDSQL_TYPE_INT:
            return xxhash64(&v->data.i, sizeof(v->data.i));

        case SPEEDSQL_TYPE_FLOAT:
            return xxhash64(&v->data.f, sizeof(v->data.f));

        case SPEEDSQL_TYPE_TEXT:
        case SPEEDSQL_TYPE_JSON:
            if (v->data.text.data) {
                return xxhash64(v->data.text.data, v->data.text.len);
            }
            return 0;

        case SPEEDSQL_TYPE_BLOB:
            if (v->data.blob.data) {
                return xxhash64(v->data.blob.data, v->data.blob.len);
            }
            return 0;

        case SPEEDSQL_TYPE_VECTOR:
            if (v->data.vec.data) {
                return xxhash64(v->data.vec.data,
                               v->data.vec.dimensions * sizeof(float));
            }
            return 0;

        default:
            return 0;
    }
}
