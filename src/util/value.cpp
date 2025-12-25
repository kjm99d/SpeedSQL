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
    v->data.i64 = i;
}

void value_init_float(value_t* v, double f) {
    if (!v) return;
    memset(v, 0, sizeof(*v));
    v->type = SPEEDSQL_TYPE_FLOAT;
    v->size = sizeof(double);
    v->data.f64 = f;
}

void value_init_text(value_t* v, const char* s, int len) {
    if (!v) return;
    memset(v, 0, sizeof(*v));
    v->type = SPEEDSQL_TYPE_TEXT;

    if (s) {
        if (len < 0) len = (int)strlen(s);
        v->data.str.data = (char*)sdb_malloc(len + 1);
        if (v->data.str.data) {
            memcpy(v->data.str.data, s, len);
            v->data.str.data[len] = '\0';
            v->data.str.len = len;
            v->size = len;
        }
    }
}

void value_init_blob(value_t* v, const void* data, int len) {
    if (!v) return;
    memset(v, 0, sizeof(*v));
    v->type = SPEEDSQL_TYPE_BLOB;

    if (data && len > 0) {
        v->data.str.data = (char*)sdb_malloc(len);
        if (v->data.str.data) {
            memcpy(v->data.str.data, data, len);
            v->data.str.len = len;
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
            dst->data.i64 = src->data.i64;
            break;

        case SPEEDSQL_TYPE_FLOAT:
            dst->data.f64 = src->data.f64;
            break;

        case SPEEDSQL_TYPE_TEXT:
        case SPEEDSQL_TYPE_BLOB:
        case SPEEDSQL_TYPE_JSON:
            if (src->data.str.data && src->data.str.len > 0) {
                dst->data.str.data = (char*)sdb_malloc(src->data.str.len + 1);
                if (dst->data.str.data) {
                    memcpy(dst->data.str.data, src->data.str.data, src->data.str.len);
                    dst->data.str.data[src->data.str.len] = '\0';
                    dst->data.str.len = src->data.str.len;
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
        case SPEEDSQL_TYPE_BLOB:
        case SPEEDSQL_TYPE_JSON:
            sdb_free(v->data.str.data);
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
                if (a->data.i64 < b->data.i64) return -1;
                if (a->data.i64 > b->data.i64) return 1;
                return 0;

            case SPEEDSQL_TYPE_FLOAT:
                if (a->data.f64 < b->data.f64) return -1;
                if (a->data.f64 > b->data.f64) return 1;
                return 0;

            case SPEEDSQL_TYPE_TEXT:
            case SPEEDSQL_TYPE_JSON: {
                int len = a->data.str.len < b->data.str.len ?
                          a->data.str.len : b->data.str.len;
                int cmp = memcmp(a->data.str.data, b->data.str.data, len);
                if (cmp != 0) return cmp;
                if (a->data.str.len < b->data.str.len) return -1;
                if (a->data.str.len > b->data.str.len) return 1;
                return 0;
            }

            case SPEEDSQL_TYPE_BLOB: {
                int len = a->data.str.len < b->data.str.len ?
                          a->data.str.len : b->data.str.len;
                int cmp = memcmp(a->data.str.data, b->data.str.data, len);
                if (cmp != 0) return cmp;
                if (a->data.str.len < b->data.str.len) return -1;
                if (a->data.str.len > b->data.str.len) return 1;
                return 0;
            }

            default:
                return 0;
        }
    }

    /* Cross-type comparison: numeric types can be compared */
    if ((a->type == SPEEDSQL_TYPE_INT || a->type == SPEEDSQL_TYPE_FLOAT) &&
        (b->type == SPEEDSQL_TYPE_INT || b->type == SPEEDSQL_TYPE_FLOAT)) {

        double da = (a->type == SPEEDSQL_TYPE_INT) ? (double)a->data.i64 : a->data.f64;
        double db = (b->type == SPEEDSQL_TYPE_INT) ? (double)b->data.i64 : b->data.f64;

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
            return xxhash64(&v->data.i64, sizeof(v->data.i64));

        case SPEEDSQL_TYPE_FLOAT:
            return xxhash64(&v->data.f64, sizeof(v->data.f64));

        case SPEEDSQL_TYPE_TEXT:
        case SPEEDSQL_TYPE_BLOB:
        case SPEEDSQL_TYPE_JSON:
            if (v->data.str.data) {
                return xxhash64(v->data.str.data, v->data.str.len);
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
