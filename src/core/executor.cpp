/*
 * SpeedSQL - Query Executor
 *
 * Executes parsed SQL statements and manages prepared statements
 */

#include "speedsql_internal.h"
#include <stdarg.h>

#ifdef _WIN32
#define strcasecmp _stricmp
#endif

/* ============================================================================
 * Parameter Counting (for prepared statements)
 * ============================================================================ */

static int count_params_in_expr(expr_t* expr);

static int count_params_in_expr(expr_t* expr) {
    if (!expr) return 0;

    switch (expr->type) {
        case EXPR_PARAMETER:
            return 1;

        case EXPR_LITERAL:
        case EXPR_COLUMN:
            return 0;

        case EXPR_BINARY_OP:
            return count_params_in_expr(expr->data.binary.left) +
                   count_params_in_expr(expr->data.binary.right);

        case EXPR_UNARY_OP:
            return count_params_in_expr(expr->data.unary.operand);

        case EXPR_FUNCTION:
            {
                int count = 0;
                for (int i = 0; i < expr->data.function.arg_count; i++) {
                    count += count_params_in_expr(expr->data.function.args[i]);
                }
                return count;
            }

        case EXPR_SUBQUERY:
            return 0;  /* Subquery params counted separately */

        default:
            return 0;
    }
}

static int count_params_in_stmt(parsed_stmt_t* stmt) {
    if (!stmt) return 0;

    int count = 0;

    /* WHERE clause */
    count += count_params_in_expr(stmt->where);

    /* SELECT columns */
    for (int i = 0; i < stmt->column_count; i++) {
        if (stmt->columns && stmt->columns[i].expr) {
            count += count_params_in_expr(stmt->columns[i].expr);
        }
    }

    /* UPDATE SET expressions */
    for (int i = 0; i < stmt->update_count; i++) {
        if (stmt->update_exprs && stmt->update_exprs[i]) {
            count += count_params_in_expr(stmt->update_exprs[i]);
        }
    }

    /* GROUP BY */
    for (int i = 0; i < stmt->group_by_count; i++) {
        if (stmt->group_by && stmt->group_by[i]) {
            count += count_params_in_expr(stmt->group_by[i]);
        }
    }

    /* HAVING */
    count += count_params_in_expr(stmt->having);

    /* ORDER BY */
    for (int i = 0; i < stmt->order_by_count; i++) {
        if (stmt->order_by && stmt->order_by[i].expr) {
            count += count_params_in_expr(stmt->order_by[i].expr);
        }
    }

    return count;
}

/* ============================================================================
 * Plan Management
 * ============================================================================ */

void plan_free(plan_node_t* plan) {
    if (!plan) return;

    /* Close cursor for scan operations */
    switch (plan->type) {
        case PLAN_SCAN:
            btree_cursor_close(&plan->data.scan.cursor);
            break;
        case PLAN_INDEX_SCAN:
            btree_cursor_close(&plan->data.index_scan.cursor);
            break;
        default:
            break;
    }

    /* Free child nodes recursively */
    if (plan->child) {
        plan_free(plan->child);
    }
    if (plan->right) {
        plan_free(plan->right);
    }

    sdb_free(plan);
}

/* ============================================================================
 * Statement Management
 * ============================================================================ */

static speedsql_stmt* stmt_alloc(speedsql* db) {
    speedsql_stmt* stmt = (speedsql_stmt*)sdb_calloc(1, sizeof(speedsql_stmt));
    if (!stmt) return nullptr;

    stmt->db = db;
    stmt->executed = false;
    stmt->has_row = false;
    stmt->step_count = 0;

    return stmt;
}

static void stmt_free_internal(speedsql_stmt* stmt) {
    if (!stmt) return;

    /* Free SQL string */
    if (stmt->sql) {
        sdb_free(stmt->sql);
    }

    /* Free parsed statement */
    if (stmt->parsed) {
        parsed_stmt_free(stmt->parsed);
    }

    /* Free execution plan */
    if (stmt->plan) {
        plan_free(stmt->plan);
    }

    /* Free parameters */
    if (stmt->params) {
        for (int i = 0; i < stmt->param_count; i++) {
            value_free(&stmt->params[i]);
        }
        sdb_free(stmt->params);
    }

    /* Free current row */
    if (stmt->current_row) {
        for (int i = 0; i < stmt->column_count; i++) {
            value_free(&stmt->current_row[i]);
        }
        sdb_free(stmt->current_row);
    }

    /* Free column names */
    if (stmt->column_names) {
        for (int i = 0; i < stmt->column_count; i++) {
            if (stmt->column_names[i]) {
                sdb_free(stmt->column_names[i]);
            }
        }
        sdb_free(stmt->column_names);
    }

    sdb_free(stmt);
}

/* ============================================================================
 * Expression Evaluation
 * ============================================================================ */

static int eval_expr(speedsql_stmt* stmt, expr_t* expr, value_t* result) {
    if (!expr || !result) return SPEEDSQL_MISUSE;

    switch (expr->type) {
        case EXPR_LITERAL:
            value_copy(result, &expr->data.literal);
            return SPEEDSQL_OK;

        case EXPR_PARAMETER: {
            int idx = expr->data.param_index;
            if (idx < 1 || idx > stmt->param_count) {
                return SPEEDSQL_RANGE;
            }
            value_copy(result, &stmt->params[idx - 1]);
            return SPEEDSQL_OK;
        }

        case EXPR_COLUMN: {
            int col_idx = expr->data.column_ref.index;
            if (col_idx < 0 || col_idx >= stmt->column_count) {
                return SPEEDSQL_RANGE;
            }
            if (stmt->current_row) {
                value_copy(result, &stmt->current_row[col_idx]);
            } else {
                value_init_null(result);
            }
            return SPEEDSQL_OK;
        }

        case EXPR_BINARY_OP: {
            value_t left, right;
            value_init_null(&left);
            value_init_null(&right);

            int rc = eval_expr(stmt, expr->data.binary.left, &left);
            if (rc != SPEEDSQL_OK) return rc;

            rc = eval_expr(stmt, expr->data.binary.right, &right);
            if (rc != SPEEDSQL_OK) {
                value_free(&left);
                return rc;
            }

            /* Handle NULL propagation */
            if (left.type == VAL_NULL || right.type == VAL_NULL) {
                /* Most operations with NULL return NULL */
                if (expr->data.binary.op != TOK_IS) {
                    value_init_null(result);
                    value_free(&left);
                    value_free(&right);
                    return SPEEDSQL_OK;
                }
            }

            switch (expr->data.binary.op) {
                case TOK_PLUS:
                    if (left.type == VAL_INT && right.type == VAL_INT) {
                        value_init_int(result, left.data.i + right.data.i);
                    } else {
                        double l = (left.type == VAL_INT) ? (double)left.data.i : left.data.f;
                        double r = (right.type == VAL_INT) ? (double)right.data.i : right.data.f;
                        value_init_float(result, l + r);
                    }
                    break;

                case TOK_MINUS:
                    if (left.type == VAL_INT && right.type == VAL_INT) {
                        value_init_int(result, left.data.i - right.data.i);
                    } else {
                        double l = (left.type == VAL_INT) ? (double)left.data.i : left.data.f;
                        double r = (right.type == VAL_INT) ? (double)right.data.i : right.data.f;
                        value_init_float(result, l - r);
                    }
                    break;

                case TOK_STAR:
                    if (left.type == VAL_INT && right.type == VAL_INT) {
                        value_init_int(result, left.data.i * right.data.i);
                    } else {
                        double l = (left.type == VAL_INT) ? (double)left.data.i : left.data.f;
                        double r = (right.type == VAL_INT) ? (double)right.data.i : right.data.f;
                        value_init_float(result, l * r);
                    }
                    break;

                case TOK_SLASH:
                    if (right.type == VAL_INT && right.data.i == 0) {
                        value_init_null(result);  /* Division by zero */
                    } else if (right.type == VAL_FLOAT && right.data.f == 0.0) {
                        value_init_null(result);
                    } else {
                        double l = (left.type == VAL_INT) ? (double)left.data.i : left.data.f;
                        double r = (right.type == VAL_INT) ? (double)right.data.i : right.data.f;
                        value_init_float(result, l / r);
                    }
                    break;

                case TOK_EQ:
                    value_init_int(result, value_compare(&left, &right) == 0 ? 1 : 0);
                    break;

                case TOK_NE:
                    value_init_int(result, value_compare(&left, &right) != 0 ? 1 : 0);
                    break;

                case TOK_LT:
                    value_init_int(result, value_compare(&left, &right) < 0 ? 1 : 0);
                    break;

                case TOK_LE:
                    value_init_int(result, value_compare(&left, &right) <= 0 ? 1 : 0);
                    break;

                case TOK_GT:
                    value_init_int(result, value_compare(&left, &right) > 0 ? 1 : 0);
                    break;

                case TOK_GE:
                    value_init_int(result, value_compare(&left, &right) >= 0 ? 1 : 0);
                    break;

                case TOK_AND:
                    value_init_int(result,
                        (left.data.i != 0 && right.data.i != 0) ? 1 : 0);
                    break;

                case TOK_OR:
                    value_init_int(result,
                        (left.data.i != 0 || right.data.i != 0) ? 1 : 0);
                    break;

                default:
                    value_init_null(result);
                    break;
            }

            value_free(&left);
            value_free(&right);
            return SPEEDSQL_OK;
        }

        case EXPR_UNARY_OP: {
            value_t operand;
            value_init_null(&operand);

            int rc = eval_expr(stmt, expr->data.unary.operand, &operand);
            if (rc != SPEEDSQL_OK) return rc;

            switch (expr->data.unary.op) {
                case TOK_MINUS:
                    if (operand.type == VAL_INT) {
                        value_init_int(result, -operand.data.i);
                    } else if (operand.type == VAL_FLOAT) {
                        value_init_float(result, -operand.data.f);
                    } else {
                        value_init_null(result);
                    }
                    break;

                case TOK_NOT:
                    if (operand.type == VAL_NULL) {
                        value_init_null(result);
                    } else {
                        value_init_int(result, operand.data.i == 0 ? 1 : 0);
                    }
                    break;

                default:
                    value_init_null(result);
                    break;
            }

            value_free(&operand);
            return SPEEDSQL_OK;
        }

        default:
            value_init_null(result);
            return SPEEDSQL_OK;
    }
}

/* ============================================================================
 * Schema Management
 * ============================================================================ */

static table_def_t* find_table(speedsql* db, const char* name) {
    if (!db || !name) return nullptr;

    for (size_t i = 0; i < db->table_count; i++) {
        if (strcmp(db->tables[i].name, name) == 0) {
            return &db->tables[i];
        }
    }
    return nullptr;
}

static int create_table(speedsql* db, table_def_t* def) {
    if (!db || !def) return SPEEDSQL_MISUSE;

    /* Check if table already exists */
    if (find_table(db, def->name)) {
        sdb_set_error(db, SPEEDSQL_ERROR, "Table '%s' already exists", def->name);
        return SPEEDSQL_ERROR;
    }

    /* Expand tables array */
    table_def_t* new_tables = (table_def_t*)sdb_realloc(
        db->tables,
        (db->table_count + 1) * sizeof(table_def_t)
    );
    if (!new_tables) return SPEEDSQL_NOMEM;

    db->tables = new_tables;

    /* Copy table definition */
    table_def_t* tbl = &db->tables[db->table_count];
    memset(tbl, 0, sizeof(*tbl));

    tbl->name = sdb_strdup(def->name);
    if (!tbl->name) return SPEEDSQL_NOMEM;

    tbl->column_count = def->column_count;
    tbl->columns = (column_def_t*)sdb_calloc(def->column_count, sizeof(column_def_t));
    if (!tbl->columns) {
        sdb_free(tbl->name);
        return SPEEDSQL_NOMEM;
    }

    for (uint32_t i = 0; i < def->column_count; i++) {
        tbl->columns[i].name = sdb_strdup(def->columns[i].name);
        tbl->columns[i].type = def->columns[i].type;
        tbl->columns[i].flags = def->columns[i].flags;
        tbl->columns[i].default_value = def->columns[i].default_value ?
            sdb_strdup(def->columns[i].default_value) : nullptr;
        tbl->columns[i].collation = def->columns[i].collation ?
            sdb_strdup(def->columns[i].collation) : nullptr;
    }

    /* Create B+Tree for table data */
    tbl->data_tree = (struct btree*)sdb_malloc(sizeof(btree_t));
    if (!tbl->data_tree) return SPEEDSQL_NOMEM;

    int rc = btree_create((btree_t*)tbl->data_tree, db->buffer_pool, &db->db_file, value_compare);
    if (rc != SPEEDSQL_OK) {
        sdb_free(tbl->data_tree);
        tbl->data_tree = nullptr;
        return rc;
    }

    tbl->root_page = ((btree_t*)tbl->data_tree)->root_page;
    db->table_count++;

    return SPEEDSQL_OK;
}

/* ============================================================================
 * Executor: CREATE TABLE
 * ============================================================================ */

static int execute_create_table(speedsql_stmt* stmt) {
    if (!stmt->parsed || !stmt->parsed->new_table) {
        return SPEEDSQL_MISUSE;
    }

    return create_table(stmt->db, stmt->parsed->new_table);
}

/* ============================================================================
 * Executor: DROP TABLE
 * ============================================================================ */

static int execute_drop_table(speedsql_stmt* stmt) {
    parsed_stmt_t* p = stmt->parsed;
    if (!p || p->table_count == 0) return SPEEDSQL_MISUSE;

    const char* table_name = p->tables[0].name;
    speedsql* db = stmt->db;

    /* Find table index */
    size_t table_idx = SIZE_MAX;
    for (size_t i = 0; i < db->table_count; i++) {
        if (strcmp(db->tables[i].name, table_name) == 0) {
            table_idx = i;
            break;
        }
    }

    if (table_idx == SIZE_MAX) {
        sdb_set_error(db, SPEEDSQL_ERROR, "Table '%s' not found", table_name);
        return SPEEDSQL_ERROR;
    }

    /* Free table resources */
    table_def_t* tbl = &db->tables[table_idx];
    sdb_free(tbl->name);
    for (uint32_t j = 0; j < tbl->column_count; j++) {
        sdb_free(tbl->columns[j].name);
        sdb_free(tbl->columns[j].default_value);
        sdb_free(tbl->columns[j].collation);
    }
    sdb_free(tbl->columns);
    if (tbl->data_tree) {
        btree_close((btree_t*)tbl->data_tree);
        sdb_free(tbl->data_tree);
    }

    /* Remove from array by shifting */
    for (size_t i = table_idx; i < db->table_count - 1; i++) {
        db->tables[i] = db->tables[i + 1];
    }
    db->table_count--;

    return SPEEDSQL_OK;
}

/* ============================================================================
 * Executor: CREATE INDEX
 * ============================================================================ */

static int execute_create_index(speedsql_stmt* stmt) {
    parsed_stmt_t* p = stmt->parsed;
    if (!p || !p->new_index) return SPEEDSQL_MISUSE;

    speedsql* db = stmt->db;
    index_def_t* def = p->new_index;

    /* Find table */
    table_def_t* table = find_table(db, def->table_name);
    if (!table) {
        sdb_set_error(db, SPEEDSQL_ERROR, "Table '%s' not found", def->table_name);
        return SPEEDSQL_ERROR;
    }

    /* Expand indices array */
    index_def_t* new_indices = (index_def_t*)sdb_realloc(
        db->indices, (db->index_count + 1) * sizeof(index_def_t));
    if (!new_indices) return SPEEDSQL_NOMEM;

    db->indices = new_indices;

    /* Copy index definition */
    index_def_t* idx = &db->indices[db->index_count];
    memset(idx, 0, sizeof(*idx));

    idx->name = sdb_strdup(def->name);
    idx->table_name = sdb_strdup(def->table_name);
    idx->column_count = def->column_count;
    idx->flags = def->flags;

    idx->column_indices = (uint32_t*)sdb_malloc(def->column_count * sizeof(uint32_t));
    if (idx->column_indices) {
        memcpy(idx->column_indices, def->column_indices,
               def->column_count * sizeof(uint32_t));
    }

    /* TODO: Create B+Tree for index and populate from table data */
    idx->root_page = INVALID_PAGE_ID;

    db->index_count++;
    return SPEEDSQL_OK;
}

/* ============================================================================
 * Executor: DROP INDEX
 * ============================================================================ */

static int execute_drop_index(speedsql_stmt* stmt) {
    parsed_stmt_t* p = stmt->parsed;
    if (!p || !p->new_index) return SPEEDSQL_MISUSE;

    speedsql* db = stmt->db;
    const char* index_name = p->new_index->name;

    /* Find index */
    size_t idx = SIZE_MAX;
    for (size_t i = 0; i < db->index_count; i++) {
        if (strcmp(db->indices[i].name, index_name) == 0) {
            idx = i;
            break;
        }
    }

    if (idx == SIZE_MAX) {
        sdb_set_error(db, SPEEDSQL_ERROR, "Index '%s' not found", index_name);
        return SPEEDSQL_ERROR;
    }

    /* Free index resources */
    sdb_free(db->indices[idx].name);
    sdb_free(db->indices[idx].table_name);
    sdb_free(db->indices[idx].column_indices);

    /* Remove from array */
    for (size_t i = idx; i < db->index_count - 1; i++) {
        db->indices[i] = db->indices[i + 1];
    }
    db->index_count--;

    return SPEEDSQL_OK;
}

/* ============================================================================
 * Executor: UPDATE
 * ============================================================================ */

static int execute_update(speedsql_stmt* stmt) {
    parsed_stmt_t* p = stmt->parsed;
    if (!p || p->table_count == 0) return SPEEDSQL_MISUSE;

    table_def_t* table = find_table(stmt->db, p->tables[0].name);
    if (!table) {
        sdb_set_error(stmt->db, SPEEDSQL_ERROR, "Table '%s' not found", p->tables[0].name);
        return SPEEDSQL_ERROR;
    }

    if (!table->data_tree) {
        sdb_set_error(stmt->db, SPEEDSQL_ERROR, "Table has no data tree");
        return SPEEDSQL_ERROR;
    }

    btree_t* tree = (btree_t*)table->data_tree;
    btree_cursor_t cursor;
    btree_cursor_init(&cursor, tree);
    btree_cursor_first(&cursor);

    int updated_count = 0;

    /* Collect keys to update (can't modify while iterating) */
    value_t* keys_to_update = nullptr;
    value_t** new_rows = nullptr;
    int update_capacity = 64;
    int update_count = 0;

    keys_to_update = (value_t*)sdb_malloc(update_capacity * sizeof(value_t));
    new_rows = (value_t**)sdb_malloc(update_capacity * sizeof(value_t*));

    while (cursor.valid && !cursor.at_end) {
        value_t key, value;
        value_init_null(&key);
        value_init_null(&value);

        btree_cursor_key(&cursor, &key);
        btree_cursor_value(&cursor, &value);

        if (value.type == VAL_BLOB && value.data.blob.data) {
            int col_count = *(int*)value.data.blob.data;
            value_t* row_vals = (value_t*)((uint8_t*)value.data.blob.data + sizeof(int));

            /* Check WHERE condition */
            bool pass_filter = true;
            if (p->where) {
                stmt->current_row = row_vals;
                stmt->column_count = col_count;

                value_t filter_result;
                value_init_null(&filter_result);
                eval_expr(stmt, p->where, &filter_result);

                pass_filter = (filter_result.type != VAL_NULL && filter_result.data.i != 0);
                value_free(&filter_result);
            }

            if (pass_filter) {
                /* Expand arrays if needed */
                if (update_count >= update_capacity) {
                    update_capacity *= 2;
                    keys_to_update = (value_t*)sdb_realloc(keys_to_update,
                        update_capacity * sizeof(value_t));
                    new_rows = (value_t**)sdb_realloc(new_rows,
                        update_capacity * sizeof(value_t*));
                }

                /* Store key for later update */
                value_copy(&keys_to_update[update_count], &key);

                /* Create new row with updated values */
                value_t* new_row = (value_t*)sdb_calloc(col_count, sizeof(value_t));
                for (int i = 0; i < col_count; i++) {
                    value_copy(&new_row[i], &row_vals[i]);
                }

                /* Apply SET expressions */
                for (int u = 0; u < p->update_count; u++) {
                    /* Find column index by name */
                    int col_idx = -1;
                    for (uint32_t c = 0; c < table->column_count; c++) {
                        if (strcmp(table->columns[c].name, p->update_columns[u]) == 0) {
                            col_idx = c;
                            break;
                        }
                    }

                    if (col_idx >= 0 && col_idx < col_count) {
                        stmt->current_row = new_row;
                        stmt->column_count = col_count;

                        value_t new_val;
                        value_init_null(&new_val);
                        eval_expr(stmt, p->update_exprs[u], &new_val);

                        value_free(&new_row[col_idx]);
                        value_copy(&new_row[col_idx], &new_val);
                        value_free(&new_val);
                    }
                }

                new_rows[update_count] = new_row;
                update_count++;
            }
        }

        value_free(&key);
        value_free(&value);
        btree_cursor_next(&cursor);
    }

    btree_cursor_close(&cursor);

    /* Now apply updates */
    for (int i = 0; i < update_count; i++) {
        /* Delete old entry */
        btree_delete(tree, &keys_to_update[i]);

        /* Insert new entry with same key */
        size_t row_size = sizeof(int) + table->column_count * sizeof(value_t);
        uint8_t* row_data = (uint8_t*)sdb_malloc(row_size);
        *(int*)row_data = table->column_count;
        memcpy(row_data + sizeof(int), new_rows[i], table->column_count * sizeof(value_t));

        value_t new_value;
        value_init_blob(&new_value, row_data, row_size);
        btree_insert(tree, &keys_to_update[i], &new_value);

        sdb_free(row_data);
        value_free(&new_value);
        value_free(&keys_to_update[i]);
        sdb_free(new_rows[i]);

        updated_count++;
    }

    sdb_free(keys_to_update);
    sdb_free(new_rows);

    stmt->db->total_changes += updated_count;
    stmt->current_row = nullptr;
    stmt->column_count = 0;

    return SPEEDSQL_DONE;
}

/* ============================================================================
 * Executor: DELETE
 * ============================================================================ */

static int execute_delete(speedsql_stmt* stmt) {
    parsed_stmt_t* p = stmt->parsed;
    if (!p || p->table_count == 0) return SPEEDSQL_MISUSE;

    table_def_t* table = find_table(stmt->db, p->tables[0].name);
    if (!table) {
        sdb_set_error(stmt->db, SPEEDSQL_ERROR, "Table '%s' not found", p->tables[0].name);
        return SPEEDSQL_ERROR;
    }

    if (!table->data_tree) {
        sdb_set_error(stmt->db, SPEEDSQL_ERROR, "Table has no data tree");
        return SPEEDSQL_ERROR;
    }

    btree_t* tree = (btree_t*)table->data_tree;
    btree_cursor_t cursor;
    btree_cursor_init(&cursor, tree);
    btree_cursor_first(&cursor);

    /* Collect keys to delete (can't modify while iterating) */
    value_t* keys_to_delete = nullptr;
    int delete_capacity = 64;
    int delete_count = 0;

    keys_to_delete = (value_t*)sdb_malloc(delete_capacity * sizeof(value_t));

    while (cursor.valid && !cursor.at_end) {
        value_t key, value;
        value_init_null(&key);
        value_init_null(&value);

        btree_cursor_key(&cursor, &key);
        btree_cursor_value(&cursor, &value);

        if (value.type == VAL_BLOB && value.data.blob.data) {
            int col_count = *(int*)value.data.blob.data;
            value_t* row_vals = (value_t*)((uint8_t*)value.data.blob.data + sizeof(int));

            /* Check WHERE condition */
            bool pass_filter = true;
            if (p->where) {
                stmt->current_row = row_vals;
                stmt->column_count = col_count;

                value_t filter_result;
                value_init_null(&filter_result);
                eval_expr(stmt, p->where, &filter_result);

                pass_filter = (filter_result.type != VAL_NULL && filter_result.data.i != 0);
                value_free(&filter_result);
            }

            if (pass_filter) {
                /* Expand array if needed */
                if (delete_count >= delete_capacity) {
                    delete_capacity *= 2;
                    keys_to_delete = (value_t*)sdb_realloc(keys_to_delete,
                        delete_capacity * sizeof(value_t));
                }

                value_copy(&keys_to_delete[delete_count], &key);
                delete_count++;
            }
        }

        value_free(&key);
        value_free(&value);
        btree_cursor_next(&cursor);
    }

    btree_cursor_close(&cursor);

    /* Now delete the collected keys */
    for (int i = 0; i < delete_count; i++) {
        btree_delete(tree, &keys_to_delete[i]);
        value_free(&keys_to_delete[i]);
    }

    sdb_free(keys_to_delete);

    stmt->db->total_changes += delete_count;
    stmt->current_row = nullptr;
    stmt->column_count = 0;

    return SPEEDSQL_DONE;
}

/* ============================================================================
 * Executor: INSERT
 * ============================================================================ */

static int execute_insert(speedsql_stmt* stmt) {
    parsed_stmt_t* p = stmt->parsed;
    if (!p || p->table_count == 0) return SPEEDSQL_MISUSE;

    table_def_t* table = find_table(stmt->db, p->tables[0].name);
    if (!table) {
        sdb_set_error(stmt->db, SPEEDSQL_ERROR, "Table '%s' not found", p->tables[0].name);
        return SPEEDSQL_ERROR;
    }

    if (!table->data_tree) {
        sdb_set_error(stmt->db, SPEEDSQL_ERROR, "Table has no data tree");
        return SPEEDSQL_ERROR;
    }

    /* For each row of values */
    for (int row = 0; row < p->insert_row_count; row++) {
        /* Build row key (use rowid) */
        value_t key;
        int64_t rowid = ++stmt->db->last_rowid;
        value_init_int(&key, rowid);

        /* Build row value (pack all columns) */
        /* For simplicity, store as a blob with packed values */
        size_t row_size = sizeof(int) + table->column_count * sizeof(value_t);
        uint8_t* row_data = (uint8_t*)sdb_malloc(row_size);
        if (!row_data) {
            value_free(&key);
            return SPEEDSQL_NOMEM;
        }

        *(int*)row_data = table->column_count;
        value_t* row_values = (value_t*)(row_data + sizeof(int));

        for (uint32_t col = 0; col < table->column_count; col++) {
            if (p->insert_values && row < p->insert_row_count &&
                p->insert_values[row] && (int)col < p->insert_column_count) {
                value_copy(&row_values[col], &p->insert_values[row][col]);
            } else {
                value_init_null(&row_values[col]);
            }
        }

        value_t value;
        value_init_blob(&value, row_data, row_size);

        int rc = btree_insert((btree_t*)table->data_tree, &key, &value);

        sdb_free(row_data);
        value_free(&key);
        value_free(&value);

        if (rc != SPEEDSQL_OK) {
            return rc;
        }

        stmt->db->total_changes++;
    }

    return SPEEDSQL_DONE;
}

/* ============================================================================
 * Aggregate Function Handling
 * ============================================================================ */

typedef struct {
    int64_t count;
    double sum;
    double min;
    double max;
    bool has_min;
    bool has_max;
} agg_state_t;

static bool is_aggregate_function(const char* name) {
    return (strcasecmp(name, "COUNT") == 0 ||
            strcasecmp(name, "SUM") == 0 ||
            strcasecmp(name, "AVG") == 0 ||
            strcasecmp(name, "MIN") == 0 ||
            strcasecmp(name, "MAX") == 0);
}

static bool has_aggregate(expr_t* expr) {
    if (!expr) return false;

    switch (expr->type) {
        case EXPR_FUNCTION:
            if (is_aggregate_function(expr->data.function.name)) {
                return true;
            }
            for (int i = 0; i < expr->data.function.arg_count; i++) {
                if (has_aggregate(expr->data.function.args[i])) {
                    return true;
                }
            }
            break;
        case EXPR_BINARY_OP:
            return has_aggregate(expr->data.binary.left) ||
                   has_aggregate(expr->data.binary.right);
        case EXPR_UNARY_OP:
            return has_aggregate(expr->data.unary.operand);
        default:
            break;
    }
    return false;
}

/* ============================================================================
 * Result Buffer for ORDER BY / GROUP BY
 * ============================================================================ */

typedef struct {
    value_t** rows;
    int* sort_keys;   /* For ORDER BY sort key values */
    int row_count;
    int capacity;
    int col_count;
} result_buffer_t;

static void result_buffer_init(result_buffer_t* buf, int col_count) {
    buf->rows = nullptr;
    buf->sort_keys = nullptr;
    buf->row_count = 0;
    buf->capacity = 0;
    buf->col_count = col_count;
}

static void result_buffer_add(result_buffer_t* buf, value_t* row) {
    if (buf->row_count >= buf->capacity) {
        int new_cap = buf->capacity == 0 ? 64 : buf->capacity * 2;
        buf->rows = (value_t**)sdb_realloc(buf->rows, new_cap * sizeof(value_t*));
        buf->capacity = new_cap;
    }

    value_t* row_copy = (value_t*)sdb_malloc(buf->col_count * sizeof(value_t));
    for (int i = 0; i < buf->col_count; i++) {
        value_init_null(&row_copy[i]);
        value_copy(&row_copy[i], &row[i]);
    }
    buf->rows[buf->row_count++] = row_copy;
}

static void result_buffer_free(result_buffer_t* buf) {
    for (int i = 0; i < buf->row_count; i++) {
        for (int j = 0; j < buf->col_count; j++) {
            value_free(&buf->rows[i][j]);
        }
        sdb_free(buf->rows[i]);
    }
    sdb_free(buf->rows);
    sdb_free(buf->sort_keys);
}

/* Sort comparison data */
static parsed_stmt_t* g_sort_stmt = nullptr;

static int compare_rows(const void* a, const void* b) {
    value_t* row_a = *(value_t**)a;
    value_t* row_b = *(value_t**)b;

    if (!g_sort_stmt || !g_sort_stmt->order_by) return 0;

    for (int i = 0; i < g_sort_stmt->order_by_count; i++) {
        order_by_t* ob = &g_sort_stmt->order_by[i];

        /* Get column index from expression */
        int col_idx = 0;
        if (ob->expr && ob->expr->type == EXPR_COLUMN) {
            col_idx = ob->expr->data.column_ref.index;
            if (col_idx < 0) col_idx = 0;
        }

        int cmp = value_compare(&row_a[col_idx], &row_b[col_idx]);
        if (cmp != 0) {
            return ob->desc ? -cmp : cmp;
        }
    }
    return 0;
}

/* ============================================================================
 * Executor: SELECT
 * ============================================================================ */

static int execute_select_init(speedsql_stmt* stmt) {
    parsed_stmt_t* p = stmt->parsed;
    if (!p) return SPEEDSQL_MISUSE;

    /* Setup column info */
    stmt->column_count = p->column_count;
    stmt->column_names = (char**)sdb_calloc(p->column_count, sizeof(char*));
    stmt->current_row = (value_t*)sdb_calloc(p->column_count, sizeof(value_t));

    if (!stmt->column_names || !stmt->current_row) {
        return SPEEDSQL_NOMEM;
    }

    for (int i = 0; i < p->column_count; i++) {
        if (p->columns[i].alias) {
            stmt->column_names[i] = sdb_strdup(p->columns[i].alias);
        } else if (p->columns[i].expr && p->columns[i].expr->type == EXPR_COLUMN) {
            stmt->column_names[i] = sdb_strdup(p->columns[i].expr->data.column_ref.column);
        } else if (p->columns[i].expr && p->columns[i].expr->type == EXPR_FUNCTION) {
            stmt->column_names[i] = sdb_strdup(p->columns[i].expr->data.function.name);
        } else {
            char buf[32];
            snprintf(buf, sizeof(buf), "column%d", i);
            stmt->column_names[i] = sdb_strdup(buf);
        }
    }

    /* Initialize cursor for table scan */
    if (p->table_count > 0) {
        table_def_t* table = find_table(stmt->db, p->tables[0].name);
        if (!table) {
            sdb_set_error(stmt->db, SPEEDSQL_ERROR, "Table '%s' not found", p->tables[0].name);
            return SPEEDSQL_ERROR;
        }

        if (table->data_tree) {
            /* Create plan node for scan */
            stmt->plan = (plan_node_t*)sdb_calloc(1, sizeof(plan_node_t));
            if (!stmt->plan) return SPEEDSQL_NOMEM;

            stmt->plan->type = PLAN_SCAN;
            stmt->plan->data.scan.table = table;

            btree_cursor_init(&stmt->plan->data.scan.cursor, (btree_t*)table->data_tree);
            btree_cursor_first(&stmt->plan->data.scan.cursor);
        }
    }

    stmt->executed = true;
    return SPEEDSQL_OK;
}

/* Evaluate aggregate expression given aggregate states */
static void eval_aggregate_expr(expr_t* expr, agg_state_t* agg, value_t* result) {
    if (!expr || expr->type != EXPR_FUNCTION) {
        value_init_null(result);
        return;
    }

    const char* name = expr->data.function.name;

    if (strcasecmp(name, "COUNT") == 0) {
        value_init_int(result, agg->count);
    } else if (strcasecmp(name, "SUM") == 0) {
        value_init_float(result, agg->sum);
    } else if (strcasecmp(name, "AVG") == 0) {
        if (agg->count > 0) {
            value_init_float(result, agg->sum / (double)agg->count);
        } else {
            value_init_null(result);
        }
    } else if (strcasecmp(name, "MIN") == 0) {
        if (agg->has_min) {
            value_init_float(result, agg->min);
        } else {
            value_init_null(result);
        }
    } else if (strcasecmp(name, "MAX") == 0) {
        if (agg->has_max) {
            value_init_float(result, agg->max);
        } else {
            value_init_null(result);
        }
    } else {
        value_init_null(result);
    }
}

/* Process aggregate from row value */
static void process_aggregate(agg_state_t* agg, value_t* val) {
    agg->count++;

    double v = 0.0;
    if (val->type == VAL_INT) {
        v = (double)val->data.i;
    } else if (val->type == VAL_FLOAT) {
        v = val->data.f;
    } else if (val->type == VAL_NULL) {
        return;
    }

    agg->sum += v;

    if (!agg->has_min || v < agg->min) {
        agg->min = v;
        agg->has_min = true;
    }
    if (!agg->has_max || v > agg->max) {
        agg->max = v;
        agg->has_max = true;
    }
}

/* ============================================================================
 * JOIN Execution Helper
 * ============================================================================ */

typedef struct {
    value_t** left_rows;
    int* left_col_counts;
    int left_row_count;
    int left_capacity;
    value_t** right_rows;
    int* right_col_counts;
    int right_row_count;
    int right_capacity;
    int left_idx;
    int right_idx;
    bool* right_matched;  /* For LEFT JOIN tracking */
} join_state_t;

static void collect_table_rows(btree_t* tree, value_t*** rows_out, int** col_counts_out,
                                int* row_count_out, int* capacity_out) {
    btree_cursor_t cursor;
    btree_cursor_init(&cursor, tree);
    btree_cursor_first(&cursor);

    int capacity = 64;
    *rows_out = (value_t**)sdb_malloc(capacity * sizeof(value_t*));
    *col_counts_out = (int*)sdb_malloc(capacity * sizeof(int));
    *row_count_out = 0;

    while (cursor.valid && !cursor.at_end) {
        value_t key, value;
        value_init_null(&key);
        value_init_null(&value);

        btree_cursor_key(&cursor, &key);
        btree_cursor_value(&cursor, &value);

        if (value.type == VAL_BLOB && value.data.blob.data) {
            int col_count = *(int*)value.data.blob.data;
            value_t* row_vals = (value_t*)((uint8_t*)value.data.blob.data + sizeof(int));

            if (*row_count_out >= capacity) {
                capacity *= 2;
                *rows_out = (value_t**)sdb_realloc(*rows_out, capacity * sizeof(value_t*));
                *col_counts_out = (int*)sdb_realloc(*col_counts_out, capacity * sizeof(int));
            }

            value_t* row_copy = (value_t*)sdb_malloc(col_count * sizeof(value_t));
            for (int i = 0; i < col_count; i++) {
                value_init_null(&row_copy[i]);
                value_copy(&row_copy[i], &row_vals[i]);
            }
            (*rows_out)[*row_count_out] = row_copy;
            (*col_counts_out)[*row_count_out] = col_count;
            (*row_count_out)++;
        }

        value_free(&key);
        value_free(&value);
        btree_cursor_next(&cursor);
    }

    btree_cursor_close(&cursor);
    *capacity_out = capacity;
}

static int execute_select_step(speedsql_stmt* stmt) {
    parsed_stmt_t* p = stmt->parsed;

    if (!stmt->plan) {
        /* No table - might be a simple expression like SELECT 1+1 */
        if (!stmt->has_row) {
            for (int i = 0; i < p->column_count; i++) {
                value_free(&stmt->current_row[i]);
                eval_expr(stmt, p->columns[i].expr, &stmt->current_row[i]);
            }
            stmt->has_row = true;
            stmt->step_count++;
            return SPEEDSQL_ROW;
        }
        return SPEEDSQL_DONE;
    }

    /* Check if we have JOINs */
    bool has_joins = (p->join_count > 0);

    /* Check if we have ORDER BY, GROUP BY, or aggregates */
    bool needs_buffering = (p->order_by_count > 0 || has_joins);
    bool has_aggregates = false;

    for (int i = 0; i < p->column_count; i++) {
        if (has_aggregate(p->columns[i].expr)) {
            has_aggregates = true;
            break;
        }
    }

    /* Handle aggregates (with or without GROUP BY) */
    if (has_aggregates && !stmt->has_row) {
        agg_state_t* agg_states = (agg_state_t*)sdb_calloc(p->column_count, sizeof(agg_state_t));

        btree_cursor_t* cursor = &stmt->plan->data.scan.cursor;
        table_def_t* table = stmt->plan->data.scan.table;

        while (cursor->valid && !cursor->at_end) {
            value_t key, value;
            value_init_null(&key);
            value_init_null(&value);

            btree_cursor_key(cursor, &key);
            btree_cursor_value(cursor, &value);

            if (value.type == VAL_BLOB && value.data.blob.data) {
                int col_count = *(int*)value.data.blob.data;
                value_t* row_vals = (value_t*)((uint8_t*)value.data.blob.data + sizeof(int));

                /* Apply WHERE filter */
                bool pass_filter = true;
                if (p->where) {
                    stmt->current_row = row_vals;
                    stmt->column_count = col_count;

                    value_t filter_result;
                    value_init_null(&filter_result);
                    eval_expr(stmt, p->where, &filter_result);

                    pass_filter = (filter_result.type != VAL_NULL && filter_result.data.i != 0);
                    value_free(&filter_result);
                }

                if (pass_filter) {
                    /* Process aggregates */
                    for (int i = 0; i < p->column_count; i++) {
                        expr_t* expr = p->columns[i].expr;
                        if (expr && expr->type == EXPR_FUNCTION &&
                            is_aggregate_function(expr->data.function.name)) {

                            if (expr->data.function.arg_count > 0 &&
                                expr->data.function.args[0]) {

                                /* Evaluate the argument */
                                stmt->current_row = row_vals;
                                stmt->column_count = col_count;

                                value_t arg_val;
                                value_init_null(&arg_val);
                                eval_expr(stmt, expr->data.function.args[0], &arg_val);

                                process_aggregate(&agg_states[i], &arg_val);
                                value_free(&arg_val);
                            } else {
                                /* COUNT(*) */
                                agg_states[i].count++;
                            }
                        }
                    }
                }
            }

            value_free(&key);
            value_free(&value);
            btree_cursor_next(cursor);
        }

        /* Build result row from aggregates */
        stmt->column_count = p->column_count;
        for (int i = 0; i < p->column_count; i++) {
            value_free(&stmt->current_row[i]);

            expr_t* expr = p->columns[i].expr;
            if (expr && expr->type == EXPR_FUNCTION &&
                is_aggregate_function(expr->data.function.name)) {
                eval_aggregate_expr(expr, &agg_states[i], &stmt->current_row[i]);
            } else {
                value_init_null(&stmt->current_row[i]);
            }
        }

        sdb_free(agg_states);
        stmt->has_row = true;
        stmt->step_count++;
        return SPEEDSQL_ROW;
    }

    /* Handle ORDER BY or JOINs - buffer all results, sort, then return */
    if (needs_buffering && !stmt->has_row) {
        result_buffer_t buf;
        result_buffer_init(&buf, p->column_count);

        table_def_t* table = stmt->plan->data.scan.table;

        /* Resolve ORDER BY column indices */
        for (int i = 0; i < p->order_by_count; i++) {
            if (p->order_by[i].expr && p->order_by[i].expr->type == EXPR_COLUMN) {
                const char* col_name = p->order_by[i].expr->data.column_ref.column;
                for (uint32_t c = 0; c < table->column_count; c++) {
                    if (strcmp(table->columns[c].name, col_name) == 0) {
                        p->order_by[i].expr->data.column_ref.index = c;
                        break;
                    }
                }
            }
        }

        if (has_joins && p->join_count > 0) {
            /* Handle JOINs - nested loop join implementation */
            value_t** left_rows = nullptr;
            int* left_col_counts = nullptr;
            int left_row_count = 0;
            int left_capacity = 0;

            /* Collect left table rows */
            collect_table_rows((btree_t*)table->data_tree, &left_rows, &left_col_counts,
                               &left_row_count, &left_capacity);

            for (int j = 0; j < p->join_count; j++) {
                join_clause_t* jc = &p->joins[j];

                /* Find right table */
                table_def_t* right_table = find_table(stmt->db, jc->table_name);
                if (!right_table || !right_table->data_tree) continue;

                value_t** right_rows = nullptr;
                int* right_col_counts = nullptr;
                int right_row_count = 0;
                int right_capacity = 0;

                collect_table_rows((btree_t*)right_table->data_tree, &right_rows, &right_col_counts,
                                   &right_row_count, &right_capacity);

                /* Track matched rows for LEFT/RIGHT JOIN */
                bool* left_matched = (jc->type == JOIN_LEFT) ?
                    (bool*)sdb_calloc(left_row_count, sizeof(bool)) : nullptr;
                bool* right_matched = (jc->type == JOIN_RIGHT) ?
                    (bool*)sdb_calloc(right_row_count, sizeof(bool)) : nullptr;

                /* Nested loop join */
                for (int li = 0; li < left_row_count; li++) {
                    bool found_match = false;

                    for (int ri = 0; ri < right_row_count; ri++) {
                        /* Build combined row for ON condition evaluation */
                        int total_cols = left_col_counts[li] + right_col_counts[ri];
                        value_t* combined = (value_t*)sdb_calloc(total_cols, sizeof(value_t));

                        for (int c = 0; c < left_col_counts[li]; c++) {
                            value_copy(&combined[c], &left_rows[li][c]);
                        }
                        for (int c = 0; c < right_col_counts[ri]; c++) {
                            value_copy(&combined[left_col_counts[li] + c], &right_rows[ri][c]);
                        }

                        /* Evaluate ON condition */
                        bool matches = true;
                        if (jc->on_condition) {
                            stmt->current_row = combined;
                            stmt->column_count = total_cols;

                            value_t result;
                            value_init_null(&result);
                            eval_expr(stmt, jc->on_condition, &result);

                            matches = (result.type != VAL_NULL && result.data.i != 0);
                            value_free(&result);
                        }

                        if (matches) {
                            found_match = true;
                            if (left_matched) left_matched[li] = true;
                            if (right_matched) right_matched[ri] = true;

                            /* Apply WHERE and project */
                            bool pass_filter = true;
                            if (p->where) {
                                stmt->current_row = combined;
                                stmt->column_count = total_cols;

                                value_t filter_result;
                                value_init_null(&filter_result);
                                eval_expr(stmt, p->where, &filter_result);

                                pass_filter = (filter_result.type != VAL_NULL && filter_result.data.i != 0);
                                value_free(&filter_result);
                            }

                            if (pass_filter) {
                                value_t* projected = (value_t*)sdb_calloc(p->column_count, sizeof(value_t));

                                for (int i = 0; i < p->column_count; i++) {
                                    if (p->columns[i].expr) {
                                        stmt->current_row = combined;
                                        stmt->column_count = total_cols;
                                        eval_expr(stmt, p->columns[i].expr, &projected[i]);
                                    }
                                }

                                result_buffer_add(&buf, projected);

                                for (int i = 0; i < p->column_count; i++) {
                                    value_free(&projected[i]);
                                }
                                sdb_free(projected);
                            }
                        }

                        for (int c = 0; c < total_cols; c++) {
                            value_free(&combined[c]);
                        }
                        sdb_free(combined);
                    }

                    /* LEFT JOIN: add unmatched left rows with NULLs */
                    if (jc->type == JOIN_LEFT && !found_match) {
                        int total_cols = left_col_counts[li] + (right_row_count > 0 ? right_col_counts[0] : 0);
                        value_t* combined = (value_t*)sdb_calloc(total_cols, sizeof(value_t));

                        for (int c = 0; c < left_col_counts[li]; c++) {
                            value_copy(&combined[c], &left_rows[li][c]);
                        }
                        /* Right side is NULL */
                        for (int c = left_col_counts[li]; c < total_cols; c++) {
                            value_init_null(&combined[c]);
                        }

                        bool pass_filter = true;
                        if (p->where) {
                            stmt->current_row = combined;
                            stmt->column_count = total_cols;

                            value_t filter_result;
                            value_init_null(&filter_result);
                            eval_expr(stmt, p->where, &filter_result);

                            pass_filter = (filter_result.type != VAL_NULL && filter_result.data.i != 0);
                            value_free(&filter_result);
                        }

                        if (pass_filter) {
                            value_t* projected = (value_t*)sdb_calloc(p->column_count, sizeof(value_t));

                            for (int i = 0; i < p->column_count; i++) {
                                if (p->columns[i].expr) {
                                    stmt->current_row = combined;
                                    stmt->column_count = total_cols;
                                    eval_expr(stmt, p->columns[i].expr, &projected[i]);
                                }
                            }

                            result_buffer_add(&buf, projected);

                            for (int i = 0; i < p->column_count; i++) {
                                value_free(&projected[i]);
                            }
                            sdb_free(projected);
                        }

                        for (int c = 0; c < total_cols; c++) {
                            value_free(&combined[c]);
                        }
                        sdb_free(combined);
                    }
                }

                /* RIGHT JOIN: add unmatched right rows with NULLs */
                if (jc->type == JOIN_RIGHT && right_matched) {
                    for (int ri = 0; ri < right_row_count; ri++) {
                        if (right_matched[ri]) continue;

                        int total_cols = (left_row_count > 0 ? left_col_counts[0] : 0) + right_col_counts[ri];
                        value_t* combined = (value_t*)sdb_calloc(total_cols, sizeof(value_t));

                        /* Left side is NULL */
                        for (int c = 0; c < (left_row_count > 0 ? left_col_counts[0] : 0); c++) {
                            value_init_null(&combined[c]);
                        }
                        for (int c = 0; c < right_col_counts[ri]; c++) {
                            value_copy(&combined[(left_row_count > 0 ? left_col_counts[0] : 0) + c],
                                       &right_rows[ri][c]);
                        }

                        value_t* projected = (value_t*)sdb_calloc(p->column_count, sizeof(value_t));

                        for (int i = 0; i < p->column_count; i++) {
                            if (p->columns[i].expr) {
                                stmt->current_row = combined;
                                stmt->column_count = total_cols;
                                eval_expr(stmt, p->columns[i].expr, &projected[i]);
                            }
                        }

                        result_buffer_add(&buf, projected);

                        for (int i = 0; i < p->column_count; i++) {
                            value_free(&projected[i]);
                        }
                        sdb_free(projected);

                        for (int c = 0; c < total_cols; c++) {
                            value_free(&combined[c]);
                        }
                        sdb_free(combined);
                    }
                }

                /* Clean up right table rows */
                for (int ri = 0; ri < right_row_count; ri++) {
                    for (int c = 0; c < right_col_counts[ri]; c++) {
                        value_free(&right_rows[ri][c]);
                    }
                    sdb_free(right_rows[ri]);
                }
                sdb_free(right_rows);
                sdb_free(right_col_counts);
                sdb_free(left_matched);
                sdb_free(right_matched);
            }

            /* Clean up left table rows */
            for (int li = 0; li < left_row_count; li++) {
                for (int c = 0; c < left_col_counts[li]; c++) {
                    value_free(&left_rows[li][c]);
                }
                sdb_free(left_rows[li]);
            }
            sdb_free(left_rows);
            sdb_free(left_col_counts);

        } else {
            /* No JOINs - simple table scan with buffering */
            btree_cursor_t* cursor = &stmt->plan->data.scan.cursor;

            while (cursor->valid && !cursor->at_end) {
                value_t key, value;
                value_init_null(&key);
                value_init_null(&value);

                btree_cursor_key(cursor, &key);
                btree_cursor_value(cursor, &value);

                if (value.type == VAL_BLOB && value.data.blob.data) {
                    int col_count = *(int*)value.data.blob.data;
                    value_t* row_vals = (value_t*)((uint8_t*)value.data.blob.data + sizeof(int));

                    /* Apply WHERE filter */
                    bool pass_filter = true;
                    if (p->where) {
                        stmt->current_row = row_vals;
                        stmt->column_count = col_count;

                        value_t filter_result;
                        value_init_null(&filter_result);
                        eval_expr(stmt, p->where, &filter_result);

                        pass_filter = (filter_result.type != VAL_NULL && filter_result.data.i != 0);
                        value_free(&filter_result);
                    }

                    if (pass_filter) {
                        /* Project row */
                        value_t* projected = (value_t*)sdb_calloc(p->column_count, sizeof(value_t));

                        for (int i = 0; i < p->column_count; i++) {
                            if (p->columns[i].expr && p->columns[i].expr->type == EXPR_COLUMN) {
                                int idx = p->columns[i].expr->data.column_ref.index;
                                if (idx < 0) {
                                    /* Resolve by name */
                                    const char* col_name = p->columns[i].expr->data.column_ref.column;
                                    for (uint32_t c = 0; c < table->column_count; c++) {
                                        if (strcmp(table->columns[c].name, col_name) == 0) {
                                            idx = c;
                                            p->columns[i].expr->data.column_ref.index = c;
                                            break;
                                        }
                                    }
                                }
                                if (idx >= 0 && idx < col_count) {
                                    value_copy(&projected[i], &row_vals[idx]);
                                }
                            } else if (p->columns[i].expr) {
                                stmt->current_row = row_vals;
                                stmt->column_count = col_count;
                                eval_expr(stmt, p->columns[i].expr, &projected[i]);
                            }
                        }

                        result_buffer_add(&buf, projected);

                        for (int i = 0; i < p->column_count; i++) {
                            value_free(&projected[i]);
                        }
                        sdb_free(projected);
                    }
                }

                value_free(&key);
                value_free(&value);
                btree_cursor_next(cursor);
            }
        }

        /* Sort the buffer */
        if (buf.row_count > 0) {
            g_sort_stmt = p;
            qsort(buf.rows, buf.row_count, sizeof(value_t*), compare_rows);
            g_sort_stmt = nullptr;
        }

        /* Store buffer in plan for iteration */
        stmt->plan->data.sort.buffer = buf.rows;
        stmt->plan->data.sort.buffer_size = buf.row_count;
        stmt->plan->data.sort.current = 0;
        stmt->plan->type = PLAN_SORT;  /* Switch plan type */

        /* Don't free buf.rows - transferred to plan */
        buf.rows = nullptr;
        buf.capacity = 0;

        stmt->has_row = true;
    }

    /* Return sorted results */
    if (stmt->plan->type == PLAN_SORT) {
        int idx = stmt->plan->data.sort.current;

        /* Apply OFFSET */
        while (idx < p->offset && idx < stmt->plan->data.sort.buffer_size) {
            idx++;
        }

        if (idx >= stmt->plan->data.sort.buffer_size) {
            /* Free buffer */
            for (int i = 0; i < stmt->plan->data.sort.buffer_size; i++) {
                for (int j = 0; j < p->column_count; j++) {
                    value_free(&stmt->plan->data.sort.buffer[i][j]);
                }
                sdb_free(stmt->plan->data.sort.buffer[i]);
            }
            sdb_free(stmt->plan->data.sort.buffer);
            stmt->plan->data.sort.buffer = nullptr;
            return SPEEDSQL_DONE;
        }

        /* Check LIMIT */
        int returned_count = idx - (int)p->offset;
        if (p->limit > 0 && returned_count >= p->limit) {
            /* Free buffer */
            for (int i = 0; i < stmt->plan->data.sort.buffer_size; i++) {
                for (int j = 0; j < p->column_count; j++) {
                    value_free(&stmt->plan->data.sort.buffer[i][j]);
                }
                sdb_free(stmt->plan->data.sort.buffer[i]);
            }
            sdb_free(stmt->plan->data.sort.buffer);
            stmt->plan->data.sort.buffer = nullptr;
            return SPEEDSQL_DONE;
        }

        /* Copy row to current_row */
        for (int i = 0; i < p->column_count; i++) {
            value_free(&stmt->current_row[i]);
            value_copy(&stmt->current_row[i], &stmt->plan->data.sort.buffer[idx][i]);
        }

        stmt->plan->data.sort.current = idx + 1;
        stmt->step_count++;
        return SPEEDSQL_ROW;
    }

    /* Simple table scan without buffering */
    btree_cursor_t* cursor = &stmt->plan->data.scan.cursor;
    table_def_t* table = stmt->plan->data.scan.table;

    /* Apply OFFSET on first access */
    while (p->offset > 0 && stmt->step_count < p->offset &&
           cursor->valid && !cursor->at_end) {

        value_t key, value;
        value_init_null(&key);
        value_init_null(&value);

        btree_cursor_key(cursor, &key);
        btree_cursor_value(cursor, &value);

        bool pass_filter = true;
        if (p->where && value.type == VAL_BLOB && value.data.blob.data) {
            int col_count = *(int*)value.data.blob.data;
            value_t* row_vals = (value_t*)((uint8_t*)value.data.blob.data + sizeof(int));

            stmt->current_row = row_vals;
            stmt->column_count = col_count;

            value_t filter_result;
            value_init_null(&filter_result);
            eval_expr(stmt, p->where, &filter_result);

            pass_filter = (filter_result.type != VAL_NULL && filter_result.data.i != 0);
            value_free(&filter_result);
        }

        value_free(&key);
        value_free(&value);

        if (pass_filter) {
            stmt->step_count++;
        }

        btree_cursor_next(cursor);
    }

    while (cursor->valid && !cursor->at_end) {
        /* Get current row */
        value_t key, value;
        value_init_null(&key);
        value_init_null(&value);

        btree_cursor_key(cursor, &key);
        btree_cursor_value(cursor, &value);

        /* Unpack row values */
        if (value.type == VAL_BLOB && value.data.blob.data) {
            int col_count = *(int*)value.data.blob.data;
            value_t* row_vals = (value_t*)((uint8_t*)value.data.blob.data + sizeof(int));

            /* Apply WHERE filter if present */
            bool pass_filter = true;
            if (p->where) {
                /* Store row values temporarily for expression evaluation */
                value_t* old_row = stmt->current_row;
                stmt->current_row = row_vals;
                int old_count = stmt->column_count;
                stmt->column_count = col_count;

                value_t filter_result;
                value_init_null(&filter_result);
                eval_expr(stmt, p->where, &filter_result);

                stmt->current_row = old_row;
                stmt->column_count = old_count;

                pass_filter = (filter_result.type != VAL_NULL && filter_result.data.i != 0);
                value_free(&filter_result);
            }

            if (pass_filter) {
                /* Resolve column indices if not done */
                for (int i = 0; i < p->column_count; i++) {
                    if (p->columns[i].expr && p->columns[i].expr->type == EXPR_COLUMN) {
                        if (p->columns[i].expr->data.column_ref.index < 0) {
                            const char* col_name = p->columns[i].expr->data.column_ref.column;
                            for (uint32_t c = 0; c < table->column_count; c++) {
                                if (strcmp(table->columns[c].name, col_name) == 0) {
                                    p->columns[i].expr->data.column_ref.index = c;
                                    break;
                                }
                            }
                        }
                    }
                }

                /* Project columns */
                for (int i = 0; i < stmt->column_count; i++) {
                    value_free(&stmt->current_row[i]);

                    if (p->columns[i].expr) {
                        if (p->columns[i].expr->type == EXPR_COLUMN) {
                            int idx = p->columns[i].expr->data.column_ref.index;
                            if (idx >= 0 && idx < col_count) {
                                value_copy(&stmt->current_row[i], &row_vals[idx]);
                            } else {
                                value_init_null(&stmt->current_row[i]);
                            }
                        } else {
                            stmt->current_row = row_vals;
                            stmt->column_count = col_count;
                            value_t temp_row[32];
                            for (int j = 0; j < col_count && j < 32; j++) {
                                value_copy(&temp_row[j], &row_vals[j]);
                            }
                            stmt->current_row = temp_row;
                            eval_expr(stmt, p->columns[i].expr, &stmt->current_row[i]);
                        }
                    }
                }

                stmt->column_count = p->column_count;

                value_free(&key);
                value_free(&value);

                /* Advance cursor for next call */
                btree_cursor_next(cursor);

                stmt->has_row = true;
                stmt->step_count++;

                /* Check LIMIT */
                if (p->limit > 0 && (stmt->step_count - p->offset) > p->limit) {
                    return SPEEDSQL_DONE;
                }

                return SPEEDSQL_ROW;
            }
        }

        value_free(&key);
        value_free(&value);
        btree_cursor_next(cursor);
    }

    return SPEEDSQL_DONE;
}

/* ============================================================================
 * Public API: speedsql_exec
 * ============================================================================ */

SPEEDSQL_API int speedsql_exec(
    speedsql* db,
    const char* sql,
    int (*callback)(void*, int, char**, char**),
    void* arg,
    char** errmsg
) {
    if (!db || !sql) return SPEEDSQL_MISUSE;

    speedsql_stmt* stmt = nullptr;
    const char* tail = sql;
    int rc;

    /* Process all statements in the SQL string */
    while (*tail) {
        /* Skip whitespace */
        while (*tail && (*tail == ' ' || *tail == '\t' || *tail == '\n' || *tail == '\r')) {
            tail++;
        }
        if (!*tail) break;

        rc = speedsql_prepare(db, tail, -1, &stmt, &tail);
        if (rc != SPEEDSQL_OK) {
            if (errmsg) {
                *errmsg = sdb_strdup(speedsql_errmsg(db));
            }
            return rc;
        }

        if (!stmt) continue;  /* Empty statement */

        /* Execute the statement */
        while ((rc = speedsql_step(stmt)) == SPEEDSQL_ROW) {
            if (callback) {
                int col_count = speedsql_column_count(stmt);
                char** values = (char**)sdb_calloc(col_count, sizeof(char*));
                char** names = (char**)sdb_calloc(col_count, sizeof(char*));

                for (int i = 0; i < col_count; i++) {
                    names[i] = (char*)speedsql_column_name(stmt, i);
                    const unsigned char* text = speedsql_column_text(stmt, i);
                    values[i] = text ? sdb_strdup((const char*)text) : nullptr;
                }

                int cb_rc = callback(arg, col_count, values, names);

                for (int i = 0; i < col_count; i++) {
                    if (values[i]) sdb_free(values[i]);
                }
                sdb_free(values);
                sdb_free(names);

                if (cb_rc != 0) {
                    speedsql_finalize(stmt);
                    return SPEEDSQL_ERROR;
                }
            }
        }

        speedsql_finalize(stmt);
        stmt = nullptr;

        if (rc != SPEEDSQL_DONE && rc != SPEEDSQL_OK) {
            if (errmsg) {
                *errmsg = sdb_strdup(speedsql_errmsg(db));
            }
            return rc;
        }
    }

    return SPEEDSQL_OK;
}

/* ============================================================================
 * Public API: speedsql_prepare
 * ============================================================================ */

SPEEDSQL_API int speedsql_prepare(
    speedsql* db,
    const char* sql,
    int sql_len,
    speedsql_stmt** stmt_out,
    const char** tail
) {
    if (!db || !sql || !stmt_out) return SPEEDSQL_MISUSE;

    *stmt_out = nullptr;

    /* Allocate statement */
    speedsql_stmt* stmt = stmt_alloc(db);
    if (!stmt) return SPEEDSQL_NOMEM;

    /* Copy SQL */
    if (sql_len < 0) sql_len = strlen(sql);
    stmt->sql = (char*)sdb_malloc(sql_len + 1);
    if (!stmt->sql) {
        stmt_free_internal(stmt);
        return SPEEDSQL_NOMEM;
    }
    memcpy(stmt->sql, sql, sql_len);
    stmt->sql[sql_len] = '\0';

    /* Parse SQL */
    parser_t parser;
    parser_init(&parser, db, stmt->sql);
    stmt->parsed = parser_parse(&parser);

    if (parser.had_error) {
        sdb_set_error(db, SPEEDSQL_ERROR, "%s", parser.error);
        stmt_free_internal(stmt);
        return SPEEDSQL_ERROR;
    }

    if (!stmt->parsed) {
        /* Empty statement */
        stmt_free_internal(stmt);
        if (tail) *tail = parser.lexer.current;
        return SPEEDSQL_OK;
    }

    /* Set tail if requested */
    if (tail) {
        *tail = parser.lexer.current;
    }

    /* Count parameters by walking the AST */
    stmt->param_count = count_params_in_stmt(stmt->parsed);

    /* Allocate parameter array if needed */
    if (stmt->param_count > 0) {
        stmt->params = (value_t*)sdb_calloc(stmt->param_count, sizeof(value_t));
        if (!stmt->params) {
            stmt_free_internal(stmt);
            return SPEEDSQL_NOMEM;
        }
    }

    *stmt_out = stmt;
    return SPEEDSQL_OK;
}

/* ============================================================================
 * Public API: speedsql_step
 * ============================================================================ */

SPEEDSQL_API int speedsql_step(speedsql_stmt* stmt) {
    if (!stmt || !stmt->db) return SPEEDSQL_MISUSE;
    if (!stmt->parsed) return SPEEDSQL_DONE;

    int rc;

    switch (stmt->parsed->op) {
        case SQL_SELECT:
            if (!stmt->executed) {
                rc = execute_select_init(stmt);
                if (rc != SPEEDSQL_OK) return rc;
            }
            return execute_select_step(stmt);

        case SQL_INSERT:
            if (!stmt->executed) {
                stmt->executed = true;
                return execute_insert(stmt);
            }
            return SPEEDSQL_DONE;

        case SQL_UPDATE:
            if (!stmt->executed) {
                stmt->executed = true;
                return execute_update(stmt);
            }
            return SPEEDSQL_DONE;

        case SQL_DELETE:
            if (!stmt->executed) {
                stmt->executed = true;
                return execute_delete(stmt);
            }
            return SPEEDSQL_DONE;

        case SQL_CREATE_TABLE:
            if (!stmt->executed) {
                stmt->executed = true;
                rc = execute_create_table(stmt);
                return (rc == SPEEDSQL_OK) ? SPEEDSQL_DONE : rc;
            }
            return SPEEDSQL_DONE;

        case SQL_DROP_TABLE:
            if (!stmt->executed) {
                stmt->executed = true;
                rc = execute_drop_table(stmt);
                return (rc == SPEEDSQL_OK) ? SPEEDSQL_DONE : rc;
            }
            return SPEEDSQL_DONE;

        case SQL_CREATE_INDEX:
            if (!stmt->executed) {
                stmt->executed = true;
                rc = execute_create_index(stmt);
                return (rc == SPEEDSQL_OK) ? SPEEDSQL_DONE : rc;
            }
            return SPEEDSQL_DONE;

        case SQL_DROP_INDEX:
            if (!stmt->executed) {
                stmt->executed = true;
                rc = execute_drop_index(stmt);
                return (rc == SPEEDSQL_OK) ? SPEEDSQL_DONE : rc;
            }
            return SPEEDSQL_DONE;

        case SQL_BEGIN:
            if (!stmt->executed) {
                stmt->executed = true;
                return speedsql_begin(stmt->db);
            }
            return SPEEDSQL_DONE;

        case SQL_COMMIT:
            if (!stmt->executed) {
                stmt->executed = true;
                return speedsql_commit(stmt->db);
            }
            return SPEEDSQL_DONE;

        case SQL_ROLLBACK:
            if (!stmt->executed) {
                stmt->executed = true;
                return speedsql_rollback(stmt->db);
            }
            return SPEEDSQL_DONE;

        default:
            sdb_set_error(stmt->db, SPEEDSQL_ERROR, "Unsupported SQL operation");
            return SPEEDSQL_ERROR;
    }
}

/* ============================================================================
 * Public API: speedsql_reset
 * ============================================================================ */

SPEEDSQL_API int speedsql_reset(speedsql_stmt* stmt) {
    if (!stmt) return SPEEDSQL_MISUSE;

    stmt->executed = false;
    stmt->has_row = false;
    stmt->step_count = 0;

    /* Reset cursor if exists */
    if (stmt->plan && stmt->plan->type == PLAN_SCAN) {
        btree_cursor_close(&stmt->plan->data.scan.cursor);
        if (stmt->plan->data.scan.table && stmt->plan->data.scan.table->data_tree) {
            btree_cursor_init(&stmt->plan->data.scan.cursor,
                             (btree_t*)stmt->plan->data.scan.table->data_tree);
        }
    }

    /* Clear current row */
    if (stmt->current_row) {
        for (int i = 0; i < stmt->column_count; i++) {
            value_free(&stmt->current_row[i]);
            value_init_null(&stmt->current_row[i]);
        }
    }

    return SPEEDSQL_OK;
}

/* ============================================================================
 * Public API: speedsql_finalize
 * ============================================================================ */

SPEEDSQL_API int speedsql_finalize(speedsql_stmt* stmt) {
    if (!stmt) return SPEEDSQL_MISUSE;

    /* Close cursor */
    if (stmt->plan && stmt->plan->type == PLAN_SCAN) {
        btree_cursor_close(&stmt->plan->data.scan.cursor);
    }

    stmt_free_internal(stmt);
    return SPEEDSQL_OK;
}

/* ============================================================================
 * Public API: Binding Functions
 * ============================================================================ */

SPEEDSQL_API int speedsql_bind_null(speedsql_stmt* stmt, int idx) {
    if (!stmt || idx < 1 || idx > stmt->param_count) return SPEEDSQL_RANGE;

    value_free(&stmt->params[idx - 1]);
    value_init_null(&stmt->params[idx - 1]);
    return SPEEDSQL_OK;
}

SPEEDSQL_API int speedsql_bind_int(speedsql_stmt* stmt, int idx, int value) {
    return speedsql_bind_int64(stmt, idx, value);
}

SPEEDSQL_API int speedsql_bind_int64(speedsql_stmt* stmt, int idx, int64_t value) {
    if (!stmt || idx < 1 || idx > stmt->param_count) return SPEEDSQL_RANGE;

    value_free(&stmt->params[idx - 1]);
    value_init_int(&stmt->params[idx - 1], value);
    return SPEEDSQL_OK;
}

SPEEDSQL_API int speedsql_bind_double(speedsql_stmt* stmt, int idx, double value) {
    if (!stmt || idx < 1 || idx > stmt->param_count) return SPEEDSQL_RANGE;

    value_free(&stmt->params[idx - 1]);
    value_init_float(&stmt->params[idx - 1], value);
    return SPEEDSQL_OK;
}

SPEEDSQL_API int speedsql_bind_text(speedsql_stmt* stmt, int idx, const char* value,
                                    int len, void(*destructor)(void*)) {
    (void)destructor;  /* We always copy */
    if (!stmt || idx < 1 || idx > stmt->param_count) return SPEEDSQL_RANGE;

    value_free(&stmt->params[idx - 1]);
    if (value) {
        value_init_text(&stmt->params[idx - 1], value, len < 0 ? strlen(value) : len);
    } else {
        value_init_null(&stmt->params[idx - 1]);
    }
    return SPEEDSQL_OK;
}

SPEEDSQL_API int speedsql_bind_blob(speedsql_stmt* stmt, int idx, const void* value,
                                    int len, void(*destructor)(void*)) {
    (void)destructor;
    if (!stmt || idx < 1 || idx > stmt->param_count) return SPEEDSQL_RANGE;

    value_free(&stmt->params[idx - 1]);
    if (value && len > 0) {
        value_init_blob(&stmt->params[idx - 1], value, len);
    } else {
        value_init_null(&stmt->params[idx - 1]);
    }
    return SPEEDSQL_OK;
}

SPEEDSQL_API int speedsql_bind_json(speedsql_stmt* stmt, int idx, const char* json, int len) {
    /* JSON stored as text for now */
    return speedsql_bind_text(stmt, idx, json, len, nullptr);
}

SPEEDSQL_API int speedsql_bind_vector(speedsql_stmt* stmt, int idx, const float* vec, int dims) {
    if (!stmt || idx < 1 || idx > stmt->param_count) return SPEEDSQL_RANGE;

    value_free(&stmt->params[idx - 1]);
    if (vec && dims > 0) {
        value_init_blob(&stmt->params[idx - 1], vec, dims * sizeof(float));
        stmt->params[idx - 1].type = VAL_VECTOR;
    } else {
        value_init_null(&stmt->params[idx - 1]);
    }
    return SPEEDSQL_OK;
}

/* ============================================================================
 * Public API: Column Access Functions
 * ============================================================================ */

SPEEDSQL_API int speedsql_column_count(speedsql_stmt* stmt) {
    if (!stmt) return 0;
    return stmt->column_count;
}

SPEEDSQL_API const char* speedsql_column_name(speedsql_stmt* stmt, int col) {
    if (!stmt || col < 0 || col >= stmt->column_count) return nullptr;
    return stmt->column_names ? stmt->column_names[col] : nullptr;
}

SPEEDSQL_API int speedsql_column_type(speedsql_stmt* stmt, int col) {
    if (!stmt || col < 0 || col >= stmt->column_count || !stmt->current_row) {
        return SPEEDSQL_TYPE_NULL;
    }

    switch (stmt->current_row[col].type) {
        case VAL_NULL:   return SPEEDSQL_TYPE_NULL;
        case VAL_INT:    return SPEEDSQL_TYPE_INT;
        case VAL_FLOAT:  return SPEEDSQL_TYPE_FLOAT;
        case VAL_TEXT:   return SPEEDSQL_TYPE_TEXT;
        case VAL_BLOB:   return SPEEDSQL_TYPE_BLOB;
        case VAL_VECTOR: return SPEEDSQL_TYPE_VECTOR;
        default:         return SPEEDSQL_TYPE_NULL;
    }
}

SPEEDSQL_API int speedsql_column_int(speedsql_stmt* stmt, int col) {
    return (int)speedsql_column_int64(stmt, col);
}

SPEEDSQL_API int64_t speedsql_column_int64(speedsql_stmt* stmt, int col) {
    if (!stmt || col < 0 || col >= stmt->column_count || !stmt->current_row) {
        return 0;
    }

    value_t* v = &stmt->current_row[col];
    switch (v->type) {
        case VAL_INT:   return v->data.i;
        case VAL_FLOAT: return (int64_t)v->data.f;
        case VAL_TEXT:  return v->data.text.data ? atoll(v->data.text.data) : 0;
        default:        return 0;
    }
}

SPEEDSQL_API double speedsql_column_double(speedsql_stmt* stmt, int col) {
    if (!stmt || col < 0 || col >= stmt->column_count || !stmt->current_row) {
        return 0.0;
    }

    value_t* v = &stmt->current_row[col];
    switch (v->type) {
        case VAL_INT:   return (double)v->data.i;
        case VAL_FLOAT: return v->data.f;
        case VAL_TEXT:  return v->data.text.data ? atof(v->data.text.data) : 0.0;
        default:        return 0.0;
    }
}

SPEEDSQL_API const unsigned char* speedsql_column_text(speedsql_stmt* stmt, int col) {
    if (!stmt || col < 0 || col >= stmt->column_count || !stmt->current_row) {
        return nullptr;
    }

    value_t* v = &stmt->current_row[col];
    if (v->type == VAL_TEXT) {
        return (const unsigned char*)v->data.text.data;
    }
    return nullptr;
}

SPEEDSQL_API const void* speedsql_column_blob(speedsql_stmt* stmt, int col) {
    if (!stmt || col < 0 || col >= stmt->column_count || !stmt->current_row) {
        return nullptr;
    }

    value_t* v = &stmt->current_row[col];
    if (v->type == VAL_BLOB || v->type == VAL_VECTOR) {
        return v->data.blob.data;
    }
    return nullptr;
}

SPEEDSQL_API int speedsql_column_bytes(speedsql_stmt* stmt, int col) {
    if (!stmt || col < 0 || col >= stmt->column_count || !stmt->current_row) {
        return 0;
    }

    value_t* v = &stmt->current_row[col];
    switch (v->type) {
        case VAL_TEXT:   return v->data.text.len;
        case VAL_BLOB:
        case VAL_VECTOR: return v->data.blob.len;
        default:         return 0;
    }
}

SPEEDSQL_API const char* speedsql_column_json(speedsql_stmt* stmt, int col) {
    return (const char*)speedsql_column_text(stmt, col);
}

SPEEDSQL_API const float* speedsql_column_vector(speedsql_stmt* stmt, int col, int* dimensions) {
    if (!stmt || col < 0 || col >= stmt->column_count || !stmt->current_row) {
        if (dimensions) *dimensions = 0;
        return nullptr;
    }

    value_t* v = &stmt->current_row[col];
    if (v->type == VAL_VECTOR) {
        if (dimensions) *dimensions = v->data.blob.len / sizeof(float);
        return (const float*)v->data.blob.data;
    }

    if (dimensions) *dimensions = 0;
    return nullptr;
}
