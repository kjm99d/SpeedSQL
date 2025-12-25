/*
 * SpeedSQL - Query Executor
 *
 * Executes parsed SQL statements and manages prepared statements
 */

#include "speedsql_internal.h"
#include <stdarg.h>

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
        tbl->columns[i].default_value = def->columns[i].default_value;
    }

    /* Create B+Tree for table data */
    tbl->data_tree = (struct btree*)sdb_malloc(sizeof(btree_t));
    if (!tbl->data_tree) return SPEEDSQL_NOMEM;

    int rc = btree_create((btree_t*)tbl->data_tree, db->buffer_pool, &db->db_file, nullptr);
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

    /* Table scan */
    btree_cursor_t* cursor = &stmt->plan->data.scan.cursor;

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
                /* Project columns */
                for (int i = 0; i < stmt->column_count && i < col_count; i++) {
                    value_free(&stmt->current_row[i]);

                    if (p->columns[i].expr) {
                        /* Store row for expr eval */
                        value_t* temp = (value_t*)sdb_malloc(col_count * sizeof(value_t));
                        if (temp) {
                            memcpy(temp, row_vals, col_count * sizeof(value_t));
                            value_t* saved = stmt->current_row;
                            stmt->current_row = temp;
                            eval_expr(stmt, p->columns[i].expr, &stmt->current_row[i]);
                            stmt->current_row = saved;
                            value_copy(&stmt->current_row[i], &temp[i]);
                            sdb_free(temp);
                        }
                    } else if (i < col_count) {
                        value_copy(&stmt->current_row[i], &row_vals[i]);
                    }
                }

                value_free(&key);
                value_free(&value);

                /* Advance cursor for next call */
                btree_cursor_next(cursor);

                stmt->has_row = true;
                stmt->step_count++;

                /* Check LIMIT */
                if (p->limit > 0 && stmt->step_count > p->limit) {
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

        case SQL_CREATE_TABLE:
            if (!stmt->executed) {
                stmt->executed = true;
                rc = execute_create_table(stmt);
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
