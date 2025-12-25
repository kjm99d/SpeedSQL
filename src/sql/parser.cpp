/*
 * SpeedSQL - SQL Parser
 *
 * Recursive descent parser for SQL statements
 */

#include "speedsql_internal.h"

void parser_init(parser_t* parser, speedsql* db, const char* sql) {
    lexer_init(&parser->lexer, sql);
    parser->db = db;
    parser->error[0] = '\0';
    parser->had_error = false;
    parser->current = lexer_next(&parser->lexer);
    parser->previous = parser->current;
}

static void parser_error(parser_t* parser, const char* message) {
    if (parser->had_error) return;
    parser->had_error = true;
    snprintf(parser->error, sizeof(parser->error),
             "Line %d: %s", parser->current.line, message);
}

static void advance(parser_t* parser) {
    parser->previous = parser->current;
    parser->current = lexer_next(&parser->lexer);

    if (parser->current.type == TOK_ERROR) {
        parser_error(parser, parser->current.start);
    }
}

static bool check(parser_t* parser, token_type_t type) {
    return parser->current.type == type;
}

static bool match(parser_t* parser, token_type_t type) {
    if (!check(parser, type)) return false;
    advance(parser);
    return true;
}

static void consume(parser_t* parser, token_type_t type, const char* message) {
    if (parser->current.type == type) {
        advance(parser);
        return;
    }
    parser_error(parser, message);
}

/* Forward declarations */
static expr_t* parse_expression(parser_t* parser);
static expr_t* parse_or(parser_t* parser);

/* Helper to create identifier string */
static char* copy_identifier(token_t* token) {
    char* str = (char*)sdb_malloc(token->length + 1);
    if (str) {
        memcpy(str, token->start, token->length);
        str[token->length] = '\0';
    }
    return str;
}

/* Expression parsing */
static expr_t* create_expr(expr_type_t type) {
    expr_t* expr = (expr_t*)sdb_calloc(1, sizeof(expr_t));
    if (expr) expr->type = type;
    return expr;
}

static expr_t* parse_primary(parser_t* parser) {
    if (match(parser, TOK_INTEGER)) {
        expr_t* expr = create_expr(EXPR_LITERAL);
        if (expr) {
            expr->data.literal.type = SPEEDSQL_TYPE_INT;
            expr->data.literal.data.i = parser->previous.value.int_val;
        }
        return expr;
    }

    if (match(parser, TOK_FLOAT)) {
        expr_t* expr = create_expr(EXPR_LITERAL);
        if (expr) {
            expr->data.literal.type = SPEEDSQL_TYPE_FLOAT;
            expr->data.literal.data.f = parser->previous.value.float_val;
        }
        return expr;
    }

    if (match(parser, TOK_STRING)) {
        expr_t* expr = create_expr(EXPR_LITERAL);
        if (expr) {
            expr->data.literal.type = SPEEDSQL_TYPE_TEXT;
            /* Remove quotes */
            int len = parser->previous.length - 2;
            expr->data.literal.data.text.data = (char*)sdb_malloc(len + 1);
            if (expr->data.literal.data.text.data) {
                memcpy(expr->data.literal.data.text.data,
                       parser->previous.start + 1, len);
                expr->data.literal.data.text.data[len] = '\0';
                expr->data.literal.data.text.len = len;
            }
        }
        return expr;
    }

    if (match(parser, TOK_NULL)) {
        expr_t* expr = create_expr(EXPR_LITERAL);
        if (expr) {
            expr->data.literal.type = SPEEDSQL_TYPE_NULL;
        }
        return expr;
    }

    if (match(parser, TOK_PARAM)) {
        static int param_count = 0;
        expr_t* expr = create_expr(EXPR_PARAMETER);
        if (expr) {
            expr->data.param_index = param_count++;
        }
        return expr;
    }

    if (match(parser, TOK_IDENT)) {
        char* name = copy_identifier(&parser->previous);

        /* Check for function call */
        if (match(parser, TOK_LPAREN)) {
            expr_t* expr = create_expr(EXPR_FUNCTION);
            if (expr) {
                expr->data.function.name = name;
                expr->data.function.args = nullptr;
                expr->data.function.arg_count = 0;

                /* Parse arguments */
                if (!check(parser, TOK_RPAREN)) {
                    int capacity = 4;
                    expr->data.function.args = (expr_t**)sdb_malloc(
                        capacity * sizeof(expr_t*));

                    do {
                        if (expr->data.function.arg_count >= capacity) {
                            capacity *= 2;
                            expr->data.function.args = (expr_t**)sdb_realloc(
                                expr->data.function.args,
                                capacity * sizeof(expr_t*));
                        }
                        /* Special handling for COUNT(*) - the * is treated as a placeholder */
                        if (check(parser, TOK_STAR)) {
                            advance(parser);
                            expr->data.function.args[expr->data.function.arg_count++] = nullptr;
                        } else {
                            expr->data.function.args[expr->data.function.arg_count++] =
                                parse_expression(parser);
                        }
                    } while (match(parser, TOK_COMMA));
                }

                consume(parser, TOK_RPAREN, "Expected ')' after function arguments");
            }
            return expr;
        }

        /* Check for table.column */
        if (match(parser, TOK_DOT)) {
            consume(parser, TOK_IDENT, "Expected column name after '.'");
            char* column = copy_identifier(&parser->previous);

            expr_t* expr = create_expr(EXPR_COLUMN);
            if (expr) {
                expr->data.column_ref.table = name;
                expr->data.column_ref.column = column;
                expr->data.column_ref.index = -1;
            }
            return expr;
        }

        /* Simple column reference */
        expr_t* expr = create_expr(EXPR_COLUMN);
        if (expr) {
            expr->data.column_ref.table = nullptr;
            expr->data.column_ref.column = name;
            expr->data.column_ref.index = -1;
        }
        return expr;
    }

    if (match(parser, TOK_LPAREN)) {
        expr_t* expr = parse_expression(parser);
        consume(parser, TOK_RPAREN, "Expected ')' after expression");
        return expr;
    }

    parser_error(parser, "Expected expression");
    return nullptr;
}

static expr_t* parse_unary(parser_t* parser) {
    if (match(parser, TOK_MINUS) || match(parser, TOK_NOT)) {
        int op = parser->previous.type;
        expr_t* operand = parse_unary(parser);

        expr_t* expr = create_expr(EXPR_UNARY_OP);
        if (expr) {
            expr->data.unary.op = op;
            expr->data.unary.operand = operand;
        }
        return expr;
    }

    return parse_primary(parser);
}

static expr_t* parse_factor(parser_t* parser) {
    expr_t* left = parse_unary(parser);

    while (match(parser, TOK_STAR) || match(parser, TOK_SLASH) ||
           match(parser, TOK_PERCENT)) {
        int op = parser->previous.type;
        expr_t* right = parse_unary(parser);

        expr_t* expr = create_expr(EXPR_BINARY_OP);
        if (expr) {
            expr->data.binary.op = op;
            expr->data.binary.left = left;
            expr->data.binary.right = right;
        }
        left = expr;
    }

    return left;
}

static expr_t* parse_term(parser_t* parser) {
    expr_t* left = parse_factor(parser);

    while (match(parser, TOK_PLUS) || match(parser, TOK_MINUS)) {
        int op = parser->previous.type;
        expr_t* right = parse_factor(parser);

        expr_t* expr = create_expr(EXPR_BINARY_OP);
        if (expr) {
            expr->data.binary.op = op;
            expr->data.binary.left = left;
            expr->data.binary.right = right;
        }
        left = expr;
    }

    return left;
}

static expr_t* parse_comparison(parser_t* parser) {
    expr_t* left = parse_term(parser);

    while (match(parser, TOK_LT) || match(parser, TOK_LE) ||
           match(parser, TOK_GT) || match(parser, TOK_GE) ||
           match(parser, TOK_EQ) || match(parser, TOK_NE)) {
        int op = parser->previous.type;
        expr_t* right = parse_term(parser);

        expr_t* expr = create_expr(EXPR_BINARY_OP);
        if (expr) {
            expr->data.binary.op = op;
            expr->data.binary.left = left;
            expr->data.binary.right = right;
        }
        left = expr;
    }

    /* Handle IS NULL / IS NOT NULL */
    if (match(parser, TOK_IS)) {
        bool negate = match(parser, TOK_NOT);
        consume(parser, TOK_NULL, "Expected NULL after IS");

        expr_t* expr = create_expr(EXPR_UNARY_OP);
        if (expr) {
            expr->data.unary.op = negate ? TOK_NOT : TOK_NULL;
            expr->data.unary.operand = left;
        }
        return expr;
    }

    /* Handle LIKE */
    if (match(parser, TOK_LIKE)) {
        expr_t* right = parse_term(parser);
        expr_t* expr = create_expr(EXPR_BINARY_OP);
        if (expr) {
            expr->data.binary.op = TOK_LIKE;
            expr->data.binary.left = left;
            expr->data.binary.right = right;
        }
        return expr;
    }

    return left;
}

static expr_t* parse_and(parser_t* parser) {
    expr_t* left = parse_comparison(parser);

    while (match(parser, TOK_AND)) {
        expr_t* right = parse_comparison(parser);

        expr_t* expr = create_expr(EXPR_BINARY_OP);
        if (expr) {
            expr->data.binary.op = TOK_AND;
            expr->data.binary.left = left;
            expr->data.binary.right = right;
        }
        left = expr;
    }

    return left;
}

static expr_t* parse_or(parser_t* parser) {
    expr_t* left = parse_and(parser);

    while (match(parser, TOK_OR)) {
        expr_t* right = parse_and(parser);

        expr_t* expr = create_expr(EXPR_BINARY_OP);
        if (expr) {
            expr->data.binary.op = TOK_OR;
            expr->data.binary.left = left;
            expr->data.binary.right = right;
        }
        left = expr;
    }

    return left;
}

static expr_t* parse_expression(parser_t* parser) {
    return parse_or(parser);
}

/* Statement parsing */
static parsed_stmt_t* parse_select(parser_t* parser) {
    parsed_stmt_t* stmt = (parsed_stmt_t*)sdb_calloc(1, sizeof(parsed_stmt_t));
    if (!stmt) return nullptr;

    stmt->op = SQL_SELECT;
    stmt->limit = -1;
    stmt->offset = 0;

    /* Parse column list */
    int col_capacity = 8;
    stmt->columns = (select_col_t*)sdb_malloc(col_capacity * sizeof(select_col_t));

    do {
        if (stmt->column_count >= col_capacity) {
            col_capacity *= 2;
            stmt->columns = (select_col_t*)sdb_realloc(stmt->columns,
                col_capacity * sizeof(select_col_t));
        }

        select_col_t* col = &stmt->columns[stmt->column_count++];

        if (match(parser, TOK_STAR)) {
            col->expr = nullptr;  /* SELECT * */
            col->alias = nullptr;
        } else {
            col->expr = parse_expression(parser);
            col->alias = nullptr;

            if (match(parser, TOK_AS)) {
                consume(parser, TOK_IDENT, "Expected alias name");
                col->alias = copy_identifier(&parser->previous);
            } else if (check(parser, TOK_IDENT)) {
                /* Alias without AS */
                advance(parser);
                col->alias = copy_identifier(&parser->previous);
            }
        }
    } while (match(parser, TOK_COMMA));

    /* FROM clause */
    if (match(parser, TOK_FROM)) {
        int table_capacity = 4;
        stmt->tables = (table_ref_t*)sdb_malloc(table_capacity * sizeof(table_ref_t));

        /* First table */
        consume(parser, TOK_IDENT, "Expected table name");
        table_ref_t* tbl = &stmt->tables[stmt->table_count++];
        tbl->name = copy_identifier(&parser->previous);
        tbl->alias = nullptr;
        tbl->def = nullptr;

        if (match(parser, TOK_AS)) {
            consume(parser, TOK_IDENT, "Expected alias name");
            tbl->alias = copy_identifier(&parser->previous);
        } else if (check(parser, TOK_IDENT) && !check(parser, TOK_JOIN) &&
                   !check(parser, TOK_LEFT) && !check(parser, TOK_RIGHT) &&
                   !check(parser, TOK_INNER) && !check(parser, TOK_WHERE) &&
                   !check(parser, TOK_ORDER) && !check(parser, TOK_GROUP)) {
            advance(parser);
            tbl->alias = copy_identifier(&parser->previous);
        }

        /* Handle JOINs or comma-separated tables */
        int join_capacity = 4;
        stmt->joins = nullptr;
        stmt->join_count = 0;

        while (true) {
            join_type_t join_type = JOIN_INNER;
            bool has_join = false;

            if (match(parser, TOK_COMMA)) {
                /* Implicit cross join via comma */
                if (stmt->table_count >= table_capacity) {
                    table_capacity *= 2;
                    stmt->tables = (table_ref_t*)sdb_realloc(stmt->tables,
                        table_capacity * sizeof(table_ref_t));
                }

                consume(parser, TOK_IDENT, "Expected table name");
                table_ref_t* next_tbl = &stmt->tables[stmt->table_count++];
                next_tbl->name = copy_identifier(&parser->previous);
                next_tbl->alias = nullptr;
                next_tbl->def = nullptr;

                if (match(parser, TOK_AS)) {
                    consume(parser, TOK_IDENT, "Expected alias name");
                    next_tbl->alias = copy_identifier(&parser->previous);
                }
                continue;
            }

            if (match(parser, TOK_LEFT)) {
                match(parser, TOK_OUTER);  /* Optional OUTER */
                consume(parser, TOK_JOIN, "Expected JOIN after LEFT");
                join_type = JOIN_LEFT;
                has_join = true;
            } else if (match(parser, TOK_RIGHT)) {
                match(parser, TOK_OUTER);  /* Optional OUTER */
                consume(parser, TOK_JOIN, "Expected JOIN after RIGHT");
                join_type = JOIN_RIGHT;
                has_join = true;
            } else if (match(parser, TOK_INNER)) {
                consume(parser, TOK_JOIN, "Expected JOIN after INNER");
                join_type = JOIN_INNER;
                has_join = true;
            } else if (match(parser, TOK_JOIN)) {
                join_type = JOIN_INNER;
                has_join = true;
            }

            if (!has_join) break;

            /* Allocate joins array if needed */
            if (!stmt->joins) {
                stmt->joins = (join_clause_t*)sdb_malloc(join_capacity * sizeof(join_clause_t));
            } else if (stmt->join_count >= join_capacity) {
                join_capacity *= 2;
                stmt->joins = (join_clause_t*)sdb_realloc(stmt->joins,
                    join_capacity * sizeof(join_clause_t));
            }

            join_clause_t* jc = &stmt->joins[stmt->join_count++];
            jc->type = join_type;
            jc->on_condition = nullptr;

            consume(parser, TOK_IDENT, "Expected table name after JOIN");
            jc->table_name = copy_identifier(&parser->previous);
            jc->table_alias = nullptr;

            if (match(parser, TOK_AS)) {
                consume(parser, TOK_IDENT, "Expected alias name");
                jc->table_alias = copy_identifier(&parser->previous);
            } else if (check(parser, TOK_IDENT) && !check(parser, TOK_ON)) {
                advance(parser);
                jc->table_alias = copy_identifier(&parser->previous);
            }

            if (match(parser, TOK_ON)) {
                jc->on_condition = parse_expression(parser);
            }
        }
    }

    /* WHERE clause */
    if (match(parser, TOK_WHERE)) {
        stmt->where = parse_expression(parser);
    }

    /* GROUP BY clause */
    if (match(parser, TOK_GROUP)) {
        consume(parser, TOK_BY, "Expected BY after GROUP");

        int capacity = 4;
        stmt->group_by = (expr_t**)sdb_malloc(capacity * sizeof(expr_t*));

        do {
            if (stmt->group_by_count >= capacity) {
                capacity *= 2;
                stmt->group_by = (expr_t**)sdb_realloc(stmt->group_by,
                    capacity * sizeof(expr_t*));
            }
            stmt->group_by[stmt->group_by_count++] = parse_expression(parser);
        } while (match(parser, TOK_COMMA));
    }

    /* HAVING clause */
    if (match(parser, TOK_HAVING)) {
        stmt->having = parse_expression(parser);
    }

    /* ORDER BY clause */
    if (match(parser, TOK_ORDER)) {
        consume(parser, TOK_BY, "Expected BY after ORDER");

        int capacity = 4;
        stmt->order_by = (order_by_t*)sdb_malloc(capacity * sizeof(order_by_t));

        do {
            if (stmt->order_by_count >= capacity) {
                capacity *= 2;
                stmt->order_by = (order_by_t*)sdb_realloc(stmt->order_by,
                    capacity * sizeof(order_by_t));
            }

            order_by_t* ob = &stmt->order_by[stmt->order_by_count++];
            ob->expr = parse_expression(parser);
            ob->desc = false;

            if (match(parser, TOK_DESC)) {
                ob->desc = true;
            } else {
                match(parser, TOK_ASC);  /* Optional ASC */
            }
        } while (match(parser, TOK_COMMA));
    }

    /* LIMIT clause */
    if (match(parser, TOK_LIMIT)) {
        consume(parser, TOK_INTEGER, "Expected number after LIMIT");
        stmt->limit = parser->previous.value.int_val;

        /* OFFSET clause */
        if (match(parser, TOK_OFFSET)) {
            consume(parser, TOK_INTEGER, "Expected number after OFFSET");
            stmt->offset = parser->previous.value.int_val;
        }
    }

    return stmt;
}

static parsed_stmt_t* parse_insert(parser_t* parser) {
    parsed_stmt_t* stmt = (parsed_stmt_t*)sdb_calloc(1, sizeof(parsed_stmt_t));
    if (!stmt) return nullptr;

    stmt->op = SQL_INSERT;

    consume(parser, TOK_INTO, "Expected INTO after INSERT");
    consume(parser, TOK_IDENT, "Expected table name");

    /* Store table name in tables array */
    stmt->tables = (table_ref_t*)sdb_malloc(sizeof(table_ref_t));
    stmt->tables[0].name = copy_identifier(&parser->previous);
    stmt->tables[0].alias = nullptr;
    stmt->tables[0].def = nullptr;
    stmt->table_count = 1;

    /* Optional column list */
    if (match(parser, TOK_LPAREN)) {
        int capacity = 8;
        stmt->insert_columns = (char**)sdb_malloc(capacity * sizeof(char*));

        do {
            if (stmt->insert_column_count >= capacity) {
                capacity *= 2;
                stmt->insert_columns = (char**)sdb_realloc(stmt->insert_columns,
                    capacity * sizeof(char*));
            }
            consume(parser, TOK_IDENT, "Expected column name");
            stmt->insert_columns[stmt->insert_column_count++] =
                copy_identifier(&parser->previous);
        } while (match(parser, TOK_COMMA));

        consume(parser, TOK_RPAREN, "Expected ')' after column list");
    }

    consume(parser, TOK_VALUES, "Expected VALUES");

    /* Parse value lists */
    int row_capacity = 8;
    stmt->insert_values = (value_t**)sdb_malloc(row_capacity * sizeof(value_t*));

    do {
        if (stmt->insert_row_count >= row_capacity) {
            row_capacity *= 2;
            stmt->insert_values = (value_t**)sdb_realloc(stmt->insert_values,
                row_capacity * sizeof(value_t*));
        }

        consume(parser, TOK_LPAREN, "Expected '(' before values");

        int col_count = stmt->insert_column_count > 0 ?
            stmt->insert_column_count : 16;  /* Guess if no column list */
        value_t* row = (value_t*)sdb_calloc(col_count, sizeof(value_t));
        int value_idx = 0;

        do {
            expr_t* expr = parse_expression(parser);
            if (expr && expr->type == EXPR_LITERAL) {
                row[value_idx++] = expr->data.literal;
            }
            sdb_free(expr);
        } while (match(parser, TOK_COMMA));

        /* If no explicit column list was provided, set insert_column_count from values */
        if (stmt->insert_column_count == 0 && stmt->insert_row_count == 0) {
            stmt->insert_column_count = value_idx;
        }

        stmt->insert_values[stmt->insert_row_count++] = row;

        consume(parser, TOK_RPAREN, "Expected ')' after values");
    } while (match(parser, TOK_COMMA));

    return stmt;
}

static parsed_stmt_t* parse_update(parser_t* parser) {
    parsed_stmt_t* stmt = (parsed_stmt_t*)sdb_calloc(1, sizeof(parsed_stmt_t));
    if (!stmt) return nullptr;

    stmt->op = SQL_UPDATE;

    consume(parser, TOK_IDENT, "Expected table name");

    stmt->tables = (table_ref_t*)sdb_malloc(sizeof(table_ref_t));
    stmt->tables[0].name = copy_identifier(&parser->previous);
    stmt->tables[0].alias = nullptr;
    stmt->tables[0].def = nullptr;
    stmt->table_count = 1;

    consume(parser, TOK_SET, "Expected SET");

    /* Parse SET clauses */
    int capacity = 8;
    stmt->update_columns = (char**)sdb_malloc(capacity * sizeof(char*));
    stmt->update_exprs = (expr_t**)sdb_malloc(capacity * sizeof(expr_t*));

    do {
        if (stmt->update_count >= capacity) {
            capacity *= 2;
            stmt->update_columns = (char**)sdb_realloc(stmt->update_columns,
                capacity * sizeof(char*));
            stmt->update_exprs = (expr_t**)sdb_realloc(stmt->update_exprs,
                capacity * sizeof(expr_t*));
        }

        consume(parser, TOK_IDENT, "Expected column name");
        stmt->update_columns[stmt->update_count] = copy_identifier(&parser->previous);

        consume(parser, TOK_EQ, "Expected '=' after column name");

        stmt->update_exprs[stmt->update_count] = parse_expression(parser);
        stmt->update_count++;
    } while (match(parser, TOK_COMMA));

    /* WHERE clause */
    if (match(parser, TOK_WHERE)) {
        stmt->where = parse_expression(parser);
    }

    return stmt;
}

static parsed_stmt_t* parse_delete(parser_t* parser) {
    parsed_stmt_t* stmt = (parsed_stmt_t*)sdb_calloc(1, sizeof(parsed_stmt_t));
    if (!stmt) return nullptr;

    stmt->op = SQL_DELETE;

    consume(parser, TOK_FROM, "Expected FROM after DELETE");
    consume(parser, TOK_IDENT, "Expected table name");

    stmt->tables = (table_ref_t*)sdb_malloc(sizeof(table_ref_t));
    stmt->tables[0].name = copy_identifier(&parser->previous);
    stmt->tables[0].alias = nullptr;
    stmt->tables[0].def = nullptr;
    stmt->table_count = 1;

    /* WHERE clause */
    if (match(parser, TOK_WHERE)) {
        stmt->where = parse_expression(parser);
    }

    return stmt;
}

static parsed_stmt_t* parse_create_table(parser_t* parser) {
    parsed_stmt_t* stmt = (parsed_stmt_t*)sdb_calloc(1, sizeof(parsed_stmt_t));
    if (!stmt) return nullptr;

    stmt->op = SQL_CREATE_TABLE;

    consume(parser, TOK_IDENT, "Expected table name");

    stmt->new_table = (table_def_t*)sdb_calloc(1, sizeof(table_def_t));
    stmt->new_table->name = copy_identifier(&parser->previous);

    consume(parser, TOK_LPAREN, "Expected '(' after table name");

    /* Parse column definitions */
    int capacity = 16;
    stmt->new_table->columns = (column_def_t*)sdb_malloc(capacity * sizeof(column_def_t));

    do {
        if ((int)stmt->new_table->column_count >= capacity) {
            capacity *= 2;
            stmt->new_table->columns = (column_def_t*)sdb_realloc(
                stmt->new_table->columns, capacity * sizeof(column_def_t));
        }

        column_def_t* col = &stmt->new_table->columns[stmt->new_table->column_count++];
        memset(col, 0, sizeof(*col));

        consume(parser, TOK_IDENT, "Expected column name");
        col->name = copy_identifier(&parser->previous);

        /* Parse type */
        consume(parser, TOK_IDENT, "Expected column type");
        /* For now, store type as generic - later parse specific types */
        col->type = SPEEDSQL_TYPE_TEXT;  /* Default */

        /* Parse constraints */
        while (!check(parser, TOK_COMMA) && !check(parser, TOK_RPAREN)) {
            if (match(parser, TOK_PRIMARY)) {
                consume(parser, TOK_KEY, "Expected KEY after PRIMARY");
                col->flags |= COL_FLAG_PRIMARY_KEY;
            } else if (match(parser, TOK_NOT)) {
                consume(parser, TOK_NULL, "Expected NULL after NOT");
                col->flags |= COL_FLAG_NOT_NULL;
            } else if (match(parser, TOK_UNIQUE)) {
                col->flags |= COL_FLAG_UNIQUE;
            } else if (match(parser, TOK_DEFAULT)) {
                /* Skip default value for now */
                advance(parser);
            } else {
                break;
            }
        }
    } while (match(parser, TOK_COMMA));

    consume(parser, TOK_RPAREN, "Expected ')' after column definitions");

    return stmt;
}

/* CREATE INDEX [UNIQUE] index_name ON table_name (column1, column2, ...) */
static parsed_stmt_t* parse_create_index(parser_t* parser, bool is_unique) {
    parsed_stmt_t* stmt = (parsed_stmt_t*)sdb_calloc(1, sizeof(parsed_stmt_t));
    if (!stmt) return nullptr;

    stmt->op = SQL_CREATE_INDEX;
    stmt->new_index = (index_def_t*)sdb_calloc(1, sizeof(index_def_t));
    if (!stmt->new_index) {
        sdb_free(stmt);
        return nullptr;
    }

    if (is_unique) {
        stmt->new_index->flags |= IDX_FLAG_UNIQUE;
    }

    /* Parse index name */
    consume(parser, TOK_IDENT, "Expected index name");
    stmt->new_index->name = copy_identifier(&parser->previous);

    /* Parse ON table_name */
    consume(parser, TOK_ON, "Expected ON after index name");
    consume(parser, TOK_IDENT, "Expected table name");
    stmt->new_index->table_name = copy_identifier(&parser->previous);

    /* Parse column list */
    consume(parser, TOK_LPAREN, "Expected '(' after table name");

    int capacity = 8;
    stmt->new_index->column_indices = (uint32_t*)sdb_malloc(capacity * sizeof(uint32_t));

    /* For now, store column names temporarily and resolve indices later */
    /* We'll store the column count and use column_indices as placeholder */
    char** column_names = (char**)sdb_malloc(capacity * sizeof(char*));
    int col_count = 0;

    do {
        if (col_count >= capacity) {
            capacity *= 2;
            column_names = (char**)sdb_realloc(column_names, capacity * sizeof(char*));
            stmt->new_index->column_indices = (uint32_t*)sdb_realloc(
                stmt->new_index->column_indices, capacity * sizeof(uint32_t));
        }

        consume(parser, TOK_IDENT, "Expected column name");
        column_names[col_count] = copy_identifier(&parser->previous);

        /* Check for ASC/DESC (ignore for now) */
        if (match(parser, TOK_IDENT)) {
            /* Skip ASC/DESC keyword */
        }

        stmt->new_index->column_indices[col_count] = col_count;  /* Placeholder */
        col_count++;
    } while (match(parser, TOK_COMMA));

    stmt->new_index->column_count = col_count;

    /* Free temporary column names (in real impl, resolve to indices) */
    for (int i = 0; i < col_count; i++) {
        sdb_free(column_names[i]);
    }
    sdb_free(column_names);

    consume(parser, TOK_RPAREN, "Expected ')' after column list");

    return stmt;
}

parsed_stmt_t* parser_parse(parser_t* parser) {
    if (match(parser, TOK_SELECT)) {
        return parse_select(parser);
    }

    if (match(parser, TOK_INSERT)) {
        return parse_insert(parser);
    }

    if (match(parser, TOK_UPDATE)) {
        return parse_update(parser);
    }

    if (match(parser, TOK_DELETE)) {
        return parse_delete(parser);
    }

    if (match(parser, TOK_CREATE)) {
        if (match(parser, TOK_TABLE)) {
            return parse_create_table(parser);
        }
        if (match(parser, TOK_UNIQUE)) {
            consume(parser, TOK_INDEX, "Expected INDEX after UNIQUE");
            return parse_create_index(parser, true);
        }
        if (match(parser, TOK_INDEX)) {
            return parse_create_index(parser, false);
        }
        parser_error(parser, "Expected TABLE, INDEX, or UNIQUE INDEX after CREATE");
        return nullptr;
    }

    if (match(parser, TOK_BEGIN)) {
        match(parser, TOK_TRANSACTION);  /* Optional */
        parsed_stmt_t* stmt = (parsed_stmt_t*)sdb_calloc(1, sizeof(parsed_stmt_t));
        if (stmt) stmt->op = SQL_BEGIN;
        return stmt;
    }

    if (match(parser, TOK_COMMIT)) {
        parsed_stmt_t* stmt = (parsed_stmt_t*)sdb_calloc(1, sizeof(parsed_stmt_t));
        if (stmt) stmt->op = SQL_COMMIT;
        return stmt;
    }

    if (match(parser, TOK_ROLLBACK)) {
        /* Check for ROLLBACK TO SAVEPOINT name */
        if (match(parser, TOK_TO)) {
            match(parser, TOK_SAVEPOINT);  /* Optional SAVEPOINT keyword */
            consume(parser, TOK_IDENT, "Expected savepoint name");
            parsed_stmt_t* stmt = (parsed_stmt_t*)sdb_calloc(1, sizeof(parsed_stmt_t));
            if (stmt) {
                stmt->op = SQL_ROLLBACK_TO;
                stmt->savepoint_name = copy_identifier(&parser->previous);
            }
            return stmt;
        }
        parsed_stmt_t* stmt = (parsed_stmt_t*)sdb_calloc(1, sizeof(parsed_stmt_t));
        if (stmt) stmt->op = SQL_ROLLBACK;
        return stmt;
    }

    if (match(parser, TOK_SAVEPOINT)) {
        consume(parser, TOK_IDENT, "Expected savepoint name");
        parsed_stmt_t* stmt = (parsed_stmt_t*)sdb_calloc(1, sizeof(parsed_stmt_t));
        if (stmt) {
            stmt->op = SQL_SAVEPOINT;
            stmt->savepoint_name = copy_identifier(&parser->previous);
        }
        return stmt;
    }

    if (match(parser, TOK_RELEASE)) {
        match(parser, TOK_SAVEPOINT);  /* Optional SAVEPOINT keyword */
        consume(parser, TOK_IDENT, "Expected savepoint name");
        parsed_stmt_t* stmt = (parsed_stmt_t*)sdb_calloc(1, sizeof(parsed_stmt_t));
        if (stmt) {
            stmt->op = SQL_RELEASE;
            stmt->savepoint_name = copy_identifier(&parser->previous);
        }
        return stmt;
    }

    if (match(parser, TOK_DROP)) {
        if (match(parser, TOK_TABLE)) {
            /* DROP TABLE table_name */
            parsed_stmt_t* stmt = (parsed_stmt_t*)sdb_calloc(1, sizeof(parsed_stmt_t));
            if (!stmt) return nullptr;
            stmt->op = SQL_DROP_TABLE;

            consume(parser, TOK_IDENT, "Expected table name");
            stmt->tables = (table_ref_t*)sdb_malloc(sizeof(table_ref_t));
            if (stmt->tables) {
                stmt->tables[0].name = copy_identifier(&parser->previous);
                stmt->tables[0].alias = nullptr;
                stmt->tables[0].def = nullptr;
                stmt->table_count = 1;
            }
            return stmt;
        }
        if (match(parser, TOK_INDEX)) {
            /* DROP INDEX index_name */
            parsed_stmt_t* stmt = (parsed_stmt_t*)sdb_calloc(1, sizeof(parsed_stmt_t));
            if (!stmt) return nullptr;
            stmt->op = SQL_DROP_INDEX;

            consume(parser, TOK_IDENT, "Expected index name");
            stmt->new_index = (index_def_t*)sdb_calloc(1, sizeof(index_def_t));
            if (stmt->new_index) {
                stmt->new_index->name = copy_identifier(&parser->previous);
            }
            return stmt;
        }
        parser_error(parser, "Expected TABLE or INDEX after DROP");
        return nullptr;
    }

    parser_error(parser, "Expected SQL statement");
    return nullptr;
}

/* Free expression tree */
static void expr_free(expr_t* expr) {
    if (!expr) return;

    switch (expr->type) {
        case EXPR_LITERAL:
            if (expr->data.literal.type == SPEEDSQL_TYPE_TEXT) {
                sdb_free(expr->data.literal.data.text.data);
            } else if (expr->data.literal.type == SPEEDSQL_TYPE_BLOB) {
                sdb_free(expr->data.literal.data.blob.data);
            }
            break;
        case EXPR_COLUMN:
            sdb_free(expr->data.column_ref.table);
            sdb_free(expr->data.column_ref.column);
            break;
        case EXPR_BINARY_OP:
            expr_free(expr->data.binary.left);
            expr_free(expr->data.binary.right);
            break;
        case EXPR_UNARY_OP:
            expr_free(expr->data.unary.operand);
            break;
        case EXPR_FUNCTION:
            sdb_free(expr->data.function.name);
            for (int i = 0; i < expr->data.function.arg_count; i++) {
                expr_free(expr->data.function.args[i]);
            }
            sdb_free(expr->data.function.args);
            break;
        default:
            break;
    }

    sdb_free(expr);
}

void parsed_stmt_free(parsed_stmt_t* stmt) {
    if (!stmt) return;

    /* Free columns */
    if (stmt->columns) {
        for (int i = 0; i < stmt->column_count; i++) {
            expr_free(stmt->columns[i].expr);
            sdb_free(stmt->columns[i].alias);
        }
        sdb_free(stmt->columns);
    }

    /* Free tables */
    if (stmt->tables) {
        for (int i = 0; i < stmt->table_count; i++) {
            sdb_free(stmt->tables[i].name);
            sdb_free(stmt->tables[i].alias);
        }
        sdb_free(stmt->tables);
    }

    /* Free joins */
    if (stmt->joins) {
        for (int i = 0; i < stmt->join_count; i++) {
            sdb_free(stmt->joins[i].table_name);
            sdb_free(stmt->joins[i].table_alias);
            expr_free(stmt->joins[i].on_condition);
        }
        sdb_free(stmt->joins);
    }

    expr_free(stmt->where);

    if (stmt->group_by) {
        for (int i = 0; i < stmt->group_by_count; i++) {
            expr_free(stmt->group_by[i]);
        }
        sdb_free(stmt->group_by);
    }

    expr_free(stmt->having);

    if (stmt->order_by) {
        for (int i = 0; i < stmt->order_by_count; i++) {
            expr_free(stmt->order_by[i].expr);
        }
        sdb_free(stmt->order_by);
    }

    if (stmt->insert_columns) {
        for (int i = 0; i < stmt->insert_column_count; i++) {
            sdb_free(stmt->insert_columns[i]);
        }
        sdb_free(stmt->insert_columns);
    }

    if (stmt->insert_values) {
        for (int i = 0; i < stmt->insert_row_count; i++) {
            sdb_free(stmt->insert_values[i]);
        }
        sdb_free(stmt->insert_values);
    }

    if (stmt->update_columns) {
        for (int i = 0; i < stmt->update_count; i++) {
            sdb_free(stmt->update_columns[i]);
            expr_free(stmt->update_exprs[i]);
        }
        sdb_free(stmt->update_columns);
        sdb_free(stmt->update_exprs);
    }

    if (stmt->new_table) {
        sdb_free(stmt->new_table->name);
        if (stmt->new_table->columns) {
            for (uint32_t i = 0; i < stmt->new_table->column_count; i++) {
                sdb_free(stmt->new_table->columns[i].name);
                sdb_free(stmt->new_table->columns[i].default_value);
                sdb_free(stmt->new_table->columns[i].collation);
            }
            sdb_free(stmt->new_table->columns);
        }
        sdb_free(stmt->new_table);
    }

    if (stmt->new_index) {
        sdb_free(stmt->new_index->name);
        sdb_free(stmt->new_index->table_name);
        sdb_free(stmt->new_index->column_indices);
        sdb_free(stmt->new_index);
    }

    /* Free savepoint name */
    sdb_free(stmt->savepoint_name);

    sdb_free(stmt);
}
