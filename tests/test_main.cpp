/*
 * SpeedSQL - Test Suite
 */

#include "speedsql.h"
#include "speedsql_internal.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>

#define TEST(name) static void test_##name()
#define RUN_TEST(name) do { \
    printf("Running %s... ", #name); \
    test_##name(); \
    printf("PASSED\n"); \
    passed++; \
} while(0)

static int passed = 0;
static int failed = 0;

/* Test helper macros */
#define ASSERT_EQ(a, b) do { \
    if ((a) != (b)) { \
        printf("FAILED at line %d: %s != %s\n", __LINE__, #a, #b); \
        failed++; \
        return; \
    } \
} while(0)

#define ASSERT_NE(a, b) do { \
    if ((a) == (b)) { \
        printf("FAILED at line %d: %s == %s\n", __LINE__, #a, #b); \
        failed++; \
        return; \
    } \
} while(0)

#define ASSERT_TRUE(x) ASSERT_NE((x), 0)
#define ASSERT_FALSE(x) ASSERT_EQ((x), 0)

#define ASSERT_STR_EQ(a, b) do { \
    if (strcmp((a), (b)) != 0) { \
        printf("FAILED at line %d: '%s' != '%s'\n", __LINE__, (a), (b)); \
        failed++; \
        return; \
    } \
} while(0)

/* ============================================================================
 * Value Tests
 * ============================================================================ */

TEST(value_null) {
    value_t v;
    value_init_null(&v);
    ASSERT_EQ(v.type, VAL_NULL);
    value_free(&v);
}

TEST(value_int) {
    value_t v;
    value_init_int(&v, 42);
    ASSERT_EQ(v.type, VAL_INT);
    ASSERT_EQ(v.data.i, 42);
    value_free(&v);
}

TEST(value_float) {
    value_t v;
    value_init_float(&v, 3.14159);
    ASSERT_EQ(v.type, VAL_FLOAT);
    ASSERT_TRUE(v.data.f > 3.14 && v.data.f < 3.15);
    value_free(&v);
}

TEST(value_text) {
    value_t v;
    value_init_text(&v, "Hello, SpeedSQL!", -1);
    ASSERT_EQ(v.type, VAL_TEXT);
    ASSERT_STR_EQ(v.data.text.data, "Hello, SpeedSQL!");
    ASSERT_EQ(v.data.text.len, 16u);
    value_free(&v);
}

TEST(value_copy) {
    value_t src, dst;
    value_init_text(&src, "Copy test", -1);
    value_copy(&dst, &src);

    ASSERT_EQ(dst.type, VAL_TEXT);
    ASSERT_STR_EQ(dst.data.text.data, "Copy test");
    ASSERT_NE(dst.data.text.data, src.data.text.data);  /* Deep copy */

    value_free(&src);
    value_free(&dst);
}

TEST(value_compare_int) {
    value_t a, b;
    value_init_int(&a, 10);
    value_init_int(&b, 20);

    ASSERT_TRUE(value_compare(&a, &b) < 0);
    ASSERT_TRUE(value_compare(&b, &a) > 0);

    value_free(&b);
    value_init_int(&b, 10);
    ASSERT_EQ(value_compare(&a, &b), 0);

    value_free(&a);
    value_free(&b);
}

TEST(value_compare_text) {
    value_t a, b;
    value_init_text(&a, "apple", -1);
    value_init_text(&b, "banana", -1);

    ASSERT_TRUE(value_compare(&a, &b) < 0);
    ASSERT_TRUE(value_compare(&b, &a) > 0);

    value_free(&a);
    value_free(&b);
}

/* ============================================================================
 * Hash Tests
 * ============================================================================ */

TEST(crc32_basic) {
    const char* data = "Hello, World!";
    uint32_t hash1 = crc32(data, strlen(data));
    uint32_t hash2 = crc32(data, strlen(data));
    ASSERT_EQ(hash1, hash2);  /* Deterministic */

    const char* data2 = "Hello, World?";
    uint32_t hash3 = crc32(data2, strlen(data2));
    ASSERT_NE(hash1, hash3);  /* Different input, different hash */
}

TEST(xxhash64_basic) {
    const char* data = "SpeedSQL test data";
    uint64_t hash1 = xxhash64(data, strlen(data));
    uint64_t hash2 = xxhash64(data, strlen(data));
    ASSERT_EQ(hash1, hash2);

    const char* data2 = "SpeedSQL test datb";
    uint64_t hash3 = xxhash64(data2, strlen(data2));
    ASSERT_NE(hash1, hash3);
}

/* ============================================================================
 * Lexer Tests
 * ============================================================================ */

TEST(lexer_select) {
    lexer_t lexer;
    lexer_init(&lexer, "SELECT * FROM users WHERE id = 1");

    token_t tok = lexer_next(&lexer);
    ASSERT_EQ(tok.type, TOK_SELECT);

    tok = lexer_next(&lexer);
    ASSERT_EQ(tok.type, TOK_STAR);

    tok = lexer_next(&lexer);
    ASSERT_EQ(tok.type, TOK_FROM);

    tok = lexer_next(&lexer);
    ASSERT_EQ(tok.type, TOK_IDENT);

    tok = lexer_next(&lexer);
    ASSERT_EQ(tok.type, TOK_WHERE);

    tok = lexer_next(&lexer);
    ASSERT_EQ(tok.type, TOK_IDENT);

    tok = lexer_next(&lexer);
    ASSERT_EQ(tok.type, TOK_EQ);

    tok = lexer_next(&lexer);
    ASSERT_EQ(tok.type, TOK_INTEGER);
    ASSERT_EQ(tok.value.int_val, 1);

    tok = lexer_next(&lexer);
    ASSERT_EQ(tok.type, TOK_EOF);
}

TEST(lexer_string) {
    lexer_t lexer;
    lexer_init(&lexer, "'hello world'");

    token_t tok = lexer_next(&lexer);
    ASSERT_EQ(tok.type, TOK_STRING);
    ASSERT_EQ(tok.length, 13);  /* Including quotes */
}

TEST(lexer_numbers) {
    lexer_t lexer;
    lexer_init(&lexer, "42 3.14 1e10");

    token_t tok = lexer_next(&lexer);
    ASSERT_EQ(tok.type, TOK_INTEGER);
    ASSERT_EQ(tok.value.int_val, 42);

    tok = lexer_next(&lexer);
    ASSERT_EQ(tok.type, TOK_FLOAT);
    ASSERT_TRUE(tok.value.float_val > 3.13 && tok.value.float_val < 3.15);

    tok = lexer_next(&lexer);
    ASSERT_EQ(tok.type, TOK_FLOAT);
}

TEST(lexer_operators) {
    lexer_t lexer;
    lexer_init(&lexer, "< <= > >= = != <>");

    ASSERT_EQ(lexer_next(&lexer).type, TOK_LT);
    ASSERT_EQ(lexer_next(&lexer).type, TOK_LE);
    ASSERT_EQ(lexer_next(&lexer).type, TOK_GT);
    ASSERT_EQ(lexer_next(&lexer).type, TOK_GE);
    ASSERT_EQ(lexer_next(&lexer).type, TOK_EQ);
    ASSERT_EQ(lexer_next(&lexer).type, TOK_NE);
    ASSERT_EQ(lexer_next(&lexer).type, TOK_NE);
}

/* ============================================================================
 * Parser Tests
 * ============================================================================ */

TEST(parser_select_simple) {
    parser_t parser;
    parser_init(&parser, nullptr, "SELECT * FROM users");

    parsed_stmt_t* stmt = parser_parse(&parser);
    ASSERT_NE(stmt, nullptr);
    ASSERT_EQ(stmt->op, SQL_SELECT);
    ASSERT_EQ(stmt->table_count, 1);
    ASSERT_STR_EQ(stmt->tables[0].name, "users");

    parsed_stmt_free(stmt);
}

TEST(parser_select_columns) {
    parser_t parser;
    parser_init(&parser, nullptr, "SELECT id, name, age FROM users");

    parsed_stmt_t* stmt = parser_parse(&parser);
    ASSERT_NE(stmt, nullptr);
    ASSERT_EQ(stmt->op, SQL_SELECT);
    ASSERT_EQ(stmt->column_count, 3);

    parsed_stmt_free(stmt);
}

TEST(parser_select_where) {
    parser_t parser;
    parser_init(&parser, nullptr, "SELECT * FROM users WHERE age > 18");

    parsed_stmt_t* stmt = parser_parse(&parser);
    ASSERT_NE(stmt, nullptr);
    ASSERT_EQ(stmt->op, SQL_SELECT);
    ASSERT_NE(stmt->where, nullptr);

    parsed_stmt_free(stmt);
}

TEST(parser_insert) {
    parser_t parser;
    parser_init(&parser, nullptr,
        "INSERT INTO users (name, age) VALUES ('Alice', 30)");

    parsed_stmt_t* stmt = parser_parse(&parser);
    ASSERT_NE(stmt, nullptr);
    ASSERT_EQ(stmt->op, SQL_INSERT);
    ASSERT_EQ(stmt->insert_column_count, 2);
    ASSERT_EQ(stmt->insert_row_count, 1);

    parsed_stmt_free(stmt);
}

TEST(parser_update) {
    parser_t parser;
    parser_init(&parser, nullptr,
        "UPDATE users SET age = 31 WHERE name = 'Alice'");

    parsed_stmt_t* stmt = parser_parse(&parser);
    ASSERT_NE(stmt, nullptr);
    ASSERT_EQ(stmt->op, SQL_UPDATE);
    ASSERT_EQ(stmt->update_count, 1);
    ASSERT_NE(stmt->where, nullptr);

    parsed_stmt_free(stmt);
}

TEST(parser_delete) {
    parser_t parser;
    parser_init(&parser, nullptr, "DELETE FROM users WHERE id = 1");

    parsed_stmt_t* stmt = parser_parse(&parser);
    ASSERT_NE(stmt, nullptr);
    ASSERT_EQ(stmt->op, SQL_DELETE);
    ASSERT_NE(stmt->where, nullptr);

    parsed_stmt_free(stmt);
}

TEST(parser_create_table) {
    parser_t parser;
    parser_init(&parser, nullptr,
        "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)");

    parsed_stmt_t* stmt = parser_parse(&parser);
    ASSERT_NE(stmt, nullptr);
    ASSERT_EQ(stmt->op, SQL_CREATE_TABLE);
    ASSERT_NE(stmt->new_table, nullptr);
    ASSERT_STR_EQ(stmt->new_table->name, "users");
    ASSERT_EQ(stmt->new_table->column_count, 2u);

    parsed_stmt_free(stmt);
}

TEST(parser_create_index) {
    parser_t parser;
    parser_init(&parser, nullptr,
        "CREATE INDEX idx_name ON users (name)");

    parsed_stmt_t* stmt = parser_parse(&parser);
    ASSERT_NE(stmt, nullptr);
    ASSERT_EQ(stmt->op, SQL_CREATE_INDEX);
    ASSERT_NE(stmt->new_index, nullptr);
    ASSERT_STR_EQ(stmt->new_index->name, "idx_name");
    ASSERT_STR_EQ(stmt->new_index->table_name, "users");

    parsed_stmt_free(stmt);
}

TEST(parser_create_unique_index) {
    parser_t parser;
    parser_init(&parser, nullptr,
        "CREATE UNIQUE INDEX idx_email ON users (email)");

    parsed_stmt_t* stmt = parser_parse(&parser);
    ASSERT_NE(stmt, nullptr);
    ASSERT_EQ(stmt->op, SQL_CREATE_INDEX);
    ASSERT_NE(stmt->new_index, nullptr);
    ASSERT_TRUE(stmt->new_index->flags & IDX_FLAG_UNIQUE);

    parsed_stmt_free(stmt);
}

/* ============================================================================
 * Database API Tests
 * ============================================================================ */

TEST(db_open_close) {
    speedsql* db = nullptr;
    int rc = speedsql_open(":memory:", &db);
    ASSERT_EQ(rc, SPEEDSQL_OK);
    ASSERT_NE(db, nullptr);

    rc = speedsql_close(db);
    ASSERT_EQ(rc, SPEEDSQL_OK);
}

TEST(db_exec_create_table) {
    speedsql* db = nullptr;
    speedsql_open(":memory:", &db);

    int rc = speedsql_exec(db,
        "CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT)",
        nullptr, nullptr, nullptr);
    ASSERT_EQ(rc, SPEEDSQL_OK);

    speedsql_close(db);
}

TEST(db_exec_insert_select) {
    speedsql* db = nullptr;
    speedsql_open(":memory:", &db);

    speedsql_exec(db, "CREATE TABLE test (id INTEGER, name TEXT)",
        nullptr, nullptr, nullptr);

    int rc = speedsql_exec(db, "INSERT INTO test VALUES (1, 'Alice')",
        nullptr, nullptr, nullptr);
    ASSERT_EQ(rc, SPEEDSQL_OK);

    rc = speedsql_exec(db, "INSERT INTO test VALUES (2, 'Bob')",
        nullptr, nullptr, nullptr);
    ASSERT_EQ(rc, SPEEDSQL_OK);

    speedsql_close(db);
}

TEST(db_prepared_stmt) {
    speedsql* db = nullptr;
    speedsql_open(":memory:", &db);

    speedsql_exec(db, "CREATE TABLE test (id INTEGER, name TEXT)",
        nullptr, nullptr, nullptr);
    speedsql_exec(db, "INSERT INTO test VALUES (1, 'Alice')",
        nullptr, nullptr, nullptr);
    speedsql_exec(db, "INSERT INTO test VALUES (2, 'Bob')",
        nullptr, nullptr, nullptr);

    speedsql_stmt* stmt = nullptr;
    int rc = speedsql_prepare(db, "SELECT * FROM test", -1, &stmt, nullptr);
    ASSERT_EQ(rc, SPEEDSQL_OK);
    ASSERT_NE(stmt, nullptr);

    int count = 0;
    while (speedsql_step(stmt) == SPEEDSQL_ROW) {
        count++;
    }
    ASSERT_EQ(count, 2);

    speedsql_finalize(stmt);
    speedsql_close(db);
}

TEST(db_transaction) {
    speedsql* db = nullptr;
    speedsql_open(":memory:", &db);

    speedsql_exec(db, "CREATE TABLE test (id INTEGER)",
        nullptr, nullptr, nullptr);

    int rc = speedsql_begin(db);
    ASSERT_EQ(rc, SPEEDSQL_OK);

    speedsql_exec(db, "INSERT INTO test VALUES (1)",
        nullptr, nullptr, nullptr);

    rc = speedsql_commit(db);
    ASSERT_EQ(rc, SPEEDSQL_OK);

    speedsql_close(db);
}

/* ============================================================================
 * Encryption Tests
 * ============================================================================ */

TEST(crypto_status) {
    speedsql* db = nullptr;
    speedsql_open(":memory:", &db);

    speedsql_cipher_t cipher;
    bool encrypted = true;
    int rc = speedsql_crypto_status(db, &cipher, &encrypted);
    ASSERT_EQ(rc, SPEEDSQL_OK);
    ASSERT_FALSE(encrypted);

    speedsql_close(db);
}

/* ============================================================================
 * Main
 * ============================================================================ */

int main() {
    printf("SpeedSQL Test Suite\n");
    printf("===================\n\n");

    /* Value tests */
    printf("Value Tests:\n");
    RUN_TEST(value_null);
    RUN_TEST(value_int);
    RUN_TEST(value_float);
    RUN_TEST(value_text);
    RUN_TEST(value_copy);
    RUN_TEST(value_compare_int);
    RUN_TEST(value_compare_text);

    /* Hash tests */
    printf("\nHash Tests:\n");
    RUN_TEST(crc32_basic);
    RUN_TEST(xxhash64_basic);

    /* Lexer tests */
    printf("\nLexer Tests:\n");
    RUN_TEST(lexer_select);
    RUN_TEST(lexer_string);
    RUN_TEST(lexer_numbers);
    RUN_TEST(lexer_operators);

    /* Parser tests */
    printf("\nParser Tests:\n");
    RUN_TEST(parser_select_simple);
    RUN_TEST(parser_select_columns);
    RUN_TEST(parser_select_where);
    RUN_TEST(parser_insert);
    RUN_TEST(parser_update);
    RUN_TEST(parser_delete);
    RUN_TEST(parser_create_table);
    RUN_TEST(parser_create_index);
    RUN_TEST(parser_create_unique_index);

    /* Database API tests */
    printf("\nDatabase API Tests:\n");
    RUN_TEST(db_open_close);
    RUN_TEST(db_exec_create_table);
    RUN_TEST(db_exec_insert_select);
    RUN_TEST(db_prepared_stmt);
    RUN_TEST(db_transaction);

    /* Encryption tests */
    printf("\nEncryption Tests:\n");
    RUN_TEST(crypto_status);

    printf("\n===================\n");
    printf("Results: %d passed, %d failed\n", passed, failed);

    return failed > 0 ? 1 : 0;
}
