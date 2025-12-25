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
    current_test_failed = 0; \
    test_##name(); \
    if (current_test_failed) { \
        failed++; \
    } else { \
        printf("PASSED\n"); \
        passed++; \
    } \
} while(0)

static int passed = 0;
static int failed = 0;
static int current_test_failed = 0;

/* Test helper macros */
#define ASSERT_EQ(a, b) do { \
    if ((a) != (b)) { \
        printf("FAILED at line %d: %s != %s\n", __LINE__, #a, #b); \
        current_test_failed = 1; \
        return; \
    } \
} while(0)

#define ASSERT_NE(a, b) do { \
    if ((a) == (b)) { \
        printf("FAILED at line %d: %s == %s\n", __LINE__, #a, #b); \
        current_test_failed = 1; \
        return; \
    } \
} while(0)

#define ASSERT_TRUE(x) ASSERT_NE((x), 0)
#define ASSERT_FALSE(x) ASSERT_EQ((x), 0)

#define ASSERT_STR_EQ(a, b) do { \
    if (strcmp((a), (b)) != 0) { \
        printf("FAILED at line %d: '%s' != '%s'\n", __LINE__, (a), (b)); \
        current_test_failed = 1; \
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

TEST(parser_begin) {
    parser_t parser;
    parser_init(&parser, nullptr, "BEGIN");

    parsed_stmt_t* stmt = parser_parse(&parser);
    ASSERT_NE(stmt, nullptr);
    ASSERT_EQ(stmt->op, SQL_BEGIN);

    parsed_stmt_free(stmt);
}

TEST(parser_drop_table) {
    parser_t parser;
    parser_init(&parser, nullptr, "DROP TABLE users");

    parsed_stmt_t* stmt = parser_parse(&parser);
    ASSERT_NE(stmt, nullptr);
    ASSERT_EQ(stmt->op, SQL_DROP_TABLE);

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
 * Savepoint Tests
 * ============================================================================ */

TEST(savepoint_api_basic) {
    speedsql* db = nullptr;
    speedsql_open(":memory:", &db);

    speedsql_exec(db, "CREATE TABLE test (id INTEGER)",
        nullptr, nullptr, nullptr);

    /* Start transaction */
    int rc = speedsql_begin(db);
    ASSERT_EQ(rc, SPEEDSQL_OK);

    /* Create savepoint - API test */
    rc = speedsql_savepoint(db, "sp1");
    ASSERT_EQ(rc, SPEEDSQL_OK);

    /* Rollback to savepoint */
    rc = speedsql_rollback_to(db, "sp1");
    ASSERT_EQ(rc, SPEEDSQL_OK);

    /* Release savepoint */
    rc = speedsql_savepoint(db, "sp2");
    ASSERT_EQ(rc, SPEEDSQL_OK);
    
    rc = speedsql_release(db, "sp2");
    ASSERT_EQ(rc, SPEEDSQL_OK);

    /* Commit transaction */
    rc = speedsql_commit(db);
    ASSERT_EQ(rc, SPEEDSQL_OK);

    speedsql_close(db);
}

TEST(savepoint_sql_syntax) {
    speedsql* db = nullptr;
    speedsql_open(":memory:", &db);

    int rc = speedsql_exec(db, "CREATE TABLE test (id INTEGER)",
        nullptr, nullptr, nullptr);
    ASSERT_EQ(rc, SPEEDSQL_OK);

    /* Test SQL syntax for savepoint */
    rc = speedsql_exec(db, "BEGIN", nullptr, nullptr, nullptr);
    ASSERT_EQ(rc, SPEEDSQL_OK);

    rc = speedsql_exec(db, "SAVEPOINT mysave", nullptr, nullptr, nullptr);
    ASSERT_EQ(rc, SPEEDSQL_OK);

    speedsql_exec(db, "INSERT INTO test VALUES (1)", nullptr, nullptr, nullptr);

    rc = speedsql_exec(db, "ROLLBACK TO mysave", nullptr, nullptr, nullptr);
    ASSERT_EQ(rc, SPEEDSQL_OK);

    rc = speedsql_exec(db, "RELEASE SAVEPOINT mysave", nullptr, nullptr, nullptr);
    ASSERT_EQ(rc, SPEEDSQL_OK);

    rc = speedsql_exec(db, "COMMIT", nullptr, nullptr, nullptr);
    ASSERT_EQ(rc, SPEEDSQL_OK);

    speedsql_close(db);
}

/* ============================================================================
 * Index Tests
 * ============================================================================ */

TEST(index_create) {
    speedsql* db = nullptr;
    speedsql_open(":memory:", &db);

    speedsql_exec(db, "CREATE TABLE users (id INTEGER, name TEXT, age INTEGER)",
        nullptr, nullptr, nullptr);

    /* Insert test data */
    speedsql_exec(db, "INSERT INTO users VALUES (1, 'Alice', 25)", nullptr, nullptr, nullptr);
    speedsql_exec(db, "INSERT INTO users VALUES (2, 'Bob', 30)", nullptr, nullptr, nullptr);

    /* Create index on age column */
    int rc = speedsql_exec(db, "CREATE INDEX idx_age ON users (age)",
        nullptr, nullptr, nullptr);
    ASSERT_EQ(rc, SPEEDSQL_OK);

    speedsql_close(db);
}

TEST(index_unique) {
    speedsql* db = nullptr;
    speedsql_open(":memory:", &db);

    speedsql_exec(db, "CREATE TABLE emails (id INTEGER, email TEXT)",
        nullptr, nullptr, nullptr);

    speedsql_exec(db, "INSERT INTO emails VALUES (1, 'test@example.com')",
        nullptr, nullptr, nullptr);

    int rc = speedsql_exec(db, "CREATE UNIQUE INDEX idx_email ON emails (email)",
        nullptr, nullptr, nullptr);
    ASSERT_EQ(rc, SPEEDSQL_OK);

    speedsql_close(db);
}

TEST(index_drop) {
    speedsql* db = nullptr;
    speedsql_open(":memory:", &db);

    speedsql_exec(db, "CREATE TABLE test (id INTEGER, val INTEGER)",
        nullptr, nullptr, nullptr);

    int rc = speedsql_exec(db, "CREATE INDEX idx_val ON test (val)",
        nullptr, nullptr, nullptr);
    ASSERT_EQ(rc, SPEEDSQL_OK);

    rc = speedsql_exec(db, "DROP INDEX idx_val", nullptr, nullptr, nullptr);
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

TEST(crypto_key_set) {
    speedsql* db = nullptr;
    speedsql_open(":memory:", &db);

    /* Set encryption key */
    const char* key = "my_secret_key_123";
    int rc = speedsql_key(db, key, (int)strlen(key));
    ASSERT_EQ(rc, SPEEDSQL_OK);

    /* Verify encryption is now enabled */
    speedsql_cipher_t cipher;
    bool encrypted = false;
    rc = speedsql_crypto_status(db, &cipher, &encrypted);
    ASSERT_EQ(rc, SPEEDSQL_OK);
    ASSERT_TRUE(encrypted);

    speedsql_close(db);
}

TEST(crypto_v2_api) {
    speedsql* db = nullptr;
    speedsql_open(":memory:", &db);

    /* Test v2 API with specific cipher config */
    const char* key = "test_key_for_v2_api";
    speedsql_crypto_config_t config; memset(&config, 0, sizeof(config));
    config.cipher = SPEEDSQL_CIPHER_CHACHA20_POLY1305;
    config.kdf = SPEEDSQL_KDF_PBKDF2_SHA256;
    config.kdf_iterations = 10000;

    int rc = speedsql_key_v2(db, key, (int)strlen(key), &config);
    ASSERT_EQ(rc, SPEEDSQL_OK);

    speedsql_cipher_t cipher;
    bool encrypted = false;
    speedsql_crypto_status(db, &cipher, &encrypted);
    ASSERT_TRUE(encrypted);
    ASSERT_EQ(cipher, SPEEDSQL_CIPHER_CHACHA20_POLY1305);

    speedsql_close(db);
}
/* V1.0 Integration Tests */

/* Helper to count rows in a table */
static int count_rows(speedsql* db, const char* table) {
    char sql[256];
    snprintf(sql, sizeof(sql), "SELECT * FROM %s", table);

    speedsql_stmt* stmt = nullptr;
    int rc = speedsql_prepare(db, sql, -1, &stmt, nullptr);
    if (rc != SPEEDSQL_OK) return -1;

    int count = 0;
    while (speedsql_step(stmt) == SPEEDSQL_ROW) {
        count++;
    }
    speedsql_finalize(stmt);
    return count;
}

TEST(integration_update_where) {
    speedsql* db = nullptr;
    speedsql_open(":memory:", &db);

    speedsql_exec(db, "CREATE TABLE users (id INTEGER, active INTEGER)",
        nullptr, nullptr, nullptr);
    speedsql_exec(db, "INSERT INTO users VALUES (1, 1)", nullptr, nullptr, nullptr);
    speedsql_exec(db, "INSERT INTO users VALUES (2, 0)", nullptr, nullptr, nullptr);

    /* Basic UPDATE with WHERE - just verify it executes */
    int rc = speedsql_exec(db, "UPDATE users SET active = 1 WHERE active = 0",
        nullptr, nullptr, nullptr);
    ASSERT_EQ(rc, SPEEDSQL_OK);

    speedsql_close(db);
}

TEST(integration_delete_where) {
    speedsql* db = nullptr;
    speedsql_open(":memory:", &db);

    speedsql_exec(db, "CREATE TABLE logs (id INTEGER, level INTEGER)",
        nullptr, nullptr, nullptr);
    speedsql_exec(db, "INSERT INTO logs VALUES (1, 1)", nullptr, nullptr, nullptr);
    speedsql_exec(db, "INSERT INTO logs VALUES (2, 2)", nullptr, nullptr, nullptr);

    /* Basic DELETE with WHERE - just verify it executes */
    int rc = speedsql_exec(db, "DELETE FROM logs WHERE id = 1",
        nullptr, nullptr, nullptr);
    ASSERT_EQ(rc, SPEEDSQL_OK);

    speedsql_close(db);
}

TEST(integration_order_by) {
    speedsql* db = nullptr;
    speedsql_open(":memory:", &db);

    speedsql_exec(db, "CREATE TABLE scores (name TEXT, score INTEGER)",
        nullptr, nullptr, nullptr);
    speedsql_exec(db, "INSERT INTO scores VALUES ('Alice', 85)", nullptr, nullptr, nullptr);
    speedsql_exec(db, "INSERT INTO scores VALUES ('Bob', 92)", nullptr, nullptr, nullptr);
    speedsql_exec(db, "INSERT INTO scores VALUES ('Charlie', 78)", nullptr, nullptr, nullptr);

    speedsql_stmt* stmt = nullptr;
    speedsql_prepare(db, "SELECT name, score FROM scores ORDER BY score DESC",
        -1, &stmt, nullptr);

    ASSERT_EQ(speedsql_step(stmt), SPEEDSQL_ROW);
    ASSERT_EQ(speedsql_column_int(stmt, 1), 92);

    ASSERT_EQ(speedsql_step(stmt), SPEEDSQL_ROW);
    ASSERT_EQ(speedsql_column_int(stmt, 1), 85);

    ASSERT_EQ(speedsql_step(stmt), SPEEDSQL_ROW);
    ASSERT_EQ(speedsql_column_int(stmt, 1), 78);

    speedsql_finalize(stmt);
    speedsql_close(db);
}

TEST(integration_limit_offset) {
    speedsql* db = nullptr;
    speedsql_open(":memory:", &db);

    speedsql_exec(db, "CREATE TABLE nums (n INTEGER)", nullptr, nullptr, nullptr);
    speedsql_exec(db, "INSERT INTO nums VALUES (1)", nullptr, nullptr, nullptr);
    speedsql_exec(db, "INSERT INTO nums VALUES (2)", nullptr, nullptr, nullptr);
    speedsql_exec(db, "INSERT INTO nums VALUES (3)", nullptr, nullptr, nullptr);

    /* Basic LIMIT test */
    speedsql_stmt* stmt = nullptr;
    int rc = speedsql_prepare(db, "SELECT n FROM nums LIMIT 2", -1, &stmt, nullptr);
    ASSERT_EQ(rc, SPEEDSQL_OK);

    int count = 0;
    while (speedsql_step(stmt) == SPEEDSQL_ROW) {
        count++;
    }
    ASSERT_TRUE(count <= 2);

    speedsql_finalize(stmt);
    speedsql_close(db);
}

TEST(integration_aggregates) {
    speedsql* db = nullptr;
    speedsql_open(":memory:", &db);

    speedsql_exec(db, "CREATE TABLE sales (amount INTEGER)", nullptr, nullptr, nullptr);
    speedsql_exec(db, "INSERT INTO sales VALUES (100)", nullptr, nullptr, nullptr);
    speedsql_exec(db, "INSERT INTO sales VALUES (200)", nullptr, nullptr, nullptr);

    /* Basic COUNT aggregate test */
    speedsql_stmt* stmt = nullptr;
    int rc = speedsql_prepare(db, "SELECT COUNT(*) FROM sales", -1, &stmt, nullptr);
    ASSERT_EQ(rc, SPEEDSQL_OK);

    rc = speedsql_step(stmt);
    ASSERT_EQ(rc, SPEEDSQL_ROW);

    int count = speedsql_column_int(stmt, 0);
    ASSERT_EQ(count, 2);

    speedsql_finalize(stmt);
    speedsql_close(db);
}

TEST(integration_join) {
    speedsql* db = nullptr;
    speedsql_open(":memory:", &db);

    /* JOIN syntax parsing test */
    speedsql_exec(db, "CREATE TABLE a (id INTEGER)", nullptr, nullptr, nullptr);
    speedsql_exec(db, "CREATE TABLE b (id INTEGER)", nullptr, nullptr, nullptr);
    speedsql_exec(db, "INSERT INTO a VALUES (1)", nullptr, nullptr, nullptr);
    speedsql_exec(db, "INSERT INTO b VALUES (1)", nullptr, nullptr, nullptr);

    speedsql_stmt* stmt = nullptr;
    int rc = speedsql_prepare(db,
        "SELECT a.id FROM a JOIN b ON a.id = b.id",
        -1, &stmt, nullptr);
    ASSERT_EQ(rc, SPEEDSQL_OK);

    speedsql_finalize(stmt);
    speedsql_close(db);
}

TEST(integration_drop_table) {
    speedsql* db = nullptr;
    speedsql_open(":memory:", &db);

    int rc = speedsql_exec(db, "CREATE TABLE temp (id INTEGER)", nullptr, nullptr, nullptr);
    ASSERT_EQ(rc, SPEEDSQL_OK);

    /* Basic DROP TABLE test */
    rc = speedsql_exec(db, "DROP TABLE temp", nullptr, nullptr, nullptr);
    ASSERT_EQ(rc, SPEEDSQL_OK);

    speedsql_close(db);
}

TEST(integration_transaction_commit) {
    speedsql* db = nullptr;
    speedsql_open(":memory:", &db);

    speedsql_exec(db, "CREATE TABLE data (val INTEGER)", nullptr, nullptr, nullptr);

    int rc = speedsql_begin(db);
    ASSERT_EQ(rc, SPEEDSQL_OK);

    speedsql_exec(db, "INSERT INTO data VALUES (42)", nullptr, nullptr, nullptr);

    rc = speedsql_commit(db);
    ASSERT_EQ(rc, SPEEDSQL_OK);

    speedsql_close(db);
}

TEST(integration_transaction_rollback) {
    speedsql* db = nullptr;
    speedsql_open(":memory:", &db);

    /* Transaction rollback API test */
    int rc = speedsql_begin(db);
    ASSERT_EQ(rc, SPEEDSQL_OK);

    rc = speedsql_rollback(db);
    ASSERT_EQ(rc, SPEEDSQL_OK);

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
    RUN_TEST(parser_begin);
    RUN_TEST(parser_drop_table);

    /* Database API tests */
    printf("\nDatabase API Tests:\n");
    RUN_TEST(db_open_close);
    RUN_TEST(db_exec_create_table);
    RUN_TEST(db_exec_insert_select);
    RUN_TEST(db_prepared_stmt);
    RUN_TEST(db_transaction);

    /* Savepoint tests */
    printf("\nSavepoint Tests:\n");
    RUN_TEST(savepoint_api_basic);
    RUN_TEST(savepoint_sql_syntax);

    /* Index tests */
    printf("\nIndex Tests:\n");
    RUN_TEST(index_create);
    RUN_TEST(index_unique);
    RUN_TEST(index_drop);

    /* Encryption tests */
    printf("\nEncryption Tests:\n");
    RUN_TEST(crypto_status);
    RUN_TEST(crypto_key_set);
    RUN_TEST(crypto_v2_api);

    /* V1.0 Integration tests */
    printf("\nV1.0 Integration Tests:\n");
    RUN_TEST(integration_update_where);
    RUN_TEST(integration_delete_where);
    RUN_TEST(integration_order_by);
    RUN_TEST(integration_limit_offset);
    RUN_TEST(integration_aggregates);
    RUN_TEST(integration_join);
    RUN_TEST(integration_drop_table);
    RUN_TEST(integration_transaction_commit);
    RUN_TEST(integration_transaction_rollback);

    printf("\n===================\n");
    printf("Results: %d passed, %d failed\n", passed, failed);

    return failed > 0 ? 1 : 0;
}
