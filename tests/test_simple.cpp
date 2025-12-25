/* Simple test to debug :memory: database */
#include "speedsql.h"
#include "speedsql_internal.h"
#include <stdio.h>

int main() {
    printf("Testing :memory: database...\n");
    fflush(stdout);

    speedsql* db = nullptr;
    int rc = speedsql_open(":memory:", &db);
    printf("Open result: %d\n", rc);
    fflush(stdout);

    if (rc != SPEEDSQL_OK) {
        printf("Failed to open: %s\n", speedsql_errmsg(db));
        return 1;
    }

    printf("DB opened, buffer pool: %p\n", (void*)db->buffer_pool);
    printf("Buffer pool page_count: %zu, free_list: %p\n",
           db->buffer_pool->page_count,
           (void*)db->buffer_pool->free_list);
    fflush(stdout);

    printf("Testing CREATE TABLE...\n");
    fflush(stdout);

    rc = speedsql_exec(db, "CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT)",
        nullptr, nullptr, nullptr);
    printf("CREATE TABLE result: %d\n", rc);
    fflush(stdout);
    if (rc != SPEEDSQL_OK) {
        printf("Error: %s\n", speedsql_errmsg(db));
    }

    printf("Table count: %zu\n", db->table_count);
    if (db->table_count > 0) {
        printf("Table name: %s\n", db->tables[0].name);
        printf("Column count: %u\n", db->tables[0].column_count);
        printf("Data tree: %p\n", (void*)db->tables[0].data_tree);
    }
    fflush(stdout);

    /* Skip INSERT for now
    printf("Testing INSERT...\n");
    fflush(stdout);
    rc = speedsql_exec(db, "INSERT INTO test VALUES (1, 'Hello')",
        nullptr, nullptr, nullptr);
    printf("INSERT result: %d\n", rc);
    fflush(stdout);
    if (rc != SPEEDSQL_OK) {
        printf("Error: %s\n", speedsql_errmsg(db));
        fflush(stdout);
    }
    */

    printf("Database opened, closing...\n");
    fflush(stdout);

    printf("Testing buffer_pool_flush...\n");
    fflush(stdout);
    rc = buffer_pool_flush(db->buffer_pool, &db->db_file);
    printf("buffer_pool_flush result: %d\n", rc);
    fflush(stdout);

    printf("Testing buffer_pool_destroy...\n");
    fflush(stdout);
    buffer_pool_destroy(db->buffer_pool);
    printf("buffer_pool_destroy done\n");
    fflush(stdout);

    printf("Freeing buffer_pool...\n");
    fflush(stdout);
    sdb_free(db->buffer_pool);
    db->buffer_pool = nullptr;
    printf("buffer_pool freed\n");
    fflush(stdout);

    printf("Calling speedsql_close()...\n");
    fflush(stdout);
    rc = speedsql_close(db);
    printf("Close result: %d\n", rc);
    fflush(stdout);

    printf("Test completed.\n");
    return 0;
}
