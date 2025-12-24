/*
 * SpeedDB - Basic Usage Example
 */

#include "speeddb.h"
#include <stdio.h>
#include <stdlib.h>

int main() {
    speeddb* db = nullptr;
    speeddb_stmt* stmt = nullptr;
    int rc;

    printf("SpeedDB Version: %s\n\n", SPEEDDB_VERSION);

    /* Open database */
    rc = speeddb_open("test.sdb", &db);
    if (rc != SPEEDDB_OK) {
        fprintf(stderr, "Failed to open database: %s\n", speeddb_errmsg(db));
        return 1;
    }
    printf("Database opened successfully.\n");

    /* Create table */
    rc = speeddb_exec(db,
        "CREATE TABLE users ("
        "  id INTEGER PRIMARY KEY,"
        "  name TEXT NOT NULL,"
        "  email TEXT UNIQUE,"
        "  age INTEGER"
        ")",
        nullptr, nullptr, nullptr);

    if (rc != SPEEDDB_OK) {
        fprintf(stderr, "Failed to create table: %s\n", speeddb_errmsg(db));
    } else {
        printf("Table created successfully.\n");
    }

    /* Insert data */
    rc = speeddb_exec(db,
        "INSERT INTO users (id, name, email, age) VALUES "
        "(1, 'Alice', 'alice@example.com', 30),"
        "(2, 'Bob', 'bob@example.com', 25),"
        "(3, 'Charlie', 'charlie@example.com', 35)",
        nullptr, nullptr, nullptr);

    if (rc != SPEEDDB_OK) {
        fprintf(stderr, "Failed to insert data: %s\n", speeddb_errmsg(db));
    } else {
        printf("Data inserted. Rows affected: %d\n", speeddb_changes(db));
    }

    /* Query with prepared statement */
    rc = speeddb_prepare(db,
        "SELECT id, name, email, age FROM users WHERE age > ?",
        -1, &stmt, nullptr);

    if (rc != SPEEDDB_OK) {
        fprintf(stderr, "Failed to prepare statement: %s\n", speeddb_errmsg(db));
    } else {
        /* Bind parameter */
        speeddb_bind_int(stmt, 1, 26);

        printf("\nUsers older than 26:\n");
        printf("%-4s %-12s %-25s %s\n", "ID", "Name", "Email", "Age");
        printf("%-4s %-12s %-25s %s\n", "----", "------------",
               "-------------------------", "---");

        /* Fetch results */
        while ((rc = speeddb_step(stmt)) == SPEEDDB_ROW) {
            int id = speeddb_column_int(stmt, 0);
            const char* name = (const char*)speeddb_column_text(stmt, 1);
            const char* email = (const char*)speeddb_column_text(stmt, 2);
            int age = speeddb_column_int(stmt, 3);

            printf("%-4d %-12s %-25s %d\n", id, name, email, age);
        }

        if (rc != SPEEDDB_DONE) {
            fprintf(stderr, "Error during fetch: %s\n", speeddb_errmsg(db));
        }

        speeddb_finalize(stmt);
    }

    /* Transaction example */
    printf("\nTransaction example:\n");

    speeddb_begin(db);

    rc = speeddb_exec(db,
        "UPDATE users SET age = age + 1 WHERE name = 'Alice'",
        nullptr, nullptr, nullptr);

    if (rc == SPEEDDB_OK) {
        speeddb_commit(db);
        printf("Transaction committed.\n");
    } else {
        speeddb_rollback(db);
        printf("Transaction rolled back.\n");
    }

    /* Close database */
    speeddb_close(db);
    printf("\nDatabase closed.\n");

    return 0;
}
