/*
 * SpeedSQL - Basic Usage Example
 */

#include "speedsql.h"
#include <stdio.h>
#include <stdlib.h>

int main() {
    speedsql* db = nullptr;
    speedsql_stmt* stmt = nullptr;
    int rc;

    printf("SpeedSQL Version: %s\n\n", SPEEDSQL_VERSION);

    /* Open database */
    rc = speedsql_open("test.sdb", &db);
    if (rc != SPEEDSQL_OK) {
        fprintf(stderr, "Failed to open database: %s\n", speedsql_errmsg(db));
        return 1;
    }
    printf("Database opened successfully.\n");

    /* Create table */
    rc = speedsql_exec(db,
        "CREATE TABLE users ("
        "  id INTEGER PRIMARY KEY,"
        "  name TEXT NOT NULL,"
        "  email TEXT UNIQUE,"
        "  age INTEGER"
        ")",
        nullptr, nullptr, nullptr);

    if (rc != SPEEDSQL_OK) {
        fprintf(stderr, "Failed to create table: %s\n", speedsql_errmsg(db));
    } else {
        printf("Table created successfully.\n");
    }

    /* Insert data */
    rc = speedsql_exec(db,
        "INSERT INTO users (id, name, email, age) VALUES "
        "(1, 'Alice', 'alice@example.com', 30),"
        "(2, 'Bob', 'bob@example.com', 25),"
        "(3, 'Charlie', 'charlie@example.com', 35)",
        nullptr, nullptr, nullptr);

    if (rc != SPEEDSQL_OK) {
        fprintf(stderr, "Failed to insert data: %s\n", speedsql_errmsg(db));
    } else {
        printf("Data inserted. Rows affected: %d\n", speedsql_changes(db));
    }

    /* Query with prepared statement */
    rc = speedsql_prepare(db,
        "SELECT id, name, email, age FROM users WHERE age > ?",
        -1, &stmt, nullptr);

    if (rc != SPEEDSQL_OK) {
        fprintf(stderr, "Failed to prepare statement: %s\n", speedsql_errmsg(db));
    } else {
        /* Bind parameter */
        speedsql_bind_int(stmt, 1, 26);

        printf("\nUsers older than 26:\n");
        printf("%-4s %-12s %-25s %s\n", "ID", "Name", "Email", "Age");
        printf("%-4s %-12s %-25s %s\n", "----", "------------",
               "-------------------------", "---");

        /* Fetch results */
        while ((rc = speedsql_step(stmt)) == SPEEDSQL_ROW) {
            int id = speedsql_column_int(stmt, 0);
            const char* name = (const char*)speedsql_column_text(stmt, 1);
            const char* email = (const char*)speedsql_column_text(stmt, 2);
            int age = speedsql_column_int(stmt, 3);

            printf("%-4d %-12s %-25s %d\n", id, name, email, age);
        }

        if (rc != SPEEDSQL_DONE) {
            fprintf(stderr, "Error during fetch: %s\n", speedsql_errmsg(db));
        }

        speedsql_finalize(stmt);
    }

    /* Transaction example */
    printf("\nTransaction example:\n");

    speedsql_begin(db);

    rc = speedsql_exec(db,
        "UPDATE users SET age = age + 1 WHERE name = 'Alice'",
        nullptr, nullptr, nullptr);

    if (rc == SPEEDSQL_OK) {
        speedsql_commit(db);
        printf("Transaction committed.\n");
    } else {
        speedsql_rollback(db);
        printf("Transaction rolled back.\n");
    }

    /* Close database */
    speedsql_close(db);
    printf("\nDatabase closed.\n");

    return 0;
}
