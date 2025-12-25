/*
 * SpeedSQL - Write-Ahead Logging (WAL) Stub
 *
 * Placeholder implementation for v0.2 roadmap
 */

#include "speedsql_internal.h"

/* WAL stub functions - to be implemented in v0.2 */

int wal_init(wal_t* wal, const char* path) {
    (void)wal;
    (void)path;
    /* WAL not yet implemented - return success to allow operation */
    return SPEEDSQL_OK;
}

void wal_close(wal_t* wal) {
    (void)wal;
    /* WAL not yet implemented */
}

int wal_commit(wal_t* wal, uint64_t txn_id) {
    (void)wal;
    (void)txn_id;
    /* WAL not yet implemented - return success */
    return SPEEDSQL_OK;
}

int wal_rollback(wal_t* wal, uint64_t txn_id) {
    (void)wal;
    (void)txn_id;
    /* WAL not yet implemented - return success */
    return SPEEDSQL_OK;
}
