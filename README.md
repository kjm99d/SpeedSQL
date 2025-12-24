# SpeedDB

Ultra-fast file-based local database engine designed to overcome SQLite's limitations.

## Features

### Core Features
- **High Performance**: 16KB page size (vs SQLite's 4KB), optimized buffer pool with LRU eviction
- **Large Data Support**: 64-bit page addressing for TB-scale databases
- **Concurrent Access**: Read-write locks for better multi-threaded performance
- **SQL Compatible**: Standard SQL syntax support (SELECT, INSERT, UPDATE, DELETE, CREATE TABLE)
- **ACID Transactions**: Write-Ahead Logging (WAL) for crash recovery

### Modern Features
- **JSON Support**: Native JSON data type with path queries
- **Vector Search**: Built-in vector similarity search for AI/ML applications
- **Full-Text Search**: Integrated FTS capabilities

## SQLite Limitations Addressed

| Limitation | SQLite | SpeedDB |
|------------|--------|---------|
| Page Size | 4KB fixed | 16KB default, configurable |
| Max File Size | 281TB (theoretical) | Unlimited (64-bit addressing) |
| Concurrent Writes | Single writer | Optimized locking |
| Cache Size | Limited | 256MB default, up to 8GB |
| Modern Types | Limited | JSON, Vector native support |

## Building

### Requirements
- CMake 3.16+
- C++17 compatible compiler
- Windows: MSVC 2019+ or MinGW
- Linux/macOS: GCC 8+ or Clang 10+

### Build Commands

```bash
# Create build directory
mkdir build && cd build

# Configure
cmake ..

# Build
cmake --build . --config Release

# Run tests
ctest -C Release
```

### Build Options

```bash
cmake .. \
    -DSPEEDDB_BUILD_SHARED=ON \
    -DSPEEDDB_BUILD_STATIC=ON \
    -DSPEEDDB_BUILD_TESTS=ON \
    -DSPEEDDB_BUILD_EXAMPLES=ON \
    -DSPEEDDB_BUILD_BENCHMARK=OFF
```

## Usage

### C API

```c
#include "speeddb.h"

int main() {
    speeddb* db;

    // Open database
    speeddb_open("mydata.sdb", &db);

    // Create table
    speeddb_exec(db,
        "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)",
        NULL, NULL, NULL);

    // Insert data
    speeddb_exec(db,
        "INSERT INTO users VALUES (1, 'Alice', 30)",
        NULL, NULL, NULL);

    // Query with prepared statement
    speeddb_stmt* stmt;
    speeddb_prepare(db, "SELECT * FROM users WHERE age > ?", -1, &stmt, NULL);
    speeddb_bind_int(stmt, 1, 25);

    while (speeddb_step(stmt) == SPEEDDB_ROW) {
        int id = speeddb_column_int(stmt, 0);
        const char* name = (const char*)speeddb_column_text(stmt, 1);
        printf("User: %d - %s\n", id, name);
    }

    speeddb_finalize(stmt);
    speeddb_close(db);

    return 0;
}
```

### Transactions

```c
speeddb_begin(db);

int rc = speeddb_exec(db, "UPDATE accounts SET balance = balance - 100 WHERE id = 1", NULL, NULL, NULL);
if (rc == SPEEDDB_OK) {
    rc = speeddb_exec(db, "UPDATE accounts SET balance = balance + 100 WHERE id = 2", NULL, NULL, NULL);
}

if (rc == SPEEDDB_OK) {
    speeddb_commit(db);
} else {
    speeddb_rollback(db);
}
```

## Architecture

```
SpeedDB/
├── include/
│   ├── speeddb.h           # Public C API
│   ├── speeddb_types.h     # Type definitions
│   └── speeddb_internal.h  # Internal structures
├── src/
│   ├── core/
│   │   └── database.cpp    # Connection management
│   ├── storage/
│   │   ├── file_io.cpp     # Cross-platform file I/O
│   │   └── buffer_pool.cpp # Page cache (LRU)
│   ├── index/
│   │   └── btree.cpp       # B+Tree implementation
│   ├── sql/
│   │   ├── lexer.cpp       # SQL tokenizer
│   │   └── parser.cpp      # SQL parser
│   └── util/
│       ├── hash.cpp        # CRC32, xxHash64
│       └── value.cpp       # Value operations
├── tests/
├── examples/
└── benchmark/
```

## Design Principles (SOLID)

- **Single Responsibility**: Each module handles one concern (storage, indexing, parsing)
- **Open/Closed**: Extensible through interfaces (compare functions, hash functions)
- **Liskov Substitution**: Value types are interchangeable where appropriate
- **Interface Segregation**: Separate public API from internal interfaces
- **Dependency Inversion**: High-level modules depend on abstractions

## Performance Targets

- **Point Query**: < 10 microseconds
- **Range Scan**: > 1M rows/second
- **Bulk Insert**: > 100K rows/second
- **Concurrent Reads**: Linear scaling with threads

## Roadmap

### v0.1 (Current)
- [x] Project structure
- [x] File I/O layer
- [x] Buffer pool / page cache
- [x] B+Tree index (basic)
- [x] SQL lexer
- [x] SQL parser
- [ ] Query executor
- [ ] Schema management

### v0.2
- [ ] B+Tree page splits
- [ ] WAL implementation
- [ ] Transaction support
- [ ] Multi-table joins

### v0.3
- [ ] Query optimizer
- [ ] Secondary indexes
- [ ] JSON functions
- [ ] Aggregate functions

### v1.0
- [ ] Vector search
- [ ] Full-text search
- [ ] Compression
- [ ] Encryption

## License

MIT License

## Contributing

Contributions are welcome! Please read our contributing guidelines before submitting PRs.
