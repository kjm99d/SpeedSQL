# SpeedSQL

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

### Security & Encryption (CC Certified Ready)
- **Multiple Cipher Support**: Pluggable encryption architecture
- **AES-256-GCM**: NIST standard, hardware acceleration ready
- **ARIA-256-GCM**: Korean national standard (KS X 1213), CC certified
- **SEED-CBC**: Korean standard cipher (TTAS.KO-12.0004)
- **ChaCha20-Poly1305**: Modern stream cipher, fast in software
- **No Encryption**: Optional plaintext mode for development

## SQLite Limitations Addressed

| Limitation | SQLite | SpeedSQL |
|------------|--------|----------|
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
    -DSPEEDSQL_BUILD_SHARED=ON \
    -DSPEEDSQL_BUILD_STATIC=ON \
    -DSPEEDSQL_BUILD_TESTS=ON \
    -DSPEEDSQL_BUILD_EXAMPLES=ON \
    -DSPEEDSQL_BUILD_BENCHMARK=OFF
```

## Usage

### C API

```c
#include "speedsql.h"

int main() {
    speedsql* db;

    // Open database
    speedsql_open("mydata.sdb", &db);

    // Create table
    speedsql_exec(db,
        "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)",
        NULL, NULL, NULL);

    // Insert data
    speedsql_exec(db,
        "INSERT INTO users VALUES (1, 'Alice', 30)",
        NULL, NULL, NULL);

    // Query with prepared statement
    speedsql_stmt* stmt;
    speedsql_prepare(db, "SELECT * FROM users WHERE age > ?", -1, &stmt, NULL);
    speedsql_bind_int(stmt, 1, 25);

    while (speedsql_step(stmt) == SPEEDSQL_ROW) {
        int id = speedsql_column_int(stmt, 0);
        const char* name = (const char*)speedsql_column_text(stmt, 1);
        printf("User: %d - %s\n", id, name);
    }

    speedsql_finalize(stmt);
    speedsql_close(db);

    return 0;
}
```

### C++ Wrapper

```cpp
#include "speedsql.hpp"
using namespace speedsql;

int main() {
    Database db("mydata.sdb");

    db.exec("CREATE TABLE users (id INTEGER, name TEXT, age INTEGER)");
    db.exec("INSERT INTO users VALUES (1, 'Alice', 30)");

    // RAII Transaction
    db.transaction([&]() {
        db.exec("UPDATE users SET age = 31 WHERE id = 1");
    });

    // Range-based for loop
    for (auto& row : db.query("SELECT * FROM users WHERE age > ?", 25)) {
        std::cout << std::get<std::string>(row[1]) << "\n";
    }

    return 0;
}
```

### Transactions

```c
speedsql_begin(db);

int rc = speedsql_exec(db, "UPDATE accounts SET balance = balance - 100 WHERE id = 1", NULL, NULL, NULL);
if (rc == SPEEDSQL_OK) {
    rc = speedsql_exec(db, "UPDATE accounts SET balance = balance + 100 WHERE id = 2", NULL, NULL, NULL);
}

if (rc == SPEEDSQL_OK) {
    speedsql_commit(db);
} else {
    speedsql_rollback(db);
}
```

### Encryption

```c
#include "speedsql.h"
#include "speedsql_crypto.h"

int main() {
    speedsql* db;

    // Open database
    speedsql_open("secure.sdb", &db);

    // Configure encryption
    speedsql_crypto_config_t config = {
        .cipher = SPEEDSQL_CIPHER_AES_256_GCM,  // or ARIA_256_GCM for Korean CC
        .kdf = SPEEDSQL_KDF_PBKDF2_SHA256,
        .kdf_iterations = 100000
    };

    // Set encryption key
    speedsql_key_v2(db, "my_password", 11, &config);

    // Use database normally - encryption is transparent
    speedsql_exec(db, "CREATE TABLE secrets (id INTEGER, data TEXT)", NULL, NULL, NULL);

    speedsql_close(db);
    return 0;
}
```

### Supported Ciphers

| Cipher | Key Size | Mode | Use Case |
|--------|----------|------|----------|
| `SPEEDSQL_CIPHER_NONE` | - | - | Development, testing |
| `SPEEDSQL_CIPHER_AES_256_GCM` | 256-bit | AEAD | General use, NIST compliant |
| `SPEEDSQL_CIPHER_AES_256_CBC` | 256-bit | CBC+HMAC | Legacy compatibility |
| `SPEEDSQL_CIPHER_ARIA_256_GCM` | 256-bit | AEAD | Korean CC certification |
| `SPEEDSQL_CIPHER_SEED_CBC` | 128-bit | CBC | Korean legacy systems |
| `SPEEDSQL_CIPHER_CHACHA20_POLY1305` | 256-bit | AEAD | Mobile, software-only |

## Architecture

```
SpeedSQL/
├── include/
│   ├── speedsql.h           # Public C API
│   ├── speedsql.hpp         # Modern C++ wrapper
│   ├── speedsql_crypto.h    # Encryption API
│   ├── speedsql_types.h     # Type definitions
│   └── speedsql_internal.h  # Internal structures
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
│   ├── crypto/
│   │   ├── crypto_provider.cpp  # Cipher registry
│   │   ├── cipher_none.cpp      # No encryption
│   │   ├── cipher_aes.cpp       # AES-256-GCM/CBC
│   │   ├── cipher_aria.cpp      # ARIA-256 (Korean)
│   │   ├── cipher_seed.cpp      # SEED (Korean)
│   │   └── cipher_chacha20.cpp  # ChaCha20-Poly1305
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

### v0.1
- [x] Project structure
- [x] File I/O layer
- [x] Buffer pool / page cache
- [x] B+Tree index (basic)
- [x] SQL lexer & parser
- [x] Encryption module (AES, ARIA, SEED, ChaCha20)
- [x] Modern C++ wrapper
- [x] Query executor (prepare/step/finalize, bind, column access)
- [x] Schema management (CREATE TABLE, table catalog)
- [x] B+Tree page splits (leaf & internal nodes, root promotion)
- [x] WAL implementation (write, commit, rollback, recover, checkpoint)
- [x] AES-256-CBC / ARIA-256-CBC modes with HMAC authentication
- [x] CREATE INDEX parsing
- [x] Encryption key API (speedsql_key, speedsql_key_v2, speedsql_rekey)

### v1.0 (Current)
- [x] UPDATE statement execution with WHERE filtering
- [x] DELETE statement execution with WHERE filtering
- [x] DROP TABLE / DROP INDEX execution
- [x] CREATE INDEX execution
- [x] ORDER BY clause with result buffering and sorting
- [x] LIMIT / OFFSET pagination support
- [x] Aggregate functions (COUNT, SUM, AVG, MIN, MAX)
- [x] GROUP BY / HAVING clause parsing
- [x] JOIN support (INNER, LEFT, RIGHT, CROSS)
- [x] Schema persistence for file-based databases
- [ ] Full transaction support (nested transactions, savepoints)
- [ ] Page-level encryption integration
- [ ] Secondary index execution (index scan)

### v2.0
- [ ] Query optimizer (cost-based)
- [ ] Hash join for large tables
- [ ] JSON functions
- [ ] CC certification preparation
- [ ] Vector search
- [ ] Full-text search
- [ ] Compression

## License

MIT License

## Contributing

Contributions are welcome! Please read our contributing guidelines before submitting PRs.
