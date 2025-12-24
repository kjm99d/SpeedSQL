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

### Security & Encryption (CC Certified Ready)
- **Multiple Cipher Support**: Pluggable encryption architecture
- **AES-256-GCM**: NIST standard, hardware acceleration ready
- **ARIA-256-GCM**: Korean national standard (KS X 1213), CC certified
- **SEED-CBC**: Korean standard cipher (TTAS.KO-12.0004)
- **ChaCha20-Poly1305**: Modern stream cipher, fast in software
- **No Encryption**: Optional plaintext mode for development

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

### Encryption

```c
#include "speeddb.h"
#include "speeddb_crypto.h"

int main() {
    speeddb* db;

    // Open database
    speeddb_open("secure.sdb", &db);

    // Configure encryption
    speeddb_crypto_config_t config = {
        .cipher = SPEEDDB_CIPHER_AES_256_GCM,  // or ARIA_256_GCM for Korean CC
        .kdf = SPEEDDB_KDF_PBKDF2_SHA256,
        .kdf_iterations = 100000
    };

    // Set encryption key
    speeddb_key_v2(db, "my_password", 11, &config);

    // Use database normally - encryption is transparent
    speeddb_exec(db, "CREATE TABLE secrets (id INTEGER, data TEXT)", NULL, NULL, NULL);

    speeddb_close(db);
    return 0;
}
```

### Supported Ciphers

| Cipher | Key Size | Mode | Use Case |
|--------|----------|------|----------|
| `SPEEDDB_CIPHER_NONE` | - | - | Development, testing |
| `SPEEDDB_CIPHER_AES_256_GCM` | 256-bit | AEAD | General use, NIST compliant |
| `SPEEDDB_CIPHER_AES_256_CBC` | 256-bit | CBC+HMAC | Legacy compatibility |
| `SPEEDDB_CIPHER_ARIA_256_GCM` | 256-bit | AEAD | Korean CC certification |
| `SPEEDDB_CIPHER_SEED_CBC` | 128-bit | CBC | Korean legacy systems |
| `SPEEDDB_CIPHER_CHACHA20_POLY1305` | 256-bit | AEAD | Mobile, software-only |

## Architecture

```
SpeedDB/
├── include/
│   ├── speeddb.h           # Public C API
│   ├── speeddb_crypto.h    # Encryption API
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

### v0.1 (Current)
- [x] Project structure
- [x] File I/O layer
- [x] Buffer pool / page cache
- [x] B+Tree index (basic)
- [x] SQL lexer & parser
- [x] Encryption module (AES, ARIA, SEED, ChaCha20)
- [ ] Query executor
- [ ] Schema management

### v0.2
- [ ] B+Tree page splits
- [ ] WAL implementation
- [ ] Full transaction support
- [ ] Page-level encryption integration

### v0.3
- [ ] Query optimizer
- [ ] Multi-table joins
- [ ] Secondary indexes
- [ ] JSON functions

### v1.0
- [ ] CC certification preparation
- [ ] Vector search
- [ ] Full-text search
- [ ] Compression

## License

MIT License

## Contributing

Contributions are welcome! Please read our contributing guidelines before submitting PRs.
