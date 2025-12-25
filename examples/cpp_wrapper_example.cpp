/*
 * SpeedSQL - C++ Wrapper Example
 *
 * Demonstrates modern C++ usage with RAII, exceptions, and type safety
 */

#include "speedsql.hpp"
#include <iostream>
#include <iomanip>

using namespace speedsql;

/* ============================================================================
 * Example 1: Basic Database Operations
 * ============================================================================ */

void example_basic_operations() {
    std::cout << "Example 1: Basic Operations\n";
    std::cout << "===========================\n\n";

    try {
        // Open database (RAII - auto-closes on scope exit)
        Database db("cpp_example.sdb");

        // Create table using exec
        db.exec(R"(
            CREATE TABLE users (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                email TEXT,
                score REAL,
                data BLOB
            )
        )");

        std::cout << "Created table 'users'\n";

        // Insert data using exec
        db.exec("INSERT INTO users VALUES (1, 'Alice', 'alice@example.com', 95.5, NULL)");
        db.exec("INSERT INTO users VALUES (2, 'Bob', 'bob@example.com', 87.3, NULL)");
        db.exec("INSERT INTO users VALUES (3, 'Charlie', NULL, 92.1, NULL)");

        std::cout << "Inserted 3 users\n\n";

        // Query using prepared statement
        Statement stmt = db.prepare("SELECT id, name, email, score FROM users WHERE score > ?");
        stmt.bind(1, 90.0);

        std::cout << "Users with score > 90:\n";
        std::cout << std::setw(4) << "ID" << " | "
                  << std::setw(10) << "Name" << " | "
                  << std::setw(20) << "Email" << " | "
                  << std::setw(6) << "Score" << "\n";
        std::cout << std::string(50, '-') << "\n";

        while (stmt.step()) {
            int64_t id = stmt.get<int64_t>(0);
            std::string name = stmt.get<std::string>(1);
            std::string email = stmt.is_null(2) ? "(none)" : stmt.get<std::string>(2);
            double score = stmt.get<double>(3);

            std::cout << std::setw(4) << id << " | "
                      << std::setw(10) << name << " | "
                      << std::setw(20) << email << " | "
                      << std::setw(6) << std::fixed << std::setprecision(1) << score << "\n";
        }

        std::cout << "\n";

    } catch (const DatabaseException& e) {
        std::cerr << "Database error: " << e.what() << " (code: " << e.code() << ")\n";
    }
}

/* ============================================================================
 * Example 2: Transactions with RAII
 * ============================================================================ */

void example_transactions() {
    std::cout << "Example 2: Transactions (RAII)\n";
    std::cout << "==============================\n\n";

    try {
        Database db("cpp_transactions.sdb");

        db.exec("CREATE TABLE accounts (id INTEGER PRIMARY KEY, name TEXT, balance REAL)");
        db.exec("INSERT INTO accounts VALUES (1, 'Checking', 1000.0)");
        db.exec("INSERT INTO accounts VALUES (2, 'Savings', 5000.0)");

        std::cout << "Initial balances:\n";
        for (auto& row : db.query("SELECT name, balance FROM accounts")) {
            std::cout << "  " << std::get<std::string>(row[0]) << ": $"
                      << std::fixed << std::setprecision(2)
                      << std::get<double>(row[1]) << "\n";
        }

        // Transfer money using transaction
        std::cout << "\nTransferring $500 from Checking to Savings...\n";

        {
            Transaction txn(db);  // Begin transaction

            db.exec("UPDATE accounts SET balance = balance - 500 WHERE id = 1");
            db.exec("UPDATE accounts SET balance = balance + 500 WHERE id = 2");

            txn.commit();  // Commit on success
            // If exception thrown before commit, destructor will rollback
        }

        std::cout << "\nFinal balances:\n";
        for (auto& row : db.query("SELECT name, balance FROM accounts")) {
            std::cout << "  " << std::get<std::string>(row[0]) << ": $"
                      << std::fixed << std::setprecision(2)
                      << std::get<double>(row[1]) << "\n";
        }

        // Lambda-based transaction
        std::cout << "\nUsing lambda transaction (adding interest)...\n";

        db.transaction([&]() {
            db.exec("UPDATE accounts SET balance = balance * 1.05 WHERE id = 2");
        });

        auto savings = db.query_single<double>("SELECT balance FROM accounts WHERE id = 2");
        std::cout << "Savings after 5% interest: $" << std::fixed << std::setprecision(2)
                  << savings << "\n\n";

    } catch (const DatabaseException& e) {
        std::cerr << "Database error: " << e.what() << "\n";
    }
}

/* ============================================================================
 * Example 3: Encrypted Database
 * ============================================================================ */

void example_encryption() {
    std::cout << "Example 3: Encrypted Database\n";
    std::cout << "=============================\n\n";

    try {
        // Open with encryption
        Database db("cpp_encrypted.sdb");

        // Configure AES-256-GCM encryption
        CryptoConfig config;
        config.cipher = Cipher::AES_256_GCM;
        config.kdf = KDF::PBKDF2_SHA256;
        config.iterations = 100000;

        db.set_key("MySecurePassword123!", config);

        std::cout << "Database encrypted with AES-256-GCM\n";
        std::cout << "KDF: PBKDF2-SHA256 with 100,000 iterations\n\n";

        db.exec("CREATE TABLE secrets (id INTEGER, data TEXT)");
        db.exec("INSERT INTO secrets VALUES (1, 'Top Secret Information')");

        // Read back
        auto secret = db.query_single<std::string>("SELECT data FROM secrets WHERE id = 1");
        std::cout << "Stored secret: " << secret << "\n";

        // Key rotation
        std::cout << "\nRotating encryption key...\n";
        db.rekey("NewSecurePassword456!");
        std::cout << "Key rotation complete\n\n";

    } catch (const CryptoException& e) {
        std::cerr << "Crypto error: " << e.what() << "\n";
    } catch (const DatabaseException& e) {
        std::cerr << "Database error: " << e.what() << "\n";
    }
}

/* ============================================================================
 * Example 4: Working with BLOBs
 * ============================================================================ */

void example_blobs() {
    std::cout << "Example 4: BLOB Data\n";
    std::cout << "====================\n\n";

    try {
        Database db("cpp_blobs.sdb");

        db.exec("CREATE TABLE files (id INTEGER PRIMARY KEY, name TEXT, content BLOB)");

        // Create some binary data
        Blob binary_data = {0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A,  // PNG header
                           0x00, 0x00, 0x00, 0x0D, 0x49, 0x48, 0x44, 0x52};

        // Insert BLOB
        Statement insert = db.prepare("INSERT INTO files (name, content) VALUES (?, ?)");
        insert.bind(1, "image.png");
        insert.bind(2, binary_data);
        insert.step();

        std::cout << "Inserted " << binary_data.size() << " bytes of binary data\n";

        // Read BLOB back
        Statement select = db.prepare("SELECT content FROM files WHERE name = ?");
        select.bind(1, "image.png");

        if (select.step()) {
            Blob retrieved = select.get<Blob>(0);
            std::cout << "Retrieved " << retrieved.size() << " bytes\n";

            std::cout << "Data (hex): ";
            for (size_t i = 0; i < std::min(retrieved.size(), size_t(8)); i++) {
                std::cout << std::hex << std::setw(2) << std::setfill('0')
                          << static_cast<int>(retrieved[i]) << " ";
            }
            std::cout << std::dec << "...\n\n";
        }

    } catch (const DatabaseException& e) {
        std::cerr << "Database error: " << e.what() << "\n";
    }
}

/* ============================================================================
 * Example 5: Statement Iterator
 * ============================================================================ */

void example_iterator() {
    std::cout << "Example 5: Range-based For Loop\n";
    std::cout << "===============================\n\n";

    try {
        Database db("cpp_iterator.sdb");

        db.exec("CREATE TABLE products (id INTEGER, name TEXT, price REAL)");
        db.exec("INSERT INTO products VALUES (1, 'Laptop', 999.99)");
        db.exec("INSERT INTO products VALUES (2, 'Mouse', 29.99)");
        db.exec("INSERT INTO products VALUES (3, 'Keyboard', 79.99)");
        db.exec("INSERT INTO products VALUES (4, 'Monitor', 299.99)");

        std::cout << "Products list:\n";

        // Using range-based for with statement
        Statement stmt = db.prepare("SELECT id, name, price FROM products ORDER BY price DESC");
        for (auto& row : stmt) {
            std::cout << "  #" << std::get<int64_t>(row[0]) << " "
                      << std::get<std::string>(row[1]) << " - $"
                      << std::fixed << std::setprecision(2)
                      << std::get<double>(row[2]) << "\n";
        }

        // Using query() which returns vector of rows
        std::cout << "\nProducts under $100:\n";
        for (auto& row : db.query("SELECT name, price FROM products WHERE price < 100")) {
            std::cout << "  " << std::get<std::string>(row[0]) << " - $"
                      << std::fixed << std::setprecision(2)
                      << std::get<double>(row[1]) << "\n";
        }

        // Single column query
        std::cout << "\nAll product names: ";
        auto names = db.query_column<std::string>("SELECT name FROM products");
        for (size_t i = 0; i < names.size(); i++) {
            std::cout << names[i];
            if (i < names.size() - 1) std::cout << ", ";
        }
        std::cout << "\n\n";

    } catch (const DatabaseException& e) {
        std::cerr << "Database error: " << e.what() << "\n";
    }
}

/* ============================================================================
 * Example 6: Error Handling
 * ============================================================================ */

void example_error_handling() {
    std::cout << "Example 6: Error Handling\n";
    std::cout << "=========================\n\n";

    // Invalid SQL
    try {
        Database db("cpp_errors.sdb");
        db.exec("SELEKT * FORM users");  // Typo
    } catch (const DatabaseException& e) {
        std::cout << "Caught syntax error: " << e.what() << "\n";
        std::cout << "Error code: " << e.code() << "\n\n";
    }

    // Constraint violation
    try {
        Database db("cpp_errors.sdb");
        db.exec("CREATE TABLE test (id INTEGER PRIMARY KEY)");
        db.exec("INSERT INTO test VALUES (1)");
        db.exec("INSERT INTO test VALUES (1)");  // Duplicate
    } catch (const DatabaseException& e) {
        std::cout << "Caught constraint violation: " << e.what() << "\n\n";
    }

    // Transaction rollback on exception
    try {
        Database db("cpp_errors.sdb");
        db.exec("CREATE TABLE safe (id INTEGER, value INTEGER)");
        db.exec("INSERT INTO safe VALUES (1, 100)");

        db.transaction([&]() {
            db.exec("UPDATE safe SET value = 200 WHERE id = 1");
            throw std::runtime_error("Simulated error!");  // This triggers rollback
        });
    } catch (const std::exception& e) {
        std::cout << "Transaction rolled back due to: " << e.what() << "\n";

        // Verify rollback
        Database db("cpp_errors.sdb");
        auto value = db.query_single<int64_t>("SELECT value FROM safe WHERE id = 1");
        std::cout << "Value after rollback: " << (value ? *value : 0) << " (unchanged)\n\n";
    }
}

/* ============================================================================
 * Main
 * ============================================================================ */

int main() {
    std::cout << "================================================\n";
    std::cout << "SpeedSQL C++ Wrapper Examples\n";
    std::cout << "================================================\n\n";

    example_basic_operations();
    example_transactions();
    example_encryption();
    example_blobs();
    example_iterator();
    example_error_handling();

    std::cout << "================================================\n";
    std::cout << "All C++ examples completed!\n";
    std::cout << "================================================\n";

    return 0;
}
