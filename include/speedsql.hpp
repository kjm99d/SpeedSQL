/*
 * SpeedSQL - Modern C++ Wrapper
 *
 * RAII-based C++ interface for SpeedSQL
 * Provides type-safe, exception-safe access to the database
 */

#ifndef SPEEDSQL_HPP
#define SPEEDSQL_HPP

#include "speedsql.h"
#include "speedsql_crypto.h"

#include <string>
#include <string_view>
#include <vector>
#include <optional>
#include <memory>
#include <stdexcept>
#include <functional>
#include <variant>
#include <span>

namespace speedsql {

/* ============================================================================
 * Exception Classes
 * ============================================================================ */

class Exception : public std::runtime_error {
public:
    explicit Exception(int code, const std::string& message)
        : std::runtime_error(message), error_code_(code) {}

    int code() const noexcept { return error_code_; }

private:
    int error_code_;
};

class DatabaseException : public Exception {
    using Exception::Exception;
};

class StatementException : public Exception {
    using Exception::Exception;
};

class CryptoException : public Exception {
    using Exception::Exception;
};

/* ============================================================================
 * Value Type (variant-based)
 * ============================================================================ */

using Null = std::monostate;
using Blob = std::vector<uint8_t>;
using Value = std::variant<Null, int64_t, double, std::string, Blob>;

/* ============================================================================
 * Forward Declarations
 * ============================================================================ */

class Database;
class Statement;
class Transaction;
class Row;

/* ============================================================================
 * Crypto Configuration
 * ============================================================================ */

enum class Cipher {
    None = SPEEDSQL_CIPHER_NONE,
    AES_256_GCM = SPEEDSQL_CIPHER_AES_256_GCM,
    AES_256_CBC = SPEEDSQL_CIPHER_AES_256_CBC,
    ARIA_256_GCM = SPEEDSQL_CIPHER_ARIA_256_GCM,
    ARIA_256_CBC = SPEEDSQL_CIPHER_ARIA_256_CBC,
    SEED_CBC = SPEEDSQL_CIPHER_SEED_CBC,
    ChaCha20_Poly1305 = SPEEDSQL_CIPHER_CHACHA20_POLY1305
};

enum class KDF {
    None = SPEEDSQL_KDF_NONE,
    PBKDF2_SHA256 = SPEEDSQL_KDF_PBKDF2_SHA256,
    PBKDF2_SHA512 = SPEEDSQL_KDF_PBKDF2_SHA512,
    Argon2id = SPEEDSQL_KDF_ARGON2ID,
    Scrypt = SPEEDSQL_KDF_SCRYPT
};

struct CryptoConfig {
    Cipher cipher = Cipher::AES_256_GCM;
    KDF kdf = KDF::PBKDF2_SHA256;
    uint32_t iterations = 100000;
    uint32_t memory_kb = 65536;      // For Argon2
    uint32_t parallelism = 4;        // For Argon2

    speedsql_crypto_config_t to_c() const {
        speedsql_crypto_config_t config = {};
        config.cipher = static_cast<speedsql_cipher_t>(cipher);
        config.kdf = static_cast<speedsql_kdf_t>(kdf);
        config.kdf_iterations = iterations;
        config.kdf_memory = memory_kb;
        config.kdf_parallelism = parallelism;
        return config;
    }
};

/* ============================================================================
 * Statement Class
 * ============================================================================ */

class Statement {
public:
    Statement(const Statement&) = delete;
    Statement& operator=(const Statement&) = delete;

    Statement(Statement&& other) noexcept
        : stmt_(other.stmt_), db_(other.db_) {
        other.stmt_ = nullptr;
    }

    Statement& operator=(Statement&& other) noexcept {
        if (this != &other) {
            finalize();
            stmt_ = other.stmt_;
            db_ = other.db_;
            other.stmt_ = nullptr;
        }
        return *this;
    }

    ~Statement() {
        finalize();
    }

    /* Binding parameters */
    Statement& bind(int index, std::nullptr_t) {
        check(speedsql_bind_null(stmt_, index));
        return *this;
    }

    Statement& bind(int index, int value) {
        check(speedsql_bind_int(stmt_, index, value));
        return *this;
    }

    Statement& bind(int index, int64_t value) {
        check(speedsql_bind_int64(stmt_, index, value));
        return *this;
    }

    Statement& bind(int index, double value) {
        check(speedsql_bind_double(stmt_, index, value));
        return *this;
    }

    Statement& bind(int index, std::string_view value) {
        check(speedsql_bind_text(stmt_, index, value.data(),
                                 static_cast<int>(value.size()), nullptr));
        return *this;
    }

    Statement& bind(int index, const std::string& value) {
        return bind(index, std::string_view(value));
    }

    Statement& bind(int index, const char* value) {
        return bind(index, std::string_view(value));
    }

    Statement& bind(int index, std::span<const uint8_t> value) {
        check(speedsql_bind_blob(stmt_, index, value.data(),
                                 static_cast<int>(value.size()), nullptr));
        return *this;
    }

    Statement& bind(int index, const Blob& value) {
        return bind(index, std::span<const uint8_t>(value));
    }

    template<typename T>
    Statement& bind(int index, const std::optional<T>& value) {
        if (value) {
            return bind(index, *value);
        }
        return bind(index, nullptr);
    }

    /* Variadic bind for convenience */
    template<typename... Args>
    Statement& bind_all(Args&&... args) {
        int index = 1;
        (bind(index++, std::forward<Args>(args)), ...);
        return *this;
    }

    /* Execution */
    bool step() {
        int rc = speedsql_step(stmt_);
        if (rc == SPEEDSQL_ROW) return true;
        if (rc == SPEEDSQL_DONE) return false;
        throw StatementException(rc, "Step failed");
    }

    void execute() {
        while (step()) {}
    }

    Statement& reset() {
        check(speedsql_reset(stmt_));
        return *this;
    }

    /* Column access */
    int column_count() const {
        return speedsql_column_count(stmt_);
    }

    std::string column_name(int col) const {
        const char* name = speedsql_column_name(stmt_, col);
        return name ? name : "";
    }

    int column_type(int col) const {
        return speedsql_column_type(stmt_, col);
    }

    bool is_null(int col) const {
        return column_type(col) == SPEEDSQL_TYPE_NULL;
    }

    int get_int(int col) const {
        return speedsql_column_int(stmt_, col);
    }

    int64_t get_int64(int col) const {
        return speedsql_column_int64(stmt_, col);
    }

    double get_double(int col) const {
        return speedsql_column_double(stmt_, col);
    }

    std::string get_text(int col) const {
        const unsigned char* text = speedsql_column_text(stmt_, col);
        int len = speedsql_column_bytes(stmt_, col);
        return text ? std::string(reinterpret_cast<const char*>(text), len) : "";
    }

    Blob get_blob(int col) const {
        const void* data = speedsql_column_blob(stmt_, col);
        int len = speedsql_column_bytes(stmt_, col);
        if (data && len > 0) {
            const uint8_t* bytes = static_cast<const uint8_t*>(data);
            return Blob(bytes, bytes + len);
        }
        return Blob();
    }

    Value get_value(int col) const {
        switch (column_type(col)) {
            case SPEEDSQL_TYPE_NULL:
                return Null{};
            case SPEEDSQL_TYPE_INT:
                return get_int64(col);
            case SPEEDSQL_TYPE_FLOAT:
                return get_double(col);
            case SPEEDSQL_TYPE_TEXT:
                return get_text(col);
            case SPEEDSQL_TYPE_BLOB:
                return get_blob(col);
            default:
                return Null{};
        }
    }

    template<typename T>
    T get(int col) const {
        if constexpr (std::is_same_v<T, int>) {
            return get_int(col);
        } else if constexpr (std::is_same_v<T, int64_t>) {
            return get_int64(col);
        } else if constexpr (std::is_same_v<T, double>) {
            return get_double(col);
        } else if constexpr (std::is_same_v<T, std::string>) {
            return get_text(col);
        } else if constexpr (std::is_same_v<T, Blob>) {
            return get_blob(col);
        } else {
            static_assert(sizeof(T) == 0, "Unsupported type");
        }
    }

    template<typename T>
    std::optional<T> get_optional(int col) const {
        if (is_null(col)) return std::nullopt;
        return get<T>(col);
    }

    /* Iterator support */
    class Iterator {
    public:
        using iterator_category = std::input_iterator_tag;
        using value_type = Statement;
        using difference_type = std::ptrdiff_t;
        using pointer = Statement*;
        using reference = Statement&;

        Iterator() : stmt_(nullptr), done_(true) {}
        explicit Iterator(Statement* stmt) : stmt_(stmt), done_(false) {
            ++(*this);
        }

        reference operator*() { return *stmt_; }
        pointer operator->() { return stmt_; }

        Iterator& operator++() {
            if (stmt_ && !stmt_->step()) {
                done_ = true;
            }
            return *this;
        }

        bool operator==(const Iterator& other) const {
            return done_ == other.done_;
        }

        bool operator!=(const Iterator& other) const {
            return !(*this == other);
        }

    private:
        Statement* stmt_;
        bool done_;
    };

    Iterator begin() { return Iterator(this); }
    Iterator end() { return Iterator(); }

private:
    friend class Database;

    explicit Statement(speedsql_stmt* stmt, speedsql* db)
        : stmt_(stmt), db_(db) {}

    void finalize() {
        if (stmt_) {
            speedsql_finalize(stmt_);
            stmt_ = nullptr;
        }
    }

    void check(int rc) {
        if (rc != SPEEDSQL_OK) {
            throw StatementException(rc, speedsql_errmsg(db_));
        }
    }

    speedsql_stmt* stmt_;
    speedsql* db_;
};

/* ============================================================================
 * Transaction Class (RAII)
 * ============================================================================ */

class Transaction {
public:
    Transaction(const Transaction&) = delete;
    Transaction& operator=(const Transaction&) = delete;

    Transaction(Transaction&& other) noexcept
        : db_(other.db_), committed_(other.committed_) {
        other.db_ = nullptr;
        other.committed_ = true;
    }

    ~Transaction() {
        if (db_ && !committed_) {
            speedsql_rollback(db_);
        }
    }

    void commit() {
        if (db_ && !committed_) {
            int rc = speedsql_commit(db_);
            if (rc != SPEEDSQL_OK) {
                throw DatabaseException(rc, speedsql_errmsg(db_));
            }
            committed_ = true;
        }
    }

    void rollback() {
        if (db_ && !committed_) {
            speedsql_rollback(db_);
            committed_ = true;
        }
    }

private:
    friend class Database;

    explicit Transaction(speedsql* db) : db_(db), committed_(false) {
        int rc = speedsql_begin(db_);
        if (rc != SPEEDSQL_OK) {
            throw DatabaseException(rc, speedsql_errmsg(db_));
        }
    }

    speedsql* db_;
    bool committed_;
};

/* ============================================================================
 * Database Class
 * ============================================================================ */

class Database {
public:
    /* Open flags */
    enum OpenFlags {
        ReadOnly    = SPEEDSQL_OPEN_READONLY,
        ReadWrite   = SPEEDSQL_OPEN_READWRITE,
        Create      = SPEEDSQL_OPEN_CREATE,
        Memory      = SPEEDSQL_OPEN_MEMORY,
        WAL         = SPEEDSQL_OPEN_WAL
    };

    /* Constructors */
    Database() : db_(nullptr) {}

    explicit Database(const std::string& filename,
                      int flags = ReadWrite | Create) : db_(nullptr) {
        open(filename, flags);
    }

    Database(const Database&) = delete;
    Database& operator=(const Database&) = delete;

    Database(Database&& other) noexcept : db_(other.db_) {
        other.db_ = nullptr;
    }

    Database& operator=(Database&& other) noexcept {
        if (this != &other) {
            close();
            db_ = other.db_;
            other.db_ = nullptr;
        }
        return *this;
    }

    ~Database() {
        close();
    }

    /* Connection management */
    void open(const std::string& filename, int flags = ReadWrite | Create) {
        close();
        int rc = speedsql_open_v2(filename.c_str(), &db_, flags, nullptr);
        if (rc != SPEEDSQL_OK) {
            throw DatabaseException(rc, "Failed to open database: " + filename);
        }
    }

    void close() {
        if (db_) {
            speedsql_close(db_);
            db_ = nullptr;
        }
    }

    bool is_open() const {
        return db_ != nullptr;
    }

    /* Encryption */
    void set_key(std::string_view password,
                 const CryptoConfig& config = CryptoConfig()) {
        auto c_config = config.to_c();
        int rc = speedsql_key_v2(db_, password.data(),
                                 static_cast<int>(password.size()), &c_config);
        if (rc != SPEEDSQL_OK) {
            throw CryptoException(rc, speedsql_errmsg(db_));
        }
    }

    void rekey(std::string_view new_password) {
        int rc = speedsql_rekey(db_, new_password.data(),
                                static_cast<int>(new_password.size()));
        if (rc != SPEEDSQL_OK) {
            throw CryptoException(rc, speedsql_errmsg(db_));
        }
    }

    void remove_encryption() {
        int rc = speedsql_decrypt(db_);
        if (rc != SPEEDSQL_OK) {
            throw CryptoException(rc, speedsql_errmsg(db_));
        }
    }

    /* SQL Execution */
    void execute(const std::string& sql) {
        char* errmsg = nullptr;
        int rc = speedsql_exec(db_, sql.c_str(), nullptr, nullptr, &errmsg);
        if (rc != SPEEDSQL_OK) {
            std::string msg = errmsg ? errmsg : speedsql_errmsg(db_);
            if (errmsg) speedsql_free(errmsg);
            throw DatabaseException(rc, msg);
        }
    }

    Statement prepare(const std::string& sql) {
        speedsql_stmt* stmt = nullptr;
        int rc = speedsql_prepare(db_, sql.c_str(),
                                  static_cast<int>(sql.size()), &stmt, nullptr);
        if (rc != SPEEDSQL_OK) {
            throw DatabaseException(rc, speedsql_errmsg(db_));
        }
        return Statement(stmt, db_);
    }

    /* Convenience query methods */
    template<typename... Args>
    Statement query(const std::string& sql, Args&&... args) {
        auto stmt = prepare(sql);
        stmt.bind_all(std::forward<Args>(args)...);
        return stmt;
    }

    template<typename... Args>
    void exec(const std::string& sql, Args&&... args) {
        query(sql, std::forward<Args>(args)...).execute();
    }

    template<typename T, typename... Args>
    std::optional<T> query_single(const std::string& sql, Args&&... args) {
        auto stmt = query(sql, std::forward<Args>(args)...);
        if (stmt.step()) {
            return stmt.get<T>(0);
        }
        return std::nullopt;
    }

    template<typename T, typename... Args>
    std::vector<T> query_column(const std::string& sql, Args&&... args) {
        std::vector<T> result;
        auto stmt = query(sql, std::forward<Args>(args)...);
        while (stmt.step()) {
            result.push_back(stmt.get<T>(0));
        }
        return result;
    }

    /* Transaction */
    Transaction begin_transaction() {
        return Transaction(db_);
    }

    template<typename Func>
    void transaction(Func&& func) {
        auto txn = begin_transaction();
        try {
            func();
            txn.commit();
        } catch (...) {
            txn.rollback();
            throw;
        }
    }

    /* Utility */
    int changes() const {
        return speedsql_changes(db_);
    }

    int64_t total_changes() const {
        return speedsql_total_changes(db_);
    }

    int64_t last_insert_rowid() const {
        return speedsql_last_insert_rowid(db_);
    }

    std::string error_message() const {
        return speedsql_errmsg(db_);
    }

    int error_code() const {
        return speedsql_errcode(db_);
    }

    /* Raw handle access (for advanced use) */
    speedsql* handle() { return db_; }
    const speedsql* handle() const { return db_; }

private:
    speedsql* db_;
};

/* ============================================================================
 * Utility Functions
 * ============================================================================ */

inline std::string version() {
    return SPEEDSQL_VERSION;
}

inline std::string crypto_version() {
    return speedsql_crypto_version();
}

inline bool run_crypto_self_test() {
    return speedsql_crypto_self_test() == SPEEDSQL_OK;
}

} // namespace speedsql

#endif // SPEEDSQL_HPP
