/*
 * SpeedDB - Encryption Example
 *
 * Demonstrates various encryption options for CC certification
 */

#include "speeddb.h"
#include "speeddb_crypto.h"
#include <stdio.h>
#include <string.h>

/* Helper to print available ciphers */
void list_available_ciphers() {
    printf("Available encryption algorithms:\n");
    printf("================================\n");

    int count = 0;
    speeddb_list_ciphers(nullptr, &count);

    speeddb_cipher_t* ciphers = new speeddb_cipher_t[count];
    speeddb_list_ciphers(ciphers, &count);

    for (int i = 0; i < count; i++) {
        const speeddb_cipher_provider_t* provider = speeddb_get_cipher(ciphers[i]);
        if (provider) {
            printf("  [%d] %s v%s\n", ciphers[i], provider->name, provider->version);
            printf("      Key: %zu bytes, IV: %zu bytes, Tag: %zu bytes\n",
                   provider->key_size, provider->iv_size, provider->tag_size);
        }
    }

    delete[] ciphers;
    printf("\n");
}

/* Example 1: No encryption (development mode) */
void example_no_encryption() {
    printf("Example 1: No Encryption\n");
    printf("------------------------\n");

    speeddb* db;
    speeddb_open("example_plain.sdb", &db);

    /* Configure no encryption */
    speeddb_crypto_config_t config = {};
    config.cipher = SPEEDDB_CIPHER_NONE;

    speeddb_key_v2(db, nullptr, 0, &config);

    speeddb_exec(db,
        "CREATE TABLE logs (id INTEGER, message TEXT, timestamp INTEGER)",
        nullptr, nullptr, nullptr);

    speeddb_exec(db,
        "INSERT INTO logs VALUES (1, 'Application started', 1703500000)",
        nullptr, nullptr, nullptr);

    printf("Created unencrypted database: example_plain.sdb\n\n");

    speeddb_close(db);
}

/* Example 2: AES-256-GCM (NIST standard) */
void example_aes_encryption() {
    printf("Example 2: AES-256-GCM Encryption\n");
    printf("---------------------------------\n");

    speeddb* db;
    speeddb_open("example_aes.sdb", &db);

    /* Generate random salt */
    uint8_t salt[32];
    speeddb_random_salt(salt, sizeof(salt));

    /* Configure AES-256-GCM */
    speeddb_crypto_config_t config = {};
    config.cipher = SPEEDDB_CIPHER_AES_256_GCM;
    config.kdf = SPEEDDB_KDF_PBKDF2_SHA256;
    config.kdf_iterations = 100000;  /* OWASP recommendation */
    memcpy(config.salt, salt, sizeof(salt));

    const char* password = "MySecurePassword123!";
    speeddb_key_v2(db, password, strlen(password), &config);

    speeddb_exec(db,
        "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT)",
        nullptr, nullptr, nullptr);

    speeddb_exec(db,
        "INSERT INTO users VALUES (1, 'Alice', 'alice@example.com')",
        nullptr, nullptr, nullptr);

    printf("Created AES-encrypted database: example_aes.sdb\n");
    printf("Password: %s\n", password);
    printf("KDF iterations: %u\n\n", config.kdf_iterations);

    speeddb_close(db);
}

/* Example 3: ARIA-256-GCM (Korean CC certification) */
void example_aria_encryption() {
    printf("Example 3: ARIA-256-GCM Encryption (Korean CC)\n");
    printf("----------------------------------------------\n");

    speeddb* db;
    speeddb_open("example_aria.sdb", &db);

    uint8_t salt[32];
    speeddb_random_salt(salt, sizeof(salt));

    /* Configure ARIA-256-GCM for Korean CC certification */
    speeddb_crypto_config_t config = {};
    config.cipher = SPEEDDB_CIPHER_ARIA_256_GCM;
    config.kdf = SPEEDDB_KDF_PBKDF2_SHA512;
    config.kdf_iterations = 150000;
    memcpy(config.salt, salt, sizeof(salt));

    const char* password = "한글비밀번호도가능합니다!";
    speeddb_key_v2(db, password, strlen(password), &config);

    speeddb_exec(db,
        "CREATE TABLE 고객정보 (번호 INTEGER, 이름 TEXT, 주민번호 TEXT)",
        nullptr, nullptr, nullptr);

    printf("Created ARIA-encrypted database: example_aria.sdb\n");
    printf("Cipher: ARIA-256-GCM (KS X 1213)\n");
    printf("Suitable for Korean CC certification\n\n");

    speeddb_close(db);
}

/* Example 4: ChaCha20-Poly1305 (mobile/embedded) */
void example_chacha20_encryption() {
    printf("Example 4: ChaCha20-Poly1305 Encryption\n");
    printf("---------------------------------------\n");

    speeddb* db;
    speeddb_open("example_chacha.sdb", &db);

    uint8_t salt[32];
    speeddb_random_salt(salt, sizeof(salt));

    /* Configure ChaCha20 - good for mobile without AES hardware */
    speeddb_crypto_config_t config = {};
    config.cipher = SPEEDDB_CIPHER_CHACHA20_POLY1305;
    config.kdf = SPEEDDB_KDF_ARGON2ID;  /* Memory-hard KDF */
    config.kdf_memory = 65536;          /* 64MB memory */
    config.kdf_parallelism = 4;         /* 4 threads */
    config.kdf_iterations = 3;
    memcpy(config.salt, salt, sizeof(salt));

    const char* password = "MobileSecurePassword";
    speeddb_key_v2(db, password, strlen(password), &config);

    speeddb_exec(db,
        "CREATE TABLE notes (id INTEGER, content TEXT, created_at INTEGER)",
        nullptr, nullptr, nullptr);

    printf("Created ChaCha20-encrypted database: example_chacha.sdb\n");
    printf("Optimized for software-only environments\n\n");

    speeddb_close(db);
}

/* Example 5: SEED (Korean legacy) */
void example_seed_encryption() {
    printf("Example 5: SEED-CBC Encryption (Korean Legacy)\n");
    printf("----------------------------------------------\n");

    speeddb* db;
    speeddb_open("example_seed.sdb", &db);

    uint8_t salt[32];
    speeddb_random_salt(salt, sizeof(salt));

    /* Configure SEED for legacy Korean systems */
    speeddb_crypto_config_t config = {};
    config.cipher = SPEEDDB_CIPHER_SEED_CBC;
    config.kdf = SPEEDDB_KDF_PBKDF2_SHA256;
    config.kdf_iterations = 100000;
    memcpy(config.salt, salt, sizeof(salt));

    const char* password = "LegacySystemPassword";
    speeddb_key_v2(db, password, strlen(password), &config);

    printf("Created SEED-encrypted database: example_seed.sdb\n");
    printf("Compatible with Korean legacy systems\n\n");

    speeddb_close(db);
}

/* Example 6: Run crypto self-tests (required for CC) */
void example_self_test() {
    printf("Example 6: Crypto Self-Test\n");
    printf("---------------------------\n");

    printf("Running crypto module self-tests...\n");

    int rc = speeddb_crypto_self_test();
    if (rc == SPEEDDB_OK) {
        printf("All self-tests PASSED\n");
    } else {
        printf("Self-tests FAILED (error: %d)\n", rc);
    }

    printf("Crypto version: %s\n", speeddb_crypto_version());
    printf("FIPS mode: %s\n\n", speeddb_crypto_fips_mode() ? "enabled" : "disabled");
}

/* Example 7: Key rotation */
void example_key_rotation() {
    printf("Example 7: Key Rotation\n");
    printf("-----------------------\n");

    speeddb* db;
    speeddb_open("example_rekey.sdb", &db);

    /* Initial encryption */
    speeddb_crypto_config_t config = {};
    config.cipher = SPEEDDB_CIPHER_AES_256_GCM;
    config.kdf = SPEEDDB_KDF_PBKDF2_SHA256;
    config.kdf_iterations = 100000;

    const char* old_password = "OldPassword123";
    speeddb_key_v2(db, old_password, strlen(old_password), &config);

    speeddb_exec(db,
        "CREATE TABLE secrets (id INTEGER, data BLOB)",
        nullptr, nullptr, nullptr);

    printf("Database created with initial password\n");

    /* Rotate to new key */
    const char* new_password = "NewSecurePassword456!";
    int rc = speeddb_rekey(db, new_password, strlen(new_password));

    if (rc == SPEEDDB_OK) {
        printf("Key rotation successful\n");
        printf("New password: %s\n", new_password);
    } else {
        printf("Key rotation failed (error: %d)\n", rc);
    }

    speeddb_close(db);
    printf("\n");
}

/* Example 8: Secure memory usage */
void example_secure_memory() {
    printf("Example 8: Secure Memory\n");
    printf("------------------------\n");

    /* Allocate secure (non-swappable) memory for key */
    size_t key_size = 32;
    uint8_t* secure_key = (uint8_t*)speeddb_secure_malloc(key_size);

    if (secure_key) {
        /* Generate random key */
        speeddb_random_key(secure_key, key_size);

        printf("Generated %zu-byte key in secure memory\n", key_size);
        printf("Memory is locked (not swappable to disk)\n");

        /* Use the key... */

        /* Securely wipe and free */
        speeddb_secure_free(secure_key, key_size);
        printf("Key securely wiped and memory freed\n\n");
    } else {
        printf("Failed to allocate secure memory\n\n");
    }
}

int main() {
    printf("==============================================\n");
    printf("SpeedDB Encryption Examples\n");
    printf("Version: %s | Crypto: %s\n", SPEEDDB_VERSION, speeddb_crypto_version());
    printf("==============================================\n\n");

    /* List available ciphers */
    list_available_ciphers();

    /* Run examples */
    example_no_encryption();
    example_aes_encryption();
    example_aria_encryption();
    example_chacha20_encryption();
    example_seed_encryption();
    example_self_test();
    example_key_rotation();
    example_secure_memory();

    printf("==============================================\n");
    printf("All encryption examples completed!\n");
    printf("==============================================\n");

    return 0;
}
