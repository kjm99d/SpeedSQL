/*
 * SpeedSQL - Cryptography Module
 *
 * CC (Common Criteria) compliant encryption support
 * Supports multiple encryption algorithms with pluggable architecture
 */

#ifndef SPEEDSQL_CRYPTO_H
#define SPEEDSQL_CRYPTO_H

#include "speedsql.h"
#include <stdint.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

/* ============================================================================
 * Encryption Algorithm Identifiers
 * ============================================================================ */

typedef enum {
    SPEEDSQL_CIPHER_NONE = 0,           /* No encryption (plaintext) */
    SPEEDSQL_CIPHER_AES_256_GCM,        /* AES-256-GCM (NIST standard) */
    SPEEDSQL_CIPHER_AES_256_CBC,        /* AES-256-CBC with HMAC-SHA256 */
    SPEEDSQL_CIPHER_AES_256_XTS,        /* AES-256-XTS (disk encryption) */
    SPEEDSQL_CIPHER_ARIA_256_GCM,       /* ARIA-256-GCM (Korean standard, CC certified) */
    SPEEDSQL_CIPHER_ARIA_256_CBC,       /* ARIA-256-CBC with HMAC */
    SPEEDSQL_CIPHER_SEED_CBC,           /* SEED-CBC (Korean standard) */
    SPEEDSQL_CIPHER_CHACHA20_POLY1305,  /* ChaCha20-Poly1305 (modern, fast) */
    SPEEDSQL_CIPHER_SM4_GCM,            /* SM4-GCM (Chinese standard) */
    SPEEDSQL_CIPHER_CUSTOM = 100        /* Custom cipher provider */
} speedsql_cipher_t;

/* Key derivation functions */
typedef enum {
    SPEEDSQL_KDF_NONE = 0,              /* Raw key (no derivation) */
    SPEEDSQL_KDF_PBKDF2_SHA256,         /* PBKDF2 with SHA-256 */
    SPEEDSQL_KDF_PBKDF2_SHA512,         /* PBKDF2 with SHA-512 */
    SPEEDSQL_KDF_ARGON2ID,              /* Argon2id (memory-hard) */
    SPEEDSQL_KDF_SCRYPT,                /* scrypt (memory-hard) */
    SPEEDSQL_KDF_HKDF_SHA256            /* HKDF with SHA-256 */
} speedsql_kdf_t;

/* ============================================================================
 * Encryption Configuration
 * ============================================================================ */

#define SPEEDSQL_KEY_SIZE_128    16
#define SPEEDSQL_KEY_SIZE_192    24
#define SPEEDSQL_KEY_SIZE_256    32
#define SPEEDSQL_KEY_SIZE_512    64

#define SPEEDSQL_IV_SIZE_96      12     /* GCM recommended */
#define SPEEDSQL_IV_SIZE_128     16     /* CBC standard */
#define SPEEDSQL_IV_SIZE_192     24     /* ChaCha20 */

#define SPEEDSQL_TAG_SIZE_128    16     /* Authentication tag */
#define SPEEDSQL_SALT_SIZE       32     /* KDF salt */

/* Encryption configuration structure */
typedef struct {
    speedsql_cipher_t cipher;           /* Encryption algorithm */
    speedsql_kdf_t kdf;                 /* Key derivation function */
    uint32_t kdf_iterations;           /* KDF iteration count (PBKDF2) */
    uint32_t kdf_memory;               /* KDF memory cost (Argon2) in KB */
    uint32_t kdf_parallelism;          /* KDF parallelism (Argon2) */
    uint8_t salt[SPEEDSQL_SALT_SIZE];   /* KDF salt */
    bool encrypt_page_header;          /* Encrypt page headers too */
    bool use_per_page_iv;              /* Unique IV per page */
} speedsql_crypto_config_t;

/* ============================================================================
 * Cipher Provider Interface (Strategy Pattern)
 *
 * Allows custom cipher implementations for CC certification
 * ============================================================================ */

/* Forward declaration */
typedef struct speedsql_cipher_provider speedsql_cipher_provider_t;

/* Cipher context (opaque) */
typedef struct speedsql_cipher_ctx speedsql_cipher_ctx_t;

/* Cipher provider virtual function table */
struct speedsql_cipher_provider {
    const char* name;                  /* Provider name */
    const char* version;               /* Provider version */
    speedsql_cipher_t cipher_id;        /* Cipher identifier */

    /* Key sizes */
    size_t key_size;                   /* Required key size */
    size_t iv_size;                    /* Required IV size */
    size_t tag_size;                   /* Authentication tag size (0 if none) */
    size_t block_size;                 /* Block size (for padding) */

    /* Lifecycle */
    int (*init)(speedsql_cipher_ctx_t** ctx, const uint8_t* key, size_t key_len);
    void (*destroy)(speedsql_cipher_ctx_t* ctx);

    /* Encryption */
    int (*encrypt)(
        speedsql_cipher_ctx_t* ctx,
        const uint8_t* plaintext,
        size_t plaintext_len,
        const uint8_t* iv,
        const uint8_t* aad,            /* Additional authenticated data */
        size_t aad_len,
        uint8_t* ciphertext,
        uint8_t* tag                   /* Output: authentication tag */
    );

    /* Decryption */
    int (*decrypt)(
        speedsql_cipher_ctx_t* ctx,
        const uint8_t* ciphertext,
        size_t ciphertext_len,
        const uint8_t* iv,
        const uint8_t* aad,
        size_t aad_len,
        const uint8_t* tag,            /* Input: expected tag */
        uint8_t* plaintext
    );

    /* Key rotation support */
    int (*rekey)(speedsql_cipher_ctx_t* ctx, const uint8_t* new_key, size_t key_len);

    /* Self-test for CC compliance */
    int (*self_test)(void);

    /* Zeroize sensitive data */
    void (*zeroize)(speedsql_cipher_ctx_t* ctx);
};

/* ============================================================================
 * Encryption API
 * ============================================================================ */

/* Initialize encryption for a database */
SPEEDSQL_API int speedsql_key(
    speedsql* db,
    const void* key,
    int key_len
);

/* Initialize encryption with configuration */
SPEEDSQL_API int speedsql_key_v2(
    speedsql* db,
    const void* key,
    int key_len,
    const speedsql_crypto_config_t* config
);

/* Change encryption key (rekey) */
SPEEDSQL_API int speedsql_rekey(
    speedsql* db,
    const void* new_key,
    int key_len
);

/* Change encryption algorithm (requires full database rewrite) */
SPEEDSQL_API int speedsql_rekey_v2(
    speedsql* db,
    const void* new_key,
    int key_len,
    const speedsql_crypto_config_t* new_config
);

/* Remove encryption */
SPEEDSQL_API int speedsql_decrypt(speedsql* db);

/* Get current encryption status */
SPEEDSQL_API int speedsql_crypto_status(
    speedsql* db,
    speedsql_cipher_t* cipher,
    bool* is_encrypted
);

/* ============================================================================
 * Cipher Provider Registration
 * ============================================================================ */

/* Register a custom cipher provider */
SPEEDSQL_API int speedsql_register_cipher(
    const speedsql_cipher_provider_t* provider
);

/* Unregister a cipher provider */
SPEEDSQL_API int speedsql_unregister_cipher(speedsql_cipher_t cipher_id);

/* Get cipher provider by ID */
SPEEDSQL_API const speedsql_cipher_provider_t* speedsql_get_cipher(
    speedsql_cipher_t cipher_id
);

/* List available ciphers */
SPEEDSQL_API int speedsql_list_ciphers(
    speedsql_cipher_t* ciphers,
    int* count
);

/* ============================================================================
 * Key Derivation API
 * ============================================================================ */

/* Derive key from password */
SPEEDSQL_API int speedsql_derive_key(
    const char* password,
    size_t password_len,
    const uint8_t* salt,
    size_t salt_len,
    speedsql_kdf_t kdf,
    uint32_t iterations,
    uint8_t* key_out,
    size_t key_len
);

/* Generate random salt */
SPEEDSQL_API int speedsql_random_salt(
    uint8_t* salt,
    size_t salt_len
);

/* Generate random key */
SPEEDSQL_API int speedsql_random_key(
    uint8_t* key,
    size_t key_len
);

/* ============================================================================
 * Secure Memory Operations
 * ============================================================================ */

/* Secure memory allocation (non-swappable) */
SPEEDSQL_API void* speedsql_secure_malloc(size_t size);

/* Secure memory free with zeroization */
SPEEDSQL_API void speedsql_secure_free(void* ptr, size_t size);

/* Secure memory zeroization */
SPEEDSQL_API void speedsql_secure_zero(void* ptr, size_t size);

/* ============================================================================
 * CC Compliance Utilities
 * ============================================================================ */

/* Run cipher self-tests (required for CC) */
SPEEDSQL_API int speedsql_crypto_self_test(void);

/* Get crypto module version info */
SPEEDSQL_API const char* speedsql_crypto_version(void);

/* Check if running in FIPS mode */
SPEEDSQL_API bool speedsql_crypto_fips_mode(void);

/* Enable FIPS mode (restricts to approved algorithms) */
SPEEDSQL_API int speedsql_crypto_enable_fips(void);

#ifdef __cplusplus
}
#endif

#endif /* SPEEDSQL_CRYPTO_H */
