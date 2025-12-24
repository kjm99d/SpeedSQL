/*
 * SpeedDB - Cryptography Module
 *
 * CC (Common Criteria) compliant encryption support
 * Supports multiple encryption algorithms with pluggable architecture
 */

#ifndef SPEEDDB_CRYPTO_H
#define SPEEDDB_CRYPTO_H

#include "speeddb.h"
#include <stdint.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

/* ============================================================================
 * Encryption Algorithm Identifiers
 * ============================================================================ */

typedef enum {
    SPEEDDB_CIPHER_NONE = 0,           /* No encryption (plaintext) */
    SPEEDDB_CIPHER_AES_256_GCM,        /* AES-256-GCM (NIST standard) */
    SPEEDDB_CIPHER_AES_256_CBC,        /* AES-256-CBC with HMAC-SHA256 */
    SPEEDDB_CIPHER_AES_256_XTS,        /* AES-256-XTS (disk encryption) */
    SPEEDDB_CIPHER_ARIA_256_GCM,       /* ARIA-256-GCM (Korean standard, CC certified) */
    SPEEDDB_CIPHER_ARIA_256_CBC,       /* ARIA-256-CBC with HMAC */
    SPEEDDB_CIPHER_SEED_CBC,           /* SEED-CBC (Korean standard) */
    SPEEDDB_CIPHER_CHACHA20_POLY1305,  /* ChaCha20-Poly1305 (modern, fast) */
    SPEEDDB_CIPHER_SM4_GCM,            /* SM4-GCM (Chinese standard) */
    SPEEDDB_CIPHER_CUSTOM = 100        /* Custom cipher provider */
} speeddb_cipher_t;

/* Key derivation functions */
typedef enum {
    SPEEDDB_KDF_NONE = 0,              /* Raw key (no derivation) */
    SPEEDDB_KDF_PBKDF2_SHA256,         /* PBKDF2 with SHA-256 */
    SPEEDDB_KDF_PBKDF2_SHA512,         /* PBKDF2 with SHA-512 */
    SPEEDDB_KDF_ARGON2ID,              /* Argon2id (memory-hard) */
    SPEEDDB_KDF_SCRYPT,                /* scrypt (memory-hard) */
    SPEEDDB_KDF_HKDF_SHA256            /* HKDF with SHA-256 */
} speeddb_kdf_t;

/* ============================================================================
 * Encryption Configuration
 * ============================================================================ */

#define SPEEDDB_KEY_SIZE_128    16
#define SPEEDDB_KEY_SIZE_192    24
#define SPEEDDB_KEY_SIZE_256    32
#define SPEEDDB_KEY_SIZE_512    64

#define SPEEDDB_IV_SIZE_96      12     /* GCM recommended */
#define SPEEDDB_IV_SIZE_128     16     /* CBC standard */
#define SPEEDDB_IV_SIZE_192     24     /* ChaCha20 */

#define SPEEDDB_TAG_SIZE_128    16     /* Authentication tag */
#define SPEEDDB_SALT_SIZE       32     /* KDF salt */

/* Encryption configuration structure */
typedef struct {
    speeddb_cipher_t cipher;           /* Encryption algorithm */
    speeddb_kdf_t kdf;                 /* Key derivation function */
    uint32_t kdf_iterations;           /* KDF iteration count (PBKDF2) */
    uint32_t kdf_memory;               /* KDF memory cost (Argon2) in KB */
    uint32_t kdf_parallelism;          /* KDF parallelism (Argon2) */
    uint8_t salt[SPEEDDB_SALT_SIZE];   /* KDF salt */
    bool encrypt_page_header;          /* Encrypt page headers too */
    bool use_per_page_iv;              /* Unique IV per page */
} speeddb_crypto_config_t;

/* ============================================================================
 * Cipher Provider Interface (Strategy Pattern)
 *
 * Allows custom cipher implementations for CC certification
 * ============================================================================ */

/* Forward declaration */
typedef struct speeddb_cipher_provider speeddb_cipher_provider_t;

/* Cipher context (opaque) */
typedef struct speeddb_cipher_ctx speeddb_cipher_ctx_t;

/* Cipher provider virtual function table */
struct speeddb_cipher_provider {
    const char* name;                  /* Provider name */
    const char* version;               /* Provider version */
    speeddb_cipher_t cipher_id;        /* Cipher identifier */

    /* Key sizes */
    size_t key_size;                   /* Required key size */
    size_t iv_size;                    /* Required IV size */
    size_t tag_size;                   /* Authentication tag size (0 if none) */
    size_t block_size;                 /* Block size (for padding) */

    /* Lifecycle */
    int (*init)(speeddb_cipher_ctx_t** ctx, const uint8_t* key, size_t key_len);
    void (*destroy)(speeddb_cipher_ctx_t* ctx);

    /* Encryption */
    int (*encrypt)(
        speeddb_cipher_ctx_t* ctx,
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
        speeddb_cipher_ctx_t* ctx,
        const uint8_t* ciphertext,
        size_t ciphertext_len,
        const uint8_t* iv,
        const uint8_t* aad,
        size_t aad_len,
        const uint8_t* tag,            /* Input: expected tag */
        uint8_t* plaintext
    );

    /* Key rotation support */
    int (*rekey)(speeddb_cipher_ctx_t* ctx, const uint8_t* new_key, size_t key_len);

    /* Self-test for CC compliance */
    int (*self_test)(void);

    /* Zeroize sensitive data */
    void (*zeroize)(speeddb_cipher_ctx_t* ctx);
};

/* ============================================================================
 * Encryption API
 * ============================================================================ */

/* Initialize encryption for a database */
SPEEDDB_API int speeddb_key(
    speeddb* db,
    const void* key,
    int key_len
);

/* Initialize encryption with configuration */
SPEEDDB_API int speeddb_key_v2(
    speeddb* db,
    const void* key,
    int key_len,
    const speeddb_crypto_config_t* config
);

/* Change encryption key (rekey) */
SPEEDDB_API int speeddb_rekey(
    speeddb* db,
    const void* new_key,
    int key_len
);

/* Change encryption algorithm (requires full database rewrite) */
SPEEDDB_API int speeddb_rekey_v2(
    speeddb* db,
    const void* new_key,
    int key_len,
    const speeddb_crypto_config_t* new_config
);

/* Remove encryption */
SPEEDDB_API int speeddb_decrypt(speeddb* db);

/* Get current encryption status */
SPEEDDB_API int speeddb_crypto_status(
    speeddb* db,
    speeddb_cipher_t* cipher,
    bool* is_encrypted
);

/* ============================================================================
 * Cipher Provider Registration
 * ============================================================================ */

/* Register a custom cipher provider */
SPEEDDB_API int speeddb_register_cipher(
    const speeddb_cipher_provider_t* provider
);

/* Unregister a cipher provider */
SPEEDDB_API int speeddb_unregister_cipher(speeddb_cipher_t cipher_id);

/* Get cipher provider by ID */
SPEEDDB_API const speeddb_cipher_provider_t* speeddb_get_cipher(
    speeddb_cipher_t cipher_id
);

/* List available ciphers */
SPEEDDB_API int speeddb_list_ciphers(
    speeddb_cipher_t* ciphers,
    int* count
);

/* ============================================================================
 * Key Derivation API
 * ============================================================================ */

/* Derive key from password */
SPEEDDB_API int speeddb_derive_key(
    const char* password,
    size_t password_len,
    const uint8_t* salt,
    size_t salt_len,
    speeddb_kdf_t kdf,
    uint32_t iterations,
    uint8_t* key_out,
    size_t key_len
);

/* Generate random salt */
SPEEDDB_API int speeddb_random_salt(
    uint8_t* salt,
    size_t salt_len
);

/* Generate random key */
SPEEDDB_API int speeddb_random_key(
    uint8_t* key,
    size_t key_len
);

/* ============================================================================
 * Secure Memory Operations
 * ============================================================================ */

/* Secure memory allocation (non-swappable) */
SPEEDDB_API void* speeddb_secure_malloc(size_t size);

/* Secure memory free with zeroization */
SPEEDDB_API void speeddb_secure_free(void* ptr, size_t size);

/* Secure memory zeroization */
SPEEDDB_API void speeddb_secure_zero(void* ptr, size_t size);

/* ============================================================================
 * CC Compliance Utilities
 * ============================================================================ */

/* Run cipher self-tests (required for CC) */
SPEEDDB_API int speeddb_crypto_self_test(void);

/* Get crypto module version info */
SPEEDDB_API const char* speeddb_crypto_version(void);

/* Check if running in FIPS mode */
SPEEDDB_API bool speeddb_crypto_fips_mode(void);

/* Enable FIPS mode (restricts to approved algorithms) */
SPEEDDB_API int speeddb_crypto_enable_fips(void);

#ifdef __cplusplus
}
#endif

#endif /* SPEEDDB_CRYPTO_H */
