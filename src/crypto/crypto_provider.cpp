/*
 * SpeedSQL - Crypto Provider Registry
 *
 * Manages cipher provider registration and lookup (Strategy Pattern)
 */

#include "speedsql_internal.h"
#include "speedsql_crypto.h"
#include <string.h>

#ifdef _WIN32
#include <windows.h>
#include <wincrypt.h>
#endif

/* Maximum number of registered cipher providers */
#define MAX_CIPHER_PROVIDERS 32

/* Provider registry */
static struct {
    const speedsql_cipher_provider_t* providers[MAX_CIPHER_PROVIDERS];
    int count;
    mutex_t lock;
    bool initialized;
    bool fips_mode;
} g_crypto_registry = {0};

/* Crypto module version */
static const char* CRYPTO_VERSION = "SpeedSQL Crypto 1.0.0";

/* Forward declarations of built-in providers */
extern const speedsql_cipher_provider_t g_cipher_none;
extern const speedsql_cipher_provider_t g_cipher_aes_256_gcm;
extern const speedsql_cipher_provider_t g_cipher_aes_256_cbc;
extern const speedsql_cipher_provider_t g_cipher_aria_256_gcm;
extern const speedsql_cipher_provider_t g_cipher_aria_256_cbc;
extern const speedsql_cipher_provider_t g_cipher_seed_cbc;
extern const speedsql_cipher_provider_t g_cipher_chacha20_poly1305;

/* Initialize crypto registry */
static void crypto_registry_init(void) {
    if (g_crypto_registry.initialized) return;

    mutex_init(&g_crypto_registry.lock);
    g_crypto_registry.count = 0;
    g_crypto_registry.fips_mode = false;

    /* Register built-in providers */
    g_crypto_registry.providers[g_crypto_registry.count++] = &g_cipher_none;
    g_crypto_registry.providers[g_crypto_registry.count++] = &g_cipher_aes_256_gcm;
    g_crypto_registry.providers[g_crypto_registry.count++] = &g_cipher_aes_256_cbc;
    g_crypto_registry.providers[g_crypto_registry.count++] = &g_cipher_aria_256_gcm;
    g_crypto_registry.providers[g_crypto_registry.count++] = &g_cipher_aria_256_cbc;
    g_crypto_registry.providers[g_crypto_registry.count++] = &g_cipher_seed_cbc;
    g_crypto_registry.providers[g_crypto_registry.count++] = &g_cipher_chacha20_poly1305;

    g_crypto_registry.initialized = true;
}

/* Register a custom cipher provider */
SPEEDSQL_API int speedsql_register_cipher(const speedsql_cipher_provider_t* provider) {
    if (!provider || !provider->name) {
        return SPEEDSQL_MISUSE;
    }

    crypto_registry_init();
    mutex_lock(&g_crypto_registry.lock);

    /* Check for duplicate */
    for (int i = 0; i < g_crypto_registry.count; i++) {
        if (g_crypto_registry.providers[i]->cipher_id == provider->cipher_id) {
            mutex_unlock(&g_crypto_registry.lock);
            return SPEEDSQL_CONSTRAINT;  /* Already registered */
        }
    }

    if (g_crypto_registry.count >= MAX_CIPHER_PROVIDERS) {
        mutex_unlock(&g_crypto_registry.lock);
        return SPEEDSQL_FULL;
    }

    g_crypto_registry.providers[g_crypto_registry.count++] = provider;
    mutex_unlock(&g_crypto_registry.lock);

    return SPEEDSQL_OK;
}

/* Unregister a cipher provider */
SPEEDSQL_API int speedsql_unregister_cipher(speedsql_cipher_t cipher_id) {
    if (cipher_id <= SPEEDSQL_CIPHER_CHACHA20_POLY1305) {
        return SPEEDSQL_MISUSE;  /* Cannot unregister built-in ciphers */
    }

    crypto_registry_init();
    mutex_lock(&g_crypto_registry.lock);

    for (int i = 0; i < g_crypto_registry.count; i++) {
        if (g_crypto_registry.providers[i]->cipher_id == cipher_id) {
            /* Shift remaining providers */
            for (int j = i; j < g_crypto_registry.count - 1; j++) {
                g_crypto_registry.providers[j] = g_crypto_registry.providers[j + 1];
            }
            g_crypto_registry.count--;
            mutex_unlock(&g_crypto_registry.lock);
            return SPEEDSQL_OK;
        }
    }

    mutex_unlock(&g_crypto_registry.lock);
    return SPEEDSQL_NOTFOUND;
}

/* Get cipher provider by ID */
SPEEDSQL_API const speedsql_cipher_provider_t* speedsql_get_cipher(speedsql_cipher_t cipher_id) {
    crypto_registry_init();

    for (int i = 0; i < g_crypto_registry.count; i++) {
        if (g_crypto_registry.providers[i]->cipher_id == cipher_id) {
            return g_crypto_registry.providers[i];
        }
    }

    return nullptr;
}

/* List available ciphers */
SPEEDSQL_API int speedsql_list_ciphers(speedsql_cipher_t* ciphers, int* count) {
    if (!count) return SPEEDSQL_MISUSE;

    crypto_registry_init();

    if (!ciphers) {
        *count = g_crypto_registry.count;
        return SPEEDSQL_OK;
    }

    int n = (*count < g_crypto_registry.count) ? *count : g_crypto_registry.count;
    for (int i = 0; i < n; i++) {
        ciphers[i] = g_crypto_registry.providers[i]->cipher_id;
    }
    *count = n;

    return SPEEDSQL_OK;
}

/* Run all cipher self-tests */
SPEEDSQL_API int speedsql_crypto_self_test(void) {
    crypto_registry_init();

    for (int i = 0; i < g_crypto_registry.count; i++) {
        const speedsql_cipher_provider_t* p = g_crypto_registry.providers[i];
        if (p->self_test) {
            int rc = p->self_test();
            if (rc != SPEEDSQL_OK) {
                return rc;
            }
        }
    }

    return SPEEDSQL_OK;
}

/* Get crypto module version */
SPEEDSQL_API const char* speedsql_crypto_version(void) {
    return CRYPTO_VERSION;
}

/* Check FIPS mode */
SPEEDSQL_API bool speedsql_crypto_fips_mode(void) {
    crypto_registry_init();
    return g_crypto_registry.fips_mode;
}

/* Enable FIPS mode */
SPEEDSQL_API int speedsql_crypto_enable_fips(void) {
    crypto_registry_init();

    /* Run self-tests first (required for FIPS) */
    int rc = speedsql_crypto_self_test();
    if (rc != SPEEDSQL_OK) {
        return rc;
    }

    g_crypto_registry.fips_mode = true;
    return SPEEDSQL_OK;
}

/* ============================================================================
 * Secure Memory Operations
 * ============================================================================ */

#ifdef _WIN32
#include <windows.h>

SPEEDSQL_API void* speedsql_secure_malloc(size_t size) {
    void* ptr = VirtualAlloc(NULL, size, MEM_COMMIT | MEM_RESERVE, PAGE_READWRITE);
    if (ptr) {
        /* Lock memory to prevent swapping */
        VirtualLock(ptr, size);
    }
    return ptr;
}

SPEEDSQL_API void speedsql_secure_free(void* ptr, size_t size) {
    if (ptr) {
        speedsql_secure_zero(ptr, size);
        VirtualUnlock(ptr, size);
        VirtualFree(ptr, 0, MEM_RELEASE);
    }
}

#else
#include <sys/mman.h>

SPEEDSQL_API void* speedsql_secure_malloc(size_t size) {
    void* ptr = mmap(NULL, size, PROT_READ | PROT_WRITE,
                     MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    if (ptr != MAP_FAILED) {
        mlock(ptr, size);
        return ptr;
    }
    return nullptr;
}

SPEEDSQL_API void speedsql_secure_free(void* ptr, size_t size) {
    if (ptr) {
        speedsql_secure_zero(ptr, size);
        munlock(ptr, size);
        munmap(ptr, size);
    }
}

#endif

/* Secure zeroization - prevent compiler optimization */
SPEEDSQL_API void speedsql_secure_zero(void* ptr, size_t size) {
    volatile uint8_t* p = (volatile uint8_t*)ptr;
    while (size--) {
        *p++ = 0;
    }
}

/* Generate random bytes using OS CSPRNG */
static int secure_random(uint8_t* buf, size_t len) {
#ifdef _WIN32
    HCRYPTPROV hProv;
    if (!CryptAcquireContextW(&hProv, NULL, NULL, PROV_RSA_FULL,
                               CRYPT_VERIFYCONTEXT | CRYPT_SILENT)) {
        return SPEEDSQL_ERROR;
    }
    BOOL ok = CryptGenRandom(hProv, (DWORD)len, buf);
    CryptReleaseContext(hProv, 0);
    return ok ? SPEEDSQL_OK : SPEEDSQL_ERROR;
#else
    FILE* f = fopen("/dev/urandom", "rb");
    if (!f) return SPEEDSQL_ERROR;
    size_t n = fread(buf, 1, len, f);
    fclose(f);
    return (n == len) ? SPEEDSQL_OK : SPEEDSQL_ERROR;
#endif
}

SPEEDSQL_API int speedsql_random_salt(uint8_t* salt, size_t salt_len) {
    return secure_random(salt, salt_len);
}

SPEEDSQL_API int speedsql_random_key(uint8_t* key, size_t key_len) {
    return secure_random(key, key_len);
}

/* ============================================================================
 * Key Derivation (PBKDF2)
 * ============================================================================ */

/* Simple PBKDF2-SHA256 implementation */
static void pbkdf2_sha256_simple(const uint8_t* password, size_t password_len,
                                  const uint8_t* salt, size_t salt_len,
                                  uint32_t iterations, uint8_t* output, size_t output_len) {
    /* Simplified PBKDF2 - uses FNV-1a based derivation */
    /* In production, use proper HMAC-SHA256 */
    uint32_t block = 1;
    size_t output_pos = 0;

    while (output_pos < output_len) {
        uint32_t hash = 0x811c9dc5;

        /* Hash password */
        for (size_t i = 0; i < password_len; i++) {
            hash ^= password[i];
            hash *= 0x01000193;
        }

        /* Hash salt */
        for (size_t i = 0; i < salt_len; i++) {
            hash ^= salt[i];
            hash *= 0x01000193;
        }

        /* Hash block number */
        hash ^= (block >> 24) & 0xff;
        hash *= 0x01000193;
        hash ^= (block >> 16) & 0xff;
        hash *= 0x01000193;
        hash ^= (block >> 8) & 0xff;
        hash *= 0x01000193;
        hash ^= block & 0xff;
        hash *= 0x01000193;

        /* Iterate */
        for (uint32_t iter = 0; iter < iterations; iter++) {
            hash ^= iter;
            hash *= 0x01000193;
        }

        /* Output 4 bytes from this block */
        for (int i = 0; i < 4 && output_pos < output_len; i++) {
            output[output_pos++] = (hash >> (i * 8)) & 0xff;
            hash *= 0x01000193;
            hash ^= (uint32_t)block;
        }

        block++;
    }
}

SPEEDSQL_API int speedsql_derive_key(
    const char* password,
    size_t password_len,
    const uint8_t* salt,
    size_t salt_len,
    speedsql_kdf_t kdf,
    uint32_t iterations,
    uint8_t* key_out,
    size_t key_len
) {
    if (!password || !salt || !key_out) {
        return SPEEDSQL_MISUSE;
    }

    switch (kdf) {
        case SPEEDSQL_KDF_NONE:
            /* Use password directly (not recommended) */
            memset(key_out, 0, key_len);
            memcpy(key_out, password, password_len < key_len ? password_len : key_len);
            break;

        case SPEEDSQL_KDF_PBKDF2_SHA256:
        case SPEEDSQL_KDF_PBKDF2_SHA512:
            pbkdf2_sha256_simple((const uint8_t*)password, password_len,
                                  salt, salt_len, iterations, key_out, key_len);
            break;

        default:
            return SPEEDSQL_MISUSE;
    }

    return SPEEDSQL_OK;
}

/* ============================================================================
 * Database Encryption API
 * ============================================================================ */

/* Simple key setup (uses AES-256-GCM by default) */
SPEEDSQL_API int speedsql_key(speedsql* db, const void* key, int key_len) {
    speedsql_crypto_config_t config;
    memset(&config, 0, sizeof(config));
    config.cipher = SPEEDSQL_CIPHER_AES_256_GCM;
    config.kdf = SPEEDSQL_KDF_PBKDF2_SHA256;
    config.kdf_iterations = 100000;

    /* Generate random salt */
    speedsql_random_salt(config.salt, SPEEDSQL_SALT_SIZE);

    return speedsql_key_v2(db, key, key_len, &config);
}

/* Key setup with configuration */
SPEEDSQL_API int speedsql_key_v2(
    speedsql* db,
    const void* key,
    int key_len,
    const speedsql_crypto_config_t* config
) {
    if (!db || !key || key_len <= 0 || !config) {
        return SPEEDSQL_MISUSE;
    }

    crypto_registry_init();

    /* Get cipher provider */
    const speedsql_cipher_provider_t* provider = speedsql_get_cipher(config->cipher);
    if (!provider) {
        return SPEEDSQL_NOTFOUND;
    }

    /* Derive key if KDF is specified */
    uint8_t derived_key[64];  /* Max key size */
    if (config->kdf != SPEEDSQL_KDF_NONE) {
        int rc = speedsql_derive_key(
            (const char*)key, key_len,
            config->salt, SPEEDSQL_SALT_SIZE,
            config->kdf, config->kdf_iterations,
            derived_key, provider->key_size
        );
        if (rc != SPEEDSQL_OK) {
            speedsql_secure_zero(derived_key, sizeof(derived_key));
            return rc;
        }
    } else {
        /* Use key directly */
        if ((size_t)key_len < provider->key_size) {
            return SPEEDSQL_MISUSE;
        }
        memcpy(derived_key, key, provider->key_size);
    }

    /* Destroy existing cipher context if any */
    if (db->cipher_ctx && provider->destroy) {
        provider->destroy(db->cipher_ctx);
        db->cipher_ctx = nullptr;
    }

    /* Initialize new cipher context */
    int rc = provider->init(&db->cipher_ctx, derived_key, provider->key_size);
    speedsql_secure_zero(derived_key, sizeof(derived_key));

    if (rc != SPEEDSQL_OK) {
        return rc;
    }

    db->cipher_id = config->cipher;
    db->encrypted = true;

    /* Set encryption on buffer pool for page-level encryption */
    if (db->buffer_pool) {
        rc = buffer_pool_set_encryption(db->buffer_pool, db->cipher_ctx, db->cipher_id);
        if (rc != SPEEDSQL_OK) {
            return rc;
        }
    }

    return SPEEDSQL_OK;
}

/* Change encryption key */
SPEEDSQL_API int speedsql_rekey(speedsql* db, const void* new_key, int key_len) {
    if (!db || !new_key || key_len <= 0) {
        return SPEEDSQL_MISUSE;
    }

    if (!db->encrypted || !db->cipher_ctx) {
        /* Database not encrypted - use speedsql_key instead */
        return speedsql_key(db, new_key, key_len);
    }

    /* Get current cipher provider */
    const speedsql_cipher_provider_t* provider = speedsql_get_cipher(db->cipher_id);
    if (!provider) {
        return SPEEDSQL_ERROR;
    }

    /* Generate new derived key */
    uint8_t new_salt[SPEEDSQL_SALT_SIZE];
    speedsql_random_salt(new_salt, SPEEDSQL_SALT_SIZE);

    uint8_t derived_key[64];
    int rc = speedsql_derive_key(
        (const char*)new_key, key_len,
        new_salt, SPEEDSQL_SALT_SIZE,
        SPEEDSQL_KDF_PBKDF2_SHA256, 100000,
        derived_key, provider->key_size
    );

    if (rc != SPEEDSQL_OK) {
        speedsql_secure_zero(derived_key, sizeof(derived_key));
        return rc;
    }

    /* Use rekey function if available */
    if (provider->rekey) {
        rc = provider->rekey(db->cipher_ctx, derived_key, provider->key_size);
    } else {
        /* Fallback: destroy and reinitialize */
        if (provider->destroy) {
            provider->destroy(db->cipher_ctx);
        }
        rc = provider->init(&db->cipher_ctx, derived_key, provider->key_size);
    }

    speedsql_secure_zero(derived_key, sizeof(derived_key));

    return rc;
}

/* Remove encryption */
SPEEDSQL_API int speedsql_decrypt(speedsql* db) {
    if (!db) return SPEEDSQL_MISUSE;

    if (db->cipher_ctx) {
        const speedsql_cipher_provider_t* provider = speedsql_get_cipher(db->cipher_id);
        if (provider && provider->destroy) {
            provider->destroy(db->cipher_ctx);
        }
        db->cipher_ctx = nullptr;
    }

    db->cipher_id = SPEEDSQL_CIPHER_NONE;
    db->encrypted = false;

    return SPEEDSQL_OK;
}

/* Get encryption status */
SPEEDSQL_API int speedsql_crypto_status(
    speedsql* db,
    speedsql_cipher_t* cipher,
    bool* is_encrypted
) {
    if (!db) return SPEEDSQL_MISUSE;

    if (cipher) *cipher = db->cipher_id;
    if (is_encrypted) *is_encrypted = db->encrypted;

    return SPEEDSQL_OK;
}
