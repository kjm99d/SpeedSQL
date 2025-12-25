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
