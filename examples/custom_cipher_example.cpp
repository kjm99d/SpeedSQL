/*
 * SpeedSQL - Custom Cipher Provider Example
 *
 * Shows how to register a custom encryption algorithm
 * for CC certification with proprietary ciphers
 */

#include "speedsql.h"
#include "speedsql_crypto.h"
#include <stdio.h>
#include <string.h>

/* ============================================================================
 * Custom XOR Cipher (for demonstration only - NOT secure!)
 * In real use, implement your CC-certified cipher here
 * ============================================================================ */

struct custom_cipher_ctx {
    uint8_t key[32];
    bool initialized;
};

static int custom_init(speedsql_cipher_ctx_t** ctx,
                       const uint8_t* key, size_t key_len) {
    if (key_len != 32) return SPEEDSQL_MISUSE;

    *ctx = (speedsql_cipher_ctx_t*)malloc(sizeof(custom_cipher_ctx));
    if (!*ctx) return SPEEDSQL_NOMEM;

    custom_cipher_ctx* c = (custom_cipher_ctx*)*ctx;
    memcpy(c->key, key, 32);
    c->initialized = true;

    return SPEEDSQL_OK;
}

static void custom_destroy(speedsql_cipher_ctx_t* ctx) {
    if (ctx) {
        memset(ctx, 0, sizeof(custom_cipher_ctx));
        free(ctx);
    }
}

static int custom_encrypt(
    speedsql_cipher_ctx_t* ctx,
    const uint8_t* plaintext,
    size_t len,
    const uint8_t* iv,
    const uint8_t* aad,
    size_t aad_len,
    uint8_t* ciphertext,
    uint8_t* tag
) {
    (void)iv;
    (void)aad;
    (void)aad_len;

    custom_cipher_ctx* c = (custom_cipher_ctx*)ctx;
    if (!c || !c->initialized) return SPEEDSQL_MISUSE;

    /* Simple XOR (NOT secure - demo only) */
    for (size_t i = 0; i < len; i++) {
        ciphertext[i] = plaintext[i] ^ c->key[i % 32];
    }

    /* Generate tag (demo: just hash of ciphertext) */
    if (tag) {
        memset(tag, 0, 16);
        for (size_t i = 0; i < len; i++) {
            tag[i % 16] ^= ciphertext[i];
        }
    }

    return SPEEDSQL_OK;
}

static int custom_decrypt(
    speedsql_cipher_ctx_t* ctx,
    const uint8_t* ciphertext,
    size_t len,
    const uint8_t* iv,
    const uint8_t* aad,
    size_t aad_len,
    const uint8_t* tag,
    uint8_t* plaintext
) {
    (void)tag;  /* Skip tag verification for demo */
    return custom_encrypt(ctx, ciphertext, len, iv, aad, aad_len, plaintext, nullptr);
}

static int custom_rekey(speedsql_cipher_ctx_t* ctx,
                        const uint8_t* new_key, size_t key_len) {
    if (!ctx || key_len != 32) return SPEEDSQL_MISUSE;

    custom_cipher_ctx* c = (custom_cipher_ctx*)ctx;
    memcpy(c->key, new_key, 32);

    return SPEEDSQL_OK;
}

static int custom_self_test(void) {
    const uint8_t key[32] = {0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,
                             0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10,
                             0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18,
                             0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f, 0x20};
    const uint8_t plaintext[16] = "Test data here!";
    uint8_t ciphertext[16], decrypted[16], tag[16];

    speedsql_cipher_ctx_t* ctx;
    int rc = custom_init(&ctx, key, 32);
    if (rc != SPEEDSQL_OK) return rc;

    rc = custom_encrypt(ctx, plaintext, 16, nullptr, nullptr, 0, ciphertext, tag);
    if (rc != SPEEDSQL_OK) {
        custom_destroy(ctx);
        return rc;
    }

    rc = custom_decrypt(ctx, ciphertext, 16, nullptr, nullptr, 0, tag, decrypted);
    if (rc != SPEEDSQL_OK) {
        custom_destroy(ctx);
        return rc;
    }

    if (memcmp(plaintext, decrypted, 16) != 0) {
        custom_destroy(ctx);
        return SPEEDSQL_ERROR;
    }

    custom_destroy(ctx);
    return SPEEDSQL_OK;
}

static void custom_zeroize(speedsql_cipher_ctx_t* ctx) {
    if (ctx) {
        custom_cipher_ctx* c = (custom_cipher_ctx*)ctx;
        memset(c->key, 0, 32);
        c->initialized = false;
    }
}

/* Custom cipher provider definition */
static const speedsql_cipher_provider_t my_custom_cipher = {
    .name = "CUSTOM-XOR-256",
    .version = "1.0.0-demo",
    .cipher_id = (speedsql_cipher_t)(SPEEDSQL_CIPHER_CUSTOM + 1),  /* Custom ID > 100 */
    .key_size = 32,
    .iv_size = 0,
    .tag_size = 16,
    .block_size = 1,
    .init = custom_init,
    .destroy = custom_destroy,
    .encrypt = custom_encrypt,
    .decrypt = custom_decrypt,
    .rekey = custom_rekey,
    .self_test = custom_self_test,
    .zeroize = custom_zeroize
};

/* ============================================================================
 * Main Example
 * ============================================================================ */

int main() {
    printf("SpeedSQL Custom Cipher Example\n");
    printf("==============================\n\n");

    /* Step 1: Register custom cipher */
    printf("1. Registering custom cipher...\n");

    int rc = speedsql_register_cipher(&my_custom_cipher);
    if (rc == SPEEDSQL_OK) {
        printf("   Registered: %s v%s (ID: %d)\n",
               my_custom_cipher.name,
               my_custom_cipher.version,
               my_custom_cipher.cipher_id);
    } else {
        printf("   Registration failed: %d\n", rc);
        return 1;
    }

    /* Step 2: Verify registration */
    printf("\n2. Verifying registration...\n");

    const speedsql_cipher_provider_t* provider =
        speedsql_get_cipher((speedsql_cipher_t)(SPEEDSQL_CIPHER_CUSTOM + 1));

    if (provider) {
        printf("   Found: %s\n", provider->name);
        printf("   Key size: %zu bytes\n", provider->key_size);
    } else {
        printf("   Cipher not found!\n");
        return 1;
    }

    /* Step 3: Run self-test */
    printf("\n3. Running self-test...\n");

    rc = provider->self_test();
    if (rc == SPEEDSQL_OK) {
        printf("   Self-test PASSED\n");
    } else {
        printf("   Self-test FAILED\n");
        return 1;
    }

    /* Step 4: Use custom cipher with database */
    printf("\n4. Using custom cipher with database...\n");

    speedsql* db;
    speedsql_open("custom_cipher.sdb", &db);

    speedsql_crypto_config_t config = {};
    config.cipher = (speedsql_cipher_t)(SPEEDSQL_CIPHER_CUSTOM + 1);
    config.kdf = SPEEDSQL_KDF_PBKDF2_SHA256;
    config.kdf_iterations = 10000;

    rc = speedsql_key_v2(db, "custom_password", 15, &config);
    if (rc == SPEEDSQL_OK) {
        printf("   Database encrypted with custom cipher\n");
    }

    speedsql_exec(db,
        "CREATE TABLE custom_data (id INTEGER, value BLOB)",
        nullptr, nullptr, nullptr);

    speedsql_close(db);

    /* Step 5: List all ciphers including custom */
    printf("\n5. All registered ciphers:\n");

    int count = 0;
    speedsql_list_ciphers(nullptr, &count);

    speedsql_cipher_t* ciphers = new speedsql_cipher_t[count];
    speedsql_list_ciphers(ciphers, &count);

    for (int i = 0; i < count; i++) {
        const speedsql_cipher_provider_t* p = speedsql_get_cipher(ciphers[i]);
        if (p) {
            const char* type = (ciphers[i] >= SPEEDSQL_CIPHER_CUSTOM) ? "[CUSTOM]" : "[BUILT-IN]";
            printf("   %s %s\n", type, p->name);
        }
    }

    delete[] ciphers;

    /* Step 6: Unregister (optional) */
    printf("\n6. Unregistering custom cipher...\n");

    rc = speedsql_unregister_cipher((speedsql_cipher_t)(SPEEDSQL_CIPHER_CUSTOM + 1));
    if (rc == SPEEDSQL_OK) {
        printf("   Unregistered successfully\n");
    }

    printf("\nCustom cipher example completed!\n");
    return 0;
}
