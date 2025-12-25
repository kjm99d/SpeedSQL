/*
 * SpeedSQL - No Encryption Cipher (Passthrough)
 *
 * Provides a cipher interface that does no encryption.
 * Useful for development, testing, and non-sensitive data.
 */

#include "speedsql_internal.h"
#include "speedsql_crypto.h"
#include <string.h>

/* Context for no-encryption (minimal state) */
struct speedsql_cipher_ctx {
    bool initialized;
};

static int cipher_none_init(speedsql_cipher_ctx_t** ctx,
                            const uint8_t* key, size_t key_len) {
    (void)key;
    (void)key_len;

    *ctx = (speedsql_cipher_ctx_t*)sdb_calloc(1, sizeof(speedsql_cipher_ctx_t));
    if (!*ctx) return SPEEDSQL_NOMEM;

    (*ctx)->initialized = true;
    return SPEEDSQL_OK;
}

static void cipher_none_destroy(speedsql_cipher_ctx_t* ctx) {
    if (ctx) {
        sdb_free(ctx);
    }
}

static int cipher_none_encrypt(
    speedsql_cipher_ctx_t* ctx,
    const uint8_t* plaintext,
    size_t plaintext_len,
    const uint8_t* iv,
    const uint8_t* aad,
    size_t aad_len,
    uint8_t* ciphertext,
    uint8_t* tag
) {
    (void)ctx;
    (void)iv;
    (void)aad;
    (void)aad_len;
    (void)tag;

    /* Just copy - no encryption */
    memcpy(ciphertext, plaintext, plaintext_len);
    return SPEEDSQL_OK;
}

static int cipher_none_decrypt(
    speedsql_cipher_ctx_t* ctx,
    const uint8_t* ciphertext,
    size_t ciphertext_len,
    const uint8_t* iv,
    const uint8_t* aad,
    size_t aad_len,
    const uint8_t* tag,
    uint8_t* plaintext
) {
    (void)ctx;
    (void)iv;
    (void)aad;
    (void)aad_len;
    (void)tag;

    /* Just copy - no decryption */
    memcpy(plaintext, ciphertext, ciphertext_len);
    return SPEEDSQL_OK;
}

static int cipher_none_rekey(speedsql_cipher_ctx_t* ctx,
                              const uint8_t* new_key, size_t key_len) {
    (void)ctx;
    (void)new_key;
    (void)key_len;
    return SPEEDSQL_OK;
}

static int cipher_none_self_test(void) {
    /* Always passes - no crypto to test */
    return SPEEDSQL_OK;
}

static void cipher_none_zeroize(speedsql_cipher_ctx_t* ctx) {
    if (ctx) {
        ctx->initialized = false;
    }
}

/* Factory function for C++17 compatibility */
static speedsql_cipher_provider_t make_cipher_none() {
    speedsql_cipher_provider_t p = {};
    p.name = "NONE";
    p.version = "1.0.0";
    p.cipher_id = SPEEDSQL_CIPHER_NONE;
    p.key_size = 0;
    p.iv_size = 0;
    p.tag_size = 0;
    p.block_size = 1;
    p.init = cipher_none_init;
    p.destroy = cipher_none_destroy;
    p.encrypt = cipher_none_encrypt;
    p.decrypt = cipher_none_decrypt;
    p.rekey = cipher_none_rekey;
    p.self_test = cipher_none_self_test;
    p.zeroize = cipher_none_zeroize;
    return p;
}

/* Global provider instance */
extern "C" const speedsql_cipher_provider_t g_cipher_none = make_cipher_none();
