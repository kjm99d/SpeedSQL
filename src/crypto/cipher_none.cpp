/*
 * SpeedDB - No Encryption Cipher (Passthrough)
 *
 * Provides a cipher interface that does no encryption.
 * Useful for development, testing, and non-sensitive data.
 */

#include "speeddb_internal.h"
#include "speeddb_crypto.h"
#include <string.h>

/* Context for no-encryption (minimal state) */
struct speeddb_cipher_ctx {
    bool initialized;
};

static int cipher_none_init(speeddb_cipher_ctx_t** ctx,
                            const uint8_t* key, size_t key_len) {
    (void)key;
    (void)key_len;

    *ctx = (speeddb_cipher_ctx_t*)sdb_calloc(1, sizeof(speeddb_cipher_ctx_t));
    if (!*ctx) return SPEEDDB_NOMEM;

    (*ctx)->initialized = true;
    return SPEEDDB_OK;
}

static void cipher_none_destroy(speeddb_cipher_ctx_t* ctx) {
    if (ctx) {
        sdb_free(ctx);
    }
}

static int cipher_none_encrypt(
    speeddb_cipher_ctx_t* ctx,
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
    return SPEEDDB_OK;
}

static int cipher_none_decrypt(
    speeddb_cipher_ctx_t* ctx,
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
    return SPEEDDB_OK;
}

static int cipher_none_rekey(speeddb_cipher_ctx_t* ctx,
                              const uint8_t* new_key, size_t key_len) {
    (void)ctx;
    (void)new_key;
    (void)key_len;
    return SPEEDDB_OK;
}

static int cipher_none_self_test(void) {
    /* Always passes - no crypto to test */
    return SPEEDDB_OK;
}

static void cipher_none_zeroize(speeddb_cipher_ctx_t* ctx) {
    if (ctx) {
        ctx->initialized = false;
    }
}

/* Global provider instance */
const speeddb_cipher_provider_t g_cipher_none = {
    .name = "NONE",
    .version = "1.0.0",
    .cipher_id = SPEEDDB_CIPHER_NONE,
    .key_size = 0,
    .iv_size = 0,
    .tag_size = 0,
    .block_size = 1,
    .init = cipher_none_init,
    .destroy = cipher_none_destroy,
    .encrypt = cipher_none_encrypt,
    .decrypt = cipher_none_decrypt,
    .rekey = cipher_none_rekey,
    .self_test = cipher_none_self_test,
    .zeroize = cipher_none_zeroize
};
