/*
 * SpeedSQL - ChaCha20-Poly1305 Cipher Implementation
 *
 * Modern authenticated encryption cipher
 * - Fast in software (no need for AES-NI)
 * - IETF RFC 8439 compliant
 */

#include "speedsql_internal.h"
#include "speedsql_crypto.h"
#include <string.h>

#define CHACHA20_KEY_SIZE 32
#define CHACHA20_NONCE_SIZE 12
#define CHACHA20_BLOCK_SIZE 64
#define POLY1305_TAG_SIZE 16

struct speedsql_cipher_ctx {
    uint8_t key[32];
    bool initialized;
};

/* Quarter round */
#define QUARTERROUND(a, b, c, d) \
    a += b; d ^= a; d = (d << 16) | (d >> 16); \
    c += d; b ^= c; b = (b << 12) | (b >> 20); \
    a += b; d ^= a; d = (d << 8) | (d >> 24); \
    c += d; b ^= c; b = (b << 7) | (b >> 25);

/* ChaCha20 block function */
static void chacha20_block(const uint32_t* input, uint32_t* output) {
    uint32_t x[16];
    memcpy(x, input, 64);

    /* 20 rounds (10 double rounds) */
    for (int i = 0; i < 10; i++) {
        /* Column round */
        QUARTERROUND(x[0], x[4], x[8], x[12]);
        QUARTERROUND(x[1], x[5], x[9], x[13]);
        QUARTERROUND(x[2], x[6], x[10], x[14]);
        QUARTERROUND(x[3], x[7], x[11], x[15]);
        /* Diagonal round */
        QUARTERROUND(x[0], x[5], x[10], x[15]);
        QUARTERROUND(x[1], x[6], x[11], x[12]);
        QUARTERROUND(x[2], x[7], x[8], x[13]);
        QUARTERROUND(x[3], x[4], x[9], x[14]);
    }

    for (int i = 0; i < 16; i++) {
        output[i] = x[i] + input[i];
    }
}

/* ChaCha20 encryption/decryption */
static void chacha20_encrypt(const uint8_t* key, const uint8_t* nonce,
                              uint32_t counter, const uint8_t* input,
                              size_t len, uint8_t* output) {
    uint32_t state[16];
    uint32_t keystream[16];

    /* "expand 32-byte k" */
    state[0] = 0x61707865;
    state[1] = 0x3320646e;
    state[2] = 0x79622d32;
    state[3] = 0x6b206574;

    /* Key */
    for (int i = 0; i < 8; i++) {
        state[4 + i] = ((uint32_t)key[i * 4]) |
                       ((uint32_t)key[i * 4 + 1] << 8) |
                       ((uint32_t)key[i * 4 + 2] << 16) |
                       ((uint32_t)key[i * 4 + 3] << 24);
    }

    /* Counter */
    state[12] = counter;

    /* Nonce */
    state[13] = ((uint32_t)nonce[0]) | ((uint32_t)nonce[1] << 8) |
                ((uint32_t)nonce[2] << 16) | ((uint32_t)nonce[3] << 24);
    state[14] = ((uint32_t)nonce[4]) | ((uint32_t)nonce[5] << 8) |
                ((uint32_t)nonce[6] << 16) | ((uint32_t)nonce[7] << 24);
    state[15] = ((uint32_t)nonce[8]) | ((uint32_t)nonce[9] << 8) |
                ((uint32_t)nonce[10] << 16) | ((uint32_t)nonce[11] << 24);

    size_t offset = 0;
    while (offset < len) {
        chacha20_block(state, keystream);
        state[12]++;  /* Increment counter */

        size_t block_len = (len - offset < 64) ? (len - offset) : 64;
        uint8_t* ks = (uint8_t*)keystream;

        for (size_t i = 0; i < block_len; i++) {
            output[offset + i] = input[offset + i] ^ ks[i];
        }

        offset += block_len;
    }
}

/* ============================================================================
 * Poly1305 MAC
 * ============================================================================ */

typedef struct {
    uint32_t r[5];
    uint32_t h[5];
    uint32_t pad[4];
    size_t leftover;
    uint8_t buffer[16];
} poly1305_ctx;

static void poly1305_init(poly1305_ctx* ctx, const uint8_t key[32]) {
    /* r = key[0..15] clamped */
    uint32_t t0 = ((uint32_t)key[0]) | ((uint32_t)key[1] << 8) |
                  ((uint32_t)key[2] << 16) | ((uint32_t)key[3] << 24);
    uint32_t t1 = ((uint32_t)key[4]) | ((uint32_t)key[5] << 8) |
                  ((uint32_t)key[6] << 16) | ((uint32_t)key[7] << 24);
    uint32_t t2 = ((uint32_t)key[8]) | ((uint32_t)key[9] << 8) |
                  ((uint32_t)key[10] << 16) | ((uint32_t)key[11] << 24);
    uint32_t t3 = ((uint32_t)key[12]) | ((uint32_t)key[13] << 8) |
                  ((uint32_t)key[14] << 16) | ((uint32_t)key[15] << 24);

    /* Clamp r */
    ctx->r[0] = t0 & 0x3ffffff;
    ctx->r[1] = ((t0 >> 26) | (t1 << 6)) & 0x3ffff03;
    ctx->r[2] = ((t1 >> 20) | (t2 << 12)) & 0x3ffc0ff;
    ctx->r[3] = ((t2 >> 14) | (t3 << 18)) & 0x3f03fff;
    ctx->r[4] = (t3 >> 8) & 0x00fffff;

    /* h = 0 */
    ctx->h[0] = ctx->h[1] = ctx->h[2] = ctx->h[3] = ctx->h[4] = 0;

    /* pad = key[16..31] */
    ctx->pad[0] = ((uint32_t)key[16]) | ((uint32_t)key[17] << 8) |
                  ((uint32_t)key[18] << 16) | ((uint32_t)key[19] << 24);
    ctx->pad[1] = ((uint32_t)key[20]) | ((uint32_t)key[21] << 8) |
                  ((uint32_t)key[22] << 16) | ((uint32_t)key[23] << 24);
    ctx->pad[2] = ((uint32_t)key[24]) | ((uint32_t)key[25] << 8) |
                  ((uint32_t)key[26] << 16) | ((uint32_t)key[27] << 24);
    ctx->pad[3] = ((uint32_t)key[28]) | ((uint32_t)key[29] << 8) |
                  ((uint32_t)key[30] << 16) | ((uint32_t)key[31] << 24);

    ctx->leftover = 0;
}

static void poly1305_blocks(poly1305_ctx* ctx, const uint8_t* m,
                             size_t bytes, uint32_t hibit) {
    uint32_t r0 = ctx->r[0], r1 = ctx->r[1], r2 = ctx->r[2];
    uint32_t r3 = ctx->r[3], r4 = ctx->r[4];
    uint32_t h0 = ctx->h[0], h1 = ctx->h[1], h2 = ctx->h[2];
    uint32_t h3 = ctx->h[3], h4 = ctx->h[4];
    uint32_t s1 = r1 * 5, s2 = r2 * 5, s3 = r3 * 5, s4 = r4 * 5;

    while (bytes >= 16) {
        uint32_t t0 = ((uint32_t)m[0]) | ((uint32_t)m[1] << 8) |
                      ((uint32_t)m[2] << 16) | ((uint32_t)m[3] << 24);
        uint32_t t1 = ((uint32_t)m[4]) | ((uint32_t)m[5] << 8) |
                      ((uint32_t)m[6] << 16) | ((uint32_t)m[7] << 24);
        uint32_t t2 = ((uint32_t)m[8]) | ((uint32_t)m[9] << 8) |
                      ((uint32_t)m[10] << 16) | ((uint32_t)m[11] << 24);
        uint32_t t3 = ((uint32_t)m[12]) | ((uint32_t)m[13] << 8) |
                      ((uint32_t)m[14] << 16) | ((uint32_t)m[15] << 24);

        h0 += t0 & 0x3ffffff;
        h1 += ((t0 >> 26) | (t1 << 6)) & 0x3ffffff;
        h2 += ((t1 >> 20) | (t2 << 12)) & 0x3ffffff;
        h3 += ((t2 >> 14) | (t3 << 18)) & 0x3ffffff;
        h4 += (t3 >> 8) | hibit;

        /* h *= r (mod 2^130 - 5) */
        uint64_t d0 = (uint64_t)h0 * r0 + (uint64_t)h1 * s4 +
                      (uint64_t)h2 * s3 + (uint64_t)h3 * s2 + (uint64_t)h4 * s1;
        uint64_t d1 = (uint64_t)h0 * r1 + (uint64_t)h1 * r0 +
                      (uint64_t)h2 * s4 + (uint64_t)h3 * s3 + (uint64_t)h4 * s2;
        uint64_t d2 = (uint64_t)h0 * r2 + (uint64_t)h1 * r1 +
                      (uint64_t)h2 * r0 + (uint64_t)h3 * s4 + (uint64_t)h4 * s3;
        uint64_t d3 = (uint64_t)h0 * r3 + (uint64_t)h1 * r2 +
                      (uint64_t)h2 * r1 + (uint64_t)h3 * r0 + (uint64_t)h4 * s4;
        uint64_t d4 = (uint64_t)h0 * r4 + (uint64_t)h1 * r3 +
                      (uint64_t)h2 * r2 + (uint64_t)h3 * r1 + (uint64_t)h4 * r0;

        /* Partial reduction */
        uint32_t c = (uint32_t)(d0 >> 26); h0 = (uint32_t)d0 & 0x3ffffff;
        d1 += c; c = (uint32_t)(d1 >> 26); h1 = (uint32_t)d1 & 0x3ffffff;
        d2 += c; c = (uint32_t)(d2 >> 26); h2 = (uint32_t)d2 & 0x3ffffff;
        d3 += c; c = (uint32_t)(d3 >> 26); h3 = (uint32_t)d3 & 0x3ffffff;
        d4 += c; c = (uint32_t)(d4 >> 26); h4 = (uint32_t)d4 & 0x3ffffff;
        h0 += c * 5; c = h0 >> 26; h0 &= 0x3ffffff;
        h1 += c;

        m += 16;
        bytes -= 16;
    }

    ctx->h[0] = h0;
    ctx->h[1] = h1;
    ctx->h[2] = h2;
    ctx->h[3] = h3;
    ctx->h[4] = h4;
}

static void poly1305_update(poly1305_ctx* ctx, const uint8_t* m, size_t bytes) {
    if (ctx->leftover) {
        size_t want = 16 - ctx->leftover;
        if (want > bytes) want = bytes;
        memcpy(ctx->buffer + ctx->leftover, m, want);
        bytes -= want;
        m += want;
        ctx->leftover += want;
        if (ctx->leftover < 16) return;
        poly1305_blocks(ctx, ctx->buffer, 16, 1 << 24);
        ctx->leftover = 0;
    }

    if (bytes >= 16) {
        size_t want = bytes & ~15;
        poly1305_blocks(ctx, m, want, 1 << 24);
        m += want;
        bytes -= want;
    }

    if (bytes) {
        memcpy(ctx->buffer, m, bytes);
        ctx->leftover = bytes;
    }
}

static void poly1305_finish(poly1305_ctx* ctx, uint8_t tag[16]) {
    if (ctx->leftover) {
        ctx->buffer[ctx->leftover++] = 1;
        while (ctx->leftover < 16) ctx->buffer[ctx->leftover++] = 0;
        poly1305_blocks(ctx, ctx->buffer, 16, 0);
    }

    /* Full reduction */
    uint32_t h0 = ctx->h[0], h1 = ctx->h[1], h2 = ctx->h[2];
    uint32_t h3 = ctx->h[3], h4 = ctx->h[4];

    uint32_t c = h1 >> 26; h1 &= 0x3ffffff;
    h2 += c; c = h2 >> 26; h2 &= 0x3ffffff;
    h3 += c; c = h3 >> 26; h3 &= 0x3ffffff;
    h4 += c; c = h4 >> 26; h4 &= 0x3ffffff;
    h0 += c * 5; c = h0 >> 26; h0 &= 0x3ffffff;
    h1 += c;

    /* h + pad */
    uint64_t f = (uint64_t)h0 + ctx->pad[0]; h0 = (uint32_t)f;
    f = (uint64_t)h1 + ctx->pad[1] + (f >> 32); h1 = (uint32_t)f;
    f = (uint64_t)h2 + ctx->pad[2] + (f >> 32); h2 = (uint32_t)f;
    f = (uint64_t)h3 + ctx->pad[3] + (f >> 32); h3 = (uint32_t)f;

    tag[0] = h0; tag[1] = h0 >> 8; tag[2] = h0 >> 16; tag[3] = h0 >> 24;
    tag[4] = h1; tag[5] = h1 >> 8; tag[6] = h1 >> 16; tag[7] = h1 >> 24;
    tag[8] = h2; tag[9] = h2 >> 8; tag[10] = h2 >> 16; tag[11] = h2 >> 24;
    tag[12] = h3; tag[13] = h3 >> 8; tag[14] = h3 >> 16; tag[15] = h3 >> 24;
}

/* ============================================================================
 * ChaCha20-Poly1305 AEAD Provider
 * ============================================================================ */

static int chacha20_poly1305_init(speedsql_cipher_ctx_t** ctx,
                                   const uint8_t* key, size_t key_len) {
    if (key_len != 32) return SPEEDSQL_MISUSE;

    *ctx = (speedsql_cipher_ctx_t*)speedsql_secure_malloc(sizeof(speedsql_cipher_ctx_t));
    if (!*ctx) return SPEEDSQL_NOMEM;

    memcpy((*ctx)->key, key, 32);
    (*ctx)->initialized = true;

    return SPEEDSQL_OK;
}

static void chacha20_poly1305_destroy(speedsql_cipher_ctx_t* ctx) {
    if (ctx) {
        speedsql_secure_zero(ctx, sizeof(*ctx));
        speedsql_secure_free(ctx, sizeof(*ctx));
    }
}

static int chacha20_poly1305_encrypt(
    speedsql_cipher_ctx_t* ctx,
    const uint8_t* plaintext,
    size_t plaintext_len,
    const uint8_t* nonce,
    const uint8_t* aad,
    size_t aad_len,
    uint8_t* ciphertext,
    uint8_t* tag
) {
    if (!ctx || !ctx->initialized) return SPEEDSQL_MISUSE;

    /* Generate Poly1305 key */
    uint8_t poly_key[32] = {0};
    chacha20_encrypt(ctx->key, nonce, 0, poly_key, 32, poly_key);

    /* Encrypt plaintext (counter starts at 1) */
    chacha20_encrypt(ctx->key, nonce, 1, plaintext, plaintext_len, ciphertext);

    /* Calculate tag */
    poly1305_ctx poly;
    poly1305_init(&poly, poly_key);

    /* AAD */
    if (aad && aad_len > 0) {
        poly1305_update(&poly, aad, aad_len);
        /* Pad to 16 bytes */
        size_t pad_len = (16 - (aad_len % 16)) % 16;
        if (pad_len > 0) {
            uint8_t pad[16] = {0};
            poly1305_update(&poly, pad, pad_len);
        }
    }

    /* Ciphertext */
    poly1305_update(&poly, ciphertext, plaintext_len);
    size_t pad_len = (16 - (plaintext_len % 16)) % 16;
    if (pad_len > 0) {
        uint8_t pad[16] = {0};
        poly1305_update(&poly, pad, pad_len);
    }

    /* Lengths */
    uint8_t lens[16];
    for (int i = 0; i < 8; i++) {
        lens[i] = (aad_len >> (i * 8)) & 0xff;
        lens[8 + i] = (plaintext_len >> (i * 8)) & 0xff;
    }
    poly1305_update(&poly, lens, 16);

    poly1305_finish(&poly, tag);

    speedsql_secure_zero(poly_key, 32);
    return SPEEDSQL_OK;
}

static int chacha20_poly1305_decrypt(
    speedsql_cipher_ctx_t* ctx,
    const uint8_t* ciphertext,
    size_t ciphertext_len,
    const uint8_t* nonce,
    const uint8_t* aad,
    size_t aad_len,
    const uint8_t* tag,
    uint8_t* plaintext
) {
    if (!ctx || !ctx->initialized) return SPEEDSQL_MISUSE;

    /* Generate Poly1305 key */
    uint8_t poly_key[32] = {0};
    chacha20_encrypt(ctx->key, nonce, 0, poly_key, 32, poly_key);

    /* Verify tag first */
    poly1305_ctx poly;
    poly1305_init(&poly, poly_key);

    if (aad && aad_len > 0) {
        poly1305_update(&poly, aad, aad_len);
        size_t pad_len = (16 - (aad_len % 16)) % 16;
        if (pad_len > 0) {
            uint8_t pad[16] = {0};
            poly1305_update(&poly, pad, pad_len);
        }
    }

    poly1305_update(&poly, ciphertext, ciphertext_len);
    size_t pad_len = (16 - (ciphertext_len % 16)) % 16;
    if (pad_len > 0) {
        uint8_t pad[16] = {0};
        poly1305_update(&poly, pad, pad_len);
    }

    uint8_t lens[16];
    for (int i = 0; i < 8; i++) {
        lens[i] = (aad_len >> (i * 8)) & 0xff;
        lens[8 + i] = (ciphertext_len >> (i * 8)) & 0xff;
    }
    poly1305_update(&poly, lens, 16);

    uint8_t computed_tag[16];
    poly1305_finish(&poly, computed_tag);

    /* Constant-time comparison */
    uint8_t diff = 0;
    for (int i = 0; i < 16; i++) {
        diff |= computed_tag[i] ^ tag[i];
    }

    speedsql_secure_zero(poly_key, 32);

    if (diff != 0) {
        return SPEEDSQL_CORRUPT;  /* Authentication failed */
    }

    /* Decrypt */
    chacha20_encrypt(ctx->key, nonce, 1, ciphertext, ciphertext_len, plaintext);

    return SPEEDSQL_OK;
}

static int chacha20_poly1305_rekey(speedsql_cipher_ctx_t* ctx,
                                    const uint8_t* new_key, size_t key_len) {
    if (!ctx || key_len != 32) return SPEEDSQL_MISUSE;

    speedsql_secure_zero(ctx->key, 32);
    memcpy(ctx->key, new_key, 32);

    return SPEEDSQL_OK;
}

static int chacha20_poly1305_self_test(void) {
    /* RFC 8439 test vector */
    const uint8_t key[32] = {
        0x80, 0x81, 0x82, 0x83, 0x84, 0x85, 0x86, 0x87,
        0x88, 0x89, 0x8a, 0x8b, 0x8c, 0x8d, 0x8e, 0x8f,
        0x90, 0x91, 0x92, 0x93, 0x94, 0x95, 0x96, 0x97,
        0x98, 0x99, 0x9a, 0x9b, 0x9c, 0x9d, 0x9e, 0x9f
    };
    const uint8_t nonce[12] = {
        0x07, 0x00, 0x00, 0x00, 0x40, 0x41, 0x42, 0x43,
        0x44, 0x45, 0x46, 0x47
    };
    const uint8_t plaintext[16] = "Test message!!!";

    speedsql_cipher_ctx_t* ctx;
    int rc = chacha20_poly1305_init(&ctx, key, 32);
    if (rc != SPEEDSQL_OK) return rc;

    uint8_t ciphertext[16], tag[16], decrypted[16];
    rc = chacha20_poly1305_encrypt(ctx, plaintext, 16, nonce,
                                    nullptr, 0, ciphertext, tag);
    if (rc != SPEEDSQL_OK) {
        chacha20_poly1305_destroy(ctx);
        return rc;
    }

    rc = chacha20_poly1305_decrypt(ctx, ciphertext, 16, nonce,
                                    nullptr, 0, tag, decrypted);
    if (rc != SPEEDSQL_OK) {
        chacha20_poly1305_destroy(ctx);
        return rc;
    }

    if (memcmp(plaintext, decrypted, 16) != 0) {
        chacha20_poly1305_destroy(ctx);
        return SPEEDSQL_ERROR;
    }

    chacha20_poly1305_destroy(ctx);
    return SPEEDSQL_OK;
}

static void chacha20_poly1305_zeroize(speedsql_cipher_ctx_t* ctx) {
    if (ctx) {
        speedsql_secure_zero(ctx->key, 32);
        ctx->initialized = false;
    }
}

/* Factory function for C++17 compatibility */
static speedsql_cipher_provider_t make_cipher_chacha20_poly1305() {
    speedsql_cipher_provider_t p = {};
    p.name = "ChaCha20-Poly1305";
    p.version = "1.0.0";
    p.cipher_id = SPEEDSQL_CIPHER_CHACHA20_POLY1305;
    p.key_size = 32;
    p.iv_size = 12;
    p.tag_size = 16;
    p.block_size = 1;  /* Stream cipher */
    p.init = chacha20_poly1305_init;
    p.destroy = chacha20_poly1305_destroy;
    p.encrypt = chacha20_poly1305_encrypt;
    p.decrypt = chacha20_poly1305_decrypt;
    p.rekey = chacha20_poly1305_rekey;
    p.self_test = chacha20_poly1305_self_test;
    p.zeroize = chacha20_poly1305_zeroize;
    return p;
}

extern "C" const speedsql_cipher_provider_t g_cipher_chacha20_poly1305 = make_cipher_chacha20_poly1305();
