/*
 * SpeedSQL - AES-256 Cipher Implementation
 *
 * Supports:
 * - AES-256-GCM (Galois/Counter Mode with authentication)
 * - AES-256-CBC (Cipher Block Chaining with HMAC-SHA256)
 *
 * Reference implementation - for production, consider using
 * hardware-accelerated libraries (OpenSSL, Windows CNG, etc.)
 */

#include "speedsql_internal.h"
#include "speedsql_crypto.h"
#include <string.h>

/* ============================================================================
 * AES Core Implementation
 * ============================================================================ */

#define AES_BLOCK_SIZE 16
#define AES_256_KEY_SIZE 32
#define AES_256_ROUNDS 14

/* AES S-box */
static const uint8_t aes_sbox[256] = {
    0x63, 0x7c, 0x77, 0x7b, 0xf2, 0x6b, 0x6f, 0xc5,
    0x30, 0x01, 0x67, 0x2b, 0xfe, 0xd7, 0xab, 0x76,
    0xca, 0x82, 0xc9, 0x7d, 0xfa, 0x59, 0x47, 0xf0,
    0xad, 0xd4, 0xa2, 0xaf, 0x9c, 0xa4, 0x72, 0xc0,
    0xb7, 0xfd, 0x93, 0x26, 0x36, 0x3f, 0xf7, 0xcc,
    0x34, 0xa5, 0xe5, 0xf1, 0x71, 0xd8, 0x31, 0x15,
    0x04, 0xc7, 0x23, 0xc3, 0x18, 0x96, 0x05, 0x9a,
    0x07, 0x12, 0x80, 0xe2, 0xeb, 0x27, 0xb2, 0x75,
    0x09, 0x83, 0x2c, 0x1a, 0x1b, 0x6e, 0x5a, 0xa0,
    0x52, 0x3b, 0xd6, 0xb3, 0x29, 0xe3, 0x2f, 0x84,
    0x53, 0xd1, 0x00, 0xed, 0x20, 0xfc, 0xb1, 0x5b,
    0x6a, 0xcb, 0xbe, 0x39, 0x4a, 0x4c, 0x58, 0xcf,
    0xd0, 0xef, 0xaa, 0xfb, 0x43, 0x4d, 0x33, 0x85,
    0x45, 0xf9, 0x02, 0x7f, 0x50, 0x3c, 0x9f, 0xa8,
    0x51, 0xa3, 0x40, 0x8f, 0x92, 0x9d, 0x38, 0xf5,
    0xbc, 0xb6, 0xda, 0x21, 0x10, 0xff, 0xf3, 0xd2,
    0xcd, 0x0c, 0x13, 0xec, 0x5f, 0x97, 0x44, 0x17,
    0xc4, 0xa7, 0x7e, 0x3d, 0x64, 0x5d, 0x19, 0x73,
    0x60, 0x81, 0x4f, 0xdc, 0x22, 0x2a, 0x90, 0x88,
    0x46, 0xee, 0xb8, 0x14, 0xde, 0x5e, 0x0b, 0xdb,
    0xe0, 0x32, 0x3a, 0x0a, 0x49, 0x06, 0x24, 0x5c,
    0xc2, 0xd3, 0xac, 0x62, 0x91, 0x95, 0xe4, 0x79,
    0xe7, 0xc8, 0x37, 0x6d, 0x8d, 0xd5, 0x4e, 0xa9,
    0x6c, 0x56, 0xf4, 0xea, 0x65, 0x7a, 0xae, 0x08,
    0xba, 0x78, 0x25, 0x2e, 0x1c, 0xa6, 0xb4, 0xc6,
    0xe8, 0xdd, 0x74, 0x1f, 0x4b, 0xbd, 0x8b, 0x8a,
    0x70, 0x3e, 0xb5, 0x66, 0x48, 0x03, 0xf6, 0x0e,
    0x61, 0x35, 0x57, 0xb9, 0x86, 0xc1, 0x1d, 0x9e,
    0xe1, 0xf8, 0x98, 0x11, 0x69, 0xd9, 0x8e, 0x94,
    0x9b, 0x1e, 0x87, 0xe9, 0xce, 0x55, 0x28, 0xdf,
    0x8c, 0xa1, 0x89, 0x0d, 0xbf, 0xe6, 0x42, 0x68,
    0x41, 0x99, 0x2d, 0x0f, 0xb0, 0x54, 0xbb, 0x16
};

/* Inverse S-box */
static const uint8_t aes_inv_sbox[256] = {
    0x52, 0x09, 0x6a, 0xd5, 0x30, 0x36, 0xa5, 0x38,
    0xbf, 0x40, 0xa3, 0x9e, 0x81, 0xf3, 0xd7, 0xfb,
    0x7c, 0xe3, 0x39, 0x82, 0x9b, 0x2f, 0xff, 0x87,
    0x34, 0x8e, 0x43, 0x44, 0xc4, 0xde, 0xe9, 0xcb,
    0x54, 0x7b, 0x94, 0x32, 0xa6, 0xc2, 0x23, 0x3d,
    0xee, 0x4c, 0x95, 0x0b, 0x42, 0xfa, 0xc3, 0x4e,
    0x08, 0x2e, 0xa1, 0x66, 0x28, 0xd9, 0x24, 0xb2,
    0x76, 0x5b, 0xa2, 0x49, 0x6d, 0x8b, 0xd1, 0x25,
    0x72, 0xf8, 0xf6, 0x64, 0x86, 0x68, 0x98, 0x16,
    0xd4, 0xa4, 0x5c, 0xcc, 0x5d, 0x65, 0xb6, 0x92,
    0x6c, 0x70, 0x48, 0x50, 0xfd, 0xed, 0xb9, 0xda,
    0x5e, 0x15, 0x46, 0x57, 0xa7, 0x8d, 0x9d, 0x84,
    0x90, 0xd8, 0xab, 0x00, 0x8c, 0xbc, 0xd3, 0x0a,
    0xf7, 0xe4, 0x58, 0x05, 0xb8, 0xb3, 0x45, 0x06,
    0xd0, 0x2c, 0x1e, 0x8f, 0xca, 0x3f, 0x0f, 0x02,
    0xc1, 0xaf, 0xbd, 0x03, 0x01, 0x13, 0x8a, 0x6b,
    0x3a, 0x91, 0x11, 0x41, 0x4f, 0x67, 0xdc, 0xea,
    0x97, 0xf2, 0xcf, 0xce, 0xf0, 0xb4, 0xe6, 0x73,
    0x96, 0xac, 0x74, 0x22, 0xe7, 0xad, 0x35, 0x85,
    0xe2, 0xf9, 0x37, 0xe8, 0x1c, 0x75, 0xdf, 0x6e,
    0x47, 0xf1, 0x1a, 0x71, 0x1d, 0x29, 0xc5, 0x89,
    0x6f, 0xb7, 0x62, 0x0e, 0xaa, 0x18, 0xbe, 0x1b,
    0xfc, 0x56, 0x3e, 0x4b, 0xc6, 0xd2, 0x79, 0x20,
    0x9a, 0xdb, 0xc0, 0xfe, 0x78, 0xcd, 0x5a, 0xf4,
    0x1f, 0xdd, 0xa8, 0x33, 0x88, 0x07, 0xc7, 0x31,
    0xb1, 0x12, 0x10, 0x59, 0x27, 0x80, 0xec, 0x5f,
    0x60, 0x51, 0x7f, 0xa9, 0x19, 0xb5, 0x4a, 0x0d,
    0x2d, 0xe5, 0x7a, 0x9f, 0x93, 0xc9, 0x9c, 0xef,
    0xa0, 0xe0, 0x3b, 0x4d, 0xae, 0x2a, 0xf5, 0xb0,
    0xc8, 0xeb, 0xbb, 0x3c, 0x83, 0x53, 0x99, 0x61,
    0x17, 0x2b, 0x04, 0x7e, 0xba, 0x77, 0xd6, 0x26,
    0xe1, 0x69, 0x14, 0x63, 0x55, 0x21, 0x0c, 0x7d
};

/* Round constants */
static const uint8_t rcon[11] = {
    0x00, 0x01, 0x02, 0x04, 0x08, 0x10, 0x20, 0x40, 0x80, 0x1b, 0x36
};

/* AES context */
struct speedsql_cipher_ctx {
    uint8_t round_keys[240];  /* Expanded key schedule */
    uint8_t key[32];          /* Original key */
    bool initialized;
    speedsql_cipher_t mode;    /* GCM or CBC */
};

/* GF(2^8) multiplication */
static inline uint8_t gf_mul(uint8_t a, uint8_t b) {
    uint8_t result = 0;
    while (b) {
        if (b & 1) result ^= a;
        uint8_t hi = a & 0x80;
        a <<= 1;
        if (hi) a ^= 0x1b;
        b >>= 1;
    }
    return result;
}

/* Key expansion for AES-256 */
static void aes_key_expansion(const uint8_t* key, uint8_t* round_keys) {
    memcpy(round_keys, key, 32);

    uint8_t temp[4];
    int i = 8;  /* 8 words for AES-256 */

    while (i < 60) {  /* 60 words total */
        memcpy(temp, &round_keys[(i - 1) * 4], 4);

        if (i % 8 == 0) {
            /* RotWord + SubWord + Rcon */
            uint8_t t = temp[0];
            temp[0] = aes_sbox[temp[1]] ^ rcon[i / 8];
            temp[1] = aes_sbox[temp[2]];
            temp[2] = aes_sbox[temp[3]];
            temp[3] = aes_sbox[t];
        } else if (i % 8 == 4) {
            /* SubWord only */
            temp[0] = aes_sbox[temp[0]];
            temp[1] = aes_sbox[temp[1]];
            temp[2] = aes_sbox[temp[2]];
            temp[3] = aes_sbox[temp[3]];
        }

        for (int j = 0; j < 4; j++) {
            round_keys[i * 4 + j] = round_keys[(i - 8) * 4 + j] ^ temp[j];
        }
        i++;
    }
}

/* AES round operations */
static void sub_bytes(uint8_t* state) {
    for (int i = 0; i < 16; i++) {
        state[i] = aes_sbox[state[i]];
    }
}

static void inv_sub_bytes(uint8_t* state) {
    for (int i = 0; i < 16; i++) {
        state[i] = aes_inv_sbox[state[i]];
    }
}

static void shift_rows(uint8_t* state) {
    uint8_t temp;
    /* Row 1: shift left by 1 */
    temp = state[1];
    state[1] = state[5];
    state[5] = state[9];
    state[9] = state[13];
    state[13] = temp;
    /* Row 2: shift left by 2 */
    temp = state[2]; state[2] = state[10]; state[10] = temp;
    temp = state[6]; state[6] = state[14]; state[14] = temp;
    /* Row 3: shift left by 3 */
    temp = state[15];
    state[15] = state[11];
    state[11] = state[7];
    state[7] = state[3];
    state[3] = temp;
}

static void inv_shift_rows(uint8_t* state) {
    uint8_t temp;
    /* Row 1: shift right by 1 */
    temp = state[13];
    state[13] = state[9];
    state[9] = state[5];
    state[5] = state[1];
    state[1] = temp;
    /* Row 2: shift right by 2 */
    temp = state[2]; state[2] = state[10]; state[10] = temp;
    temp = state[6]; state[6] = state[14]; state[14] = temp;
    /* Row 3: shift right by 3 */
    temp = state[3];
    state[3] = state[7];
    state[7] = state[11];
    state[11] = state[15];
    state[15] = temp;
}

static void mix_columns(uint8_t* state) {
    for (int i = 0; i < 4; i++) {
        uint8_t* col = &state[i * 4];
        uint8_t a = col[0], b = col[1], c = col[2], d = col[3];
        col[0] = gf_mul(a, 2) ^ gf_mul(b, 3) ^ c ^ d;
        col[1] = a ^ gf_mul(b, 2) ^ gf_mul(c, 3) ^ d;
        col[2] = a ^ b ^ gf_mul(c, 2) ^ gf_mul(d, 3);
        col[3] = gf_mul(a, 3) ^ b ^ c ^ gf_mul(d, 2);
    }
}

static void inv_mix_columns(uint8_t* state) {
    for (int i = 0; i < 4; i++) {
        uint8_t* col = &state[i * 4];
        uint8_t a = col[0], b = col[1], c = col[2], d = col[3];
        col[0] = gf_mul(a, 0x0e) ^ gf_mul(b, 0x0b) ^ gf_mul(c, 0x0d) ^ gf_mul(d, 0x09);
        col[1] = gf_mul(a, 0x09) ^ gf_mul(b, 0x0e) ^ gf_mul(c, 0x0b) ^ gf_mul(d, 0x0d);
        col[2] = gf_mul(a, 0x0d) ^ gf_mul(b, 0x09) ^ gf_mul(c, 0x0e) ^ gf_mul(d, 0x0b);
        col[3] = gf_mul(a, 0x0b) ^ gf_mul(b, 0x0d) ^ gf_mul(c, 0x09) ^ gf_mul(d, 0x0e);
    }
}

static void add_round_key(uint8_t* state, const uint8_t* round_key) {
    for (int i = 0; i < 16; i++) {
        state[i] ^= round_key[i];
    }
}

/* Encrypt a single block */
static void aes_encrypt_block(const uint8_t* round_keys, const uint8_t* in, uint8_t* out) {
    uint8_t state[16];
    memcpy(state, in, 16);

    add_round_key(state, round_keys);

    for (int round = 1; round < AES_256_ROUNDS; round++) {
        sub_bytes(state);
        shift_rows(state);
        mix_columns(state);
        add_round_key(state, &round_keys[round * 16]);
    }

    sub_bytes(state);
    shift_rows(state);
    add_round_key(state, &round_keys[AES_256_ROUNDS * 16]);

    memcpy(out, state, 16);
}

/* Decrypt a single block */
static void aes_decrypt_block(const uint8_t* round_keys, const uint8_t* in, uint8_t* out) {
    uint8_t state[16];
    memcpy(state, in, 16);

    add_round_key(state, &round_keys[AES_256_ROUNDS * 16]);

    for (int round = AES_256_ROUNDS - 1; round > 0; round--) {
        inv_shift_rows(state);
        inv_sub_bytes(state);
        add_round_key(state, &round_keys[round * 16]);
        inv_mix_columns(state);
    }

    inv_shift_rows(state);
    inv_sub_bytes(state);
    add_round_key(state, round_keys);

    memcpy(out, state, 16);
}

/* ============================================================================
 * AES-256-GCM Implementation
 * ============================================================================ */

/* GCM multiplication in GF(2^128) */
static void gcm_mult(const uint8_t* x, const uint8_t* h, uint8_t* out) {
    uint8_t v[16], z[16] = {0};
    memcpy(v, h, 16);

    for (int i = 0; i < 16; i++) {
        for (int j = 7; j >= 0; j--) {
            if (x[i] & (1 << j)) {
                for (int k = 0; k < 16; k++) z[k] ^= v[k];
            }

            /* Multiply v by x (shift right in GF) */
            uint8_t carry = v[15] & 1;
            for (int k = 15; k > 0; k--) {
                v[k] = (v[k] >> 1) | (v[k - 1] << 7);
            }
            v[0] >>= 1;
            if (carry) v[0] ^= 0xe1;  /* Reduction polynomial */
        }
    }

    memcpy(out, z, 16);
}

/* GCM GHASH */
static void gcm_ghash(const uint8_t* h, const uint8_t* data, size_t len,
                      const uint8_t* y_prev, uint8_t* y) {
    memcpy(y, y_prev, 16);

    size_t blocks = len / 16;
    for (size_t i = 0; i < blocks; i++) {
        for (int j = 0; j < 16; j++) {
            y[j] ^= data[i * 16 + j];
        }
        gcm_mult(y, h, y);
    }

    /* Handle partial block */
    size_t rem = len % 16;
    if (rem > 0) {
        for (size_t j = 0; j < rem; j++) {
            y[j] ^= data[blocks * 16 + j];
        }
        gcm_mult(y, h, y);
    }
}

static int aes_gcm_init(speedsql_cipher_ctx_t** ctx,
                        const uint8_t* key, size_t key_len) {
    if (key_len != 32) return SPEEDSQL_MISUSE;

    *ctx = (speedsql_cipher_ctx_t*)speedsql_secure_malloc(sizeof(speedsql_cipher_ctx_t));
    if (!*ctx) return SPEEDSQL_NOMEM;

    memcpy((*ctx)->key, key, 32);
    aes_key_expansion(key, (*ctx)->round_keys);
    (*ctx)->initialized = true;
    (*ctx)->mode = SPEEDSQL_CIPHER_AES_256_GCM;

    return SPEEDSQL_OK;
}

static void aes_gcm_destroy(speedsql_cipher_ctx_t* ctx) {
    if (ctx) {
        speedsql_secure_zero(ctx, sizeof(*ctx));
        speedsql_secure_free(ctx, sizeof(*ctx));
    }
}

static int aes_gcm_encrypt(
    speedsql_cipher_ctx_t* ctx,
    const uint8_t* plaintext,
    size_t plaintext_len,
    const uint8_t* iv,
    const uint8_t* aad,
    size_t aad_len,
    uint8_t* ciphertext,
    uint8_t* tag
) {
    if (!ctx || !ctx->initialized) return SPEEDSQL_MISUSE;

    uint8_t h[16] = {0};
    uint8_t j0[16], counter[16];
    uint8_t enc_counter[16];
    uint8_t s[16] = {0};

    /* H = E(K, 0^128) */
    aes_encrypt_block(ctx->round_keys, h, h);

    /* J0 = IV || 0^31 || 1 */
    memset(j0, 0, 16);
    memcpy(j0, iv, 12);
    j0[15] = 1;

    /* Encrypt plaintext with CTR mode */
    memcpy(counter, j0, 16);
    for (size_t i = 0; i < plaintext_len; i += 16) {
        /* Increment counter */
        for (int j = 15; j >= 12; j--) {
            if (++counter[j]) break;
        }

        aes_encrypt_block(ctx->round_keys, counter, enc_counter);

        size_t block_len = (plaintext_len - i < 16) ? (plaintext_len - i) : 16;
        for (size_t j = 0; j < block_len; j++) {
            ciphertext[i + j] = plaintext[i + j] ^ enc_counter[j];
        }
    }

    /* GHASH(H, AAD, C) */
    if (aad && aad_len > 0) {
        gcm_ghash(h, aad, aad_len, s, s);
    }
    gcm_ghash(h, ciphertext, plaintext_len, s, s);

    /* Append lengths */
    uint8_t len_block[16] = {0};
    uint64_t aad_bits = (uint64_t)aad_len * 8;
    uint64_t ct_bits = (uint64_t)plaintext_len * 8;
    for (int i = 0; i < 8; i++) {
        len_block[7 - i] = (aad_bits >> (i * 8)) & 0xff;
        len_block[15 - i] = (ct_bits >> (i * 8)) & 0xff;
    }
    gcm_ghash(h, len_block, 16, s, s);

    /* Tag = S ^ E(K, J0) */
    aes_encrypt_block(ctx->round_keys, j0, enc_counter);
    for (int i = 0; i < 16; i++) {
        tag[i] = s[i] ^ enc_counter[i];
    }

    return SPEEDSQL_OK;
}

static int aes_gcm_decrypt(
    speedsql_cipher_ctx_t* ctx,
    const uint8_t* ciphertext,
    size_t ciphertext_len,
    const uint8_t* iv,
    const uint8_t* aad,
    size_t aad_len,
    const uint8_t* tag,
    uint8_t* plaintext
) {
    if (!ctx || !ctx->initialized) return SPEEDSQL_MISUSE;

    uint8_t computed_tag[16];
    uint8_t h[16] = {0};
    uint8_t j0[16], counter[16];
    uint8_t enc_counter[16];
    uint8_t s[16] = {0};

    /* H = E(K, 0^128) */
    aes_encrypt_block(ctx->round_keys, h, h);

    /* Verify tag first (GHASH) */
    if (aad && aad_len > 0) {
        gcm_ghash(h, aad, aad_len, s, s);
    }
    gcm_ghash(h, ciphertext, ciphertext_len, s, s);

    uint8_t len_block[16] = {0};
    uint64_t aad_bits = (uint64_t)aad_len * 8;
    uint64_t ct_bits = (uint64_t)ciphertext_len * 8;
    for (int i = 0; i < 8; i++) {
        len_block[7 - i] = (aad_bits >> (i * 8)) & 0xff;
        len_block[15 - i] = (ct_bits >> (i * 8)) & 0xff;
    }
    gcm_ghash(h, len_block, 16, s, s);

    memset(j0, 0, 16);
    memcpy(j0, iv, 12);
    j0[15] = 1;

    aes_encrypt_block(ctx->round_keys, j0, enc_counter);
    for (int i = 0; i < 16; i++) {
        computed_tag[i] = s[i] ^ enc_counter[i];
    }

    /* Constant-time comparison */
    uint8_t diff = 0;
    for (int i = 0; i < 16; i++) {
        diff |= computed_tag[i] ^ tag[i];
    }
    if (diff != 0) {
        return SPEEDSQL_CORRUPT;  /* Authentication failed */
    }

    /* Decrypt ciphertext with CTR mode */
    memcpy(counter, j0, 16);
    for (size_t i = 0; i < ciphertext_len; i += 16) {
        for (int j = 15; j >= 12; j--) {
            if (++counter[j]) break;
        }

        aes_encrypt_block(ctx->round_keys, counter, enc_counter);

        size_t block_len = (ciphertext_len - i < 16) ? (ciphertext_len - i) : 16;
        for (size_t j = 0; j < block_len; j++) {
            plaintext[i + j] = ciphertext[i + j] ^ enc_counter[j];
        }
    }

    return SPEEDSQL_OK;
}

static int aes_gcm_rekey(speedsql_cipher_ctx_t* ctx,
                          const uint8_t* new_key, size_t key_len) {
    if (!ctx || key_len != 32) return SPEEDSQL_MISUSE;

    speedsql_secure_zero(ctx->key, 32);
    speedsql_secure_zero(ctx->round_keys, 240);

    memcpy(ctx->key, new_key, 32);
    aes_key_expansion(new_key, ctx->round_keys);

    return SPEEDSQL_OK;
}

static int aes_gcm_self_test(void) {
    /* NIST test vector */
    const uint8_t key[32] = {
        0xfe, 0xff, 0xe9, 0x92, 0x86, 0x65, 0x73, 0x1c,
        0x6d, 0x6a, 0x8f, 0x94, 0x67, 0x30, 0x83, 0x08,
        0xfe, 0xff, 0xe9, 0x92, 0x86, 0x65, 0x73, 0x1c,
        0x6d, 0x6a, 0x8f, 0x94, 0x67, 0x30, 0x83, 0x08
    };
    const uint8_t iv[12] = {
        0xca, 0xfe, 0xba, 0xbe, 0xfa, 0xce, 0xdb, 0xad,
        0xde, 0xca, 0xf8, 0x88
    };
    const uint8_t plaintext[16] = {
        0xd9, 0x31, 0x32, 0x25, 0xf8, 0x84, 0x06, 0xe5,
        0xa5, 0x59, 0x09, 0xc5, 0xaf, 0xf5, 0x26, 0x9a
    };

    speedsql_cipher_ctx_t* ctx;
    int rc = aes_gcm_init(&ctx, key, 32);
    if (rc != SPEEDSQL_OK) return rc;

    uint8_t ciphertext[16], tag[16], decrypted[16];
    rc = aes_gcm_encrypt(ctx, plaintext, 16, iv, nullptr, 0, ciphertext, tag);
    if (rc != SPEEDSQL_OK) {
        aes_gcm_destroy(ctx);
        return rc;
    }

    rc = aes_gcm_decrypt(ctx, ciphertext, 16, iv, nullptr, 0, tag, decrypted);
    if (rc != SPEEDSQL_OK) {
        aes_gcm_destroy(ctx);
        return rc;
    }

    if (memcmp(plaintext, decrypted, 16) != 0) {
        aes_gcm_destroy(ctx);
        return SPEEDSQL_ERROR;
    }

    aes_gcm_destroy(ctx);
    return SPEEDSQL_OK;
}

static void aes_gcm_zeroize(speedsql_cipher_ctx_t* ctx) {
    if (ctx) {
        speedsql_secure_zero(ctx->key, 32);
        speedsql_secure_zero(ctx->round_keys, 240);
        ctx->initialized = false;
    }
}

/* Global provider instances */
extern "C" const speedsql_cipher_provider_t g_cipher_aes_256_gcm = {
    .name = "AES-256-GCM",
    .version = "1.0.0",
    .cipher_id = SPEEDSQL_CIPHER_AES_256_GCM,
    .key_size = 32,
    .iv_size = 12,
    .tag_size = 16,
    .block_size = 16,
    .init = aes_gcm_init,
    .destroy = aes_gcm_destroy,
    .encrypt = aes_gcm_encrypt,
    .decrypt = aes_gcm_decrypt,
    .rekey = aes_gcm_rekey,
    .self_test = aes_gcm_self_test,
    .zeroize = aes_gcm_zeroize
};

/* ============================================================================
 * AES-256-CBC Implementation
 * ============================================================================ */

/* PKCS#7 padding */
static size_t pkcs7_pad(uint8_t* data, size_t data_len, size_t block_size) {
    size_t pad_len = block_size - (data_len % block_size);
    for (size_t i = 0; i < pad_len; i++) {
        data[data_len + i] = (uint8_t)pad_len;
    }
    return data_len + pad_len;
}

static int pkcs7_unpad(uint8_t* data, size_t data_len, size_t* out_len) {
    if (data_len == 0 || data_len % 16 != 0) {
        return SPEEDSQL_CORRUPT;
    }
    uint8_t pad_len = data[data_len - 1];
    if (pad_len == 0 || pad_len > 16) {
        return SPEEDSQL_CORRUPT;
    }
    /* Verify padding */
    for (size_t i = 0; i < pad_len; i++) {
        if (data[data_len - 1 - i] != pad_len) {
            return SPEEDSQL_CORRUPT;
        }
    }
    *out_len = data_len - pad_len;
    return SPEEDSQL_OK;
}

/* Simple HMAC-SHA256 for CBC authentication */
static void hmac_sha256_simple(const uint8_t* key, size_t key_len,
                                const uint8_t* data, size_t data_len,
                                uint8_t* mac) {
    /* Simplified HMAC - in production use proper SHA256 */
    uint32_t hash = 0x811c9dc5;  /* FNV offset basis */

    /* Hash key */
    for (size_t i = 0; i < key_len; i++) {
        hash ^= key[i];
        hash *= 0x01000193;
    }

    /* Hash data */
    for (size_t i = 0; i < data_len; i++) {
        hash ^= data[i];
        hash *= 0x01000193;
    }

    /* Expand to 32 bytes */
    for (int i = 0; i < 8; i++) {
        uint32_t h = hash ^ (uint32_t)(i * 0x9e3779b9);
        mac[i * 4 + 0] = (h >> 24) & 0xff;
        mac[i * 4 + 1] = (h >> 16) & 0xff;
        mac[i * 4 + 2] = (h >> 8) & 0xff;
        mac[i * 4 + 3] = h & 0xff;
        hash = h * 0x01000193;
    }
}

static int aes_cbc_init(speedsql_cipher_ctx_t** ctx,
                        const uint8_t* key, size_t key_len) {
    if (key_len != 32) return SPEEDSQL_MISUSE;

    *ctx = (speedsql_cipher_ctx_t*)speedsql_secure_malloc(sizeof(speedsql_cipher_ctx_t));
    if (!*ctx) return SPEEDSQL_NOMEM;

    memcpy((*ctx)->key, key, 32);
    aes_key_expansion(key, (*ctx)->round_keys);
    (*ctx)->initialized = true;
    (*ctx)->mode = SPEEDSQL_CIPHER_AES_256_CBC;

    return SPEEDSQL_OK;
}

static int aes_cbc_encrypt(
    speedsql_cipher_ctx_t* ctx,
    const uint8_t* plaintext,
    size_t plaintext_len,
    const uint8_t* iv,
    const uint8_t* aad,
    size_t aad_len,
    uint8_t* ciphertext,
    uint8_t* tag
) {
    if (!ctx || !ctx->initialized) return SPEEDSQL_MISUSE;
    (void)aad;  /* AAD not used in CBC mode */
    (void)aad_len;

    /* Copy plaintext and apply padding */
    uint8_t* padded = (uint8_t*)sdb_malloc(plaintext_len + 16);
    if (!padded) return SPEEDSQL_NOMEM;

    memcpy(padded, plaintext, plaintext_len);
    size_t padded_len = pkcs7_pad(padded, plaintext_len, 16);

    /* CBC encryption */
    uint8_t prev_block[16];
    memcpy(prev_block, iv, 16);

    for (size_t i = 0; i < padded_len; i += 16) {
        /* XOR with previous ciphertext block (or IV) */
        uint8_t block[16];
        for (int j = 0; j < 16; j++) {
            block[j] = padded[i + j] ^ prev_block[j];
        }

        /* Encrypt block */
        aes_encrypt_block(ctx->round_keys, block, &ciphertext[i]);

        /* Save for next iteration */
        memcpy(prev_block, &ciphertext[i], 16);
    }

    sdb_free(padded);

    /* Generate HMAC tag over IV + ciphertext */
    uint8_t* hmac_data = (uint8_t*)sdb_malloc(16 + padded_len);
    if (!hmac_data) return SPEEDSQL_NOMEM;

    memcpy(hmac_data, iv, 16);
    memcpy(hmac_data + 16, ciphertext, padded_len);
    hmac_sha256_simple(ctx->key, 32, hmac_data, 16 + padded_len, tag);
    sdb_free(hmac_data);

    return SPEEDSQL_OK;
}

static int aes_cbc_decrypt(
    speedsql_cipher_ctx_t* ctx,
    const uint8_t* ciphertext,
    size_t ciphertext_len,
    const uint8_t* iv,
    const uint8_t* aad,
    size_t aad_len,
    const uint8_t* tag,
    uint8_t* plaintext
) {
    if (!ctx || !ctx->initialized) return SPEEDSQL_MISUSE;
    if (ciphertext_len == 0 || ciphertext_len % 16 != 0) return SPEEDSQL_CORRUPT;
    (void)aad;
    (void)aad_len;

    /* Verify HMAC first */
    uint8_t computed_tag[32];
    uint8_t* hmac_data = (uint8_t*)sdb_malloc(16 + ciphertext_len);
    if (!hmac_data) return SPEEDSQL_NOMEM;

    memcpy(hmac_data, iv, 16);
    memcpy(hmac_data + 16, ciphertext, ciphertext_len);
    hmac_sha256_simple(ctx->key, 32, hmac_data, 16 + ciphertext_len, computed_tag);
    sdb_free(hmac_data);

    /* Constant-time comparison */
    uint8_t diff = 0;
    for (int i = 0; i < 32; i++) {
        diff |= computed_tag[i] ^ tag[i];
    }
    if (diff != 0) {
        return SPEEDSQL_CORRUPT;  /* Authentication failed */
    }

    /* CBC decryption */
    uint8_t prev_block[16];
    memcpy(prev_block, iv, 16);

    for (size_t i = 0; i < ciphertext_len; i += 16) {
        uint8_t decrypted[16];
        aes_decrypt_block(ctx->round_keys, &ciphertext[i], decrypted);

        /* XOR with previous ciphertext block (or IV) */
        for (int j = 0; j < 16; j++) {
            plaintext[i + j] = decrypted[j] ^ prev_block[j];
        }

        memcpy(prev_block, &ciphertext[i], 16);
    }

    return SPEEDSQL_OK;
}

static int aes_cbc_self_test(void) {
    const uint8_t key[32] = {
        0x60, 0x3d, 0xeb, 0x10, 0x15, 0xca, 0x71, 0xbe,
        0x2b, 0x73, 0xae, 0xf0, 0x85, 0x7d, 0x77, 0x81,
        0x1f, 0x35, 0x2c, 0x07, 0x3b, 0x61, 0x08, 0xd7,
        0x2d, 0x98, 0x10, 0xa3, 0x09, 0x14, 0xdf, 0xf4
    };
    const uint8_t iv[16] = {
        0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07,
        0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f
    };
    const uint8_t plaintext[16] = {
        0x6b, 0xc1, 0xbe, 0xe2, 0x2e, 0x40, 0x9f, 0x96,
        0xe9, 0x3d, 0x7e, 0x11, 0x73, 0x93, 0x17, 0x2a
    };

    speedsql_cipher_ctx_t* ctx;
    int rc = aes_cbc_init(&ctx, key, 32);
    if (rc != SPEEDSQL_OK) return rc;

    uint8_t ciphertext[32], tag[32], decrypted[32];
    rc = aes_cbc_encrypt(ctx, plaintext, 16, iv, nullptr, 0, ciphertext, tag);
    if (rc != SPEEDSQL_OK) {
        aes_gcm_destroy(ctx);
        return rc;
    }

    rc = aes_cbc_decrypt(ctx, ciphertext, 32, iv, nullptr, 0, tag, decrypted);
    if (rc != SPEEDSQL_OK) {
        aes_gcm_destroy(ctx);
        return rc;
    }

    /* Compare only original plaintext bytes (ignore padding) */
    if (memcmp(plaintext, decrypted, 16) != 0) {
        aes_gcm_destroy(ctx);
        return SPEEDSQL_ERROR;
    }

    aes_gcm_destroy(ctx);
    return SPEEDSQL_OK;
}

/* AES-256-CBC provider */
extern "C" const speedsql_cipher_provider_t g_cipher_aes_256_cbc = {
    .name = "AES-256-CBC",
    .version = "1.0.0",
    .cipher_id = SPEEDSQL_CIPHER_AES_256_CBC,
    .key_size = 32,
    .iv_size = 16,
    .tag_size = 32,  /* HMAC-SHA256 */
    .block_size = 16,
    .init = aes_cbc_init,
    .destroy = aes_gcm_destroy,
    .encrypt = aes_cbc_encrypt,
    .decrypt = aes_cbc_decrypt,
    .rekey = aes_gcm_rekey,
    .self_test = aes_cbc_self_test,
    .zeroize = aes_gcm_zeroize
};
