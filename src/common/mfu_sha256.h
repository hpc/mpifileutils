/*
 * mfu_sha256 - self-contained SHA-256 (FIPS 180-4).
 *
 * Adapted for mpifileutils from Brad Conte's public-domain SHA-256
 * (https://github.com/B-Con/crypto-algorithms).  The original code is
 * released into the public domain free of any restrictions.  Vendoring it
 * here provides a small, dependency-free SHA-256 so that ddup no longer
 * requires OpenSSL.
 *
 * The context is a flat, pointer-free struct so it may be snapshotted with a
 * plain memcpy: ddup copies a running context and finalizes the copy to obtain
 * an intermediate digest while continuing to update the original.  The
 * implementation is endian independent and produces standard big-endian
 * SHA-256 output, identical on amd64 and arm64.
 */

#ifndef MFU_SHA256_H
#define MFU_SHA256_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* length of a SHA-256 digest in bytes */
#define MFU_SHA256_DIGEST_LEN 32

/* SHA-256 hashing context.  This is a plain, pointer-free struct so that it
 * can be copied by value (memcpy) to snapshot intermediate hash state. */
typedef struct {
    uint8_t  data[64];   /* buffered bytes of the current 64-byte block */
    uint32_t datalen;    /* number of bytes currently buffered in data[] */
    uint64_t bitlen;     /* total message length processed so far, in bits */
    uint32_t state[8];   /* current hash state (H0..H7) */
} mfu_sha256_ctx;

/* initialize a hashing context */
void mfu_sha256_init(mfu_sha256_ctx* ctx);

/* feed len bytes of data into the running hash */
void mfu_sha256_update(mfu_sha256_ctx* ctx, const void* data, size_t len);

/* finalize the hash, writing MFU_SHA256_DIGEST_LEN (32) bytes to digest;
 * ctx is consumed and must be re-initialized with mfu_sha256_init before
 * it is used again */
void mfu_sha256_final(mfu_sha256_ctx* ctx, uint8_t digest[MFU_SHA256_DIGEST_LEN]);

#ifdef __cplusplus
}
#endif

#endif /* MFU_SHA256_H */
