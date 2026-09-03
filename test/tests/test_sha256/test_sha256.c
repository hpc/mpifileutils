/*
 * Known-answer tests for the self-contained mfu_sha256 implementation.
 *
 * Validates that mfu_sha256 produces byte-correct standard SHA-256 output
 * against the canonical FIPS 180-4 test vectors, that streaming (multi-call
 * update) matches a one-shot hash, and that the "snapshot a running context
 * and finalize the copy" pattern ddup relies on works.  This is a plain
 * standalone program (no MPI): it returns EXIT_FAILURE on any mismatch so it
 * can run as a CTest.  Because the vectors are architecture independent, a
 * pass here on both amd64 and arm64 confirms the endian-neutral output.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>

#include "mfu_sha256.h"

#define HEXLEN (2 * MFU_SHA256_DIGEST_LEN + 1)

/* well-known SHA-256 digests */
static const char* SHA_EMPTY  = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
static const char* SHA_ABC    = "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad";
static const char* SHA_448    = "248d6a61d20638b8e5c026930c3e6039a33ce45964ff2167f6ecedd419db06c1";
static const char* SHA_1M_A   = "cdc76e5c9914fb9281a1c7e284d73e67f1809a48a497200e046d39ccc7112cd0";

/* the standard 448-bit message; note it begins with "abc" */
static const char* MSG_448 = "abcdbcdecdefdefgefghfghighijhijkijkljklmklmnlmnomnopnopq";

static void to_hex(const uint8_t digest[MFU_SHA256_DIGEST_LEN], char out[HEXLEN])
{
    static const char hexchars[] = "0123456789abcdef";
    int i;
    for (i = 0; i < MFU_SHA256_DIGEST_LEN; i++) {
        out[2 * i]     = hexchars[(digest[i] >> 4) & 0xf];
        out[2 * i + 1] = hexchars[digest[i] & 0xf];
    }
    out[2 * MFU_SHA256_DIGEST_LEN] = '\0';
}

/* hash an entire buffer with a single update call */
static void sha256_oneshot(const void* data, size_t len, char hex[HEXLEN])
{
    mfu_sha256_ctx ctx;
    uint8_t digest[MFU_SHA256_DIGEST_LEN];
    mfu_sha256_init(&ctx);
    mfu_sha256_update(&ctx, data, len);
    mfu_sha256_final(&ctx, digest);
    to_hex(digest, hex);
}

static int check(const char* label, const char* got, const char* expected)
{
    if (strcmp(got, expected) == 0) {
        printf("PASS %s\n", label);
        return 0;
    }
    printf("FAIL %s\n  expected %s\n  got      %s\n", label, expected, got);
    return 1;
}

int main(void)
{
    int failures = 0;
    char hex[HEXLEN];
    size_t n448 = strlen(MSG_448);

    /* One-shot known-answer tests. */
    sha256_oneshot("", 0, hex);
    failures += check("empty string", hex, SHA_EMPTY);

    sha256_oneshot("abc", 3, hex);
    failures += check("\"abc\"", hex, SHA_ABC);

    sha256_oneshot(MSG_448, n448, hex);
    failures += check("448-bit message", hex, SHA_448);

    /* Streaming: feed the 448-bit message one byte at a time; must equal the
     * one-shot digest.  Exercises the partial-block buffering ddup depends on. */
    {
        mfu_sha256_ctx ctx;
        uint8_t digest[MFU_SHA256_DIGEST_LEN];
        size_t i;
        mfu_sha256_init(&ctx);
        for (i = 0; i < n448; i++) {
            mfu_sha256_update(&ctx, MSG_448 + i, 1);
        }
        mfu_sha256_final(&ctx, digest);
        to_hex(digest, hex);
        failures += check("448-bit streamed one byte at a time", hex, SHA_448);
    }

    /* Snapshot-and-continue: copy a running context, finalize the copy for an
     * intermediate digest, and confirm the original keeps hashing correctly.
     * This is exactly the pattern ddup uses (memcpy of the ctx, then final on
     * the copy). */
    {
        mfu_sha256_ctx ctx, snapshot;
        uint8_t digest[MFU_SHA256_DIGEST_LEN];

        mfu_sha256_init(&ctx);
        mfu_sha256_update(&ctx, "abc", 3);

        /* finalize a copy after "abc" -> must equal sha256("abc") */
        memcpy(&snapshot, &ctx, sizeof(snapshot));
        mfu_sha256_final(&snapshot, digest);
        to_hex(digest, hex);
        failures += check("snapshot after \"abc\" == sha256(\"abc\")", hex, SHA_ABC);

        /* original must be undisturbed: continue with the rest of the 448-bit
         * message so the whole thing is "abc" + remainder */
        mfu_sha256_update(&ctx, MSG_448 + 3, n448 - 3);
        mfu_sha256_final(&ctx, digest);
        to_hex(digest, hex);
        failures += check("continue after snapshot == sha256(448-bit)", hex, SHA_448);
    }

    /* Multi-block streaming: one million 'a' characters fed in chunks. */
    {
        mfu_sha256_ctx ctx;
        uint8_t digest[MFU_SHA256_DIGEST_LEN];
        char buf[4096];
        size_t remaining = 1000000;
        memset(buf, 'a', sizeof(buf));
        mfu_sha256_init(&ctx);
        while (remaining > 0) {
            size_t chunk = remaining < sizeof(buf) ? remaining : sizeof(buf);
            mfu_sha256_update(&ctx, buf, chunk);
            remaining -= chunk;
        }
        mfu_sha256_final(&ctx, digest);
        to_hex(digest, hex);
        failures += check("one million 'a' characters", hex, SHA_1M_A);
    }

    if (failures == 0) {
        printf("\nAll SHA-256 known-answer tests passed.\n");
        return EXIT_SUCCESS;
    }
    printf("\n%d SHA-256 known-answer test(s) FAILED.\n", failures);
    return EXIT_FAILURE;
}
