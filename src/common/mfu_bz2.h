#ifndef MFU_DBZ2_H
#define MFU_DBZ2_H

#include "mfu.h"

void mfu_compress_bz2(int b_size, const char* fname, ssize_t opts_memory);
void mfu_decompress_bz2(const char* fname, const char* fname_out);

#endif /* MFU_DBZ2_H */
