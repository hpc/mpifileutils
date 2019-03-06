#ifndef MFU_BZ2_H
#define MFU_BZ2_H

int mfu_compress_bz2(const char* src_name, const char* dst_name, int b_size);
int mfu_decompress_bz2(const char* src_name, const char* dst_name);

/****************
 * Private internal functions
 ***************/

#include "sys/types.h"

int mfu_create_fully_striped(const char* name, mode_t mode);

#endif /* MFU_BZ2_H */
