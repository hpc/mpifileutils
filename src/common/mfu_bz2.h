#ifndef MFU_BZ2_H
#define MFU_BZ2_H

void mfu_compress_bz2(const char* src_name, const char* dst_name, int b_size);
void mfu_decompress_bz2(const char* src_name, const char* dst_name);

#endif /* MFU_BZ2_H */
