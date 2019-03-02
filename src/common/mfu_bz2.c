#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>

void mfu_compress_bz2_libcircle(const char* src_name, int b_size, ssize_t opts_memory);
int mfu_compress_bz2_static(const char* src_name, const char* dst_name, int b_size);

void mfu_decompress_bz2_libcircle(const char* src_name, const char* fname_op);
int mfu_decompress_bz2_static(const char* src_name, const char* dst_name);

int mfu_compress_bz2(const char* src_name, const char* dst_name, int b_size)
{
    //mfu_compress_bz2_libcircle(src_name, b_size, opts_memory);
    return mfu_compress_bz2_static(src_name, dst_name, b_size);
}


int mfu_decompress_bz2(const char* src_name, const char* dst_name)
{
    return mfu_decompress_bz2_static(src_name, dst_name);
}
