#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>

int mfu_compress_bz2_libcircle(const char* src_name, const char* dst_name, int b_size, ssize_t opts_memory);
int mfu_compress_bz2_static(const char* src_name, const char* dst_name, int b_size);

int mfu_decompress_bz2_libcircle(const char* src_name, const char* dst_name);
int mfu_decompress_bz2_static(const char* src_name, const char* dst_name);

int mfu_compress_bz2(const char* src_name, const char* dst_name, int b_size)
{
    //return mfu_compress_bz2_libcircle(src_name, dst_name, b_size, 0);
    return mfu_compress_bz2_static(src_name, dst_name, b_size);
}


int mfu_decompress_bz2(const char* src_name, const char* dst_name)
{
    //return mfu_decompress_bz2_libcircle(src_name, dst_name);
    return mfu_decompress_bz2_static(src_name, dst_name);
}
