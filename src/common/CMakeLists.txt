cmake_minimum_required(VERSION 2.8)
project(fileutils_common)

file(GLOB common_h_files *.h)
file(GLOB common_c_files *.c)
list(APPEND common_src_files ${common_h_files} ${common_c_files})

add_library(bayer ${common_src_files})
target_link_libraries(bayer ${MPI_LIBRARIES} dtcmp )
