cmake_minimum_required(VERSION 2.8)
project(fileutils_dbz2)

set(CMAKE_C_FLAGS        "-std=c99 -Wall -O0 -g")

pkg_check_modules(GLIB2 REQUIRED glib-2.0)
add_definitions(${GLIB2_CFLAGS})
link_libraries(${GLIB2_LDFLAGS})

pkg_check_modules(ARCHIVE REQUIRED libarchive)
add_definitions(${ARCHIVE_CFLAGS})
link_libraries(${ARCHIVE_LDFLAGS} -lz -lbz2)

pkg_check_modules(LWGRP REQUIRED liblwgrp)
add_definitions(${LWGRP_CFLAGS})
link_libraries(${LWGRP_LDFLAGS})
set(dbzip2_src_files common_dbz2.c dbz2_func.c)
set(dbz2_src_files common_dbz2.c dbz2_func.c dbz2.c)
add_executable(dbz2 ${dbz2_src_files})
add_library(dbzip2 ${dbzip2_src_files})
target_link_libraries(dbz2 ${MPI_LIBRARIES} mfu circle dtcmp lwgrp bz2)
