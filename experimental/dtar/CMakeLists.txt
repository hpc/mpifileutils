cmake_minimum_required(VERSION 2.8)
project(fileutils_dtar)

set(CMAKE_C_FLAGS        "-std=c99 -Wall -O0 -g")


pkg_check_modules(GLIB2 REQUIRED glib-2.0)
add_definitions(${GLIB2_CFLAGS})
link_libraries(${GLIB2_LDFLAGS})

pkg_check_modules(ARCHIVE REQUIRED libarchive)
add_definitions(${ARCHIVE_CFLAGS})
link_libraries(${ARCHIVE_LDFLAGS})

set(dtar_src_files common.c handle_args.c dtar.c)

add_executable(dtar ${dtar_src_files})
target_link_libraries(dtar ${MPI_LIBRARIES} bayer circle)
