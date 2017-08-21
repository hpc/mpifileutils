cmake_minimum_required(VERSION 2.8)
project(fileutils)
set(CMAKE_MODULE_PATH ${PROJECT_SOURCE_DIR}/CMakeScripts)


if (DEFINED ENV{FILEUTILS_EXT})
    message("-- Using $ENV{FILEUTILS_EXT}")
    set(ENV{PKG_CONFIG_PATH} "$ENV{FILEUTILS_EXT}/lib/pkgconfig" )
endif()

set(CMAKE_C_FLAGS_DEBUG         "-Wall -O0 -g")
set(CMAKE_C_FLAGS_RELEASE       "-Wall -Werror -O3")


find_package(PkgConfig)
find_package(MPI REQUIRED)
find_package(BZip2 REQUIRED)
include_directories(${MPI_INCLUDE_PATH})

pkg_check_modules(CIRCLE REQUIRED libcircle)
pkg_check_modules(DTCMP REQUIRED libdtcmp)

add_definitions(${CIRCLE_CFLAGS})
add_definitions(${DTCMP_CFLAGS})

include_directories(src/common)

add_subdirectory(src/common)
add_subdirectory(experimental/dbz2)
add_subdirectory(experimental/dtar)

