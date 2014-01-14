cmake_minimum_required(VERSION 2.8)
project(fileutils)
set(CMAKE_MODULE_PATH ${PROJECT_SOURCE_DIR}/CMakeScripts)


#include(CMakeForceCompiler)
#CMAKE_FORCE_C_COMPILER(mpicc ${CMAKE_C_COMPILER_ID})
#CMAKE_FORCE_CXX_COMPILER(mpicxx ${CMAKE_CXX_COMPILER_ID})

if (DEFINED ENV{FILEUTILS_EXT})
    message("-- Using $ENV{FILEUTILS_EXT}")
    set(ENV{PKG_CONFIG_PATH} "$ENV{FILEUTILS_EXT}/lib/pkgconfig" )
endif()

find_package(PkgConfig)
find_package(MPI REQUIRED)

set(CMAKE_C_FLAGS_DEBUG         "-Wall -O0 -g")
set(CMAKE_C_FLAGS_RELEASE       "-Wall -Werror -O3")


# pkg_check_modules(ARCHIVE REQUIRED libarchive)
pkg_check_modules(CIRCLE REQUIRED libcircle)
pkg_check_modules(DTCMP REQUIRED libdtcmp)

include_directories(src/common)

add_subdirectory(src/common)
add_subdirectory(experimental/dtar)


