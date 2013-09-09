cmake_minimum_required(VERSION 2.8)
project(dtar)

set(CMAKE_MODULE_PATH ${PROJECT_SOURCE_DIR}/CMakeScripts)

if (DEFINED ENV{DTAR_EXT})
	message("-- Using $ENV{DTAR_EXT}")
    set(ENV{PKG_CONFIG_PATH} "$ENV{DTAR_EXT}/lib/pkgconfig" )
else ()
	set(ENV{PKG_CONFIG_PATH} "/usr/local/lib/pkgconfig")
endif()

find_package(PkgConfig)
find_package(MPI REQUIRED)
##
## MPI_<lang>_INCLUDE_PATH
## MPI_<lang>_LIBRARIES
## MPI_<lang>_COMPILE_FLAGS
## MPI_<lang>_LINK_FLAGS

include(CMakeForceCompiler)
CMAKE_FORCE_C_COMPILER(mpicc ${CMAKE_C_COMPILER_ID})
CMAKE_FORCE_CXX_COMPILER(mpicxx ${CMAKE_CXX_COMPILER_ID})

pkg_check_modules(ARCHIVE REQUIRED libarchive)
pkg_check_modules(CIRCLE REQUIRED libcircle)
pkg_check_modules(ATTR REQUIRED libattr)

# make include available
add_definitions(${ATTR_CFLAGS})
add_definitions(${ARCHIVE_CFLAGS})

# make library available
link_libraries(${ARCHIVE_LDFLAGS})
link_libraries(${CIRCLE_LDFLAGS})

# build

add_executable(dtar 
    src/dtar.c src/copy.c src/helper.c src/treewalk.c)


