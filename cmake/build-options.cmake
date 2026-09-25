# CMake initial-cache file for mpiFileUtils Docker builds.
#
# This file is loaded with "cmake -C cmake/build-options.cmake" and
# pre-seeds CMake cache variables before CMakeLists.txt is read.
#
# To customise a build (e.g. from a GitLab pipeline in another repository):
#   1. Copy this file to your own repo as, say, my-build-options.cmake.
#   2. Edit the values you need.
#   3. In your CI script, copy it over this file before running docker build:
#
#        cp mpifileutils-build-options.cmake mpifileutils/cmake/build-options.cmake
#        docker build -t mpifileutils:custom mpifileutils/
#
# Variables set here can be overridden by explicit -D arguments on the cmake
# command line, but the Dockerfile intentionally passes none so this file
# has full control.

# ---------------------------------------------------------------------------
# Build type
# ---------------------------------------------------------------------------
set(CMAKE_BUILD_TYPE "Release" CACHE STRING "CMake build type")

# ---------------------------------------------------------------------------
# Compiler flags
# Uncomment and adjust to enable things like address sanitizer, frame
# pointers, link-time optimisation, etc.
# ---------------------------------------------------------------------------
#set(CMAKE_C_FLAGS             "" CACHE STRING "Extra C compiler flags")
#set(CMAKE_CXX_FLAGS           "" CACHE STRING "Extra C++ compiler flags")
#set(CMAKE_EXE_LINKER_FLAGS    "" CACHE STRING "Extra linker flags")
#set(CMAKE_SHARED_LINKER_FLAGS "" CACHE STRING "Extra shared-library linker flags")

# ---------------------------------------------------------------------------
# Optional mpiFileUtils features
# ---------------------------------------------------------------------------
set(ENABLE_DAOS   OFF CACHE BOOL "Enable DAOS support")
set(ENABLE_GPFS   OFF CACHE BOOL "Enable GPFS/Spectrum Scale support")
set(ENABLE_HDF5   OFF CACHE BOOL "Enable HDF5 support")
set(ENABLE_HPSS   OFF CACHE BOOL "Enable HPSS support")
set(ENABLE_LUSTRE OFF CACHE BOOL "Enable Lustre support")
