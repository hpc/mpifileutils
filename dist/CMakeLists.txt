PROJECT(MFU)

CMAKE_MINIMUM_REQUIRED(VERSION 3.1)

IF(POLICY CMP0042)
  CMAKE_POLICY(SET CMP0042 NEW)
ENDIF(POLICY CMP0042)
SET(CMAKE_MACOSX_RPATH ON)
SET(CMAKE_POSITION_INDEPENDENT_CODE ON)

LIST(APPEND CMAKE_MODULE_PATH "${PROJECT_SOURCE_DIR}/cmake")

# Configuration Options

OPTION(ENABLE_XATTRS "Enable code for extended attributes" ON)
MESSAGE(STATUS "ENABLE_XATTRS: ${ENABLE_XATTRS}")
IF(ENABLE_XATTRS)
  ADD_DEFINITIONS(-DDCOPY_USE_XATTRS)

  FIND_PACKAGE(LibAttr REQUIRED)
  IF(LibAttr_FOUND)
    ADD_DEFINITIONS(-DHAVE_LIBATTR)
    INCLUDE_DIRECTORIES(${LibAttr_INCLUDE_DIRS})
    LIST(APPEND MFU_EXTERNAL_LIBS ${LibAttr_LIBRARIES})
  ENDIF(LibAttr_FOUND)
ENDIF(ENABLE_XATTRS)

OPTION(ENABLE_LUSTRE "Enable optimization and features for Lustre" OFF)
MESSAGE(STATUS "ENABLE_LUSTRE: ${ENABLE_LUSTRE}")
IF(ENABLE_LUSTRE)
  ADD_DEFINITIONS(-DLUSTRE_SUPPORT)

  FIND_LIBRARY(LUSTREAPI lustreapi)
  IF(LUSTREAPI)
#  INCLUDE_DIRECTORIES(${LUSTREAPI_INCLUDE_DIRS})
    LIST(APPEND MFU_EXTERNAL_LIBS ${LUSTREAPI})
  ENDIF(LUSTREAPI)

  INCLUDE(CheckLibraryExists)

  CHECK_LIBRARY_EXISTS(lustreapi llapi_layout_alloc ${LUSTREAPI} HAVE_LLAPI_LAYOUT)
  IF(HAVE_LLAPI_LAYOUT)
    ADD_DEFINITIONS(-DHAVE_LLAPI_LAYOUT)
  ENDIF(HAVE_LLAPI_LAYOUT)

  CHECK_LIBRARY_EXISTS(lustreapi llapi_file_create ${LUSTREAPI} HAVE_LLAPI_FILE_CREATE)
  IF(HAVE_LLAPI_FILE_CREATE)
    ADD_DEFINITIONS(-DHAVE_LLAPI_FILE_CREATE)
  ENDIF(HAVE_LLAPI_FILE_CREATE)

  CHECK_LIBRARY_EXISTS(lustreapi llapi_file_get_stripe ${LUSTREAPI} HAVE_LLAPI_FILE_GET_STRIPE)
  IF(HAVE_LLAPI_FILE_GET_STRIPE)
    ADD_DEFINITIONS(-DHAVE_LLAPI_FILE_GET_STRIPE)
  ENDIF(HAVE_LLAPI_FILE_GET_STRIPE)

  # todo investigate usage of other lustre #defs
  # - LUSTRE_STAT
ENDIF(ENABLE_LUSTRE)

OPTION(ENABLE_GPFS "Enable GFPS/Spectrum Scale support")
MESSAGE(STATUS "ENABLE_GPFS: ${ENABLE_GPFS}")
IF(ENABLE_GPFS)
  FIND_PACKAGE(GPFS REQUIRED)
  INCLUDE_DIRECTORIES(${GPFS_INCLUDE_DIRS})
  LIST(APPEND MFU_EXTERNAL_LIBS ${GPFS_LIBRARIES})
  ADD_DEFINITIONS(-DGPFS_SUPPORT)
ENDIF(ENABLE_GPFS)

OPTION(ENABLE_EXPERIMENTAL "Build experimental tools" OFF)
MESSAGE(STATUS "ENABLE_EXPERIMENTAL: ${ENABLE_EXPERIMENTAL}")

## HEADERS
INCLUDE(CheckIncludeFile)
CHECK_INCLUDE_FILE(byteswap.h HAVE_BYTESWAP_H)
if(HAVE_BYTESWAP_H)
  ADD_DEFINITIONS(-DHAVE_BYTESWAP_H)
ELSE(HAVE_BYTESWAP_H)
  MESSAGE(SEND_ERROR "byteswap.h is required")
ENDIF(HAVE_BYTESWAP_H)

# Dependencies

## MPI
INCLUDE(SetupMPI)
INCLUDE_DIRECTORIES(${MPI_C_INCLUDE_PATH})
LIST(APPEND MFU_EXTERNAL_LIBS ${MPI_C_LIBRARIES})

## LIBARCHIVE
OPTION(ENABLE_LIBARCHIVE "Enable usage of libarchive and corresponding tools" ON)
MESSAGE(STATUS "ENABLE_LIBARCHIVE: ${ENABLE_LIBARCHIVE}")
# TODO how would we pass a version from spack?
# libarchive 3.1.2 is available on some systems,
# but pick a newer libarchive to avoid bug with files starting with "._",
# which is misinterpretted as a MacOS extension on Linuxlibarchive 3.1.2
IF(ENABLE_LIBARCHIVE)
  FIND_PACKAGE(LibArchive 3.5.1 REQUIRED)
  INCLUDE_DIRECTORIES(${LibArchive_INCLUDE_DIRS})
  LIST(APPEND MFU_EXTERNAL_LIBS ${LibArchive_LIBRARIES})
  ADD_DEFINITIONS(-DLIBARCHIVE_SUPPORT)
ENDIF(ENABLE_LIBARCHIVE)

## hdf5
OPTION(ENABLE_HDF5 "Enable HDF5 library")
MESSAGE(STATUS "ENABLE_HDF5: ${ENABLE_HDF5}")
IF(ENABLE_HDF5)
  FIND_PACKAGE(HDF5 REQUIRED)
  INCLUDE_DIRECTORIES(${HDF5_INCLUDE_DIRS})
  LIST(APPEND MFU_EXTERNAL_LIBS ${HDF5_LIBRARIES})
  ADD_DEFINITIONS(-DHDF5_SUPPORT)
  ADD_DEFINITIONS(-DHDF5_BIN_DIR=\"${HDF5_BIN_DIR}\")
ENDIF(ENABLE_HDF5)

OPTION(ENABLE_DAOS "Enable DAOS support")
MESSAGE(STATUS "ENABLE_DAOS: ${ENABLE_DAOS}")
IF(ENABLE_DAOS)
  SET(CMAKE_EXE_LINKER_FLAGS -luuid)
  FIND_PACKAGE(DAOS REQUIRED)
  INCLUDE_DIRECTORIES(${DAOS_INCLUDE_DIRS})
  LIST(APPEND MFU_EXTERNAL_LIBS ${DAOS_LIBRARIES})
  LIST(APPEND MFU_EXTERNAL_LIBS ${GURT_LIBRARIES})
  LIST(APPEND MFU_EXTERNAL_LIBS ${CART_LIBRARIES})
  LIST(APPEND MFU_EXTERNAL_LIBS ${DUNS_LIBRARIES})
  LIST(APPEND MFU_EXTERNAL_LIBS ${DFS_LIBRARIES})
  LIST(APPEND MFU_EXTERNAL_LIBS ${DAOS_COMMON_LIBRARIES})
  ADD_DEFINITIONS(-DDAOS_SUPPORT)
ENDIF(ENABLE_DAOS)

## BZip2
FIND_PACKAGE(BZip2 REQUIRED)
LIST(APPEND MFU_EXTERNAL_LIBS ${BZIP2_LIBRARIES})

## libcap for checks on linux capabilities
FIND_PACKAGE(LibCap)
IF(LibCap_FOUND)
  ADD_DEFINITIONS(-DHAVE_LIBCAP)
  INCLUDE_DIRECTORIES(${LibCap_INCLUDE_DIRS})
  LIST(APPEND MFU_EXTERNAL_LIBS ${LibCap_LIBRARIES})
ENDIF(LibCap_FOUND)

## OPENSSL for ddup
FIND_PACKAGE(OpenSSL)

# Setup Installation

INCLUDE(GNUInstallDirs)
SET(X_BINDIR ${CMAKE_INSTALL_FULL_BINDIR} CACHE INTERNAL "bin")
SET(X_DATADIR ${CMAKE_INSTALL_FULL_DATADIR} CACHE INTERNAL "share")
SET(X_INCLUDEDIR ${CMAKE_INSTALL_FULL_INCLUDEDIR} CACHE INTERNAL "include")
SET(X_LIBDIR ${CMAKE_INSTALL_FULL_LIBDIR} CACHE INTERNAL "lib")

############
# This sets an rpath to buildtime libraries in build directory
# and rewrites the rpath to the install location during install
# these lines must come before add_library and add_executable macros
############

# https://gitlab.kitware.com/cmake/community/wikis/doc/cmake/RPATH-handling
# use, i.e. don't skip the full RPATH for the build tree
SET(CMAKE_SKIP_BUILD_RPATH  FALSE)

# when building, don't use the install RPATH already
# (but later on when installing)
SET(CMAKE_BUILD_WITH_INSTALL_RPATH FALSE)

SET(CMAKE_INSTALL_RPATH "${CMAKE_INSTALL_PREFIX}/${CMAKE_INSTALL_LIBDIR}")

# add the automatically determined parts of the RPATH
# which point to directories outside the build tree to the install RPATH
SET(CMAKE_INSTALL_RPATH_USE_LINK_PATH TRUE)

# the RPATH to be used when installing, but only if it's not a system directory
LIST(FIND CMAKE_PLATFORM_IMPLICIT_LINK_DIRECTORIES "${CMAKE_INSTALL_PREFIX}/${CMAKE_INSTALL_LIBDIR}" isSystemDir)
IF("${isSystemDir}" STREQUAL "-1")
   SET(CMAKE_INSTALL_RPATH "${CMAKE_INSTALL_PREFIX}/${CMAKE_INSTALL_LIBDIR}")
ENDIF("${isSystemDir}" STREQUAL "-1")

############
# End rpath stuff
############

# Subdirectories
INCLUDE(MFU_ADD_TOOL)
INCLUDE_DIRECTORIES(
  ${CMAKE_CURRENT_SOURCE_DIR}/lwgrp/src
  ${CMAKE_CURRENT_SOURCE_DIR}/dtcmp/src
  ${CMAKE_CURRENT_SOURCE_DIR}/libcircle/libcircle
  ${CMAKE_CURRENT_SOURCE_DIR}/mpifileutils/src/common
)

ADD_SUBDIRECTORY(mpifileutils/src)
ADD_SUBDIRECTORY(mpifileutils/test)
ADD_SUBDIRECTORY(mpifileutils/man)

# Version for the shared mfu library
set(MFU_VERSION_MAJOR 4) # Incompatible API changes
set(MFU_VERSION_MINOR 0) # Backwards-compatible functionality
set(MFU_VERSION_PATCH 0) # Backwards-compatible fixes
set(MFU_VERSION ${MFU_VERSION_MAJOR}.${MFU_VERSION_MINOR}.${MFU_VERSION_PATCH})

LIST(APPEND lwgrp_srcs
    lwgrp/src/lwgrp.c
    lwgrp/src/lwgrp_util.c
    lwgrp/src/lwgrp_chain_ops.c
    lwgrp/src/lwgrp_ring_ops.c
    lwgrp/src/lwgrp_logchain_ops.c
    lwgrp/src/lwgrp_logring_ops.c
    lwgrp/src/lwgrp_comm.c
    lwgrp/src/lwgrp_comm_split.c
)

LIST(APPEND dtcmp_srcs
    dtcmp/src/dtcmp.c
    dtcmp/src/dtcmp_util.c
    dtcmp/src/dtcmp_ops.c
    dtcmp/src/dtcmp_uniqify.c
    dtcmp/src/dtcmp_search_binary.c
    dtcmp/src/dtcmp_partitionz.c
    dtcmp/src/dtcmp_partitionz_list.c
    dtcmp/src/dtcmp_partition_local.c
    dtcmp/src/dtcmp_merge_2way.c
    dtcmp/src/dtcmp_merge_kway_heap.c
    dtcmp/src/dtcmp_select_local_ends.c
    dtcmp/src/dtcmp_select_local_randpartition.c
    dtcmp/src/dtcmp_selectv_rand.c
    dtcmp/src/dtcmp_selectv_medianofmedians.c
    dtcmp/src/dtcmp_is_sorted.c
    dtcmp/src/dtcmp_sort_local_insertionsort.c
    dtcmp/src/dtcmp_sort_local_randquicksort.c
    dtcmp/src/dtcmp_sort_local_mergesort.c
    dtcmp/src/dtcmp_sort_local_qsort.c
    dtcmp/src/dtcmp_sort_allgather.c
    dtcmp/src/dtcmp_sort_bitonic.c
    dtcmp/src/dtcmp_sort_samplesort.c
    dtcmp/src/dtcmp_sortv_allgather.c
    dtcmp/src/dtcmp_sortv_sortgather_scatter.c
    dtcmp/src/dtcmp_sortv_cheng.c
    dtcmp/src/dtcmp_sortz_samplesort.c
    dtcmp/src/dtcmp_rankv_sort.c
    dtcmp/src/dtcmp_seg_exscan.c
)

LIST(APPEND libcircle_srcs
    libcircle/libcircle/lib.c
    libcircle/libcircle/queue.c
    libcircle/libcircle/token.c
    libcircle/libcircle/worker.c
)

# todo re-asses if all of these must be *installed*
LIST(APPEND libmfu_install_headers
  mpifileutils/src/common/mfu.h
  mpifileutils/src/common/mfu_errors.h
  mpifileutils/src/common/mfu_bz2.h
  mpifileutils/src/common/mfu_flist.h
  mpifileutils/src/common/mfu_flist_internal.h
  mpifileutils/src/common/mfu_io.h
  mpifileutils/src/common/mfu_param_path.h
  mpifileutils/src/common/mfu_path.h
  mpifileutils/src/common/mfu_pred.h
  mpifileutils/src/common/mfu_progress.h
  mpifileutils/src/common/mfu_util.h
  )
if(ENABLE_DAOS)
  LIST(APPEND libmfu_install_headers
    mpifileutils/src/common/mfu_daos.h
    )
ENDIF(ENABLE_DAOS)

INSTALL(FILES ${libmfu_install_headers} DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})

# common library
LIST(APPEND libmfu_srcs
  mpifileutils/src/common/mfu_bz2.c
  mpifileutils/src/common/mfu_bz2_static.c
  mpifileutils/src/common/mfu_compress_bz2_libcircle.c
  mpifileutils/src/common/mfu_decompress_bz2_libcircle.c
  mpifileutils/src/common/mfu_flist.c
  mpifileutils/src/common/mfu_flist_chunk.c
  mpifileutils/src/common/mfu_flist_copy.c
  mpifileutils/src/common/mfu_flist_io.c
  mpifileutils/src/common/mfu_flist_chmod.c
  mpifileutils/src/common/mfu_flist_create.c
  mpifileutils/src/common/mfu_flist_remove.c
  mpifileutils/src/common/mfu_flist_sort.c
  mpifileutils/src/common/mfu_flist_usrgrp.c
  mpifileutils/src/common/mfu_flist_walk.c
  mpifileutils/src/common/mfu_io.c
  mpifileutils/src/common/mfu_param_path.c
  mpifileutils/src/common/mfu_path.c
  mpifileutils/src/common/mfu_pred.c
  mpifileutils/src/common/mfu_progress.c
  mpifileutils/src/common/mfu_util.c
  mpifileutils/src/common/strmap.c
  ${lwgrp_srcs}
  ${dtcmp_srcs}
  ${libcircle_srcs}
  )
IF(ENABLE_LIBARCHIVE)
  LIST(APPEND libmfu_srcs
    mpifileutils/src/common/mfu_flist_archive.c
    )
ENDIF(ENABLE_LIBARCHIVE)
IF(ENABLE_DAOS)
  LIST(APPEND libmfu_srcs
    mpifileutils/src/common/mfu_daos.c
    )
ENDIF(ENABLE_DAOS)

ADD_LIBRARY(mfu_o OBJECT ${libmfu_srcs})
SET_TARGET_PROPERTIES(mfu_o PROPERTIES C_STANDARD 99)

ADD_LIBRARY(mfu SHARED $<TARGET_OBJECTS:mfu_o>)
TARGET_LINK_LIBRARIES(mfu LINK_PUBLIC ${MFU_EXTERNAL_LIBS})
SET_TARGET_PROPERTIES(mfu PROPERTIES VERSION ${MFU_VERSION} OUTPUT_NAME mfu CLEAN_DIRECT_OUTPUT 1)
INSTALL(TARGETS mfu DESTINATION ${CMAKE_INSTALL_LIBDIR})

ADD_LIBRARY(mfu-static STATIC $<TARGET_OBJECTS:mfu_o>)
TARGET_LINK_LIBRARIES(mfu-static LINK_PUBLIC ${MFU_EXTERNAL_LIBS})
SET_TARGET_PROPERTIES(mfu-static PROPERTIES OUTPUT_NAME mfu CLEAN_DIRECT_OUTPUT 1)
INSTALL(TARGETS mfu-static DESTINATION ${CMAKE_INSTALL_LIBDIR})

# some projects require a "make install" command to work,
# so define at least a basic INSTALL function
INSTALL(FILES lwgrp/README lwgrp/LICENSE.TXT DESTINATION share/lwgrp)
INSTALL(FILES dtcmp/README.md dtcmp/LICENSE.TXT DESTINATION share/dtcmp)
INSTALL(FILES libcircle/COPYING DESTINATION share/libcircle)
INSTALL(FILES mpifileutils/LICENSE mpifileutils/NOTICE DESTINATION share/mpifileutils)
