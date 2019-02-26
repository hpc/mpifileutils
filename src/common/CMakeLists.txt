# todo re-asses if all of these must be *installed*
LIST(APPEND libmfu_install_headers
  mfu.h
  mfu_bz2.h
  mfu_flist.h
  mfu_flist_internal.h
  mfu_io.h
  mfu_param_path.h
  mfu_path.h
  mfu_pred.h
  mfu_util.h
  )
INSTALL(FILES ${libmfu_install_headers} DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})

# common library
LIST(APPEND libmfu_srcs
  mfu_compress_bz2_libcircle.c
  mfu_decompress_bz2_libcircle.c
  mfu_flist.c
  mfu_flist_chunk.c
  mfu_flist_copy.c
  mfu_flist_io.c
  mfu_flist_chmod.c
  mfu_flist_create.c
  mfu_flist_remove.c
  mfu_flist_sort.c
  mfu_flist_usrgrp.c
  mfu_flist_walk.c
  mfu_io.c
  mfu_param_path.c
  mfu_path.c
  mfu_pred.c
  mfu_util.c
  strmap.c
  )

## TODO: figure out how to build object files

BLT_ADD_LIBRARY(NAME mfu
  SOURCES ${libmfu_srcs}
  OUTPUT_NAME mfu
  SHARED TRUE
  INCLUDES ${CMAKE_CURRENT_SOURCE_DIR}
  DEPENDS_ON mpi dtcmp libcircle bzip2)
SET_TARGET_PROPERTIES(mfu PROPERTIES C_STANDARD 99)
INSTALL(TARGETS mfu DESTINATION ${CMAKE_INSTALL_LIBDIR})

BLT_ADD_LIBRARY(NAME mfu-static
  SOURCES ${libmfu_srcs}
  OUTPUT_NAME mfu
  SHARED FALSE
  INCLUDES ${CMAKE_CURRENT_SOURCE_DIR}
  DEPENDS_ON mpi dtcmp libcircle bzip2)
SET_TARGET_PROPERTIES(mfu-static PROPERTIES C_STANDARD 99)
INSTALL(TARGETS mfu-static DESTINATION ${CMAKE_INSTALL_LIBDIR})
