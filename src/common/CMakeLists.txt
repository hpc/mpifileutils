# Version for the shared mfu library
set(MFU_VERSION_MAJOR 4) # Incompatible API changes
set(MFU_VERSION_MINOR 0) # Backwards-compatible functionality
set(MFU_VERSION_PATCH 0) # Backwards-compatible fixes
set(MFU_VERSION ${MFU_VERSION_MAJOR}.${MFU_VERSION_MINOR}.${MFU_VERSION_PATCH})

# todo re-asses if all of these must be *installed*
LIST(APPEND libmfu_install_headers
  mfu.h
  mfu_errors.h
  mfu_bz2.h
  mfu_flist.h
  mfu_flist_internal.h
  mfu_io.h
  mfu_param_path.h
  mfu_path.h
  mfu_pred.h
  mfu_progress.h
  mfu_util.h
  )
if(ENABLE_DAOS)
  LIST(APPEND libmfu_install_headers
    mfu_daos.h
    )
ENDIF(ENABLE_DAOS)

INSTALL(FILES ${libmfu_install_headers} DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})

# common library
LIST(APPEND libmfu_srcs
  mfu_bz2.c
  mfu_bz2_static.c
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
  mfu_progress.c
  mfu_util.c
  strmap.c
  )
IF(ENABLE_LIBARCHIVE)
  LIST(APPEND libmfu_srcs
    mfu_flist_archive.c
	)
ENDIF(ENABLE_LIBARCHIVE)
IF(ENABLE_DAOS)
  LIST(APPEND libmfu_srcs
    mfu_daos.c
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
