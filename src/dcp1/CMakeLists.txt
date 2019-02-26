INCLUDE_DIRECTORIES(${CMAKE_CURRENT_SOURCE_DIR})

LIST(APPEND dcp1_srcs
  dcp1.c
  cleanup.c
  common.c
  compare.c
  copy.c
  handle_args.c
  treewalk.c
  )

BLT_ADD_EXECUTABLE(NAME dcp1
  SOURCES ${dcp1_srcs}
  DEPENDS_ON mfu)
INSTALL(TARGETS dcp1 RUNTIME DESTINATION ${X_BINDIR})
