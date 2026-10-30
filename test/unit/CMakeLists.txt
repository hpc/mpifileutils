# Known-answer test for the self-contained mfu_sha256 implementation.
# Compile mfu_sha256.c directly into the test so it stays standalone (no MPI /
# libmfu link required).  src/common is already on the include path (added
# with INCLUDE_DIRECTORIES(BEFORE ...) in the top-level CMakeLists).
ADD_EXECUTABLE(test_sha256
  test_sha256.c
  ${CMAKE_SOURCE_DIR}/src/common/mfu_sha256.c
)
SET_TARGET_PROPERTIES(test_sha256 PROPERTIES C_STANDARD 99)
ADD_TEST(NAME unit.sha256_kat COMMAND test_sha256)
SET_TESTS_PROPERTIES("unit.sha256_kat" PROPERTIES LABELS unit)



SET(TEST_TMPDIR "${CMAKE_CURRENT_SOURCE_DIR}/test_tempdir" CACHE PATH
  "Directory used for running unit tests"
)

SET(MFU_UNIT_TEST_ENVIRONMENT
  "TEST_TMPDIR=${TEST_TMPDIR}"
)

SET(MFU_UNIT_TEST_MPI_NAMES
  test_mfu_flist_walk
  test_mfu_flist_chmod
)

FOREACH(name ${MFU_UNIT_TEST_MPI_NAMES})
  ADD_EXECUTABLE(${name} ${name}.c common.c scenarios.c)
  TARGET_LINK_LIBRARIES(${name} PRIVATE mfu)

  ADD_TEST(
    NAME unit.${name}
    COMMAND ${MPIEXEC_EXECUTABLE} ${MPIEXEC_NUMPROC_FLAG} ${MPIEXEC_MAX_NUMPROCS} ${name}
  )
  SET_TESTS_PROPERTIES("unit.${name}" PROPERTIES
    ENVIRONMENT "${MFU_UNIT_TEST_ENVIRONMENT}"
    LABELS "unit"
  )
ENDFOREACH()
