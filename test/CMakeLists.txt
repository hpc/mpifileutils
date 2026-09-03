# Known-answer test for the self-contained mfu_sha256 implementation.
# Compile mfu_sha256.c directly into the test so it stays standalone (no MPI /
# libmfu link required).  src/common is already on the include path (added
# with INCLUDE_DIRECTORIES(BEFORE ...) in the top-level CMakeLists).
ADD_EXECUTABLE(test_sha256
  tests/test_sha256/test_sha256.c
  ${CMAKE_SOURCE_DIR}/src/common/mfu_sha256.c
)
SET_TARGET_PROPERTIES(test_sha256 PROPERTIES C_STANDARD 99)
ADD_TEST(NAME sha256_kat COMMAND test_sha256)
