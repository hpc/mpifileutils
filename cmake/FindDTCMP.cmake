# - Try to find libdtcmp
# Once done this will define
#  DTCMP_FOUND - System has libdtcmp
#  DTCMP_INCLUDE_DIRS - The libdtcmp include directories
#  DTCMP_LIBRARIES - The libraries needed to use libdtcmp

FIND_PATH(WITH_DTCMP_PREFIX
    NAMES include/dtcmp.h
)

FIND_LIBRARY(DTCMP_LIBRARIES
    NAMES dtcmp
    HINTS ${WITH_DTCMP_PREFIX}/lib
)

FIND_PATH(DTCMP_INCLUDE_DIRS
    NAMES dtcmp.h
    HINTS ${WITH_DTCMP_PREFIX}/include
)

INCLUDE(FindPackageHandleStandardArgs)
FIND_PACKAGE_HANDLE_STANDARD_ARGS(DTCMP DEFAULT_MSG
    DTCMP_LIBRARIES
    DTCMP_INCLUDE_DIRS
)

# Hide these vars from ccmake GUI
MARK_AS_ADVANCED(
	DTCMP_LIBRARIES
	DTCMP_INCLUDE_DIRS
)
