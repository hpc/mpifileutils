# - Try to find libgpfs
# Once done this will define
#  GPFS_FOUND - System has libgpfs
#  GPFS_INCLUDE_DIRS - The libgpfs include directories
#  GPFS_LIBRARIES - The libraries needed to use libgpfs

FIND_PATH(WITH_GPFS_PREFIX
    NAMES include/gpfs.h
)

FIND_LIBRARY(GPFS_LIBRARIES
    NAMES gpfs
    HINTS ${WITH_GPFS_PREFIX}/lib
)

FIND_PATH(GPFS_INCLUDE_DIRS
    NAMES gpfs.h
    HINTS ${WITH_GPFS_PREFIX}/include
)

INCLUDE(FindPackageHandleStandardArgs)
FIND_PACKAGE_HANDLE_STANDARD_ARGS(GPFS DEFAULT_MSG
    GPFS_LIBRARIES
    GPFS_INCLUDE_DIRS
)

# Hide these vars from ccmake GUI
MARK_AS_ADVANCED(
	GPFS_LIBRARIES
	GPFS_INCLUDE_DIRS
)
