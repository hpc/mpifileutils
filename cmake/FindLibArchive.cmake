# - Try to find libarchive
# Once done this will define
#  LibArchive_FOUND - System has libarchive
#  LibArchive_INCLUDE_DIRS - The libarchive include directories
#  LibArchive_LIBRARIES - The libraries needed to use libarchive

FIND_PATH(WITH_LibArchive_PREFIX
    NAMES include/libarchive.h
)

FIND_LIBRARY(LibArchive_LIBRARIES
    NAMES archive
    HINTS ${WITH_LibArchive_PREFIX}/lib
)

FIND_PATH(LibArchive_INCLUDE_DIRS
    NAMES archive.h
    HINTS ${WITH_LibArchive_PREFIX}/include
)

INCLUDE(FindPackageHandleStandardArgs)
FIND_PACKAGE_HANDLE_STANDARD_ARGS(LibArchive DEFAULT_MSG
    LibArchive_LIBRARIES
    LibArchive_INCLUDE_DIRS
)

# Hide these vars from ccmake GUI
MARK_AS_ADVANCED(
	LibArchive_LIBRARIES
	LibArchive_INCLUDE_DIRS
)
