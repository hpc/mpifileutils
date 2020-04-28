# - Try to find libcap
# Once done this will define
#  LibCap_FOUND - System has libcap
#  LibCap_INCLUDE_DIRS - The libcap include directories
#  LibCap_LIBRARIES - The libraries needed to use libcap

FIND_LIBRARY(LibCap_LIBRARIES
    NAMES cap
)

FIND_PATH(LibCap_INCLUDE_DIRS
    NAMES sys/capability.h
)

INCLUDE(FindPackageHandleStandardArgs)
FIND_PACKAGE_HANDLE_STANDARD_ARGS(LibCap DEFAULT_MSG
    LibCap_LIBRARIES
    LibCap_INCLUDE_DIRS
)

# Hide these vars from ccmake GUI
MARK_AS_ADVANCED(
	LibCap_LIBRARIES
	LibCap_INCLUDE_DIRS
)
