# - Try to find libattr
# Once done this will define
#  LibAttr_FOUND - System has libattr
#  LibAttr_INCLUDE_DIRS - The libattr include directories
#  LibAttr_LIBRARIES - The libraries needed to use libattr

FIND_LIBRARY(LibAttr_LIBRARIES
    NAMES attr
)

FIND_PATH(LibAttr_INCLUDE_DIRS
    NAMES attr/libattr.h
)

INCLUDE(FindPackageHandleStandardArgs)
FIND_PACKAGE_HANDLE_STANDARD_ARGS(LibAttr DEFAULT_MSG
    LibAttr_LIBRARIES
    LibAttr_INCLUDE_DIRS
)

# Hide these vars from ccmake GUI
MARK_AS_ADVANCED(
	LibAttr_LIBRARIES
	LibAttr_INCLUDE_DIRS
)
