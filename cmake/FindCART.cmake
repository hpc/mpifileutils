# - Try to find cart libs 
# Once done this will define
#  cart_FOUND - System has libcart
#  cart_INCLUDE_DIRS - cart include directories
#  cart_LIBRARIES - The libraries needed to use cart
#  gurt_LIBRARIES - The libraries needed to use gurt (part of cart)

FIND_PATH(WITH_CART_PREFIX
    NAMES cart/include
)

FIND_LIBRARY(CART_LIBRARIES
    NAMES cart
    HINTS ${WITH_CART_PREFIX}/lib
)

FIND_LIBRARY(GURT_LIBRARIES
    NAMES gurt
    HINTS ${WITH_CART_PREFIX}/lib
)

FIND_PATH(CART_INCLUDE_DIRS
    NAMES cart/types.h
    HINTS ${WITH_CART_PREFIX}/include
)

INCLUDE(FindPackageHandleStandardArgs)
FIND_PACKAGE_HANDLE_STANDARD_ARGS(CART DEFAULT_MSG
    CART_LIBRARIES
    CART_INCLUDE_DIRS
)

# Hide these vars from ccmake GUI
MARK_AS_ADVANCED(
	CART_LIBRARIES
	GURT_LIBRARIES
	CART_INCLUDE_DIRS
)
