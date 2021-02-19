# - Try to find hdf5
# Once done this will define
#  HDF5_FOUND - System has hdf5
#  HDF5_INCLUDE_DIRS - The hdf5 include directories
#  HDF5_LIBRARIES - The libraries needed to use hdf5

FIND_PATH(WITH_HDF5_PREFIX
    NAMES include/hdf5.h
)

FIND_LIBRARY(HDF5_LIBRARIES
    NAMES hdf5 
    HINTS ${WITH_HDF5_PREFIX}/lib
)

FIND_PATH(HDF5_INCLUDE_DIRS
    NAMES hdf5.h
    HINTS ${WITH_HDF5_PREFIX}/include
)

FIND_PATH(HDF5_BIN_DIR
    NAMES h5cc h5pcc
    HINTS ${WITH_HDF5_PREFIX}/bin
)

INCLUDE(FindPackageHandleStandardArgs)
FIND_PACKAGE_HANDLE_STANDARD_ARGS(HDF5 DEFAULT_MSG
    HDF5_LIBRARIES
    HDF5_INCLUDE_DIRS
    HDF5_BIN_DIR
)

# Hide these vars from ccmake GUI
MARK_AS_ADVANCED(
    HDF5_LIBRARIES
    HDF5_INCLUDE_DIRS
    HDF5_BIN_DIR
)
