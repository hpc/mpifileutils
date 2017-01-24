AC_INIT([mpifileutils],[0.6],[mpifileutils-discuss@googlegroups.com],[mpifileutils],[http://fileutils.io])

# Keep artifacts in directories as much as possible.
AC_CONFIG_AUX_DIR(auxdir)
AC_CONFIG_MACRO_DIR([m4])
AC_CONFIG_HEADERS([config.h])

# add foreign option to avoid GPL files
AM_INIT_AUTOMAKE([foreign])
m4_ifdef([AM_SILENT_RULES], [AM_SILENT_RULES([yes])])

# Check for programs
AC_PROG_CXX
AC_PROG_AWK
AC_PROG_CC
AC_PROG_CPP
AC_PROG_INSTALL
AC_PROG_LN_S
AC_PROG_MAKE_SET
AC_PROG_RANLIB

# Checks for header files.
AC_CHECK_HEADERS([fcntl.h inttypes.h limits.h stdint.h stdlib.h string.h sys/ioctl.h sys/time.h unistd.h utime.h])
AC_CHECK_HEADER_STDBOOL

# Checks for typedefs, structures, and compiler characteristics.
AC_TYPE_UID_T
AC_C_INLINE
AC_TYPE_INT32_T
AC_TYPE_INT64_T
AC_TYPE_MODE_T
AC_TYPE_OFF_T
AC_TYPE_SIZE_T
AC_TYPE_SSIZE_T
AC_CHECK_MEMBERS([struct stat.st_blksize])
AC_STRUCT_ST_BLOCKS
AC_CHECK_MEMBERS([struct stat.st_rdev])
AC_TYPE_UINT16_T
AC_TYPE_UINT32_T
AC_TYPE_UINT64_T

# Checks for library functions.
AC_FUNC_ERROR_AT_LINE
AC_FUNC_LSTAT_FOLLOWS_SLASHED_SYMLINK
AC_FUNC_MALLOC
AC_CHECK_FUNCS([endgrent endpwent getcwd lchown memset mkdir realpath rmdir strchr strdup strerror strstr utime])

##
# enable libtool
##
AC_PROG_LIBTOOL

# Check for nose test program.
X_AC_PANDOC

# Check for nose test program.
X_AC_NOSETESTS

# Check for libarchive (FIXME: should eventually remove this)
PKG_CHECK_MODULES([libarchive], libarchive)

# Check for MPI
LX_FIND_MPI
test "x$have_C_mpi" = xyes || \
    AC_MSG_ERROR([You should check to see if MPI is setup properly.])
AM_CONDITIONAL(HAVE_MPI, [test "x$have_C_mpi" = xyes])

# Check for libcircle
PKG_CHECK_MODULES([libcircle], libcircle)
#PKG_CHECK_MODULES([DTCMP], libdtcmp)

# Check for libdtcmp.
X_AC_DTCMP

# Check for Lustre
X_AC_LUSTRE

# Check for libcrypto
#PKG_CHECK_MODULES([CRYPTO], [libcrypto], [INCLUDES="$INCLUDES $CRYPTO_CFLAGS"; LIBS="$LIBS $CRYPTO_LIBS"], [AC_MSG_ERROR(libcrypto not found.)])
AC_SEARCH_LIBS([SHA256_Init], [crypto], [], [AC_MSG_ERROR([could not find libcrypto])], [])

AC_CONFIG_FILES([Makefile                \
                 src/Makefile            \
                 src/common/Makefile     \
                 src/dchmod/Makefile     \
                 src/dcmp/Makefile       \
                 src/dcp1/Makefile       \
                 src/dcp/Makefile       \
                 src/ddup/Makefile       \
                 src/dfilemaker/Makefile \
                 src/drm/Makefile        \
                 src/dwalk/Makefile      \
                 doc/Makefile            \
                 doc/man/Makefile        \
                 doc/man/man1/Makefile   \
])
#                 doc/html/Makefile       \

# Check if we should build the experimental utilities.
X_AC_EXPERIMENTAL
if test [EXPERIMENTAL_ENABLED]; then 
AC_CONFIG_FILES([experimental/Makefile           \
                 experimental/dfind/Makefile     \
                 experimental/dgrep/Makefile     \
                 experimental/dparallel/Makefile \
                 experimental/dtar/Makefile      \
                 experimental/dsh/Makefile        \
])
fi

AC_OUTPUT
