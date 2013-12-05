AC_INIT([dparallel],[0.0.1],[jon@bringhurst.org],[dparallel],[http://filecopy.org])

# Keep artifacts in directories as much as possible.
AC_CONFIG_AUX_DIR(auxdir)
AC_CONFIG_MACRO_DIR([m4])
AC_CONFIG_HEADERS([config.h])

AM_INIT_AUTOMAKE([foreign])
m4_ifdef([AM_SILENT_RULES], [AM_SILENT_RULES([yes])])

# Check for programs
AC_PROG_CC
AC_PROG_CXX
AC_PROG_AWK
AC_PROG_CC
AC_PROG_CPP
AC_PROG_INSTALL
AC_PROG_MAKE_SET
AC_PROG_RANLIB

# Needed for docs
X_AC_DPARALLEL_DOXYGEN

# Check for libcircle
PKG_CHECK_MODULES([libcircle], libcircle)

# Pull in libtool. We need to support ancient autotools, so don't use LT_INIT.
#LT_INIT
AC_PROG_LIBTOOL

# Checks for headers.
AC_CHECK_HEADERS([inttypes.h limits.h stdint.h stdlib.h string.h uinstd.h])

# Checks for typedefs, structures, and compiler characteristics.
AC_HEADER_STDBOOL
AC_TYPE_OFF_T
AC_TYPE_SIZE_T
AC_TYPE_SSIZE_T
AC_TYPE_UINT16_T
AC_TYPE_UINT32_T
AC_TYPE_UINT64_T
AC_TYPE_INT64_T

# Check for library functions and headers.
AC_CHECK_FUNCS([memset realpath strerror lchown strdup utime])
AC_CHECK_HEADERS([sys/time.h utime.h])
AC_FUNC_MALLOC

# Check for MPI
LX_FIND_MPI
test "x$have_C_mpi" = xyes || \
    AC_MSG_ERROR([You should check to see if MPI is setup properly.])
AM_CONDITIONAL(HAVE_MPI, [test "x$have_C_mpi" = xyes])

echo
echo "========================================================"
echo "==       dparallel: final build configuration         =="
echo "========================================================"
echo "  MPI ............................................ $have_C_mpi"
echo "========================================================"
echo

AC_CONFIG_FILES([Makefile              \
                 src/Makefile          \
                 doc/Makefile          \
                 doc/man/Makefile      \
                 doc/man/man1/Makefile \
])
AC_OUTPUT
