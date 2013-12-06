AC_INIT([dcp],[0.1.0-rc.2],[github-hpc@lanl.gov],[dcp],[http://filecopy.org])

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
AC_PROG_LN_S
AC_PROG_MAKE_SET
AC_PROG_RANLIB

# Needed for docs
X_AC_DCOPY_DOXYGEN

# Needed for tests
#X_AC_DCP_CHECK

# Check for libcircle
PKG_CHECK_MODULES([libcircle], libcircle)

# Pull in libtool. We need to support ancient autotools, so don't use LT_INIT.
#LT_INIT
AC_PROG_LIBTOOL

# Checks for headers.
AC_CHECK_HEADERS([fcntl.h inttypes.h limits.h stdint.h stdlib.h string.h \
                  unistd.h lustre/lustre_user.h lustre/ll_fiemap.h \
                  lustre/liblustreapi.h])

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

# Check for largefile support.
AC_SYS_LARGEFILE
AC_FUNC_FSEEKO
AC_FUNC_LSTAT_FOLLOWS_SLASHED_SYMLINK
AC_FUNC_MALLOC
AC_CHECK_FUNC(lseek64, , AC_MSG_ERROR([This OS does not appear to support lseek64(3).]))
AC_CHECK_FUNC(fstat64, , AC_MSG_ERROR([This OS does not appear to support fstat64(3).]))
AC_CHECK_FUNC(stat64, , AC_MSG_ERROR([This OS does not appear to support stat64(3).]))

# Check for MPI
LX_FIND_MPI
test "x$have_C_mpi" = xyes || \
    AC_MSG_ERROR([You should check to see if MPI is setup properly.])
AM_CONDITIONAL(HAVE_MPI, [test "x$have_C_mpi" = xyes])

# Check for xattr support.
X_AC_DCOPY_XATTRS

echo
echo "========================================================"
echo "==           dcp: final build configuration           =="
echo "========================================================"
echo "  MPI ............................................ $have_C_mpi"
echo "  XATTRs ......................................... $x_ac_dcopy_xattr"
echo "========================================================"
echo

AC_CONFIG_FILES([Makefile              \
                 src/Makefile          \
                 doc/Makefile          \
                 doc/man/Makefile      \
                 doc/man/man1/Makefile \
])
AC_OUTPUT
