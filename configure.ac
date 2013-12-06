AC_INIT([fileutils],[0.0.1-alpha.1],[fileutils-discuss@googlegroups.com],[fileutils],[http://fileutils.io])

# Keep artifacts in directories as much as possible.
AC_CONFIG_AUX_DIR(auxdir)
AC_CONFIG_MACRO_DIR([m4])
AC_CONFIG_HEADERS([config.h])

AM_INIT_AUTOMAKE([foreign])
#m4_ifdef([AM_SILENT_RULES], [AM_SILENT_RULES([yes])])

# Check for programs

# Check for doxygen.
X_AC_DOXYGEN

# Check for libcircle
PKG_CHECK_MODULES([libcircle], libcircle)

# Check for liblwgrp
AC_CHECK_HEADER([lwgrp.h])

# Check for libdtcmp.
AC_CHECK_HEADER([dtcmp.h])

# Pull in libtool.
LT_INIT

# Check for MPI
LX_FIND_MPI
test "x$have_C_mpi" = xyes || \
    AC_MSG_ERROR([You should check to see if MPI is setup properly.])
AM_CONDITIONAL(HAVE_MPI, [test "x$have_C_mpi" = xyes])

echo
echo "========================================================"
echo "==        fileutils: final build configuration        =="
echo "========================================================"
echo "  MPI ............................................ $have_C_mpi"
echo "========================================================"
echo

AC_CONFIG_FILES([Makefile              \
                 src/Makefile          \
                 src/common/Makefile   \
                 src/dcmp/Makefile     \
                 doc/Makefile          \
                 doc/man/Makefile      \
                 doc/man/man1/Makefile \
])

AC_OUTPUT
