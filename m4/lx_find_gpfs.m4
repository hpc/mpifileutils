dnl #
dnl # Determines if GPFS API is installed.
dnl #
AC_DEFUN([X_AC_GPFS], [

  AC_ARG_WITH([gpfs], [AC_HELP_STRING([--with-gpfs=PATH],
	  [path to GPFS installion [default=/usr/lpp/mmfs]])], [
  	GPFS_INCLUDE="${withval}/include"
	  GPFS_LIB="${withval}/lib"
	  AC_MSG_RESULT("${withval}")
  ], [
    GPFS_INCLUDE="/usr/lpp/mmfs/include"
    GPFS_LIB="/usr/lpp/mmfs/lib"
  ])

	AC_ARG_ENABLE(gpfs,
		AC_HELP_STRING([--enable-gpfs],
		[enable GPFS/Spectrum Scale support]),
		[], [enable_gpfs=no])



	AS_CASE(["x$enable_gpfs"],
		["xcheck"],
		[gpfs_found="yes"],
		["xyes"],
		[gpfs_found="yes"],
		["xno"],
		[gpfs_found="no"],
		[AC_MSG_ERROR([Unknown option $enable_gpfs])])


	AS_IF([test "x$enable_gpfs" != "xno"], [
		AC_CHECK_HEADERS([gpfs.h],
			[], [gpfs_found="no"], [])

		AS_IF([test "x$gpfs_found" != "xno"], [
			AC_DEFINE([GPFS_SUPPORT], [1],
			    [Define to 1 if GPFS can be used])
		  CFLAGS="$CFLAGS -I${GPFS_INCLUDE} ${MPI_CFLAGS}"
		  CXXFLAGS="$CXXFLAGS -I${GPFS_INCLUDE} ${MPI_CXXFLAGS}"
			LDFLAGS="$LDFLAGS -L${GPFS_LIB} ${MPI_CLDFLAGS}"
			GPFS_LIBS="-lgpfs"
			AC_SUBST(GPFS_LIBS)
		])
	])

	AC_MSG_CHECKING([whether to enable GPFS support])
	AS_IF([test "x$enable_gpfs" != "xno"], [
	  AS_IF([test "x$gpfs_found" != "xyes"], [
		  AC_MSG_ERROR([GPFS libraries are not available.])
	  ])
	])
	AC_MSG_RESULT([$gpfs_found])
	AS_IF([test "x$enable_gpfs" != "xno"], [
  	AC_SEARCH_LIBS([gpfs_fgetattrs], [gpfs], [], [
      AC_MSG_ERROR(
				[couldn't find a suitable GPFS library, use --with-gpfs=PATH])], [])
  ])
])
