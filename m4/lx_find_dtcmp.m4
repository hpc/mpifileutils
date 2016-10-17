AC_DEFUN([X_AC_DTCMP], [
  AC_MSG_CHECKING([for libDTCMP])
  AC_ARG_WITH([dtcmp], [AC_HELP_STRING([--with-dtcmp=PATH],
    [path to installed libDTCMP [default=/usr/local]])], [
    LIBDTCMP_INCLUDE="${withval}/include"
    LIBDTCMP_LIB="${withval}/lib"
    AC_MSG_RESULT("${withval}")
  ], [
    LIBDTCMP_INCLUDE="/usr/local/include"
    LIBDTCMP_LIB="/usr/local/lib"
    AC_MSG_RESULT(/usr/local)
  ])
  AC_SUBST(LIBDTCMP_INCLUDE)
  AC_SUBST(LIBDTCMP_LIB)

  CFLAGS="$CFLAGS -I${LIBDTCMP_INCLUDE} ${MPI_CFLAGS}"
  CXXFLAGS="$CXXFLAGS -I${LIBDTCMP_INCLUDE} ${MPI_CXXFLAGS}"
  LDFLAGS="$LDFLAGS -L${LIBDTCMP_LIB} ${MPI_CLDFLAGS}"

  AC_SEARCH_LIBS([DTCMP_Init], [dtcmp], [], [
    AC_MSG_ERROR([couldn't find a suitable libdtcmp, use --with-dtcmp=PATH])], [])
])
