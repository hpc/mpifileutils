AC_DEFUN([X_AC_LWGRP], [
  AC_MSG_CHECKING([for libLWGRP])
  AC_ARG_WITH([liblwgrp], [AC_HELP_STRING([--with-lwgrp=PATH], [path to installed libLWGRP [default=/usr/local]])], [
    LIBLWGRP_INCLUDE="${withval}/include"
    LIBLWGRP_LIB="${withval}/lib"
    AC_MSG_RESULT("${withval}")
  ], [
    LIBLWGRP_INCLUDE="/usr/local/include"
    LIBLWGRP_LIB="/usr/local/lib"
    AC_MSG_RESULT(/usr/local)
  ])
  AC_SUBST(LIBLWGRP_INCLUDE)
  AC_SUBST(LIBLWGRP_LIB)

  CFLAGS="$CFLAGS -I${LIBLWGRP_INCLUDE}"
  CXXFLAGS="$CXXFLAGS -I${LIBLWGRP_INCLUDE}"
  LDFLAGS="$LDFLAGS -L${LIBLWGRP_LIB}"

  AC_CHECK_LIB([lwgrp], [lwgrp_free], [], [AC_MSG_ERROR([couldn't find a suitable liblwgrp, use --with-lwgrp=PATH])])
])
