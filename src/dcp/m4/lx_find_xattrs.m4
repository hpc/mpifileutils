AC_DEFUN([X_AC_DCOPY_XATTRS], [
  AC_MSG_CHECKING([for xattr support])

  AC_ARG_ENABLE([xattr],
    AC_HELP_STRING([--disable-xattr], [Disable xattr support (default: auto)]),
      [x_ac_dcopy_xattr=$enableval],
      [x_ac_dcopy_xattr=auto])
  AC_MSG_RESULT([$x_ac_dcopy_xattr])

  if test xno != "x$x_ac_dcopy_xattr"; then
    AC_CHECK_HEADER([sys/xattr.h],
    AC_CHECK_HEADER([attr/xattr.h],
    AC_CHECK_FUNC([setxattr], [x_ac_dcopy_xattr=yes])))

    if test yes = $x_ac_dcopy_xattr; then
      AC_DEFINE(DCOPY_USE_XATTRS, 1, [if you want to build xattr support])
    fi
  fi
])
