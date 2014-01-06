AC_DEFUN([X_AC_EXPERIMENTAL], [
  AC_MSG_CHECKING([if experimental tools are enabled])
  AC_ARG_ENABLE([experimental], [AC_HELP_STRING([--with-experimental], [enable experimental utilities])],
    [
      x_ac_experimental=yes
    ],
    [
      x_ac_experimental=no
    ]
  )
  AM_CONDITIONAL(EXPERIMENTAL_ENABLED, [test "x$x_ac_experimental" = xyes])
])
