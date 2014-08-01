AC_DEFUN([X_AC_EXPERIMENTAL], [
  AC_MSG_CHECKING([if experimental tools are enabled])
  AC_ARG_ENABLE([experimental], [AC_HELP_STRING([--enable-experimental], [enable experimental utilities])],
    [
      x_ac_experimental=yes
      AC_MSG_RESULT(yes)
    ],
    [
      x_ac_experimental=no
      AC_MSG_RESULT(no)
    ]
  )
  AM_CONDITIONAL(EXPERIMENTAL_ENABLED, [test "x$x_ac_experimental" = xyes])
])
