AC_DEFUN([X_AC_DCOPY_DOXYGEN], [
  AC_MSG_CHECKING([for dcopy doxygen generation])
  AC_ARG_ENABLE(
    [doxygen],
    AS_HELP_STRING(--enable-doxygen, enable doxygen documentation),
    [ AC_CHECK_PROGS([DOXYGEN], [doxygen])
      x_ac_dcopy_doxygen=yes
    ],
    [
      x_ac_dcopy_doxygen=no
    ]
  )
  AM_CONDITIONAL([HAVE_DOXYGEN],
    [test -n "$DOXYGEN"])
  if test [HAVE_DOXYGEN]; then 
    AC_OUTPUT([doc/Doxyfile])
  fi
])
