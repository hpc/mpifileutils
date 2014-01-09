AC_DEFUN([X_AC_PANDOC], [
  AC_MSG_CHECKING([for pandoc documentation program])
  AC_CHECK_PROGS([PANDOC], [pandoc])
  AM_CONDITIONAL([HAVE_PANDOC],
    [test -n "$PANDOC"])
])
