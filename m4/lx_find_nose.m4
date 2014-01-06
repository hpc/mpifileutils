AC_DEFUN([X_AC_NOSETESTS], [
  AC_MSG_CHECKING([for nose test program])
  AC_CHECK_PROGS([NOSETESTS], [nosetests])
  AM_CONDITIONAL([HAVE_NOSETESTS],
    [test -n "$NOSETESTS"])
])
