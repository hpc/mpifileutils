AC_DEFUN([X_AC_LUSTRE], [
AC_MSG_CHECKING([whether to enable Lustre support])
AC_ARG_ENABLE([lustre],
        AC_HELP_STRING([--enable-lustre],
                [enable Lustre support]),
        [], [enable_lustre="no"])
AC_MSG_RESULT([$enable_lustre])

AC_MSG_CHECKING([whether to Lustre head files exist])
AS_IF([test -e "/usr/include/lustre/lustre_user.h" && test -e "/usr/include/lustre/lustreapi.h"],
        [enable_lustre="yes"],
        [enable_lustre="no"])
AC_MSG_RESULT([$enable_lustre])

AS_IF([test "x$enable_lustre" = xyes],
        [AC_DEFINE(LUSTRE_SUPPORT, 1, [enable Lustre support])])
])
