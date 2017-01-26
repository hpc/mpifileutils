# looks for lustre header files
# if found, defines LUSTRE_SUPPORT=1 in config.h
AC_DEFUN([X_AC_LUSTRE], [
  AC_MSG_CHECKING([whether to enable Lustre support])
  AC_ARG_ENABLE([lustre],
                AC_HELP_STRING([--enable-lustre], [enable Lustre support]),
                [], [enable_lustre="no"])
  AC_MSG_RESULT([$enable_lustre])

  AC_MSG_CHECKING([for Lustre])
  AS_IF([test -e "/usr/include/lustre/lustre_user.h" && test -e "/usr/include/lustre/lustreapi.h"],
        [enable_lustre="yes"],
        [enable_lustre="no"])
  AC_MSG_RESULT([$enable_lustre])

#  AC_SEARCH_LIBS([llapi_file_create], [lustreapi], [], [
#    AC_MSG_ERROR([couldn't find liblustreapi])], [])

  AS_IF([test "x$enable_lustre" = xyes], [
    AC_DEFINE(LUSTRE_SUPPORT, 1, [enable Lustre support])
    LUSTRE_LIBS="-llustreapi"
    AC_SUBST(LUSTRE_LIBS)
  ])
])
