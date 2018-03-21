dnl #
dnl # Determines if Lustre API is installed.
dnl # Checks if Lustre API has llapi_layout.
dnl #
AC_DEFUN([X_AC_LUSTRE], [
	AC_ARG_ENABLE(lustre,
		AC_HELP_STRING([--enable-lustre],
		[enable Lustre support]),
		[], [enable_lustre=no])

	AS_CASE(["x$enable_lustre"],
		["xcheck"],
		[lustre_found="yes"],
		["xyes"],
		[lustre_found="yes"],
		["xno"],
		[lustre_found="no"],
		[AC_MSG_ERROR([Unknown option $enable_lustre])])


	if test "x$enable_lustre" != xno; then
		AC_CHECK_HEADERS([lustre/lustre_user.h lustre/lustreapi.h],
			[], [lustre_found="no"], [])

		if test "x$lustre_found" != "xno"; then
			AC_DEFINE([LUSTRE_SUPPORT], [1],
			    [Define to 1 if Lustre can be used])
			LUSTRE_LIBS="-llustreapi"
			AC_SUBST(LUSTRE_LIBS)

			AC_CHECK_LIB(
			    [lustreapi],
			    [llapi_layout_alloc],
			    [AC_DEFINE([HAVE_LLAPI_LAYOUT], [1],
			        [Define to 1 if llapi_layout can be used.])],
			    [], [])

			AC_CHECK_LIB(
			    [lustreapi],
			    [llapi_file_create],
			    [AC_DEFINE([HAVE_LLAPI_FILE_CREATE], [1],
			        [Define to 1 if llapi_file_create can be used.])],
			    [], [])

			AC_CHECK_LIB(
			    [lustreapi],
			    [llapi_file_get_stripe],
			    [AC_DEFINE([HAVE_LLAPI_FILE_GET_STRIPE], [1],
			        [Define to 1 if llapi_file_get_stripe can be used.])],
			    [], [])
		fi
	fi

	AC_MSG_CHECKING([whether to enable Lustre support])
	if test "x$lustre_found" = "xno" && test "xenable_lustre" = xyes; then
		AC_MSG_ERROR([Lustre libraries are not available.])
	fi
	AC_MSG_RESULT([$lustre_found])
])
