include $(top_srcdir)/common.mk

bin_PROGRAMS = unit_tests

noinst_HEADERS = test_suite1.h
unit_tests_SOURCES = unit_tests.c test_suite1.c
unit_tests_CPPFLAGS = -I../common/ $(MPI_CFLAGS) $(libcircle_CFLAGS) $(cunit_CFLAGS)
unit_tests_LDFLAGS = $(MPI_CLDFLAGS) $(cunit_CLDFLAGS) 
unit_tests_LDADD = ../common/libfileutils_common.a $(MPI_CLDFLAGS) $(libcircle_LIBS) $(cunit_LIBS)
