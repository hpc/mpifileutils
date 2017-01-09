include $(top_srcdir)/common.mk

bin_PROGRAMS = unit_tests

unit_tests_SOURCES = test_suite1.c unit_tests.c
unit_tests_CPPFLAGS = -I../common/ $(MPI_CFLAGS) $(libcircle_CFLAGS)
unit_tests_LDFLAGS = $(MPI_CLDFLAGS) -lcunit 
unit_tests_LDADD = ../common/libfileutils_common.a $(MPI_CLDFLAGS) $(libcircle_LIBS)
