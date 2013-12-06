include $(top_srcdir)/common.mk

bin_PROGRAMS = dcmp

dcmp_SOURCES = dcmp.c
dcmp_CPPFLAGS = -I../common/ $(MPI_CFLAGS) $(libcircle_CFLAGS)
dcmp_LDFLAGS = $(MPI_CLDFLAGS)
dcmp_LDADD = ../common/libfileutils_common.a $(MPI_CLDFLAGS) $(libcircle_LIBS)
