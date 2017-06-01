include $(top_srcdir)/common.mk

bin_PROGRAMS = dsh

dsh_SOURCES = dsh.c
dsh_CPPFLAGS = -I../../src/common/ $(MPI_CFLAGS) $(libcircle_CFLAGS)
dsh_LDFLAGS = $(MPI_CLDFLAGS)
dsh_LDADD = ../../src/common/libmfu.la $(MPI_CLDFLAGS) $(libcircle_LIBS)
