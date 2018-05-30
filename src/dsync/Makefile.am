include $(top_srcdir)/common.mk

bin_PROGRAMS = dsync

dsync_SOURCES = dsync.c
dsync_CPPFLAGS = -I../common/ $(MPI_CFLAGS) $(libcircle_CFLAGS)
dsync_LDFLAGS = $(MPI_CLDFLAGS)
dsync_LDADD = ../common/libmfu.la $(MPI_CLDFLAGS) $(libcircle_LIBS)
