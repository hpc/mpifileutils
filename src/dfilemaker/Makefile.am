include $(top_srcdir)/common.mk

bin_PROGRAMS = dfilemaker

dfilemaker_SOURCES = dfilemaker.c
dfilemaker_CPPFLAGS = -I../common/ $(MPI_CFLAGS) $(libcircle_CFLAGS)
dfilemaker_LDFLAGS = $(MPI_CLDFLAGS)
dfilemaker_LDADD = ../common/libmfu.la $(MPI_CLDFLAGS) $(libcircle_LIBS)
