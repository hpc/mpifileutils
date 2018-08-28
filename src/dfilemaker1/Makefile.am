include $(top_srcdir)/common.mk

bin_PROGRAMS = dfilemaker1

dfilemaker1_SOURCES = dfilemaker1.c
dfilemaker1_CPPFLAGS = -I../common/ $(MPI_CFLAGS) $(libcircle_CFLAGS)
dfilemaker1_LDFLAGS = $(MPI_CLDFLAGS)
dfilemaker1_LDADD = ../common/libmfu.la $(MPI_CLDFLAGS) $(libcircle_LIBS)
