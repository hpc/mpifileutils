include $(top_srcdir)/common.mk

bin_PROGRAMS = dchmod

dchmod_SOURCES = dchmod.c
dchmod_CPPFLAGS = -I../common/ $(MPI_CFLAGS) $(libcircle_CFLAGS)
dchmod_LDFLAGS = $(MPI_CLDFLAGS)
dchmod_LDADD = ../common/libmfu.la $(MPI_CLDFLAGS) $(libcircle_LIBS)
