include $(top_srcdir)/common.mk

bin_PROGRAMS = dreln

dreln_SOURCES = dreln.c
dreln_CPPFLAGS = -I../common/ $(MPI_CFLAGS) $(libcircle_CFLAGS)
dreln_LDFLAGS = $(MPI_CLDFLAGS)
dreln_LDADD = ../common/libmfu.la $(MPI_CLDFLAGS) $(libcircle_LIBS)
