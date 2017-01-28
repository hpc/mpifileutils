include $(top_srcdir)/common.mk

bin_PROGRAMS = dbcast

dbcast_SOURCES = dbcast.c
dbcast_CPPFLAGS = -I../common/ $(MPI_CFLAGS) $(libcircle_CFLAGS)
dbcast_LDFLAGS = $(MPI_CLDFLAGS)
dbcast_LDADD = ../common/libmfu.la $(MPI_CLDFLAGS) $(libcircle_LIBS)
