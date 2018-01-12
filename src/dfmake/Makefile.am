include $(top_srcdir)/common.mk

bin_PROGRAMS = dfmake

dfmake_SOURCES = dfmake.c
dfmake_CPPFLAGS = -I../common/ $(MPI_CFLAGS) $(libcircle_CFLAGS)
dfmake_LDFLAGS = $(MPI_CLDFLAGS)
dfmake_LDADD = ../common/libmfu.la $(MPI_CLDFLAGS) $(libcircle_LIBS)
