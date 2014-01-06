include $(top_srcdir)/common.mk

bin_PROGRAMS = dwalk

dwalk_SOURCES = dwalk.c
dwalk_CPPFLAGS = -I../common/ $(MPI_CFLAGS) $(libcircle_CFLAGS)
dwalk_LDFLAGS = $(MPI_CLDFLAGS)
dwalk_LDADD = ../common/libfileutils_common.a $(MPI_CLDFLAGS) $(libcircle_LIBS)
