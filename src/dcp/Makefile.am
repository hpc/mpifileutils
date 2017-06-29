include $(top_srcdir)/common.mk

bin_PROGRAMS = dcp

dcp_SOURCES = \
    dcp.c 

dcp_CPPFLAGS = -I../common/ $(MPI_CFLAGS) $(libcircle_CFLAGS)
dcp_LDFLAGS = $(MPI_CLDFLAGS)
dcp_LDADD = ../common/libmfu.la $(MPI_CLDFLAGS) $(libcircle_LIBS)
