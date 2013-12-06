include $(top_srcdir)/common.mk

bin_PROGRAMS = dcp

dcp_SOURCES = cleanup.c common.c compare.c copy.c dcp.c handle_args.c treewalk.c
dcp_CPPFLAGS = -I../common/ $(MPI_CFLAGS) $(libcircle_CFLAGS)
dcp_LDFLAGS = $(MPI_CLDFLAGS)
dcp_LDADD = ../common/libfileutils_common.a $(MPI_CLDFLAGS) $(libcircle_LIBS)
