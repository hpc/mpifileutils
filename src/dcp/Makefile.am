include $(top_srcdir)/common.mk

bin_PROGRAMS = dcp

noinst_HEADERS = \
    common.h \
    dcp.h \
    handle_args.h

dcp_SOURCES = \
    common.c \
    dcp.c \
    handle_args.c

dcp_CPPFLAGS = -I../common/ $(MPI_CFLAGS) $(libcircle_CFLAGS)
dcp_LDFLAGS = $(MPI_CLDFLAGS)
dcp_LDADD = ../common/libmfu.la $(MPI_CLDFLAGS) $(libcircle_LIBS)
