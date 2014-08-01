include $(top_srcdir)/common.mk

bin_PROGRAMS = dcp

noinst_HEADERS = \
    cleanup.h \
    common.h \
    compare.h \
    copy.h \
    dcp.h \
    handle_args.h \
    treewalk.h

dcp_SOURCES = \
    cleanup.c \
    common.c \
    compare.c \
    copy.c \
    dcp.c \
    handle_args.c \
    treewalk.c

dcp_CPPFLAGS = -I../common/ $(MPI_CFLAGS) $(libcircle_CFLAGS)
dcp_LDFLAGS = $(MPI_CLDFLAGS)
dcp_LDADD = ../common/libfileutils_common.a $(MPI_CLDFLAGS) $(libcircle_LIBS)
