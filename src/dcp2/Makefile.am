include $(top_srcdir)/common.mk

bin_PROGRAMS = dcp2

noinst_HEADERS = \
    common.h \
    dcp.h \
    handle_args.h

dcp2_SOURCES = \
    common.c \
    dcp2.c \
    handle_args.c

dcp2_CPPFLAGS = -I../common/ $(MPI_CFLAGS) $(libcircle_CFLAGS)
dcp2_LDFLAGS = $(MPI_CLDFLAGS)
dcp2_LDADD = ../common/libfileutils_common.a $(MPI_CLDFLAGS) $(libcircle_LIBS)
