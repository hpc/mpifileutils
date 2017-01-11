include $(top_srcdir)/common.mk

bin_PROGRAMS = dcp1

noinst_HEADERS = \
    cleanup.h \
    common.h \
    compare.h \
    copy.h \
    dcp1.h \
    handle_args.h \
    treewalk.h

dcp1_SOURCES = \
    cleanup.c \
    common.c \
    compare.c \
    copy.c \
    dcp1.c \
    handle_args.c \
    treewalk.c

dcp1_CPPFLAGS = -I../common/ $(MPI_CFLAGS) $(libcircle_CFLAGS)
dcp1_LDFLAGS = $(MPI_CLDFLAGS)
dcp1_LDADD = ../common/libfileutils_common.a $(MPI_CLDFLAGS) $(libcircle_LIBS)
