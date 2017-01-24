include $(top_srcdir)/common.mk

bin_PROGRAMS = dfind

noinst_HEADERS = \
    common.h \
    handle.h \
    pred.h \
    queue.h

dfind_SOURCES = \
    dfind.c \
    handle.c \
    pred.c \
    queue.c

dfind_CPPFLAGS = -I../../src/common/ $(MPI_CFLAGS) $(libcircle_CFLAGS)
dfind_LDFLAGS = $(MPI_CLDFLAGS)
dfind_LDADD = ../../src/common/libmfu.la $(MPI_CLDFLAGS) $(libcircle_LIBS)
