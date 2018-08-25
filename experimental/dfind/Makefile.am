include $(top_srcdir)/common.mk

bin_PROGRAMS = dfind

noinst_HEADERS = \
    common.h \
    pred.h

dfind_SOURCES = \
    dfind.c \
    pred.c

dfind_CPPFLAGS = -I../../src/common/ $(MPI_CFLAGS) $(libcircle_CFLAGS)
dfind_LDFLAGS = $(MPI_CLDFLAGS)
dfind_LDADD = ../../src/common/libmfu.la $(MPI_CLDFLAGS) $(libcircle_LIBS)
