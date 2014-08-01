include $(top_srcdir)/common.mk

bin_PROGRAMS = dparallel

noinst_HEADERS = \
    dparallel.h \
    log.h

dparallel_SOURCES = dparallel.c
dparallel_CPPFLAGS = -I../../src/common/ $(MPI_CFLAGS) $(libcircle_CFLAGS)
dparallel_LDFLAGS = $(MPI_CLDFLAGS)
dparallel_LDADD = ../../src/common/libfileutils_common.a $(MPI_CLDFLAGS) $(libcircle_LIBS)
