include $(top_srcdir)/common.mk

bin_PROGRAMS = dgrep

noinst_HEADERS = \
    dgrep.h \
    log.h

dgrep_SOURCES = dgrep.c
dgrep_CPPFLAGS = -I../../src/common/ $(MPI_CFLAGS) $(libcircle_CFLAGS)
dgrep_LDFLAGS = $(MPI_CLDFLAGS)
dgrep_LDADD = ../../src/common/libmfu.la $(MPI_CLDFLAGS) $(libcircle_LIBS)
