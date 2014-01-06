include $(top_srcdir)/common.mk

bin_PROGRAMS = dgrep

dgrep_SOURCES = dgrep.c
dgrep_CPPFLAGS = -I../../src/common/ $(MPI_CFLAGS) $(libcircle_CFLAGS)
dgrep_LDFLAGS = $(MPI_CLDFLAGS)
dgrep_LDADD = ../../src/common/libfileutils_common.a $(MPI_CLDFLAGS) $(libcircle_LIBS)
