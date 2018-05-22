include $(top_srcdir)/common.mk

bin_PROGRAMS = dbz2 
noinst_HEADERS = dbz2.h

dbz2_SOURCES = dbz2.c common_dbz2.c dbz2_func.c
dbz2_CPPFLAGS = -I../../src/common/ $(MPI_CFLAGS) $(libcircle_CFLAGS) $(libarchive_CFLAGS)
dbz2_LDFLAGS = $(MPI_CLDFLAGS)
dbz2_LDADD = ../../src/common/libmfu.la $(MPI_CLDFLAGS) $(libcircle_LIBS) $(libarchive_LIBS)
