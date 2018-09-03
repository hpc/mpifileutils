include $(top_srcdir)/common.mk

bin_PROGRAMS = dbz2 

dbz2_SOURCES = dbz2.c
dbz2_CPPFLAGS = -I../../src/common/ $(MPI_CFLAGS) $(libcircle_CFLAGS) $(libarchive_CFLAGS)
dbz2_LDFLAGS = $(MPI_CLDFLAGS)
dbz2_LDADD = ../../src/common/libmfu.la $(MPI_CLDFLAGS) $(libcircle_LIBS) $(libarchive_LIBS) -lbz2
