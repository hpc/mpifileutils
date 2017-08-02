include $(top_srcdir)/common.mk

bin_PROGRAMS =dbz2 
noinst_HEADERS = \
    dbz2.h

dtar_SOURCES = dbz2.c common_dbz2.c 
dtar_CPPFLAGS = -I../../src/common/ \
                                $(MPI_CFLAGS) $(libcircle_CFLAGS) $(libarchive_CFLAGS)
dtar_LDFLAGS = $(MPI_CLDFLAGS)
dtar_LDADD = ../../src/common/libmfu.la \
                         $(MPI_CLDFLAGS) $(libcircle_LIBS) $(libarchive_LIBS)
