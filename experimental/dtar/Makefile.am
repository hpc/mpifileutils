include $(top_srcdir)/common.mk

bin_PROGRAMS = dtar

noinst_HEADERS = \
    common.h

dtar_SOURCES = dtar.c common.c handle_args.c
dtar_CPPFLAGS = -I../../src/common/ \
				$(MPI_CFLAGS) $(libcircle_CFLAGS) $(libarchive_CFLAGS)
dtar_LDFLAGS = $(MPI_CLDFLAGS)
dtar_LDADD = ../../src/common/libmfu.la \
			 $(MPI_CLDFLAGS) $(libcircle_LIBS) $(libarchive_LIBS)
