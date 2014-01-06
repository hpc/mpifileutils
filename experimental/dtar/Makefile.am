include $(top_srcdir)/common.mk

bin_PROGRAMS = dtar

dtar_SOURCES = copy.c dtar.c helper.c
dtar_CPPFLAGS = -I../../src/common/ \
				$(MPI_CFLAGS) $(libcircle_CFLAGS) $(libarchive_CFLAGS)
dtar_LDFLAGS = $(MPI_CLDFLAGS)
dtar_LDADD = ../../src/common/libfileutils_common.a \
			 $(MPI_CLDFLAGS) $(libcircle_LIBS) $(libarchive_LIBS)
