include $(top_srcdir)/common.mk

bin_PROGRAMS = ddup

ddup_SOURCES = ddup.c
ddup_CPPFLAGS = -I../common/ $(MPI_CFLAGS) $(libcircle_CFLAGS)
ddup_LDFLAGS = $(MPI_CLDFLAGS)
ddup_LDADD = ../common/libfileutils_common.a $(MPI_CLDFLAGS) $(libcircle_LIBS) $(libcrypto_LIBS)
