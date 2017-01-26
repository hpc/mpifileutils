include $(top_srcdir)/common.mk

bin_PROGRAMS = dstripe

dstripe_SOURCES = dstripe.c
dstripe_CPPFLAGS = -I../common/ $(MPI_CFLAGS) $(libcircle_CFLAGS)
dstripe_LDFLAGS = $(MPI_CLDFLAGS)
dstripe_LDADD = ../common/libmfu.la $(MPI_CLDFLAGS) $(libcircle_LIBS) $(LUSTRE_LIBS)
