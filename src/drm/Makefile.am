include $(top_srcdir)/common.mk

bin_PROGRAMS = drm

drm_SOURCES = drm.c
drm_CPPFLAGS = -I../common/ $(MPI_CFLAGS) $(libcircle_CFLAGS)
drm_LDFLAGS = $(MPI_CLDFLAGS)
drm_LDADD = ../common/libfileutils_common.a $(MPI_CLDFLAGS) $(libcircle_LIBS)
