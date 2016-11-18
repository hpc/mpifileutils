include $(top_srcdir)/common.mk

noinst_LIBRARIES = libfileutils_common.a

noinst_HEADERS = \
    bayer.h \
    bayer_flist.h \
    bayer_flist_internal.h \
    bayer_io.h \
    bayer_param_path.h \
    bayer_path.h \
    bayer_util.h \
    strmap.h \
    list.h

libfileutils_common_a_SOURCES = \
    bayer_flist.c \
    bayer_flist_io.c \
    bayer_flist_remove.c \
    bayer_flist_sort.c \
    bayer_flist_usrgrp.c \
    bayer_flist_walk.c \
    bayer_io.c \
    bayer_param_path.c \
    bayer_path.c \
    bayer_util.c \
    strmap.c

libfileutils_common_a_CPPFLAGS = \
     $(MPI_CFLAGS) \
     $(libcircle_CFLAGS)
