include $(top_srcdir)/common.mk

noinst_LIBRARIES = libfileutils_common.a

noinst_HEADERS = \
    mfu.h \
    mfu_flist.h \
    mfu_flist_internal.h \
    mfu_io.h \
    mfu_param_path.h \
    mfu_path.h \
    mfu_util.h \
    strmap.h \
    list.h

libfileutils_common_a_SOURCES = \
    mfu_flist.c \
    mfu_flist_chunk.c \
    mfu_flist_io.c \
    mfu_flist_remove.c \
    mfu_flist_sort.c \
    mfu_flist_usrgrp.c \
    mfu_flist_walk.c \
    mfu_io.c \
    mfu_param_path.c \
    mfu_path.c \
    mfu_util.c \
    strmap.c

libfileutils_common_a_CPPFLAGS = \
     $(MPI_CFLAGS) \
     $(libcircle_CFLAGS)
