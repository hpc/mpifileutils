include $(top_srcdir)/common.mk

noinst_LIBRARIES = libfileutils_common.a

libfileutils_common_a_SOURCES = \
                                bayer_flist.c \
                                bayer_flist_remove.c \
                                bayer_io.c \
                                bayer_param_path.c \
                                bayer_path.c \
                                bayer_util.c \
                                strmap.c

libfileutils_common_a_CPPFLAGS = \
                                 $(MPI_CFLAGS) \
                                 $(libcircle_CFLAGS)
