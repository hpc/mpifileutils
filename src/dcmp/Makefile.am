include $(top_srcdir)/common.mk

bin_PROGRAMS = dcmp

dcmp_SOURCES = dcmp.c

dcmp_LDADD = \
    $(libcircle_LIBS) \
    $(MPI_CLDFLAGS)

dcp_CPPFLAGS = \
    $(MPI_CFLAGS)       \
    $(libcircle_CFLAGS)
