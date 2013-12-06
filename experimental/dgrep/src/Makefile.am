include $(top_srcdir)/common.mk

bin_PROGRAMS = dgrep
dgrep_SOURCES = dgrep.c
dgrep_LDADD = \
    $(libcircle_LIBS) \
    $(MPI_CLDFLAGS)

dgrep_CPPFLAGS = \
    $(MPI_CFLAGS)       \
    $(libcircle_CFLAGS)
