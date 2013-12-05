include $(top_srcdir)/common.mk

bin_PROGRAMS = dparallel

dparallel_SOURCES = dparallel.c

dparallel_LDADD = \
			$(libcircle_LIBS) \
			$(MPI_CLDFLAGS)

dparallel_CPPFLAGS = \
			$(MPI_CFLAGS)       \
			$(libcircle_CFLAGS)
