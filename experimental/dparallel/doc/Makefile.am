SUBDIRS = man

if HAVE_DOXYGEN
all-local: Doxyfile
	$(DOXYGEN) Doxyfile

clean-local:
	rm -rf $(top_srcdir)/doc/html
	rm -rf $(top_srcdir)/doc/latex
	rm -rf $(top_srcdir)/doc/Doxyfile
endif
