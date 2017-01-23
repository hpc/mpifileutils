#include <CUnit/CUnit.h>AUTOMAKE_OPTIONS = foreign
ACLOCAL_AMFLAGS = -I m4
AM_LDFLAGS = -lcunit
#SUBDIRS = src doc
SUBDIRS = src

if EXPERIMENTAL_ENABLED
  SUBDIRS += experimental
endif

if HAVE_NOSETESTS
test:
	-(pushd $(top_srcdir)/test>/dev/null && ~/fileutils.git/venv/bin/nose2 -v && popd>/dev/null)
else
test:
	@(echo "The nosetests command could not be found. Please install Nose2 with your")
	@(echo "package manager or 'pip install nose2', then rerun the configure script.")
	@false;
endif

MAINTAINERCLEANFILES = \
	aclocal.m4 config.guess config.xml              \
	config.h.in config.sub configure install-sh     \
	ltconfig ltmain.sh missing mkinstalldirs        \
	stamp-h.in

distclean-local:
	-(cd $(top_srcdir) && rm -rf autom4te*.cache autoscan.*)
	-(cd $(top_srcdir) && rm -rf $(PACKAGE)-*)

mrproper: distclean-local clean
	-(cd $(top_srcdir) && rm -rf autom4te.cache config.h config.log)
	-(cd $(top_srcdir) && rm -rf config.status libtool stamp-h1)
	-(cd $(top_srcdir) && rm -rf auxdir)
	-(cd $(top_srcdir) && rm -f configure configure.ac~)
	-(cd $(top_srcdir) && rm -f config.h.in config.h.in~)
	-(cd $(top_srcdir)/m4 && rm -f libtool.m4 ltoptions.m4)
	-(cd $(top_srcdir)/m4 && rm -f ltsugar.m4 ltversion.m4)
	-(cd $(top_srcdir)/m4 && rm -f lt~obsolete.m4)
	-find $(top_srcdir)/ -name "Makefile" -exec rm {} \;
	-find $(top_srcdir)/ -name "Makefile.in" -exec rm {} \;
	-find $(top_srcdir)/ -name ".dirstamp" -exec rm {} \;
	-find $(top_srcdir) -depth -name ".deps" -exec rm -rf {} \;

.PHONY: test
