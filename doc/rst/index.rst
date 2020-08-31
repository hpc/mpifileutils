.. mpiFileUtils documentation master file, created by
   sphinx-quickstart on Fri Apr 13 11:47:51 2018.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

===================================
**Documentation for mpiFileUtils**
===================================

Overview
*************

mpiFileUtils provides both a library called libmfu and a suite of MPI-based
tools to manage large datasets, which may vary from large directory trees to
large files. High-performance computing users often generate large datasets with
parallel applications that run with many processes (millions in some cases).
However those users are then stuck with single-process tools like cp and rm to
manage their datasets. This suite provides MPI-based tools to handle typical
jobs like copy, remove, and compare for such datasets, providing speedups of up
to 50x. The libmfu library simplifies the creation of new tools
and it can be called directly from within HPC applications.

Video Overview: `"Scalable Management of HPC Datasets with mpiFileUtils" <https://youtu.be/cxjPOUS-ZBY>`_, HPCKP'20.

User Guide
***************************

.. toctree::
   :maxdepth: 3

   proj-design.rst
   tools.rst
   libmfu.rst
   build.rst

Man Pages
***************************

.. toctree::
   :maxdepth: 1

   dbcast.1
   dbz2.1
   dchmod.1
   dcmp.1
   dcp.1
   ddup.1
   dfind.1
   dreln.1
   drm.1
   dstripe.1
   dsync.1
   dwalk.1
   dgrep.1
   dparallel.1
   dtar.1

Indices and tables
*********************

* :ref:`genindex`
* :ref:`search`
