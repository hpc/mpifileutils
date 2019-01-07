==============================
Build
==============================

mpiFileUtils depends on several libraries. mpiFileUtils is available in
`Spack <https://github.com/spack/spack>`_, which simplifies the install to just:

.. code-block:: Bash

    $ spack install mpifileutils

or to enable all features:

.. code-block:: Bash

    $ spack install mpifileutils +lustre +experimental

To build from a release tarball, use CMake. Note that this requires the manual
installation of the dependencies. One can also use spack to create an environment.


To build from a release tarball, there are two scripts: buildme_dependencies and
buildme. The buildme_dependencies script downloads and installs all the
necessary libraries. The buildme script then builds mpiFileUtils assuming the
libraries have been installed. Both scripts require that mpicc is in your path,
and that it is for an MPI library that supports at least v2.2 of the MPI
standard. Please review each buildme script, and edit if necessary. Then run
them in sequence:

.. code-block:: Bash

    $ ./buildme_dependencies
    $ ./buildme

To build from a clone, it may also be necessary to first run the
buildme_autotools script to obtain the required set of autotools, then use
buildme_dependencies_dev and buildme_dev:

.. code-block:: Bash

    $ ./buildme_autotools
    $ ./buildme_dependencies_dev
    $ ./buildme_dev
