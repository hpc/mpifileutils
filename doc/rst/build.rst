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
installation of the dependencies. Assuming the dependencies have been placed in
an `install` directory the build commands are thus:

.. code-block:: Bash

   $ git clone https://github.com/hpc/mpifileutils
   $ mkdir build install
   $ # build DTCMP and other dependencies
   $ cd build
   $ cmake ../mpifileutils -DWITH_DTCMP_PREFIX=../install -DWITH_LibCircle_PREFIX=../install -DCMAKE_INSTALL_PREFIX=../install

One can also use spack to create an environment.
