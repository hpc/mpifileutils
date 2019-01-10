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

One can also use spack to create an environment and view with the provided `spack.yaml` file.
First, make sure that you've set up spack in your shell (see `these instructions <https://spack.readthedocs.io/en/latest/getting_started.html>`_).
Next, be sure that your `~/.spack/packages.yaml` is configured to ensure that spack can detect system-provided packages.

From the root directory of mpifileutils, run the command `spack find` to determine which packages spack will install.
Next, run `spack concretize` to build have spack perform dependency analysis.
Finally, run `spack install` to build the dependencies.

There are two ways to tell CMake about the dependencies.
First, you can use `spack load [depname]` to put the installed dependency into your environment paths.
Then, at configure time, CMake will automatically detect the location of these dependencies.
Thus, the commands to build become:

.. code-block:: Bash

   $ git clone https://github.com/hpc/mpifileutils
   $ mkdir build install
   $ cd mpifileutils
   $ spack install
   $ spack load dtcmp
   $ spack load libcircle
   $ spack load libarchive
   $ cd ../build
   $ cmake ../mpifileutils

The other way to use spack is to create a "view" to the installed dependencies.
Details on this are coming soon.
