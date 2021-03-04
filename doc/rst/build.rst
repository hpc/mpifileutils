==============================
Build
==============================

mpiFileUtils and its dependencies can be installed with and without Spack.
There are several common variations described here:

- install both mpiFileUtils and its dependencies with Spack
- install both mpiFileUtils and its dependencies directly
- install mpiFileUtis directly after installing its dependencies with Spack

---------------------------
Build everything with Spack
---------------------------

To use `Spack <https://github.com/spack/spack>`_, it is recommended that one first create a `packages.yaml` file to list system-provided packages, like MPI.
Without doing this, Spack will fetch and install an MPI library that may not work on your system.
Make sure that you've set up spack in your shell (see `these instructions <https://spack.readthedocs.io/en/latest/getting_started.html>`_).

Once Spack has been configured, mpiFileUtils can be installed as:

.. code-block:: Bash

    spack install mpifileutils

or to enable all features:

.. code-block:: Bash

    spack install mpifileutils +lustre +gpfs +experimental

-------------------------
Build everything directly
-------------------------

To build directly, mpiFileUtils requires CMake 3.1 or higher.
First ensure MPI wrapper scripts like mpicc are loaded in your environment.
Then to install the dependencies, run the following commands:

.. code-block:: Bash

   #!/bin/bash
   mkdir install
   installdir=`pwd`/install

   mkdir deps
   cd deps
     wget https://github.com/hpc/libcircle/releases/download/v0.3/libcircle-0.3.0.tar.gz
     wget https://github.com/llnl/lwgrp/releases/download/v1.0.3/lwgrp-1.0.3.tar.gz
     wget https://github.com/llnl/dtcmp/releases/download/v1.1.1/dtcmp-1.1.1.tar.gz
     wget https://github.com/libarchive/libarchive/releases/download/3.5.1/libarchive-3.5.1.tar.gz
     
     tar -zxf libcircle-0.3.0.tar.gz
     cd libcircle-0.3.0
       ./configure --prefix=$installdir
       make install
     cd ..
     
     tar -zxf lwgrp-1.0.3.tar.gz
     cd lwgrp-1.0.3
       ./configure --prefix=$installdir
       make install
     cd ..
     
     tar -zxf dtcmp-1.1.1.tar.gz
     cd dtcmp-1.1.1
       ./configure --prefix=$installdir --with-lwgrp=$installdir
       make install
     cd ..

     tar -zxf libarchive-3.5.1.tar.gz
     cd libarchive-3.5.1
       ./configure --prefix=$installdir
       make install
     cd ..
   cd ..

To build on PowerPC, one may need to add :code:`--build=powerpc64le-redhat-linux-gnu`
to the configure commands.

Assuming the dependencies have been placed in
an `install` directory as shown above, build mpiFileUtils from a release like v0.10:

.. code-block:: Bash

   wget https://github.com/hpc/mpifileutils/archive/v0.10.tar.gz
   tar -zxf v0.10.tar.gz
   mkdir build install
   cd build
   cmake ../mpifileutils-0.10 \
     -DWITH_DTCMP_PREFIX=../install \
     -DWITH_LibCircle_PREFIX=../install \
     -DCMAKE_INSTALL_PREFIX=../install
   make install

or to build the latest mpiFileUtils from the master branch:

.. code-block:: Bash

   git clone --depth 1 https://github.com/hpc/mpifileutils
   mkdir build install
   cd build
   cmake ../mpifileutils \
     -DWITH_DTCMP_PREFIX=../install \
     -DWITH_LibCircle_PREFIX=../install \
     -DCMAKE_INSTALL_PREFIX=../install
   make install

To enable Lustre, GPFS, and experimental tools, add the following flags during CMake:

.. code-block:: Bash

    -DENABLE_LUSTRE=ON
    -DENABLE_GPFS=ON
    -DENABLE_EXPERIMENTAL=ON

To disable linking against libarchive, and tools requiring libarchive, add the following flag during CMake:

.. code-block:: Bash

    -DENABLE_LIBARCHIVE=OFF

-------------------------------------------
Build everything directly with DAOS support
-------------------------------------------

To build with DAOS support, first install the dependenies as mentioned above,
and make sure DAOS is installed. If CART and DAOS are installed under a standard
system path then specifying the CART and DAOS paths is unnecessary.

.. code-block:: Bash

   git clone --depth 1 https://github.com/hpc/mpifileutils
   mkdir build install
   cd build
   cmake ../mpifileutils \
     -DWITH_DTCMP_PREFIX=../install \
     -DWITH_LibCircle_PREFIX=../install \
     -DCMAKE_INSTALL_PREFIX=../install \
     -DWITH_CART_PREFIX=</path/to/daos/> \
     -DWITH_DAOS_PREFIX=</path/to/daos/> \
     -DENABLE_DAOS=ON
   make install


To build with HDF5 support, add the following flags during CMake.
To use the `daos-serialize` and `daos-deserialize` tools, HDF5 1.2+ is required.
To copy HDF5 containers with `dcp`, HDF5 1.8+ is required, along with the daos-vol.
If HDF5 is installed under a standard system path then specifying the HDF5 path is unnecessary.

.. code-block:: Bash

   -DENABLE_HDF5=ON \
   -DWITH_HDF5_PREFIX=</path/to/hdf5>

--------------------------------------------------------------
Build mpiFileUtils directly, build its dependencies with Spack
--------------------------------------------------------------

One can use Spack to install mpiFileUtils dependencies using the `spack.yaml` file distributed with mpiFileUtils.
From the root directory of mpiFileUtils, run the command `spack find` to determine which packages spack will install.
Next, run `spack concretize` to have spack perform dependency analysis.
Finally, run `spack install` to build the dependencies.

There are two ways to tell CMake about the dependencies.
First, you can use `spack load [depname]` to put the installed dependency into your environment paths.
Then, at configure time, CMake will automatically detect the location of these dependencies.
Thus, the commands to build become:

.. code-block:: Bash

   git clone --depth 1 https://github.com/hpc/mpifileutils
   mkdir build install
   cd mpifileutils
   spack install
   spack load dtcmp
   spack load libcircle
   spack load libarchive
   cd ../build
   cmake ../mpifileutils

The other way to use spack is to create a "view" to the installed dependencies.
Details on this are coming soon.
