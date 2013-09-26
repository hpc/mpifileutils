
# Build Instructions

## Prerequisites

- Make sure you have cmake (2.8+) installed and on the path
- Make sure you have mpicc installed and on the path, openmpi tested

## Build dependencies

    ./buildme_dependencies
    source env.sh

## Build dtar

    mkdir build; cd build; cmake ..
    make

You can script this if you want, except the `source env.sh` 
must be run through your shell for the environment setup to be correct

