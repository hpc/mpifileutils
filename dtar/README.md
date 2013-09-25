
# How to build 

## step 1

- Make sure you have cmake (2.8+) installed and on the path
- Make sure you have mpicc installed and on the path

## step 2

    ./buildme_dependencies
    source env.sh

## step 3

    mkdir build; cd build; cmake ..
    make


You can script this if you want, except the `source env.sh` 
must be run through your shell for the environment setup to be correct

