#!/bin/bash

for f in *.c ; do 
	mpicc -o $f.o -c $f -I../../install/include
done
ar cr libmfu.a *.o
