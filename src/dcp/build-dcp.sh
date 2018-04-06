#!/bin/bash
mpicc -o ../../install/bin/dcp dcp.c ../common/libmfu.a -I../../install/include -L../../install/lib -ldtcmp -llwgrp -larchive -lcircle  -I../common
