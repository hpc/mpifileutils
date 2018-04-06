#!/bin/bash
mpicc -o ../../install/bin/dbcast dbcast.c ../common/libmfu.a -I../../install/include -L../../install/lib -ldtcmp -llwgrp -larchive -lcircle  -I../common
