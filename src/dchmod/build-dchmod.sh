#!/bin/bash
mpicc -std=gnu99 -o ../../install/bin/dchmod dchmod.c ../common/libmfu.a -I../../install/include -L../../install/lib -ldtcmp -llwgrp -larchive -lcircle  -I../common
