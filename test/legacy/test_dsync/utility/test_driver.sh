#!/usr/bin/bash

. set_funcs.sh

#echo test union with wrong arg count
#union one || echo expected failure for wrong arg count
#
#echo test union with non-files
#union notafile1 notafile2 || echo expected failure for non-files


echo union: $(union one two)
echo intersection: $(intersection one two)
echo sets_equal 1 and 2: $(sets_equal one two; echo $?)
echo sets_equal 1 and 1: $(sets_equal one one; echo $?)
