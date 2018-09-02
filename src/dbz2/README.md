# dbz2
# Running dbz2
**Compression**

mpirun -n 2 ./dbz2 -z<options> file_to_compress

**Decompression**

mpirun -n 2 ./dbz2 -d<options> file_to_decompress

# OPTIONS
-d--------decompress

-z--------compress

-k--------keep input file. optional

-f--------overwrite if output file exists.optional.

-b--------block size 1=100kB.......9=900kB. Optional. Default is 9

-m--------optional limit to emory that can be used by a processs

-v-------verbose.optional

-y------debug. optional
