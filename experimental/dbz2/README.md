# dbz2
TO COMPILE
cd mpifileutils
./buildme dependencies
mkdir build
cd build
cmake ..
make

TO EXECUTE
Compression
mpirun -n 2 ./dbz2 -z<options> file_to_compress
Decompression
mpirun -n 2 ./dbz2 -d<options> file_to_decompress
OPTIONS
-d--------decompress
-z--------compress
-k--------keep input file. optional
-f--------overwrite if output file exists.optional.
-b--------block size 1=100kB.......9=900kB. Optional. Default is 9
-m--------optional limit to emory that can be used by a processs
-v-------verbose.optional
-d------debug. optional
