mpicxx -o dtar   \
        dtar.c  copy.c   helper.c  treewalk.c      \
       -L/ccs/home/yj7/local/libcircle/lib       \
       -L/ccs/home/yj7/local/libarchive/lib      \
       -I/ccs/home/yj7/local/libcircle/include   \
       -I/ccs/home/yj7/local/libarchive/include  \
	   -I/usr/include  \
	   -I/ccs/home/yj7/local/attr-2.4.47/include  \
       -lcircle  -larchive           \
       -D__STDC_FORMAT_MACROS


rm *.o	   
