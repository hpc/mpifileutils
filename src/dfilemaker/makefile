#***************************************************************
#
#    This makefile is just for dfilemaker.
#     Please do not remove.  For testing only.
#
#***************************************************************

BASEDIR = /usr/tce/packages/mpifileutils/mpifileutils-0.9
INCLOC = ${BASEDIR}/install/include
LIBLOC = ${BASEDIR}/install/lib
MPCC = mpicc
CC = gcc

dfilemaker: dfilemaker.o
	${MPCC} -o dfilemaker dfilemaker.o -L ${LIBLOC} -ldtcmp -lcircle -larchive -L${BASEDIR}/lib64 -lmfu -Wl,-rpath=${BASEDIR}/install/lib -Wl,-rpath=${BASEDIR}/lib
dfilemaker.o: dfilemaker.c
	${MPCC} -c -DHAVE_BYTESWAP_H dfilemaker.c -I../common -I ${INCLOC} -I/usr/include
clean: dfilemaker
	rm dfilemaker

goexam: goexam.c
	$(CC) -g -o goexam goexam.c
golly: golly.c
	$(CC) -g -o golly golly.c
atest: atest.c
	$(CC) -g -o atest atest.c
btest: btest.c
	$(CC) -g -o btest btest.c
wf: wf.o
	${MPCC} -o wf wf.o -L ${LIBLOC} -L${BASEDIR}/lib64 -lmfu -Wl,-rpath=${BASEDIR}/install/lib -Wl,-rpath=${BASEDIR}/lib
wf.o: wf.c
	$(MPCC) -DHAVE_BYTESWAP_H -I../common -I ${INCLOC} -I/usr/include -g -c  wf.c

