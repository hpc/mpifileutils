#***************************************************************
#
#    This makefile is just for dfilemaker.
#     Please do not remove.  For testing only.
#
#***************************************************************

BASEDIR = /usr/tce/packages/mpifileutils/mpifileutils-0.8
INCLOC = ${BASEDIR}/install/include
LIBLOC = ${BASEDIR}/install/lib
MPCC = mpicc

dfilemaker: dfilemaker.o
	${MPCC} -o dfilemaker dfilemaker.o -L ${LIBLOC} -ldtcmp -lcircle -larchive -L${BASEDIR}/lib -lmfu -Wl,-rpath=${BASEDIR}/install/lib -Wl,-rpath=${BASEDIR}/lib
dfilemaker.o: dfilemaker.c
	${MPCC} -c -DHAVE_BYTESWAP_H dfilemaker.c -I../common -I ${INCLOC} -I/usr/include
clean: dfilemaker
	rm dfilemaker
