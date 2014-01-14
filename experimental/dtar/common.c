/*
 * common.c
 *
 *  Created on: Jan 13, 2014
 *      Author: fwang2
 */

#include "common.h"

void DTAR_writer_init() {
    char * filename = DTAR_user_opts.dest_path;
    DTAR_writer.name = filename;
    DTAR_writer.flags = O_WRONLY | O_CREAT | O_CLOEXEC | O_LARGEFILE;
    DTAR_writer.fd_tar = open(filename, DTAR_writer.flags, 0664);

}

void DTAR_abort(int code) {
    MPI_Abort(MPI_COMM_WORLD, code);
    exit(code);
}

void DTAR_exit(int code) {
    bayer_finalize();
    MPI_Finalize();
    exit(code);
}
