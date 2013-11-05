#include "dtar.h"

#include <archive.h>
#include <archive_entry.h>

#include <dirent.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <inttypes.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "bayer.h"
#include "libcircle.h"

MPI_Comm new_comm;
MPI_Comm inter_comm;

int64_t g_tar_offset = 0;
int verbose = 0;

DTAR_options_t DTAR_user_opts;
DTAR_writer_t DTAR_writer;
int xattr_flag = 0;

void (*DTAR_jump_table[3])(DTAR_operation_t* op, CIRCLE_handle* handle);

static void create(char *filename, char compress, int opt_index, int argc,
        char **argv);
static void extract(const char *filename, int do_extract, int flags);
static int copy_data(struct archive *, struct archive *);

static void errmsg(const char* m) {
    fprintf(stderr, "%s\n", m);
}

static void msg(const char *m) {
    fprintf(stdout, m);
}

void usage(void) {
    const char *m = "\nUsage: dtar [-"
            "c"
            "x"
            "] [-f file] [file]\n\n";
    fprintf(stderr, m);
    exit(1);
}

int main(int argc, char **argv) {
    char *filename = NULL;
    char compress, mode, opt;
    int flags;
    int opt_index = 0;

    MPI_Init(&argc, &argv);
    bayer_init();

    mode = 'x';
    verbose = 0;
    compress = '\0';
    flags = ARCHIVE_EXTRACT_TIME;

    if (argc == 1) {
        usage();
    }

    /* Among other sins, getopt(3) pulls in printf(3). */
    while (*++argv != NULL && **argv == '-') {
        char *p = *argv + 1;
        opt_index++;

        while ((opt = *p++) != '\0') {
            switch (opt) {
            case 'c':
                mode = opt;
                break;
            case 'e':
                xattr_flag = 1;
                flags |= ARCHIVE_EXTRACT_XATTR;
                break;
            case 'f':
                if (*p != '\0')
                    filename = p;
                else
                    filename = *++argv;
                p += strlen(p);
                break;
            case 'j':
                compress = opt;
                break;
            case 'p':
                flags |= ARCHIVE_EXTRACT_OWNER;
                flags |= ARCHIVE_EXTRACT_PERM;
                flags |= ARCHIVE_EXTRACT_ACL;
                flags |= ARCHIVE_EXTRACT_FFLAGS;
                break;
            case 't':
                mode = opt;
                break;
            case 'v':
                verbose++;
                break;
            case 'x':
                mode = opt;
                break;
            case 'y':
                compress = opt;
                break;
            case 'Z':
                compress = opt;
                break;
            case 'z':
                compress = opt;
                break;
            default:
                usage();
            }
        }
    }

    switch (mode) {
    case 'c':
        create(filename, compress, opt_index, argc, argv);
        break;
    case 't':
        extract(filename, 0, flags);
        break;
    case 'x':
        extract(filename, 1, flags);
        break;
    }

    bayer_finalize();
    MPI_Finalize();
    return (0);
}

int HDR_LENGTH = 1536;

inline static struct archive* new_archive()
{
    /* create a new archive data structure */
    struct archive *a = archive_write_new();

    /* set compression format */
    char compress = DTAR_user_opts.compress;
    switch (compress) {
    case 'j':
    case 'y':
        archive_write_add_filter_bzip2(a);
        break;
    case 'Z':
        archive_write_add_filter_compress(a);
        break;
    case 'z':
        archive_write_add_filter_gzip(a);
        break;
    default:
        archive_write_add_filter_none(a);
        break;
    }

    /* select archive output type */
    int r = archive_write_set_format_pax(a);

    if ( r != ARCHIVE_OK ) {
        BAYER_LOG(BAYER_LOG_ERR, archive_error_string(a));
        return NULL;
    }

    archive_write_set_bytes_per_block(a, 0);

    return a;
}

static int write_header(struct archive* a, bayer_flist flist, uint64_t index, uint64_t offset)
{
    /* create a new archive entry structure */
    struct archive_entry *entry = archive_entry_new();

    /* get file type */
    bayer_filetype type = bayer_flist_file_get_type(flist, index);

    /* get file name */
    const char* name = bayer_flist_file_get_name(flist, index);
printf("Writing entry at %llu for %s\n", (unsigned long long)offset, name);

    /* TODO: get name relative to source directory */
    //bayer_path* path = bayer_path_from_str(name);

    /* copy path name */
    archive_entry_set_pathname(entry, name);

#if 0
    /* since we already have stat info in bayer_flist,
     * use flist to avoid this extra stat call */
    struct stat stbuf;
    bayer_lstat(name, &stbuf);
    archive_entry_copy_stat(entry, &stbuf);
#endif

    /* set file type */
    if (type == BAYER_TYPE_FILE) {
        archive_entry_set_filetype(entry, AE_IFREG);
    } else if (type == BAYER_TYPE_DIR) {
        archive_entry_set_filetype(entry, AE_IFDIR);
    } else if (type == BAYER_TYPE_LINK) {
        archive_entry_set_filetype(entry, AE_IFLNK);
    }

    /* if object is a link, copy the link target */
    if (type == BAYER_TYPE_LINK) {
        char target[PATH_MAX];
        bayer_readlink(name, target, sizeof(target));
        archive_entry_set_symlink(entry, target);
    }
        
    /* if file, set file size */
    if (type == BAYER_TYPE_FILE) {
        uint64_t bsize = bayer_flist_file_get_size(flist, index);
        archive_entry_set_size(entry, (int64_t)bsize);
    }

    /* set uid */
    uint32_t buid = bayer_flist_file_get_uid(flist, index);
    uid_t uid = (uid_t) buid;
    archive_entry_set_uid(entry, uid);

    /* set username */
    const char* uname = bayer_flist_file_get_username(flist, index);
    archive_entry_set_uname(entry, uname);

    /* set gid */
    uint32_t bgid = bayer_flist_file_get_gid(flist, index);
    gid_t gid = (gid_t) bgid;
    archive_entry_set_gid(entry, gid);

    /* set group name */
    const char* gname = bayer_flist_file_get_groupname(flist, index);
    archive_entry_set_gname(entry, gname);

    /* set permission bits */
    uint32_t bmode = bayer_flist_file_get_mode(flist, index);
    mode_t mode = (mode_t) bmode;
    archive_entry_set_mode(entry, mode);

    /* set atime */
    uint32_t batime = bayer_flist_file_get_atime(flist, index);
    time_t atime = (time_t) batime;
    archive_entry_set_atime(entry, atime, 0);

    /* set mtime */
    uint32_t bmtime = bayer_flist_file_get_mtime(flist, index);
    time_t mtime = (time_t) bmtime;
    archive_entry_set_mtime(entry, mtime, 0);

    /* set ctime */
    uint32_t bctime = bayer_flist_file_get_ctime(flist, index);
    time_t ctime = (time_t) bctime;
    archive_entry_set_ctime(entry, ctime, 0);

    /* TODO: copy xattrs */
#if 0
    fd = open(bname, O_RDONLY); // Error check?
    struct archive* ard = archive_read_disk_new();
    archive_read_disk_set_standard_lookup(ard);
    archive_read_disk_entry_from_file(ard, entry, fd, NULL);
    archive_read_free(ard);
#endif

    /* seek to position to write entry */
    bayer_lseek(DTAR_writer.name, DTAR_writer.fd_tar, offset, SEEK_SET);

    /* write entry to archive file */
    archive_write_header(a, entry);

    /* free entry */
    archive_entry_free(entry);

    return 0;
}

static bayer_flist g_flist;
static uint64_t g_offset;
static uint64_t* g_sizes = NULL;

/* marches through items in g_flist, identifies files, and inserts
 * copy work elements in libcircle */
static void copy_enqueue(CIRCLE_handle* handle)
{
    /* get our offset within the archive file */
    uint64_t offset = g_offset;

    /* add copy work items for each file in our local list */
    uint64_t index;
    uint64_t count = bayer_flist_size(g_flist);
    for (index = 0; index < count; index++) {
        /* get type of item */
        bayer_filetype type = bayer_flist_file_get_type(g_flist, index);

        /* add copy items if it's a file */
        if (type == BAYER_TYPE_FILE) {
            uint64_t dataoffset = offset + HDR_LENGTH;
            uint64_t chunk_index;

            /* get name and file size */
            const char* name = bayer_flist_file_get_name(g_flist, index);
            uint64_t size = bayer_flist_file_get_size(g_flist, index);

            /* compute whole number of chunks */
            uint64_t num_chunks = size / DTAR_CHUNK_SIZE;

            /* create copy work items for all whole chunks */
            for (chunk_index = 0; chunk_index < num_chunks; chunk_index++) {
                char* newop = DTAR_encode_operation(
                    COPY, chunk_index, name, dataoffset, size
                );
                handle->enqueue(newop);
                bayer_free(&newop);
            }

            /* create copy work item for last (possibly partial chunk) */
            if ((num_chunks * DTAR_CHUNK_SIZE) < size || num_chunks == 0) {
                char* newop = DTAR_encode_operation(
                    COPY, chunk_index, name, dataoffset, size
                );
                handle->enqueue(newop);
                bayer_free(&newop);
            }
        }

        /* increase our offset */
        offset += g_sizes[index];
    }

    return;
}

/* process a copy work element */
static void copy_process(CIRCLE_handle* handle)
{
    char opstr[CIRCLE_MAX_STRING_LEN];
    handle->dequeue(opstr);
  
    DTAR_operation_t* op = DTAR_decode_operation(opstr);
    DTAR_do_copy(op);
    bayer_free(&op);

    return;
}

static void create(
    char *filename,
    char compress,
    int opt_index,
    int argc,
    char **argv)
{
    /* TODO: pass in file list as input param */

    /* get our rank and number of ranks in job */
    int rank, ranks;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &ranks);

    /* parse file path */
    DTAR_parse_path_args(filename, compress, argv);

    /* open archive file for writing */
    DTAR_writer_init();

    /* TODO: handle input paths consistently among tools */
    /* TODO: support multiple source paths here */

    /* recursively walk path to get stat info on all files */
    bayer_flist flist = bayer_flist_new();
    bayer_flist_walk_path(DTAR_user_opts.src_path[0], 1, flist);

    /* TODO: better to order items by directory or depth for extraction? */

    /* get number of items in our local list */
    uint64_t count = bayer_flist_size(flist);

    /* allocate an array to record size of each object in archive,
     * save in a global array so circle callbacks can access it */
    g_sizes = (uint64_t*) BAYER_MALLOC(count * sizeof(uint64_t));

    /* compute bytes needed for local files */
    uint64_t total = 0;
    uint64_t index = 0;
    for (index = 0; index < count; index++) {
        /* assume we don't write anything for this item */
        g_sizes[index] = 0;

        /* get type of file */
        bayer_filetype type = bayer_flist_file_get_type(flist, index);

        /* based on the type, compute its archive size */
        if (type == BAYER_TYPE_DIR) {
            /* for a directory, we just write a header */
            g_sizes[index] = HDR_LENGTH;
        } else if (type == BAYER_TYPE_FILE) {
            /* for a file, we write a header plus the file contents */

            /* get file size */
            uint64_t data = bayer_flist_file_get_size(flist, index);

            /* round size up to smallest number of 512-byte chunks */
            uint64_t chunks = data / 512;
            if (chunks * 512 < data) {
                data = (chunks + 1) * 512;
            }

            /* also include header size */
            g_sizes[index] = HDR_LENGTH + data;
        } else if (type == BAYER_TYPE_LINK) {
            /* for a link, we just write a header */
            g_sizes[index] += HDR_LENGTH;
        }

        /* add bytes to our total */
        total += g_sizes[index];
    }

    /* compute global offset in archive for our first file */
    uint64_t archive_offset;
    MPI_Scan(&total, &archive_offset, 1, MPI_UINT64_T, MPI_SUM, MPI_COMM_WORLD);
    archive_offset -= total;
    g_offset = archive_offset;

    /* create archive structure */
    struct archive* a = new_archive();

    /* point archive to write to our file descriptor */
    archive_write_open_fd(a, DTAR_writer.fd_tar);

    /* writer headers for each item */
    uint64_t offset = archive_offset;
    for (index = 0; index < count; index++) {
        /* get item type */
        bayer_filetype type = bayer_flist_file_get_type(flist, index);

        /* writer header for supported types */
        if (type == BAYER_TYPE_FILE ||
            type == BAYER_TYPE_DIR ||
            type == BAYER_TYPE_LINK)
        {
            write_header(a, flist, index, offset);
        }

        /* update our current offset */
        offset += g_sizes[index];
    }

    /* set flist for CIRLCE to enqueue data */
    g_flist = flist;

    /* initialize libcircle to enqueue data */
    CIRCLE_init(0, NULL, CIRCLE_SPLIT_EQUAL | CIRCLE_CREATE_GLOBAL);

    /* set libcircle verbosity level */
    enum CIRCLE_loglevel loglevel = CIRCLE_LOG_WARN;
    CIRCLE_enable_logging(loglevel);

    /* register callbacks */
    CIRCLE_cb_create(&copy_enqueue);
    CIRCLE_cb_process(&copy_process);

    /* run the libcircle job */
    CIRCLE_begin();
    CIRCLE_finalize();

    /* compute global size of archive with all files */
    uint64_t archive_size;
    MPI_Allreduce(&total, &archive_size, 1, MPI_UINT64_T, MPI_SUM, MPI_COMM_WORLD);

    /* is this necessary? */
    /* write archive trailer */
    if (rank == 0) {
        /* write two consecutive 512-blocks of 0's at end */
        char* buf = (char*) BAYER_MALLOC(1024);
        memset(buf, 0, 1024);

        off_t trailer_offset = (off_t) archive_size;
        bayer_lseek(DTAR_writer.name, DTAR_writer.fd_tar, trailer_offset, SEEK_SET);
        bayer_write(DTAR_writer.name, DTAR_writer.fd_tar, buf, 1024);

        bayer_free(&buf);
    }

    /* free archive */
    archive_write_free(a);

    /* free sizes array */
    bayer_free(&g_sizes);

    /* free file list */
    bayer_flist_free(&flist);

    /* close archive file */
    bayer_close(DTAR_writer.name, DTAR_writer.fd_tar);
}

static void
extract(const char *filename, int do_extract, int flags) {

    struct archive *a;
    struct archive *ext;
    struct archive_entry *entry;
    int r;
    /* initiate archive object for reading */
    a = archive_read_new();
    /* initiate archive object for writing */
    ext = archive_write_disk_new();
    archive_write_disk_set_options(ext, flags);

    /* we want all the format supports */
    archive_read_support_filter_bzip2(a);
    archive_read_support_filter_gzip(a);
    archive_read_support_filter_compress(a);
    archive_read_support_format_tar(a);

    archive_write_disk_set_standard_lookup(ext);

    if (filename != NULL && strcmp(filename, "-") == 0)
        filename = NULL;

    /* blocksize set to 1024K */
    if (( r = archive_read_open_filename(a, filename, 10240))) {
        errmsg(archive_error_string(a));
        exit(r);
    }

    for (;;) {
        r = archive_read_next_header(a, &entry);
        if (r == ARCHIVE_EOF)
            break;
        if (r != ARCHIVE_OK) {
            errmsg(archive_error_string(a));
            exit(r);
        }

        if (verbose && do_extract)
            msg("x ");

        if (verbose || !do_extract)
            msg(archive_entry_pathname(entry));

        if (do_extract) {
            r = archive_write_header(ext, entry);
            if (r != ARCHIVE_OK)
                errmsg(archive_error_string(a));
            else
                copy_data(a, ext);
        }

        if (verbose || !do_extract)
            msg("\n");
    }

    archive_read_close(a);
    archive_read_free(a);
    exit(0);
}

static int
copy_data(struct archive *ar, struct archive *aw) {
    int r;
    const void *buff;
    size_t size;
    off_t offset;
    for (;;) {
        r = archive_read_data_block(ar, &buff, &size, &offset);
        if (r == ARCHIVE_EOF) {
            return (ARCHIVE_OK);
        }

        if (r != ARCHIVE_OK)
            return (r);

        r = archive_write_data_block(aw, buff, size, offset);

        if (r != ARCHIVE_OK) {
            errmsg(archive_error_string(ar));
            return (r);
        }
    }
    return 0;
}
