#include <stdlib.h>
#include <errno.h>
#include <string.h>

#include "mpi.h"
#include "mfu.h"

int main (int argc, char* argv[])
{
    MPI_Init(&argc, &argv);
    mfu_init();

    /* get our rank and the size of comm_world */
    int rank, ranks;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &ranks);

    if (argc != 4) {
        if (rank == 0) {
            printf("Usage: dreln oldprefix newprefix path\n");
            fflush(stdout);
        }
        mfu_finalize();
        MPI_Finalize();
        return 0;
    }

    char* oldpath  = argv[1];
    char* newpath  = argv[2];
    char* walkpath = argv[3];

    /* create path objects of old and new prefix */
    mfu_path* path_old = mfu_path_from_str(oldpath);
    mfu_path* path_new = mfu_path_from_str(newpath);
    mfu_path_reduce(path_old);
    mfu_path_reduce(path_new);

    /* get number of elements in old prefix */
    int components_old = mfu_path_components(path_old);

    /* process path to be walked */
    int numpaths = 1;
    mfu_param_path* paths = (mfu_param_path*) MFU_MALLOC((size_t)numpaths * sizeof(mfu_param_path));
    mfu_param_path_set_all((uint64_t)numpaths, (const char**)&walkpath, paths);

    /* TODO: check that walk path is valid */

    mfu_flist flist = mfu_flist_new();

    /* walk list of input paths */
    int walk_stat = 0;
    int walk_perm = 0;
    mfu_flist_walk_param_paths(numpaths, paths, walk_stat, walk_perm, flist);

    /* create a new list by filtering out links */
    mfu_flist linklist_prestat = mfu_flist_subset(flist);
    uint64_t size = mfu_flist_size(flist);
    uint64_t idx;
    for (idx = 0; idx < size; idx++) {
        mfu_filetype type = mfu_flist_file_get_type(flist, idx);
        if (type == MFU_TYPE_LINK) {
            mfu_flist_file_copy(flist, idx, linklist_prestat);
        }
    }
    mfu_flist_summarize(linklist_prestat);
    mfu_flist_free(&flist);

    /* create a third list by stat'ing each link to get timestamps and group info */
    mfu_flist linklist = mfu_flist_new();
    mfu_flist_stat(linklist_prestat, linklist, NULL, NULL);
    mfu_flist_free(&linklist_prestat);

    /* iterate over links, readlink, check against old prefix */
    uint64_t count_changed = 0;
    size = mfu_flist_size(linklist);
    for (idx = 0; idx < size; idx++) {
        /* get path to link */
        const char* name = mfu_flist_file_get_name(linklist, idx);

        /* read link target */
        char path[PATH_MAX + 1];
        ssize_t readlink_rc = mfu_readlink(name, path, sizeof(path) - 1);
        if(readlink_rc < 0) {
            MFU_LOG(MFU_LOG_ERR, "Failed to read link `%s' readlink() (errno=%d %s)",
                name, errno, strerror(errno)
            );
            continue;
        }

        /* ensure that string ends with NUL */
        path[readlink_rc] = '\0';

        /* generate path object for target */
        mfu_path* path_target = mfu_path_from_str(path);

        /* check whether target is an absolute path and falls under the old prefix */
        int is_absolute = mfu_path_is_absolute(path_target);
        mfu_path_result path_cmp = mfu_path_cmp(path_old, path_target);
        if (is_absolute && path_cmp == MFU_PATH_DEST_CHILD) {
            /* increment number of changed links */
            count_changed++;

            /* got a link we need to update,
             * delete current link */
            int unlink_rc = mfu_unlink(name);
            if (unlink_rc != 0) {
              MFU_LOG(MFU_LOG_WARN,
                      "Failed to delete link: `%s' (errno=%d %s)",
                      name, errno, strerror(errno));
            }

            /* define new path, chop old prefix, prepend new prefix */
            mfu_path_slice(path_target, components_old, -1);
            mfu_path_prepend(path_target, path_new);
            const char* target_str = mfu_path_strdup(path_target);

            /* create new link */
            int symlink_rc = mfu_symlink(target_str, name);
            if (symlink_rc < 0) {
                if(errno == EEXIST) {
                    MFU_LOG(MFU_LOG_WARN,
                            "Original link exists, skip the creation: `%s' (errno=%d %s)",
                            name, errno, strerror(errno));
                } else {
                    MFU_LOG(MFU_LOG_ERR, "Create `%s' symlink() failed, (errno=%d %s)",
                            name, errno, strerror(errno)
                    );
                }
            }

            /* TODO: copy xattrs */

            /* get user id and group id of file */
            uid_t uid = (uid_t) mfu_flist_file_get_uid(linklist, idx);
            gid_t gid = (gid_t) mfu_flist_file_get_gid(linklist, idx);

            /* note that we use lchown to change ownership of link itself, it path happens to be a link */
            if (mfu_lchown(name, uid, gid) != 0) {
                /* TODO: are there other EPERM conditions we do want to report? */

                /* since the user running dcp may not be the owner of the
                 * file, we could hit an EPERM error here, and the file
                 * will be left with the effective uid and gid of the dcp
                 * process, don't bother reporting an error for that case */
                if (errno != EPERM) {
                    MFU_LOG(MFU_LOG_ERR, "Failed to change ownership on `%s' lchown() (errno=%d %s)",
                        name, errno, strerror(errno)
                       );
                }
            }

            /* get atime seconds and nsecs */
            uint64_t atime      = mfu_flist_file_get_atime(linklist, idx);
            uint64_t atime_nsec = mfu_flist_file_get_atime_nsec(linklist, idx);

            /* get mtime seconds and nsecs */
            uint64_t mtime      = mfu_flist_file_get_mtime(linklist, idx);
            uint64_t mtime_nsec = mfu_flist_file_get_mtime_nsec(linklist, idx);

            /* fill in time structures */
            struct timespec times[2];
            times[0].tv_sec  = (time_t) atime;
            times[0].tv_nsec = (long)   atime_nsec;
            times[1].tv_sec  = (time_t) mtime;
            times[1].tv_nsec = (long)   mtime_nsec;

            /* set times with nanosecond precision using utimensat,
             * assume path is relative to current working directory,
             * if it's not absolute, and set times on link (not target file)
             * if dest_path refers to a link */
            if (mfu_utimensat(AT_FDCWD, name, times, AT_SYMLINK_NOFOLLOW) != 0) {
                MFU_LOG(MFU_LOG_ERR, "Failed to change timestamps on `%s' utime() (errno=%d %s)",
                    name, errno, strerror(errno)
                   );
            }

            mfu_free(&target_str);
        }

        mfu_path_delete(&path_target);
    }

    /* TODO: print number of changed links */

    mfu_flist_free(&linklist);

    mfu_param_path_free_all(numpaths, paths);

    mfu_path_delete(&path_new);
    mfu_path_delete(&path_old);

    mfu_finalize();
    MPI_Finalize();

    return 0;
}
