#define _GNU_SOURCE

#include "mfu_daos.h"

#include "mpi.h"
#include "mfu_errors.h"

#include <sys/types.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>

#include <daos_fs.h>
#include <daos_uns.h>
#include <gurt/common.h>
#include <gurt/hash.h>
#include <libgen.h>

#ifdef HDF5_SUPPORT
#include <hdf5.h>
#if H5_VERS_MAJOR == 1 && H5_VERS_MINOR < 12
#define H5Sencode1 H5Sencode
#endif
#endif

#if defined(DAOS_API_VERSION_MAJOR) && defined(DAOS_API_VERSION_MINOR)
#define CHECK_DAOS_API_VERSION(major, minor)                            \
        ((DAOS_API_VERSION_MAJOR > (major))                             \
         || (DAOS_API_VERSION_MAJOR == (major) && DAOS_API_VERSION_MINOR >= (minor)))
#else
#define CHECK_DAOS_API_VERSION(major, minor) 0
#endif

/*
 * Private definitions.
 * TODO - Need to reorganize some functions in this file.
 */

void mfu_daos_stats_init(mfu_daos_stats_t* stats)
{
    stats->total_oids = 0;
    stats->total_dkeys = 0;
    stats->total_dkeys = 0;
    stats->total_akeys = 0;
    stats->bytes_read = 0;
    stats->bytes_written = 0;
    stats->wtime_started = 0;
    stats->wtime_ended = 0;
}

void mfu_daos_stats_start(mfu_daos_stats_t* stats)
{
    time(&stats->time_started);
    stats->wtime_started = MPI_Wtime();
}

void mfu_daos_stats_end(mfu_daos_stats_t* stats)
{
    time(&stats->time_ended);
    stats->wtime_ended = MPI_Wtime();
}

void mfu_daos_stats_sum(mfu_daos_stats_t* stats, mfu_daos_stats_t* stats_sum)
{
    int num_values = 5;
    /* put local values into buffer */
    uint64_t values[num_values];
    values[0] = stats->total_oids;
    values[1] = stats->total_dkeys;
    values[2] = stats->total_akeys;
    values[3] = stats->bytes_read;
    values[4] = stats->bytes_written;

    /* sum the values */
    MPI_Allreduce(MPI_IN_PLACE, values, num_values, MPI_UINT64_T, MPI_SUM, MPI_COMM_WORLD);

    /* store summed values */
    stats_sum->total_oids = values[0];
    stats_sum->total_dkeys = values[1];
    stats_sum->total_akeys = values[2];
    stats_sum->bytes_read = values[3];
    stats_sum->bytes_written = values[4];

    /* copy times */
    stats_sum->time_started = stats->time_started;
    stats_sum->time_ended = stats->time_ended;
    stats_sum->wtime_started = stats->wtime_started;
    stats_sum->wtime_ended = stats->wtime_ended;
}

void mfu_daos_stats_print(
    mfu_daos_stats_t* stats,
    bool print_read,
    bool print_write,
    bool print_read_rate,
    bool print_write_rate)
{
    /* format start time */
    char starttime_str[256];
    struct tm* localstart = localtime(&stats->time_started);
    strftime(starttime_str, 256, "%b-%d-%Y,%H:%M:%S", localstart);

    /* format end time */
    char endtime_str[256];
    struct tm* localend = localtime(&stats->time_ended);
    strftime(endtime_str, 256, "%b-%d-%Y,%H:%M:%S", localend);

    /* compute relative time elapsed */
    double rel_time = stats->wtime_ended - stats->wtime_started;

    /* convert read size to units */
    double read_size_tmp;
    const char* read_size_units;
    mfu_format_bytes(stats->bytes_read, &read_size_tmp, &read_size_units);

    /* convert write size to units */
    double write_size_tmp;
    const char* write_size_units;
    mfu_format_bytes(stats->bytes_written, &write_size_tmp, &write_size_units);

    MFU_LOG(MFU_LOG_INFO, "Started       : %s", starttime_str);
    MFU_LOG(MFU_LOG_INFO, "Completed     : %s", endtime_str);
    MFU_LOG(MFU_LOG_INFO, "Seconds       : %.3lf", rel_time);
    MFU_LOG(MFU_LOG_INFO, "Objects       : %" PRId64, stats->total_oids);
    MFU_LOG(MFU_LOG_INFO, "  D-Keys      : %" PRId64, stats->total_dkeys);
    MFU_LOG(MFU_LOG_INFO, "  A-Keys      : %" PRId64, stats->total_akeys);

    if (print_read) {
        MFU_LOG(MFU_LOG_INFO, "Bytes read    : %.3lf %s (%" PRId64 " bytes)",
                read_size_tmp, read_size_units, stats->bytes_read);
    }
    if (print_write) {
        MFU_LOG(MFU_LOG_INFO, "Bytes written : %.3lf %s (%" PRId64 " bytes)",
                write_size_tmp, write_size_units, stats->bytes_written);
    }

    if (print_read_rate) {
        /* Compute read rate */
        double read_rate = 0;
        if (rel_time > 0) {
            read_rate = (double) stats->bytes_read / rel_time;
        }

        /* Convert read rate to units */
        double read_rate_tmp;
        const char* read_rate_units;
        mfu_format_bw(read_rate, &read_rate_tmp, &read_rate_units);

        MFU_LOG(MFU_LOG_INFO, "Read rate     : %.3lf %s",
                read_rate_tmp, read_rate_units);
    }

    if (print_write_rate) {
        /* Compute write rate */
        double write_rate = 0;
        if (rel_time > 0) {
            write_rate = (double) stats->bytes_written / rel_time;
        }

        /* Convert write rate to units */
        double write_rate_tmp;
        const char* write_rate_units;
        mfu_format_bw(write_rate, &write_rate_tmp, &write_rate_units);

        MFU_LOG(MFU_LOG_INFO, "Write rate    : %.3lf %s",
                write_rate_tmp, write_rate_units);
    }
}

void mfu_daos_stats_print_sum(
    int rank,
    mfu_daos_stats_t* stats,
    bool print_read,
    bool print_write,
    bool print_read_rate,
    bool print_write_rate)
{
    mfu_daos_stats_t stats_sum;
    mfu_daos_stats_sum(stats, &stats_sum);
    if (rank == 0) {
        mfu_daos_stats_print(&stats_sum, print_read, print_write, print_read_rate, print_write_rate);
    }
}

bool daos_uuid_valid(const uuid_t uuid)
{
    return uuid && !uuid_is_null(uuid);
}

/* Verify DAOS arguments are valid */
static int daos_check_args(
    int rank,
    char** argpaths,
    int numpaths,
    daos_args_t* da,
    int* flag_daos_args)
{
    char* src_path = NULL;
    char* dst_path = NULL;
    if (numpaths > 0) {
        src_path = argpaths[0];
    }
    if (numpaths > 1) {
        dst_path = argpaths[1];
    }

    bool have_src_path  = (src_path != NULL);
    bool have_dst_path  = (dst_path != NULL);
    bool have_src_pool  = strlen(da->src_pool) ? true : false;
    bool have_src_cont  = strlen(da->src_cont) ? true : false;
    bool have_dst_pool  = strlen(da->dst_pool) ? true : false;
    bool have_dst_cont  = strlen(da->dst_cont) ? true : false;
    bool have_prefix    = (da->dfs_prefix != NULL);

    /* Determine whether any DAOS arguments are supplied. 
     * If not, then there is nothing to check. */
    *flag_daos_args = 0;
    if (have_src_pool || have_src_cont || have_dst_pool || have_dst_cont
            || have_prefix) {
        *flag_daos_args = 1;
    }
    if ((have_src_path && (strncmp(src_path, "daos:", 5) == 0)) ||
            (have_dst_path && (strncmp(dst_path, "daos:", 5) == 0))) {
        *flag_daos_args = 1;
    }
    if (*flag_daos_args == 0) {
        return 0;
    }
    
    /* if passed in values for src/dst are both uuids ignore the case when comparing */

    /* Determine whether the source and destination
     * use the same pool and container */
    int rc1, rc2;
    bool same_pool = false;
    uuid_t src_pool;
    uuid_t dst_pool;
    rc1 = uuid_parse(da->src_pool, src_pool); 
    rc2 = uuid_parse(da->dst_pool, dst_pool); 
    if (rc1 == 0 && rc2 == 0) {
        same_pool = (strcasecmp(da->src_pool, da->dst_pool) == 0);
    } else {
        same_pool = (strcmp(da->src_pool, da->dst_pool) == 0);
    }

    bool same_cont = false;
    uuid_t src_cont;
    uuid_t dst_cont;
    rc1 = uuid_parse(da->src_cont, src_cont); 
    rc2 = uuid_parse(da->dst_cont, dst_cont); 
    if (rc1 == 0 && rc2 == 0) {
        same_cont = same_pool && (strcasecmp(da->src_cont, da->dst_cont) == 0);
    } else {
        same_cont = same_pool && (strcmp(da->src_cont, da->dst_cont) == 0);
    }

    /* Determine whether the source and destination paths are the same.
     * Assume NULL == NULL. */
    bool same_path = false;
    if (have_src_path && have_dst_path && strcmp(src_path, dst_path) == 0) {
        same_path = true;
    } else if (!have_src_path && !have_dst_path) {
        same_path = true;
    }

    int rc = 0;

    /* Make sure the source and destination are different */
    if (same_cont && same_path) {
        if (rank == 0) {
             MFU_LOG(MFU_LOG_ERR, "DAOS source is DAOS destination");
        }
        rc = 1;
    }

    return rc;
}

/* Checks if the prefix is valid.
 * If valid, returns matching string into suffix */
static bool daos_check_prefix(
    char* path,
    const char* dfs_prefix,
    char** suffix)
{
    bool is_prefix = false;
    int prefix_len = strlen(dfs_prefix);
    int path_len = strlen(path);

    /* ignore trailing '/' on the prefix */
    if (dfs_prefix[prefix_len-1] == '/') {
        prefix_len--;
    }

    /* figure out if dfs_prefix is a prefix of the file path */
    if (strncmp(path, dfs_prefix, prefix_len) == 0) {
        /* if equal, assume root */
        if (path_len == prefix_len) {
            *suffix = strdup("/");
            is_prefix = true;
        }
        /* if path is longer, it must start with '/' */
        else if (path_len > prefix_len && path[prefix_len] == '/') {
            *suffix = strdup(path + prefix_len);
            is_prefix = true;
        }
    }
    return is_prefix;
}

/*
 * Parse a path of the format:
 * daos://<pool>/<cont>/<path> | <UNS path> | <POSIX path>
 * Modifies "path" to be the relative container path, defaulting to "/".
 * Returns 0 IFF a daos path was successfully parsed.
 * Returns 1 if a daos path was not parsed.
 * Returns -1 for actual errors.
 */
int daos_parse_path(
    char* path,
    size_t path_len,
    char (*pool_str)[],
    char (*cont_str)[])
{
    struct duns_attr_t  dattr = {0};
    int                 rc;
    char*               tmp_path1 = NULL;
    char*               path_dirname = NULL;
    char*               tmp_path2 = NULL;
    char*               path_basename = NULL;
    char*               tmp = NULL;

    /* check first if duns_resolve_path succeeds on regular path */
    rc = duns_resolve_path(path, &dattr);
    if (rc == 0) {
        /* daos:// or UNS path */
        snprintf(*pool_str, DAOS_PROP_LABEL_MAX_LEN + 1, "%s", dattr.da_pool);
        snprintf(*cont_str, DAOS_PROP_LABEL_MAX_LEN + 1, "%s", dattr.da_cont);
        if (dattr.da_rel_path == NULL) {
            strncpy(path, "/", path_len);
        } else {
            strncpy(path, dattr.da_rel_path, path_len);
        }
    } else {
        /* If basename does not exist yet then duns_resolve_path will fail even
        * if dirname is a UNS path */

        /* get dirname */
        tmp_path1 = strdup(path);
        if (tmp_path1 == NULL) {
            rc = -ENOMEM;
            goto out;
        }
        path_dirname = dirname(tmp_path1);

        /* reset before calling duns_resolve_path with new string */
        memset(&dattr, 0, sizeof(struct duns_attr_t));

        /* Check if this path represents a daos pool and/or container. */
        rc = duns_resolve_path(path_dirname, &dattr);
        /* if it succeeds get the basename and append it to the rel_path */
        if (rc == 0) {
            /* if duns_resolve_path succeeds then concat basename to 
            * da_rel_path */
            tmp_path2 = strdup(path);
            if (tmp_path2 == NULL) {
                rc = -ENOMEM;
                goto out;
            }
            path_basename = basename(tmp_path2);
   
            /* dirname might be root uns path, if that is the case,
             * then da_rel_path might be NULL */
            if (dattr.da_rel_path == NULL) {
                tmp = MFU_CALLOC(path_len, sizeof(char));
            } else {
                tmp = realloc(dattr.da_rel_path, path_len);
            }
            if (tmp == NULL) {
                rc = -ENOMEM;
                goto out;
            }
            dattr.da_rel_path = tmp;
            strcat(dattr.da_rel_path, "/");
            strcat(dattr.da_rel_path, path_basename);

            snprintf(*pool_str, DAOS_PROP_LABEL_MAX_LEN + 1, "%s", dattr.da_pool);
            snprintf(*cont_str, DAOS_PROP_LABEL_MAX_LEN + 1, "%s", dattr.da_cont);
            strncpy(path, dattr.da_rel_path, path_len);
        } else if (strncmp(path, "daos:", 5) == 0) {
            /* Actual error, since we expect a daos path */
            rc = -1;
        } else {
            /* We didn't parse a daos path,
            * but we weren't necessarily looking for one */
            rc = 1;
        }
    }
out:
    mfu_free(&tmp_path1);
    mfu_free(&tmp_path2);
    mfu_free(&dattr.da_rel_path);
    duns_destroy_attr(&dattr);
    return rc;
}

/* Checks for UNS paths and sets
 * paths and DAOS args accordingly */
static int daos_set_paths(
  int rank,
  char** argpaths,
  int numpaths,
  daos_args_t* da,
  bool *dst_cont_passed)
{
    int     rc = 0;
    bool    have_dst = (numpaths > 1);
    bool    prefix_on_src = false;
    bool    prefix_on_dst = false;
    char*   prefix_path = NULL;
    char*   src_path = NULL;
    char*   dst_path = NULL;

    /* find out if a dfs_prefix is being used,
     * if so, then that means that the container
     * is not being copied from the root of the
     * UNS path  */
    if (da->dfs_prefix != NULL) {
        char prefix_pool[DAOS_PROP_LABEL_MAX_LEN + 1];
        char prefix_cont[DAOS_PROP_LABEL_MAX_LEN + 1];
        int prefix_rc;

        size_t prefix_len = strlen(da->dfs_prefix);
        prefix_path = strndup(da->dfs_prefix, prefix_len);
        if (prefix_path == NULL) {
            MFU_LOG(MFU_LOG_ERR, "Unable to allocate space for DAOS prefix.");
            rc = 1;
            goto out;
        }

        memset(prefix_pool, '\0', DAOS_PROP_LABEL_MAX_LEN + 1);
        memset(prefix_cont, '\0', DAOS_PROP_LABEL_MAX_LEN + 1);

        /* Get the pool/container uuids from the prefix */
        prefix_rc = daos_parse_path(prefix_path, prefix_len,
                                    &prefix_pool, &prefix_cont);
        if (prefix_rc != 0) {
            MFU_LOG(MFU_LOG_ERR, "Failed to resolve DAOS Prefix UNS path");
            rc = 1;
            goto out;
        }

        /* In case the user tries to give a sub path in the UNS path */
        if (strcmp(prefix_path, "/") != 0) {
            MFU_LOG(MFU_LOG_ERR, "DAOS prefix must be a UNS path");
            rc = 1;
            goto out;
        }

        /* Check if the prefix matches the source */
        prefix_on_src = daos_check_prefix(argpaths[0], da->dfs_prefix, &da->src_path);
        if (prefix_on_src) {
            snprintf(da->src_pool, DAOS_PROP_LABEL_MAX_LEN + 1, "%s", prefix_pool);
            snprintf(da->src_cont, DAOS_PROP_LABEL_MAX_LEN + 1, "%s", prefix_cont);
            argpaths[0] = da->src_path;
        }

        if (have_dst) {
            /* Check if the prefix matches the destination */
            prefix_on_dst = daos_check_prefix(argpaths[1], da->dfs_prefix, &da->dst_path);
            if (prefix_on_dst) {
                snprintf(da->dst_pool, DAOS_PROP_LABEL_MAX_LEN + 1, "%s", prefix_pool);
                snprintf(da->dst_cont, DAOS_PROP_LABEL_MAX_LEN + 1, "%s", prefix_cont);
                argpaths[1] = da->dst_path;
            }
        }

        if (!prefix_on_src && !prefix_on_dst) {
            MFU_LOG(MFU_LOG_ERR, "DAOS prefix does not match source or destination");
            rc = 1;
            goto out;
        }
    }

    /*
     * For the source and destination paths,
     * if they are not using a prefix,
     * then just directly try to parse a DAOS path.
     */
    if (!prefix_on_src) {
        size_t src_len = strlen(argpaths[0]);
        src_path = strndup(argpaths[0], src_len);
        if (src_path == NULL) {
            MFU_LOG(MFU_LOG_ERR, "Unable to allocate space for source path.");
            rc = 1;
            goto out;
        }
        int src_rc = daos_parse_path(src_path, src_len, &da->src_pool, &da->src_cont);
        if (src_rc == 0) {
            if (strlen(da->src_cont) == 0) {
                MFU_LOG(MFU_LOG_ERR, "Source pool requires a source container.");
                rc = 1;
                goto out;
            }
            argpaths[0] = da->src_path = strdup(src_path);
            if (argpaths[0] == NULL) {
                MFU_LOG(MFU_LOG_ERR, "Unable to allocate space for source path.");
                rc = 1;
                goto out;
            }
        } else if (src_rc == -1) {
            MFU_LOG(MFU_LOG_ERR, "Failed to parse DAOS source path: daos://<pool>/<cont>[/<path>]");
            rc = 1;
            goto out;
        }
    }
    if (have_dst && !prefix_on_dst) {
        size_t dst_len = strlen(argpaths[1]);
        dst_path = strndup(argpaths[1], dst_len);
        if (dst_path == NULL) {
            MFU_LOG(MFU_LOG_ERR, "Unable to allocate space for destination path.");
            rc = 1;
            goto out;
        }
        int dst_rc = daos_parse_path(dst_path, dst_len, &da->dst_pool, &da->dst_cont);
        if (dst_rc == 0) {
            argpaths[1] = da->dst_path = strdup(dst_path);
            if (argpaths[1] == NULL) {
                MFU_LOG(MFU_LOG_ERR, "Unable to allocate space for destination path.");
                rc = 1;
                goto out;
            }
        } else if (dst_rc == -1) {
            MFU_LOG(MFU_LOG_ERR, "Failed to parse DAOS destination path: daos://<pool>/<cont>[/<path>]");
            rc = 1;
            goto out;
        }
    }

    if (have_dst) {
        int dst_cont_len = strlen(da->dst_cont);
        *dst_cont_passed = dst_cont_len > 0 ? true : false;
        /* Generate a new container uuid if only a pool was given. */
        if (!*dst_cont_passed) {
            uuid_generate(da->dst_cont_uuid);
        }
    }

out:
    mfu_free(&prefix_path);
    mfu_free(&src_path);
    mfu_free(&dst_path);
    return rc;
}

static int daos_get_cont_type(
    daos_handle_t coh,
    enum daos_cont_props* type)
{
    daos_prop_t*    prop = daos_prop_alloc(1);
    int             rc;

    if (prop == NULL) {
        MFU_LOG(MFU_LOG_ERR, "Failed to allocate prop (%d)", rc);
        return 1;
    }

    prop->dpp_entries[0].dpe_type = DAOS_PROP_CO_LAYOUT_TYPE;

    rc = daos_cont_query(coh, NULL, prop, NULL);
    if (rc) {
        MFU_LOG(MFU_LOG_ERR, "daos_cont_query() failed (%d)", rc);
        daos_prop_free(prop);
        return 1;
    }

    *type = prop->dpp_entries[0].dpe_val;
    daos_prop_free(prop);
    return 0;
}

/* check if cont status is unhealthy */
static int daos_check_cont_status(daos_handle_t coh, bool *status_healthy)
{
    daos_prop_t*            prop = daos_prop_alloc(1);
    struct daos_prop_entry  *entry;
    struct daos_co_status   stat = {0};
    int                     rc = 0;
    
    if (prop == NULL) {
        MFU_LOG(MFU_LOG_ERR, "Failed to allocate prop: "DF_RC, DP_RC(rc));
        rc = ENOMEM;
        goto out;
    }

    prop->dpp_entries[0].dpe_type = DAOS_PROP_CO_STATUS;

    rc = daos_cont_query(coh, NULL, prop, NULL);
    if (rc) {
        MFU_LOG(MFU_LOG_ERR, "daos_cont_query() failed "DF_RC, DP_RC(rc));
        rc = 1;
        goto out;
    }

    entry = &prop->dpp_entries[0];
    daos_prop_val_2_co_status(entry->dpe_val, &stat);                            
    if (stat.dcs_status == DAOS_PROP_CO_HEALTHY) {
        *status_healthy = true;
    } else {
        *status_healthy = false;
    }

out:
    daos_prop_free(prop);
    return rc;
}

/*
 * Try to set the file type based on the container type,
 * using api as a guide.
 */
static int daos_set_api_cont_type(
    mfu_file_t* mfu_file,
    enum daos_cont_props cont_type,
    daos_api_t api)
{
    /* If explicitly using DAOS, just set the type to DAOS */
    if (api == DAOS_API_DAOS) {
        mfu_file->type = DAOS;
        return 0;
    }

    if (cont_type == DAOS_PROP_CO_LAYOUT_POSIX) {
        mfu_file->type = DFS;
    } else {
        mfu_file->type = DAOS;
    }

    /* If explicitly using DFS, the container types must be POSIX */
    if (api == DAOS_API_DFS && mfu_file->type != DFS) {
        MFU_LOG(MFU_LOG_ERR, "Cannot use non-POSIX container with DFS API.");
        return 1;
    }

    return 0;
}

/*
 * Set the mfu_file types to either DAOS or DFS,
 * and make sure they are compatible.
 */
static int daos_set_api_compat(
    mfu_file_t* mfu_src_file,
    mfu_file_t* mfu_dst_file,
    daos_args_t* da,
    char** argpaths)
{
    bool have_dst = (mfu_dst_file != NULL);

    /* Check whether we have pool/cont uuids */
    bool have_src_cont  = strlen(da->src_cont) ? true : false;
    bool have_dst_cont  = strlen(da->dst_cont) ? true : false;

    /* Containers must be the same type */
    if (have_src_cont && have_dst_cont) {
        if (da->src_cont_type != da->dst_cont_type) {
            MFU_LOG(MFU_LOG_ERR, "Containers must be the same type.");
            return 1;
        }
    }

    /* Check whether we have source and destination paths */
    char* src_path = argpaths[0];
    char* dst_path = argpaths[1];
    bool have_src_path = (src_path != NULL);
    bool have_dst_path = (dst_path != NULL);
    bool src_path_is_root = (have_src_path && (strcmp(src_path, "/") == 0));
    bool dst_path_is_root = (have_dst_path && (strcmp(dst_path, "/") == 0));

    /* If either type is DAOS:
     * Both paths must be root.
     * The other must be DAOS or DFS.
     * The other will be set to DAOS, for obj-level copy. */
    if (mfu_src_file->type == DAOS) {
        if (!src_path_is_root || !dst_path_is_root) {
            MFU_LOG(MFU_LOG_ERR, "Cannot use path with non-POSIX container.");
            return 1;
        }
        if (have_dst) {
            if (mfu_dst_file->type != DAOS && mfu_dst_file->type != DFS) {
                MFU_LOG(MFU_LOG_ERR, "Cannot copy non-POSIX container outside DAOS.");
                return 1;
            }
            mfu_dst_file->type = DAOS;
        }
    }
    if (have_dst && mfu_dst_file->type == DAOS) {
        if (!dst_path_is_root || !src_path_is_root) {
            MFU_LOG(MFU_LOG_ERR, "Cannot use path with non-POSIX container.");
            return 1;
        }
        if (mfu_src_file->type != DAOS && mfu_src_file->type != DFS) {
            MFU_LOG(MFU_LOG_ERR, "Cannot copy non-POSIX container outside DAOS.");
            return 1;
        }
        mfu_src_file->type = DAOS;
    }
    return 0;
}

/* return 1 if any process has a local error
 * return 0 otherwise
 * ignore if no daos args supplied */
static int daos_any_error(int rank, bool local_daos_error, int flag_daos_args)
{
    bool no_error = !local_daos_error;
    if (flag_daos_args == 0) {
        no_error = true;
    }

    if (! mfu_alltrue(no_error, MPI_COMM_WORLD)) {
        if (rank == 0) {
           MFU_LOG(MFU_LOG_ERR, "Detected one or more DAOS errors: "
                   MFU_ERRF, MFU_ERRP(-MFU_ERR_DAOS));
        }
        return 1;
    }

    return 0;
}

/*
 * Get the user attributes for a container in a format similar
 * to what daos_cont_set_prop expects.
 * The last entry is the ACL and is conditionally set only if
 * the user has permissions.
 * The properties should remain in the order expected by serialization.
 * Returns 1 on error, 0 on success.
 */
static int cont_get_props(daos_handle_t coh, daos_prop_t** _props,
                          bool get_oid, bool get_label, bool get_roots)
{
    int             rc;
    daos_prop_t*    props = NULL;
    daos_prop_t*    prop_acl = NULL;
    daos_prop_t*    props_merged = NULL;
    /* total amount of properties to allocate */
    uint32_t        total_props = 15;
    /* minimum number of properties that are always allocated/used to start
     * count */
    int             prop_index = 15;

    if (get_oid) {
        total_props++;
    }

    /* container label is required to be unique, so do not
     * retrieve it for copies. The label is retrieved for
     * serialization, but only deserialized if the label
     * no longer exists in the pool */
    if (get_label) {
        total_props++;
    }

    if (get_roots) {
        total_props++;
    }

    /* Allocate space for all props except ACL. */
    props = daos_prop_alloc(total_props);
    if (props == NULL) {
        MFU_LOG(MFU_LOG_ERR, "Failed to allocate container properties.");
        rc = 1;
        goto out;
    }

    /* The order of properties MUST match the order expected by serialization. */
    props->dpp_entries[0].dpe_type = DAOS_PROP_CO_LAYOUT_TYPE;
    props->dpp_entries[1].dpe_type = DAOS_PROP_CO_LAYOUT_VER;
    props->dpp_entries[2].dpe_type = DAOS_PROP_CO_CSUM;
    props->dpp_entries[3].dpe_type = DAOS_PROP_CO_CSUM_CHUNK_SIZE;
    props->dpp_entries[4].dpe_type = DAOS_PROP_CO_CSUM_SERVER_VERIFY;
    props->dpp_entries[5].dpe_type = DAOS_PROP_CO_REDUN_FAC;
    props->dpp_entries[6].dpe_type = DAOS_PROP_CO_REDUN_LVL;
    props->dpp_entries[7].dpe_type = DAOS_PROP_CO_SNAPSHOT_MAX;
    props->dpp_entries[8].dpe_type = DAOS_PROP_CO_COMPRESS;
    props->dpp_entries[9].dpe_type = DAOS_PROP_CO_ENCRYPT;
    props->dpp_entries[10].dpe_type = DAOS_PROP_CO_OWNER;
    props->dpp_entries[11].dpe_type = DAOS_PROP_CO_OWNER_GROUP;
    props->dpp_entries[12].dpe_type = DAOS_PROP_CO_DEDUP;
    props->dpp_entries[13].dpe_type = DAOS_PROP_CO_DEDUP_THRESHOLD;
    props->dpp_entries[14].dpe_type = DAOS_PROP_CO_EC_CELL_SZ;

    /* Conditionally get the OID. Should always be true for serialization. */
    if (get_oid) {
        props->dpp_entries[prop_index].dpe_type = DAOS_PROP_CO_ALLOCED_OID;
        prop_index++;
    }

    if (get_label) {
        props->dpp_entries[prop_index].dpe_type = DAOS_PROP_CO_LABEL;
        prop_index++;
    }

    if (get_roots) {
        props->dpp_entries[prop_index].dpe_type = DAOS_PROP_CO_ROOTS;
    }

    /* Get all props except ACL first. */
    rc = daos_cont_query(coh, NULL, props, NULL);
    if (rc != 0) {
        MFU_LOG(MFU_LOG_ERR, "Failed to query container: "DF_RC, DP_RC(rc));
        rc = 1;
        goto out;
    }

    /* Fetch the ACL separately in case user doesn't have access */
    rc = daos_cont_get_acl(coh, &prop_acl, NULL);
    if (rc == 0) {
        /* ACL will be appended to the end */
        props_merged = daos_prop_merge(props, prop_acl);
        if (props_merged == NULL) {
            MFU_LOG(MFU_LOG_ERR, "Failed to set container ACL: "DF_RC, DP_RC(rc));
            rc = 1;
            goto out;
        }
        daos_prop_free(props);
        props = props_merged;
    } else if (rc && rc != -DER_NO_PERM) {
        MFU_LOG(MFU_LOG_ERR, "Failed to query container ACL: "DF_RC, DP_RC(rc));
        rc = 1;
        goto out;
    }

    rc = 0;
    *_props = props;

out:
    daos_prop_free(prop_acl);
    if (rc != 0) {
        daos_prop_free(props);
    }

    return rc;
}

/*
 * Free the user attribute buffers created by cont_get_usr_attrs.
 */
static void cont_free_usr_attrs(int n, char*** _names, void*** _buffers,
                               size_t** _sizes)
{
    char**  names = *_names;
    void**  buffers = *_buffers;

    if (names != NULL) {
        for (size_t i = 0; i < n; i++) {
            mfu_free(&names[i]);
        }
        mfu_free(_names);
    }
    if (buffers != NULL) {
        for (size_t i = 0; i < n; i++) {
            mfu_free(&buffers[i]);
        }
        mfu_free(_buffers);
    }
    mfu_free(_sizes);
}

/*
 * Get the user attributes for a container in a format similar
 * to what daos_cont_set_attr expects.
 * cont_free_usr_attrs should be called to free the allocations.
 * Returns 1 on error, 0 on success.
 */
static int cont_get_usr_attrs(daos_handle_t coh,
                              int* _n, char*** _names, void*** _buffers,
                              size_t** _sizes)
{
    int         rc = 0;
    uint64_t    total_size = 0;
    uint64_t    cur_size = 0;
    uint64_t    num_attrs = 0;
    uint64_t    name_len = 0;
    char*       name_buf = NULL;
    char**      names = NULL;
    void**      buffers = NULL;
    size_t*     sizes = NULL;

    /* Get the total size needed to store all names */
    rc = daos_cont_list_attr(coh, NULL, &total_size, NULL);
    if (rc != 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to list user attributes "DF_RC, DP_RC(rc));
        rc = 1;
        goto out;
    }

    /* no attributes found */
    if (total_size == 0) {
        *_n = 0;
        goto out;
    }

    /* Allocate a buffer to hold all attribute names */
    name_buf = MFU_CALLOC(total_size, sizeof(char));
    if (name_buf == NULL) {
        MFU_LOG(MFU_LOG_ERR, "failed to allocate user attribute buffer");
        rc = 1;
        goto out;
    }

    /* Get the attribute names */
    rc = daos_cont_list_attr(coh, name_buf, &total_size, NULL);
    if (rc != 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to list user attributes "DF_RC, DP_RC(rc));
        rc = 1;
        goto out;
    }

    /* Figure out the number of attributes */
    while (cur_size < total_size) {
        name_len = strnlen(name_buf + cur_size, total_size - cur_size);
        if (name_len == total_size - cur_size) {
            /* end of buf reached but no end of string, ignoring */
            break;
        }
        num_attrs++;
        cur_size += name_len + 1;
    }

    /* Sanity check */
    if (num_attrs == 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to parse user attributes");
        rc = 1;
        goto out;
    }

    /* Allocate arrays for attribute names, buffers, and sizes */
    names = MFU_CALLOC(num_attrs, sizeof(char*));
    if (names == NULL) {
        MFU_LOG(MFU_LOG_ERR, "failed to allocate user attribute buffer");
        rc = 1;
        goto out;
    }
    sizes = MFU_CALLOC(num_attrs, sizeof(size_t));
    if (sizes == NULL) {
        MFU_LOG(MFU_LOG_ERR, "failed to allocate user attribute buffer");
        rc = 1;
        goto out;
    }
    buffers = MFU_CALLOC(num_attrs, sizeof(void*));
    if (buffers == NULL) {
        MFU_LOG(MFU_LOG_ERR, "failed to allocate user attribute buffer");
        rc = 1;
        goto out;
    }

    /* Create the array of names */
    cur_size = 0;
    for (uint64_t i = 0; i < num_attrs; i++) {
        name_len = strnlen(name_buf + cur_size, total_size - cur_size);
        if (name_len == total_size - cur_size) {
            /* end of buf reached but no end of string, ignoring */
            break;
        }
        names[i] = strndup(name_buf + cur_size, name_len + 1);
        cur_size += name_len + 1;
    }

    /* Get the buffer sizes */
    rc = daos_cont_get_attr(coh, num_attrs,
                            (const char* const*)names,
                            NULL, sizes, NULL);
    if (rc != 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to get user attribute sizes "DF_RC, DP_RC(rc));
        rc = 1;
        goto out;
    }

    /* Allocate space for each value */
    for (uint64_t i = 0; i < num_attrs; i++) {
        buffers[i] = MFU_CALLOC(sizes[i], sizeof(size_t));
        if (buffers[i] == NULL) {
            MFU_LOG(MFU_LOG_ERR, "failed to allocate user attribute buffer");
            rc = 1;
            goto out;
        }
    }

    /* Get the attribute values */
    rc = daos_cont_get_attr(coh, num_attrs,
                            (const char* const*)names,
                            (void * const*)buffers, sizes,
                            NULL);
    if (rc != 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to get user attribute values "DF_RC, DP_RC(rc));
        rc = 1;
        goto out;
    }

    /* Return values to the caller */
    *_n = num_attrs;
    *_names = names;
    *_buffers = buffers;
    *_sizes = sizes;
out:
    if (rc != 0) {
        cont_free_usr_attrs(num_attrs, &names, &buffers, &sizes);
    }

    mfu_free(&name_buf);
    return rc;
}

/* Copy all user attributes from one container to another.
 * Returns 1 on error, 0 on success. */
static int copy_usr_attrs(daos_handle_t src_coh, daos_handle_t dst_coh)
{
    int         num_attrs = 0;
    char**      names = NULL;
    void**      buffers = NULL;
    size_t*     sizes = NULL;
    int         rc;

    /* Get all user attributes */
    rc = cont_get_usr_attrs(src_coh, &num_attrs, &names, &buffers, &sizes);
    if (rc != 0) {
        rc = 1;
        goto out;
    }

    if (num_attrs == 0) {
        rc = 0;
        goto out;
    }

    rc = daos_cont_set_attr(dst_coh, num_attrs,
                            (char const* const*) names,
                            (void const* const*) buffers,
                            sizes, NULL);
    if (rc != 0) {
        MFU_LOG(MFU_LOG_ERR, "Failed to set user attrs: "DF_RC, DP_RC(rc));
        rc = 1;
        goto out;
    }

out:
    cont_free_usr_attrs(num_attrs, &names, &buffers, &sizes);
    return rc;
}

/* Distribute process 0's pool or container handle to others. */
void daos_bcast_handle(
  int rank,              /* root rank for broadcast */
  daos_handle_t* handle, /* handle value to be broadcasted */
  daos_handle_t* poh,    /* daos pool for global2local conversion of container handle */
  enum handleType type)  /* handle type: POOL_HANDLE or CONT_HANDLE */
{
    int rc;

    d_iov_t global;
    global.iov_buf     = NULL;
    global.iov_buf_len = 0;
    global.iov_len     = 0;

    /* Get the global handle size. */
    if (rank == 0) {
        if (type == POOL_HANDLE) {
            rc = daos_pool_local2global(*handle, &global);
        } else {
            rc = daos_cont_local2global(*handle, &global);
        }
        if (rc != 0) {
            MFU_ABORT(-1, "Failed to get global handle size");
        }
    }

    /* broadcast size of global handle */
    MPI_Bcast(&global.iov_buf_len, 1, MPI_UINT64_T, 0, MPI_COMM_WORLD);

    /* allocate memory to hold global handle value */
    global.iov_len = global.iov_buf_len;
    global.iov_buf = MFU_MALLOC(global.iov_buf_len);

    /* convert from local handle to global handle */
    if (rank == 0) {
       if (type == POOL_HANDLE) {
           rc = daos_pool_local2global(*handle, &global);
       } else {
           rc = daos_cont_local2global(*handle, &global);
       }
       if (rc != 0) {
           MFU_ABORT(-1, "Failed to create global handle");
       }
    }

    /* broadcast global handle value */
    MPI_Bcast(global.iov_buf, global.iov_buf_len, MPI_BYTE, 0, MPI_COMM_WORLD);

    /* convert global handle to local value */
    if (rank != 0) {
        if (type == POOL_HANDLE) {
            rc = daos_pool_global2local(global, handle);
        } else {
            rc = daos_cont_global2local(*poh, global, handle);
        }
        if (rc != 0) {
            MFU_ABORT(-1, "Failed to get local handle");
        }
    }

    /* free temporary buffer used to hold global handle value */
    mfu_free(&global.iov_buf);
}

/* connect to DAOS pool, and then open container */
int daos_connect(
  int rank,
  daos_args_t* da,
  char (*pool)[],
  char (*cont)[],
  daos_handle_t* poh,
  daos_handle_t* coh,
  bool force_serialize,
  bool connect_pool,
  bool create_cont,
  bool require_new_cont,
  bool preserve,
  mfu_file_t* mfu_src_file,
  bool dst_cont_passed)
{
    /* sanity check */
    if (require_new_cont && !create_cont) {
        if (rank == 0) {
            MFU_LOG(MFU_LOG_ERR, "create_cont must be true when require_new_cont is true");
        }
        return -1;
    }

    int                         valid = 0; /* assume failure until otherwise */
    int                         rc;
    daos_prop_t                 *props = NULL;
    struct daos_prop_co_roots   roots = {0};
#ifdef HDF5_SUPPORT
    struct hdf5_args hdf5;
#endif

    /* have rank 0 connect to the pool and container,
     * we'll then broadcast the handle ids from rank 0 to everyone else */
    if (rank == 0) {
#ifdef HDF5_SUPPORT
        /* initialization for hdf5 file for preserve option */
        if (preserve) {
            hdf5.file = H5Fopen(da->daos_preserve_path, H5F_ACC_RDONLY, H5P_DEFAULT);
            if (hdf5.file < 0) {
                MFU_LOG(MFU_LOG_ERR, "failed to open hdf5 file");
                rc = 1;
                goto bcast;
            }
        }
#endif
        /* Connect to DAOS pool */
        if (connect_pool) {
            daos_pool_info_t pool_info = {0};
#if DAOS_API_VERSION_MAJOR < 1
            rc = daos_pool_connect(*pool, NULL, NULL, DAOS_PC_RW, poh, &pool_info, NULL);
#else
            rc = daos_pool_connect(*pool, NULL, DAOS_PC_RW, poh, &pool_info, NULL);
#endif
            if (rc != 0) {
                MFU_LOG(MFU_LOG_ERR, "Failed to connect to pool: "DF_RC, DP_RC(rc));
                goto bcast;
            }
        }

        /* Try to open the container
         * If NOEXIST we create it */
        daos_cont_info_t co_info = {0};
        if (!dst_cont_passed && create_cont) {
            /* Use uuid if container was created by mpifileutils.
             * If nothing is passed in for destination a uuid is always generated
             * unless user passed one in, because destination container labels are
             * not generated */
            rc = daos_cont_open(*poh, da->dst_cont_uuid, DAOS_COO_RW, coh, &co_info, NULL);
        } else {
            rc = daos_cont_open(*poh, *cont, DAOS_COO_RW, coh, &co_info, NULL);
        }
        if (rc != 0) {
            if (rc != -DER_NONEXIST || !create_cont) {
                MFU_LOG(MFU_LOG_ERR, "Failed to open container: "DF_RC, DP_RC(rc));
                goto bcast;
            }

            bool have_src_cont = daos_handle_is_valid(da->src_coh);

            /* Get the src container properties. */
            if (have_src_cont) {
                if ((mfu_src_file != NULL) && (mfu_src_file->type == DFS)) {
                    /* Don't get the allocated OID */
                    rc = cont_get_props(da->src_coh, &props, false, false, false);
                } else {
                    rc = cont_get_props(da->src_coh, &props, true, false, true);
                }
                if (rc != 0) {
                    goto bcast;
                }
            }

            /* if a destination container string was passed in we need to
             * check if it is a uuid or cont label string. This is necessary
             * because a user can generate a uuid then pass it as destination,
             * which should use uuid to create/open the container */
            bool is_uuid;
            if (dst_cont_passed) {
                rc = uuid_parse(*cont, da->dst_cont_uuid); 
                if (rc == 0) {
                    is_uuid = true;
                } else {
                    is_uuid = false;
                }
            }

            /* Create a new container. If we don't have a source container,
             * create a POSIX container. Otherwise, create a container of
             * the same type as the source. */
            if (!have_src_cont || (da->src_cont_type == DAOS_PROP_CO_LAYOUT_POSIX)) {
#ifdef HDF5_SUPPORT
                /* read container properties in hdf5 file when moving data
                 * back to DAOS if the preserve option has been set */
                if (preserve) {
                    daos_cont_layout_t ctype = DAOS_PROP_CO_LAYOUT_POSIX;
                    MFU_LOG(MFU_LOG_INFO, "Reading metadata file: %s", da->daos_preserve_path);
                    rc = cont_deserialize_all_props(&hdf5, &props, &roots, &ctype, *poh);
                    if (rc != 0) {
                        MFU_LOG(MFU_LOG_ERR, "Failed to read cont props: "DF_RC, DP_RC(rc));
                        goto bcast;
                    }
                }
#endif
                dfs_attr_t attr = {0};
                attr.da_props = props;
                if (dst_cont_passed && !is_uuid) {
                    rc = dfs_cont_create_with_label(*poh, *cont, &attr, &da->dst_cont_uuid, NULL, NULL);
                } else {
                    /* if nothing is passed in for destination a uuid is always
                     * generated unless user passed one in, destination container
                     * labels are not generated */
                    rc = dfs_cont_create(*poh, da->dst_cont_uuid, &attr, NULL, NULL);
                }
                if (rc != 0) {
                    MFU_LOG(MFU_LOG_ERR, "Failed to create container: (%d %s)", rc, strerror(rc));
                    goto bcast;
                }
            } else {
                if (dst_cont_passed && !is_uuid) {
                    rc = daos_cont_create_with_label(*poh, *cont, props, &da->dst_cont_uuid, NULL);
                } else {
                    rc = daos_cont_create(*poh, da->dst_cont_uuid, props, NULL);
                }
                if (rc != 0) {
                    MFU_LOG(MFU_LOG_ERR, "Failed to create container: "DF_RC, DP_RC(rc));
                    goto bcast;
                }
            }

            if (dst_cont_passed && !is_uuid) {
                MFU_LOG(MFU_LOG_INFO, "Successfully created container %s", *cont);
            } else {
                char uuid_str[130];
                uuid_unparse_lower(da->dst_cont_uuid, uuid_str);
                MFU_LOG(MFU_LOG_INFO, "Successfully created container %s", uuid_str);
            }

            /* try to open it again */
            if (dst_cont_passed) {
                if (is_uuid) {
                    rc = daos_cont_open(*poh, da->dst_cont_uuid, DAOS_COO_RW, coh, &co_info, NULL);
                } else {
                    rc = daos_cont_open(*poh, *cont, DAOS_COO_RW, coh, &co_info, NULL);
                }
            } else {
                rc = daos_cont_open(*poh, da->dst_cont_uuid, DAOS_COO_RW, coh, &co_info, NULL);
            }
            if (rc != 0) {
                MFU_LOG(MFU_LOG_ERR, "Failed to open container: "DF_RC, DP_RC(rc));
                goto bcast;
            }

#ifdef HDF5_SUPPORT
            /* need to wait until destination container is valid to set user
             * attributes, preserve is only set true when destination is a DFS
             * copy */
            if (preserve) { 
                /* deserialize and set the user attributes if they exist */
                htri_t usr_attrs_exist = H5Lexists(hdf5.file, "User Attributes", H5P_DEFAULT);
                if (usr_attrs_exist > 0) {
                    rc = cont_deserialize_usr_attrs(&hdf5, *coh);
                    if (rc != 0) {
                        goto bcast;
                    }
                }
            }
#endif

            /* Copy user attributes from source container */
            if (have_src_cont) {
                rc = copy_usr_attrs(da->src_coh, *coh);
                if (rc != 0) {
                    goto bcast;
                }
            }
        } else if (require_new_cont) {
            /* We successfully opened the container, but it should not exist */
            MFU_LOG(MFU_LOG_ERR, "Destination container already exists");
            goto bcast;
        }

        /* check container status, and if unhealthy do not continue unless --force
         * option is used (only for serialization, never for copies) */
        bool status_healthy;
        rc = daos_check_cont_status(*coh, &status_healthy);
        if (rc != 0) {
            MFU_LOG(MFU_LOG_ERR, "Checking DAOS container status failed\n");
            goto bcast;
        } else if (!status_healthy && !force_serialize) {
            MFU_LOG(MFU_LOG_ERR, "Container status is unhealthy, stopping\n");
            goto bcast;
        }

        /* everything looks good so far */
        valid = 1;
    }
bcast:
    if (rank == 0) {
        daos_prop_free(props);
        mfu_free(&roots);
#ifdef HDF5_SUPPORT
        if (preserve) {
            /* only close if handle is open */
            if (hdf5.file > 0) {
                H5Fclose(hdf5.file);
            }
        }
#endif
    }

    /* broadcast valid from rank 0 */
    MPI_Bcast(&valid, 1, MPI_INT, 0, MPI_COMM_WORLD);

    /* return if invalid */
    if (valid == 0) {
        return -1;
    }

    /* broadcast pool handle from rank 0
     * If connect_pool is false, then the handle was unchanged */
    if (connect_pool) {
        daos_bcast_handle(rank, poh, poh, POOL_HANDLE);
    }

    /* broadcast container handle from rank 0 */
    daos_bcast_handle(rank, coh, poh, CONT_HANDLE);
    return 0;
}

/* Mount DAOS dfs */
static int mfu_dfs_mount(
  mfu_file_t* mfu_file,
  daos_handle_t* poh,
  daos_handle_t* coh)
{
    /* Mount dfs_sys */
    int rc = dfs_sys_mount(*poh, *coh, O_RDWR, DFS_SYS_NO_LOCK, &mfu_file->dfs_sys);
    if (rc !=0) {
        MFU_LOG(MFU_LOG_ERR, "Failed to mount DAOS filesystem (DFS): %s", strerror(rc));
        rc = -1;
        goto out;
    }

    /* Get underlying DFS base for DFS API calls */
    rc = dfs_sys2base(mfu_file->dfs_sys, &mfu_file->dfs);
    if (rc != 0) {
        MFU_LOG(MFU_LOG_ERR, "Failed to get DAOS filesystem (DFS) base: %s", strerror(rc));
        rc = -1;
        dfs_sys_umount(mfu_file->dfs_sys);
        mfu_file->dfs_sys = NULL;
    }

out:
    return rc;
}

/* Unmount DAOS dfs */
static int mfu_dfs_umount(
  mfu_file_t* mfu_file)
{
    if ((mfu_file == NULL) || (mfu_file->dfs_sys == NULL)) {
        return 0;
    }

    /* Unmount dfs_sys */
    int rc = dfs_sys_umount(mfu_file->dfs_sys);
    if (rc != 0) {
        MFU_LOG(MFU_LOG_ERR, "Failed to unmount DAOS filesystem (DFS): %s", strerror(rc));
        rc = -1;
    }
    mfu_file->dfs_sys = NULL;

    return rc;
}

static inline int mfu_daos_destroy_snap(daos_handle_t coh, daos_epoch_t epoch)
{
    daos_epoch_range_t epr;
    epr.epr_lo = epoch;
    epr.epr_hi = epoch;

    int rc = daos_cont_destroy_snap(coh, epr, NULL);
    if (rc != 0) {
        MFU_LOG(MFU_LOG_ERR, "DAOS destroy snapshot failed: "DF_RC, DP_RC(rc));
    }

    return rc;
}

/* 
 * Public definitions.
 */

daos_args_t* daos_args_new(void)
{
    daos_args_t* da = (daos_args_t*) MFU_MALLOC(sizeof(daos_args_t));

    da->src_poh    = DAOS_HDL_INVAL;
    da->dst_poh    = DAOS_HDL_INVAL;
    da->src_coh    = DAOS_HDL_INVAL;
    da->dst_coh    = DAOS_HDL_INVAL;
    da->dfs_prefix = NULL;
    da->src_path   = NULL;
    da->dst_path   = NULL;

    memset(da->src_pool, '\0', DAOS_PROP_LABEL_MAX_LEN + 1);
    memset(da->src_cont, '\0', DAOS_PROP_LABEL_MAX_LEN + 1);
    memset(da->dst_pool, '\0', DAOS_PROP_LABEL_MAX_LEN + 1);
    memset(da->dst_cont, '\0', DAOS_PROP_LABEL_MAX_LEN + 1);

    /* By default, try to automatically determine the API */
    /* By default, try to automatically determine the API */
    da->api = DAOS_API_AUTO;

    /* Default to 0 for "no epoch" */
    da->src_epc = 0;
    da->dst_epc = 0;

    /* Default does not allow the destination container to exist for DAOS API */
    da->allow_exist_dst_cont = false;

    da->src_cont_type = DAOS_PROP_CO_LAYOUT_UNKOWN;
    da->dst_cont_type = DAOS_PROP_CO_LAYOUT_UNKOWN;

    /* by default do not preserve daos metadata */
    da->daos_preserve = false;
    da->daos_preserve_path = NULL;

    return da;
}

void daos_args_delete(daos_args_t** pda)
{
    if (pda != NULL) {
        daos_args_t* da = *pda;
        mfu_free(&da->dfs_prefix);
        mfu_free(&da->src_path);
        mfu_free(&da->dst_path);
        mfu_free(&da->daos_preserve_path);
        mfu_free(pda);
    }
}

int daos_parse_api_str(
    const char* api_str,
    daos_api_t* api)
{
    int rc = 0;

    if (strcasecmp(api_str, "AUTO") == 0) {
        *api = DAOS_API_AUTO;
    } else if (strcasecmp(api_str, "DFS") == 0) {
        *api = DAOS_API_DFS;
    } else if (strcasecmp(api_str, "DAOS") == 0) {
        *api = DAOS_API_DAOS;
    } else if (strcasecmp(api_str, "HDF5") == 0) {
        *api = DAOS_API_HDF5;
    } else {
        rc = 1;
    }

    return rc;
}

int daos_parse_epc_str(
    const char* epc_str,
    daos_epoch_t* epc)
{
    *epc = strtoull(epc_str, NULL, 10);
    if (*epc == 0 || (*epc == ULLONG_MAX && errno != 0)) {
        return 1;
    }

    return 0;
}

#ifdef HDF5_SUPPORT
static int serialize_daos_metadata(char *filename,
                                   daos_args_t* da)
{
    int    rc = 0;
    hid_t  status = 0;
    struct hdf5_args hdf5 = {0};

    MFU_LOG(MFU_LOG_INFO, "Writing metadata file: %s", da->daos_preserve_path);

    /* TODO: much of this HDF5 setup should get moved into the HDF5 user
     * interface */
    hdf5.file = H5Fcreate(da->daos_preserve_path, H5F_ACC_TRUNC, H5P_DEFAULT, H5P_DEFAULT);
    if (hdf5.file < 0) {
        rc = 1;
        MFU_LOG(MFU_LOG_ERR, "failed to create hdf5 file");
        goto out;
    }
    rc = cont_serialize_props(&hdf5, da->src_coh);
    if (rc != 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to serialize cont layout: "DF_RC, DP_RC(rc));
        goto out;
    }

    /* create User Attributes Dataset in daos_metadata file */
    hdf5.usr_attr_memtype = H5Tcreate(H5T_COMPOUND, sizeof(usr_attr_t));
    if (hdf5.usr_attr_memtype < 0) {
        rc = 1;
        MFU_LOG(MFU_LOG_ERR, "failed to create user attr memtype");
        goto out;
    }
    hdf5.usr_attr_name_vtype = H5Tcopy(H5T_C_S1);
    if (hdf5.usr_attr_name_vtype < 0) {
        rc = 1;
        MFU_LOG(MFU_LOG_ERR, "failed to create user attr name vtype");
        goto out;
    }
    status = H5Tset_size(hdf5.usr_attr_name_vtype, H5T_VARIABLE);
    if (status < 0) {
        rc = 1;
        MFU_LOG(MFU_LOG_ERR, "failed to create user attr name vtype");
        goto out;
    }
    hdf5.usr_attr_val_vtype = H5Tvlen_create(H5T_NATIVE_OPAQUE);
    if (hdf5.usr_attr_val_vtype < 0) {
        rc = 1;
        MFU_LOG(MFU_LOG_ERR, "failed to create user attr val vtype");
        goto out;
    }
    status = H5Tinsert(hdf5.usr_attr_memtype, "Attribute Name",
                       HOFFSET(usr_attr_t, attr_name), hdf5.usr_attr_name_vtype);
    if (status < 0) {
        rc = 1;
        MFU_LOG(MFU_LOG_ERR, "failed to insert user attr name");
        goto out;
    }
    status = H5Tinsert(hdf5.usr_attr_memtype, "Attribute Value",
                       HOFFSET(usr_attr_t, attr_val), hdf5.usr_attr_val_vtype);
    if (status < 0) {
        rc = 1;
        MFU_LOG(MFU_LOG_ERR, "failed to insert user attr val");
        goto out;
    }

    /* serialize the DAOS user attributes*/
    rc = cont_serialize_usr_attrs(&hdf5, da->src_coh);
    if (rc != 0) {
       MFU_LOG(MFU_LOG_ERR, "failed to serialize user attributes: "DF_RC, DP_RC(rc));
       goto out;
    }
    status = H5Tclose(hdf5.usr_attr_name_vtype);
    if (status < 0) {
       rc = 1;
       MFU_LOG(MFU_LOG_ERR, "failed to close user attr name datatype");
        
    }
    status = H5Tclose(hdf5.usr_attr_val_vtype);
    if (status < 0) {
       rc = 1;
       MFU_LOG(MFU_LOG_ERR, "failed to close user attr value datatype");
    }
out:
    mfu_free(&filename);
    return rc;
}
#endif

int daos_setup(
    int rank,
    char** argpaths,
    int numpaths,
    daos_args_t* da,
    mfu_file_t* mfu_src_file,
    mfu_file_t* mfu_dst_file)
{
    int tmp_rc;
    bool have_src = ((numpaths > 0) && (mfu_src_file != NULL));
    bool have_dst = ((numpaths > 1) && (mfu_dst_file != NULL));
    bool dst_cont_passed = false;

    /* Sanity check that we have paths */
    if (!have_src && !have_dst) {
        return 0;
    }

    /* Each process keeps track of whether it had any DAOS errors.
     * If there weren't any daos args, then ignore daos_init errors.
     * Then, perform a reduction and exit if any process errored. */
    bool local_daos_error = false;
    int flag_daos_args = 0;

    /* For now, track the error.
     * Later, ignore if no daos args supplied */
    tmp_rc = daos_init();
    if (tmp_rc != 0) {
        MFU_LOG(MFU_LOG_ERR, "Failed to initialize daos: "DF_RC, DP_RC(tmp_rc));
        local_daos_error = true;
    }

    /* Do a preliminary check on the DAOS args */
    if (!local_daos_error) {
        tmp_rc = daos_check_args(rank, argpaths, numpaths, da, &flag_daos_args);
        if (tmp_rc != 0) {
            MFU_LOG(MFU_LOG_ERR, "Invalid DAOS args: " MFU_ERRF, MFU_ERRP(-MFU_ERR_DAOS_INVAL_ARG));
            local_daos_error = true;
        }
    }

    /* Figure out if daos path is the src or dst,
     * using UNS path, then chop off UNS path
     * prefix since the path is mapped to the root
     * of the container in the DAOS DFS mount */
    if (!local_daos_error) {
        tmp_rc = daos_set_paths(rank, argpaths, numpaths, da, &dst_cont_passed);
        if (tmp_rc != 0) {
            MFU_LOG(MFU_LOG_ERR, "Invalid DAOS args: " MFU_ERRF, MFU_ERRP(-MFU_ERR_DAOS_INVAL_ARG));
            local_daos_error = true;
        }
    }

    /* Re-check the required DAOS arguments (if any) */
    if (!local_daos_error) {
        tmp_rc = daos_check_args(rank, argpaths, numpaths, da, &flag_daos_args);
        if (tmp_rc != 0) {
            MFU_LOG(MFU_LOG_ERR, "Invalid DAOS args: " MFU_ERRF, MFU_ERRP(-MFU_ERR_DAOS_INVAL_ARG));
            local_daos_error = true;
        }
    }

    /* Make sure there weren't any errors before continuing.
     * Since daos_connect has a collective broadcast.
     * we have to make sure same_pool below is valid. */
    if (daos_any_error(rank, local_daos_error, flag_daos_args)) {
        tmp_rc = daos_fini();
        return 1;
    }

    /* At this point, we don't have any errors */
    local_daos_error = false;

    /* Check whether we have pool/cont uuids */
    bool have_src_pool  = strlen(da->src_pool) > 0 ? true : false;
    bool have_src_cont  = strlen(da->src_cont) > 0 ? true : false;
    bool have_dst_pool  = strlen(da->dst_pool) > 0 ? true : false;

    /* Check if containers are in the same pool */
    bool same_pool = (strcmp(da->src_pool, da->dst_pool) == 0);

    bool connect_pool = true;
    bool create_cont = false;
    bool require_new_cont = false;
    bool preserve = false;

    /* connect to DAOS source pool if uuid is valid */
    if (have_src_cont) {
        /* Sanity check */
        if (!have_src_pool) {
            if (rank == 0) {
                MFU_LOG(MFU_LOG_ERR, "Source container requires source pool");
            }
            local_daos_error = true;
            goto out;
        }
        tmp_rc = daos_connect(rank, da, &da->src_pool, &da->src_cont,
                              &da->src_poh, &da->src_coh, false,
                              connect_pool, create_cont, require_new_cont,
                              preserve, mfu_src_file, dst_cont_passed);
        if (tmp_rc != 0) {
            /* tmp_rc from daos_connect is collective */
            local_daos_error = true;
            goto out;
        }
        /* Get the container type */
        tmp_rc = daos_get_cont_type(da->src_coh, &da->src_cont_type);
        if (tmp_rc != 0) {
            /* ideally, this should be the same for each process */
            local_daos_error = true;
            goto out;
        }
        /* Set the src api based on the container type */
        tmp_rc = daos_set_api_cont_type(mfu_src_file, da->src_cont_type, da->api);
        if (tmp_rc != 0) {
            local_daos_error = true;
            goto out;
        }
        /* Sanity check before we create a new container */
        if (da->src_cont_type != DAOS_PROP_CO_LAYOUT_POSIX) {
            if (strcmp(da->src_path, "/") != 0) {
                if (rank == 0) {
                    MFU_LOG(MFU_LOG_ERR, "Cannot use path with non-POSIX container.");
                }
                local_daos_error = true;
                goto out;
            }
        }
    }

    /* If we're using the DAOS API, the destination container cannot
     * exist already, unless overriden by allow_exist_dst_cont. */
    if (mfu_src_file->type != POSIX && mfu_src_file->type != DFS && !da->allow_exist_dst_cont) {
        require_new_cont = true;
    }

    /* If the source and destination are in the same pool,
     * then open the container in that pool.
     * Otherwise, connect to the second pool and open the container */
    if (have_dst_pool) {
        /* Sanity check before we create a new container */
        if (require_new_cont && da->api == DAOS_API_DAOS) {
            if (strcmp(da->dst_path, "/") != 0) {
                if (rank == 0) {
                    MFU_LOG(MFU_LOG_ERR, "Cannot use path with non-POSIX container.");
                }
                local_daos_error = true;
                goto out;
            }
        }

        /* TODO: pass daos_preserve bool here to daos_connect=true */
        create_cont = true;
        /* do check that src is POSIX and since dst has a pool,
         * then the dst *should* always be DFS, but this is not set
         * on the destintaion until after daos_connect is called, and
         * we should read the properties *before* the container is
         * created. We do not have the destination container type
         * here yet which is why extra check is used then passed
         * to daos_connect */
        if (da->daos_preserve) {
            if (mfu_src_file->type == POSIX) {
                preserve = true; 
            } else {
                local_daos_error = true;
                MFU_LOG(MFU_LOG_ERR, "Cannot use daos-preserve if source and destination"
                                     " are DAOS. DAOS metadata is copied by default "
                                     "when both the source and destination is DAOS");
                goto out;
            }
        }
        if (same_pool) {
            connect_pool = false;
            tmp_rc = daos_connect(rank, da, &da->dst_pool, &da->dst_cont,
                                  &da->src_poh, &da->dst_coh, false,
                                  connect_pool, create_cont, require_new_cont,
                                  preserve, mfu_src_file, dst_cont_passed);
        } else {
            connect_pool = true;
            tmp_rc = daos_connect(rank, da, &da->dst_pool, &da->dst_cont,
                                  &da->dst_poh, &da->dst_coh, false,
                                  connect_pool, create_cont, require_new_cont,
                                  preserve, mfu_src_file, dst_cont_passed);
        }
        if (tmp_rc != 0) {
            /* tmp_rc from daos_connect is collective */
            local_daos_error = true;
            goto out;
        }
        /* Get the container type */
        tmp_rc = daos_get_cont_type(da->dst_coh, &da->dst_cont_type);
        if (tmp_rc != 0) {
            /* ideally, this should be the same for each process */
            local_daos_error = true;
            goto out;
        }
        if (have_dst) {
            /* Set the dst api based on the container type */
            tmp_rc = daos_set_api_cont_type(mfu_dst_file, da->dst_cont_type, da->api);
            if (tmp_rc != 0) {
                local_daos_error = true;
                goto out;
            }
        }
    }

    /* Figure out if we should use the DFS or DAOS API */
    tmp_rc = daos_set_api_compat(mfu_src_file, mfu_dst_file, da, argpaths);
    if (tmp_rc != 0) {
        local_daos_error = true;
        goto out;
    }

    /* Mount source DFS container */
    if (mfu_src_file->type == DFS) {
        tmp_rc = mfu_dfs_mount(mfu_src_file, &da->src_poh, &da->src_coh);
        if (tmp_rc != 0) {
            local_daos_error = true;
            goto out;
        }
    }

    if (have_dst) {
        /* Mount destination DFS container */
        if (mfu_dst_file->type == DFS) {
            if (same_pool) {
                tmp_rc = mfu_dfs_mount(mfu_dst_file, &da->src_poh, &da->dst_coh);
            } else {
                tmp_rc = mfu_dfs_mount(mfu_dst_file, &da->dst_poh, &da->dst_coh);
            }
            if (tmp_rc != 0) {
                local_daos_error = true;
                goto out;
            }
        }
    }

    /* check daos_preserve
     * if src cont is DFS and dst is POSIX, then write 
     * cont props and user attrs */
#ifdef HDF5_SUPPORT
    char *filename = NULL;
    if (da->daos_preserve && rank == 0) {
        if (mfu_src_file->type == DFS && mfu_dst_file->type == POSIX) {
            tmp_rc = serialize_daos_metadata(filename, da);
            if (tmp_rc != 0) {
                local_daos_error = true;
                MFU_LOG(MFU_LOG_ERR, "failed serialize DAOS metadata: "DF_RC,
                        DP_RC(tmp_rc));
            }
        }
    }
#endif
out:
    /* Return if any process had a daos error */
    if (daos_any_error(rank, local_daos_error, flag_daos_args)) {
        return 1;
    }

    /* Everything looks good so far */
    return 0;
}

int daos_cleanup(
    daos_args_t* da,
    mfu_file_t* mfu_src_file,
    mfu_file_t* mfu_dst_file)
{
    /* assume success until failure */
    int rc = 0;
    int tmp_rc;

    /* get our rank */
    int rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    bool same_pool = (strcmp(da->src_pool, da->dst_pool) == 0);

    /* Destroy source snapshot */
    if (rank == 0 && da->src_epc != 0) {
        tmp_rc = mfu_daos_destroy_snap(da->src_coh, da->src_epc);
        if (tmp_rc != 0) {
            rc = 1;
        }
    }

    /* Destroy destination snapshot */
    if (rank == 0 && da->dst_epc != 0) {
        tmp_rc = mfu_daos_destroy_snap(da->dst_coh, da->dst_epc);
        if (tmp_rc != 0) {
            rc = 1;
        }
    }

    /* Don't close containers until snapshots are destroyed */
    MPI_Barrier(MPI_COMM_WORLD);

    /* Unmount source DFS container */
    tmp_rc = mfu_dfs_umount(mfu_src_file);
    if (tmp_rc != 0) {
        rc = 1;
    }

    /* Unmount destination DFS container */
    tmp_rc = mfu_dfs_umount(mfu_dst_file);
    if (tmp_rc != 0) {
        rc = 1;
    }

    /* Wait for unmount */
    MPI_Barrier(MPI_COMM_WORLD);

    /* Close source container */
    if (daos_handle_is_valid(da->src_coh)) {
        tmp_rc = daos_cont_close(da->src_coh, NULL);
        if (tmp_rc != 0) {
            MFU_LOG(MFU_LOG_ERR, "Failed to close source container "DF_RC, DP_RC(tmp_rc));
            rc = 1;
        }
        da->src_coh = DAOS_HDL_INVAL;
    }

    /* Close destination container */
    if (daos_handle_is_valid(da->dst_coh)) {
        tmp_rc = daos_cont_close(da->dst_coh, NULL);
        if (tmp_rc != 0) {
            MFU_LOG(MFU_LOG_ERR, "Failed to close destination container "DF_RC, DP_RC(tmp_rc));
            rc = 1;
        }
        da->dst_coh = DAOS_HDL_INVAL;
    }

    /* Wait for container close */
    MPI_Barrier(MPI_COMM_WORLD);

    /* Close source pool */
    if (daos_handle_is_valid(da->src_poh)) {
        tmp_rc = daos_pool_disconnect(da->src_poh, NULL);
        if (tmp_rc != 0) {
            MFU_LOG(MFU_LOG_ERR, "Failed to disconnect from source pool "DF_RC, DP_RC(tmp_rc));
            rc = 1;
        }
        da->src_poh = DAOS_HDL_INVAL;
    }

    /* Close destination pool */
    if (daos_handle_is_valid(da->dst_poh) && !same_pool) {
        tmp_rc = daos_pool_disconnect(da->dst_poh, NULL);
        if (tmp_rc != 0) {
            MFU_LOG(MFU_LOG_ERR, "Failed to disconnect from destination pool "DF_RC, DP_RC(tmp_rc));
            rc = 1;
        }
        da->dst_poh = DAOS_HDL_INVAL;
    }

    /* Wait for pool close */
    MPI_Barrier(MPI_COMM_WORLD);

    /* Finalize DAOS */
    tmp_rc = daos_fini();
    if (tmp_rc != 0) {
        rc = 1;
    }

    /* Free DAOS args */
    daos_args_delete(&da);

    return rc;
}

/* Copy a single recx from a src obj to dst obj for a given dkey/akey.
 * returns -1 on error, 0 if same, 1 if different. */
static int mfu_daos_obj_sync_recx_single(
    daos_key_t *dkey,
    daos_handle_t *src_oh,
    daos_handle_t *dst_oh,
    daos_iod_t *iod,
    bool compare_dst,
    bool write_dst,
    mfu_daos_stats_t* stats)
{
    bool        dst_equal = false;  /* equal until found otherwise */
    uint64_t    src_buf_len = (*iod).iod_size;
    char        src_buf[src_buf_len];
    d_sg_list_t src_sgl;
    d_iov_t     src_iov;
    int         rc;

    /* set src_sgl values */
    src_sgl.sg_nr     = 1;
    src_sgl.sg_nr_out = 0;
    src_sgl.sg_iovs   = &src_iov;
    d_iov_set(&src_iov, src_buf, src_buf_len);

    /* Fetch the source */
    rc = daos_obj_fetch(*src_oh, DAOS_TX_NONE, 0, dkey, 1, iod, &src_sgl, NULL, NULL);
    if (rc != 0) {
        MFU_LOG(MFU_LOG_ERR, "DAOS object fetch returned with errors "DF_RC, DP_RC(rc));
        goto out_err;
    }
    stats->bytes_read += src_buf_len;

    /* Sanity check */
    if (src_sgl.sg_nr_out != 1) {
        MFU_LOG(MFU_LOG_ERR, "Failed to fetch single recx.");
        goto out_err;
    }

    /* Conditionally compare the destination before writing */
    if (compare_dst) {
        uint64_t    dst_buf_len = (*iod).iod_size;
        char        dst_buf[dst_buf_len];
        d_sg_list_t dst_sgl;
        d_iov_t     dst_iov;

        dst_sgl.sg_nr       = 1;
        dst_sgl.sg_nr_out   = 0;
        dst_sgl.sg_iovs     = &dst_iov;

        d_iov_set(&dst_iov, dst_buf, dst_buf_len);
        rc = daos_obj_fetch(*dst_oh, DAOS_TX_NONE, 0, dkey, 1, iod, &dst_sgl, NULL, NULL);
        if (rc != 0) {
            MFU_LOG(MFU_LOG_ERR, "DAOS object fetch returned with errors "DF_RC, DP_RC(rc));
            goto out_err;
        }
        /* Reset iod values after fetching the destination */
        (*iod).iod_nr = 1;
        (*iod).iod_size = src_buf_len;

        /* Determine whether the dst is equal to the src */
        if (dst_sgl.sg_nr_out > 0) {
            stats->bytes_read += dst_buf_len;
            dst_equal = (memcmp(src_buf, dst_buf, src_buf_len) == 0);
        }
    }

    /* Conditionally write to the destination */
    if (write_dst && !dst_equal) {
        rc = daos_obj_update(*dst_oh, DAOS_TX_NONE, 0, dkey, 1, iod, &src_sgl, NULL);
        if (rc != 0) {
            MFU_LOG(MFU_LOG_ERR, "DAOS object update returned with errors "DF_RC, DP_RC(rc));
            goto out_err;
        }
        stats->bytes_written += src_buf_len;
    }

    /* return 0 if equal, 1 if different */
    if (dst_equal) {
        return 0;
    }
    return 1;

out_err:
    /* return -1 on true error */
    return -1;
}

/* Free a buffer created with alloc_iov_buf */
static void free_iov_buf(
    uint32_t    number, /* buffer length */
    char**      buf)    /* pointer to static buffer */
{
    if (buf != NULL) {
        for (uint32_t i = 0; i < number; i++) {
            mfu_free(&buf[i]);
        }
    }
}

/* Create a buffer based on recxs and set iov for each index */
static int alloc_iov_buf(
    uint32_t        number, /* number of recxs, iovs, and buffer */
    daos_size_t     size,   /* size of each record */
    daos_recx_t*    recxs,  /* array of recxs */
    d_iov_t*        iov,    /* array of iovs */
    char**          buf,    /* pointer to static buffer */
    uint64_t*       buf_len)/* pointer to static buffer lengths */
{
    for (uint32_t i = 0; i < number; i++) {
        buf_len[i] = recxs[i].rx_nr * size;
        buf[i] = calloc(buf_len[i], sizeof(void*));
        if (buf[i] == NULL) {
            free_iov_buf(number, buf);
            return -1;
        }
        d_iov_set(&iov[i], buf[i], buf_len[i]);
    }

    return 0;
}

/* Sum an array of uint64_t */
static uint64_t sum_uint64_t(uint32_t number, uint64_t* buf)
{
    uint64_t sum = 0;
    for (uint32_t i = 0; i < number; i++) {
        sum += buf[i];
    }
    return sum;
}

/* Copy all array recx from a src obj to dst obj for a given dkey/akey.
 * returns -1 on error, 0 if same, 1 if different */
static int mfu_daos_obj_sync_recx_array(
    daos_key_t *dkey,
    daos_key_t *akey,
    daos_handle_t *src_oh,
    daos_handle_t *dst_oh,
    daos_iod_t *iod,
    bool compare_dst,
    bool write_dst,
    mfu_daos_stats_t* stats)
{
    bool        all_dst_equal = true;   /* equal until found otherwise */
    uint32_t    max_number = 5;         /* max recxs per fetch */
    char*       src_buf[max_number];    /* src buffer data */
    uint64_t    src_buf_len[max_number];/* src buffer lengths */
    d_sg_list_t src_sgl;
    daos_recx_t recxs[max_number];
    daos_size_t size;
    daos_epoch_range_t eprs[max_number];

    daos_anchor_t recx_anchor = {0};
    int rc;
    while (!daos_anchor_is_eof(&recx_anchor)) {
        /* list all recx for this dkey/akey */
        uint32_t number = max_number;
        rc = daos_obj_list_recx(*src_oh, DAOS_TX_NONE, dkey, akey,
                                &size, &number, recxs, eprs,
                                &recx_anchor, true, NULL);
        if (rc != 0) {
            MFU_LOG(MFU_LOG_ERR, "DAOS daos_obj_list_recx returned with errors "DF_RC, DP_RC(rc));
            goto out_err;
        }

        /* if no recx is returned for this dkey/akey move on */
        if (number == 0) 
            continue;

        d_iov_t src_iov[number];

        /* set iod values */
        (*iod).iod_type  = DAOS_IOD_ARRAY;
        (*iod).iod_nr    = number;
        (*iod).iod_recxs = recxs;
        (*iod).iod_size  = size;

        /* set src_sgl values */
        src_sgl.sg_nr_out = 0;
        src_sgl.sg_iovs   = src_iov;
        src_sgl.sg_nr     = number;

        /* allocate and setup src_buf */
        if (alloc_iov_buf(number, size, recxs, src_iov, src_buf, src_buf_len) != 0) {
            MFU_LOG(MFU_LOG_ERR, "DAOS failed to allocate source buffer.");
            goto out_err;
        }

        bool recx_equal = false;
        uint64_t total_bytes = sum_uint64_t(number, src_buf_len);

        /* fetch recx values from source */
        rc = daos_obj_fetch(*src_oh, DAOS_TX_NONE, 0, dkey, 1, iod,
                            &src_sgl, NULL, NULL);
        if (rc != 0) {
            MFU_LOG(MFU_LOG_ERR, "DAOS object fetch returned with errors "DF_RC, DP_RC(rc));
            goto out_err;
        }

        /* Sanity check */
        if (src_sgl.sg_nr_out != number) {
            MFU_LOG(MFU_LOG_ERR, "Failed to fetch array recxs.");
            goto out_err;
        }

        stats->bytes_read += total_bytes;

        /* Conditionally compare the destination before writing */
        if (compare_dst) {
            char*       dst_buf[number];
            uint64_t    dst_buf_len[number];
            d_sg_list_t dst_sgl;
            d_iov_t     dst_iov[number];

            dst_sgl.sg_nr_out = 0;
            dst_sgl.sg_iovs   = dst_iov;
            dst_sgl.sg_nr     = number;

            /* allocate and setup dst_buf */
            if (alloc_iov_buf(number, size, recxs, dst_iov, dst_buf, dst_buf_len) != 0) {
                MFU_LOG(MFU_LOG_ERR, "DAOS failed to allocate destination buffer.");
                goto out_err;
            }

            rc = daos_obj_fetch(*dst_oh, DAOS_TX_NONE, 0, dkey, 1, iod,
                                &dst_sgl, NULL, NULL);
            if (rc != 0) {
                MFU_LOG(MFU_LOG_ERR, "DAOS object fetch returned with errors "DF_RC, DP_RC(rc));
                free_iov_buf(number, dst_buf);
                goto out_err;
            }

            /* Reset iod values after fetching the destination */
            (*iod).iod_nr    = number;
            (*iod).iod_size  = size;

            /* Determine whether all recxs in the dst are equal to the src.
             * If any recx is different, update all recxs in dst and flag
             * this akey as different. */
            if (dst_sgl.sg_nr_out > 0) {
                stats->bytes_read += total_bytes;
                recx_equal = true;
                for (uint32_t i = 0; i < number; i++) {
                    if (memcmp(src_buf[i], dst_buf[i], src_buf_len[i]) != 0) {
                        recx_equal = false;
                        all_dst_equal = false;
                        break;
                    }
                }
            }
            free_iov_buf(number, dst_buf);
        }

        /* Conditionally write to the destination */
        if (write_dst && !recx_equal) {
            rc = daos_obj_update(*dst_oh, DAOS_TX_NONE, 0, dkey, 1, iod,
                                 &src_sgl, NULL);
            if (rc != 0) {
                MFU_LOG(MFU_LOG_ERR, "DAOS object update returned with errors "DF_RC, DP_RC(rc));
                goto out_err;
            }
            stats->bytes_written += total_bytes;
        }
        free_iov_buf(number, src_buf);
    }

    /* return 0 if equal, 1 if different */
    if (all_dst_equal) {
        return 0;
    }
    return 1;

out_err:
    /* return -1 on true errors */
    return -1;
}

/* Copy all dkeys and akeys from a src obj to dst obj.
 * returns -1 on error, 0 if same, 1 if different */
static int mfu_daos_obj_sync_keys(
  daos_handle_t* src_oh,
  daos_handle_t* dst_oh,
  bool compare_dst,
  bool write_dst,
  mfu_daos_stats_t* stats)
{
    /* Assume src and dst are equal until found otherwise */
    bool all_dst_equal = true;

    /* loop to enumerate dkeys */
    daos_anchor_t dkey_anchor = {0}; 
    int rc;
    while (!daos_anchor_is_eof(&dkey_anchor)) {
        d_sg_list_t     dkey_sgl = {0};
        d_iov_t         dkey_iov = {0};
        daos_key_desc_t dkey_kds[ENUM_DESC_NR]       = {0};
        uint32_t        dkey_number                  = ENUM_DESC_NR;
        char            dkey_enum_buf[ENUM_DESC_BUF] = {0};
        char            dkey[ENUM_KEY_BUF]           = {0};

        dkey_sgl.sg_nr     = 1;
        dkey_sgl.sg_nr_out = 0;
        dkey_sgl.sg_iovs   = &dkey_iov;

        d_iov_set(&dkey_iov, dkey_enum_buf, ENUM_DESC_BUF);

        /* get dkeys */
        rc = daos_obj_list_dkey(*src_oh, DAOS_TX_NONE, &dkey_number, dkey_kds,
                                &dkey_sgl, &dkey_anchor, NULL);
        if (rc != 0) {
            MFU_LOG(MFU_LOG_ERR, "DAOS daos_obj_list_dkey returned with errors "DF_RC, DP_RC(rc));
            goto out_err;
        }

        /* if no dkeys were returned move on */
        if (dkey_number == 0)
            continue;

        char* dkey_ptr;
        int   i;

        /* parse out individual dkeys based on key length and number of dkeys returned */
        for (dkey_ptr = dkey_enum_buf, i = 0; i < dkey_number; i++) {

            /* Print enumerated dkeys */
            daos_key_t diov;
            memcpy(dkey, dkey_ptr, dkey_kds[i].kd_key_len);
            d_iov_set(&diov, (void*)dkey, dkey_kds[i].kd_key_len);
            dkey_ptr += dkey_kds[i].kd_key_len;

            /* loop to enumerate akeys */
            daos_anchor_t akey_anchor = {0}; 
            while (!daos_anchor_is_eof(&akey_anchor)) {
                d_sg_list_t     akey_sgl = {0};
                d_iov_t         akey_iov = {0};
                daos_key_desc_t akey_kds[ENUM_DESC_NR]       = {0};
                uint32_t        akey_number                  = ENUM_DESC_NR;
                char            akey_enum_buf[ENUM_DESC_BUF] = {0};
                char            akey[ENUM_KEY_BUF]           = {0};

                akey_sgl.sg_nr     = 1;
                akey_sgl.sg_nr_out = 0;
                akey_sgl.sg_iovs   = &akey_iov;

                d_iov_set(&akey_iov, akey_enum_buf, ENUM_DESC_BUF);

                /* get akeys */
                rc = daos_obj_list_akey(*src_oh, DAOS_TX_NONE, &diov, &akey_number, akey_kds,
                                        &akey_sgl, &akey_anchor, NULL);
                if (rc != 0) {
                    MFU_LOG(MFU_LOG_ERR, "DAOS daos_obj_list_akey returned with errors "DF_RC, DP_RC(rc));
                    goto out_err;
                }

                /* if no akeys returned move on */
                if (akey_number == 0)
                    continue;

                int j;
                char* akey_ptr;

                /* parse out individual akeys based on key length and number of dkeys returned */
                for (akey_ptr = akey_enum_buf, j = 0; j < akey_number; j++) {
                    daos_key_t aiov = {0};
                    daos_iod_t iod = {0};
                    memcpy(akey, akey_ptr, akey_kds[j].kd_key_len);
                    d_iov_set(&aiov, (void*)akey, akey_kds[j].kd_key_len);

                    /* set iod values */
                    iod.iod_nr    = 1;
                    iod.iod_type  = DAOS_IOD_SINGLE;
                    iod.iod_size  = DAOS_REC_ANY;
                    iod.iod_recxs = NULL;
                    iod.iod_name  = aiov;

                    /* Do a fetch (with NULL sgl) of single value type, and if that
                     * returns iod_size == 0, then a single value does not exist. */
                    rc = daos_obj_fetch(*src_oh, DAOS_TX_NONE, 0, &diov, 1, &iod, NULL, NULL, NULL);
                    if (rc != 0) {
                        MFU_LOG(MFU_LOG_ERR, "DAOS daos_obj_fetch returned with errors "DF_RC, DP_RC(rc));
                        goto out_err;
                    }

                    /* if iod_size == 0 then this is a DAOS_IOD_ARRAY type */
                    if ((int)iod.iod_size == 0) {
                        rc = mfu_daos_obj_sync_recx_array(&diov, &aiov, src_oh, dst_oh,
                                                          &iod, compare_dst, write_dst, stats);
                        if (rc == -1) {
                            MFU_LOG(MFU_LOG_ERR, "DAOS mfu_daos_obj_sync_recx_array returned with errors: "
                                    MFU_ERRF, MFU_ERRP(-MFU_ERR_DAOS));
                            goto out_err;
                        } else if (rc == 1) {
                            all_dst_equal = false;
                        }
                    } else {
                        rc = mfu_daos_obj_sync_recx_single(&diov, src_oh, dst_oh,
                                                           &iod, compare_dst, write_dst, stats);
                        if (rc == -1) {
                            MFU_LOG(MFU_LOG_ERR, "DAOS mfu_daos_obj_sync_recx_single returned with errors: "
                                    MFU_ERRF, MFU_ERRP(-MFU_ERR_DAOS));
                            goto out_err;
                        } else if (rc == 1) {
                            all_dst_equal = false;
                        }
                    }

                    /* Increment akeys traversed */
                    stats->total_akeys++;

                    /* advance to next akey returned */
                    akey_ptr += akey_kds[j].kd_key_len;
                }
            }

            /* Increment dkeys traversed */
            stats->total_dkeys++;
        }
    }

    /* return 0 if equal, 1 if different */
    if (all_dst_equal) {
        return 0;
    }
    return 1;

out_err:
    /* return -1 on true errors */
    return -1;
}

static int mfu_daos_obj_sync(
    daos_args_t* da,
    daos_handle_t src_coh,
    daos_handle_t dst_coh,
    daos_obj_id_t oid,
    bool compare_dst,             /* Whether to compare the src and dst before writing */
    bool write_dst,               /* Whether to write to the dst */
    mfu_daos_stats_t* stats)
{
    int rc = 0;

    /* open handle of object in src container */
    daos_handle_t src_oh;
    rc = daos_obj_open(src_coh, oid, DAOS_OO_RO, &src_oh, NULL);
    if (rc != 0) {
        MFU_LOG(MFU_LOG_ERR, "DAOS object open returned with errors: " MFU_ERRF,
                MFU_ERRP(-MFU_ERR_DAOS));
        goto out_err;
    }

    /* open handle of object in dst container */
    daos_handle_t dst_oh;
    rc = daos_obj_open(dst_coh, oid, DAOS_OO_EXCL, &dst_oh, NULL);
    if (rc != 0) {
        MFU_LOG(MFU_LOG_ERR, "DAOS object open returned with errors: " MFU_ERRF,
                MFU_ERRP(-MFU_ERR_DAOS));
        /* make sure to close the source if opening dst fails */
        daos_obj_close(src_oh, NULL);
        goto out_err;
    }
    int copy_rc = mfu_daos_obj_sync_keys(&src_oh, &dst_oh, compare_dst, write_dst, stats);
    if (copy_rc == -1) {
        MFU_LOG(MFU_LOG_ERR, "DAOS copy list keys returned with errors: " MFU_ERRF,
                MFU_ERRP(-MFU_ERR_DAOS));
        /* cleanup object handles on failure */
        daos_obj_close(src_oh, NULL);
        daos_obj_close(dst_oh, NULL);
        goto out_err;
    }

    stats->total_oids++;

    /* close source and destination object */
    daos_obj_close(src_oh, NULL);
    daos_obj_close(dst_oh, NULL);

    return copy_rc;

out_err:
    /* return -1 on true error */
    return -1;
}

/* returns -1 on error, 0 if same, 1 if different */
static int mfu_daos_flist_obj_sync(
  daos_args_t* da,
  mfu_flist bflist,
  daos_handle_t src_coh,
  daos_handle_t dst_coh,
  bool compare_dst,
  bool write_dst,
  mfu_daos_stats_t* stats)
{
    int rc = 0;

    flist_t* flist = (flist_t*) bflist;

    uint64_t i;
    const elem_t* p = flist->list_head;
    for (i = 0; i < flist->list_count; i++) {
        daos_obj_id_t oid;
        oid.lo = p->obj_id_lo;
        oid.hi = p->obj_id_hi;

        /* Copy this object */
        rc = mfu_daos_obj_sync(da, src_coh, dst_coh, oid,
                               compare_dst, write_dst, stats);
        if (rc == -1) {
            MFU_LOG(MFU_LOG_ERR, "mfu_daos_obj_sync return with error");
            return rc;
        }

        p = p->next;
    }

    return rc;
}

/*
 * Create a snapshot and oit at the supplied epc.
 * Add an flist entry for each oid in the oit.
 */
static int mfu_daos_obj_list_oids(
    daos_args_t* da, mfu_flist bflist,
    daos_handle_t coh, daos_epoch_t epoch)
{
    daos_obj_id_t        oids[OID_ARR_SIZE];
    daos_anchor_t        anchor;
    uint32_t             oids_nr;
    daos_handle_t        toh = DAOS_HDL_INVAL;
    uint32_t             oids_total = 0;
    int                  rc = 0;

    /* open object iterator table */
    rc = daos_oit_open(coh, epoch, &toh, NULL);
    if (rc != 0) {
        MFU_LOG(MFU_LOG_ERR, "DAOS failed to open oit "DF_RC, DP_RC(rc));
        return 1;
    }
    memset(&oids, 0, OID_ARR_SIZE*sizeof(daos_obj_id_t));
    memset(&anchor, 0, sizeof(anchor));

    /* list and store all object ids in flist for this epoch */
    while (1) {
        oids_nr = OID_ARR_SIZE;
        rc = daos_oit_list(toh, oids, &oids_nr, &anchor, NULL);
        if (rc != 0) {
            MFU_LOG(MFU_LOG_ERR, "DAOS daos_oit_list returned with errors "DF_RC, DP_RC(rc));
            daos_oit_close(toh, NULL);
            return 1;
        }

        if (oids_nr < 1) {
            MFU_LOG(MFU_LOG_ERR, "No Objects Currently in Container, Exiting.");
            daos_oit_close(toh, NULL);
            return 1;
        }

        /* create element in flist for each obj id retrived */
        for (int i = 0; i < oids_nr; i++) {
            uint64_t idx = mfu_flist_file_create(bflist);
            mfu_flist_file_set_oid(bflist, idx, oids[i]);
            char* name = NULL;
            if (asprintf(&name, "%llu.%llu", oids[i].hi, oids[i].lo) == -1) {
                MFU_LOG(MFU_LOG_ERR, "Unable to allocate space for object.");
                daos_oit_close(toh, NULL);
                return 1;
            }
            mfu_flist_file_set_name(bflist, idx, name);
            mfu_free(&name);
        }
        oids_total = oids_nr + oids_total;
        if (daos_anchor_is_eof(&anchor)) {
            break;
        }
    }

    /* close object iterator */
    rc = daos_oit_close(toh, NULL);
    if (rc != 0) {
        MFU_LOG(MFU_LOG_ERR, "DAOS failed to close oit "DF_RC, DP_RC(rc));
        rc = 1;
    }
    return rc;
}

/* Walk objects in daos and insert to given flist.
 * Returns -1 on failure, 0 on success. */
int mfu_daos_flist_walk(
    daos_args_t* da,
    daos_handle_t coh,
    daos_epoch_t* epoch,
    mfu_flist flist)
{
    /* assume we'll succeed */
    int rc = 0;

    /* get our rank */
    int rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    /* have rank 0 do the work of listing the objects */
    if (rank == 0) {
        /* TODO give this some meaningful name? (arg 3) */
        rc = daos_cont_create_snap_opt(coh, epoch, NULL,
                                       DAOS_SNAP_OPT_CR | DAOS_SNAP_OPT_OIT,
                                       NULL);
        if (rc != 0) {
            MFU_LOG(MFU_LOG_ERR, "DAOS failed to create snapshot "DF_RC, DP_RC(rc));
            rc = -1;
            goto out_broadcast;
        }

        rc = mfu_daos_obj_list_oids(da, flist, coh, *epoch);
        if (rc != 0) {
            rc = -1;
            goto out_broadcast;
        }
    }

    /* summarize list since we added items to it */
    mfu_flist_summarize(flist);

out_broadcast:
    /* broadcast return code from rank 0 so everyone knows whether walk succeeded */
    MPI_Bcast(&rc, 1, MPI_INT, 0, MPI_COMM_WORLD);

    return rc;
}

/* Collectively copy/sync objects in flist to destination listed in daos args.
 * Copies DAOS data at object level (non-posix).
 * Returns 0 on success, 1 on failure. */
int mfu_daos_flist_sync(
    daos_args_t* da,    /* DAOS args */
    mfu_flist flist,    /* flist containing oids */
    bool compare_dst,   /* whether to compare the dst before writing */
    bool write_dst)     /* whether to actually write to the dst */
{
    /* get our rank */
    int rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    /* Initialize some stats */
    mfu_daos_stats_t stats;
    mfu_daos_stats_init(&stats);
    mfu_daos_stats_start(&stats);

    /* evenly spread the objects across all ranks */
    mfu_flist newflist = mfu_flist_spread(flist);

    /* copy object ids listed in newflist to destination in daos args */
    int rc = mfu_daos_flist_obj_sync(da, newflist, da->src_coh, da->dst_coh,
                                     compare_dst, write_dst, &stats);

    /* wait until all procs are done copying,
     * and determine whether everyone succeeded. */
    if (! mfu_alltrue(rc != -1, MPI_COMM_WORLD)) {
        /* someone failed, so return failure on all ranks */
        rc = 1;
        if (rank == 0) {
            MFU_LOG(MFU_LOG_ERR, "DAOS object copy failed: "
                    MFU_ERRF, MFU_ERRP(-MFU_ERR_DAOS));
        }
    } else {
        /* either rc == 0 or rc == 1.
         * both are success. */
        rc = 0;
    }

    /* Record end time */
    mfu_daos_stats_end(&stats);

    /* Sum and print the stats */
    mfu_daos_stats_print_sum(rank, &stats, true, true, true, true);

    mfu_flist_free(&newflist);

    return rc;
}

#ifdef HDF5_SUPPORT
static inline void init_hdf5_args(struct hdf5_args *hdf5)
{
    hdf5->status = 0;
    hdf5->file = -1;
    /* User attribute data */
    hdf5->usr_attr_memtype = 0;
    hdf5->usr_attr_name_vtype = 0;
    hdf5->usr_attr_val_vtype = 0;
    /* OID Data */
    hdf5->oid_memtype = 0;
    hdf5->oid_dspace = 0;
    hdf5->oid_dset = 0;
    /* DKEY Data */
    hdf5->dkey_memtype = 0;
    hdf5->dkey_vtype = 0;
    hdf5->dkey_dspace = 0;
    hdf5->dkey_dset = 0;
    /* AKEY Data */
    hdf5->akey_memtype = 0;
    hdf5->akey_vtype = 0;
    hdf5->akey_dspace = 0;
    hdf5->akey_dset = 0;
    /* dims for dsets */
    hdf5->oid_dims[0] = 0;
    hdf5->dkey_dims[0] = 0;     
    hdf5->akey_dims[0] = 0;     
    /* data for keys */
    hdf5->oid_data = NULL;
    hdf5->dkey_data = NULL;
    hdf5->akey_data = NULL;
    hdf5->oid = NULL;
    hdf5->dk = NULL;
    hdf5->ak = NULL;
}

static int serialize_kv_rec(struct hdf5_args *hdf5, 
                            daos_key_t dkey,
                            daos_handle_t *oh,
                            uint64_t *dk_index,
                            char *dkey_val,
                            mfu_daos_stats_t* stats)
{
    void        *buf;
    int         rc;
    hvl_t       *kv_val;
    daos_size_t size = 0;

    /* get the size of the value */
    rc = daos_kv_get(*oh, DAOS_TX_NONE, 0, dkey_val, &size, buf, NULL);
    if (rc != 0) {
        MFU_LOG(MFU_LOG_ERR, "Failed to fetch object "DF_RC, DP_RC(rc));
        goto out;
    }
    buf = MFU_CALLOC(1, size);
    if (buf == NULL) {
        rc = -DER_NOMEM;
        goto out;
    }

    rc = daos_kv_get(*oh, DAOS_TX_NONE, 0, dkey_val, &size, buf, NULL);
    if (rc != 0) {
        MFU_LOG(MFU_LOG_ERR, "Failed to fetch object "DF_RC, DP_RC(rc));
        goto out;
    }

    stats->bytes_read += size;
    kv_val = &(*hdf5->dk)[*dk_index].rec_kv_val;
    kv_val->p = MFU_CALLOC(1, (uint64_t)size);
    if (kv_val->p == NULL) {
        rc = -DER_NOMEM;
        goto out;
    }
    memcpy(kv_val->p, buf, (uint64_t)size);
    kv_val->len = (uint64_t)size; 
out:
    mfu_free(&buf);
    return rc;
}

static int serialize_recx_single(struct hdf5_args *hdf5, 
                                 daos_key_t *dkey,
                                 daos_handle_t *oh,
                                 daos_iod_t *iod,
                                 uint64_t *ak_index,
                                 mfu_daos_stats_t* stats)
{
    /* if iod_type is single value just fetch iod size from source
     * and update in destination object */
    int         buf_len = (int)(*iod).iod_size;
    void        *buf;
    d_sg_list_t sgl;
    d_iov_t     iov;
    int         rc;
    hvl_t       *single_val;

    buf = MFU_CALLOC(1, buf_len);

    /* set sgl values */
    sgl.sg_nr     = 1;
    sgl.sg_nr_out = 0;
    sgl.sg_iovs   = &iov;
    d_iov_set(&iov, buf, buf_len);
    rc = daos_obj_fetch(*oh, DAOS_TX_NONE, 0, dkey, 1, iod, &sgl,
                        NULL, NULL);
    if (rc != 0) {
        MFU_LOG(MFU_LOG_ERR, "Failed to fetch object");
        goto out;
    }

    /* Sanity check */
    if (sgl.sg_nr_out != 1) {
        MFU_LOG(MFU_LOG_ERR, "Failed to fetch single recx.");
        rc = 1;
        goto out;
    }

    stats->bytes_read += buf_len;

    /* store the single values inside of the akey dataset */
    single_val = &(*hdf5->ak)[*ak_index].rec_single_val;
    single_val->p = MFU_CALLOC(1, (uint64_t)(*iod).iod_size);
    if (single_val->p == NULL) {
        rc = ENOMEM;
        goto out;
    }
    memcpy(single_val->p, sgl.sg_iovs[0].iov_buf, (uint64_t)(*iod).iod_size);
    single_val->len = (uint64_t)(*iod).iod_size; 
out:
    mfu_free(&buf);
    return rc;
}

static int serialize_recx_array(struct hdf5_args *hdf5,
                                daos_key_t *dkey,
                                daos_key_t *akey,
                                char *rec_name,
                                uint64_t *ak_index,
                                daos_handle_t *oh,
                                daos_iod_t *iod,
                                mfu_daos_stats_t* stats)
{
    int                 rc = 0;
    int                 i = 0;
    int                 attr_num = 0;
    int                 buf_len = 0;
    int                 path_len = 0;
    uint32_t            number = 5;
    size_t              nalloc = 0;
    daos_anchor_t       recx_anchor = {0}; 
    daos_anchor_t       fetch_anchor = {0}; 
    daos_epoch_range_t  eprs[5] = {0};
    daos_recx_t         recxs[5] = {0};
    daos_size_t         size = 0;
    char                attr_name[ATTR_NAME_LEN] = {0};
    char                number_str[ATTR_NAME_LEN] = {0};
    char                attr_num_str[ATTR_NAME_LEN] = {0};
    unsigned char       *encode_buf = NULL;
    d_sg_list_t         sgl = {0};
    d_iov_t             iov = {0};
    hid_t               status = 0;
    char                *buf = NULL;

    hdf5->rx_dset = 0;
    hdf5->selection_attr = 0;
    hdf5->rx_memspace = 0;
    hdf5->rx_dtype = 0;

    /* need to do a fetch for size, so that we can
     * create the dataset with the correct datatype size */
    number = 1;
    rc = daos_obj_list_recx(*oh, DAOS_TX_NONE, dkey,
                            akey, &size, &number, NULL, eprs, &fetch_anchor,
                            true, NULL);
    if (rc != 0) {
        MFU_LOG(MFU_LOG_ERR, "Failed to list recx "DF_RC, DP_RC(rc));
        goto out;
    }
    if (number == 0) {
        rc = 0;
        goto out;
    }

    if (size > 2000) {
        MFU_LOG(MFU_LOG_ERR, "recx size is too large: %llu", size);
        rc = 1;
        goto out;
    }

    /* create the dataset with the correct type size */
    hdf5->rx_dtype = H5Tcreate(H5T_OPAQUE, size);
    if (hdf5->rx_dtype < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to create rx_dtype");
        rc = 1;
        goto out;
    }

    hdf5->rx_dset = H5Dcreate(hdf5->file,
                              rec_name,
                              hdf5->rx_dtype,
                              hdf5->rx_dspace,
                              H5P_DEFAULT,
                              hdf5->plist,
                              H5P_DEFAULT);
    if (hdf5->rx_dset < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to create rx_dset");
        rc = 1;
        goto out;
    }
    size = 0;

    hdf5->rx_dims[0] = 0;
    while (!daos_anchor_is_eof(&recx_anchor)) {
        memset(recxs, 0, sizeof(recxs));
        memset(eprs, 0, sizeof(eprs));

        /* list all recx for this dkey/akey */
        number = 5;
        rc = daos_obj_list_recx(*oh, DAOS_TX_NONE, dkey,
                                akey, &size, &number, recxs, eprs, &recx_anchor,
                                true, NULL);
        if (rc != 0) {
            MFU_LOG(MFU_LOG_ERR, "Failed to list recx "DF_RC, DP_RC(rc));
            goto out;
        }

        /* if no recx is returned for this dkey/akey move on */
        if (number == 0) 
            continue;
        for (i = 0; i < number; i++) {
            buf_len = recxs[i].rx_nr * size;
            buf = MFU_CALLOC(buf_len, 1);

            memset(&sgl, 0, sizeof(sgl));
            memset(&iov, 0, sizeof(iov));

            /* set iod values */
            (*iod).iod_type  = DAOS_IOD_ARRAY;
            (*iod).iod_size  = size;
            (*iod).iod_nr    = 1;
            (*iod).iod_recxs = &recxs[i];

            /* set sgl values */
            sgl.sg_nr     = 1;
            sgl.sg_nr_out = 0;
            sgl.sg_iovs   = &iov;

            d_iov_set(&iov, buf, buf_len);  
            /* fetch recx values from source */
            rc = daos_obj_fetch(*oh, DAOS_TX_NONE, 0, dkey, 1, iod,
                                &sgl, NULL, NULL);
            if (rc != 0) {
                MFU_LOG(MFU_LOG_ERR, "Failed to fetch object "DF_RC, DP_RC(rc));
                goto out;
            }

            /* Sanity check */
            if (sgl.sg_nr_out != 1) {
                MFU_LOG(MFU_LOG_ERR, "Failed to fetch array recxs.");
                rc = 1;
                goto out;
            }

            stats->bytes_read += buf_len;

            /* write data to record dset */
            hdf5->mem_dims[0] = recxs[i].rx_nr;
            hdf5->rx_memspace = H5Screate_simple(1, hdf5->mem_dims,
                                                 hdf5->mem_dims);
            if (hdf5->rx_memspace < 0) {
                MFU_LOG(MFU_LOG_ERR, "Failed to create rx_memspace");
                rc = 1;
                goto out;
            }
            /* extend dataset */
            hdf5->rx_dims[0] += recxs[i].rx_nr;
            status = H5Dset_extent(hdf5->rx_dset, hdf5->rx_dims);
            if (status < 0) {
                MFU_LOG(MFU_LOG_ERR, "Failed to extend rx_dset");
                rc = 1;
                goto out;
            }
            /* retrieve extended dataspace */
            hdf5->rx_dspace = H5Dget_space(hdf5->rx_dset);
            if (hdf5->rx_dspace < 0) {
                MFU_LOG(MFU_LOG_ERR, "Failed to get rx_dspace");
                rc = 1;
                goto out;
            }
            hsize_t start = (hsize_t)recxs[i].rx_idx;
            hsize_t count = (hsize_t)recxs[i].rx_nr;
            status = H5Sselect_hyperslab(hdf5->rx_dspace,
                                         H5S_SELECT_AND, &start,
                                         NULL, &count, NULL);
            if (status < 0) {
                MFU_LOG(MFU_LOG_ERR, "Failed to select hyperslab");
                rc = 1;
                goto out;
            }

            status = H5Dwrite(hdf5->rx_dset, hdf5->rx_dtype,
                              hdf5->rx_memspace, hdf5->rx_dspace,
                              H5P_DEFAULT, sgl.sg_iovs[0].iov_buf);
            if (status < 0) {
                MFU_LOG(MFU_LOG_ERR, "Failed to write rx_dset");
                rc = 1;
                goto out;
            }
            /* get size of buffer needed
             * from nalloc
             */
            status = H5Sencode1(hdf5->rx_dspace, NULL, &nalloc);
            if (status < 0) {
                MFU_LOG(MFU_LOG_ERR, "Failed to get size of buffer needed");
                rc = 1;
                goto out;
            }
            /* encode dataspace description
             * in buffer then store in
             * attribute on dataset
             */
            encode_buf = MFU_CALLOC(nalloc, sizeof(unsigned char));
            if (encode_buf == NULL) {
                rc = ENOMEM;
                goto out;
            }
            status = H5Sencode1(hdf5->rx_dspace, encode_buf,
                                &nalloc);
            if (status < 0) {
                MFU_LOG(MFU_LOG_ERR, "Failed to encode dataspace");
                rc = 1;
                goto out;
            }
            /* created attribute in HDF5 file with encoded
             * dataspace for this record extent */
            path_len = snprintf(number_str, ATTR_NAME_LEN, "%lu",
                                (*ak_index));
            if (path_len >= ATTR_NAME_LEN) {
                MFU_LOG(MFU_LOG_ERR, "number_str is too long");
                rc = 1;
                goto out;
            }
            path_len = snprintf(attr_num_str, ATTR_NAME_LEN, "-%d", attr_num);
            if (path_len >= ATTR_NAME_LEN) {
                MFU_LOG(MFU_LOG_ERR, "attr number str is too long");
                rc = 1;
                goto out;
            }
            path_len = snprintf(attr_name, ATTR_NAME_LEN, "%s%lu%d", "A-",
                                *ak_index, attr_num);
            if (path_len >= ATTR_NAME_LEN) {
                MFU_LOG(MFU_LOG_ERR, "attr name is too long");
                rc = 1;
                goto out;
            }
            hdf5->attr_dims[0] = 1;
            hdf5->attr_dspace = H5Screate_simple(1, hdf5->attr_dims, NULL);
            if (hdf5->attr_dspace < 0) {
                MFU_LOG(MFU_LOG_ERR, "failed to create attr");
                rc = 1;
                goto out;
            }
            hdf5->attr_dtype = H5Tcreate(H5T_OPAQUE, nalloc);
            if (hdf5->attr_dtype < 0) {
                MFU_LOG(MFU_LOG_ERR, "failed to create attr dtype");
                rc = 1;
                goto out;
            }
            hdf5->selection_attr = H5Acreate2(hdf5->rx_dset,
                                              attr_name,
                                              hdf5->attr_dtype,
                                              hdf5->attr_dspace,
                                              H5P_DEFAULT,
                                              H5P_DEFAULT);
            if (hdf5->selection_attr < 0) {
                MFU_LOG(MFU_LOG_ERR, "failed to create selection attr");
                rc = 1;
                goto out;
            }
            status = H5Awrite(hdf5->selection_attr, hdf5->attr_dtype,
                              encode_buf);
            if (status < 0) {
                MFU_LOG(MFU_LOG_ERR, "failed to write attr");
                rc = 1;
                goto out;
            }
            if (hdf5->selection_attr > 0) {
                H5Aclose(hdf5->selection_attr);
            }
            if (hdf5->rx_memspace > 0) {
                H5Sclose(hdf5->rx_memspace);
            }
            if (hdf5->attr_dtype > 0) {
                H5Tclose(hdf5->attr_dtype);
            }
            mfu_free(&encode_buf);
            mfu_free(&buf);
            attr_num++;
        }
    }
out:
    if (hdf5->rx_dset > 0) {
        H5Dclose(hdf5->rx_dset);
    }
    if (hdf5->rx_dtype > 0) {
        H5Tclose(hdf5->rx_dtype);
    }
    if (rc != 0) {
        if (hdf5->selection_attr > 0) {
            H5Aclose(hdf5->selection_attr);
        }
        if (hdf5->rx_memspace > 0) {
            H5Sclose(hdf5->rx_memspace);
        }
        mfu_free(&encode_buf);
    }
    return rc;
}

static int init_recx_data(struct hdf5_args *hdf5)
{
    int     rc = 0;
    herr_t  err = 0;

    hdf5->rx_dims[0] = 0;
    hdf5->rx_max_dims[0] = H5S_UNLIMITED;

    /* TODO consider other chunk sizes or possibly use different
     * chunk sizes for different dkeys/akeys. */
    hdf5->rx_chunk_dims[0] = 1024;

    hdf5->plist = H5Pcreate(H5P_DATASET_CREATE);
    if (hdf5->plist < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to create prop list");
        rc = 1;
        goto out;
    }
    hdf5->rx_dspace = H5Screate_simple(1, hdf5->rx_dims, hdf5->rx_max_dims);
    if (hdf5->rx_dspace < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to create rx_dspace");
        rc = 1;
        goto out;
    }
    err = H5Pset_layout(hdf5->plist, H5D_CHUNKED);
    if (err < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to set property layout");
        rc = 1;
        goto out;
    }
    err = H5Pset_chunk(hdf5->plist, 1, hdf5->rx_chunk_dims);
    if (err < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to set chunk size");
        rc = 1;
        goto out;
    }
out:
    if (rc != 0) {
        H5Tclose(hdf5->usr_attr_memtype);
        H5Tclose(hdf5->usr_attr_name_vtype);
        H5Tclose(hdf5->usr_attr_val_vtype);
        H5Tclose(hdf5->dkey_memtype);
        H5Tclose(hdf5->dkey_vtype);
        H5Tclose(hdf5->akey_memtype);
        H5Tclose(hdf5->akey_vtype);
    }
    return rc;
}

static int serialize_akeys(struct hdf5_args *hdf5,
                           daos_key_t diov,
                           uint64_t *dk_index,
                           uint64_t *ak_index,
                           daos_handle_t *oh,
                           mfu_daos_stats_t *stats)
{
    int             rc = 0;
    int             j = 0;
    daos_anchor_t   akey_anchor = {0}; 
    d_sg_list_t     akey_sgl;
    d_iov_t         akey_iov;
    daos_key_desc_t akey_kds[ENUM_DESC_NR] = {0};
    uint32_t        akey_number = ENUM_DESC_NR;
    char            akey_enum_buf[ENUM_DESC_BUF] = {0};
    char            akey[ENUM_KEY_BUF] = {0};
    char            *akey_ptr = NULL;
    daos_key_t      aiov = {0};
    daos_iod_t      iod = {0};
    size_t          rec_name_len = 32;
    char            rec_name[rec_name_len];
    int             path_len = 0;
    int             size = 0;
    hvl_t           *akey_val;

    (*hdf5->dk)[*dk_index].akey_offset = *ak_index;
    while (!daos_anchor_is_eof(&akey_anchor)) {
        memset(akey_kds, 0, sizeof(akey_kds));
        memset(akey, 0, sizeof(akey));
        memset(akey_enum_buf, 0, sizeof(akey_enum_buf));
        akey_number = ENUM_DESC_NR;

        akey_sgl.sg_nr     = 1;
        akey_sgl.sg_nr_out = 0;
        akey_sgl.sg_iovs   = &akey_iov;

        d_iov_set(&akey_iov, akey_enum_buf, ENUM_DESC_BUF);

        /* get akeys */
        rc = daos_obj_list_akey(*oh, DAOS_TX_NONE, &diov,
                                &akey_number, akey_kds,
                                &akey_sgl, &akey_anchor, NULL);
        if (rc != 0) {
            MFU_LOG(MFU_LOG_ERR, "failed to list akeys: %d", rc);
            goto out;
        }

        /* if no akeys returned move on */
        if (akey_number == 0)
            continue;

        size = (akey_number + stats->total_akeys) * sizeof(akey_t);
        *hdf5->ak = realloc(*hdf5->ak, size);
        if (*hdf5->ak == NULL) {
            rc = ENOMEM;
            goto out;
        }

        /* parse out individual akeys based on key length and
         * numver of dkeys returned
         */
        for (akey_ptr = akey_enum_buf, j = 0; j < akey_number; j++) {
            memcpy(akey, akey_ptr, akey_kds[j].kd_key_len);
            memset(&aiov, 0, sizeof(aiov));
            d_iov_set(&aiov, (void*)akey,
                      akey_kds[j].kd_key_len);
            akey_val = &(*hdf5->ak)[*ak_index].akey_val;
            akey_val->p = MFU_CALLOC((uint64_t)akey_kds[j].kd_key_len, sizeof(char));
            if (akey_val->p == NULL) {
                rc = ENOMEM;
                goto out;
            }
            memcpy(akey_val->p, (void*)akey_ptr,
                   (uint64_t)akey_kds[j].kd_key_len);
            akey_val->len = (uint64_t)akey_kds[j].kd_key_len; 
            (*hdf5->ak)[*ak_index].rec_dset_id = *ak_index;

            /* set iod values */
            iod.iod_nr   = 1;
            iod.iod_type = DAOS_IOD_SINGLE;
            iod.iod_size = DAOS_REC_ANY;
            iod.iod_recxs = NULL;
            iod.iod_name  = aiov;

            /* do a fetch (with NULL sgl) of single value type,
            * and if that returns iod_size == 0, then a single
            * value does not exist.
            */
            rc = daos_obj_fetch(*oh, DAOS_TX_NONE, 0, &diov,
                                1, &iod, NULL, NULL, NULL);
            if (rc != 0) {
                MFU_LOG(MFU_LOG_ERR, "failed to fetch object");
                rc = 1;
                goto out;
            }

            /* if iod_size == 0 then this is a
             * DAOS_IOD_ARRAY type
             */
            if ((int)iod.iod_size == 0) {
                /* set single value field to NULL, 0 for array types */
                (*hdf5->ak)[*ak_index].rec_single_val.p = NULL;
                (*hdf5->ak)[*ak_index].rec_single_val.len = 0;

                /* create a record dset only for array types */
                memset(&rec_name, 0, rec_name_len);
                path_len = snprintf(rec_name, rec_name_len, "%lu", *ak_index);
                if (path_len > FILENAME_LEN) {
                    MFU_LOG(MFU_LOG_ERR, "record name too long");
                    rc = 1;
                    goto out;
                }

                rc = serialize_recx_array(hdf5, &diov, &aiov, rec_name,
                                          ak_index, oh, &iod, stats);
                if (rc != 0) {
                    MFU_LOG(MFU_LOG_ERR, "failed to serialize recx array: %d",
                            rc);
                    goto out;
                }
            } else {
                rc = serialize_recx_single(hdf5, &diov, oh,
                                           &iod, ak_index, stats);
                if (rc != 0) {
                    MFU_LOG(MFU_LOG_ERR, "failed to serialize recx single: %d",
                            rc);
                    goto out;
                }
            }
            /* advance to next akey returned */ 
            akey_ptr += akey_kds[j].kd_key_len;
            (*ak_index)++;
        }
        stats->total_akeys += akey_number;
    }
out:
    return rc;
}

static int serialize_dkeys(struct hdf5_args *hdf5,
                           uint64_t *dk_index,
                           uint64_t *ak_index,
                           daos_handle_t *oh,
                           int *oid_index,
                           daos_args_t *da,
                           daos_obj_id_t oid,
                           bool is_kv,
                           mfu_daos_stats_t* stats)
{
    int             rc = 0;
    herr_t          err = 0;
    int             i = 0;
    daos_anchor_t   dkey_anchor = {0}; 
    d_sg_list_t     dkey_sgl;
    d_iov_t         dkey_iov;
    daos_key_desc_t dkey_kds[ENUM_DESC_NR] = {0};
    uint32_t        dkey_number = ENUM_DESC_NR;
    char            dkey_enum_buf[ENUM_DESC_BUF] = {0};
    char            dkey[ENUM_KEY_BUF] = {0};
    char            *dkey_ptr;
    daos_key_t      diov;
    int             path_len = 0;
    hvl_t           *dkey_val;

    rc = init_recx_data(hdf5);
    if (rc != 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to initialize recx data: %d", rc);
        rc = 1;
        goto out;
    }
    (*hdf5->oid)[*oid_index].dkey_offset = *dk_index;
    while (!daos_anchor_is_eof(&dkey_anchor)) {
        memset(dkey_kds, 0, sizeof(dkey_kds));
        memset(dkey, 0, sizeof(dkey));
        memset(dkey_enum_buf, 0, sizeof(dkey_enum_buf));
        dkey_number = ENUM_DESC_NR;

        dkey_sgl.sg_nr     = 1;
        dkey_sgl.sg_nr_out = 0;
        dkey_sgl.sg_iovs   = &dkey_iov;

        d_iov_set(&dkey_iov, dkey_enum_buf, ENUM_DESC_BUF);

        if (is_kv) {
            rc = daos_kv_list(*oh, DAOS_TX_NONE, &dkey_number,
                              dkey_kds, &dkey_sgl, &dkey_anchor, NULL);
            if (rc != 0) {
                MFU_LOG(MFU_LOG_ERR, "failed to list dkeys: "DF_RC, DP_RC(rc));
                goto out;
            }
        } else {
            rc = daos_obj_list_dkey(*oh, DAOS_TX_NONE, &dkey_number,
                                    dkey_kds, &dkey_sgl, &dkey_anchor, NULL);
            if (rc != 0) {
                MFU_LOG(MFU_LOG_ERR, "failed to list dkeys: "DF_RC, DP_RC(rc));
                goto out;
            }
        }

        /* if no dkeys were returned move on */
        if (dkey_number == 0)
            continue;
        *hdf5->dk = realloc(*hdf5->dk,
                            (dkey_number + stats->total_dkeys) * sizeof(dkey_t));
        if (*hdf5->dk == NULL) {
            rc = ENOMEM;
            goto out;
        }

        /* parse out individual dkeys based on key length and
         * number of dkeys returned
         */
        for (dkey_ptr = dkey_enum_buf, i = 0; i < dkey_number; i++) {
            /* Print enumerated dkeys */
            memcpy(dkey, dkey_ptr, dkey_kds[i].kd_key_len);
            memset(&diov, 0, sizeof(diov));
            d_iov_set(&diov, (void*)dkey, dkey_kds[i].kd_key_len);
            dkey_val = &(*hdf5->dk)[*dk_index].dkey_val;
            dkey_val->p = MFU_CALLOC((uint64_t)dkey_kds[i].kd_key_len, sizeof(char));
            if (dkey_val->p == NULL) {
                rc = ENOMEM;
                goto out;
            }
            memcpy(dkey_val->p, (void*)dkey_ptr,
                   (uint64_t)dkey_kds[i].kd_key_len);
            dkey_val->len = (uint64_t)dkey_kds[i].kd_key_len; 
            (*hdf5->dk)[*dk_index].rec_kv_val.p = NULL;
            (*hdf5->dk)[*dk_index].rec_kv_val.len = 0;
            if (is_kv) {
                /* akey offset will not be used in this case */
                (*hdf5->dk)[*dk_index].akey_offset = 0;

                /** create the KV store */
                rc = daos_kv_open(da->src_coh, oid, DAOS_OO_RW, oh, NULL);
                if (rc != 0) {
                    MFU_LOG(MFU_LOG_ERR, "Failed to open kv object: "DF_RC, DP_RC(rc));
                    rc = 1;
                    goto out;
                }

                /* daos_kv_get takes a char *key */
                char key_val[ENUM_LARGE_KEY_BUF];
                path_len = snprintf(key_val, ENUM_LARGE_KEY_BUF, "%s", dkey_val->p);
                if (path_len > ENUM_LARGE_KEY_BUF) {
                    rc = 1;
                    goto out;
                }

                /* TODO: serialize the array that was read */
                rc = serialize_kv_rec(hdf5, diov, oh, dk_index, key_val, stats);
                if (rc != 0) {
                    MFU_LOG(MFU_LOG_ERR, "Failed to serialize kv record: "DF_RC, DP_RC(rc));
                    rc = 1;
                    goto out;
                }
            } else {
                rc = serialize_akeys(hdf5, diov, dk_index, ak_index,
                                     oh, stats); 
                if (rc != 0) {
                    MFU_LOG(MFU_LOG_ERR, "failed to list akeys: %d", rc);
                    rc = 1;
                    goto out;
                }
            }
            dkey_ptr += dkey_kds[i].kd_key_len;
            (*dk_index)++;
        }
        stats->total_dkeys += dkey_number;
    }
    err = H5Sclose(hdf5->rx_dspace);
    if (err < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to close rx_dspace");
        rc = 1;
        goto out;
    }
out:
    return rc;
}

static int init_hdf5_file(struct hdf5_args *hdf5, char *filename) {
    int rc = 0;
    hdf5->file = H5Fcreate(filename, H5F_ACC_TRUNC, H5P_DEFAULT,
                           H5P_DEFAULT);
    if (hdf5->file < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to create hdf5 file");
        rc = 1;
        goto out;
    }

    /* Set user attribute dataset types */
    hdf5->usr_attr_memtype = H5Tcreate(H5T_COMPOUND, sizeof(usr_attr_t));
    if (hdf5->usr_attr_memtype < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to create user attr memtype");
        rc = 1;
        goto out;
    }
    hdf5->usr_attr_name_vtype = H5Tcopy(H5T_C_S1);
    if (hdf5->usr_attr_name_vtype < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to create user attr name vtype");
        rc = 1;
        goto out;
    }
    hdf5->status = H5Tset_size(hdf5->usr_attr_name_vtype, H5T_VARIABLE);
    if (hdf5->status < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to create user attr name vtype");
        rc = 1;
        goto out;
    }
    hdf5->usr_attr_val_vtype = H5Tvlen_create(H5T_NATIVE_OPAQUE);
    if (hdf5->usr_attr_val_vtype < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to create user attr val vtype");
        rc = 1;
        goto out;
    }
    hdf5->status = H5Tinsert(hdf5->usr_attr_memtype, "Attribute Name",
                             HOFFSET(usr_attr_t, attr_name), hdf5->usr_attr_name_vtype);
    if (hdf5->status < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to insert user attr name");
        rc = 1;
        goto out;
    }
    hdf5->status = H5Tinsert(hdf5->usr_attr_memtype, "Attribute Value",
                             HOFFSET(usr_attr_t, attr_val), hdf5->usr_attr_val_vtype);
    if (hdf5->status < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to insert user attr val");
        rc = 1;
        goto out;
    }

    /* Set oid dataset types */
    hdf5->oid_memtype = H5Tcreate(H5T_COMPOUND, sizeof(oid_t));
    if (hdf5->oid_memtype < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to create oid memtype");
        rc = 1;
        goto out;
    }
    hdf5->status = H5Tinsert(hdf5->oid_memtype, "OID Hi",
                             HOFFSET(oid_t, oid_hi), H5T_NATIVE_UINT64);
    if (hdf5->status < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to insert oid hi");
        rc = 1;
        goto out;
    }
    hdf5->status = H5Tinsert(hdf5->oid_memtype, "OID Low",
                             HOFFSET(oid_t, oid_low), H5T_NATIVE_UINT64);
    if (hdf5->status < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to insert oid low");
        rc = 1;
        goto out;
    }
    hdf5->status = H5Tinsert(hdf5->oid_memtype, "Dkey Offset",
                             HOFFSET(oid_t, dkey_offset), H5T_NATIVE_UINT64);
    if (hdf5->status < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to insert dkey offset");
        rc = 1;
        goto out;
    }

    /* Set dkey dataset types */
    hdf5->dkey_memtype = H5Tcreate(H5T_COMPOUND, sizeof(dkey_t));
    if (hdf5->dkey_memtype < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to create dkey memtype");
        rc = 1;
        goto out;
    }
    hdf5->dkey_vtype = H5Tvlen_create(H5T_NATIVE_OPAQUE);
    if (hdf5->dkey_vtype < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to create dkey vtype");
        rc = 1;
        goto out;
    }
    hdf5->status = H5Tinsert(hdf5->dkey_memtype, "Akey Offset",
                             HOFFSET(dkey_t, akey_offset),
                             H5T_NATIVE_UINT64);
    if (hdf5->status < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to insert akey offset");
        rc = 1;
        goto out;
    }
    hdf5->status = H5Tinsert(hdf5->dkey_memtype, "Dkey Value",
                             HOFFSET(dkey_t, dkey_val), hdf5->dkey_vtype);
    if (hdf5->status < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to insert dkey value");
        rc = 1;
        goto out;
    }

    hdf5->status = H5Tinsert(hdf5->dkey_memtype, "Record KV Value",
                             HOFFSET(dkey_t, rec_kv_val),
                             hdf5->dkey_vtype);
    if (hdf5->status < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to insert record KV value");
        rc = 1;
        goto out;
    }

    /* Set akey dataset types */
    hdf5->akey_memtype = H5Tcreate(H5T_COMPOUND, sizeof(akey_t));
    if (hdf5->akey_memtype < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to create akey memtype");
        rc = 1;
        goto out;
    }
    hdf5->akey_vtype = H5Tvlen_create(H5T_NATIVE_OPAQUE);
    if (hdf5->akey_vtype < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to create akey vtype");
        rc = 1;
        goto out;
    }
    hdf5->status = H5Tinsert(hdf5->akey_memtype, "Dataset ID",
                             HOFFSET(akey_t, rec_dset_id),
                             H5T_NATIVE_UINT64);
    if (hdf5->status < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to insert record dset id");
        rc = 1;
        goto out;
    }
    hdf5->status = H5Tinsert(hdf5->akey_memtype, "Record Single Value",
                             HOFFSET(akey_t, rec_single_val),
                             hdf5->akey_vtype);
    if (hdf5->status < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to insert record single value");
        rc = 1;
        goto out;
    }
    hdf5->status = H5Tinsert(hdf5->akey_memtype, "Akey Value",
                             HOFFSET(akey_t, akey_val), hdf5->akey_vtype);
    if (hdf5->status < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to insert akey value");
        rc = 1;
        goto out;
    }
out:
    return rc;
}

static int cont_serialize_version(struct hdf5_args *hdf5, float version)
{
    int     rc = 0;
    hid_t   status = 0;
    char    *version_name = "Version";

    hdf5->version_attr_dims[0] = 1;
    hdf5->version_attr_type = H5Tcopy(H5T_NATIVE_FLOAT);
    status = H5Tset_size(hdf5->version_attr_type, 4);
    if (status < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to create version dtype");
        rc = 1;
        goto out;
    }
    if (hdf5->version_attr_type < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to create version attr type");
        rc = 1;
        goto out;
    }
    hdf5->version_attr_dspace = H5Screate_simple(1, hdf5->version_attr_dims,
                                                 NULL);
    if (hdf5->version_attr_dspace < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to create version attr dspace");
        rc = 1;
        goto out;
    }
    hdf5->version_attr = H5Acreate2(hdf5->file,
                                    version_name,
                                    hdf5->version_attr_type,
                                    hdf5->version_attr_dspace,
                                    H5P_DEFAULT,
                                    H5P_DEFAULT);
    if (hdf5->version_attr < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to create version attr");
        rc = 1;
        goto out;
    }   
    status = H5Awrite(hdf5->version_attr, hdf5->version_attr_type,
                      &version);
    if (status < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to write attribute");
        rc = 1;
        goto out;
    }
    status = H5Aclose(hdf5->version_attr);
    if (status < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to close attribute");
        rc = 1;
        goto out;
    }
out:
    return rc;
}

int cont_serialize_usr_attrs(struct hdf5_args *hdf5, daos_handle_t cont)
{
    int         rc = 0;
    hid_t       status = 0;
    hid_t       dset = 0;
    hid_t       dspace = 0;
    hsize_t     dims[1];
    usr_attr_t* attr_data = NULL;
    int         num_attrs = 0;
    char**      names = NULL;
    void**      buffers = NULL;
    size_t*     sizes = NULL;

    /* Get all user attributes */
    rc = cont_get_usr_attrs(cont, &num_attrs, &names, &buffers, &sizes);
    if (rc != 0) {
        rc = 1;
        goto out;
    }

    if (num_attrs == 0) {
        goto out_no_attrs;
    }

    /* Create the user attribute data space */
    dims[0] = num_attrs;
    dspace = H5Screate_simple(1, dims, NULL);
    if (dspace < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to create user attr dspace");
        rc = 1;
        goto out;
    }

    /* Create the user attribute dataset */
    dset = H5Dcreate(hdf5->file, "User Attributes",
                     hdf5->usr_attr_memtype, dspace,
                     H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
    if (dset < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to create user attribute dset");
        rc = 1;
        goto out;
    }

    /* Allocate space for all attributes */
    attr_data = MFU_CALLOC(num_attrs, sizeof(usr_attr_t));
    if (attr_data == NULL) {
        MFU_LOG(MFU_LOG_ERR, "failed to allocate user attributes");
        rc = 1;
        goto out;
    }

    /* Set the data for all attributes */
    for (int i = 0; i < num_attrs; i++) {
        attr_data[i].attr_name = names[i];
        attr_data[i].attr_val.p = buffers[i];
        attr_data[i].attr_val.len = sizes[i];
    }

    status = H5Dwrite(dset, hdf5->usr_attr_memtype, H5S_ALL,
                      H5S_ALL, H5P_DEFAULT, attr_data);
    if (status < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to write user attr dset");
        rc = 1;
        goto out;
    }

out:
    cont_free_usr_attrs(num_attrs, &names, &buffers, &sizes);
    mfu_free(&attr_data);
    H5Dclose(dset);
    H5Tclose(hdf5->usr_attr_memtype);
    H5Sclose(dspace);
out_no_attrs:
    return rc;
}

static int cont_serialize_prop_roots(struct hdf5_args* hdf5,
                                     struct daos_prop_entry* entry,
                                     const char* prop_str)
{
    int                         rc = 0;
    hid_t                       status = 0;
    struct daos_prop_co_roots   *roots;
    obj_id_t                    root_oids[4];
    hsize_t                     attr_dims[1];
    /* HDF5 returns non-negative identifiers if successfully opened/created */
    hid_t                       attr_dtype = -1;
    hid_t                       attr_dspace = -1;
    hid_t                       usr_attr = -1;
    int                         i = 0;

    if (entry == NULL || entry->dpe_val_ptr == NULL) {
        goto out;
    }

    roots = entry->dpe_val_ptr;
    attr_dims[0] = 4;

    for (i = 0; i < 4; i++) {
        root_oids[i].hi = roots->cr_oids[i].hi;
        root_oids[i].lo = roots->cr_oids[i].lo;
    }

    attr_dtype = H5Tcreate(H5T_COMPOUND, sizeof(obj_id_t));
    if (attr_dtype < 0) {
        rc = 1;
        MFU_LOG(MFU_LOG_ERR, "failed to create attr dtype");
        goto out;
    }
    status = H5Tinsert(attr_dtype, "hi", HOFFSET(obj_id_t, hi), H5T_NATIVE_UINT64);
    if (hdf5->status < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to insert oid hi");
        rc = 1;
        goto out;
    }
    status = H5Tinsert(attr_dtype, "lo", HOFFSET(obj_id_t, lo), H5T_NATIVE_UINT64);
    if (hdf5->status < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to insert oid low");
        rc = 1;
        goto out;
    }

    attr_dspace = H5Screate_simple(1, attr_dims, NULL);
    if (attr_dspace < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to create version attribute");
        rc = 1;
        goto out;
    }
    usr_attr = H5Acreate2(hdf5->file, prop_str, attr_dtype, attr_dspace,
                          H5P_DEFAULT, H5P_DEFAULT);
    if (usr_attr < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to create attribute");
        rc = 1;
        goto out;
    }   
    status = H5Awrite(usr_attr, attr_dtype, root_oids);
    if (status < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to write attribute");
        rc = 1;
        goto out;
    }
out:
    if (usr_attr >= 0) {
        H5Aclose(usr_attr);
    }
    if (attr_dtype >= 0) {
        H5Tclose(attr_dtype);
    }
    if (attr_dspace >= 0) {
        H5Sclose(attr_dspace);
    }
    return rc;
}

static int cont_serialize_prop_acl(struct hdf5_args* hdf5,
                                   struct daos_prop_entry* entry,
                                   const char* prop_str)
{
    int                 rc = 0;
    hid_t               status = 0;
    struct daos_acl     *acl = NULL;
    char                **acl_strs = NULL;
    size_t              len_acl = 0;
    hsize_t             attr_dims[1];
    hid_t               attr_dtype;
    hid_t               attr_dspace;
    hid_t               usr_attr;
    int                 i = 0;


    if (entry == NULL || entry->dpe_val_ptr == NULL) {
        goto out;
    }

    /* convert acl to list of strings */
    acl = (struct daos_acl *)entry->dpe_val_ptr;                         
    rc = daos_acl_to_strs(acl, &acl_strs, &len_acl);
    if (rc != 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to convert acl to strs");
        goto out;
    }
    attr_dims[0] = len_acl;
    attr_dtype = H5Tcopy(H5T_C_S1);
    if (attr_dtype < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to create acl type");
        rc = 1;
        goto out;
    }
    status = H5Tset_size(attr_dtype, H5T_VARIABLE);
    if (status < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to set acl dtype size");
        rc = 1;
        goto out;
    }
    attr_dspace = H5Screate_simple(1, attr_dims, NULL);
    if (attr_dspace < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to create version attribute");
        rc = 1;
        goto out;
    }
    usr_attr = H5Acreate2(hdf5->file, prop_str, attr_dtype, attr_dspace,
                                H5P_DEFAULT, H5P_DEFAULT);
    if (usr_attr < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to create attribute");
        rc = 1;
        goto out;
    }   
    status = H5Awrite(usr_attr, attr_dtype, acl_strs);
    if (status < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to write attribute");
        rc = 1;
        goto out;
    }
    status = H5Aclose(usr_attr);
    if (status < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to close attribute");
        rc = 1;
        goto out;
    }
    status = H5Tclose(attr_dtype);
    if (status < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to close dtype");
        rc = 1;
        goto out;
    }
    status = H5Sclose(attr_dspace);
    if (status < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to close dspace");
        rc = 1;
        goto out;
    }
out:
    for (i = 0; i < len_acl; i++) {
        mfu_free(&acl_strs[i]);
    }
    mfu_free(&acl_strs);
    return rc;
}

static int cont_serialize_prop_str(struct hdf5_args* hdf5,
                                   struct daos_prop_entry* entry,
                                   const char* prop_str)
{
    int     rc = 0;
    hid_t   status = 0;
    hsize_t attr_dims[1];
    hid_t   attr_dtype;
    hid_t   attr_dspace;
    hid_t   usr_attr;

    if (entry == NULL || entry->dpe_str == NULL) {
        MFU_LOG(MFU_LOG_ERR, "Property %s not found", prop_str);
        rc = 1;
        goto out;
    }

    attr_dims[0] = 1;
    attr_dtype = H5Tcopy(H5T_C_S1);
    if (attr_dtype < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to create usr attr type");
        rc = 1;
        goto out;
    }
    status = H5Tset_size(attr_dtype, strlen(entry->dpe_str) + 1);
    if (status < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to set dtype size");
        rc = 1;
        goto out;
    }
    status = H5Tset_strpad(attr_dtype, H5T_STR_NULLTERM);
    if (status < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to set null terminator");
        rc = 1;
        goto out;
    }
    attr_dspace = H5Screate_simple(1, attr_dims, NULL);
    if (attr_dspace < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to create version attribute dataspace");
        rc = 1;
        goto out;
    }
    usr_attr = H5Acreate2(hdf5->file, prop_str, attr_dtype, attr_dspace,
                          H5P_DEFAULT, H5P_DEFAULT);
    if (usr_attr < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to create attribute");
        rc = 1;
        goto out;
    }   
    status = H5Awrite(usr_attr, attr_dtype, entry->dpe_str);
    if (status < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to write attribute");
        rc = 1;
        goto out;
    }
    status = H5Aclose(usr_attr);
    if (status < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to close attribute");
        rc = 1;
        goto out;
    }
    status = H5Tclose(attr_dtype);
    if (status < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to close dtype");
        rc = 1;
        goto out;
    }
    status = H5Sclose(attr_dspace);
    if (status < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to close dspace");
        rc = 1;
        goto out;
    }
out:
    return rc;
}

int daos_cont_serialize_files_generated(struct hdf5_args *hdf5,
                                        uint64_t *files_generated)
{
    int     rc = 0;
    hid_t   status = 0;

    hdf5->attr_dims[0] = 1;
    hdf5->attr_dtype = H5Tcopy(H5T_NATIVE_UINT64);
    status = H5Tset_size(hdf5->attr_dtype, 8);
    if (status < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to create version dtype");
        rc = 1;
        goto out;
    }
    if (hdf5->attr_dtype < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to create usr attr type");
        rc = 1;
        goto out;
    }
    hdf5->attr_dspace = H5Screate_simple(1, hdf5->attr_dims,
                                         NULL);
    if (hdf5->attr_dspace < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to create version attr dspace");
        rc = 1;
        goto out;
    }
    hdf5->usr_attr = H5Acreate2(hdf5->file,
                                "Files Generated",
                                hdf5->attr_dtype,
                                hdf5->attr_dspace,
                                H5P_DEFAULT,
                                H5P_DEFAULT);
    if (hdf5->usr_attr < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to create attr");
        rc = 1;
        goto out;
    }   
    status = H5Awrite(hdf5->usr_attr, hdf5->attr_dtype,
                      files_generated);
    if (status < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to write attr");
        rc = 1;
        goto out;
    }
    status = H5Aclose(hdf5->usr_attr);
    if (status < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to close attr");
        rc = 1;
        goto out;
    }
out:
    if (hdf5->file > 0) {
        H5Fclose(hdf5->file);
    }
    return rc;
}

static int cont_serialize_prop_uint(struct hdf5_args *hdf5,
                                    struct daos_prop_entry* entry,
                                    const char *prop_str)
{
    int     rc = 0;
    hid_t   status = 0;
    hsize_t attr_dims[1];
    hid_t   attr_dtype;
    hid_t   attr_dspace;
    hid_t   usr_attr;


    if (entry == NULL) {
        MFU_LOG(MFU_LOG_ERR, "Property %s not found", prop_str);
        rc = 1;
        goto out;
    }

    attr_dims[0] = 1;
    attr_dtype = H5Tcopy(H5T_NATIVE_UINT64);
    status = H5Tset_size(attr_dtype, 8);
    if (status < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to create version dtype");
        rc = 1;
        goto out;
    }
    if (attr_dtype < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to create usr attr type");
        rc = 1;
        goto out;
    }
    attr_dspace = H5Screate_simple(1, attr_dims, NULL);
    if (attr_dspace < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to create version attr dspace");
        rc = 1;
        goto out;
    }
    usr_attr = H5Acreate2(hdf5->file, prop_str, attr_dtype,
                          attr_dspace, H5P_DEFAULT, H5P_DEFAULT);
    if (usr_attr < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to create attr");
        rc = 1;
        goto out;
    }   
    status = H5Awrite(usr_attr, attr_dtype, &entry->dpe_val);
    if (status < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to write attr");
        rc = 1;
        goto out;
    }
    status = H5Aclose(usr_attr);
    if (status < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to close attr");
        rc = 1;
        goto out;
    }
    status = H5Tclose(attr_dtype);
    if (status < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to close dtype");
        rc = 1;
        goto out;
    }
    status = H5Sclose(attr_dspace);
    if (status < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to close dspace");
        rc = 1;
        goto out;
    }
out:
    return rc;
}

int cont_serialize_props(struct hdf5_args *hdf5,
                         daos_handle_t cont)
{
    int                     rc = 0;
    daos_prop_t*            prop_query = NULL;
    struct daos_prop_entry* entry;

    rc = cont_get_props(cont, &prop_query, true, true, true);
    if (rc != 0) {
        rc = 1;
        goto out;
    }

    entry = &prop_query->dpp_entries[0];
    rc = cont_serialize_prop_uint(hdf5, entry, "DAOS_PROP_CO_LAYOUT_TYPE");
    if (rc != 0) {
        goto out;
    }

    entry = &prop_query->dpp_entries[1];
    rc = cont_serialize_prop_uint(hdf5, entry, "DAOS_PROP_CO_LAYOUT_VER");
    if (rc != 0) {
        goto out;
    }

    entry = &prop_query->dpp_entries[2];
    rc = cont_serialize_prop_uint(hdf5, entry, "DAOS_PROP_CO_CSUM");
    if (rc != 0) {
        goto out;
    }

    entry = &prop_query->dpp_entries[3];
    rc = cont_serialize_prop_uint(hdf5, entry, "DAOS_PROP_CO_CSUM_CHUNK_SIZE");
    if (rc != 0) {
        goto out;
    }

    entry = &prop_query->dpp_entries[4];
    rc = cont_serialize_prop_uint(hdf5, entry, "DAOS_PROP_CO_CSUM_SERVER_VERIFY");
    if (rc != 0) {
        goto out;
    }

    entry = &prop_query->dpp_entries[5];
    rc = cont_serialize_prop_uint(hdf5, entry, "DAOS_PROP_CO_REDUN_FAC");
    if (rc != 0) {
        goto out;
    }

    entry = &prop_query->dpp_entries[6];
    rc = cont_serialize_prop_uint(hdf5, entry, "DAOS_PROP_CO_REDUN_LVL");
    if (rc != 0) {
        goto out;
    }

    entry = &prop_query->dpp_entries[7];
    rc = cont_serialize_prop_uint(hdf5, entry, "DAOS_PROP_CO_SNAPSHOT_MAX");
    if (rc != 0) {
        goto out;
    }

    entry = &prop_query->dpp_entries[8];
    rc = cont_serialize_prop_uint(hdf5, entry, "DAOS_PROP_CO_COMPRESS");
    if (rc != 0) {
        goto out;
    }

    entry = &prop_query->dpp_entries[9];
    rc = cont_serialize_prop_uint(hdf5, entry, "DAOS_PROP_CO_ENCRYPT");
    if (rc != 0) {
        goto out;
    }

    entry = &prop_query->dpp_entries[10];
    rc = cont_serialize_prop_str(hdf5, entry, "DAOS_PROP_CO_OWNER");
    if (rc != 0) {
        goto out;
    }

    entry = &prop_query->dpp_entries[11];
    rc = cont_serialize_prop_str(hdf5, entry, "DAOS_PROP_CO_OWNER_GROUP");
    if (rc != 0) {
        goto out;
    }

    entry = &prop_query->dpp_entries[12];
    rc = cont_serialize_prop_uint(hdf5, entry, "DAOS_PROP_CO_DEDUP");
    if (rc != 0) {
        goto out;
    }

    entry = &prop_query->dpp_entries[13];
    rc = cont_serialize_prop_uint(hdf5, entry, "DAOS_PROP_CO_DEDUP_THRESHOLD");
    if (rc != 0) {
        goto out;
    }

    entry = &prop_query->dpp_entries[14];
    rc = cont_serialize_prop_uint(hdf5, entry, "DAOS_PROP_CO_EC_CELL_SZ");
    if (rc != 0) {
        goto out;
    }

    entry = &prop_query->dpp_entries[15];
    rc = cont_serialize_prop_uint(hdf5, entry, "DAOS_PROP_CO_ALLOCED_OID");
    if (rc != 0) {
        goto out;
    }

    entry = &prop_query->dpp_entries[16];
    rc = cont_serialize_prop_str(hdf5, entry, "DAOS_PROP_CO_LABEL");
    if (rc != 0) {
        goto out;
    }

    entry = &prop_query->dpp_entries[17];
    rc = cont_serialize_prop_roots(hdf5, entry, "DAOS_PROP_CO_ROOTS");
    if (rc != 0) {
        goto out;
    }

    /* serialize ACL */
    if (prop_query->dpp_nr > 18) {
        entry = &prop_query->dpp_entries[18];
        rc = cont_serialize_prop_acl(hdf5, entry, "DAOS_PROP_CO_ACL");
        if (rc != 0) {
            goto out;
        }
    }

out:
    daos_prop_free(prop_query);
    return rc;
}

static bool obj_is_kv(daos_obj_id_t oid)
{

#if CHECK_DAOS_API_VERSION(2, 0)
	return daos_obj_id2type(oid) == DAOS_OT_KV_HASHED;
#else
	daos_ofeat_t ofeat;

        ofeat = (oid.hi & OID_FMT_FEAT_MASK) >> OID_FMT_FEAT_SHIFT;
        if ((ofeat & DAOS_OF_KV_FLAT) &&
            !(ofeat & DAOS_OF_ARRAY_BYTE) && !(ofeat & DAOS_OF_ARRAY)) {
		return true;
        }
	return false;
#endif
}

int daos_cont_serialize_hdlr(int rank, struct hdf5_args *hdf5, char *output_dir,
                             uint64_t *files_written, daos_args_t *da,
                             mfu_flist flist, uint64_t num_oids,
                             mfu_daos_stats_t* stats)
{
    int             rc = 0;
    int             i = 0;
    uint64_t        dk_index = 0;
    uint64_t        ak_index = 0;
    daos_handle_t   oh;
    float           version = 0.0;
    char            *filename = NULL;
    char            cont_str[FILENAME_LEN];
    bool            is_kv = false;
    int             size = 0;

    /* init HDF5 args */
    init_hdf5_args(hdf5);

    uuid_unparse(da->src_cont, cont_str);

    size = asprintf(&filename, "%s/%s%s%s%d%s", output_dir, cont_str, "_", "rank", rank, ".h5");
    if (size == -1) {
        rc = 1;
        goto out;
    }

    printf("Serializing Container to %s\n", filename);

    /* keep track of number of files written */
    (*files_written)++;

    /* init HDF5 datatypes in HDF5 file */
    rc = init_hdf5_file(hdf5, filename);
    if (rc != 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to init hdf5 file");
        rc = 1;
        goto out;
    }
    hdf5->oid_data = MFU_CALLOC(num_oids, sizeof(oid_t));
    if (hdf5->oid_data == NULL) {
        rc = ENOMEM;
        goto out;
    }
    hdf5->dkey_data = MFU_CALLOC(1, sizeof(dkey_t));
    if (hdf5->dkey_data == NULL) {
        rc = ENOMEM;
        mfu_free(&hdf5->oid_data);
        goto out;
    }
    hdf5->akey_data = MFU_CALLOC(1, sizeof(akey_t));
    if (hdf5->akey_data == NULL) {
        rc = ENOMEM;
        mfu_free(&hdf5->oid_data);
        mfu_free(&hdf5->dkey_data);
        goto out;
    }
    hdf5->dk = &(hdf5->dkey_data);
    hdf5->ak = &(hdf5->akey_data);
    hdf5->oid = &(hdf5->oid_data);

    /* size is total oids for this rank, loop over each oid and serialize */
    for (i = 0; i < num_oids; i++) {
        /* open DAOS object based on oid to get obj
         * handle
         */
        daos_obj_id_t oid;
        oid.hi = mfu_flist_file_get_oid_high(flist, i);
        oid.lo = mfu_flist_file_get_oid_low(flist, i);
        (*hdf5->oid)[i].oid_hi = oid.hi;
        (*hdf5->oid)[i].oid_low = oid.lo;

	is_kv = obj_is_kv(oid);
        /* TODO: DAOS_OF_KV_FLAT uses daos_kv_* functions, and
         * other object types use daos_obj_* functions. Maybe there is
         * a better way to organize this with swtich statements, or
         * creating "daos_obj" wrappers, etc. */

        if (is_kv) {
            rc = daos_kv_open(da->src_coh, oid, DAOS_OO_RW, &oh, NULL);
            if (rc != 0) {
                MFU_LOG(MFU_LOG_ERR, "failed to open kv object: "DF_RC, DP_RC(rc));
                goto out;
            }
            rc = serialize_dkeys(hdf5, &dk_index, &ak_index,
                                 &oh, &i, da, oid, is_kv, stats);
            if (rc != 0) {
                MFU_LOG(MFU_LOG_ERR, "failed to serialize keys: %d", rc);
                goto out;
            }
            /* close source and destination object */
            rc = daos_kv_close(oh, NULL);
            if (rc != 0) {
                MFU_LOG(MFU_LOG_ERR, "failed to close kv object: "DF_RC, DP_RC(rc));
                goto out;
            }
        } else {
            rc = daos_obj_open(da->src_coh, oid, 0, &oh, NULL);
            if (rc != 0) {
                MFU_LOG(MFU_LOG_ERR, "failed to open object: "DF_RC, DP_RC(rc));
                goto out;
            }
            rc = serialize_dkeys(hdf5, &dk_index, &ak_index,
                                 &oh, &i, da, oid, is_kv, stats);
            if (rc != 0) {
                MFU_LOG(MFU_LOG_ERR, "failed to serialize keys: %d", rc);
                goto out;
            }
            /* close source and destination object */
            rc = daos_obj_close(oh, NULL);
            if (rc != 0) {
                MFU_LOG(MFU_LOG_ERR, "failed to close object: "DF_RC, DP_RC(rc));
                goto out;
            }
        }
        /* Increment as we go */
        stats->total_oids++;
    }

    /* write container version as attribute */
    rc = cont_serialize_version(hdf5, version);
    if (rc != 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to serialize version");
        goto out;
    }

    rc = cont_serialize_usr_attrs(hdf5, da->src_coh);
    if (rc != 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to serialize user attributes");
        goto out;
    }

    rc = cont_serialize_props(hdf5, da->src_coh);
    if (rc != 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to serialize cont layout");
        goto out;
    }

    hdf5->oid_dims[0] = num_oids;
    hdf5->oid_dspace = H5Screate_simple(1, hdf5->oid_dims, NULL);
    if (hdf5->oid_dspace < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to create oid dspace");
        rc = 1;
        goto out;
    }
    hdf5->oid_dset = H5Dcreate(hdf5->file, "Oid Data",
                              hdf5->oid_memtype, hdf5->oid_dspace,
                              H5P_DEFAULT, H5P_DEFAULT,
                              H5P_DEFAULT);
    if (hdf5->oid_dset < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to create oid dset");
        rc = 1;
        goto out;
    }
    hdf5->dkey_dims[0] = stats->total_dkeys;     
    hdf5->dkey_dspace = H5Screate_simple(1, hdf5->dkey_dims, NULL);
    if (hdf5->dkey_dspace < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to create dkey dspace");
        rc = 1;
        goto out;
    }
    hdf5->dkey_dset = H5Dcreate(hdf5->file, "Dkey Data",
                                hdf5->dkey_memtype, hdf5->dkey_dspace,
                                H5P_DEFAULT, H5P_DEFAULT,
                                H5P_DEFAULT);
    if (hdf5->dkey_dset < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to create dkey dset");
        rc = 1;
        goto out;
    }
    hdf5->akey_dims[0] = stats->total_akeys;     
    hdf5->akey_dspace = H5Screate_simple(1, hdf5->akey_dims, NULL);
    if (hdf5->akey_dspace < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to create akey dspace");
        rc = 1;
        goto out;
    }
    hdf5->akey_dset = H5Dcreate(hdf5->file, "Akey Data",
                                hdf5->akey_memtype, hdf5->akey_dspace,
                                H5P_DEFAULT, H5P_DEFAULT,
                                H5P_DEFAULT);
    if (hdf5->akey_dset < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to create akey dset");
        rc = 1;
        goto out;
    }
    hdf5->status = H5Dwrite(hdf5->oid_dset, hdf5->oid_memtype, H5S_ALL,
                            H5S_ALL, H5P_DEFAULT, *(hdf5->oid));
    if (hdf5->status < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to write oid dset");
        rc = 1;
        goto out;
    }
    hdf5->status = H5Dwrite(hdf5->dkey_dset, hdf5->dkey_memtype,
                           H5S_ALL, H5S_ALL, H5P_DEFAULT,
                           *(hdf5->dk));
    if (hdf5->status < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to write dkey dset");
        rc = 1;
        goto out;
    }
    hdf5->status = H5Dwrite(hdf5->akey_dset, hdf5->akey_memtype,
                           H5S_ALL, H5S_ALL, H5P_DEFAULT,
                           *(hdf5->ak));
    if (hdf5->status < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to write akey dset");
        rc = 1;
        goto out;
    }

out:
    /* free dkey, akey values and single record values */
    for (i = 0; i < stats->total_dkeys; i++) {
        mfu_free(&((*hdf5->dk)[i].dkey_val.p));
    }
    for (i = 0; i < stats->total_akeys; i++) {
        mfu_free(&((*hdf5->ak)[i].akey_val.p));
        mfu_free(&((*hdf5->ak)[i].rec_single_val.p));
    }
    if (hdf5->oid_dset > 0)
        H5Dclose(hdf5->oid_dset);
    if (hdf5->dkey_dset > 0)
        H5Dclose(hdf5->dkey_dset);
    if (hdf5->akey_dset > 0)
        H5Dclose(hdf5->akey_dset);
    if (hdf5->oid_dspace > 0)
        H5Sclose(hdf5->oid_dspace);
    if (hdf5->dkey_dspace > 0)
        H5Sclose(hdf5->dkey_dspace);
    if (hdf5->akey_dspace > 0)
        H5Sclose(hdf5->akey_dspace);
    if (hdf5->oid_memtype > 0)
        H5Tclose(hdf5->oid_memtype);
    if (hdf5->dkey_memtype > 0)
        H5Tclose(hdf5->dkey_memtype);
    if (hdf5->akey_memtype > 0)
        H5Tclose(hdf5->akey_memtype);
    mfu_free(&hdf5->oid_data);
    mfu_free(&hdf5->dkey_data);
    mfu_free(&hdf5->akey_data);
    mfu_free(&filename);
    /* dont close file until the files generated is serialized */
    return rc;
}

static int hdf5_read_key_data(struct hdf5_args *hdf5)
{
    int     rc = 0;
    hid_t   status = 0;
    int     oid_ndims = 0;
    int     dkey_ndims = 0;
    int     akey_ndims = 0;

    /* read oid data */
    hdf5->oid_dset = H5Dopen(hdf5->file, "Oid Data", H5P_DEFAULT);
    if (hdf5->oid_dset < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to open OID dset");
        rc = 1;
        goto out;
    }
    hdf5->oid_dspace = H5Dget_space(hdf5->oid_dset);
    if (hdf5->oid_dspace < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to get oid dspace");
        rc = 1;
        goto out;
    }
    hdf5->oid_dtype = H5Dget_type(hdf5->oid_dset);
    if (hdf5->oid_dtype < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to get oid dtype");
        rc = 1;
        goto out;
    }
    oid_ndims = H5Sget_simple_extent_dims(hdf5->oid_dspace, hdf5->oid_dims,
                                          NULL);
    if (oid_ndims < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to get oid dimensions");
        rc = 1;
        goto out;
    }
    hdf5->oid_data = MFU_CALLOC(hdf5->oid_dims[0], sizeof(oid_t));
    if (hdf5->oid_data == NULL) {
        rc = ENOMEM;
        goto out;
    }
    status = H5Dread(hdf5->oid_dset, hdf5->oid_dtype, H5S_ALL, H5S_ALL,
                     H5P_DEFAULT, hdf5->oid_data);
    if (status < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to read oid data");
        rc = 1;
        goto out;
    }

    /* read dkey data */
    hdf5->dkey_dset = H5Dopen(hdf5->file, "Dkey Data", H5P_DEFAULT);
    if (hdf5->dkey_dset < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to open dkey dset");
        rc = 1;
        goto out;
    }
    hdf5->dkey_dspace = H5Dget_space(hdf5->dkey_dset);
    if (hdf5->dkey_dspace < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to get dkey dspace");
        rc = 1;
        goto out;
    }
    hdf5->dkey_vtype = H5Dget_type(hdf5->dkey_dset);
    if (hdf5->dkey_vtype < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to get dkey vtype");
        rc = 1;
        goto out;
    }
    dkey_ndims = H5Sget_simple_extent_dims(hdf5->dkey_dspace,
                                           hdf5->dkey_dims, NULL);
    if (dkey_ndims < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to get dkey dimensions");
        rc = 1;
        goto out;
    }
    hdf5->dkey_data = MFU_CALLOC(hdf5->dkey_dims[0], sizeof(dkey_t));
    if (hdf5->dkey_data == NULL) {
        rc = ENOMEM;
        goto out;
    }
    status = H5Dread(hdf5->dkey_dset, hdf5->dkey_vtype, H5S_ALL, H5S_ALL,
                     H5P_DEFAULT, hdf5->dkey_data);
    if (status < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to read dkey data");
        rc = 1;
        goto out;
    }

    /* read akey data */
    hdf5->akey_dset = H5Dopen(hdf5->file, "Akey Data", H5P_DEFAULT);
    if (hdf5->akey_dset < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to open akey dset");
        rc = 1;
        goto out;
    }
    hdf5->akey_dspace = H5Dget_space(hdf5->akey_dset);
    if (hdf5->akey_dspace < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to get akey dset");
        rc = 1;
        goto out;
    }
    hdf5->akey_vtype = H5Dget_type(hdf5->akey_dset);
    if (hdf5->akey_vtype < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to get akey vtype");
        rc = 1;
        goto out;
    }
    akey_ndims = H5Sget_simple_extent_dims(hdf5->akey_dspace,
                                           hdf5->akey_dims, NULL);
    if (akey_ndims < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to get akey dimensions");
        rc = 1;
        goto out;
    }
    if (hdf5->akey_dims[0] > 0) {
        hdf5->akey_data = MFU_CALLOC(hdf5->akey_dims[0], sizeof(akey_t));
        if (hdf5->akey_data == NULL) {
            rc = ENOMEM;
            goto out;
        }
        status = H5Dread(hdf5->akey_dset, hdf5->akey_vtype, H5S_ALL, H5S_ALL,
                         H5P_DEFAULT, hdf5->akey_data);
        if (status < 0) {
            MFU_LOG(MFU_LOG_ERR, "failed to read akey data");
            rc = 1;
            goto out;
        }
    }
out:
    return rc;
}

static int cont_deserialize_recx(struct hdf5_args *hdf5,
                                 daos_handle_t *oh,
                                 daos_key_t diov,
                                 int num_attrs,
                                 uint64_t ak_off,
                                 int k,
                                 mfu_daos_stats_t* stats)
{
    int             rc = 0;
    hid_t           status = 0;
    int             i = 0;
    ssize_t         attr_len = 0;
    char            attr_name_buf[124]; 
    hsize_t         attr_space;
    hid_t           attr_type;
    size_t          type_size;
    size_t          rx_dtype_size;
    unsigned char   *decode_buf;
    hid_t           rx_range_id;
    hsize_t         *rx_range = NULL;
    uint64_t        recx_len = 0;
    void            *recx_data = NULL;
    hssize_t        nblocks = 0;
    d_sg_list_t     sgl;
    d_iov_t         iov;
    daos_iod_t      iod;
    daos_recx_t     recxs;
    hid_t           aid;

    for (i = 0; i < num_attrs; i++) {
        memset(attr_name_buf, 0, sizeof(attr_name_buf));
        aid = H5Aopen_idx(hdf5->rx_dset, (unsigned int)i);
        if (aid < 0) {
            MFU_LOG(MFU_LOG_ERR, "failed to get open attr");
            rc = 1;
            goto out;
        }
        attr_len = H5Aget_name(aid, 124, attr_name_buf);
        if (attr_len < 0) {
            MFU_LOG(MFU_LOG_ERR, "failed to get attr name");
            rc = 1;
            goto out;
        }
        attr_space = H5Aget_storage_size(aid);
        if (attr_len < 0) {
            MFU_LOG(MFU_LOG_ERR, "failed to get attr space");
            rc = 1;
            goto out;
        }
        attr_type = H5Aget_type(aid);
        if (attr_type < 0) {
            MFU_LOG(MFU_LOG_ERR, "failed to get attr type");
            rc = 1;
            goto out;
        }
        type_size = H5Tget_size(attr_type);
        if (type_size < 0) {
            MFU_LOG(MFU_LOG_ERR, "failed to get type size");
            rc = 1;
            goto out;
        }
        rx_dtype_size = H5Tget_size(hdf5->rx_dtype);
        if (rx_dtype_size < 0) {
            MFU_LOG(MFU_LOG_ERR, "failed to get rx type size");
            rc = 1;
            goto out;
        }
        decode_buf = MFU_CALLOC(1, type_size * attr_space);
        if (decode_buf == NULL) {
            rc = ENOMEM;
            goto out;
        }
        rx_range = MFU_CALLOC(1, type_size * attr_space);
        if (rx_range == NULL) {
            rc = ENOMEM;
            goto out;
        }
        status = H5Aread(aid, attr_type, decode_buf);
        if (status < 0) {
            MFU_LOG(MFU_LOG_ERR, "failed to read attribute");
            rc = 1;
            goto out;
        }
        rx_range_id = H5Sdecode(decode_buf);
        if (rx_range_id < 0) {
            MFU_LOG(MFU_LOG_ERR, "failed to decode attribute buffer");
            rc = 1;
            goto out;
        }
        nblocks = H5Sget_select_hyper_nblocks(rx_range_id);
        if (nblocks < 0) {
            MFU_LOG(MFU_LOG_ERR, "failed to get hyperslab blocks");
            rc = 1;
            goto out;
        }
        status = H5Sget_select_hyper_blocklist(rx_range_id, 0,
                                               nblocks, rx_range);
        if (status < 0) {
            MFU_LOG(MFU_LOG_ERR, "failed to get blocklist");
            rc = 1;
            goto out;
        }

        /* read recx data then update */
        hdf5->rx_dspace = H5Dget_space(hdf5->rx_dset);
        if (hdf5->rx_dspace < 0) {
            MFU_LOG(MFU_LOG_ERR, "failed to get rx_dspace");
            rc = 1;
            goto out;
        }

        hsize_t start = rx_range[0];
        hsize_t count = (rx_range[1] - rx_range[0]) + 1;
        status = H5Sselect_hyperslab(hdf5->rx_dspace,
                                     H5S_SELECT_AND,
                                     &start, NULL,
                                     &count, NULL);
        if (status < 0) {
            MFU_LOG(MFU_LOG_ERR, "failed to select hyperslab");
            rc = 1;
            goto out;
        }
        recx_len = count;
        recx_data = MFU_CALLOC(recx_len, rx_dtype_size);
        if (recx_data == NULL) {
            rc = ENOMEM;
            goto out;
        }
        hdf5->mem_dims[0] = count;
        hdf5->rx_memspace = H5Screate_simple(1, hdf5->mem_dims,
                                             hdf5->mem_dims);
        status = H5Dread(hdf5->rx_dset,
                         hdf5->rx_dtype,
                         hdf5->rx_memspace,
                         hdf5->rx_dspace,
                         H5P_DEFAULT,
                         recx_data);
        if (status < 0) {
            MFU_LOG(MFU_LOG_ERR, "failed to read record extent");
            rc = 1;
            goto out;
        }
        memset(&sgl, 0, sizeof(sgl));
        memset(&iov, 0, sizeof(iov));
        memset(&iod, 0, sizeof(iod));
        memset(&recxs, 0, sizeof(recxs));
        d_iov_set(&iod.iod_name,
                  (void*)hdf5->akey_data[ak_off + k].akey_val.p,
                  hdf5->akey_data[ak_off + k].akey_val.len);
        /* set iod values */
        iod.iod_type  = DAOS_IOD_ARRAY;
        iod.iod_size  = rx_dtype_size;
        iod.iod_nr    = 1;

        recxs.rx_nr = recx_len;
        recxs.rx_idx = start;
        iod.iod_recxs = &recxs;

        /* set sgl values */
        sgl.sg_nr     = 1;
        sgl.sg_iovs   = &iov;

        uint64_t buf_size = recx_len * rx_dtype_size;
        d_iov_set(&iov, recx_data, buf_size); 

        /* update fetched recx values and place in destination object */
        rc = daos_obj_update(*oh, DAOS_TX_NONE, 0, &diov, 1, &iod,
                             &sgl, NULL);
        if (rc != 0) {
            MFU_LOG(MFU_LOG_ERR, "failed to update object: "DF_RC, DP_RC(rc));
            goto out;
        }

        stats->bytes_written += buf_size;

        H5Aclose(aid);
        mfu_free(&rx_range);
        mfu_free(&recx_data);
        mfu_free(&decode_buf);
    }
out:
    return rc;
}

static int cont_deserialize_akeys(struct hdf5_args *hdf5,
                                  daos_key_t diov,
                                  uint64_t *ak_off,
                                  int k,
                                  daos_handle_t *oh,
                                  mfu_daos_stats_t* stats)
{
    int             rc = 0;
    daos_key_t      aiov;
    char            akey[ENUM_KEY_BUF] = {0};
    int             rx_ndims;
    uint64_t        index = 0;
    int             len = 0;
    int             num_attrs;
    size_t          single_tsize;
    void            *single_data = NULL;
    d_sg_list_t     sgl;
    d_iov_t         iov;
    daos_iod_t      iod;
    hvl_t           *akey_val;
    hvl_t           *rec_single_val;
    
    memset(&aiov, 0, sizeof(aiov));
    akey_val = &(hdf5->akey_data)[*ak_off + k].akey_val;
    rec_single_val = &(hdf5->akey_data)[*ak_off + k].rec_single_val;
    memcpy(akey, akey_val->p, akey_val->len);
    d_iov_set(&aiov, (void*)akey_val->p, akey_val->len);

    /* if the len of the single value is set to zero,
     * then this akey points to an array record dataset */
    if (rec_single_val->len == 0) {
        index = *ak_off + k;
        len = snprintf(NULL, 0, "%lu", index);
        char *dset_name = NULL;
        dset_name = MFU_CALLOC(1, len + 1);
        snprintf(dset_name, len + 1, "%lu", index);
        hdf5->rx_dset = H5Dopen(hdf5->file, dset_name,
                                H5P_DEFAULT);
        if (hdf5->rx_dset < 0) {
            MFU_LOG(MFU_LOG_ERR, "failed to read rx_dset");
            rc = 1;
            goto out;
        }
        hdf5->rx_dspace = H5Dget_space(hdf5->rx_dset);
        if (hdf5->rx_dspace < 0) {
            MFU_LOG(MFU_LOG_ERR, "failed to read rx_dspace");
            rc = 1;
            goto out;
        }
        hdf5->rx_dtype = H5Dget_type(hdf5->rx_dset);
        if (hdf5->rx_dtype < 0) {
            MFU_LOG(MFU_LOG_ERR, "failed to read rx_dtype");
            rc = 1;
            goto out;
        }
        hdf5->plist = H5Dget_create_plist(hdf5->rx_dset);
        if (hdf5->plist < 0) {
            MFU_LOG(MFU_LOG_ERR, "failed to get plist");
            rc = 1;
            goto out;
        }
        rx_ndims = H5Sget_simple_extent_dims(hdf5->rx_dspace,
                                             hdf5->rx_dims,
                                             NULL);
        if (rx_ndims < 0) {
            MFU_LOG(MFU_LOG_ERR, "failed to get rx_ndims");
            rc = 1;
            goto out;
        }
        num_attrs = H5Aget_num_attrs(hdf5->rx_dset);
        if (num_attrs < 0) {
            MFU_LOG(MFU_LOG_ERR, "failed to get num attrs");
            rc = 1;
            goto out;
        }
        rc = cont_deserialize_recx(hdf5, oh, diov, num_attrs, *ak_off, k, stats);
        if (rc != 0) {
            MFU_LOG(MFU_LOG_ERR, "failed to deserialize recx");
            rc = 1;
            goto out;
        }
        H5Pclose(hdf5->plist);
        H5Tclose(hdf5->rx_dtype);
        H5Sclose(hdf5->rx_dspace);
        H5Dclose(hdf5->rx_dset);
        mfu_free(&dset_name);
    } else {
        memset(&sgl, 0, sizeof(sgl));
        memset(&iov, 0, sizeof(iov));
        memset(&iod, 0, sizeof(iod));
        single_tsize = rec_single_val->len;
        if (single_tsize == 0) {
            MFU_LOG(MFU_LOG_ERR, "failed to get size of type in single "
                    "record datatype");
            rc = 1;
            goto out;
        }
        single_data = MFU_CALLOC(1, single_tsize);
        if (single_data == NULL) {
            rc = ENOMEM;
            goto out;
        }
        memcpy(single_data, rec_single_val->p, rec_single_val->len);

        /* set iod values */
        iod.iod_type  = DAOS_IOD_SINGLE;
        iod.iod_size  = single_tsize;
        iod.iod_nr    = 1;
        iod.iod_recxs = NULL;
        iod.iod_name  = aiov;

        /* set sgl values */
        sgl.sg_nr     = 1;
        sgl.sg_nr_out = 0;
        sgl.sg_iovs   = &iov;
        d_iov_set(&iov, single_data, single_tsize);

        /* update fetched recx values and place in destination object */
        rc = daos_obj_update(*oh, DAOS_TX_NONE, 0,
                             &diov, 1, &iod, &sgl, NULL);
        if (rc != 0) {
            MFU_LOG(MFU_LOG_ERR, "failed to update object: "DF_RC, DP_RC(rc));
            goto out;
        }
        stats->bytes_written += single_tsize;
        mfu_free(&single_data);
    }
    stats->total_akeys++;
out:
    mfu_free(&single_data);
    return rc;
}

static int cont_deserialize_keys(struct hdf5_args *hdf5,
                                 uint64_t *total_dkeys_this_oid,
                                 uint64_t *dk_off,
                                 daos_handle_t *oh,
                                 mfu_daos_stats_t* stats)
{
    int             rc = 0;
    int             j = 0;
    daos_key_t      diov;
    char            dkey[ENUM_KEY_BUF] = {0};
    uint64_t        ak_off = 0;
    uint64_t        ak_next = 0;
    uint64_t        total_akeys_this_dkey = 0;
    int             k = 0;
    hvl_t           *dkey_val;
    hvl_t           *rec_kv_val;
    
    for(j = 0; j < *total_dkeys_this_oid; j++) {
        memset(&diov, 0, sizeof(diov));
        memset(dkey, 0, sizeof(dkey));
        dkey_val = &(hdf5->dkey_data)[*dk_off + j].dkey_val;
        rec_kv_val = &(hdf5->dkey_data)[*dk_off + j].rec_kv_val;
        memcpy(dkey, dkey_val->p, dkey_val->len);
        d_iov_set(&diov, (void*)dkey_val->p, dkey_val->len);
        ak_off = hdf5->dkey_data[*dk_off + j].akey_offset;
        ak_next = 0;
        total_akeys_this_dkey = 0;
        if (*dk_off + j + 1 < (int)hdf5->dkey_dims[0]) {
            ak_next = hdf5->dkey_data[(*dk_off + j) + 1].akey_offset;
            total_akeys_this_dkey = ak_next - ak_off;
        } else if (*dk_off + j == ((int)hdf5->dkey_dims[0] - 1)) {
            total_akeys_this_dkey = ((int)hdf5->akey_dims[0]) - ak_off;
        }

        /* if rec_kv_val.len != 0 then skip akey iteration, we can
         * write data back into DAOS using the daos_kv.h API using just
         * oid, dkey, and key value (stored in dkey dataset) */

        /* run daos_kv_put on rec_kv_val (dkey val) and key (dkey) */
        /* skip akey iteration for DAOS_OF_KV_FLAT objects */
        if (rec_kv_val->len > 0) {
            daos_size_t kv_single_size = 0;
            kv_single_size = rec_kv_val->len;
            rc = daos_kv_put(*oh, DAOS_TX_NONE, 0, dkey, kv_single_size,
                             rec_kv_val->p, NULL);
            if (rc != 0) {
                MFU_LOG(MFU_LOG_ERR, "failed to put kv object: "DF_RC, DP_RC(rc));
                goto out;
            }
            stats->bytes_written += kv_single_size;
        } else {
            for (k = 0; k < total_akeys_this_dkey; k++) {
                rc = cont_deserialize_akeys(hdf5, diov, &ak_off, k, oh, stats);
                if (rc != 0) {
                    MFU_LOG(MFU_LOG_ERR, "failed to deserialize akeys: "DF_RC,
                            DP_RC(rc));
                    goto out;
                }
            }
        }
        stats->total_dkeys++;
    }
out:
    return rc;
}

static int cont_deserialize_prop_str(struct hdf5_args* hdf5,
                                     struct daos_prop_entry* entry,
                                     const char* prop_str)
{
    hid_t   status = 0;
    int     rc = 0;
    hid_t   attr_dtype;
    hid_t   cont_attr;

    cont_attr = H5Aopen(hdf5->file, prop_str, H5P_DEFAULT);
    if (cont_attr < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to open property attribute %s", prop_str);
        rc = 1;
        goto out;
    }
    attr_dtype = H5Aget_type(cont_attr);
    if (attr_dtype < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to open property attribute type %s", prop_str);
        rc = 1;
        goto out;
    }
    size_t buf_size = H5Tget_size(attr_dtype);
    if (buf_size <= 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to get size for property attribute %s", prop_str);
        rc = 1;
        goto out;
    }
    entry->dpe_str = MFU_CALLOC(1, buf_size);
    if (entry->dpe_str == NULL) {
        MFU_LOG(MFU_LOG_ERR, "failed to allocate property attribute %s", prop_str);
        rc = 1;
        goto out;
    }
    status = H5Aread(cont_attr, attr_dtype, entry->dpe_str);
    if (status < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to read property attribute %s", prop_str);
        rc = 1;
        goto out;
    }
    status = H5Aclose(cont_attr);
    if (status < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to close property attribute %s", prop_str);
        rc = 1;
        goto out;
    }
    status = H5Tclose(attr_dtype);
    if (status < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to close attribute datatype");
        rc = 1;
        goto out;
    }
out:
    return rc;
}

static int cont_deserialize_prop_uint(struct hdf5_args* hdf5,
                                      struct daos_prop_entry* entry,
                                      const char* prop_str)
{
    hid_t   status = 0;
    int     rc = 0;
    hid_t   cont_attr;
    hid_t   attr_dtype;

    cont_attr = H5Aopen(hdf5->file, prop_str, H5P_DEFAULT);
    if (cont_attr < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to open property attribute %s", prop_str);
        rc = 1;
        goto out;
    }
    attr_dtype = H5Aget_type(cont_attr);
    if (attr_dtype < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to open property attribute type %s", prop_str);
        rc = 1;
        goto out;
    }
    status = H5Aread(cont_attr, attr_dtype, &entry->dpe_val);
    if (status < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to read property attribute %s", prop_str);
        rc = 1;
        goto out;
    }
    status = H5Aclose(cont_attr);
    if (status < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to close property attribute %s", prop_str);
        rc = 1;
        goto out;
    }
    status = H5Tclose(attr_dtype);
    if (status < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to close attribute datatype", prop_str);
        rc = 1;
        goto out;
    }
out:
    return rc;
}

static int cont_deserialize_prop_roots(struct hdf5_args* hdf5,
                                       struct daos_prop_entry* entry,
                                       const char* prop_str,
                                       struct daos_prop_co_roots *roots)
{
    hid_t                       status = 0;
    int                         rc = 0;
    int                         i = 0;
    int                         ndims = 0;
    obj_id_t                    *root_oids;
    htri_t                      roots_exist;
    hid_t                       cont_attr = -1;
    hid_t                       attr_dtype = -1;
    hid_t                       attr_dspace = -1;
    hsize_t                     attr_dims[1];
    size_t                      attr_dtype_size;

    /* First check if the ACL attribute exists. */
    roots_exist = H5Aexists(hdf5->file, prop_str);
    if (roots_exist < 0) {
        /* Actual error */
        MFU_LOG(MFU_LOG_ERR, "failed to open property attribute type %s", prop_str);
        rc = 1;
        goto out;
    } else if (roots_exist == 0) {
        /* Does not exist, but that's okay. */
        rc = 0;
        goto out;
    }
    cont_attr = H5Aopen(hdf5->file, prop_str, H5P_DEFAULT);
    if (cont_attr < 0) {
        /* Could not open, but that's okay. */
        rc = 0;
        goto out;
    }
    attr_dtype = H5Aget_type(cont_attr);
    if (attr_dtype < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to open property attribute type %s", prop_str);
        rc = 1;
        goto out;
    }
    attr_dtype_size = H5Tget_size(attr_dtype);
    if (attr_dtype_size < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to open property attribute type %s", prop_str);
        rc = 1;
        goto out;
    }
    attr_dspace = H5Aget_space(cont_attr);
    if (status < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to read acl dspace");
        rc = 1;
        goto out;
    }
    ndims = H5Sget_simple_extent_dims(attr_dspace, attr_dims, NULL);
    if (ndims < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to get dimensions of dspace");
        rc = 1;
        goto out;
    }
    root_oids = MFU_CALLOC(attr_dims[0], sizeof(obj_id_t));
    if (root_oids == NULL) {
        rc = ENOMEM;
        goto out;
    }
    /* freed ahead of daos_prop_free in daos_cont_deserialize_connect */
    roots = MFU_CALLOC(1, sizeof(struct daos_prop_co_roots));
    if (roots == NULL) {
        rc = ENOMEM;
        goto out;
    }
    status = H5Aread(cont_attr, attr_dtype, root_oids);
    if (status < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to read property attribute %s", prop_str);
        rc = 1;
        goto out;
    }
    entry->dpe_val_ptr = (void*)roots;
    for (i = 0; i < 4; i++) {
        roots->cr_oids[i].hi = root_oids[i].hi;
        roots->cr_oids[i].lo = root_oids[i].lo;
    }
out:
    if (cont_attr >= 0) {
        status = H5Aclose(cont_attr);
    }
    if (attr_dtype >= 0) {
        status = H5Tclose(attr_dtype);
    }
    if (attr_dspace >= 0) {
        status = H5Sclose(attr_dspace);
    }
    mfu_free(&root_oids);
    return rc;
}

static int cont_deserialize_prop_acl(struct hdf5_args* hdf5,
                                     struct daos_prop_entry* entry,
                                     const char* prop_str)
{
    hid_t           status = 0;
    int             rc = 0;
    int             ndims = 0;
    const char      **rdata = NULL;
    struct daos_acl *acl;
    htri_t          acl_exist;
    hid_t           cont_attr;
    hid_t           attr_dtype;
    hid_t           attr_dspace;
    hsize_t         attr_dims[1];

    /* First check if the ACL attribute exists. */
    acl_exist = H5Aexists(hdf5->file, prop_str);
    if (acl_exist < 0) {
        /* Actual error */
        MFU_LOG(MFU_LOG_ERR, "failed to open property attribute type %s", prop_str);
        rc = 1;
        goto out;
    } else if (acl_exist == 0) {
        /* Does not exist, but that's okay. */
        rc = 0;
        goto out;
    }

    cont_attr = H5Aopen(hdf5->file, prop_str, H5P_DEFAULT);
    if (cont_attr < 0) {
        /* Could not open, but that's okay. */
        rc = 0;
        goto out;
    }
    attr_dtype = H5Aget_type(cont_attr);
    if (attr_dtype < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to open property attribute type %s", prop_str);
        rc = 1;
        goto out;
    }
    attr_dspace = H5Aget_space(cont_attr);
    if (status < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to read acl dspace");
        rc = 1;
        goto out;
    }
    ndims = H5Sget_simple_extent_dims(attr_dspace, attr_dims, NULL);
    if (ndims < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to get dimensions of dspace");
        rc = 1;
        goto out;
    }
    rdata = MFU_CALLOC(attr_dims[0], sizeof(char*));
    if (rdata == NULL) {
        rc = ENOMEM;
        goto out;
    }
    attr_dtype = H5Tcopy(H5T_C_S1);
    if (status < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to create dtype");
        rc = 1;
        goto out;
    }
    status = H5Tset_size(attr_dtype, H5T_VARIABLE);
    if (status < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to set acl dtype size");
        rc = 1;
        goto out;
    }
    status = H5Aread(cont_attr, attr_dtype, rdata);
    if (status < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to read property attribute %s", prop_str);
        rc = 1;
        goto out;
    }
    /* convert acl strings back to struct acl, then store in entry */
    rc = daos_acl_from_strs(rdata, (size_t)attr_dims[0], &acl);
    if (rc != 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to convert acl strs");
        goto out;
    }
    entry->dpe_val_ptr = (void*)acl;
    status = H5Aclose(cont_attr);
    if (status < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to close property attribute %s", prop_str);
        rc = 1;
        goto out;
    }
    status = H5Tclose(attr_dtype);
    if (status < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to close attribute datatype");
        rc = 1;
        goto out;
    }
    status = H5Sclose(attr_dspace);
    if (status < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to close attribute dataspace");
        rc = 1;
        goto out;
    }
out:
    mfu_free(&rdata);
    return rc;
}

int cont_deserialize_usr_attrs(struct hdf5_args* hdf5,
                               daos_handle_t coh)
{
    hid_t       status = 0;
    int         rc = 0;
    int         num_attrs = 0;
    int         num_dims = 0;
    char**      names = NULL;
    void**      buffers = NULL;
    size_t*     sizes = NULL;
    hid_t       dset = 0;
    hid_t       dspace = 0;
    hid_t       vtype = 0;
    hsize_t     dims[1];
    usr_attr_t* attr_data = NULL;

    /* Read the user attributes */
    dset= H5Dopen(hdf5->file, "User Attributes", H5P_DEFAULT);
    if (dset < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to open user attributes dset");
        rc = 1;
        goto out;
    }
    dspace = H5Dget_space(dset);
    if (dspace < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to get user attributes dspace");
        rc = 1;
        goto out;
    }
    vtype = H5Dget_type(dset);
    if (vtype < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to get user attributes vtype");
        rc = 1;
        goto out;
    }
    num_dims = H5Sget_simple_extent_dims(dspace, dims, NULL);
    if (num_dims < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to get user attributes dimensions");
        rc = 1;
        goto out;
    }
    num_attrs = dims[0];
    attr_data = MFU_CALLOC(dims[0], sizeof(usr_attr_t));
    if (attr_data == NULL) {
        MFU_LOG(MFU_LOG_ERR, "failed to allocate user attributes");
        rc = 1;
        goto out;
    }
    status = H5Dread(dset, vtype, H5S_ALL, H5S_ALL,
                     H5P_DEFAULT, attr_data);
    if (status < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to read user attributes data");
        rc = 1;
        goto out;
    }

    names = MFU_CALLOC(num_attrs, sizeof(char*));
    if (names == NULL) {
        MFU_LOG(MFU_LOG_ERR, "failed to allocate user attributes");
        rc = 1;
        goto out;
    }
    buffers = MFU_CALLOC(num_attrs, sizeof(void*));
    if (buffers == NULL) {
        MFU_LOG(MFU_LOG_ERR, "failed to allocate user attributes");
        rc = 1;
        goto out;
    }
    sizes = MFU_CALLOC(num_attrs, sizeof(size_t));
    if (sizes == NULL) {
        MFU_LOG(MFU_LOG_ERR, "failed to allocate user attributes");
        rc = 1;
        goto out;
    }

    /* Set the user attribute buffers */
    for (int i = 0; i < num_attrs; i++) {
        names[i] = attr_data[i].attr_name;
        buffers[i] = attr_data[i].attr_val.p;
        sizes[i] = attr_data[i].attr_val.len;
    }

    rc = daos_cont_set_attr(coh, num_attrs,
                            (const char * const*) names,
                            (const void * const*) buffers,
                            sizes, NULL);
    if (rc != 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to set user attributes "DF_RC, DP_RC(rc));
        rc = 1;
        goto out;
    }

out:
    H5Dclose(dset);
    H5Sclose(dspace);
    H5Tclose(vtype);
    cont_free_usr_attrs(num_attrs, &names, &buffers, &sizes);
    mfu_free(&attr_data);
    return rc;
}

int cont_deserialize_all_props(struct hdf5_args *hdf5, 
                               daos_prop_t **_prop,
                               struct daos_prop_co_roots *roots,
                               daos_cont_layout_t *cont_type,
                               daos_handle_t poh)
{
    int                     rc = 0;
    bool                    deserialize_label = false;
    uint32_t                num_props = 18;
    daos_prop_t             *label = NULL;
    daos_prop_t             *prop = NULL;
    struct daos_prop_entry  *entry;
    struct daos_prop_entry  *label_entry;
    daos_handle_t           coh;
    daos_cont_info_t        cont_info = {0};

    label = daos_prop_alloc(1);
    if (label == NULL) {
        return ENOMEM;
    }
    label->dpp_entries[0].dpe_type = DAOS_PROP_CO_LABEL; 

    /* read the container label entry to decide if it should be added
     * to property list. The container label is required to be unique in
     * DAOS, which is why it is handled differently than the other 
     * container properties. If the label already  exists in the
     * pool then this property will be skipped for deserialization */
    label_entry = &label->dpp_entries[0];
    rc = cont_deserialize_prop_str(hdf5, label_entry, "DAOS_PROP_CO_LABEL");
    if (rc != 0) {
        goto out;
    }

    rc = daos_cont_open(poh, label_entry->dpe_str, DAOS_COO_RW, &coh, &cont_info, NULL);
    if (rc == -DER_NONEXIST) {
        /* doesn't exist so ok to deserialize this container label */
        deserialize_label = true;
    } else if (rc != 0) {
        MFU_LOG(MFU_LOG_ERR, "daos_cont_open failed: "DF_RC, DP_RC(rc));
        goto out;
    }  else {
        /* if this succeeds then label already exists, close container after
         * checking */
        rc = daos_cont_close(coh, NULL);
        if (rc != 0) {
            goto out;
        }
    }

    if (deserialize_label) {
        num_props++;
    }

    prop = daos_prop_alloc(num_props);
    if (prop == NULL) {
        return ENOMEM;
    }

    prop->dpp_entries[0].dpe_type = DAOS_PROP_CO_LAYOUT_TYPE;
    prop->dpp_entries[1].dpe_type = DAOS_PROP_CO_LAYOUT_VER;
    prop->dpp_entries[2].dpe_type = DAOS_PROP_CO_CSUM;
    prop->dpp_entries[3].dpe_type = DAOS_PROP_CO_CSUM_CHUNK_SIZE;
    prop->dpp_entries[4].dpe_type = DAOS_PROP_CO_CSUM_SERVER_VERIFY;
    prop->dpp_entries[5].dpe_type = DAOS_PROP_CO_REDUN_FAC;
    prop->dpp_entries[6].dpe_type = DAOS_PROP_CO_REDUN_LVL;
    prop->dpp_entries[7].dpe_type = DAOS_PROP_CO_SNAPSHOT_MAX;
    prop->dpp_entries[8].dpe_type = DAOS_PROP_CO_COMPRESS;
    prop->dpp_entries[9].dpe_type = DAOS_PROP_CO_ENCRYPT;
    prop->dpp_entries[10].dpe_type = DAOS_PROP_CO_OWNER;
    prop->dpp_entries[11].dpe_type = DAOS_PROP_CO_OWNER_GROUP;
    prop->dpp_entries[12].dpe_type = DAOS_PROP_CO_DEDUP;
    prop->dpp_entries[13].dpe_type = DAOS_PROP_CO_DEDUP_THRESHOLD;
    prop->dpp_entries[14].dpe_type = DAOS_PROP_CO_EC_CELL_SZ;
    prop->dpp_entries[15].dpe_type = DAOS_PROP_CO_ALLOCED_OID;
    prop->dpp_entries[16].dpe_type = DAOS_PROP_CO_ACL;
    prop->dpp_entries[17].dpe_type = DAOS_PROP_CO_ROOTS;
    if (deserialize_label) {
        prop->dpp_entries[18].dpe_type = DAOS_PROP_CO_LABEL; 
    }

    entry = &prop->dpp_entries[0];
    rc = cont_deserialize_prop_uint(hdf5, entry, "DAOS_PROP_CO_LAYOUT_TYPE");
    if (rc != 0) {
        goto out;
    }

    entry = &prop->dpp_entries[1];
    rc = cont_deserialize_prop_uint(hdf5, entry, "DAOS_PROP_CO_LAYOUT_VER");
    if (rc != 0) {
        goto out;
    }

    entry = &prop->dpp_entries[2];
    rc = cont_deserialize_prop_uint(hdf5, entry, "DAOS_PROP_CO_CSUM");
    if (rc != 0) {
        goto out;
    }

    entry = &prop->dpp_entries[3];
    rc = cont_deserialize_prop_uint(hdf5, entry, "DAOS_PROP_CO_CSUM_CHUNK_SIZE");
    if (rc != 0) {
        goto out;
    }

    entry = &prop->dpp_entries[4];
    rc = cont_deserialize_prop_uint(hdf5, entry, "DAOS_PROP_CO_CSUM_SERVER_VERIFY");
    if (rc != 0) {
        goto out;
    }

    entry = &prop->dpp_entries[5];
    rc = cont_deserialize_prop_uint(hdf5, entry, "DAOS_PROP_CO_REDUN_FAC");
    if (rc != 0) {
        goto out;
    }

    entry = &prop->dpp_entries[6];
    rc = cont_deserialize_prop_uint(hdf5, entry, "DAOS_PROP_CO_REDUN_LVL");
    if (rc != 0) {
        goto out;
    }

    entry = &prop->dpp_entries[7];
    rc = cont_deserialize_prop_uint(hdf5, entry, "DAOS_PROP_CO_SNAPSHOT_MAX");
    if (rc != 0) {
        goto out;
    }

    entry = &prop->dpp_entries[8];
    rc = cont_deserialize_prop_uint(hdf5, entry, "DAOS_PROP_CO_COMPRESS");
    if (rc != 0) {
        goto out;
    }

    entry = &prop->dpp_entries[9];
    rc = cont_deserialize_prop_uint(hdf5, entry, "DAOS_PROP_CO_ENCRYPT");
    if (rc != 0) {
        goto out;
    }

    entry = &prop->dpp_entries[10];
    rc = cont_deserialize_prop_str(hdf5, entry, "DAOS_PROP_CO_OWNER");
    if (rc != 0) {
        goto out;
    }

    entry = &prop->dpp_entries[11];
    rc = cont_deserialize_prop_str(hdf5, entry, "DAOS_PROP_CO_OWNER_GROUP");
    if (rc != 0) {
        goto out;
    }

    entry = &prop->dpp_entries[12];
    rc = cont_deserialize_prop_uint(hdf5, entry, "DAOS_PROP_CO_DEDUP");
    if (rc != 0) {
        goto out;
    }

    entry = &prop->dpp_entries[13];
    rc = cont_deserialize_prop_uint(hdf5, entry, "DAOS_PROP_CO_DEDUP_THRESHOLD");
    if (rc != 0) {
        goto out;
    }

    entry = &prop->dpp_entries[14];
    rc = cont_deserialize_prop_uint(hdf5, entry, "DAOS_PROP_CO_EC_CELL_SZ");
    if (rc != 0) {
        goto out;
    }

    entry = &prop->dpp_entries[15];
    rc = cont_deserialize_prop_uint(hdf5, entry, "DAOS_PROP_CO_ALLOCED_OID");
    if (rc != 0) {
        goto out;
    }

    entry = &prop->dpp_entries[16];
    /* read acl as a list of strings in deserialize, then convert
     * back to acl for property entry
     */
    rc = cont_deserialize_prop_acl(hdf5, entry, "DAOS_PROP_CO_ACL");
    if (rc != 0) {
        goto out;
    }

    entry = &prop->dpp_entries[17];
    rc = cont_deserialize_prop_roots(hdf5, entry, "DAOS_PROP_CO_ROOTS", roots);
    if (rc != 0) {
        goto out;
    }

    if (deserialize_label) {
        prop->dpp_entries[18].dpe_str = strdup(label_entry->dpe_str);
    }
    *cont_type = prop->dpp_entries[0].dpe_val;
    *_prop = prop;
out:
    daos_prop_free(label);
    return rc;
}

/* on rank 0, connect to pool, read cont properties because
 * MAX_OID can only works if set on container creation,
 * and then broadcast handles to all ranks */
int daos_cont_deserialize_connect(daos_args_t *daos_args,
                                  struct hdf5_args *hdf5,
                                  daos_cont_layout_t *cont_type,
                                  char *label)
{
    int                         rc = 0;
    daos_prop_t                 *prop = NULL;
    struct daos_prop_co_roots   roots = {0};

    /* generate container UUID */
    uuid_generate(daos_args->dst_cont_uuid);

    daos_pool_info_t pool_info = {0};
    daos_cont_info_t co_info = {0};
#if DAOS_API_VERSION_MAJOR < 1
    rc = daos_pool_connect(daos_args->src_pool, NULL, NULL, DAOS_PC_RW,
                           &daos_args->src_poh, &pool_info, NULL);
#else
    rc = daos_pool_connect(daos_args->src_pool, NULL, DAOS_PC_RW,
                           &daos_args->src_poh, &pool_info, NULL);
#endif
    if (rc != 0) {
        MFU_LOG(MFU_LOG_ERR, "Failed to connect to pool: "DF_RC, DP_RC(rc));
        goto out;
    }

    /* need to read cont props before creating container, then
     * broadcast handles to the rest of the ranks */
    rc = cont_deserialize_all_props(hdf5, &prop, &roots, cont_type,
                                    daos_args->src_poh);
    if (rc != 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to deserialize container properties");
        goto out;
    }

    if (label != NULL) {
        rc = daos_cont_create_with_label(daos_args->src_poh, label, prop, &daos_args->dst_cont_uuid, NULL);
    } else {
        rc = daos_cont_create(daos_args->src_poh, daos_args->dst_cont_uuid, prop, NULL);
    }
    if (rc != 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to create container: "DF_RC, DP_RC(rc));
        goto out;
    }
    char cont_str[130];
    if (label != NULL) {
        MFU_LOG(MFU_LOG_INFO, "Successfully created container %s", label);
        rc = daos_cont_open(daos_args->src_poh, label, DAOS_COO_RW,
                            &daos_args->src_coh, &co_info, NULL);
    } else {
        uuid_unparse(daos_args->dst_cont_uuid, cont_str);
        MFU_LOG(MFU_LOG_INFO, "Successfully created container %s", cont_str);
        rc = daos_cont_open(daos_args->src_poh, daos_args->dst_cont_uuid,
                            DAOS_COO_RW, &daos_args->src_coh, &co_info, NULL);
    }
    if (rc != 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to open container: "DF_RC, DP_RC(rc));
        goto out;
    }
    mfu_free(&roots);
    daos_prop_free(prop);
out:
    return rc;
}

int daos_cont_deserialize_hdlr(int rank, daos_args_t *da, const char *h5filename,
                               mfu_daos_stats_t* stats)
{
    int                     rc = 0;
    int                     i = 0;
    struct                  hdf5_args hdf5;
    daos_obj_id_t           oid;
    daos_handle_t           oh;
    uint64_t                dk_off = 0;
    uint64_t                dk_next = 0;
    uint64_t                total_dkeys_this_oid = 0;
    hid_t                   status = 0;
    float                   version;
    bool                    is_kv = false;

    /* init HDF5 args */
    init_hdf5_args(&hdf5);

    printf("\tDeserializing filename: %s\n", h5filename);

    /* open passed in HDF5 file */
    hdf5.file = H5Fopen(h5filename, H5F_ACC_RDONLY, H5P_DEFAULT);
    if (hdf5.file < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to open hdf5 file");
        rc = 1;
        goto out;
    }

    /* deserialize version -- serialization version/format should
     * be compatible with deserialization version
     */
    hdf5.version_attr = H5Aopen(hdf5.file, "Version", H5P_DEFAULT);
    if (hdf5.version_attr < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to open version attr");
        rc = 1;
        goto out;
    }
    hdf5.attr_dtype = H5Aget_type(hdf5.version_attr);
    if (hdf5.attr_dtype < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to get attr type");
        rc = 1;
        goto out;
    }
    status = H5Aread(hdf5.version_attr, hdf5.attr_dtype, &version); 
    if (status < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to read version");
        rc = 1;
        goto out;
    }
    if (version > 0.0) {
        MFU_LOG(MFU_LOG_ERR, "serialization format is not compatible with "
                "deserialization version\n");
        rc = 1;
        goto out;
    }

    /* deserialize and set the user attributes if they exist */
    htri_t usr_attrs_exist = H5Lexists(hdf5.file, "User Attributes", H5P_DEFAULT);
    if (usr_attrs_exist > 0) {
        rc = cont_deserialize_usr_attrs(&hdf5, da->src_coh);
        if (rc != 0) {
            rc = 1;
            goto out;
        }
    }

    rc = hdf5_read_key_data(&hdf5);
    if (rc != 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to read hdf5 key data");
        rc = 1;
        goto out;
    }
    for (i = 0; i < (int)hdf5.oid_dims[0]; i++) {
        oid.lo = hdf5.oid_data[i].oid_low;
        oid.hi = hdf5.oid_data[i].oid_hi;
	is_kv = obj_is_kv(oid);
        if (is_kv) {
            rc = daos_kv_open(da->src_coh, oid, DAOS_OO_RW, &oh, NULL);
            if (rc != 0) {
                MFU_LOG(MFU_LOG_ERR, "Failed to open kv object: "DF_RC, DP_RC(rc));
                rc = 1;
                goto out;
            }
        } else {
            rc = daos_obj_open(da->src_coh, oid, 0, &oh, NULL);
            if (rc != 0) {
                MFU_LOG(MFU_LOG_ERR, "failed to open object: "DF_RC, DP_RC(rc));
                goto out;
            }
        }
        dk_off = hdf5.oid_data[i].dkey_offset;
        dk_next = 0;
        total_dkeys_this_oid = 0;
        if (i + 1 < (int)hdf5.oid_dims[0]) {
            dk_next = hdf5.oid_data[i + 1].dkey_offset;
            total_dkeys_this_oid = dk_next - dk_off;
        } else if (i == ((int)hdf5.oid_dims[0] - 1)){
            total_dkeys_this_oid = (int)hdf5.dkey_dims[0] - (dk_off);
        } 
        rc = cont_deserialize_keys(&hdf5, &total_dkeys_this_oid, &dk_off, &oh, stats);
        if (rc != 0) {
            MFU_LOG(MFU_LOG_ERR, "failed to deserialize keys: %d", rc);
            goto out;
        }
        if (is_kv) {
            rc = daos_kv_close(oh, NULL);
            if (rc != 0) {
                MFU_LOG(MFU_LOG_ERR, "failed to close kv object: "DF_RC, DP_RC(rc));
                goto out;
            }
        } else {
            rc = daos_obj_close(oh, NULL);
            if (rc != 0) {
                MFU_LOG(MFU_LOG_ERR, "failed to close object: "DF_RC, DP_RC(rc));
                goto out;
            }
        }

        /* Increment as we go */
        stats->total_oids++;
    }
out:
    if (hdf5.oid_dset > 0) {
        H5Dclose(hdf5.oid_dset);
    }
    if (hdf5.dkey_dset > 0) {
        H5Dclose(hdf5.dkey_dset);
    }
    if (hdf5.akey_dset > 0) {
        H5Dclose(hdf5.akey_dset);
    }
    if (hdf5.oid_dspace > 0) {
        H5Sclose(hdf5.oid_dspace);
    }
    if (hdf5.dkey_dspace > 0) {
        H5Sclose(hdf5.dkey_dspace);
    }
    if (hdf5.akey_dspace > 0) {
        H5Sclose(hdf5.akey_dspace);
    }
    if (hdf5.oid_dtype > 0) {
        H5Tclose(hdf5.oid_dtype);
    }
    if (hdf5.dkey_vtype > 0) {
        H5Tclose(hdf5.dkey_vtype);
    }
    if (hdf5.akey_vtype > 0) {
        H5Tclose(hdf5.akey_vtype);
    }
    if (hdf5.file > 0) {
        H5Fclose(hdf5.file);
    }
    return rc;
}

int mfu_daos_hdf5_copy(char **argpaths,
                       daos_args_t *daos_args)
{
    int                 rc = 0;
    int                 size;
    char                src_path[FILENAME_LEN];
    char                dst_path[FILENAME_LEN];
    char                dst_cont_str[UUID_LEN];
    struct duns_attr_t  src_dattr = {0};
    struct duns_attr_t  dst_dattr = {0};
    bool                src_daos = false;
    bool                dst_daos = false;
    int                 len = 0;

    MPI_Comm_size(MPI_COMM_WORLD, &size);
    if (size > 1) {
        MFU_LOG(MFU_LOG_ERR, "dcp uses h5repack to copy DAOS HDF5 containers "
                "to and from a POSIX filesystem. Only one process "
                "is currently supported. Restart program using only "
                "one MPI process "
                MFU_ERRF, MFU_ERRP(-MFU_ERR_INVAL_ARG));
                rc = 1;
                return rc;
    }

    /* check for DAOS path and set VOL Name accordingly */
    rc = duns_resolve_path(argpaths[0], &src_dattr);
    if (rc == 0) {
        src_daos = true;
        snprintf(daos_args->src_pool, DAOS_PROP_LABEL_MAX_LEN + 1, "%s", src_dattr.da_pool);
        snprintf(daos_args->src_cont, DAOS_PROP_LABEL_MAX_LEN + 1, "%s", src_dattr.da_cont);

        /* build src path for h5repack */
        len = snprintf(src_path, FILENAME_LEN, "daos://%s/%s",
                       daos_args->src_pool, daos_args->src_cont);
        if (len > FILENAME_LEN) {
            MFU_LOG(MFU_LOG_ERR, "source path exceeds max length "
                    MFU_ERRF, MFU_ERRP(-MFU_ERR_DCP));
        }
    } else {
        /* source path is POSIX */
        rc = 0;
    }

    rc = duns_resolve_path(argpaths[1], &dst_dattr);
    if (rc == 0) {
        dst_daos = true;
        snprintf(daos_args->dst_pool, DAOS_PROP_LABEL_MAX_LEN + 1, "%s", dst_dattr.da_pool);
        snprintf(daos_args->dst_cont, DAOS_PROP_LABEL_MAX_LEN + 1, "%s", dst_dattr.da_cont);
        bool dst_cont_passed = strlen(daos_args->dst_cont) > 0 ? true : false;
        if (!dst_cont_passed) {
            uuid_generate(daos_args->dst_cont_uuid);
            uuid_unparse(daos_args->dst_cont_uuid, dst_cont_str);
        }

        /* build dst path for h5repack */
        if (dst_cont_passed) {
            len = snprintf(dst_path, FILENAME_LEN, "daos://%s/%s",
                           daos_args->dst_pool, daos_args->dst_cont);
        } else {
            len = snprintf(dst_path, FILENAME_LEN, "daos://%s/%s",
                           daos_args->dst_pool, dst_cont_str);
        }
        if (len > FILENAME_LEN) {
            MFU_LOG(MFU_LOG_ERR, "destination path exceeds max length "
                    MFU_ERRF, MFU_ERRP(-MFU_ERR_DCP));
        }
    } else {
        /* destination path is POSIX */
        rc = 0;
    }

    pid_t child;
    int child_status;
    pid_t child_pid;
    char *args[4];
    args[0] = "h5repack";
    args[1] = NULL;
    args[2] = NULL;
    args[3] = NULL;
    args[4] = NULL;

    if (src_daos && dst_daos) {
        /* this must be a DAOS->DAOS copy */
        args[1] = strdup(src_path);
        args[2] = strdup(dst_path);
        args[3] = NULL;
    } else if (src_daos && !dst_daos) {
        args[1] = strdup("--dst-vol-name=native");
        args[2] = strdup(src_path);
        args[3] = strdup(argpaths[1]);
        args[4] = NULL; 
    } else if (!src_daos && dst_daos) {
        args[1] = strdup("--src-vol-name=native");
        args[2] = strdup(argpaths[0]);
        args[3] = strdup(dst_path);
        args[4] = NULL; 
    } else {
        MFU_LOG(MFU_LOG_ERR, "Invalid Format " MFU_ERRF,
                MFU_ERRP(-MFU_ERR_INVAL_ARG));
        rc = 1;
        return rc;
    }

    /* Child process */
    if ((child = fork()) == 0) {

        /* start h5repack */
        execvp(args[0], args);

        /* if child process reaches this point then execvp must have failed */
        MFU_LOG(MFU_LOG_ERR, "execvp on h5repack failed: "
                MFU_ERRF, MFU_ERRP(-MFU_ERR_DCP));
        mfu_free(&args[1]);
        mfu_free(&args[2]);
        mfu_free(&args[3]);
        mfu_free(&args[4]);
        rc = 1;
        return rc;
    /* Parent process */
    } else {
        if (child == (pid_t)(-1)) {
            MFU_LOG(MFU_LOG_ERR, "fork failed: "
                    MFU_ERRF, MFU_ERRP(-MFU_ERR_DCP));
            mfu_free(&args[1]);
            mfu_free(&args[2]);
            mfu_free(&args[3]);
            mfu_free(&args[4]);
            rc = 1;
            return rc;
        } else {
            /* parent will wait for child to complete */
            child_pid = wait(&child_status);
        }
    }

    /* if child status is 0, then copy was successful */
    if (child_status != 0) {
        MFU_LOG(MFU_LOG_ERR, "errors occurred while copying to/from container: "
                MFU_ERRF, MFU_ERRP(-MFU_ERR_DCP));
        mfu_free(&args[1]);
        mfu_free(&args[2]);
        mfu_free(&args[3]);
        mfu_free(&args[4]);
        rc = 1;
        return rc;
    } else {
        if (dst_daos) {
            MFU_LOG(MFU_LOG_INFO, "Successfully copied to %s", dst_path);
        } else {
            MFU_LOG(MFU_LOG_INFO, "Successfully copied to %s", argpaths[1]);
        }
    }

    mfu_free(&args[1]);
    mfu_free(&args[2]);
    mfu_free(&args[3]);
    mfu_free(&args[4]);
    return rc;
}
#endif
