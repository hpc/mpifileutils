#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <getopt.h>
#include <errno.h>
//#include "handle_args.h"
#include "mfu.h"
#include "strmap.h"
#include "dtcmp.h"

#define FILE_PERMS (S_IRUSR | S_IWUSR)
#define DIR_PERMS  (S_IRWXU)

/* filltype describes what data to put in generated files */
typedef enum {ft_random, ft_true, ft_false, ft_alternate, ft_last} filltype_t ;
static char *filltype_name[]={"random","true","false","alternate"};

/* keep stats during walk */
uint64_t total_dirs    = 0;
uint64_t total_files   = 0;
uint64_t total_links   = 0;
uint64_t total_unknown = 0;
uint64_t total_bytes   = 0;

DTCMP_Op op_dnamcomp, op_tnamcomp;
/*------------------------------------------------------------*/
/* routine for sorting paths in ascending order by final item */
/*------------------------------------------------------------*/
int dnamcomp(const void* a, const void* b)
{
    const char* adir, *bdir;
    adir = (const char*)a;
    bdir = (const char*)b;
    const char* adir2, *bdir2;
    adir2 = adir;
    if (strchr(adir, '/'))  {
        adir2 =  1 + strrchr(adir, '/');
    }
    bdir2 = bdir;
    if (strchr(bdir, '/'))  {
        bdir2 =  1 + strrchr(bdir, '/');
    }
    int rc = strcmp(adir2, bdir2);
    if (rc > 0) {
        rc = 1;
    }
    if (rc < 0) {
        rc = -1;
    }
    return rc;
}

/*------------------------------------------------------------*/
/* routine for sorting paths in ascending order by final item */
/*------------------------------------------------------------*/
int tnamcomp(const void* a, const void* b)
{
    const char* adir, *bdir;
    adir = (const char*)a;
    bdir = (const char*)b;
    const char* adir2, *bdir2;
    adir2 = adir;
    if (strchr(adir, '_'))  {
        adir2 =  1 + strrchr(adir, '_');
    }
    bdir2 = bdir;
    if (strchr(bdir, '_'))  {
        bdir2 =  1 + strrchr(bdir, '_');
    }
    int rc = strcmp(adir2, bdir2);
    if (rc > 0) {
        rc = 1;
    }
    if (rc < 0) {
        rc = -1;
    }
    return rc;
}

static void print_summary(mfu_flist flist, int level)
{
    /* get our rank and the size of comm_world */
    int rank, ranks;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &ranks);

    /* step through and print data */
    uint64_t idx = 0;
    uint64_t max = mfu_flist_size(flist);
    while (idx < max) {
        if (mfu_flist_have_detail(flist)) {
            /* get mode */
            mode_t mode = (mode_t) mfu_flist_file_get_mode(flist, idx);

            /* set file type */
            if (S_ISDIR(mode)) {
                total_dirs++;
            }
            else if (S_ISREG(mode)) {
                total_files++;
            }
            else if (S_ISLNK(mode)) {
                total_links++;
            }
            else {
                /* unknown file type */
                total_unknown++;
            }

            uint64_t size = mfu_flist_file_get_size(flist, idx);
            total_bytes += size;
        }
        else {
            /* get type */
            mfu_filetype type = mfu_flist_file_get_type(flist, idx);

            if (type == MFU_TYPE_DIR) {
                total_dirs++;
            }
            else if (type == MFU_TYPE_FILE) {
                total_files++;
            }
            else if (type == MFU_TYPE_LINK) {
                total_links++;
            }
            else {
                /* unknown file type */
                total_unknown++;
            }
        }

        /* go to next file */
        idx++;
    }

    /* get total directories, files, links, and bytes */
    uint64_t all_dirs, all_files, all_links, all_unknown, all_bytes;
    uint64_t all_count = mfu_flist_global_size(flist);
    MPI_Allreduce(&total_dirs,    &all_dirs,    1, MPI_UINT64_T, MPI_SUM, MPI_COMM_WORLD);
    MPI_Allreduce(&total_files,   &all_files,   1, MPI_UINT64_T, MPI_SUM, MPI_COMM_WORLD);
    MPI_Allreduce(&total_links,   &all_links,   1, MPI_UINT64_T, MPI_SUM, MPI_COMM_WORLD);
    MPI_Allreduce(&total_unknown, &all_unknown, 1, MPI_UINT64_T, MPI_SUM, MPI_COMM_WORLD);
    MPI_Allreduce(&total_bytes,   &all_bytes,   1, MPI_UINT64_T, MPI_SUM, MPI_COMM_WORLD);

    /* convert total size to units */
    if (mfu_debug_level >= MFU_LOG_VERBOSE && rank == 0) {
        MFU_LOG(MFU_LOG_VERBOSE, "");
        MFU_LOG(MFU_LOG_VERBOSE, "  Items: %llu", (unsigned long long) all_count);
        MFU_LOG(MFU_LOG_VERBOSE, "    Directories: %llu", (unsigned long long) all_dirs);
        MFU_LOG(MFU_LOG_VERBOSE, "    Files: %llu", (unsigned long long) all_files);
        MFU_LOG(MFU_LOG_VERBOSE, "    Links: %llu", (unsigned long long) all_links);
        /* MFU_LOG(MFU_LOG_VERBOSE, "  Unknown: %lu", (unsigned long long) all_unknown); */

        if (mfu_flist_have_detail(flist)) {
            double agg_size_tmp;
            const char* agg_size_units;
            mfu_format_bytes(all_bytes, &agg_size_tmp, &agg_size_units);

            uint64_t size_per_file = 0.0;
            if (all_files > 0) {
                size_per_file = (uint64_t)((double)all_bytes / (double)all_files);
            }
            double size_per_file_tmp;
            const char* size_per_file_units;
            mfu_format_bytes(size_per_file, &size_per_file_tmp, &size_per_file_units);

            MFU_LOG(MFU_LOG_VERBOSE, "     Data: %.3lf %s (%.3lf %s per file)", agg_size_tmp, agg_size_units, size_per_file_tmp, size_per_file_units);
        }
    }

    return;
}

/*--------------------------------------*/
/* write specified info to list element */
/*--------------------------------------*/
void fillelem(mfu_flist flist, uint64_t index, char* fname, long int flen, mfu_filetype ftype)
{
    //-----------------------------------------------------------
    // the following numbers are from /usr/include/bits/stats.h
    //-----------------------------------------------------------
    long int fmode;
    if (ftype == MFU_TYPE_DIR) {
        fmode = S_IFDIR;
        fmode |= DIR_PERMS;
    }
    else if (ftype == MFU_TYPE_FILE) {
        fmode = S_IFREG;
        fmode |= FILE_PERMS;
    }
    else if (ftype == MFU_TYPE_LINK) {
        fmode = S_IFLNK;
    }
    else  {
        MFU_LOG(MFU_LOG_ERR,"In fillelem() ftype = %ld is not legal value", ftype);
        exit(0);
    }

    //----------------------------------
    // fill element with information
    //----------------------------------
    mfu_flist_file_set_name(flist,   index, fname);
    mfu_flist_file_set_type(flist,   index, ftype);
    mfu_flist_file_set_detail(flist, index, 1);
    mfu_flist_file_set_mode(flist,   index, (uint64_t) fmode);
    mfu_flist_file_set_size(flist,   index, (uint64_t) flen);
}

static int numfile = 0, numdir = 0, numlink = 0;

//--------------------------------------
// set name and type of flist item
//--------------------------------------
void setname(char* aname, unsigned long int ftype, int n, const char* path)
{
    switch (ftype) {
        case MFU_TYPE_NULL:
            // printf("MFU_TYPE_NULL\n");
            break;
        case MFU_TYPE_UNKNOWN:
            // printf("MFU_TYPE_UNKNOWN\n");
            break;
        case MFU_TYPE_FILE:
            //  printf("MFU_TYPE_FILE\n");
            sprintf(aname, "%s/file_%08d", path, n);
            break;
        case MFU_TYPE_DIR:
            // printf("MFU_TYPE_DIR\n");
            sprintf(aname, "%s/dir_%08d", path, n);
            break;
        case MFU_TYPE_LINK:
            // printf("MFU_TYPE_LINK\n");
            sprintf(aname, "%s/link_%08d", path, n);
            break;
        default:
            // printf("bad value\n");
            break;
    }
}

//------------------------------
// get number from file name
//------------------------------
int getnum(const char* fname)
{
    const char* cp;
    cp = strrchr(fname, '_');
    return atoi(cp + 1);
}

//-----------------------------------
// put nwds ints into buffer
//------------------------------------
void fillbuff(int* ibuff, size_t nwds, filltype_t ft)
{
    int i;
    switch (ft)
    {
        case ft_random:
          for (i = 0; i < nwds; i++) {
            ibuff[i] = rand();
          }
          break;
        case ft_true:
          for (i = 0; i < nwds; i++) {
            ibuff[i] = 0xFFFFFFFF;
          }
          break;
        case ft_false:
          for (i = 0; i < nwds; i++) {
            ibuff[i] = 0x00000000;
          }
          break;
        case ft_alternate:
          for (i = 0; i < nwds; i++) {
            ibuff[i] = 0xAAAAAAAA;
          }
          break;
        default:
          MFU_LOG(MFU_LOG_ERR,"Invalid filltype=%d", ft);
          break;
    }
}

/*
 * buf points to bufsize bytes allocated in main()
 * written to in main()->write_files()->write_file()->fillbuf()
 * used to provide data for write() call in main()->write_files()->write_file()->mfu_write()
 * TODO: eliminate these global variables and pass values through arguments
 */
size_t bufsize = 1024 * 1024 * 4;
char* buf;
size_t size, isize;
int nnum;

/*----------------------------------------------*/
/* add content to a node created by create_file */
/*----------------------------------------------*/
static int write_file(mfu_flist list, uint64_t idx, filltype_t ft)
{
    int rc = 0;

    /* get destination name */
    const char* dest_path = mfu_flist_file_get_name(list, idx);

    /* get destination size */
    uint64_t fsize =  mfu_flist_file_get_size(list, idx);
    size = fsize;
    nnum = getnum(dest_path);
    srand(nnum);
    isize = bufsize/4;
    fillbuff((int*)buf, isize, ft);

    /* open file */
    int fd = mfu_open(dest_path, O_TRUNC | O_WRONLY, S_IRUSR | S_IWUSR);

    /*  write stuff to destination file  */
    if (fd != -1) {
        /* we opened the file, now start writing */
        size_t written = 0;
        char* ptr = (char*) buf;
        while (written < (size_t) size) {
            /* determine amount to write */
            size_t left = fsize;
            size_t remaining = size - written;
            if (remaining < fsize) left = remaining;
            if (left > isize) left = isize;

            /* write data to file */
            ssize_t n = mfu_write(dest_path, fd, ptr, left);
            if (n == -1) {
                MFU_LOG(MFU_LOG_ERR,"Failed to write to file: dest_path=%s errno=%d (%s)", dest_path, errno, strerror(errno));
                rc = 1;
                break;
            }

            /* update amount written */
            written += (size_t) n;
        }

        /* sync output to disk and close the file */
        /* jll temporary  mfu_fsync(dest_path, fd);  */
        mfu_close(dest_path, fd);
    }
    else {
        /* failed to open the file */
        MFU_LOG(MFU_LOG_ERR,"Failed to open file: dest_path=%s errno=%d (%s)", dest_path, errno, strerror(errno));
        rc = 1;
    }

    return rc;
}

/*----------------------------------------------*/
/* add content to nodes created by create_files */
/*----------------------------------------------*/
static int write_files(mfu_flist list, filltype_t ft)
{
    int rc = 0;

    /* get our rank */
    int rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    /* indicate to user what phase we're in */
    if (rank == 0) {
        MFU_LOG(MFU_LOG_INFO, "Writing content to files.");
    }

    /* iterate over items and set write bit on directories if needed */
    uint64_t idx;
    uint64_t size = mfu_flist_size(list);
    for (idx = 0; idx < size; idx++) {
        /* get type of item */
        mfu_filetype type = mfu_flist_file_get_type(list, idx);

        /* process files and links */
        if (type == MFU_TYPE_FILE) {
            /* TODO: skip file if it's not readable */
            write_file(list, idx, ft);
        }
    }

    /* wait for all procs to finish before we start
     * with files at next level */
    MPI_Barrier(MPI_COMM_WORLD);

    return rc;
}

//------------------------------------------------------
// get targets for links from list of target IDs
//-------------------------------------------------------
static void create_targets(int nlevels, int linktot, int* nfiles, uint64_t* targIDs, char** tnames, char** tarray)
{
    int rank, ranks;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &ranks);

    //---------------------------------
    // first get ranges for each level
    //---------------------------------
    uint64_t* ifst = (uint64_t*) MFU_MALLOC(nlevels * sizeof(uint64_t));
    uint64_t* ilst = (uint64_t*) MFU_MALLOC(nlevels * sizeof(uint64_t));
    uint64_t ist = 0;
    int ilev;
    for (ilev = 0; ilev < nlevels; ilev++) {
        ifst[ilev] = ist;
        ilst[ilev] = ist + nfiles[ilev];
        ist = ist + nfiles[ilev];
    }

    int tnamlen = PATH_MAX;
    int i, j;
    for (i = 0; i < linktot; i++) {
        for (ilev = 0; ilev < nlevels; ilev++) {
            if (targIDs[i] >= ifst[ilev] && targIDs[i] < ilst[ilev]) {
                break;
            }
        }
        j = (int)(targIDs[i] - ifst[ilev]);
        strcpy(tarray[i], tnames[ilev] + j * tnamlen);
    }
}

//-------------------------------------------------------------
// writes links to file, dirs, and other links for one proc
//--------------------------------------------------------------
static void write_links(int nlink, char* linknames, char* targnames, int level)
{
    /* get our rank and number of ranks in job */
    int rank, ranks;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &ranks);
    if (rank == 0) {
        MFU_LOG(MFU_LOG_INFO, "Creating and writing links for level %d", level);
    }

    int i;
    int lnamlen = PATH_MAX;
    int tnamlen = PATH_MAX;
    for (i = 0; i < nlink; i++) {
        const char* link_path = linknames + i * lnamlen;
        const char* targ_path = targnames + i * tnamlen;
        //printf("from write_links: mfu_symlink(%s,%s)\n",targ_path,link_path);
        int linkdesc = mfu_symlink(targ_path, link_path);
    }

    return;
}

//----------------------------------------
// sort directories by base (last) name
//----------------------------------------
void dnamsort(char** buff, int nitems)
{
    int i, j;
    char* cp1, *cp2, *cptemp;
    for (j = nitems - 1; j >= 1; j--) {
        for (i = 0; i < j; i++) {
            cp1 = 1 + strrchr(buff[i], '/');
            cp2 = 1 + strrchr(buff[j], '/');
            if (strcmp(cp1, cp2) > 0) {
                cptemp  = buff[i];
                buff[i] = buff[j];
                buff[j] = cptemp;
            }
        }
    }
}

//----------------------------------------
// sort targets by number at end of name
//----------------------------------------
void tnamsort(char** buff, int nitems)
{
    int i, j;
    char* cp1, *cp2, *cptemp;
    for (j = nitems - 1; j >= 1; j--) {
        for (i = 0; i < j; i++) {
            cp1 = 1 + strrchr(buff[i], '_');
            cp2 = 1 + strrchr(buff[j], '_');
            if (strcmp(cp1, cp2) > 0) {
                cptemp  = buff[i];
                buff[i] = buff[j];
                buff[j] = cptemp;
            }
        }
    }
}

//----------------------------------------
// sort links by base (last) name
// and an order index lind with them
//----------------------------------------
void lnamsort(char** buff, int nitems, int* lind)
{
    int i, j, ltemp;
    char* cp1, *cp2, *cptemp;
    for (j = nitems - 1; j >= 1; j--) {
        for (i = 0; i < j; i++) {
            cp1 = 1 + strrchr(buff[i], '/');
            cp2 = 1 + strrchr(buff[j], '/');
            if (strcmp(cp1, cp2) > 0) {
                cptemp  = buff[i];
                ltemp   = lind[i];
                buff[i] = buff[j];
                lind[i] = lind[j];
                buff[j] = cptemp;
                lind[j] = ltemp;
            }
        }
    }
}

//----------------------------------------
//  unsort link names and targIDs
//  with order index to restore order
//----------------------------------------
void lnamunsort(char** buff, char** tarray, int* lind, int nitems)
{
    int i, j;
    int tempi;
    char* cptemp;
    char* cptemp2;
    for (j = nitems - 1; j >= 1; j--) {
        for (i = 0; i < j; i++) {
            if (lind[i] > lind[j]) {
                tempi     = lind[i];
                cptemp    = buff[i];
                cptemp2   = tarray[i];
                lind[i]   = lind[j];
                buff[i]   = buff[j];
                tarray[i] = tarray[j];
                lind[j]   = tempi;
                buff[j]   = cptemp;
                tarray[j] = cptemp2;
            }
        }
    }
}

/*---------------------------------------------------------------------
*  shortopts below are followed by a colon if they take an argument
*---------------------------------------------------------------------*/
static char *short_options = "i:f:d:n:r:s:w:vh";
static struct option long_options[] = {
    {"seed",     1, 0, 'i'},
    {"fill",     1, 0, 'f'},
    {"depth",    1, 0, 'd'},
    {"nitems",   1, 0, 'n'},
    {"ratio",    1, 0, 'r'},
    {"size",     1, 0, 's'},
    {"width",    1, 0, 'w'},
    {"version",  0, 0, 'v'},
    {"help",     0, 0, 'h'},
    {0, 0, 0, 0}
};

/*-----------------------*/
/* Print help message */
/*-----------------------*/
static void print_usage(void)
{
    printf("\n");
    printf("Usage: dfilemaker [options] \n");
    printf("\n");
    printf("Options:\n");
    printf("  -i, --seed=*integer*      - seed to use for random number generation\n");
    printf("  -f, --fill=*filltype*     - filltype = random, true, false, or alternate\n");
    printf("  -d, --depth=*min*-*max*   - directory depth, integers min and max\n");
    printf("  -n, --nitems=*min*-*max*  - number of items, integers min and max (subs for -r and -w)\n");
    printf("  -r, --ratio=*min*-*max*   - ratio of files to directories as a percent (not implemented)\n");
    printf("  -s, --size=*min*-*max*    - file size, integers min and max followed by MB or GB\n");
    printf("  -w, --width=*min*-*max*   - directory width, integers min and max (not implemented)\n");
    printf("  -v, --verbose             - print version number\n");
    printf("  -h, --help                - print usage\n");
    printf("\n");
    printf("For more information see https://mpifileutils.readthedocs.io.\n");
    printf("\n");
    fflush(stdout);
}
/*-----------------------------------------------------------------*/
/*   Extract min and max strings from arguments to certain options */
/*-----------------------------------------------------------------*/
void mmparse(char* optarg,char** minterm,char** maxterm)
{
      char delim[5]="-\n";
      //printf("optarg = %s\n",optarg);
      *minterm=strtok(optarg,delim);
      *maxterm=strtok(0,delim);
}

/*****************************************************************
 *
 *  Create trees of directories, files, links
 *  Usage: dfilemaker [options]
 *
 ****************************************************************/
int main(int narg, char** arg)
{
    char* cbuff;
    uint64_t i, j, ifst, ilst;
    int namlen;
    long int* ftypes, *flens;
    long int maxflen = 2000L;
    int ifrac, *nfiles; // nfiles is number of files at levels from 0 top
    int ntotal=3000;
    int nfsum = 0;
    int nlevels = 5, nsum, ilev; // number of levels with top (./)
    int outlevels, outmin;
    mfu_flist* outlists;
    char* cp;
    char* dirname;
    unsigned int iseed = 1;
    unsigned int jseed = 0;
    int c;
    uint64_t idx;
    uint64_t* idlist;
    int ndir = 0, *ndirs;
    uint64_t size;
    uint64_t* randir;
    int dirtot;
    int linktot;
    int nitot;
    int dfirst, dlast;
    uint64_t* idflist, *idllist;
    int root = 0;
    int dnamlen; // directory name lengths
    int lnamlen; // link name lengths
    int tnamlen; // length of all items
    int* lndirs, *ddispls; // directory displacements for each proc
    int* lnlinks, *ldispls; // directory displacements for each proc
    int* lnitems, *tdispls; // directory displacements for each proc
    char* ldnames, *dnames; // lists of directory paths
    char** larray;
    char** tarray;
    char* lnames; // global lists of link names
    //char** tnames; // global lists of items as targets over all levels
    int nlink, *linksg;
    int nitem, *itemsg;
    uint64_t* targIDs;  // global indices of things that links point to for a dir level
    int* nlinksg;  // number of links for each rank bcast to all procs
    uint64_t* gidlist; // global (gathered) link array
    char* tnamelist; // list of path names of items associated with targIDs
    int* lind; // list of ints in order to resort things
    int initsum, noff;
    static filltype_t filltype = ft_random; // what data to put into files
    unsigned long long sizeminl,sizemaxl;  // for mfu_abtoul
    uint64_t sizemin=0,sizemax=0;
    double ratio=0.;
    int longind=0;
    char *minterm,*maxterm;
    int depmin=0,depmax=0;
    int nmin=0,nmax=0;
    int widmin=0,widmax=0;
    static struct option long_options[] = {
       {"seed",     1, 0, 'i'},
       {"fill",     1, 0, 'f'},
       {"depth",    1, 0, 'd'},
       {"nitems",   1, 0, 'n'},
       {"ratio",    1, 0, 'r'},
       {"size",     1, 0, 's'},
       {"width",    1, 0, 'w'},
       {"version",  0, 0, 'v'},
       {"help",     0, 0, 'h'},
       {0, 0, 0, 0}
    };

    /*--------------------------
     * initialize mfu and MPI
     *--------------------------*/
    MPI_Init(&narg, &arg);
    DTCMP_Init();
    mfu_init();
    mfu_debug_level = MFU_LOG_WARN;

    int rank, nrank;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &nrank);
    MPI_Datatype dirname_type;
    MPI_Datatype tname_type;

   /*---------------------
    *  loop over options
    *---------------------*/
    while (1) {
        c=getopt_long(narg,arg,short_options,long_options,&longind);
        if (c <= 0) break;
        minterm=(char*)MFU_MALLOC(10*sizeof(char));
        maxterm=(char*)MFU_MALLOC(10*sizeof(char));
        switch (c) {
           case 'i':
             jseed = atoi(optarg);
             if (jseed) iseed=jseed;
             break;
           case 'f':
             for (filltype=0;filltype<ft_last;filltype++) {
                 if (strcmp(optarg,filltype_name[filltype])==0) {
                     break;
                 }
             }
             if (filltype==ft_last) {
                 if (rank==0) {
                     MFU_LOG(MFU_LOG_ERR,"%s not a fill option",optarg);
                 }
                 MPI_Finalize();
                 exit(1);
             }
             break;
           case 'd':
             // range for nlevels
             mmparse(optarg,&minterm,&maxterm);
             depmin=atoi(minterm);
             depmax=atoi(maxterm);
             break;
           case 'n':
              // range for ntotal
              mmparse(optarg,&minterm,&maxterm);
              nmin=atoi(minterm);
              nmax=atoi(maxterm);
              break;
            case 'r':
              ratio = atof(optarg);
              if (rank == 0) {
                  MFU_LOG(MFU_LOG_DBG,"ratio = %f",ratio);
              }
              break;
            case 's':
              // range for maxflen
              mmparse(optarg,&minterm,&maxterm);

              /* read file size */
              if (mfu_abtoull(minterm, &sizeminl) != MFU_SUCCESS) {
                if (rank == 0) {
                  MFU_LOG(MFU_LOG_ERR,"Could not interpret %s as file size", minterm);
                }
                mfu_finalize();
                MPI_Finalize();
                return 1;
              }
              sizemin=(uint64_t)sizeminl;
              if (mfu_abtoull(maxterm, &sizemaxl) != MFU_SUCCESS) {
                if (rank == 0) {
                  MFU_LOG(MFU_LOG_ERR,"Could not interpret %s as file size", maxterm);
                }
                mfu_finalize();
                MPI_Finalize();
                return 1;
              }
              sizemax=(uint64_t)sizemaxl;
              break;
            case 'w':
              mmparse(optarg,&minterm,&maxterm);
              widmin=atoi(minterm);
              widmax=atoi(maxterm);
              break;
            case 'v':
              if (mfu_debug_level < MFU_LOG_DBG) {
                  mfu_debug_level++;
              }
              break;
            case 'h':
              if (rank == 0) {
                  print_usage();
              }
              mfu_finalize();
              MPI_Finalize();
              exit(0);
              break;
            default:
              break;
        }
     }

     srand(iseed);
     if (nmax > 0) {
         ntotal=nmin+rand()%(1+nmax-nmin);
     }
     if (depmax > 0) {
         nlevels=depmin+rand()%(1+depmax-depmin);
     }
     if (sizemax > 0) {
         maxflen=sizemin+rand()%(1+sizemax-sizemin);
     }

    /*-------------------------------------------------------
     * each level has nfiles[0] more than the one above
     * on this first pass
     *-------------------------------------------------------*/
    nsum = nlevels * (nlevels + 1) / 2;
    nfiles = (int*) MFU_MALLOC(nlevels * sizeof(int));
    nfiles[0] = ntotal / nsum;
    if (nfiles[0] < 1) {
        if (rank == 0) {
            MFU_LOG(MFU_LOG_ERR,"ntotal must be greater than (levels * (nlevels + 1) /2)");
        }
        MPI_Finalize();
        exit(0);
    }

    if (rank == 0 ) {
        MFU_LOG(MFU_LOG_VERBOSE, "ntotal = %d",ntotal);
        MFU_LOG(MFU_LOG_VERBOSE, "nlevels = %d",nlevels);
        MFU_LOG(MFU_LOG_VERBOSE, "maxflen = %d",maxflen);
    }

    for (ilev = 1; ilev < nlevels; ilev++) {
        nfiles[ilev] = (ilev + 1) * nfiles[0];
    }

    //------------------------------------------------------------
    // adjust nfiles for levels so they sum to ntotal
    //------------------------------------------------------------
    initsum = 0;
    for (ilev = 0; ilev < nlevels; ilev++) {
        initsum += nfiles[ilev];
    }
    if (initsum < ntotal) {
        noff = ntotal - initsum;
        for (i = 0; i < 100; i++) {
            for (ilev = 0; ilev < nlevels; ilev++) {
                nfiles[ilev] += 1;
                noff--;
                if (noff == 0) {
                    break;
                }
            }
            if (noff == 0) {
                break;
            }
        }
    }

    //-----------------------
    // fill buff with stuff
    //-----------------------
    buf = MFU_MALLOC(bufsize);

    //-----------------------------------
    // get depth of './' or top
    //-----------------------------------
    ifrac = (nfiles[0] + nrank - 1) / nrank;
    dirname = (char*) MFU_MALLOC(PATH_MAX + 1);
    mfu_getcwd(dirname, PATH_MAX);

    //---------------------------------------------------
    // instantiate flist and elements of flist top level
    //---------------------------------------------------
    mfu_flist mybflist = mfu_flist_new();
    mfu_flist_set_detail(mybflist, 1);
    ifst = rank * ifrac;
    ilst = (rank + 1) * ifrac;
    if (nfiles[0] < ilst) {
        ilst = nfiles[0];
    }

    //------------------------------------------------------
    // set properties of elements and add to flist for proc
    //------------------------------------------------------
    ftypes = (long int*) MFU_MALLOC(nfiles[0] * sizeof(long int));
    flens  = (long int*) MFU_MALLOC(nfiles[0] * sizeof(long int));
    for (i = 0; i < ifst; i++) {
        rand();
    }
    for (i = ifst; i < ilst; i++) {
        ftypes[i] = rand() % 3 + 2;
    }
    srand(iseed + 100);
    for (i = 0; i < ifst; i++) {
        rand();
    }
    for (i = ifst; i < ilst; i++) {
        flens[i] = rand() % maxflen;
    }
    cbuff = (char*) MFU_MALLOC(strlen(dirname) + 20 * sizeof(char));

    for (i = ifst; i < ilst; i++) {
        setname(cbuff, ftypes[i], i, dirname);
        uint64_t index = mfu_flist_file_create(mybflist);
        fillelem(mybflist, index, cbuff, flens[i], ftypes[i]);
    }

    //----------------------------
    // generate summary of flist
    //-----------------------------
    MPI_Barrier(MPI_COMM_WORLD);
    mfu_flist_summarize(mybflist);

    print_summary(mybflist, 0);
    MPI_Barrier(MPI_COMM_WORLD);
    mfu_free(&ftypes);
    mfu_free(&flens);
    mfu_free(&cbuff);

    //**********************************************************************************************************
    const char* directory_name = (char*) MFU_MALLOC(PATH_MAX + 1);
    char* dir_name = (char*) MFU_MALLOC(PATH_MAX + 1);
    lndirs = (int*) MFU_MALLOC(nrank * sizeof(int));
    ddispls = (int*) MFU_MALLOC(nrank * sizeof(int));
    for (ilev = 1; ilev < nlevels; ilev++) {
        nfsum += nfiles[ilev - 1];
        ifrac = (nfiles[ilev] + nrank - 1) / nrank;
        ifst = rank * ifrac;
        ilst = (rank + 1) * ifrac;
        if (nfiles[ilev] < ilst) {
            ilst = nfiles[ilev];
        }

        //---------------------------------------------------------
        /* get number of levels and number of files at each level */
        //---------------------------------------------------------
        mfu_flist_array_by_depth(mybflist, &outlevels, &outmin, &outlists);
        // if (rank==0) printf("\nnum levels: %d\nminlevel: %d\n\n",outlevels,outmin);

        //-------------------------------------------------
        // get list of items for this level
        //-------------------------------------------------
        mfu_flist list = outlists[ilev - 1];
        size = mfu_flist_size(list);

        //------------------------------------------------
        // list each directory for this level
        //-------------------------------------------------
        idlist = (uint64_t*) MFU_MALLOC(nfiles[ilev - 1] * sizeof(uint64_t)); // directory id's for proc
        ndir = 0;
        for (idx = 0; idx < size; idx++) {
            /* check whether we have a directory */
            mfu_filetype type = mfu_flist_file_get_type(list, idx);
            if (type == MFU_TYPE_DIR) {
                /* print dir name */
                directory_name = mfu_flist_file_get_name(list, idx);
                idlist[ndir] = idx;
                ndir++;
            }
        }
        //       printf("ndir for rank %d = %d, %s\n",rank,ndir,directory_name);
        MPI_Barrier(MPI_COMM_WORLD);
        dnamlen = strlen(dirname) + 20 * ilev; // length of paths above this level
        ldnames = (char*) MFU_MALLOC(ndir * dnamlen); // array to hold path list
        for (i = 0; i < ndir; i++) {
            idx = idlist[i];
            directory_name = mfu_flist_file_get_name(list, idx);
            strcpy(ldnames + i * dnamlen, directory_name);
        }

        //-----------------------------------------------------------
        // count total directories at this level (dirtot)
        // get first and last dir for this processor at this level
        // randomly selected indices stored in randir
        //-----------------------------------------------------------
        ndirs = (int*) MFU_MALLOC(nrank * sizeof(int)); // array to hold ndir for all procs
        MPI_Allgather(&ndir, 1, MPI_INT, ndirs, 1, MPI_INT, MPI_COMM_WORLD);
        dirtot = 0;
        for (i = 0; i < nrank; i++) {
            dirtot += ndirs[i];    // could use MPI_Allreduce for this
        }
        dnames = (char*) MFU_MALLOC(dirtot * dnamlen); // length of names of all dirs in level above
        for (i = 0; i < nrank; i++) {
            lndirs[i] = ndirs[i] * dnamlen;    // total length of all dirnames on this proc
        }
        ddispls[0] = 0;
        for (i = 1; i < nrank; i++) {
            ddispls[i] = ddispls[i - 1] + lndirs[i - 1];    // displacements of dirnames
        }
        MPI_Allgatherv(ldnames, ndir * dnamlen, MPI_CHAR, dnames, lndirs, ddispls, MPI_CHAR, MPI_COMM_WORLD);

        //---------------------------------------------------------------------
        // sort dnames so that randir always points to same directory path
        //---------------------------------------------------------------------
        MPI_Type_contiguous(dnamlen, MPI_CHAR, &dirname_type);
        MPI_Type_commit(&dirname_type);
        if (DTCMP_Op_create(dirname_type, &dnamcomp, &op_dnamcomp) != DTCMP_SUCCESS) {
            MFU_LOG(MFU_LOG_ERR, "Failed to create string sort");
            MPI_Finalize();
            exit(0);
        }
        DTCMP_Sort_local(DTCMP_IN_PLACE, dnames, dirtot, dirname_type, dirname_type, op_dnamcomp, DTCMP_FLAG_NONE);
        DTCMP_Op_free(&op_dnamcomp);
        MPI_Type_free(&dirname_type);

        //-----------------------------------------------------------
        // get first and last dir for this processor at this level
        // randomly selected indices stored in randir
        //-----------------------------------------------------------
        dfirst = 0;
        dlast = 0;
        for (i = 0; i < rank; i++) {
            dfirst += ndirs[i];    // index of first dir for processor
        }
        for (i = 0; i < rank + 1; i++) {
            dlast += ndirs[i];    // index of last dir for proc
        }
        randir = (uint64_t*)  MFU_MALLOC(nfiles[ilev] * sizeof(uint64_t));
        ftypes = (long int*) MFU_MALLOC(nfiles[ilev] * sizeof(long int));
        flens  = (long int*) MFU_MALLOC(nfiles[ilev] * sizeof(long int));

        srand(iseed + 1);
        for (i = 0; i < nfiles[ilev]; i++) {
            randir[i] = (uint64_t)(rand() % dirtot);    // was 305
        }

        srand(iseed + 2);
        for (i = 0; i < nfiles[ilev]; i++) {
            ftypes[i] = (long int)(rand() % 3 + 2);    // item type f,d,l
        }

        srand(iseed + 3);
        for (i = 0; i < nfiles[ilev]; i++) {
            flens[i] = (long int)(rand() % maxflen);    // item length could be zero
        }
        /*
               if (rank==0)
               {
                  printf("randir values for rank=0, nfiles=%d:\n",nfiles[ilev]);
                  for (i=0;i<nfiles[ilev]-1;i++) printf("%d ",randir[i]);
                  printf("%d\n",randir[nfiles[ilev]-1]);
               }
        */

        //------------------------------------------------------------------------
        // get type of object and put in element if there is a directory for it
        //------------------------------------------------------------------------
        namlen = strlen(directory_name) + 20 * sizeof(char);
        cbuff = (char*) MFU_MALLOC(namlen);
        for (i = 0; i < nfiles[ilev]; i++)
            if (randir[i] >= dfirst && randir[i] < dlast) {
                strcpy(dir_name, dnames + randir[i] * dnamlen);
                //printf("dir_name = %s\n",dir_name);
                setname(cbuff, ftypes[i], i + nfsum, dir_name);
                //printf("cbuff = %s\n",cbuff);
                uint64_t index = mfu_flist_file_create(mybflist);
                fillelem(mybflist, index, cbuff, flens[i], ftypes[i]);
            }
        mfu_free(&cbuff);

        //-----------------------------------------------------------
        // pass data for all new elements in RANK ORDER to proc 0
        //-----------------------------------------------------------
        MPI_Barrier(MPI_COMM_WORLD);

        //----------------------------
        // generate summary of flist
        //-----------------------------
        total_dirs    = 0;
        total_files   = 0;
        total_links   = 0;
        total_unknown = 0;
        total_bytes   = 0;
        mfu_flist_summarize(mybflist);
        print_summary(mybflist, ilev);
        MPI_Barrier(MPI_COMM_WORLD);
        mfu_free(&randir);
        mfu_free(&ftypes);
        mfu_free(&flens);
        mfu_free(&idlist);
        mfu_free(&ndirs);
        mfu_free(&ldnames);
        mfu_free(&dnames);

        mfu_flist_array_free(outlevels, &outlists);

        iseed += 3;
    }  // end of ilev loop for creating files and directories

    //**********************************************************************************************************
    //--------------------------------
    //  create directories and files
    //---------------------------------
    mfu_create_opts_t* create_opts = mfu_create_opts_new();
    mfu_flist_mkdir(mybflist, create_opts);
    mfu_flist_mknod(mybflist, create_opts);
    write_files(mybflist, filltype);
    mfu_free(buf); // used only in write_files()->write_file()
    mfu_create_opts_delete(&create_opts);

    //------------------------------------
    //  reset statistics at this point
    //  before writing links
    //------------------------------------
    total_dirs    = 0;
    total_files   = 0;
    total_links   = 0;
    total_unknown = 0;
    total_bytes   = 0;
    // print_summary(mybflist);
    // printf("rank = %d, total_files = %d\n",rank,total_files);

    //*****************************************************************************
    //
    //   make lists of link target items for each level, sorted by number in name
    //   targets may be files, directories, or links
    //
    //*****************************************************************************
    char* itemnames; // local to proc
    char** tnames; // global lists of items as targets over all levels
    tnames = (char**) MFU_MALLOC(nlevels * sizeof(char*));
    itemsg = (int*) MFU_MALLOC(nrank * sizeof(int));
    const char* item_name = (char*) MFU_MALLOC(PATH_MAX + 1);
    lnitems = (int*) MFU_MALLOC(nrank * sizeof(int));
    tdispls = (int*) MFU_MALLOC(nrank * sizeof(int));

    //---------------------------------------------------------
    /* get number of levels and number of files at each level */
    //---------------------------------------------------------
    mfu_flist_array_by_depth(mybflist, &outlevels, &outmin, &outlists);
    for (ilev = 0; ilev < nlevels; ilev++) {
        //------------------------------------------------
        // list items at this level for each processor
        //------------------------------------------------
        mfu_flist list = outlists[ilev];
        size = mfu_flist_size(list);

        //----------------------------------------------------
        // get number everthing at this level on a processor
        //----------------------------------------------------
        tnamlen = PATH_MAX;
        itemnames = (char*) MFU_MALLOC(size * tnamlen);
        nitem = 0;
        for (idx = 0; idx < size; idx++) {
            item_name = mfu_flist_file_get_name(list, idx);
            strcpy(itemnames + nitem * tnamlen, item_name);
            nitem++;
        }
        MPI_Barrier(MPI_COMM_WORLD);

        //-----------------------------------------------
        // make global array of all items at this level
        //-----------------------------------------------
        MPI_Allgather(&nitem, 1, MPI_INT, itemsg, 1, MPI_INT, MPI_COMM_WORLD);  // fill itemsg
        nitot = 0.;
        for (i = 0; i < nrank; i++) {
            nitot += itemsg[i];
        }
        tdispls[0] = 0;
        for (i = 0; i < nrank; i++) {
            lnitems[i] = itemsg[i] * tnamlen;    // total length of all item names on each proc
        }
        for (i = 1; i < nrank; i++) {
            tdispls[i] = tdispls[i - 1] + lnitems[i - 1];    // rel. displacements of item names for each proc
        }
        tnames[ilev] = (char*) MFU_MALLOC(nitot * tnamlen); // length of names of all items in this level
        MPI_Allgatherv(itemnames, nitem * tnamlen, MPI_CHAR, tnames[ilev], lnitems, tdispls, MPI_CHAR, MPI_COMM_WORLD);

        //-------------------------
        // sort tnames[ilev]
        //-------------------------
        MPI_Type_contiguous(tnamlen, MPI_CHAR, &tname_type);
        MPI_Type_commit(&tname_type);
        if (DTCMP_Op_create(tname_type, &tnamcomp, &op_tnamcomp) != DTCMP_SUCCESS) {
            MFU_LOG(MFU_LOG_ERR,"Failed to create string sort");
            exit(0);
        }
        DTCMP_Sort_local(DTCMP_IN_PLACE, tnames[ilev], nitot, tname_type, tname_type, op_tnamcomp, DTCMP_FLAG_NONE);
        DTCMP_Op_free(&op_tnamcomp);
        MPI_Type_free(&tname_type);

        mfu_free(&itemnames);
        MPI_Barrier(MPI_COMM_WORLD);
    }
    mfu_flist_array_free(outlevels, &outlists);

    //**********************************************************************************************************
    //
    //  set up links pointing to any file, dir, or other link
    //  must be indep of number of processor
    //
    //**********************************************************************************************************
    char* linknames; // local to proc
    const char* link_name = (char*) MFU_MALLOC(PATH_MAX + 1);
    lnamlen = PATH_MAX;
    lnlinks = (int*) MFU_MALLOC(nrank * sizeof(int));
    ldispls = (int*) MFU_MALLOC(nrank * sizeof(int));

    //--------------------------------
    // generate links
    //--------------------------------
    mfu_flist_array_by_depth(mybflist, &outlevels, &outmin, &outlists);
    for (ilev = 0; ilev < nlevels; ilev++) {
        //------------------------------------------------
        // list items at this level for each processor
        //------------------------------------------------
        mfu_flist list = outlists[ilev];
        size = mfu_flist_size(list);

        //------------------------------------------------
        // get  number links at this level on a processor
        //-------------------------------------------------
        nlink = 0;
        for (idx = 0; idx < size; idx++) {
            mfu_filetype type = mfu_flist_file_get_type(list, idx);
            if (type == MFU_TYPE_LINK) {
                nlink++;
            }
        }
        linknames = (char*) MFU_MALLOC(nlink * lnamlen);
        nlink = 0;
        for (idx = 0; idx < size; idx++) {
            mfu_filetype type = mfu_flist_file_get_type(list, idx);
            if (type == MFU_TYPE_LINK) {
                link_name = mfu_flist_file_get_name(list, idx);
                strcpy(linknames + nlink * lnamlen, link_name);
                nlink++;
            }
        }
        //if (rank==0)for (i=0;i<nlink;i++) printf("%d %s\n",rank,linknames+i*lnamlen);
        MPI_Barrier(MPI_COMM_WORLD);

        //----------------------------------------------------------
        // make larray, global array of linknames at this dir level
        // should be indep of number of processes
        //----------------------------------------------------------
        srand(200 + iseed);  // seed for list of links
        nlinksg = (int*) MFU_MALLOC(nrank * sizeof(int));  // global array of number of links on each proc
        MPI_Allgather(&nlink, 1, MPI_INT, nlinksg, 1, MPI_INT, MPI_COMM_WORLD);  // fill nlinksg
        linktot = 0;  // total number of links over all procs at this directory level
        for (i = 0; i < nrank; i++) {
            linktot += nlinksg[i];    // could use MPI_Allreduce for this
        }
        targIDs = (uint64_t*) MFU_MALLOC(linktot * sizeof(uint64_t));
        ldispls[0] = 0;
        for (i = 0; i < nrank; i++) {
            lnlinks[i] = nlinksg[i] * lnamlen;    // total length of all linknames on each proc
        }
        for (i = 1; i < nrank; i++) {
            ldispls[i] = ldispls[i - 1] + lnlinks[i - 1];    // rel. displacements of link names for each proc
        }
        lnames = (char*) MFU_MALLOC(linktot * lnamlen); // length of names of all links in this level
        MPI_Allgatherv(linknames, nlink * lnamlen, MPI_CHAR, lnames, lnlinks, ldispls, MPI_CHAR, MPI_COMM_WORLD);

        //-----------------------------------------------
        // create global lnames and separate index array
        //-----------------------------------------------
        larray = (char**) MFU_MALLOC(linktot * sizeof(char*));
        for (i = 0; i < linktot; i++) {
            larray[i] = (char*) MFU_MALLOC(lnamlen * sizeof(char));
        }
        for (i = 0; i < linktot; i++) {
            strncpy(larray[i], lnames + i * lnamlen, lnamlen);
        }
        lind = (int*) MFU_MALLOC(linktot * sizeof(int)); // link index to permit unsorting later
        for (i = 0; i < linktot; i++) {
            lind[i] = i;    // set 0 to linktot
        }

        //---------------------------------------------------------
        // assign random target ID to each linkname is sorted list
        //----------------------------------------------------------
        for (i = 0; i < linktot; i++) {
            targIDs[i] = rand() % ntotal;    // idx from 1 to tot number of items
        }

        //----------------------------------------------------------------
        // sort linknames and associate actual target names with targIDs
        //-----------------------------------------------------------------
        lnamsort(larray, linktot, lind);
        tarray = (char**) MFU_MALLOC(linktot * sizeof(char*));
        for (i = 0; i < linktot; i++) {
            tarray[i] = (char*) MFU_MALLOC(tnamlen * sizeof(char));
        }
        MPI_Barrier(MPI_COMM_WORLD);

        //------------------------
        // create target names
        //-------------------------
        create_targets(nlevels, linktot, nfiles, targIDs, tnames, tarray);

        //-----------------------------------------
        // unsort links and targIDs with them
        // will associate targID's with tnames
        //-----------------------------------------
        lnamunsort(larray, tarray, lind, linktot);
        for (i = 0; i < linktot; i++) {
            strncpy(lnames + i * lnamlen, larray[i], lnamlen);
        }
        mfu_free(&larray);
        tnamelist = (char*) MFU_MALLOC(linktot * tnamlen); // length of names of all links in this level
        for (i = 0; i < linktot; i++) {
            strncpy(tnamelist + i * tnamlen, tarray[i], tnamlen);
        }
        mfu_free(&tarray);
        MPI_Barrier(MPI_COMM_WORLD);

        /*--------------------------------------------------------------------------*/
        /* scatterv linknames (lnames) and target names (tarray)to each processor   */
        /* reuse and redefine lnitems, tdispls,itemnames with length for linklist   */
        /*--------------------------------------------------------------------------*/
        MPI_Scatterv(lnames, lnlinks, ldispls, MPI_CHAR, linknames, nlink * lnamlen, MPI_CHAR, 0, MPI_COMM_WORLD);
        for (i = 0; i < nrank; i++) {
            lnitems[i] = nlinksg[i] * tnamlen;    // total length of all linknames on each proc
        }
        tdispls[0] = 0;
        for (i = 1; i < nrank; i++) {
            tdispls[i] = tdispls[i - 1] + lnitems[i - 1];    // rel. displacements of link names for each proc
        }
        itemnames = (char*) MFU_MALLOC(nlink * tnamlen * sizeof(char));
        MPI_Scatterv(tnamelist, lnitems, tdispls, MPI_CHAR, itemnames, nlink * tnamlen, MPI_CHAR, 0, MPI_COMM_WORLD);
        MPI_Barrier(MPI_COMM_WORLD);

        /*---------------------------------*/
        /* write links with targets        */
        /*---------------------------------*/
        write_links(nlink, linknames, itemnames, ilev); // write links for this processor

        mfu_free(&linknames);
        mfu_free(&tnamelist);
        mfu_free(&nlinksg);
        mfu_free(&targIDs);
        mfu_free(&lnames);
        mfu_free(&lind);
        mfu_free(&itemnames);
        iseed++;
        MPI_Barrier(MPI_COMM_WORLD);
    } /* end of ilev loop for links */
    mfu_flist_array_free(outlevels, &outlists);
    mfu_free(&tnames);

    //****************************************************************************************************

    /*------------
     *  delete
     *------------*/
    mfu_flist_free((void**)&mybflist);

    mfu_finalize();
    DTCMP_Finalize();
    MPI_Finalize();

    return 0;
}

//  vim: et ts=4 :
