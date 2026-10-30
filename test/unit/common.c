#include "common.h"
#include <errno.h>
#include <fcntl.h>
#include <unistd.h>
#include <time.h>
#include <sys/stat.h>

/****************************************
 * File Tree Creation
 ***************************************/

/* Create an entry for the file tree structure */
static TreeNode* create_tree_node(const char* name, EntryType type, const char* symlink_target)
{    
    TreeNode* new_node = (TreeNode*)malloc(sizeof(TreeNode));
    if (!new_node) {
        return NULL;
    }

    new_node->name = (char *)malloc(strlen(name) + 1);
    if (!new_node->name) {
        free(new_node);
        return NULL;
    }

    strcpy(new_node->name, name);
    new_node->type = type;
    new_node->child_count = 0;
    new_node->children = NULL;

    if (new_node->type == TREE_SYMLINK) {
        if (symlink_target == NULL) {
            free(new_node->name);
            free(new_node);
            return NULL;
        }
        new_node->symlink_target = (char *)malloc(strlen(symlink_target) + 1);
        if (!new_node->symlink_target) {
            free(new_node->name);
            free(new_node);
            return NULL;
        }
        strcpy(new_node->symlink_target, symlink_target);
    }

    return new_node;
}

/* Create a file node */
TreeNode* create_file_node(const char* name)
{
    return create_tree_node(name, TREE_FILE, NULL);
}

/* Create a directory node */
TreeNode* create_dir_node(const char* name)
{
    return create_tree_node(name, TREE_DIRECTORY, NULL);
}

/* Create a symlink node */
TreeNode* create_symlink_node(const char* name, const char* symlink_target)
{
    return create_tree_node(name, TREE_SYMLINK, symlink_target);
}

/* Add entry as the child of a parent directory entry */
int add_tree_child(TreeNode *parent, TreeNode *child)
{
    if (!parent || !child || parent->type != TREE_DIRECTORY) {
        return 0;
    }

    size_t new_count = parent->child_count + 1;

    TreeNode **new_children = realloc(parent->children,
                                      new_count * sizeof *new_children);

    if (!new_children) {
        return 0;
    }

    new_children[parent->child_count] = child;
    parent->children = new_children;
    parent->child_count = new_count;

    return 1;
}

/* Free file tree structure from memory */
void free_tree(TreeNode* node) {
    if (!node) {
        return;
    }

    for (size_t i = 0; i < node->child_count; i++) {
        free_tree(node->children[i]);
    }

    if (node->type == TREE_SYMLINK) {
        free(node->symlink_target);
    }
    free(node->children);
    free(node->name);
    free(node);

}

/* Create the file tree structure on the filesystem */
int create_empty_test_tree(TreeNode* node, const char* dest_path, mode_t mode, char** ret_full_path)
{
    if (!node) {
        return 0;
    }

    *ret_full_path = (char *)malloc(strlen(node->name) + strlen(dest_path) + 2);
    if (!*ret_full_path) {
        printf("Error allocating memory for path\n");
        return 0;
    }

    strcpy(*ret_full_path, dest_path);
    strcat(*ret_full_path, "/");
    strcat(*ret_full_path, node->name);

    if (node->type == TREE_DIRECTORY) {
        if (mkdir(*ret_full_path, mode) == -1 && errno != EEXIST) {
            printf("Error creating directory: %s\n", *ret_full_path);
            free(*ret_full_path);
            *ret_full_path = NULL;
            return 0;
        }
    } else if (node->type == TREE_FILE) {
        int fd = open(*ret_full_path, O_WRONLY | O_CREAT | O_EXCL, mode);

        if (fd == -1) {
            printf("Error opening file: %s\n", *ret_full_path);
            free(*ret_full_path);
            *ret_full_path = NULL;
            return 0;
        }

        if (close(fd) == -1) {
            printf("Error closing file: %s\n", *ret_full_path);
            free(*ret_full_path);
            *ret_full_path = NULL;
            return 0;
        }
    } else if (node->type == TREE_SYMLINK) {
        if (symlink(node->symlink_target, *ret_full_path) == -1) {
            printf("Error creating symlink: %s -> %s\n",
                   *ret_full_path, node->symlink_target);
            free(*ret_full_path);
            *ret_full_path = NULL;
            return 0;
        }
    } else {
        printf("Invalid file type.\n");
        free(*ret_full_path);
        *ret_full_path = NULL;
        return 0;
    }

    for (size_t i = 0; i < node->child_count; i++) {
        char* tmp_path;
        if (create_empty_test_tree(node->children[i], *ret_full_path, mode, 
                                   &tmp_path)) {
            free(tmp_path);
        }
        else {
            free(*ret_full_path);
            *ret_full_path = NULL;
            return 0;
        }
    }

    return 1;
}

/* Dereference symlinks in both the TreeNode structure and on the filesystem */
int test_tree_dereference_symlinks(TreeNode* node, const char* dest_path)
{
    if (!node || !dest_path) {
        return 0;
    }

    size_t path_len = strlen(dest_path) + strlen(node->name) + 2;
    char* full_path = (char*)malloc(path_len);
    if (!full_path) {
        printf("Error allocating memory for path\n");
        return 0;
    }
    snprintf(full_path, path_len, "%s/%s", dest_path, node->name);

    if (node->type == TREE_SYMLINK) {
        struct stat st;
        struct timespec times[2];
        
        if (stat(full_path, &st) == -1) {
            printf("Error dereferencing symlink: %s\n", full_path);
            free(full_path);
            return 0;
        }

        /* dereference node */
        if (S_ISREG(st.st_mode)) {
            node->type = TREE_FILE;
        } else if (S_ISDIR(st.st_mode)) {
            node->type = TREE_DIRECTORY;
        } else {
            printf("Symlink points to unsupported type: %s\n", full_path);
            free(full_path);
            return 0;
        }

        if (node->symlink_target) {
            free(node->symlink_target);
            node->symlink_target = NULL;
        }

        /* dereference in filesystem */
        if (unlink(full_path) == -1) {
            printf("Error removing symlink: %s\n", full_path);
            free(full_path);
            return 0;
        }

        if (node->type == TREE_FILE) {
            int fd = open(full_path, O_WRONLY | O_CREAT | O_EXCL, st.st_mode);
            if (fd == -1) {
                printf("Error creating dereferenced file: %s\n", full_path);
                free(full_path);
                return 0;
            }
            close(fd);
        } else if (node->type == TREE_DIRECTORY) {
            if (mkdir(full_path, st.st_mode) == -1) {
                printf("Error creating dereferenced directory: %s\n",
                       full_path);
                free(full_path);
                return 0;
            }
        }

        /* Preserve ownership */
        if (chown(full_path, st.st_uid, st.st_gid) == -1) {
            printf("Warning: could not set ownership for %s\n", full_path);
        }

        /* Preserve permissions */
        if (chmod(full_path, st.st_mode) == -1) {
            printf("Warning: could not set permissions for %s\n", full_path);
        }

        /* Preserve timestamps */
        times[0].tv_sec = st.st_atime;
        times[0].tv_nsec = 0;
        times[1].tv_sec = st.st_mtime;
        times[1].tv_nsec = 0;
        if (utimensat(AT_FDCWD, full_path, times, 0) == -1) {
            printf("Warning: could not set timestamps for %s\n", full_path);
        }
    }

    for (size_t i = 0; i < node->child_count; i++) {
        if (!test_tree_dereference_symlinks(node->children[i], full_path)) {
            free(full_path);
            return 0;
        }
    }

    free(full_path);
    return 1;
}

/****************************************
 * Comparison Between File Tree and flist
 ***************************************/

static TreeNode* find_node_name_match(const char* search_name, TreeNode* node, char* current_path, char** out_path)
{
    /* get the full path */
    size_t path_len = strlen(current_path) + strlen(node->name) + 2;
    char* full_path = (char *)malloc(path_len);
    if (!full_path) {
        printf("Error allocating memory for path\n");
        return NULL;
    }
    snprintf(full_path, path_len, "%s/%s", current_path, node->name);

    /* check for match */
    if (strcmp(search_name, full_path) == 0) {
        *out_path = full_path;  /* caller must free */
        return node;
    }

    /* otherwise, continue searching */
    TreeNode* match_node = NULL;
    for (size_t i = 0; i < node->child_count; i++) {
        match_node = find_node_name_match(search_name, node->children[i],
                                          full_path, out_path);
        if (match_node != NULL) {
            break;
        }
    }

    free(full_path);
    return match_node;
}

static int file_detail_match(mfu_flist* flist, uint64_t i, const char* path)
{
    /* do not follow symlinks */
    struct stat st;
    int stat_rc = lstat(path, &st);
    if (stat_rc != 0) {
        printf("Error stating file: %s\n", path);
        return 0;
    }

    /* verify filetype match */
    mfu_filetype flist_type = mfu_flist_file_get_type(*flist, i);
    int type_matches = 0;

    if (S_ISREG(st.st_mode) && flist_type == MFU_TYPE_FILE) {
        type_matches = 1;
    } else if (S_ISDIR(st.st_mode) && flist_type == MFU_TYPE_DIR) {
        type_matches = 1;
    } else if (S_ISLNK(st.st_mode) && flist_type == MFU_TYPE_LINK) {
        type_matches = 1;
    }

    if (!type_matches) {
        printf("Type mismatch for %s: flist type=%d, fs mode=%o\n",
               path, flist_type, st.st_mode);
        return 0;
    }

    /* check detailed stats if available */
    if (!mfu_flist_have_detail(*flist)) {
        return 1;
    }

    /* verify detailed stats similar to verify_node_stat_in_flist */
    uint64_t flist_elem_mode = mfu_flist_file_get_mode(*flist, i);
    uint64_t flist_elem_uid = mfu_flist_file_get_uid(*flist, i);
    uint64_t flist_elem_gid = mfu_flist_file_get_gid(*flist, i);
    uint64_t flist_elem_size = mfu_flist_file_get_size(*flist, i);
    uint64_t flist_elem_mtime = mfu_flist_file_get_mtime(*flist, i);

    if (flist_elem_mode != st.st_mode) {
        printf("Mode mismatch for %s: flist=%lo, fs=%lo\n",
               path, (unsigned long)flist_elem_mode, (unsigned long)st.st_mode);
        return 0;
    }

    if (flist_elem_uid != st.st_uid) {
        printf("UID mismatch for %s: flist=%lu, fs=%lu\n",
               path, (unsigned long)flist_elem_uid, (unsigned long)st.st_uid);
        return 0;
    }

    if (flist_elem_gid != st.st_gid) {
        printf("GID mismatch for %s: flist=%lu, fs=%lu\n",
               path, (unsigned long)flist_elem_gid, (unsigned long)st.st_gid);
        return 0;
    }

    if (flist_elem_size != st.st_size) {
        printf("Size mismatch for %s: flist=%lu, fs=%lu\n",
               path, (unsigned long)flist_elem_size, (unsigned long)st.st_size);
        return 0;
    }

    if (flist_elem_mtime != st.st_mtime) {
        printf("Mtime mismatch for %s: flist=%lu, fs=%lu\n",
               path, (unsigned long)flist_elem_mtime, (unsigned long)st.st_mtime);
        return 0;
    }

    return 1;
}

static int found_flist_file_match_in_tree(mfu_flist* flist, uint64_t i, TreeNode* root, char* parent_path)
{
    /* find name match in tree */
    const char* flist_file_name = mfu_flist_file_get_name(*flist, i);
    char* match_path = NULL;
    TreeNode* match_node = find_node_name_match(flist_file_name, root,
                                                parent_path, &match_path);
    if (!match_node) {
        return 0;
    }

    if (!file_detail_match(flist, i, match_path)) {
        free(match_path);
        return 0;
    }

    free(match_path);
    return 1;
}

static int found_tree_match_in_flist(mfu_flist* flist, TreeNode* node, const char* current_path)
{
    if (!node) {
        return 1;
    }

    size_t path_len = strlen(current_path) + strlen(node->name) + 2;
    char* full_path = (char *)malloc(path_len);
    if (!full_path) {
        printf("Error allocating memory for path\n");
        return 0;
    }
    snprintf(full_path, path_len, "%s/%s", current_path, node->name);

    int found = 0;
    uint64_t found_idx = 0;
    for (uint64_t i = 0; i < mfu_flist_size(*flist); i++) {
        const char* flist_name = mfu_flist_file_get_name(*flist, i);
        if (strcmp(flist_name, full_path) == 0) {
            found = 1;
            found_idx = i;
            break;
        }
    }

    if (!found) {
        printf("File match not found in flist: %s\n", full_path);
        free(full_path);
        return 0;
    }

    if (!file_detail_match(flist, found_idx, full_path)) {
        free(full_path);
        return 0;
    }

    for (size_t i = 0; i < node->child_count; i++) {
        if (!found_tree_match_in_flist(flist, node->children[i], full_path)) {
            free(full_path);
            return 0;
        }
    }

    free(full_path);
    return 1;
}

static int map_to_rank_0(mfu_flist flist, uint64_t idx, int ranks, const void* args)
{
    return 0;
}

/* Check that all files in the flist is in a tree across all ranks, and
 * that all files in the tree are in a combined flist */
int verify_flist_match(int rank, mfu_flist flist, int num_trees, TreeNode** tree_roots, char** parent_paths)
{
    int verified = 1;

    /* check that every element in flist is in a tree */
    for (uint64_t i = 0; i < mfu_flist_size(flist); i++) {
        int found_match = 0;

        for( int j = 0; j < num_trees; j++) {
            found_match = found_flist_file_match_in_tree(
                &flist, 
                i,
                tree_roots[j], 
                parent_paths[j]
            );
            if (found_match) {
                break;
            }
        }

        if (!found_match){
            printf("flist file match not found in a tree: %s\n",
                   mfu_flist_file_get_name(flist, i));
            verified = 0;
        }
    }

    /* check that every tree is in a combined flist */
    mfu_flist combined_flist = mfu_flist_remap(flist, map_to_rank_0, NULL);
    if (rank == 0) {
        for( int i = 0; i < num_trees; i++) {
            if (!found_tree_match_in_flist(&combined_flist, 
                                           tree_roots[i], parent_paths[i])) {
                verified = 0;
            }
        }
    }
    mfu_flist_free(&combined_flist);

    /* combine verification results from all ranks */
    int all_verified = 0;
    MPI_Allreduce(&verified, &all_verified, 1, MPI_INT, MPI_LAND,
                  MPI_COMM_WORLD);

    return all_verified;
}

/* Check that all tree files have the expected permission mode in the filesystem */
int verify_test_tree_permissions(TreeNode* node, const char* dest_path, mode_t expected_mode)
{
    if (!node) {
        return 1;
    }

    size_t path_len = strlen(dest_path) + strlen(node->name) + 2;
    char* full_path = (char *)malloc(path_len);
    if (!full_path) {
        printf("Error allocating memory for path\n");
        return 0;
    }
    snprintf(full_path, path_len, "%s/%s", dest_path, node->name);

    struct stat st;
    int stat_rc = lstat(full_path, &st);
    if (stat_rc != 0) {
        printf("Error stating file: %s\n", full_path);
        free(full_path);
        return 0;
    }

    mode_t mode = st.st_mode & 0777;
    if (mode != expected_mode) {
        printf("%s has an unexpected permission mode %04o (expected %04o)\n",
               full_path, mode, expected_mode);
        free(full_path);
        return 0;
    }

    for (size_t i = 0; i < node->child_count; i++) {
        if (!verify_test_tree_permissions(node->children[i], full_path, expected_mode)) {
            free(full_path);
            return 0;
        }
    }

    free(full_path);
    return 1;
}

/****************************************
 * MPI Utilities
 ***************************************/

/* Broadcast a single path from rank 0 to all ranks */
void broadcast_path(int rank, char** path)
{
    int path_len = 0;
    if (rank == 0) {
        path_len = strlen(*path) + 1;
    }
    MPI_Bcast(&path_len, 1, MPI_INT, 0, MPI_COMM_WORLD);

    if (rank != 0) {
        *path = (char *)malloc(path_len);
    }
    MPI_Bcast(*path, path_len, MPI_CHAR, 0, MPI_COMM_WORLD);
}

/****************************************
 * Test Checking
 ***************************************/

int check_str(const char* label, const char* got, const char* expected)
{
    if (strcmp(got, expected) == 0) {
        printf("PASS %s\n", label);
        return 0;
    }
    printf("FAIL %s\n  expected %s\n  got      %s\n", label, expected, got);
    return 1;
}

int check_int(const char* label, const int got, const int expected)
{
    if (got == expected) {
        printf("PASS %s\n", label);
        return 0;
    }
    printf("FAIL %s\n  expected %d\n  got      %d\n", label, expected, got);
    return 1;
}

int mpi_check_int(int rank, const char* label, const int got, const int expected)
{
    int check = (got == expected);
    int reduced_check = 0;

    MPI_Allreduce(&check, &reduced_check, 1, MPI_INT, MPI_LAND, MPI_COMM_WORLD);

    if (reduced_check) {
        if (rank == 0) {
            printf("all ranks: PASS %s\n", label);
        }
        return 0;
    }

    if (!check) {
        printf("rank %d: FAIL %s\n  expected %d\n  got      %d\n", 
            rank, label, expected, got);
    }
    return 1;
}