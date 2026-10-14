#ifndef COMMON_H
#define COMMON_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>

#include "mfu.h"

/****************************************
 * Test Checking
 ***************************************/

int check_str(const char* label, const char* got, const char* expected);
int check_int(const char* label, const int got, const int expected);
int mpi_check_int(int rank, const char* label, const int got, const int expected);

/****************************************
 * MPI Utilities
 ***************************************/

void broadcast_path(int rank, char** path);

/****************************************
 * File Tree Creation
 ***************************************/

typedef enum TreeEntryType {
    TREE_FILE,
    TREE_DIRECTORY,
    TREE_SYMLINK
} EntryType;

typedef struct FileTreeNode {
    char* name;                     /* name of the tree entry */
    EntryType type;                 /* file, directory, or symlink*/
    size_t child_count;             /* number of directory children */
    struct FileTreeNode** children; /* array of pointers to child nodes*/
    char* symlink_target;           /* path of the symlink target */
} TreeNode;

TreeNode* create_file_node(const char* name);
TreeNode* create_dir_node(const char* name);
TreeNode* create_symlink_node(const char* name, const char* symlink_target);
int add_tree_child(TreeNode *parent, TreeNode *child);
void free_tree(TreeNode* node);
int create_empty_test_tree(TreeNode* node, const char* dest_path, mode_t mode, char** ret_full_path);
int test_tree_dereference_symlinks(TreeNode* node, const char* dest_path);

/****************************************
 * File Tree and Flist Verification
 ***************************************/

int verify_flist_match(int rank, mfu_flist flist, int num_trees, TreeNode** tree_roots, char** parent_paths);
int verify_test_tree_permissions(TreeNode* node, const char* dest_path, mode_t expected_mode);

/****************************************
 * Various Test Scenarios
 ***************************************/

TreeNode* tree_scenario_1(const char* rootpath);
TreeNode* tree_scenario_2(const char* rootpath);
TreeNode* tree_scenario_3(const char* rootpath);
TreeNode* tree_scenario_4(const char* rootpath);
TreeNode* tree_scenario_5(const char* rootpath);
TreeNode* tree_scenario_5_dereferenced(const char* rootpath);

#endif /* COMMON_H */
