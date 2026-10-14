#include "common.h"

/****************************************
 * Test scenarios that will be created under $TEST_TMPDIR
 ***************************************/

/* <rootpath>/
 *   ├── file1
 *   ├── file2
 *   └── subdir1/
 *       ├── file3
 *       └── subdir2/
 *           └── subdir3/
 *               └── file4
 */
TreeNode* tree_scenario_1(const char* test_name)
{
    TreeNode* root = create_dir_node(test_name);
    TreeNode* file1 = create_file_node("file1");
    TreeNode* file2 = create_file_node("file2");
    TreeNode* subdir1 = create_dir_node("subdir1");
    TreeNode* file3 = create_file_node("file3");
    TreeNode* subdir2 = create_dir_node("subdir2");
    TreeNode* subdir3 = create_dir_node("subdir3");
    TreeNode* file4 = create_file_node("file4");

    add_tree_child(root, file1);
    add_tree_child(root, file2);
    add_tree_child(root, subdir1);
    add_tree_child(subdir1, file3);
    add_tree_child(subdir1, subdir2);
    add_tree_child(subdir2, subdir3);
    add_tree_child(subdir3, file4);

    return root;
}

/* <rootpath>/
 *   ├── subdir1/
 *   │   ├── file1
 *   │   ├── file2
 *   │   └── file3
 *   ├── subdir2/
 *   │   ├── file4
 *   │   └── file5
 *   └── file6
 */
TreeNode* tree_scenario_2(const char* test_name)
{
    TreeNode* root = create_dir_node(test_name);
    TreeNode* subdir1 = create_dir_node("subdir1");
    TreeNode* file1 = create_file_node("file1");
    TreeNode* file2 = create_file_node("file2");
    TreeNode* file3 = create_file_node("file3");
    TreeNode* subdir2 = create_dir_node("subdir2");
    TreeNode* file4 = create_file_node("file4");
    TreeNode* file5 = create_file_node("file5");
    TreeNode* file6 = create_file_node("file6");

    add_tree_child(root, subdir1);
    add_tree_child(subdir1, file1);
    add_tree_child(subdir1, file2);
    add_tree_child(subdir1, file3);
    add_tree_child(root, subdir2);
    add_tree_child(subdir2, file4);
    add_tree_child(subdir2, file5);
    add_tree_child(root, file6);

    return root;
}

/* <rootpath>/
 *   ├── dir_a/
 *   │   └── dir_b/
 *   │       └── dir_c/
 *   │           ├── file1
 *   │           ├── file2
 *   │           └── file3
 *   └── file4
 */
TreeNode* tree_scenario_3(const char* test_name)
{
    TreeNode* root = create_dir_node(test_name);
    TreeNode* dir_a = create_dir_node("dir_a");
    TreeNode* dir_b = create_dir_node("dir_b");
    TreeNode* dir_c = create_dir_node("dir_c");
    TreeNode* file1 = create_file_node("file1");
    TreeNode* file2 = create_file_node("file2");
    TreeNode* file3 = create_file_node("file3");
    TreeNode* file4 = create_file_node("file4");

    add_tree_child(root, dir_a);
    add_tree_child(dir_a, dir_b);
    add_tree_child(dir_b, dir_c);
    add_tree_child(dir_c, file1);
    add_tree_child(dir_c, file2);
    add_tree_child(dir_c, file3);
    add_tree_child(root, file4);

    return root;
}

/* <rootpath>/
 *   ├── file1
 *   ├── file2
 *   ├── file3
 *   ├── file4
 *   └── file5
 */
TreeNode* tree_scenario_4(const char* test_name)
{
    TreeNode* root = create_dir_node(test_name);
    TreeNode* file1 = create_file_node("file1");
    TreeNode* file2 = create_file_node("file2");
    TreeNode* file3 = create_file_node("file3");
    TreeNode* file4 = create_file_node("file4");
    TreeNode* file5 = create_file_node("file5");

    add_tree_child(root, file1);
    add_tree_child(root, file2);
    add_tree_child(root, file3);
    add_tree_child(root, file4);
    add_tree_child(root, file5);

    return root;
}

/* <rootpath>/
 *   ├── file1
 *   └── link1 -> file1
 */
TreeNode* tree_scenario_5(const char* test_name)
{
    TreeNode* root = create_dir_node(test_name);
    TreeNode* file1 = create_file_node("file1");
    TreeNode* link1 = create_symlink_node("link1", "file1");

    add_tree_child(root, file1);
    add_tree_child(root, link1);

    return root;
}

/* <rootpath>/
 *   ├── file1
 *   └── link1
 */
TreeNode* tree_scenario_5_dereferenced(const char* test_name)
{
    TreeNode* root = create_dir_node(test_name);
    TreeNode* file1 = create_file_node("file1");
    TreeNode* link1 = create_file_node("link1");

    add_tree_child(root, file1);
    add_tree_child(root, link1);

    return root;
}
