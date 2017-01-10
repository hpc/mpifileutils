#include <stdlib.h>
#include <stdio.h>
#include <math.h>
#include <CUnit/CUnit.h>
#include <CUnit/Basic.h>

//#include "test_suite1.h"
int init_test_suite1(void)
{
    return 0;
}

int clean_test_suite1(void)
{
    return 0;
}

int is_odd (int x)
{
  return (x % 2 != 0);
}

void test_is_odd (void)
{
  CU_ASSERT(is_odd(1)  == 1);
  CU_ASSERT(is_odd(2)  == 0);
  CU_ASSERT(is_odd(3)  == 1);
}

int setup_test_suite1(void) {
    CU_pSuite pSuite = NULL;
    pSuite = CU_add_suite("test_suite1", init_test_suite1, clean_test_suite1);
    if (NULL == pSuite) {
        return -1;
    }

    if ((NULL == CU_add_test(pSuite, "test of test_suite1()", test_is_odd)))
    {
        return -1;
    }
    return 0;
}

int main(void)
{
    /* initialize the CUnit test registry */
    if (CUE_SUCCESS != CU_initialize_registry())
        return CU_get_error();

    /* setup_test_suite1 will add a test suite to the CUnit test registry see further on */
    if (setup_test_suite1() == -1) { //setup_test_suite1 will add a test suite to the CUnit test registry see further on
        CU_cleanup_registry();
        return CU_get_error();
    }

    /* Run all tests using the CUnit Basic interface */
    CU_basic_set_mode(CU_BRM_VERBOSE);
    CU_basic_run_tests();
    CU_cleanup_registry();
    return CU_get_error();
}
