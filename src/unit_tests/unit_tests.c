#include <stdlib.h>
#include <stdio.h>
#include <math.h>
#include <CUnit/CUnit.h>
#include <CUnit/Basic.h>

#include "test_suite1.h"

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
