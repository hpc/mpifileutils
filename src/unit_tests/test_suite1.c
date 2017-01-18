#include <CUnit/CUnit.h>
#include <CUnit/Basic.h>
#include "test_suite1.h"

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
  CU_ASSERT(is_odd(1) == 1);
  CU_ASSERT(is_odd(2) == 0);
  CU_ASSERT(is_odd(3) == 1);
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
