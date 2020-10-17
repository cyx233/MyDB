#include <gtest/gtest.h>
#include "../src/recordmanager/table.h"

TEST(TABLE_TEST, TABLE_TEST_SIZE)
{
    printf("TableHead has size %d\n", (int)sizeof(HeaderPage));
    ASSERT_LE(sizeof(HeaderPage), PAGE_SIZE);
}

TEST(TABLE_TEST, TABLE_TEST_CREATE)
{
}
