#include <gtest/gtest.h>
#include "recordmanager/database.h"

class TableTest : public ::testing::Test
{
protected:
    // You can define per-test set-up and tear-down logic as usual.
    virtual void SetUp()
    {
        database_ = new Database();
        database_->create("testbase");
        database_->open("testbase");
        database_->createTable("testTable");
        table_ = database_->get_table_by_name("testTable");
        database_->close();
    }
    virtual void TearDown()
    {
        database_->open("testbase");
        database_->dropTableByName("testTable");
        database_->drop();
        delete database_;
        database_ = NULL;
    }

    // Some expensive resource shared by all tests.
    Database *database_;
    Table *table_;
};

TEST_F(TableTest, ModifyRecord)
{
    database_->open("testbase");
    table_->addColumn("IntTest", CT_INT, 10, true, false, nullptr);
    table_->addColumn("FloatTest", CT_FLOAT, 10, true, false, nullptr);
    table_->addColumn("VarCharTest", CT_VARCHAR, 10, true, false, nullptr);
    table_->clearTempRecord();
    int *intdata = new int;
    float *floatdata = new float;
    char *chardata = new char[10];
    *intdata = 1;
    *floatdata = 1.0;
    strcpy(chardata, "abc");
    int id = table_->get_col_id("IntTest");
    table_->set_temp_record(id, (char *)intdata);

    id = table_->get_col_id("FloatTest");
    table_->set_temp_record(id, (char *)floatdata);

    id = table_->get_col_id("VarCharTest");
    table_->set_temp_record(id, (char *)chardata);

    table_->insertTempRecord();
    table_->printSchema();

    database_->close();
    database_->open("testbase");
    table_ = database_->get_table_by_name("testTable");

    char *buf = new char[table_->get_record_bytes()];
    RID temp = table_->get_next((RID)-1);
    id = table_->get_col_id("IntTest");
    table_->get_record(temp, buf);
    ASSERT_EQ(*((int *)(buf + table_->get_col_offset(id))), 1);
    id = table_->get_col_id("FloatTest");
    table_->get_record(temp, buf);
    ASSERT_EQ(*((float *)(buf + table_->get_col_offset(id))), 1.0);
    id = table_->get_col_id("VarCharTest");
    table_->get_record(temp, buf);
    ASSERT_STREQ((buf + table_->get_col_offset(id)), "abc");
    database_->close();
}

TEST_F(TableTest, MulitRecord)
{
    database_->open("testbase");
    table_->addColumn("Test", CT_INT, 10, true, false, nullptr);
    table_->clearTempRecord();
    int *intdata = new int;
    int id = table_->get_col_id("Test");

    *intdata = 1;
    table_->set_temp_record(id, (char *)intdata);
    table_->insertTempRecord();

    *intdata = 2;
    table_->set_temp_record(id, (char *)intdata);
    table_->insertTempRecord();

    table_->printSchema();

    database_->close();
    database_->open("testbase");
    table_ = database_->get_table_by_name("testTable");

    char *buf = new char[table_->get_record_bytes()];
    id = table_->get_col_id("Test");
    RID temp = table_->get_next((RID)-1);
    table_->get_record(temp, buf);
    ASSERT_EQ(*((int *)(buf + table_->get_col_offset(id))), 2);
    temp = table_->get_next(temp);
    table_->get_record(temp, buf);
    ASSERT_EQ(*((int *)(buf + table_->get_col_offset(id))), 1);
    database_->close();
}