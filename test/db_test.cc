#include <gtest/gtest.h>
#include <fstream>
#include "recordmanager/database.h"

class DatabaseTest : public ::testing::Test
{
protected:
    // You can define per-test set-up and tear-down logic as usual.
    virtual void SetUp()
    {
        database_ = new Database();
        database_->create("testbase");
        database_->open("testbase");
        database_->createTable("testTable");
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
};

TEST_F(DatabaseTest, OpenCloseBase)
{
    database_->open("testbase");
    ASSERT_EQ(database_->isOpen(), 1);
    database_->close();
    ASSERT_EQ(database_->isOpen(), 0);
}

TEST_F(DatabaseTest, CreateDropBase)
{
    database_->open("testbase");
    database_->drop();
    std::ifstream fin("testbase.db");
    bool a = !fin;
    ASSERT_EQ(a, true);
    database_->create("testbase");
    fin.open("testbase.db");
    a = !fin;
    ASSERT_EQ(a, false);
}

TEST_F(DatabaseTest, CreateDropTable)
{
    database_->open("testbase");
    database_->dropTableByName("testTable");
    database_->close();
    database_->open("testbase");
    Table *a = database_->get_table_by_name("testTable");
    bool temp = !a;
    ASSERT_EQ(temp, true);
    database_->createTable("testTable");
    database_->close();
    database_->open("testbase");
    a = database_->get_table_by_name("testTable");
    temp = !a;
    ASSERT_EQ(temp, false);
    database_->close();
}

TEST_F(DatabaseTest, GetTableByName)
{
    database_->open("testbase");
    Table *a = database_->get_table_by_name("testTable");
    std::string table_name = database_->get_db_name() + "." + "testTable" + ".table";
    ASSERT_STREQ(a->get_table_name().c_str(), table_name.c_str());
    database_->close();
}

TEST_F(DatabaseTest, GetTableById)
{
    database_->open("testbase");
    std::string temp = database_->get_db_name() + "." + "testTable" + ".table";
    Table *a = database_->get_table_by_id(database_->get_table_id_by_name("testTable"));
    std::string table_name = database_->get_db_name() + "." + "testTable" + ".table";
    ASSERT_STREQ(a->get_table_name().c_str(), table_name.c_str());
    database_->close();
}
