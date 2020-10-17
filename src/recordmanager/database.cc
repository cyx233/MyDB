#include <cassert>
#include <fstream>
#include "database.h"

Database::Database()
{
    db_is_ready_ = false;
    for (Table *&tb : table_)
    {
        tb = nullptr;
    }
}

Database::~Database()
{
    if (db_is_ready_)
        close();
}

bool Database::isOpen()
{
    return db_is_ready_;
}

std::string Database::get_db_name()
{
    return db_name_;
}

void Database::close()
{
    assert(db_is_ready_);
    FILE *file = fopen((db_name_ + ".db").c_str(), "w");
    fprintf(file, "%zu\n", tot_table_num_);
    for (size_t i = 0; i < tot_table_num_; ++i)
    {
        table_[i]->close();
        delete table_[i];
        table_[i] = nullptr;
        fprintf(file, "%s\n", table_name_[i].c_str());
    }
    tot_table_num_ = 0;
    fclose(file);
    db_is_ready_ = false;
}

void Database::drop()
{
    assert(db_is_ready_);
    remove((db_name_ + ".db").c_str());
    for (size_t i = 0; i < tot_table_num_; ++i)
    {
        table_[i]->drop();
        delete table_[i];
        table_[i] = nullptr;
        remove((db_name_ + "." + table_name_[i] + ".table").c_str());
    }
    db_is_ready_ = false;
}

void Database::open(const std::string &name)
{
    assert(!db_is_ready_);
    db_name_ = name;
    std::ifstream fin((name + ".db").c_str());
}

void Database::create(const std::string &name)
{
    FILE *file = fopen((name + ".db").c_str(), "w");
    assert(file);
    fclose(file);
    assert(db_is_ready_ == 0);
    tot_table_num_ = 0;
    db_name_ = name;
}

Table *Database::get_table_by_name(const std::string name)
{
    for (size_t i = 0; i < tot_table_num_; ++i)
    {
        if (table_name_[i] == name)
        {
            return table_[i];
        }
    }
    return nullptr;
}

size_t Database::get_table_id_by_name(const std::string &name)
{
    for (size_t i = 0; i < tot_table_num_; ++i)
    {
        if (table_name_[i] == name)
        {
            return (size_t)i;
        }
    }
    return (size_t)-1;
}

Table *Database::get_table_by_id(const size_t id)
{
    if (id < tot_table_num_)
    {
        return table_[id];
    }
    else
    {
        return nullptr;
    }
}

Table *Database::createTable(const std::string &name)
{
    assert(db_is_ready_);
    table_name_[tot_table_num_] = name;
    assert(table_[tot_table_num_] == nullptr);
    table_[tot_table_num_] = new Table();
    table_[tot_table_num_]->create((db_name_ + "." + name + ".table").c_str());
    ++tot_table_num_;
    return table_[tot_table_num_ - 1];
}
void Database::dropTableByName(const std::string &name)
{
    int p = -1;
    for (size_t i = 0; i < tot_table_num_; i++)
        if (table_name_[i] == name)
        {
            p = (int)i;
            break;
        }
    if (p == -1)
    {
        printf("drop Table: Table not found!\n");
        return;
    }
    table_[p]->drop();
    delete table_[p];
    table_[p] = table_[tot_table_num_ - 1];
    table_[tot_table_num_ - 1] = 0;
    table_name_[p] = table_name_[tot_table_num_ - 1];
    table_name_[tot_table_num_ - 1] = "";
    tot_table_num_--;
    remove((db_name_ + "." + name + ".table").c_str());
}

std::vector<std::string> Database::get_all_table_names()
{
    std::vector<std::string> name;
    for (size_t i = 0; i < tot_table_num_; i++)
    {
        name.push_back(table_name_[i]);
    }
    return name;
}