#ifndef DATABASE_H
#define DATABASE_H
#include <string>
#include <vector>
#include "table.h"
class Database
{
public:
    Database();
    ~Database();

    bool isOpen();
    std::string get_db_name();

    void close();
    void drop();
    void open(const std::string &name);
    void create(const std::string &name);

    Table *get_table_by_name(const std::string name);
    Table *get_table_by_id(const size_t id);
    size_t get_table_id_by_name(const std::string &name);

    Table *createTable(const std::string &name);

    void dropTableByName(const std::string &name);

    std::vector<std::string> get_all_table_names();

private:
    bool db_is_ready_;
    std::string table_name_[MAX_TABLE_SIZE];
    size_t tot_table_num_;
    Table *table_[MAX_TABLE_SIZE];
    std::string db_name_;
};

#endif