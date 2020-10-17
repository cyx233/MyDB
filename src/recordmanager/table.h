#ifndef TABLE_H
#define TABLE_H

#include <string>
#include "header_page.h"
#include "const.h"
#include "filesystem/file_system_manager.h"

extern bool initMode;

class Table
{
    friend class Database;

public:
    std::string get_table_name();
    void printSchema();
    RID get_next(RID rid);

    void clearTempRecord();
    std::string insertTempRecord();

    std::string set_temp_record(int col, const char *data);
    void set_temp_record_null(int col);

    void dropRecord(RID rid);
    std::string loadRecordToTemp(RID rid, char *page, int offset);
    std::string modifyRecord(RID rid, int col, char *data);
    std::string modifyRecordNull(RID rid, int col);

    int get_record_bytes();
    char *get_record_temp_ptr(RID rid);
    void get_record(RID rid, char *buf);

    int get_col_offset(int col);
    ColumnType get_col_type(int col);
    char *get_col_name(int col);
    int get_col_count();
    int get_col_id(const char *name);

private:
    HeaderPage header_;
    bool table_is_ready_;
    int file_id_;
    char *buf_;
    std::string table_name_;

    Table();
    ~Table();

    void initTempRecord();
    void allocPage();
    void set_footer(const char *page, int index);
    void set_footer_zero(const char *page);
    int get_footer(const char *page, int index);

    void create(const char *table_name);
    void open(const char *table_name);
    void close();
    void drop();

    char *select(RID rid, int col);
    bool selectIsNull(RID rid, int col);

    // return -1 if name exist, columnId otherwise
    // size: maxlen for varchar, outputwidth for int
    int addColumn(const char *name, ColumnType type, int size,
                  bool notNull, bool hasDefault, const char *data);
};

#endif //TABLE_H
