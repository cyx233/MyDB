#ifndef TABLE_H
#define TABLE_H

#include <string>
#include "header_page.h"
#include "const.h"
#include "type.h"

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

    // return -1 if name exist, columnId otherwise
    // size: maxlen for varchar, outputwidth for int
    int addColumn(const char *name, ColumnType type, int size,
                  bool notNull, bool hasDefault, const char *data);

    // RelType is used to create Inset operation.
    // Dirty interface.
    // With `relation` RE_OR and same `col`, the result will OR together
    // others will AND together
    // data == 0 if data is null
    // Example:
    //   List: [1, >, 10, AND], [2, ==, 'a', OR], [2, ==, 'b', OR], [3, ==, 'c', OR]
    //   Result: col[1]>10 AND (col[2]=='a' OR col[2]=='b') AND col[3]=='c'
    // Example:
    //   List: [1, >, 10, AND], [2, ==, 'a', OR], [3, ==, 'c', OR], [2, ==, 'b', OR]
    //   Result: col[1]>10 AND col[2]=='a' AND col[3]=='c' AND col[2]=='b'
    void addCheck(int col, OpType op, char *data, RelType relation);

private:
    HeaderPage header_;
    bool table_is_ready_;
    int file_id_;
    int buf_size_;
    char *buf_ = nullptr;
    unsigned buf_not_null_;
    std::string table_name_;

    Table();
    ~Table();

    void initTempRecord();
    void expandBuf(bool isClear);
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
    std::string checkRecord();
    std::string checkValueConstraint();
    std::string genCheckError(int checkId);
};

#endif //TABLE_H
