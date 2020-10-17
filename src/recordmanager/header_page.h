#ifndef MYDB_HEADERPAGE_H
#define MYDB_HEADERPAGE_H
#include "../const.h"

struct HeaderPage
{
    unsigned record_column_size;
    unsigned record_byte_size;
    unsigned tot_page_num;
    unsigned record_per_page;
    unsigned first_spare_page;
    unsigned header_data_used;
    unsigned next_available;
    unsigned not_null;

    char column_name[MAX_COLUMN_SIZE][MAX_NAME_LEN];
    int column_offset[MAX_COLUMN_SIZE];
    ColumnType column_type[MAX_COLUMN_SIZE];
    int column_array_len[MAX_COLUMN_SIZE];
    int default_value_addr[MAX_COLUMN_SIZE];
    char header_data[MAX_COLUMN_SIZE];
};

#endif //MYDB_HEADERPAGE_H
