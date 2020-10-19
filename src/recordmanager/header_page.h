#ifndef MYDB_HEADERPAGE_H
#define MYDB_HEADERPAGE_H
#include "type.h"
#include "../const.h"

struct Check
{
    int col;
    int offset;
    OpType op;
    RelType rel;
};
struct HeaderPage
{
    unsigned not_null;
    unsigned next_available;

    int record_column_size;
    int record_byte_size;
    int tot_page_num;
    int record_per_page;
    int first_spare_page;
    int header_data_used;
    int tot_check_num;

    char column_name[MAX_COLUMN_SIZE][MAX_NAME_LEN];
    int column_offset[MAX_COLUMN_SIZE];
    ColumnType column_type[MAX_COLUMN_SIZE];
    int column_array_len[MAX_COLUMN_SIZE];
    int default_value_addr[MAX_COLUMN_SIZE];
    char header_data[MAX_COLUMN_SIZE];
    Check check_list[MAX_CHECK];
};

#endif //MYDB_HEADERPAGE_H
