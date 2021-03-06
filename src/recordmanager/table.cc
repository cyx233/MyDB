#include <cstring>
#include <cassert>
#include <sstream>
#include "table.h"
#include "filesystem/file_system_manager.h"

void Table::expandBuf(bool isClear)
{
    if (buf_ == nullptr)
    {
        buf_ = new char[header_.record_byte_size];
        buf_size_ = header_.record_byte_size;
    }
    else if (buf_size_ < header_.record_byte_size)
    {
        if (isClear)
        {
            delete buf_;
            buf_ = new char[header_.record_byte_size];
        }
        else
        {
            char *temp = new char[header_.record_byte_size];
            memcpy(temp, buf_, buf_size_);
            buf_size_ = header_.record_byte_size;
            delete buf_;
            buf_ = temp;
        }
    }
}

void Table::set_footer_zero(const char *page)
{
    memset((char *)(page + PAGE_SIZE - PAGE_FOOTER_SIZE), 0, PAGE_FOOTER_SIZE);
}

void Table::initTempRecord()
{
    buf_not_null_ = 0;
    for (int i = 0; i < header_.record_column_size; ++i)
    {
        if (header_.default_value_addr[i] != -1)
        {
            switch (header_.column_type[i])
            {
            case CT_INT:
            case CT_FLOAT:
            case CT_DATE:
                memcpy(buf_ + header_.column_offset[i],
                       header_.header_data + header_.default_value_addr[i],
                       4);
                break;
            case CT_VARCHAR:
                strcpy(buf_ + header_.column_offset[i],
                       header_.header_data + header_.default_value_addr[i]);
                break;
            default:
                assert(false);
                break;
            }
            buf_not_null_ |= (1u << i);
        }
    };
}

void Table::clearTempRecord()
{
    expandBuf(true);
    initTempRecord();
}

void Table::allocPage()
{
    int index;
    char *new_page = (char *)FileSystemManager::get_buf_manager()->allocPage(file_id_, header_.tot_page_num, index);
    int n = (PAGE_SIZE - PAGE_FOOTER_SIZE) / header_.record_byte_size;
    n = (n < MAX_REC_PER_PAGE) ? n : MAX_REC_PER_PAGE;
    for (int i = 0, p = 0; i < n; ++i, p += header_.record_byte_size)
    {
        unsigned &ptr = *(unsigned *)(new_page + p);
        ptr = header_.next_available;
        header_.next_available = (unsigned)header_.tot_page_num * PAGE_SIZE + p;
    }
    set_footer_zero((char *)new_page);
    FileSystemManager::get_buf_manager()->markDirty(index);
    header_.tot_page_num += 1;
}

void Table::set_footer(const char *page, int index)
{
    int u = index / 32;
    int v = index % 32;
    unsigned int &tmp = *(unsigned int *)(page + PAGE_SIZE - PAGE_FOOTER_SIZE + u * 4);
    tmp ^= (1u << v);
}

int Table::get_footer(const char *page, int index)
{
    int u = index / 32;
    int v = index % 32;
    unsigned int tmp = *(unsigned int *)(page + PAGE_SIZE - PAGE_FOOTER_SIZE + u * 4);
    return (tmp >> v) & 1;
}

Table::Table()
{
    table_is_ready_ = false;
}

Table::~Table()
{
    if (table_is_ready_)
        close();
}

std::string Table::get_table_name()
{
    assert(table_is_ready_);
    return table_name_;
}

void Table::printSchema()
{
    for (int i = 1; i < header_.record_column_size; i++)
    {
        printf("%s", header_.column_name[i]);
        switch (header_.column_type[i])
        {
        case CT_INT:
            printf(" INT(%d)", header_.column_array_len[i]);
            break;
        case CT_FLOAT:
            printf(" FLOAT");
            break;
        case CT_DATE:
            printf(" DATE");
            break;
        case CT_VARCHAR:
            printf(" VARCHAR(%d)", header_.column_array_len[i]);
            break;
        default:
            assert(0);
        }
        if (header_.not_null & (1 << i))
            printf(" NotNull");
        printf("\n");
    }
}

RID Table::get_next(RID rid)
{
    int page_id, id, n;
    n = (PAGE_SIZE - PAGE_FOOTER_SIZE) / header_.record_byte_size;
    n = (n < MAX_REC_PER_PAGE) ? n : MAX_REC_PER_PAGE;
    if (rid == (RID)-1)
    {
        page_id = 0;
        id = n - 1;
    }
    else
    {
        page_id = rid / PAGE_SIZE;
        id = (rid % PAGE_SIZE) / header_.record_byte_size;
    }
    int index;
    char *page = (char *)FileSystemManager::get_buf_manager()->getPage(file_id_, page_id, index);

    while (true)
    {
        id++;
        if (id == n)
        {
            page_id++;
            if (page_id >= header_.tot_page_num)
                return (RID)-1;
            page = (char *)FileSystemManager::get_buf_manager()->getPage(file_id_, page_id, index);
            id = 0;
        }
        if (get_footer(page, id))
            return (RID)page_id * PAGE_SIZE + id * header_.record_byte_size;
    }
}

// return -1 if name exist, columnId otherwise
// size: maxlen for varchar, outputwidth for int
int Table::addColumn(const char *name, ColumnType type, int size,
                     bool notNull, bool hasDefault, const char *data)
{
    printf("adding %s %d %d\n", name, type, size);
    assert(header_.tot_page_num == 1);
    assert(strlen(name) < MAX_NAME_LEN);
    for (int i = 0; i < header_.record_column_size; i++)
        if (strcmp(header_.column_name[i], name) == 0)
            return -1;
    assert(header_.record_column_size < MAX_COLUMN_SIZE);
    int id = header_.record_column_size++;
    strcpy(header_.column_name[id], name);
    header_.column_type[id] = type;
    header_.column_offset[id] = header_.record_byte_size;
    header_.column_array_len[id] = size;
    if (notNull)
        header_.not_null |= (1 << id);
    header_.default_value_addr[id] = -1;
    switch (type)
    {
    case CT_INT:
    case CT_FLOAT:
    case CT_DATE:
        header_.record_byte_size += 4;
        if (hasDefault)
        {
            header_.default_value_addr[id] = header_.header_data_used;
            memcpy(header_.header_data + header_.header_data_used, data, 4);
            header_.header_data_used += 4;
        }
        break;
    case CT_VARCHAR:
        header_.record_byte_size += size + 1;
        header_.record_per_page += 4 - header_.record_byte_size % 4;
        if (hasDefault)
        {
            header_.default_value_addr[id] = header_.header_data_used;
            strcpy(header_.header_data + header_.header_data_used, data);
            header_.header_data_used += strlen(data) + 1;
        }
        break;
    default:
        assert(0);
    }
    assert(header_.header_data_used <= MAX_DATA_SIZE);
    assert(header_.record_byte_size <= PAGE_SIZE);
    return id;
}

bool Table::selectIsNull(RID rid, int col)
{
    char *p = select(rid, col);
    delete[] p;
    return p == nullptr;
}

void Table::create(const char *table_name)
{
    assert(!table_is_ready_);
    table_name_ = std::string(table_name);
    FileSystemManager::get_file_manager()->createFile(table_name);
    FileSystemManager::get_file_manager()->openFile(table_name, file_id_);
    int index;
    FileSystemManager::get_buf_manager()->allocPage(file_id_, 0, index);
    table_is_ready_ = true;
    header_.tot_page_num = 1;
    header_.record_byte_size = 0; // reserve first 4 bytes for notnull info
    //header_.rowTot = 0;
    header_.record_column_size = 0;
    header_.header_data_used = 0;
    header_.next_available = (unsigned int)-1;
    header_.not_null = 0;
    header_.tot_check_num = 0;
    addColumn("RID", CT_INT, 10, true, false, nullptr);
    buf_size_ = 0;
}

void Table::open(const char *table_name)
{
    assert(table_is_ready_ == 0);
    table_name_ = std::string(table_name);
    int index;
    FileSystemManager::get_file_manager()->openFile(table_name, file_id_);
    memcpy(&header_, FileSystemManager::get_buf_manager()->getPage(file_id_, 0, index), sizeof(HeaderPage));
    table_is_ready_ = true;
    buf_size_ = 0;
}

void Table::close()
{
    assert(table_is_ready_);
    int index;
    memcpy(FileSystemManager::get_buf_manager()->getPage(file_id_, 0, index),
           &header_,
           sizeof(HeaderPage));
    FileSystemManager::get_buf_manager()->markDirty(index);
    FileSystemManager::get_buf_manager()->close();
    FileSystemManager::get_file_manager()->closeFile(file_id_);
    table_is_ready_ = false;
}

void Table::drop()
{
    assert(table_is_ready_ == 1);
    FileSystemManager::get_buf_manager()->closeFile(file_id_, false);
    FileSystemManager::get_file_manager()->closeFile(file_id_);
    table_is_ready_ = false;
}

int Table::get_col_count()
{
    return header_.record_column_size;
}

int Table::get_col_id(const char *name)
{
    for (int i = 1; i < header_.record_column_size; ++i)
    {
        if (strcmp(header_.column_name[i], name) == 0)
            return i;
    }
    return -1;
}

std::string Table::set_temp_record(int col, const char *data)
{
    if (data == nullptr)
    {
        set_temp_record_null(col);
        return "";
    }
    expandBuf(false);
    switch (header_.column_type[col])
    {
    case CT_INT:
    case CT_DATE:
    case CT_FLOAT:
        memcpy(buf_ + header_.column_offset[col], data, 4);
        break;
    case CT_VARCHAR:
        if ((unsigned int)header_.column_array_len[col] < strlen(data))
        {
            printf("%d %s\n", header_.column_array_len[col], data);
        }
        if (strlen(data) > (unsigned int)header_.column_array_len[col])
        {
            return "ERROR: varchar too long";
        }
        strcpy(buf_ + header_.column_offset[col], data);
        break;
    default:
        assert(0);
    }
    buf_not_null_ |= (1u << col);
    return "";
}

void Table::set_temp_record_null(int col)
{
    expandBuf(false);
    if (buf_not_null_ & (1u << col))
        buf_not_null_ ^= (1u << col);
}

// return value change. Urgly interface.
// return "" if success.
// return error description otherwise.
std::string Table::insertTempRecord()
{
    assert(buf_ != nullptr);
    if (header_.next_available == (RID)-1)
    {
        allocPage();
    }
    // RID rid = header_.next_available;
    set_temp_record(0, (char *)&header_.next_available);
    std::string error = checkRecord();
    if (!error.empty())
    {
        printf("Error occurred when inserting record, aborting...\n");
        return error;
    }
    int page_id = header_.next_available / PAGE_SIZE;
    int offset = header_.next_available % PAGE_SIZE;
    int index;
    char *page = (char *)FileSystemManager::get_buf_manager()->getPage(file_id_, page_id, index);
    header_.next_available = *(unsigned int *)(page + offset);
    memcpy(page + offset, buf_, header_.record_byte_size);
    FileSystemManager::get_buf_manager()->markDirty(index);
    set_footer(page, offset / header_.record_byte_size);
    return "";
}

void Table::dropRecord(RID rid)
{
    int page_id = rid / PAGE_SIZE;
    int offset = rid % PAGE_SIZE;
    int index;
    char *page = (char *)FileSystemManager::get_buf_manager()->getPage(file_id_, page_id, index);
    char *record = page + offset;
    unsigned int &next = *(unsigned int *)record;
    next = header_.next_available;
    header_.next_available = rid;
    set_footer(page, offset / header_.record_byte_size);
    FileSystemManager::get_buf_manager()->markDirty(index);
}

std::string Table::loadRecordToTemp(RID rid, char *page, int offset)
{
    UNUSED(rid);
    expandBuf(false);
    char *record = page + offset;
    if (!get_footer(page, offset / header_.record_byte_size))
    {
        return "ERROR: RID invalid";
    }
    memcpy(buf_, record, (size_t)header_.record_byte_size);
    return "";
}

std::string Table::modifyRecord(RID rid, int col, char *data)
{
    if (data == nullptr)
    {
        return modifyRecordNull(rid, col);
    }
    int page_id = rid / PAGE_SIZE;
    int offset = rid % PAGE_SIZE;
    int index;
    char *page = (char *)FileSystemManager::get_buf_manager()->getPage(file_id_, page_id, index);
    char *record = page + offset;
    std::string err = loadRecordToTemp(rid, page, offset);
    if (!err.empty())
    {
        return err;
    }
    assert(col != 0);
    err = set_temp_record(col, data);
    if (!err.empty())
    {
        return err;
    }
    memcpy(record, buf_, header_.record_byte_size);
    FileSystemManager::get_buf_manager()->markDirty(index);
    return "";
}

std::string Table::modifyRecordNull(RID rid, int col)
{
    int page_id = rid / PAGE_SIZE;
    int offset = rid % PAGE_SIZE;
    int index;
    char *page = (char *)FileSystemManager::get_buf_manager()->getPage(file_id_, page_id, index);
    char *record = page + offset;
    std::string err = loadRecordToTemp(rid, page, offset);
    if (!err.empty())
    {
        return err;
    }
    assert(col != 0);
    set_temp_record_null(col);
    memcpy(record, buf_, header_.record_byte_size);
    FileSystemManager::get_buf_manager()->markDirty(index);
    return "";
}

int Table::get_record_bytes()
{
    return header_.record_byte_size;
}

char *Table::get_record_temp_ptr(RID rid)
{
    int page_id = rid / PAGE_SIZE;
    int offset = rid % PAGE_SIZE;
    int index;
    assert(1 <= page_id && page_id < header_.tot_page_num);
    char *page = (char *)FileSystemManager::get_buf_manager()->getPage(file_id_, page_id, index);
    assert(get_footer(page, offset / header_.record_byte_size));
    return page + offset;
}

void Table::get_record(RID rid, char *buf)
{
    char *ptr = get_record_temp_ptr(rid);
    memcpy(buf, ptr, (size_t)header_.record_byte_size);
}

int Table::get_col_offset(int col)
{
    return header_.column_offset[col];
}

ColumnType Table::get_col_type(int col)
{
    return header_.column_type[col];
}

//return 0 when null
//return value in tempbuf when rid = -1
char *Table::select(RID rid, int col)
{
    char *ptr;
    if (rid != (RID)-1)
    {
        ptr = get_record_temp_ptr(rid);
    }
    else
    {
        ptr = buf_;
    }
    char *temp;
    if ((~buf_not_null_) & (1 << col))
    {
        return nullptr;
    }
    switch (header_.column_type[col])
    {
    case CT_INT:
    case CT_DATE:
    case CT_FLOAT:
        temp = new char[4];
        memcpy(temp, ptr + get_col_offset(col), 4);
        return temp;
    case CT_VARCHAR:
        temp = new char[header_.column_array_len[col] + 1];
        strcpy(temp, ptr + get_col_offset(col));
        return temp;
    default:
        assert(false);
        exit(1);
    }
}

char *Table::get_col_name(int col)
{
    assert(0 <= col && col < header_.record_column_size);
    return header_.column_name[col];
}

std::string Table::checkRecord()
{
    if ((buf_not_null_ & header_.not_null) != header_.not_null)
    {
        return "Insert Error: not null column is null.";
    }

    std::string valueCheck = checkValueConstraint();
    if (!valueCheck.empty())
    {
        return valueCheck;
    }

    return std::string();
}

std::string Table::checkValueConstraint()
{
    bool flag = true, checkResult = false;
    for (int i = 0; i < header_.tot_check_num; i++)
    {
        Check chk = header_.check_list[i];
        if (chk.offset == -1) //data is null
        {
            checkResult |= (chk.op == OP_EQ) && (((~buf_not_null_) & (1 << chk.col)) == 0);
        }
        else
        {
            switch (header_.column_type[chk.col])
            {
            case CT_INT:
            case CT_DATE:
                checkResult |= compareInt(*(int *)(buf_ + header_.column_offset[chk.col]), chk.op,
                                          *(int *)(header_.header_data + chk.offset));
                break;
            case CT_FLOAT:
                checkResult |= compareFloat(*(float *)(buf_ + header_.column_offset[chk.col]), chk.op,
                                            *(float *)(header_.header_data + chk.offset));
                break;
            case CT_VARCHAR:
                checkResult |= compareVarchar(buf_ + header_.column_offset[chk.col], chk.op,
                                              header_.header_data + chk.offset);
                break;
            default:
                assert(false);
            }
        }
        if (i == header_.tot_check_num - 1 || chk.rel == RE_AND ||
            !(header_.check_list[i + 1].rel == RE_OR && chk.col == header_.check_list[i + 1].col))
        {
            flag &= checkResult;
            checkResult = false;
        }
        if (!flag)
            return genCheckError(i);
    }
    return std::string();
}

std::string Table::genCheckError(int checkId)
{
    int ed = checkId + 1, st = checkId;
    while (header_.check_list[st - 1].col == header_.check_list[checkId].col &&
           header_.check_list[st - 1].rel == RE_OR &&
           header_.check_list[checkId].rel == RE_OR)
    {
        checkId++;
    }
    std::ostringstream stm;
    stm << "Insert Error: Col " << header_.column_name[header_.check_list[checkId].col];
    stm << " CHECK ";

    for (int i = st; i < ed; i++)
    {
        if (i != st)
            stm << " OR ";
        Check chk = header_.check_list[i];
        switch (header_.column_type[chk.col])
        {
        case CT_INT:
        case CT_DATE:
            if (buf_not_null_ & (1 << chk.col))
            {
                stm << *(int *)(buf_ + header_.column_offset[chk.col]);
            }
            else
            {
                stm << "null";
            } // TODO parse date to string here
            stm << opTypeToString(chk.op) << *(int *)(header_.header_data + chk.offset);
            break;
        case CT_FLOAT:
            if (buf_not_null_ & (1 << chk.col))
            {
                stm << *(float *)(buf_ + header_.column_offset[chk.col]);
            }
            else
            {
                stm << "null";
            }
            stm << opTypeToString(chk.op) << *(float *)(header_.header_data + chk.offset);
            break;
        case CT_VARCHAR:
            if (buf_not_null_ & (1 << chk.col))
            {
                stm << *(int *)(buf_ + header_.column_offset[chk.col]);
            }
            else
            {
                stm << "null";
            }
            stm << "'" << buf_ + header_.header_data[chk.col] << "''" << opTypeToString(chk.op) << "'"
                << header_.header_data + chk.offset << "'";
            break;
        default:
            assert(false);
        }
    }
    return stm.str();
}

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

void Table::addCheck(int col, OpType op, char *data, RelType relation)
{
    UNUSED(op);
    assert(header_.tot_page_num == 1);
    assert(header_.tot_check_num < MAX_CHECK);
    int id = header_.tot_check_num;
    header_.check_list[id].col = col;
    header_.check_list[id].offset = header_.header_data_used;
    header_.check_list[id].rel = relation;
    if (data == nullptr)
    {
        header_.check_list[id].offset = -1;
        header_.tot_check_num++;
        return;
    }
    switch (header_.column_type[col])
    {
    case CT_INT:
    case CT_FLOAT:
    case CT_DATE:
        memcpy(header_.header_data + header_.header_data_used, data, 4);
        header_.header_data_used += 4;
        break;
    case CT_VARCHAR:
        strcpy(header_.header_data + header_.header_data_used, data);
        header_.header_data_used += strlen(data) + 1;
        header_.header_data_used += (4 - header_.header_data_used % 4) % 4;
        break;
    default:
        assert(0);
    }
    assert(header_.header_data_used <= MAX_DATA_SIZE);
    header_.tot_check_num++;
}