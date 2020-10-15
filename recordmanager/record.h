#ifndef RECORD_H
#define RECORD_H

#include <string.h>
#include "rid.h"

class Record
{
private:
    RID rid_;
    char *data_;
    unsigned size_;

public:
    Record() : data_(nullptr), size_(0){};
    Record(const RID &rid, const char *data, unsigned size) : rid_(rid), size_(size)
    {
        data_ = new char[size];
        memcpy(data_, data, size);
    };
    Record(Record &&record)
    {
        data_ = record.data_;
        size_ = record.size_;
        rid_ = record.rid_;
        record.data_ = nullptr;
        record.size_ = 0;
    }
    ~Record()
    {
        delete[] data_;
    };

    RID GetRid() const
    {
        return rid_;
    }

    char *GetData() const
    {
        return data_;
    }

    unsigned GetSize() const
    {
        return size_;
    }

    void init(const char *data, unsigned size, const RID &rid)
    {
        rid_ = rid;
        delete[] data_;
        data_ = new char[size];
        memcpy(data_, data, size);
    }
};

#endif //RECORD_H