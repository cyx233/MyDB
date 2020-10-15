#ifndef RID_H
#define RID_H

class RID
{
public:
    RID() = default;
    RID(unsigned page_num, unsigned slot_num) : page_num_(page_num), slot_num_(slot_num) {}
    RID(const RID &rid) = default;

    unsigned long getPageNum() const { return page_num_; }
    unsigned getSlotNum() const { return slot_num_; }

private:
    unsigned long page_num_ = 0;
    unsigned slot_num_ = 0;
};

#endif //RID_H
