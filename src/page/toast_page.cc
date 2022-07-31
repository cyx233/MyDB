#include "toast_page.h"

#include <assert.h>

#include <algorithm>
#include <cstring>

#include "exception/exceptions.h"
#include "macros.h"

namespace thdb {
const PageOffset USED_SLOTS_OFFSET = 12;
const PageOffset SLOTS_NUM = 16;
const PageOffset SPEAR_LOWER_OFFSET = 20;
const PageOffset SPEAR_UPPER_OFFSET = 24;

ToastPage::ToastPage() : LinkedPage() {
  _usedSlots = 0;
  spareLower = 0;
  spareUpper = DATA_SIZE;
}

ToastPage::ToastPage(PageID nPageID) : LinkedPage(nPageID) {
  SlotID slots_num = 0;
  GetHeader((uint8_t *)&_usedSlots, sizeof(SlotID), USED_SLOTS_OFFSET);
  GetHeader((uint8_t *)&slots_num, sizeof(SlotID), SLOTS_NUM);
  GetHeader((uint8_t *)&spareLower, sizeof(PageOffset), SPEAR_LOWER_OFFSET);
  GetHeader((uint8_t *)&spareUpper, sizeof(PageOffset), SPEAR_UPPER_OFFSET);
  if (slots_num == 0) {
    _usedSlots = 0;
    spareLower = 0;
    spareUpper = DATA_SIZE;
    return;
  }
  Slot_t *data;
  data = new Slot_t[slots_num];
  GetData((uint8_t *)data, sizeof(Slot_t) * slots_num, 0);
  slots.insert(slots.begin(), data, data + slots_num);
  for (SlotID i = 0; i < slots.size(); ++i) {
    if (HasRecord(i)) {
      prev_len.push_back(slots[i].length);
    } else {
      prev_len.push_back(0);
    }
  }
  delete[] data;
}

ToastPage::~ToastPage() {
  SlotID slots_num = (SlotID)slots.size();
  SetHeader((uint8_t *)&_usedSlots, sizeof(SlotID), USED_SLOTS_OFFSET);
  SetHeader((uint8_t *)&slots_num, sizeof(SlotID), SLOTS_NUM);
  SetHeader((uint8_t *)&spareLower, sizeof(PageOffset), SPEAR_LOWER_OFFSET);
  SetHeader((uint8_t *)&spareUpper, sizeof(PageOffset), SPEAR_UPPER_OFFSET);
  SetData((uint8_t *)slots.data(), sizeof(Slot_t) * slots.size(), 0);
}

void ToastPage::RearrangeRec(SlotID nSlotID) {
  if (_usedSlots == 0) return;
  if (slots[nSlotID].offset + slots[nSlotID].length - spareUpper == 0) return;
  uint8_t *data = new uint8_t[DATA_SIZE - spareUpper];
  PageOffset delta = prev_len[nSlotID] - slots[nSlotID].length;
  if (delta > 0) {
    GetData(data, slots[nSlotID].offset + slots[nSlotID].length - spareUpper,
            spareUpper);
    SetData(data, slots[nSlotID].offset + slots[nSlotID].length - spareUpper,
            spareUpper + delta);
  }
  spareLower += delta;
  for (SlotID i = 0; i < slots.size(); ++i) {
    if (HasRecord(i) && slots[i].offset <= slots[nSlotID].offset)
      slots[i].offset += delta;
  }
}

bool ToastPage::Full(const PageOffset len) const {
  if (_usedSlots < slots.size()) return len > spareUpper - spareLower;
  return len + (PageOffset)sizeof(Slot_t) > spareUpper - spareLower;
}
Size ToastPage::GetUsed() const { return _usedSlots; };

SlotID ToastPage::InsertRecord(const uint8_t *src, const PageOffset len) {
  if (Full(len)) throw ToastPageFullException();
  spareUpper -= len;
  SetData(src, len, spareUpper);

  _usedSlots += 1;
  for (SlotID i = 0; i < slots.size(); ++i) {
    if (!HasRecord(i)) {
      slots[i].offset = spareUpper;
      slots[i].length = len;
      return i;
    }
  }
  Slot_t temp(len, spareUpper);
  slots.push_back(temp);
  spareLower += sizeof(Slot_t);
  return slots.size() - 1;
}

uint8_t *ToastPage::GetRecord(SlotID nSlotID) {
  // LAB1 BEGIN
  // TODO: 获得nSlotID槽位置的数据
  // TIPS: 使用GetData实现读数据
  // TIPS: 注意需要使用new分配_nFixed大小的空间
  // LAB1 END
  if (!HasRecord(nSlotID)) throw ToastPageException(nSlotID);
  uint8_t *data = new uint8_t[slots[nSlotID].length];
  GetData(data, slots[nSlotID].length, slots[nSlotID].offset);
  return data;
}

bool ToastPage::HasRecord(SlotID nSlotID) const {
  return slots[nSlotID].length > 0;
}

void ToastPage::DeleteRecord(SlotID nSlotID) {
  if (!HasRecord(nSlotID)) throw ToastPageException(nSlotID);
  //删除记录，直接将slot的长度置为0
  slots[nSlotID].length = 0;
  _usedSlots -= 1;
  RearrangeRec(nSlotID);
  return;
}

void ToastPage::UpdateRecord(SlotID nSlotID, const uint8_t *src,
                             const PageOffset len) {
  if (!HasRecord(nSlotID)) throw ToastPageException(nSlotID);
  if (len <= slots[nSlotID].length) {
    SetData(src, len, slots[nSlotID].offset);
    slots[nSlotID].length = len;
    RearrangeRec(nSlotID);
  } else {
    throw ToastPageFullException();
  }
}

}  // namespace thdb
