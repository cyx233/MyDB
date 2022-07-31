#ifndef THDB_TOAST_PAGE_H_
#define THDB_TOAST_PAGE_H_

#include "page/linked_page.h"
#include "utils/bitmap.h"

namespace thdb {

/**
 * @brief 变长记录页面。
 *
 */
class ToastPage : public LinkedPage {
 public:
  ToastPage();
  ToastPage(PageID nPageID);
  ~ToastPage();

  /**
   * @brief 插入一条定长记录
   *
   * @param src 记录定长格式化后的内容
   * @return SlotID 插入位置的槽编号
   */
  SlotID InsertRecord(const uint8_t *src, const PageOffset len);
  /**
   * @brief 获取指定位置的记录的内容
   *
   * @param nSlotID 槽编号
   * @return uint8_t* 记录定长格式化的内容
   */
  uint8_t *GetRecord(SlotID nSlotID);
  /**
   * @brief 判断某一个槽是否存在记录
   *
   * @param nSlotID 槽编号
   * @return true 存在记录
   * @return false 不存在记录
   */
  bool HasRecord(SlotID nSlotID) const;
  /**
   * @brief 删除制定位置的记录
   *
   * @param nSlotID 槽编号
   */
  void DeleteRecord(SlotID nSlotID);
  /**
   * @brief 原地更新一条记录的内容
   *
   * @param nSlotID 槽编号
   * @param src 新的变长格式化内容
   */
  void UpdateRecord(SlotID nSlotID, const uint8_t *src, const PageOffset len);

  bool Full(const PageOffset len) const;
  Size GetUsed() const;

 private:
  void RearrangeRec(SlotID nSlotID);
  SlotID _usedSlots;
  //槽
  struct Slot_t {
    PageOffset length;
    PageOffset offset;
    /* 如果槽是空的，length的长度为0 */
    Slot_t(PageOffset len = 0, PageOffset offset = 0) {
      this->length = len;
      this->offset = offset;
    }
  };
  /* 页的配置项所占用的空 ， 大小 - DPFIXED = 数据的空间 */
  PageOffset spareLower; /* 空闲空间的起始地址，终止地址 - 起始地址 =
                             剩余空闲空间的大小 */
  PageOffset spareUpper; /* 空闲空间的终止地址，终止地址 - 起始地址 =
                             剩余空闲空间的大小 */

  std::vector<Slot_t> slots;        /* 槽数据 */
  std::vector<PageOffset> prev_len; /* 初始槽长 */
};

}  // namespace thdb
#endif