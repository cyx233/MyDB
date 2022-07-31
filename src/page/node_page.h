#ifndef THDB_NODE_PAGE_H_
#define THDB_NODE_PAGE_H_

#include "defines.h"
#include "field/field.h"
#include "page/page.h"

namespace thdb {

class Index;

/**
 * @brief 同时表示了中间结点和叶结点。
 * 子节点Value为子结点{PageID,0}。
 * iDataKeyVec为实际字段的Key，Value为对应记录的PageSlotID。
 */
class NodePage : public Page {
 public:
  /**
   * @brief 初始化结点页面
   * @param nKeyLen Key长度
   * @param iKeyType Key类型
   */
  NodePage(Size nKeyLen, FieldType iKeyType);
  /**
   * @brief 构建一个包含一定数量子结点的结点页面
   *
   * @param nKeyLen Key长度
   * @param iKeyType Key类型
   * @param iDataKeyVec 数据结点的Key值
   * @param iDataVec 数据结点的Value值
   * @param iChildVec 子结点的Value值
   */
  NodePage(Size nKeyLen, FieldType iKeyType,
           const std::vector<Field *> &iDataKeyVec,
           const std::vector<PageSlotID> &iDataVec,
           const std::vector<PageID> &iChildVec);
  /**
   * @brief 导入一个已经存在的页面结点。
   *
   * @param nPageID 页面结点的页编号
   */
  NodePage(PageID nPageID);
  ~NodePage();

  /**
   * @brief 插入一条Key Value Pair
   * @param pKey 插入的Key
   * @param iPair 插入的Value
   * @return true 插入成功
   * @return false 插入失败
   */
  bool Insert(Field *pKey, const PageSlotID &iPair);
  /**
   * @brief 删除某个Key下所有的Key Value Pair
   * @param pKey 删除的Key
   * @return Size 删除的键值数量
   */
  Size Delete(Field *pKey);
  /**
   * @brief 删除某个Key Value Pair
   * @param pKey 删除的Key
   * @param iPair 删除的Value
   * @return true 删除成功
   * @return false 删除失败
   */
  bool Delete(Field *pKey, const PageSlotID &iPair);
  /**
   * @brief 更新某个Key Value Pair到新的Value
   * @param pKey 更新的Key
   * @param iOld 原始的Value
   * @param iNew 要更新成的新Value
   * @return true 更新成功
   * @return false 更新失败
   */
  bool Update(Field *pKey, const PageSlotID &iOld, const PageSlotID &iNew);
  /**
   * @brief 使用索引进行范围查找，左闭右开区间[pLow, pHigh)
   *
   * @param pLow
   * @param pHigh
   * @return std::vector<PageSlotID> 所有符合范围条件的Value数组
   */
  std::vector<PageSlotID> Range(Field *pLow, Field *pHigh);
  /**
   * @brief 使用索引进行查找
   *
   * @param key
   * @return std::vector<PageSlotID> 所有符合范围条件的Value数组
   */
  std::vector<PageSlotID> Search(Field *key);
  /**
   * @brief 清空当前结点和所有子结点所占用的所有空间
   */

  /**
   * @brief 连接一个子节点
   * @param insert_pos 插入的位置
   * @param PageID 插入的子节点页面
   * @return true 插入成功
   * @return false 插入失败
   */
  bool InsertChild(Size insertPos, const PageID pageID);

  void Clear();

  /**
   * @brief 判断结点是否为满结点
   */
  bool Full() const;
  /**
   * @brief 判断结点是否为空结点
   */
  bool Empty() const;

  /**
   * @brief 判断结点是否为数据结点
   */
  bool IsDataPtr() const;

  /**
   * @brief 获得结点保存的索引字段类型
   */
  FieldType GetType() const;

  /**
   * @brief 获得结点保存的Key长度
   */
  Size GetKeyLen() const;

  /**
   * @brief 分裂当前结点，清除当前结点中后一半的结点。
   * @return std::pair<Field *, PageSlotID>
   * 分裂出的后一半Key Value Pair数据，返回值为上升的结点
   */
  std::pair<Field *, PageSlotID> PopHalf(std::vector<Field *> &newDataKeyVec,
                                         std::vector<PageSlotID> &newDataVec,
                                         std::vector<PageID> &newChildVec);

 private:
  /**
   * @brief 解析格式化的页面数据，初始化结点信息。
   */
  void Load();
  /**
   * @brief 将结点信息保存为格式化的页面数据。
   */
  void Store();

  bool MergeWithChild();
  void Copy(NodePage *copy);

  /**
   * @brief 获得该结点第一个Key
   */
  Field *FirstKey() const;

  /**
   * @brief 获得该结点第一个Value
   */
  PageSlotID FirstValue() const;

  /**
   * @brief 不小于pKey的第一个Key在KeyVec中的位置
   */
  Size LowerBound(Field *pKey);
  /**
   * @brief 大于pKey的第一个Key在KeyVec中的位置
   */
  Size UpperBound(Field *pKey);
  /**
   * @brief 小于等于pKey的最后一个Key在KeyVec中的位置
   */
  Size LessBound(Field *pKey);

  /**
   * @brief 结点页面一个Key占用的空间
   */
  Size _nKeyLen;

  /**
   * @brief 结点页面能存储的最大KeyValuePair容量
   */
  Size _nCap;
  /**
   * @brief 结点页面已经存储的KeyValuePair数量
   */
  Size _nUsed;
  /**
   * @brief 结点页面Key的类型
   */
  FieldType _iKeyType;

  /**
   * @brief Key数组，用于存储类型为Field*的数据Key
   */
  std::vector<Field *> _iDataKeyVec;
  /**
   * @brief Value数组，用于存储类型为PageSlotID的数据Value
   */
  std::vector<PageSlotID> _iDataVec;
  /**
   * @brief Value数组，用于存储类型为PageID的子节点Value
   */
  std::vector<PageID> _iChildVec;
  friend class Index;
  bool removed = false;
};

}  // namespace thdb

#endif
