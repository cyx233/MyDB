#include "index/index.h"

#include "page/node_page.h"

namespace thdb {
Index::Index(FieldType iType) {
  // TODO: 建立一个新的根结点，注意需要基于类型判断根结点的属性
  // TODO: 根结点需要设为中间结点
  // TODO: 注意记录RootID
  Size nKeyLen = 0;
  switch (iType) {
    case FieldType::FLOAT_TYPE:
      nKeyLen = 8;
      break;
    case FieldType::INT_TYPE:
      nKeyLen = 4;
      break;
    case FieldType::STRING_TYPE:
      nKeyLen = 20;
      break;
    default:
      break;
  }
  NodePage node = NodePage(nKeyLen, iType);
  _nRootID = node.GetPageID();
}

Index::Index(PageID nPageID) {
  // TODO: 记录RootID即可
  NodePage root = NodePage(nPageID);
  _nRootID = root.GetPageID();
}

Index::~Index() {
  // TODO: 如果不添加额外的指针，理论上不用额外写回内容
}

void Index::Clear() {
  // TODO: 利用RootID获得根结点
  // TODO: 利用根结点的Clear函数清除全部索引占用页面
  NodePage root = NodePage(_nRootID);
  root.Clear();
}

PageID Index::GetRootID() const { return _nRootID; }

bool Index::Insert(Field *pKey, const PageSlotID &iPair) {
  // TODO: 利用RootID获得根结点
  // TODO: 利用根结点的Insert执行插入
  // TODO: 根结点满时，需要进行分裂操作，同时更新RootID
  NodePage root = NodePage(_nRootID);
  root.Insert(pKey, iPair);
  if (root.Full()) {
    std::vector<Field *> newDataKeyVec;
    std::vector<PageSlotID> newDataVec;
    std::vector<PageID> newChildVec;
    std::pair<Field *, PageSlotID> ascend_node =
        root.PopHalf(newDataKeyVec, newDataVec, newChildVec);

    NodePage new_child = NodePage(root.GetKeyLen(), root.GetType(),
                                  newDataKeyVec, newDataVec, newChildVec);

    NodePage new_root = NodePage(root.GetKeyLen(), root.GetType());

    new_root.Insert(ascend_node.first, ascend_node.second);
    new_root.InsertChild(0, _nRootID);
    new_root.InsertChild(1, new_child.GetPageID());

    _nRootID = new_root.GetPageID();
  }
  return true;
}

Size Index::Delete(Field *pKey) {
  // ALERT:
  // 结点合并实现难度较高，没有测例，不要求实现，感兴趣的同学可自行实现并设计测例
  // TODO: 利用RootID获得根结点
  // TODO: 利用根结点的Delete执行删除
  NodePage root = NodePage(_nRootID);
  return root.Delete(pKey);
}

bool Index::Delete(Field *pKey, const PageSlotID &iPair) {
  // ALERT:
  // 结点合并实现难度较高，没有测例，不要求实现，感兴趣的同学可自行实现并设计测例
  // TODO: 利用RootID获得根结点
  // TODO: 利用根结点的Delete执行删除
  NodePage root = NodePage(_nRootID);
  return root.Delete(pKey, iPair);
}

bool Index::Update(Field *pKey, const PageSlotID &iOld,
                   const PageSlotID &iNew) {
  // TODO: 利用RootID获得根结点
  // TODO: 利用根结点的Update执行删除
  NodePage root = NodePage(_nRootID);
  return root.Update(pKey, iOld, iNew);
}

std::vector<PageSlotID> Index::Range(Field *pLow, Field *pHigh) {
  // TODO: 利用RootID获得根结点
  // TODO: 利用根结点的Range执行范围查找
  NodePage root = NodePage(_nRootID);
  return root.Range(pLow, pHigh);
}

std::vector<PageSlotID> Index::Search(Field *key) {
  // TODO: 利用RootID获得根结点
  // TODO: 利用根结点的Range执行范围查找
  NodePage root = NodePage(_nRootID);
  return root.Search(key);
}
}  // namespace thdb
