#include "page/node_page.h"

#include <assert.h>
#include <float.h>

#include "exception/exceptions.h"
#include "field/fields.h"
#include "macros.h"
#include "minios/os.h"

namespace thdb {

const PageOffset LEAF_OFFSET = 8;
const PageOffset USED_SLOT_OFFSET = 12;
const PageOffset KEY_LEN_OFFSET = 16;
const PageOffset KEY_TYPE_OFFSET = 20;

NodePage::NodePage(Size nKeyLen, FieldType iKeyType)
    : _nKeyLen(nKeyLen), _iKeyType(iKeyType) {
  // TODO: 基于自己实现的Store算法确定最大容量
  _nUsed = 0;
  _nCap = (DATA_SIZE - sizeof(PageID)) /
              (_nKeyLen + sizeof(PageSlotID) + sizeof(PageID)) -
          1;
}

NodePage::NodePage(Size nKeyLen, FieldType iKeyType,
                   const std::vector<Field *> &iDataKeyVec,
                   const std::vector<PageSlotID> &iDataVec,
                   const std::vector<PageID> &iChildVec)
    : _nKeyLen(nKeyLen),
      _iKeyType(iKeyType),
      _iDataKeyVec(iDataKeyVec),
      _iDataVec(iDataVec),  // TODO: 基于自己实现的Store算法确定最大容量
      _iChildVec(iChildVec) {
  _nUsed = _iDataKeyVec.size();
  _nCap = (DATA_SIZE - sizeof(PageID)) /
              (_nKeyLen + sizeof(PageSlotID) + sizeof(PageID)) -
          1;
}

NodePage::NodePage(PageID nPageID) : Page(nPageID) {
  // TODO: 从格式化页面中导入结点信息
  // TODO: 确定最大容量
  Load();
  _nCap =
      (DATA_SIZE - sizeof(PageSlotID)) / (_nKeyLen + sizeof(PageSlotID)) + 1;
}

NodePage::~NodePage() {
  // TODO: 将结点信息格式化并写回到页面中
  // TODO: 注意析构KeyVec中的指针
  if (removed == false) {
    Store();
  }
  for (Field *i : _iDataKeyVec) delete i;
}

bool NodePage::Insert(Field *pKey, const PageSlotID &iPair) {
  // 叶节点：
  // 1.确定插入位置后插入数据即可
  // 中间节点:
  // 1.确定执行插入函数的子节点
  // 2.对应的子节点执行插入函数
  // 3.判断子节点是否为满结点，满结点时执行分裂
  if (pKey == NULL) return false;
  if (Empty()) {
    _iDataKeyVec.push_back(pKey->Copy());
    _iDataVec.push_back(iPair);
    return true;
  }
  Size insert_pos = LowerBound(pKey);
  if (_iChildVec.size() == 0) {  //叶节点
    _iDataKeyVec.insert(_iDataKeyVec.begin() + insert_pos, pKey->Copy());
    _iDataVec.insert(_iDataVec.begin() + insert_pos, iPair);
    return true;
  }
  NodePage child_node = NodePage(_iChildVec[insert_pos]);
  child_node.Insert(pKey, iPair);
  if (child_node.Full()) {
    std::vector<Field *> newDataKeyVec;
    std::vector<PageSlotID> newDataVec;
    std::vector<PageID> newChildVec;
    std::pair<Field *, PageSlotID> ascend_node =
        child_node.PopHalf(newDataKeyVec, newDataVec, newChildVec);
    NodePage new_child =
        NodePage(_nKeyLen, _iKeyType, newDataKeyVec, newDataVec, newChildVec);
    _iDataKeyVec.insert(_iDataKeyVec.begin() + insert_pos, ascend_node.first);
    _iDataVec.insert(_iDataVec.begin() + insert_pos, ascend_node.second);
    _iChildVec.insert(_iChildVec.begin() + insert_pos + 1,
                      new_child.GetPageID());
  }
  return true;
}

Size NodePage::Delete(Field *pKey) {
  // ALERT: 注意删除结点过程中如果清除了Key则需要析构
  // ALERT:
  // 注意存在键值相同的情况发生，所以需要保证所有需要执行删除函数的子结点都执行了删除函数
  // ALERT: 可以适当简化合并函数，例如不删除空的中间结点true
  // TODO: 需要基于结点类型判断执行过程
  // 叶结点：
  // 1.确定删除位置后删除数据即可
  // 中间结点:
  // 1.确定执行删除函数的范围
  // 2.对应的子结点执行删除函数
  // 3.判断子结点是否为空结点，空结点时清除空结点，执行Merge后跳转至1
  // 4.删除节点上数据

  // ALERT:
  // 由于Insert过程中保证了没用相同的Value值，所以只要成功删除一个结点即可保证删除成功
  Size lower = LowerBound(pKey);
  Size upper = UpperBound(pKey);
  Size ans = 0;
  if (_iChildVec.size() == 0) {  //叶子节点
    for (Size i = lower; i < upper; ++i) {
      ans += 1;
      delete _iDataKeyVec[i];
      _iDataKeyVec.erase(_iDataKeyVec.begin() + i);
      _iDataVec.erase(_iDataVec.begin() + i);
      --i;
      --upper;
    }
  } else {                                   //中间节点
    for (Size i = lower; i <= upper; ++i) {  //在子节点
      NodePage child = NodePage(_iChildVec[i]);
      Size child_delete = child.Delete(pKey);
      ans += child_delete;
      if (child.Empty()) {
        Size pair = i < _nUsed ? i : i - 1;
        Field *pair_key = _iDataKeyVec[pair];
        PageSlotID pair_val = _iDataVec[pair];
        _iChildVec.erase(_iChildVec.begin() + i);
        _iDataKeyVec.erase(_iDataKeyVec.begin() + pair);
        _iDataVec.erase(_iDataVec.begin() + pair);
        if (MergeWithChild()) {
          Insert(pair_key, pair_val);
          ans += Delete(pKey);
          return ans;
        }
        MiniOS::GetOS()->DeletePage(child.GetPageID());
        child.removed = true;
        Insert(pair_key, pair_val);
        upper = UpperBound(pKey);
      }
    }
    for (Size i = lower; i < upper; ++i) {  //在节点上
      ans += 1;
      delete _iDataKeyVec[i];
      NodePage next = NodePage(_iChildVec[i + 1]);
      Field *next_key = next.FirstKey();
      PageSlotID next_val = next.FirstValue();
      ans += next.Delete(next.FirstKey(), next.FirstValue());
      if (next.Empty()) {
        _iChildVec.erase(_iChildVec.begin() + i + 1);
        _iDataKeyVec.erase(_iDataKeyVec.begin() + i);
        _iDataVec.erase(_iDataVec.begin() + i);
        if (MergeWithChild()) {
          Insert(next_key, next_val);
          ans += Delete(pKey);
          return ans;
        }
        MiniOS::GetOS()->DeletePage(next.GetPageID());
        next.removed = true;
        Insert(next_key, next_val);
      } else {
        _iDataKeyVec[i] = next_key;
        _iDataVec[i] = next_val;
      }
      upper = UpperBound(pKey);
      --i;
    }
  }
  return ans;
}

bool NodePage::Delete(Field *pKey, const PageSlotID &iPair) {
  // TODO: 需要基于结点类型判断执行过程
  // 叶结点：
  // 1.确定删除位置后删除数据即可
  // 中间结点:
  // 1.确定执行删除函数的范围
  // 2.对应的子结点执行删除函数
  // 3.若成功，判断子结点是否为空结点，空结点时清除空结点，执行Merge并返回
  // 4.尝试删除此节点的数据

  // ALERT:
  // 由于Insert过程中保证了没用相同的Value值，所以只要成功删除一个结点即可保证删除成功
  Size lower = LowerBound(pKey);
  Size upper = UpperBound(pKey);
  if (_iChildVec.size() == 0) {  //叶子节点
    for (Size i = lower; i < upper; ++i) {
      if (_iDataVec[i] == iPair) {
        delete _iDataKeyVec[i];
        _iDataKeyVec.erase(_iDataKeyVec.begin() + i);
        _iDataVec.erase(_iDataVec.begin() + i);
        return true;
      }
    }
  } else {  //中间节点
    for (Size i = lower; i < upper; ++i) {
      if (_iDataVec[i] == iPair) {  //在节点上
        delete _iDataKeyVec[i];
        NodePage next = NodePage(_iChildVec[i + 1]);
        Field *next_key = next.FirstKey();
        PageSlotID next_val = next.FirstValue();
        next.Delete(next.FirstKey(), next.FirstValue());
        if (next.Empty()) {
          _iChildVec.erase(_iChildVec.begin() + i + 1);
          _iDataKeyVec.erase(_iDataKeyVec.begin() + i);
          _iDataVec.erase(_iDataVec.begin() + i);
          MergeWithChild();
          Insert(next_key, next_val);
        } else {
          MiniOS::GetOS()->DeletePage(next.GetPageID());
          next.removed = true;
          _iDataKeyVec[i] = next_key;
          _iDataVec[i] = next_val;
        }
        return true;
      }
    }
    for (Size i = lower; i <= upper; ++i) {  //在子节点
      NodePage child = NodePage(_iChildVec[i]);
      if (child.Delete(pKey, iPair)) {
        if (child.Empty()) {
          Size pair = i < _nUsed ? i : i - 1;
          Field *pair_key = _iDataKeyVec[pair];
          PageSlotID pair_val = _iDataVec[pair];
          _iChildVec.erase(_iChildVec.begin() + i);
          _iDataKeyVec.erase(_iDataKeyVec.begin() + pair);
          _iDataVec.erase(_iDataVec.begin() + pair);
          if (!MergeWithChild()) {
            MiniOS::GetOS()->DeletePage(child.GetPageID());
            child.removed = true;
          }
          Insert(pair_key, pair_val);
        }
        return true;
      }
    }
  }
  return false;
}

bool NodePage::Update(Field *pKey, const PageSlotID &iOld,
                      const PageSlotID &iNew) {
  // TODO: 需要基于结点类型判断执行过程
  // 叶结点：
  // 1.确定更新位置后更新数据即可
  // 中间结点:
  // 1.确定执行更新函数的子结点
  // 2.对应的子结点执行更新函数

  // ALERT: 由于更新函数不改变结点内存储的容量，所以不需要结构变化
  // ALERT:
  // 由于Insert过程中保证了没用相同的Value值，所以只要成功更新一个结点即可保证更新成功
  Size lower = LowerBound(pKey);
  Size upper = UpperBound(pKey);
  if (_iChildVec.size() == 0) {  //叶子节点
    for (Size i = lower; i < upper; ++i) {
      if (_iDataVec[i] == iOld) {
        _iDataVec[i] = iNew;
        return true;
      }
    }
  } else {  //中间节点
    for (Size i = lower; i < upper; ++i) {
      if (_iDataVec[i] == iOld) {  //在节点上
        _iDataVec[i] = iNew;
        return true;
      }
    }
    for (Size i = lower; i <= upper; ++i) {  //在子节点
      NodePage child = NodePage(_iChildVec[i]);
      if (child.Update(pKey, iOld, iNew)) {
        return true;
      }
    }
  }
  return false;
}

std::vector<PageSlotID> NodePage::Range(Field *pLow, Field *pHigh) {
  // TODO: 需要基于结点类型判断执行过程
  // 叶结点：
  // 1.确定上下界范围，返回这一区间内的所有Value值
  // 中间结点:
  // 1.确定所有可能包含上下界范围的子结点
  // 2.依次对添加各个子结点执行查询函数所得的结果

  // ALERT: 注意叶结点可能为空结点，需要针对这种情况进行特判
  Size lower = LowerBound(pLow);
  Size upper = LowerBound(pHigh);
  std::vector<PageSlotID> ans;
  if (_iChildVec.size() == 0) {  //叶子节点
    return std::vector<PageSlotID>(_iDataVec.begin() + lower,
                                   _iDataVec.begin() + upper);
  }
  for (Size i = lower; i < upper; ++i) {
    NodePage child = NodePage(_iChildVec[i]);
    for (auto child_ans : child.Range(pLow, pHigh)) ans.push_back(child_ans);
    ans.push_back(_iDataVec[i]);
  }
  NodePage child = NodePage(_iChildVec[upper]);
  for (auto i : child.Range(pLow, pHigh)) ans.push_back(i);
  return ans;
}

std::vector<PageSlotID> NodePage::Search(Field *key) {
  // TODO: 需要基于结点类型判断执行过程
  // 叶结点：
  // 1.确定上下界范围，返回这一区间内的所有Value值
  // 中间结点:
  // 1.确定所有可能包含上下界范围的子结点
  // 2.依次对添加各个子结点执行查询函数所得的结果

  // ALERT: 注意叶结点可能为空结点，需要针对这种情况进行特判
  Size lower = LowerBound(key);
  Size upper = UpperBound(key);
  std::vector<PageSlotID> ans;
  if (_iChildVec.size() == 0) {  //叶子节点
    return std::vector<PageSlotID>(_iDataVec.begin() + lower,
                                   _iDataVec.begin() + upper);
  }
  for (Size i = lower; i < upper; ++i) {
    NodePage child = NodePage(_iChildVec[i]);
    for (auto child_ans : child.Search(key)) ans.push_back(child_ans);
    ans.push_back(_iDataVec[i]);
  }
  NodePage child = NodePage(_iChildVec[upper]);
  for (auto i : child.Search(key)) ans.push_back(i);
  return ans;
}

bool NodePage::InsertChild(Size insertPos, const PageID pageID) {
  _iChildVec.insert(_iChildVec.begin() + insertPos, pageID);
  return true;
}

void NodePage::Clear() {
  // TODO: 需要基于结点类型判断执行过程
  // 叶结点：直接释放占用空间
  // 中间结点：先释放子结点空间，之后释放自身占用空间
  for (auto i : _iChildVec) {
    NodePage child = NodePage(i);
    child.Clear();
    MiniOS::GetOS()->DeletePage(i);
    child.removed = true;
  }
  for (auto i : _iDataKeyVec) delete i;
  _iChildVec.clear();
  _iDataKeyVec.clear();
  _iDataVec.clear();
  _nUsed = 0;
}

bool NodePage::Full() const { return _iDataKeyVec.size() >= _nCap; }
bool NodePage::Empty() const { return _iDataKeyVec.size() == 0; }

FieldType NodePage::GetType() const { return _iKeyType; }
Size NodePage::GetKeyLen() const { return _nKeyLen; }

Field *NodePage::FirstKey() const {
  if (Empty()) return nullptr;
  return _iDataKeyVec[0];
}

PageSlotID NodePage::FirstValue() const {
  if (Empty()) return {0, 0};
  return _iDataVec[0];
}

std::pair<Field *, PageSlotID> NodePage::PopHalf(
    std::vector<Field *> &newDataKeyVec, std::vector<PageSlotID> &newDataVec,
    std::vector<PageID> &newChildVec) {
  Size mid = _nCap / 2;
  for (auto it = _iDataKeyVec.begin() + mid + 1; it != _iDataKeyVec.end();) {
    newDataKeyVec.push_back(*it);
    it = _iDataKeyVec.erase(it);
  }
  for (auto it = _iDataVec.begin() + mid + 1; it != _iDataVec.end();) {
    newDataVec.push_back(*it);
    it = _iDataVec.erase(it);
  }
  if (_iChildVec.size() > 0) {
    for (auto it = _iChildVec.begin() + mid + 1; it != _iChildVec.end();) {
      newChildVec.push_back(*it);
      it = _iChildVec.erase(it);
    }
  }
  std::pair<Field *, PageSlotID> ascend_node = {_iDataKeyVec.back()->Copy(),
                                                _iDataVec.back()};
  _iDataKeyVec.pop_back();
  _iDataVec.pop_back();
  return ascend_node;
}

void NodePage::Load() {
  // TODO: 从格式化页面数据中导入结点信息
  // TODO: 自行设计，注意和Store匹配
  Size _bLeaf = 0;
  GetHeader((uint8_t *)&_bLeaf, sizeof(Size), LEAF_OFFSET);
  GetHeader((uint8_t *)&_nUsed, sizeof(Size), USED_SLOT_OFFSET);
  GetHeader((uint8_t *)&_nKeyLen, sizeof(Size), KEY_LEN_OFFSET);
  GetHeader((uint8_t *)&_iKeyType, sizeof(Size), KEY_TYPE_OFFSET);
  if (_nUsed == 0) return;

  PageOffset key_begin = 0;
  uint8_t *key_data = new uint8_t[_nKeyLen];
  for (Size i = 0; i < _nUsed; ++i) {
    GetData((uint8_t *)key_data, _nKeyLen, key_begin + i * _nKeyLen);
    Field *key = NULL;
    switch (_iKeyType) {
      case FieldType::FLOAT_TYPE:
        key = new FloatField;
        break;
      case FieldType::INT_TYPE:
        key = new IntField;
        break;
      case FieldType::STRING_TYPE:
        key = new StringField(_nKeyLen);
        break;
      default:
        break;
    }
    key->SetData(key_data, _nKeyLen);
    _iDataKeyVec.push_back(key);
  }

  PageOffset val_begin = _nUsed * _nKeyLen;
  PageSlotID *val_data;
  val_data = new PageSlotID[_nUsed];
  GetData((uint8_t *)val_data, sizeof(PageSlotID) * _nUsed, val_begin);
  _iDataVec.insert(_iDataVec.begin(), val_data, val_data + _nUsed);

  delete key_data;
  delete val_data;

  if (_bLeaf > 0) {
    PageOffset child_begin = val_begin + _nUsed * sizeof(PageSlotID);
    PageID *child_data;
    child_data = new PageID[_bLeaf];
    GetData((uint8_t *)child_data, sizeof(PageID) * _bLeaf, child_begin);
    _iChildVec.insert(_iChildVec.begin(), child_data, child_data + _bLeaf);
    delete child_data;
  }
}

void NodePage::Store() {
  // TODO: 格式化结点信息并保存到页面中
  // TODO: 自行设计，注意和Load匹配
  Size _bLeaf = _iChildVec.size();
  _nUsed = _iDataKeyVec.size();
  SetHeader((uint8_t *)&_bLeaf, sizeof(Size), LEAF_OFFSET);
  SetHeader((uint8_t *)&_nUsed, sizeof(Size), USED_SLOT_OFFSET);
  SetHeader((uint8_t *)&_nKeyLen, sizeof(Size), KEY_LEN_OFFSET);
  SetHeader((uint8_t *)&_iKeyType, sizeof(Size), KEY_TYPE_OFFSET);

  PageOffset key_begin = 0;
  uint8_t *data = new uint8_t[_nKeyLen];
  for (Size i = 0; i < _iDataKeyVec.size(); ++i) {
    _iDataKeyVec[i]->GetData(data, _nKeyLen);
    SetData(data, _nKeyLen, key_begin + i * _nKeyLen);
  }

  PageOffset val_begin = _nUsed * _nKeyLen;
  SetData((uint8_t *)_iDataVec.data(), sizeof(PageSlotID) * _nUsed, val_begin);

  if (_bLeaf > 0) {
    PageOffset child_begin = val_begin + _nUsed * sizeof(PageSlotID);
    SetData((uint8_t *)_iChildVec.data(), sizeof(PageID) * _bLeaf, child_begin);
  }
}

bool NodePage::MergeWithChild() {
  if (_iChildVec.size() >= 2) return false;
  NodePage child = NodePage(_iChildVec[0]);
  Field *my_key = FirstKey();
  PageSlotID my_val = FirstValue();
  for (Field *i : child._iDataKeyVec) {
    _iDataKeyVec.push_back(i->Copy());
  }
  _iDataVec = child._iDataVec;
  _iChildVec = child._iChildVec;
  Insert(my_key, my_val);
  MiniOS::GetOS()->DeletePage(child.GetPageID());
  child.removed = true;
  return true;
}

void NodePage::Copy(NodePage *copy) {
  copy->_iChildVec = this->_iChildVec;
  copy->_iDataKeyVec = this->_iDataKeyVec;
  copy->_iDataVec = this->_iDataVec;
}

Size NodePage::LowerBound(Field *pField) {
  // TODO: 二分查找找下界，此处给出实现
  // TODO: 边界的理解非常重要，可以自行重新测试一下
  Size nBegin = 0, nEnd = _iDataKeyVec.size();
  while (nBegin < nEnd) {
    Size nMid = (nBegin + nEnd) / 2;
    if (!Less(_iDataKeyVec[nMid], pField, _iKeyType)) {
      nEnd = nMid;
    } else {
      nBegin = nMid + 1;
    }
  }
  return nBegin;
}

Size NodePage::UpperBound(Field *pField) {
  // TODO: 二分查找找上界，此处给出实现
  // TODO: 边界的理解非常重要，可以自行重新测试一下
  Size nBegin = 0, nEnd = _iDataKeyVec.size();
  while (nBegin < nEnd) {
    Size nMid = (nBegin + nEnd) / 2;
    if (Greater(_iDataKeyVec[nMid], pField, _iKeyType)) {
      nEnd = nMid;
    } else {
      nBegin = nMid + 1;
    }
  }
  return nBegin;
}

Size NodePage::LessBound(Field *pField) {
  if (Empty()) return 0;
  if (Less(pField, _iDataKeyVec[0], _iKeyType)) return 0;
  return UpperBound(pField) - 1;
}

}  // namespace thdb
