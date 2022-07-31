#include "record/fixed_record.h"

#include <assert.h>

#include "exception/exceptions.h"
#include "field/fields.h"

namespace thdb {

FixedRecord::FixedRecord(Size nFieldSize,
                         const std::vector<FieldType> &iTypeVec,
                         const std::vector<Size> &iSizeVec)
    : Record(nFieldSize), _iTypeVec(iTypeVec), _iSizeVec(iSizeVec) {
  assert(_iTypeVec.size() == nFieldSize);
  assert(_iSizeVec.size() == nFieldSize);
}

Size FixedRecord::Load(const uint8_t *src) {
  // LAB1 BEGIN
  // TODO: 反序列化，为定长记录导入各个字段数据
  // TIPS: 利用Field的抽象方法SetData导入数据
  // TIPS: 基于类型判断构建的指针类型
  // LAB1 END
  int size = 0;
  for (FieldID i = 0; i < _iFields.size(); ++i) {
    FieldType iType = _iTypeVec[i];
    if (iType == FieldType::NONE_TYPE) {
      SetField(i, new NoneField());
      continue;
    } else if (iType == FieldType::INT_TYPE) {
      SetField(i, new IntField());
    } else if (iType == FieldType::FLOAT_TYPE) {
      SetField(i, new FloatField());
    } else if (iType == FieldType::STRING_TYPE) {
      SetField(i, new StringField(""));
    } else {
      throw RecordTypeException();
    }
    _iFields[i]->SetData(src + size, _iSizeVec[i]);
    size += _iSizeVec[i];
  }
  return size;
}

Size FixedRecord::Store(uint8_t *dst) {
  // LAB1 BEGIN
  // TODO: 序列化，将定长数据转化为特定格式
  // TIPS: 利用Field的抽象方法GetData写出数据
  // TIPS: 基于类型进行dynamic_cast进行指针转化
  // LAB1 END
  int size = 0;
  for (uint32_t i = 0; i < _iFields.size(); ++i) {
    _iFields[i]->GetData(dst + size, _iSizeVec[i]);
    size += _iSizeVec[i];
  }
  return size;
}

void FixedRecord::Build(const std::vector<String> &iRawVec) {
  assert(iRawVec.size() == _iTypeVec.size());
  Clear();
  for (FieldID i = 0; i < _iFields.size(); ++i) {
    FieldType iType = _iTypeVec[i];
    if (iRawVec[i] == "NULL") {
      SetField(i, new NoneField());
      continue;
    }
    if (iType == FieldType::INT_TYPE) {
      int nVal = std::stoi(iRawVec[i]);
      SetField(i, new IntField(nVal));
    } else if (iType == FieldType::FLOAT_TYPE) {
      double fVal = std::stod(iRawVec[i]);
      SetField(i, new FloatField(fVal));
    } else if (iType == FieldType::STRING_TYPE) {
      SetField(i, new StringField(iRawVec[i].substr(1, iRawVec[i].size() - 2)));
    } else {
      throw RecordTypeException();
    }
  }
}

Record *FixedRecord::Copy() const {
  Record *pRecord = new FixedRecord(GetSize(), _iTypeVec, _iSizeVec);
  for (Size i = 0; i < GetSize(); ++i)
    pRecord->SetField(i, GetField(i)->Copy());
  return pRecord;
}

void FixedRecord::Sub(const std::vector<Size> &iPos) {
  bool bInSub[GetSize()];
  memset(bInSub, 0, GetSize() * sizeof(bool));
  for (const auto nPos : iPos) bInSub[nPos] = 1;
  auto itField = _iFields.begin();
  auto itType = _iTypeVec.begin();
  auto itSize = _iSizeVec.begin();
  for (Size i = 0; i < GetSize(); ++i) {
    if (!bInSub[i]) {
      Field *pField = *itField;
      if (pField) delete pField;
      itField = _iFields.erase(itField);
      itType = _iTypeVec.erase(itType);
      itSize = _iSizeVec.erase(itSize);
    } else {
      ++itField;
      ++itType;
      ++itSize;
    }
  }
}

void FixedRecord::Add(Record *pRecord) {
  FixedRecord *pFixed = dynamic_cast<FixedRecord *>(pRecord);
  assert(pFixed != nullptr);
  for (Size i = 0; i < pFixed->GetSize(); ++i) {
    _iFields.push_back(pFixed->GetField(i)->Copy());
    _iTypeVec.push_back(pFixed->_iTypeVec[i]);
    _iSizeVec.push_back(pFixed->_iSizeVec[i]);
  }
}

void FixedRecord::Remove(FieldID nPos) {
  Record::Remove(nPos);
  auto itType = _iTypeVec.begin() + nPos;
  auto itSize = _iSizeVec.begin() + nPos;
  _iTypeVec.erase(itType);
  _iSizeVec.erase(itSize);
}

}  // namespace thdb
