#include "record/variable_record.h"

#include <assert.h>

#include <string>

#include "exception/exceptions.h"
#include "field/fields.h"

namespace thdb {

VariableRecord::VariableRecord(Size nFieldSize,
                               const std::vector<FieldType> &iTypeVec,
                               const std::vector<Size> &iSizeVec)
    : Record(nFieldSize), _iTypeVec(iTypeVec), _iSizeVec(iSizeVec) {
  assert(_iTypeVec.size() == nFieldSize);
  assert(_iSizeVec.size() == nFieldSize);
}

void VariableRecord::Sub(const std::vector<Size> &iPos) {
  bool bInSub[GetSize()];
  memset(bInSub, 0, GetSize() * sizeof(bool));
  for (const auto nPos : iPos) bInSub[nPos] = 1;
  auto itField = _iFields.begin();
  auto itSize = _iSizeVec.begin();
  auto itType = _iTypeVec.begin();
  for (Size i = 0; i < GetSize(); ++i) {
    if (!bInSub[i]) {
      Field *pField = *itField;
      if (pField) delete pField;
      itField = _iFields.erase(itField);
      itSize = _iSizeVec.erase(itSize);
      itType = _iTypeVec.erase(itType);
    } else {
      ++itField;
      ++itSize;
      ++itType;
    }
  }
}

void VariableRecord::Add(Record *pRecord) {
  VariableRecord *temp = (VariableRecord *)pRecord;
  for (Size i = 0; i < temp->GetSize(); ++i) {
    _iFields.push_back(temp->_iFields[i]->Copy());
    _iSizeVec.push_back(temp->_iSizeVec[i]);
    _iTypeVec.push_back(temp->_iTypeVec[i]);
  }
}

void VariableRecord::Remove(FieldID nPos) {
  assert(nPos < GetSize());
  auto it = _iFields.begin() + nPos;
  if (*it) delete (*it);
  _iFields.erase(it);
  _iSizeVec.erase(_iSizeVec.begin() + nPos);
  _iTypeVec.erase(_iTypeVec.begin() + nPos);
}

Record *VariableRecord::Copy() const {
  Record *pRecord = new VariableRecord(GetSize(), _iTypeVec, _iSizeVec);
  for (Size i = 0; i < GetSize(); ++i)
    pRecord->SetField(i, GetField(i)->Copy());
  return pRecord;
}

Size VariableRecord::GetTotSize() const {
  Size nTotal = sizeof(PageOffset);
  for (uint32_t i = 0; i < _iFields.size(); ++i) {
    if (_iFields[i]->GetType() == FieldType::STRING_TYPE) {
      nTotal += sizeof(PageOffset);
    }
  }
  for (Size nSize : _iSizeVec) nTotal += nSize;
  return nTotal;
}
Size VariableRecord::Load(const uint8_t *src) { return 0; }

Size VariableRecord::VarLoad(const uint8_t *src) {
  uint32_t size = sizeof(PageOffset);
  PageOffset str_num = *(PageOffset *)src;
  std::vector<PageOffset> strLen(str_num);
  memcpy(strLen.data(), src + size, sizeof(PageOffset) * str_num);
  size += sizeof(PageOffset) * str_num;
  int cur_str = 0;
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
      _iSizeVec[i] = strLen[cur_str];
      cur_str += 1;
    } else {
      throw RecordTypeException();
    }
    _iFields[i]->SetData(src + size, _iSizeVec[i]);
    size += _iSizeVec[i];
  }
  return size;
}
Size VariableRecord::Store(uint8_t *dst) { return 0; }

Size VariableRecord::VarStore(uint8_t *dst) {
  int size = 0;
  std::vector<PageOffset> strLen;
  for (uint32_t i = 0; i < _iFields.size(); ++i) {
    if (_iFields[i]->GetType() == FieldType::STRING_TYPE) {
      _iSizeVec[i] = ((StringField *)_iFields[i])->GetString().size();
      strLen.push_back(_iSizeVec[i]);
    }
  }
  PageOffset str_num = strLen.size();
  memcpy(dst, &str_num, sizeof(PageOffset));
  size += sizeof(PageOffset);
  memcpy(dst + size, strLen.data(), sizeof(PageOffset) * strLen.size());
  size += sizeof(PageOffset) * strLen.size();
  for (uint32_t i = 0; i < _iFields.size(); ++i) {
    _iFields[i]->GetData(dst + size, _iSizeVec[i]);
    size += _iSizeVec[i];
  }
  return size;
}

void VariableRecord::Build(const std::vector<String> &iRawVec) {
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

}  // namespace thdb
