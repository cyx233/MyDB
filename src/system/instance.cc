#include "system/instance.h"

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>

#include <algorithm>
#include <iostream>

#include "exception/exceptions.h"
#include "manager/table_manager.h"
#include "record/fixed_record.h"
#include "record/variable_record.h"

namespace thdb {

Instance::Instance() {
  _pTableManager = new TableManager();
  _pIndexManager = new IndexManager();
  _pRecoveryManager = new RecoveryManager(_pTableManager);
  _pTransactionManager =
      new TransactionManager(_pTableManager, _pRecoveryManager);
}

Instance::~Instance() {
  delete _pTableManager;
  delete _pIndexManager;
  delete _pTransactionManager;
  delete _pRecoveryManager;
}

Table *Instance::GetTable(const String &sTableName) const {
  return _pTableManager->GetTable(sTableName);
}

bool Instance::CreateTable(const String &sTableName, const Schema &iSchema,
                           bool useTxn) {
  if (useTxn) {
    std::vector<Column> temp;
    for (Size i = 0; i < iSchema.GetSize(); ++i) {
      temp.push_back(iSchema.GetColumn(i));
    }
    String name = "TxnID";
    Column col(name, FieldType::INT_TYPE);
    temp.push_back(col);
    Schema new_schema(temp);
    _pTableManager->AddTable(sTableName, new_schema);
  } else {
    _pTableManager->AddTable(sTableName, iSchema);
  }
  return true;
}

bool Instance::DropTable(const String &sTableName) {
  for (const auto &sColName : _pIndexManager->GetTableIndexes(sTableName))
    _pIndexManager->DropIndex(sTableName, sColName);
  _pTableManager->DropTable(sTableName);
  return true;
}

FieldID Instance::GetColID(const String &sTableName,
                           const String &sColName) const {
  Table *pTable = GetTable(sTableName);
  if (pTable == nullptr) throw TableException();
  return pTable->GetPos(sColName);
}

FieldType Instance::GetColType(const String &sTableName,
                               const String &sColName) const {
  Table *pTable = GetTable(sTableName);
  if (pTable == nullptr) throw TableException();
  return pTable->GetType(sColName);
}

Size Instance::GetColSize(const String &sTableName,
                          const String &sColName) const {
  Table *pTable = GetTable(sTableName);
  if (pTable == nullptr) throw TableException();
  return pTable->GetSize(sColName);
}

bool CmpPageSlotID(const PageSlotID &iA, const PageSlotID &iB) {
  if (iA.first == iB.first) return iA.second < iB.second;
  return iA.first < iB.first;
}

std::vector<PageSlotID> Intersection(std::vector<PageSlotID> iA,
                                     std::vector<PageSlotID> iB) {
  std::sort(iA.begin(), iA.end(), CmpPageSlotID);
  std::sort(iB.begin(), iB.end(), CmpPageSlotID);
  std::vector<PageSlotID> iRes{};
  std::set_intersection(iA.begin(), iA.end(), iB.begin(), iB.end(),
                        std::back_inserter(iRes));
  return iRes;
}

std::vector<PageSlotID> Instance::Search(
    const String &sTableName, Condition *pCond,
    const std::vector<Condition *> &iIndexCond, Transaction *txn) {
  Table *pTable = GetTable(sTableName);
  if (pTable == nullptr) throw TableException();
  if (iIndexCond.size() > 0) {
    IndexCondition *pIndexCond = dynamic_cast<IndexCondition *>(iIndexCond[0]);
    assert(pIndexCond != nullptr);
    auto iName = pIndexCond->GetIndexName();
    auto iRange = pIndexCond->GetIndexRange();
    std::vector<PageSlotID> iRes =
        GetIndex(iName.first, iName.second)->Range(iRange.first, iRange.second);
    for (Size i = 1; i < iIndexCond.size(); ++i) {
      IndexCondition *pIndexCond =
          dynamic_cast<IndexCondition *>(iIndexCond[i]);
      auto iName = pIndexCond->GetIndexName();
      auto iRange = pIndexCond->GetIndexRange();
      iRes = Intersection(iRes, GetIndex(iName.first, iName.second)
                                    ->Range(iRange.first, iRange.second));
    }
    if (txn != nullptr) {
      for (auto it = iRes.begin(); it != iRes.end();) {
        Record *temp = pTable->GetRecord(it->first, it->second);
        TxnID txn_id;
        temp->GetField(temp->GetSize() - 1)
            ->GetData((uint8_t *)&txn_id, sizeof(txn_id));
        if (txn->is_Active(txn_id) || txn->GetID() < txn_id) {
          it = iRes.erase(it);
        } else {
          ++it;
        }
      }
    }
    return iRes;
  } else {
    std::vector<PageSlotID> iRes = pTable->SearchRecord(pCond);
    if (txn != nullptr) {
      for (auto it = iRes.begin(); it != iRes.end();) {
        Record *temp = pTable->GetRecord(it->first, it->second);
        TxnID txn_id;
        temp->GetField(temp->GetSize() - 1)
            ->GetData((uint8_t *)&txn_id, sizeof(txn_id));
        if (txn->is_Active(txn_id) || txn->GetID() < txn_id) {
          it = iRes.erase(it);
        } else {
          ++it;
        }
      }
    }
    return iRes;
  }
}

PageSlotID Instance::Insert(const String &sTableName,
                            const std::vector<String> &iRawVec,
                            Transaction *txn) {
  Table *pTable = GetTable(sTableName);
  if (pTable == nullptr) throw TableException();
  Record *pRecord = pTable->EmptyRecord();
  if (txn == nullptr) {
    pRecord->Build(iRawVec);
  } else {
    std::vector<String> temp = iRawVec;
    temp.push_back(std::to_string(txn->GetID()));
    pRecord->Build(temp);
  }
  PageSlotID iPair = pTable->InsertRecord(pRecord);
  // Handle Insert on Index
  if (_pIndexManager->HasIndex(sTableName)) {
    auto iColNames = _pIndexManager->GetTableIndexes(sTableName);
    for (const auto &sCol : iColNames) {
      FieldID nPos = pTable->GetPos(sCol);
      Field *pKey = pRecord->GetField(nPos);
      _pIndexManager->GetIndex(sTableName, sCol)->Insert(pKey, iPair);
    }
  }
  if (txn) txn->InsertRecord(_pRecoveryManager, sTableName, iPair);

  delete pRecord;
  return iPair;
}

uint32_t Instance::Delete(const String &sTableName, Condition *pCond,
                          const std::vector<Condition *> &iIndexCond,
                          Transaction *txn) {
  auto iResVec = Search(sTableName, pCond, iIndexCond);
  Table *pTable = GetTable(sTableName);
  bool bHasIndex = _pIndexManager->HasIndex(sTableName);
  for (const auto &iPair : iResVec) {
    // Handle Delete on Index
    if (bHasIndex) {
      Record *pRecord = pTable->GetRecord(iPair.first, iPair.second);
      auto iColNames = _pIndexManager->GetTableIndexes(sTableName);
      for (const auto &sCol : iColNames) {
        FieldID nPos = pTable->GetPos(sCol);
        Field *pKey = pRecord->GetField(nPos);
        _pIndexManager->GetIndex(sTableName, sCol)->Delete(pKey, iPair);
      }
      delete pRecord;
    }

    pTable->DeleteRecord(iPair.first, iPair.second);
  }
  return iResVec.size();
}

uint32_t Instance::Update(const String &sTableName, Condition *pCond,
                          const std::vector<Condition *> &iIndexCond,
                          const std::vector<Transform> &iTrans,
                          Transaction *txn) {
  auto iResVec = Search(sTableName, pCond, iIndexCond);
  Table *pTable = GetTable(sTableName);
  bool bHasIndex = _pIndexManager->HasIndex(sTableName);
  for (const auto &iPair : iResVec) {
    // Handle Delete on Index
    if (bHasIndex) {
      Record *pRecord = pTable->GetRecord(iPair.first, iPair.second);
      auto iColNames = _pIndexManager->GetTableIndexes(sTableName);
      for (const auto &sCol : iColNames) {
        FieldID nPos = pTable->GetPos(sCol);
        Field *pKey = pRecord->GetField(nPos);
        _pIndexManager->GetIndex(sTableName, sCol)->Delete(pKey, iPair);
      }
      delete pRecord;
    }

    pTable->UpdateRecord(iPair.first, iPair.second, iTrans);

    // Handle Delete on Index
    if (bHasIndex) {
      Record *pRecord = pTable->GetRecord(iPair.first, iPair.second);
      auto iColNames = _pIndexManager->GetTableIndexes(sTableName);
      for (const auto &sCol : iColNames) {
        FieldID nPos = pTable->GetPos(sCol);
        Field *pKey = pRecord->GetField(nPos);
        _pIndexManager->GetIndex(sTableName, sCol)->Insert(pKey, iPair);
      }
      delete pRecord;
    }
  }
  return iResVec.size();
}

Record *Instance::GetRecord(const String &sTableName, const PageSlotID &iPair,
                            Transaction *txn) const {
  Table *pTable = GetTable(sTableName);
  if (txn == nullptr) {
    return pTable->GetRecord(iPair.first, iPair.second);
  } else {
    Record *temp = pTable->GetRecord(iPair.first, iPair.second);
    temp->Remove(temp->GetSize() - 1);
    return temp;
  }
}

std::vector<Record *> Instance::GetTableInfos(const String &sTableName) const {
  std::vector<Record *> iVec{};
  for (const auto &sName : GetColumnNames(sTableName)) {
    FixedRecord *pDesc = new FixedRecord(
        3,
        {FieldType::STRING_TYPE, FieldType::STRING_TYPE, FieldType::INT_TYPE},
        {COLUMN_NAME_SIZE, 10, 4});
    pDesc->SetField(0, new StringField(sName));
    pDesc->SetField(1,
                    new StringField(toString(GetColType(sTableName, sName))));
    pDesc->SetField(2, new IntField(GetColSize(sTableName, sName)));
    iVec.push_back(pDesc);
  }
  return iVec;
}
std::vector<String> Instance::GetTableNames() const {
  return _pTableManager->GetTableNames();
}
std::vector<String> Instance::GetColumnNames(const String &sTableName) const {
  return _pTableManager->GetColumnNames(sTableName);
}

bool Instance::IsIndex(const String &sTableName, const String &sColName) const {
  return _pIndexManager->IsIndex(sTableName, sColName);
}

Index *Instance::GetIndex(const String &sTableName,
                          const String &sColName) const {
  return _pIndexManager->GetIndex(sTableName, sColName);
}

std::vector<Record *> Instance::GetIndexInfos() const {
  std::vector<Record *> iVec{};
  for (const auto &iPair : _pIndexManager->GetIndexInfos()) {
    FixedRecord *pInfo =
        new FixedRecord(4,
                        {FieldType::STRING_TYPE, FieldType::STRING_TYPE,
                         FieldType::STRING_TYPE, FieldType::INT_TYPE},
                        {TABLE_NAME_SIZE, COLUMN_NAME_SIZE, 10, 4});
    pInfo->SetField(0, new StringField(iPair.first));
    pInfo->SetField(1, new StringField(iPair.second));
    pInfo->SetField(
        2, new StringField(toString(GetColType(iPair.first, iPair.second))));
    pInfo->SetField(3, new IntField(GetColSize(iPair.first, iPair.second)));
    iVec.push_back(pInfo);
  }
  return iVec;
}

bool Instance::CreateIndex(const String &sTableName, const String &sColName,
                           FieldType iType) {
  auto iAll = Search(sTableName, nullptr, {});
  _pIndexManager->AddIndex(sTableName, sColName, iType);
  Table *pTable = GetTable(sTableName);
  // Handle Exists Data
  for (const auto &iPair : iAll) {
    FieldID nPos = pTable->GetPos(sColName);
    Record *pRecord = pTable->GetRecord(iPair.first, iPair.second);
    Field *pKey = pRecord->GetField(nPos);
    _pIndexManager->GetIndex(sTableName, sColName)->Insert(pKey, iPair);
    delete pRecord;
  }
  return true;
}

bool Instance::DropIndex(const String &sTableName, const String &sColName) {
  auto iAll = Search(sTableName, nullptr, {});
  Table *pTable = GetTable(sTableName);
  for (const auto &iPair : iAll) {
    FieldID nPos = pTable->GetPos(sColName);
    Record *pRecord = pTable->GetRecord(iPair.first, iPair.second);
    Field *pKey = pRecord->GetField(nPos);
    _pIndexManager->GetIndex(sTableName, sColName)->Delete(pKey, iPair);
    delete pRecord;
  }
  _pIndexManager->DropIndex(sTableName, sColName);
  return true;
}

void Advance(std::vector<std::pair<Field *, Record *>> &all, Size &subset,
             Size &cur, FieldType ftype) {
  subset = cur;
  cur += 1;
  while (cur < all.size()) {
    if (Equal(all[subset].first, all[cur].first, ftype)) {
      cur += 1;
    } else {
      break;
    }
  }
}

void JoinWithIndex(std::vector<std::pair<Field *, Record *>> &a_all, Table *tb,
                   Index *b_index, JoinCondition *join_cond) {
  for (Size i = 0; i < a_all.size(); ++i) {
    Field *key = a_all[i].first;
    for (PageSlotID pageslot : b_index->Search(key)) {
      a_all[i].second->Add(tb->GetRecord(pageslot.first, pageslot.second));
    }
  }
  return;
}

void JoinNoIndex(std::vector<std::pair<Field *, Record *>> &a_all,
                 std::vector<std::pair<Field *, Record *>> &b_all,
                 FieldType ftype) {
  Size cur_a = 0;
  Size cur_b = 0;
  Size subset_a = 0;
  Size subset_b = 0;
  std::vector<std::pair<Field *, Record *>> filter;
  Advance(a_all, subset_a, cur_a, ftype);
  Advance(b_all, subset_b, cur_b, ftype);
  while (subset_a < a_all.size() && subset_b < b_all.size()) {
    if (Equal(a_all[subset_a].first, b_all[subset_b].first, ftype)) {
      for (Size i = subset_a; i < cur_a; ++i) {
        for (Size j = subset_b; j < cur_b; ++j) {
          Record *temp = a_all[i].second->Copy();
          temp->Add(b_all[j].second);
          filter.push_back({NULL, temp});
        }
      }
      Advance(a_all, subset_a, cur_a, ftype);
      Advance(b_all, subset_b, cur_b, ftype);
    } else if (Less(a_all[subset_a].first, b_all[subset_b].first, ftype)) {
      Advance(a_all, subset_a, cur_a, ftype);
    } else {
      Advance(b_all, subset_b, cur_b, ftype);
    }
  }
  for (auto i : a_all) {
    delete i.second;
  }
  a_all.clear();
  a_all = filter;
  return;
}

std::pair<std::vector<String>, std::vector<Record *>> Instance::Join(
    std::map<String, std::vector<PageSlotID>> &iResultMap,
    std::vector<Condition *> &iJoinConds) {
  // LAB3 BEGIN
  // TODO:实现正确且高效的表之间JOIN过程

  // ALERT:由于实现临时表存储具有一定难度，所以允许JOIN过程中将中间结果保留在内存中，不需要存入临时表
  // ALERT:一定要注意，存在JOIN字段值相同的情况，需要特别重视
  // ALERT:针对于不同的JOIN情况（此处只需要考虑数据量和是否为索引列），可以选择使用不同的JOIN算法
  // ALERT:JOIN前已经经过了Filter过程
  // ALERT:建议不要使用不经过优化的NestedLoopJoin算法

  // TIPS:JoinCondition中保存了JOIN两方的表名和列名
  // TIPS:利用GetTable(TableName)的方式可以获得Table*指针，之后利用lab1中的Table::GetRecord获得初始Record*数据
  // TIPs:利用Table::GetColumnNames可以获得Table初始的列名，与初始Record*顺序一致
  // TIPS:Record对象添加了Copy,Sub,Add,Remove函数，方便同学们对于Record进行处理
  // TIPS:利用GetColID/Type/Size三个函数可以基于表名和列名获得列的信息
  // TIPS:利用IsIndex可以判断列是否存在索引
  // TIPS:利用GetIndex可以获得索引Index*指针

  // EXTRA:JOIN的表的数量超过2时，所以需要先计算一个JOIN执行计划（不要求复杂算法）,有兴趣的同学可以自行实现
  // EXTRA:在多表JOIN时，可以采用并查集或执行树来确定执行JOIN的数据内容
  std::pair<std::vector<String>, std::vector<Record *>> ans;
  std::map<String, String> merged_tables;
  std::map<String, std::map<String, Size>> sub_table_offset;
  std::map<String, std::vector<std::pair<Field *, Record *>>> temp_table_record;
  std::map<String, std::vector<String>> temp_table_field;
  for (Condition *i : iJoinConds) {
    merged_tables[((JoinCondition *)i)->sTableA] = "";
    merged_tables[((JoinCondition *)i)->sTableB] = "";
    sub_table_offset[((JoinCondition *)i)->sTableA]
                    [((JoinCondition *)i)->sTableA] = 0;
    sub_table_offset[((JoinCondition *)i)->sTableB]
                    [((JoinCondition *)i)->sTableB] = 0;
  }
  for (Condition *i : iJoinConds) {
    JoinCondition *join_cond = (JoinCondition *)i;
    FieldType ftype;
    String tableA = join_cond->sTableA;
    String tableB = join_cond->sTableB;
    String src[2] = {join_cond->sTableA, join_cond->sTableB};

    bool indexA = false;
    bool indexB = false;
    if (IsIndex(join_cond->sTableA, join_cond->sColA) &&
        merged_tables[tableA] == "") {
      indexA = true;
    } else if (IsIndex(join_cond->sTableB, join_cond->sColB) &&
               merged_tables[tableB] == "") {
      indexB = true;
    }

    while (merged_tables[tableA] != "" && merged_tables[tableA] != tableA) {
      tableA = merged_tables[tableA];
    }
    while (merged_tables[tableB] != "" && merged_tables[tableB] != tableB) {
      tableB = merged_tables[tableB];
    }

    if (indexA) {
      String temp = tableB;
      tableB = tableA;
      tableA = temp;
      temp = src[1];
      src[1] = src[0];
      src[0] = temp;
    }

    merged_tables[tableB] = merged_tables[join_cond->sTableB] =
        merged_tables[tableA] = merged_tables[join_cond->sTableA] = tableA;

    Size field_size = 0;
    if (temp_table_field.find(tableA) == temp_table_field.end()) {
      Table *table = GetTable(tableA);
      field_size = table->GetColumnNames().size();
    } else {
      field_size = temp_table_field[tableA].size();
    }

    for (auto i : sub_table_offset[tableB]) {
      sub_table_offset[tableA][i.first] = i.second + field_size;
    }

    String table_name[2] = {tableA, tableB};
    String col_name[2] = {join_cond->sColA, join_cond->sColB};

    for (int i = 0; i < 2; ++i) {
      if ((indexA || indexB) && i == 1) {
        break;
      }
      if (temp_table_record.find(table_name[i]) == temp_table_record.end()) {
        Table *table = GetTable(table_name[i]);
        ftype = table->GetType(col_name[i]);
        FieldID col;
        for (Size j = 0; j < table->GetColumnNames().size(); ++j)
          if (table->GetColumnNames()[j] == col_name[i]) {
            col = j;
            break;
          }
        temp_table_field[table_name[i]] = table->GetColumnNames();
        temp_table_record[table_name[i]] =
            std::vector<std::pair<Field *, Record *>>();
        std::vector<PageSlotID> result = iResultMap[table_name[i]];
        for (PageSlotID page_slot : result) {
          Record *temp = table->GetRecord(page_slot.first, page_slot.second);
          temp_table_record[table_name[i]].push_back(
              {temp->GetField(col), temp});
        }
      } else {
        FieldID col;
        for (Size j = sub_table_offset[table_name[i]][src[i]];
             j < temp_table_field[table_name[i]].size(); ++j)
          if (temp_table_field[table_name[i]][j] == col_name[i]) {
            col = j;
            break;
          }
        for (Size j = 0; j < temp_table_field[table_name[i]].size(); ++j)
          if (temp_table_field[table_name[i]][j] == col_name[i]) {
            for (Size k = 0; k < temp_table_record[table_name[i]].size(); ++k) {
              temp_table_record[table_name[i]][k].first =
                  temp_table_record[table_name[i]][k].second->GetField(col);
            }
            ftype = temp_table_record[table_name[i]].front().first->GetType();
            break;
          }
      }
    }

    if (indexA) {
      Index *index = GetIndex(join_cond->sTableA, join_cond->sColA);
      Table *ta = GetTable(join_cond->sTableA);
      JoinWithIndex(temp_table_record[tableA], ta, index, join_cond);
    } else if (indexB) {
      Index *index = GetIndex(join_cond->sTableB, join_cond->sColB);
      Table *tb = GetTable(join_cond->sTableB);
      JoinWithIndex(temp_table_record[tableA], tb, index, join_cond);
    } else {
      sort(temp_table_record[tableA].begin(), temp_table_record[tableA].end(),
           [ftype](std::pair<Field *, Record *> a,
                   std::pair<Field *, Record *> b) -> bool {
             return Less(a.first, b.first, ftype);
           });
      sort(temp_table_record[tableB].begin(), temp_table_record[tableB].end(),
           [ftype](std::pair<Field *, Record *> a,
                   std::pair<Field *, Record *> b) -> bool {
             return Less(a.first, b.first, ftype);
           });
      for (auto i : temp_table_field[tableB]) {
        temp_table_field[tableA].push_back(i);
      }
      JoinNoIndex(temp_table_record[tableA], temp_table_record[tableB], ftype);
    }
  }
  for (auto i : merged_tables) {
    if (i.second == i.first) {
      ans.first = temp_table_field[i.second];
      for (auto j : temp_table_record[i.second]) ans.second.push_back(j.second);
    } else {
      for (auto j : temp_table_record[i.first]) delete j.second;
    }
  }
  return ans;
}

}  // namespace thdb
