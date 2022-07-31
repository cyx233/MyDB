#include "manager/recovery_manager.h"

namespace thdb {

void RecoveryManager::Redo() {}

void RecoveryManager::Undo() {
  RecordPage *pPage = new RecordPage(RECOVERY_MANAGER_PAGEID);
  uint8_t *data;
  PageID *pageID = new PageID;
  SlotID *slotID = new SlotID;
  String table_name;
  FixedRecord *pRecord = new FixedRecord(
      3, {FieldType::STRING_TYPE, FieldType::INT_TYPE, FieldType::INT_TYPE},
      {TABLE_NAME_SIZE, 4, 4});
  for (SlotID i = 0; i < pPage->GetCap(); ++i) {
    if (pPage->HasRecord(i)) {
      data = pPage->GetRecord(i);
      pRecord->Load(data);
      delete data;
      Table *table = _pTableManager->GetTable(pRecord->GetField(0)->ToString());
      pRecord->GetField(1)->GetData((uint8_t *)pageID, 4);
      pRecord->GetField(2)->GetData((uint8_t *)slotID, 4);
      table->DeleteRecord(*pageID, *slotID);
    }
  }
  pPage->Clear();
  delete pPage;
  delete pageID;
  delete slotID;
}

void RecoveryManager::Remove(const String table_name, PageSlotID iPair) {
  RecordPage *pPage = new RecordPage(RECOVERY_MANAGER_PAGEID);
  uint8_t *data;
  PageID *pageID = new PageID;
  SlotID *slotID = new SlotID;
  FixedRecord *pRecord = new FixedRecord(
      3, {FieldType::STRING_TYPE, FieldType::INT_TYPE, FieldType::INT_TYPE},
      {TABLE_NAME_SIZE, 4, 4});
  for (SlotID i = 0; i < pPage->GetCap(); ++i) {
    if (pPage->HasRecord(i)) {
      data = pPage->GetRecord(i);
      pRecord->Load(data);
      delete data;
      pRecord->GetField(1)->GetData((uint8_t *)pageID, 4);
      pRecord->GetField(2)->GetData((uint8_t *)slotID, 4);
      if (pRecord->GetField(0)->ToString() == table_name &&
          *pageID == iPair.first && *slotID == iPair.second) {
        pPage->DeleteRecord(i);
        break;
      }
    }
  }
  delete pPage;
}

void RecoveryManager::Insert(const String table_name, PageSlotID iPair) {
  RecordPage *pPage = new RecordPage(RECOVERY_MANAGER_PAGEID);

  FixedRecord *pRecord = new FixedRecord(
      3, {FieldType::STRING_TYPE, FieldType::INT_TYPE, FieldType::INT_TYPE},
      {TABLE_NAME_SIZE, 4, 4});
  StringField *pString = new StringField(table_name);
  IntField *pPageID = new IntField(iPair.first);
  IntField *pSlotID = new IntField(iPair.second);

  pRecord->SetField(0, pString);
  pRecord->SetField(1, pPageID);
  pRecord->SetField(2, pSlotID);

  uint8_t *data = new uint8_t[100];
  pRecord->Store(data);
  pPage->InsertRecord(data);

  delete data;
  delete pPage;
  delete pRecord;
}

}  // namespace thdb
