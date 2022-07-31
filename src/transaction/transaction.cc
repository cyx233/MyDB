#include "transaction/transaction.h"

namespace thdb {

Transaction::Transaction(TxnID txn_id, std::vector<TxnID> active_Txn)
    : txn_id_(txn_id), active_Txn_(active_Txn) {}

TxnID Transaction::GetID() const { return txn_id_; }

bool Transaction::is_Active(TxnID a) const {
  for (TxnID i : active_Txn_)
    if (i == a) return true;
  return false;
}

void Transaction::InsertRecord(RecoveryManager* recovery_manager,
                               const String sTableName, PageSlotID iPair) {
  WriteRecord.push_back({sTableName, iPair});
  recovery_manager->Insert(sTableName, iPair);
}

void Transaction::ClearJournal(RecoveryManager* recovery_manager) {
  for (auto i : WriteRecord) {
    recovery_manager->Remove(i.first, i.second);
  }
}

void Transaction::Abort(TableManager* _pTableManager) {
  for (auto i : WriteRecord) {
    _pTableManager->GetTable(i.first)->DeleteRecord(i.second.first,
                                                    i.second.second);
  }
}

}  // namespace thdb
