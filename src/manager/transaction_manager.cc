#include "manager/transaction_manager.h"

#include "transaction/transaction.h"

namespace thdb {

Transaction *TransactionManager::Begin() {
  Transaction *new_txn = new Transaction(NewTxnID++, active_Txn_);
  active_Txn_.push_back(new_txn->GetID());
  return new_txn;
}

void TransactionManager::Commit(Transaction *txn) {
  txn->ClearJournal(recovery_manager_);
  for (auto it = active_Txn_.begin(); it != active_Txn_.end(); ++it) {
    if (*it == txn->GetID()) {
      active_Txn_.erase(it);
      break;
    }
  }
  return;
}

void TransactionManager::Abort(Transaction *txn) {
  txn->ClearJournal(recovery_manager_);
  txn->Abort(_pTableManager);
  for (auto it = active_Txn_.begin(); it != active_Txn_.end(); ++it) {
    if (*it == txn->GetID()) {
      active_Txn_.erase(it);
      break;
    }
  }
  return;
}

}  // namespace thdb
