#ifndef TRANSACTION_MANAGER_H_
#define TRANSACTION_MANAGER_H_

#include <unordered_map>

#include "defines.h"
#include "transaction/transaction.h"

namespace thdb {

class TransactionManager {
 public:
  TransactionManager(TableManager *a, RecoveryManager *b)
      : _pTableManager(a), recovery_manager_(b){};
  ~TransactionManager() = default;

  Transaction *Begin();
  void Commit(Transaction *txn);
  void Abort(Transaction *txn);
  void InsertRecord(Transaction *txn);

 private:
  TxnID NewTxnID = 0;
  std::vector<TxnID> active_Txn_;
  TableManager *_pTableManager;
  RecoveryManager *recovery_manager_;
};

}  // namespace thdb

#endif  // TRANSACTION_MANAGER_H_
