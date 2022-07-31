#ifndef TRANSACTION_H_
#define TRANSACTION_H_

#include "defines.h"
#include "manager/recovery_manager.h"
#include "manager/table_manager.h"

namespace thdb {

class Transaction {
 public:
  explicit Transaction(TxnID txn_id, std::vector<TxnID> active_list);
  ~Transaction() = default;
  TxnID GetID() const;
  bool is_Active(TxnID a) const;
  void InsertRecord(RecoveryManager* recovery_manager, const String table_name,
                    PageSlotID iPair);
  void Abort(TableManager* _pTableManager);
  void ClearJournal(RecoveryManager* recovery_manager);

 private:
  std::vector<std::pair<String, PageSlotID>> WriteRecord;
  TxnID txn_id_;
  std::vector<TxnID> active_Txn_;
};

}  // namespace thdb

#endif  // TRANSACTION_H_
