#ifndef RECOVERY_MANAGER_H_
#define RECOVERY_MANAGER_H_

#include "macros.h"
#include "manager/table_manager.h"
#include "page/record_page.h"
#include "record/fixed_record.h"

namespace thdb {

class RecoveryManager {
 public:
  RecoveryManager(TableManager* a) : _pTableManager(a){};
  ~RecoveryManager() = default;

  void Redo();
  void Undo();
  void Insert(const String table_name, PageSlotID iPair);
  void Remove(const String table_name, PageSlotID iPair);

 private:
  TableManager* _pTableManager;
};

}  // namespace thdb

#endif  // RECOVERY_MANAGER_H_
