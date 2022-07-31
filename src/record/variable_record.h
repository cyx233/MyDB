#ifndef THDB_VARIABLE_RECORD_H_
#define THDB_VARIABLE_RECORD_H_

#include "defines.h"
#include "field/field.h"
#include "record/record.h"

namespace thdb {

class VariableRecord : public Record {
 public:
  VariableRecord(Size nFieldSize, const std::vector<FieldType> &iTypeVec,
                 const std::vector<Size> &iSizeVec);
  ~VariableRecord() = default;

  /**
   * @brief 记录反序列化
   *
   * @param src 反序列化源数据
   * @return Size 反序列化使用的数据长度
   */
  Size Load(const uint8_t *src) override;
  Size VarLoad(const uint8_t *src);
  /**
   * @brief 记录序列化
   *
   * @param dst 序列化结果存储位置
   * @return Size 序列化使用的数据长度
   */
  Size Store(uint8_t *dst) override;
  Size VarStore(uint8_t *dst);
  /**
   * @brief 从String数据构建记录
   *
   * @param iRawVec Insert语句中的String数组
   */
  void Build(const std::vector<String> &iRawVec) override;
  Size GetTotSize() const;

  Record *Copy() const;
  /**
   * @brief 截取Record的部分字段
   */
  void Sub(const std::vector<Size> &iPos);
  /**
   * @brief 向Record后添加部分字段
   */
  void Add(Record *pRecord);
  /**
   * @brief 删除Record中一个字段
   */
  void Remove(FieldID nPos);

 private:
  /**
   * @brief 各个字段的类型
   */
  std::vector<FieldType> _iTypeVec;
  /**
   * @brief 各个字段分配的空间长度
   */
  std::vector<Size> _iSizeVec;
};

}  // namespace thdb

#endif
