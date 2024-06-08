#include "record/schema.h"

/**
 * TODO: Student Implement
 */
uint32_t Schema::SerializeTo(char *buf) const {
  // replace with your code here
  uint32_t offset = 0;
    // SCHEMA_MAGIC_NUM
    MACH_WRITE_TO(uint32_t, buf + offset, SCHEMA_MAGIC_NUM);
    offset += sizeof(uint32_t);

    // number of columns
    uint32_t num_columns = columns_.size();
    MACH_WRITE_TO(uint32_t, buf + offset, num_columns);
    offset += sizeof(uint32_t);

    // each column
    for (const Column* column : columns_) {
        offset += column->SerializeTo(buf + offset);
    }

    // is_manage
    MACH_WRITE_TO(bool, buf + offset, is_manage_);
    offset += sizeof(bool);

  return offset;
}

uint32_t Schema::GetSerializedSize() const {
  // replace with your code here
  uint32_t serialize_size = 0;

  // magic
  serialize_size += sizeof(uint32_t);

  // number of columns
  serialize_size += sizeof(uint32_t);

  // each column
  for (Column *column : columns_) {
      serialize_size += column->GetSerializedSize();
  }

  // is_manage
  serialize_size += sizeof(bool);

  return serialize_size;
}

uint32_t Schema::DeserializeFrom(char *buf, Schema *&schema) {
  // replace with your code here
  uint32_t offset = 0;

  // magic
  uint32_t schema_magic_num = MACH_READ_FROM(uint32_t, buf + offset);
  offset += sizeof(uint32_t);

  // the number of column
  uint32_t num_columns = MACH_READ_FROM(uint32_t, buf + offset);
  offset += sizeof(uint32_t);

  // each colum
  std::vector<Column *> columns_;
  for (uint32_t i = 0; i < num_columns; i++) {
      Column *column;
      offset += Column::DeserializeFrom(buf + offset, column);
      columns_.push_back(column);
  }

  // is_manage
  bool is_manage = MACH_READ_FROM(bool, buf + offset);
  offset += sizeof(bool);

  schema = new Schema(columns_, is_manage);
  
  return offset;
}

bool Schema::CompareEqualTo(const Schema *other) {
    if (this->GetColumnCount() != other->GetColumnCount())
        return false;
    for (uint32_t i = 0; i < this->GetColumnCount(); i++) {
        if (!columns_[i]->CompareEqualTo(other->GetColumn(i)))
            return false;
    }
    return true;
}
