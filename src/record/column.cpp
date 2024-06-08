#include "record/column.h"

#include "glog/logging.h"

Column::Column(std::string column_name, TypeId type, uint32_t index, bool nullable, bool unique)
    : name_(std::move(column_name)), type_(type), table_ind_(index), nullable_(nullable), unique_(unique) {
  ASSERT(type != TypeId::kTypeChar, "Wrong constructor for CHAR type.");
  switch (type) {
    case TypeId::kTypeInt:
      len_ = sizeof(int32_t);
      break;
    case TypeId::kTypeFloat:
      len_ = sizeof(float_t);
      break;
    default:
      ASSERT(false, "Unsupported column type.");
  }
}

Column::Column(std::string column_name, TypeId type, uint32_t length, uint32_t index, bool nullable, bool unique)
    : name_(std::move(column_name)),
      type_(type),
      len_(length),
      table_ind_(index),
      nullable_(nullable),
      unique_(unique) {
  ASSERT(type == TypeId::kTypeChar, "Wrong constructor for non-VARCHAR type.");
}

Column::Column(const Column *other)
    : name_(other->name_),
      type_(other->type_),
      len_(other->len_),
      table_ind_(other->table_ind_),
      nullable_(other->nullable_),
      unique_(other->unique_) {}

/**
* TODO: Student Implement
*/
uint32_t Column::SerializeTo(char *buf) const {
  // replace with your code here
  uint32_t offset = 0;

    // COLUMN_MAGIC_NUM
    MACH_WRITE_TO(uint32_t, buf + offset, COLUMN_MAGIC_NUM);
    offset += sizeof(uint32_t);

    // name and its size
    size_t str_length = name_.size();
    MACH_WRITE_TO(size_t, buf + offset, str_length);
    offset += sizeof(size_t);

    memcpy(buf + offset, name_.c_str(), str_length);
    offset += str_length;

    // typeId
    uint32_t type_int = static_cast<int>(type_);
    MACH_WRITE_TO(uint32_t , buf + offset, type_int);
    offset += sizeof(uint32_t);

    // len
    MACH_WRITE_TO(uint32_t, buf + offset, len_);
    offset += sizeof(uint32_t);

    // table_ind
    MACH_WRITE_TO(uint32_t, buf + offset, table_ind_);
    offset += sizeof(uint32_t);

    // nullable
    MACH_WRITE_TO(bool, buf + offset, nullable_);
    offset += sizeof(bool);

    // unique
    MACH_WRITE_TO(bool, buf + offset, unique_);
    offset += sizeof(bool);

  return offset;
}

/**
 * TODO: Student Implement
 */
uint32_t Column::GetSerializedSize() const {
  // replace with your code here
  uint32_t serialize_size = 0;

  // COLUMN_MAGIC_NUM
  serialize_size += sizeof(uint32_t);

  // name and its size
  serialize_size += sizeof(size_t);
  serialize_size += name_.size();

  // type
  serialize_size += sizeof(uint32_t);

  // len
  serialize_size += sizeof(uint32_t);

  // table_ind
  serialize_size += sizeof(uint32_t);

  // nullable
  serialize_size += sizeof(bool);

  // unique
  serialize_size += sizeof(bool);

  return serialize_size;
}

/**
 * TODO: Student Implement
 */
uint32_t Column::DeserializeFrom(char *buf, Column *&column) {
  // replace with your code here
    uint32_t offset = 0;
    if (column != nullptr) {
        LOG(WARNING) << "Pointer to column is not null in column deserialize." 									 << std::endl;
    }
    /* deserialize field from buf */
    uint32_t column_magic_num = MACH_READ_FROM(uint32_t, buf + offset);
    offset += sizeof(uint32_t);

    // name and its size
    size_t name_size = MACH_READ_FROM(size_t, buf + offset);
    offset += sizeof(size_t);

    std::string column_name = std::string(buf + offset, buf + offset + name_size);
    offset += name_size;

    // typeId
    uint32_t type_int = MACH_READ_FROM(uint32_t, buf + offset);
    TypeId type = static_cast<TypeId>(type_int);
    offset += sizeof(uint32_t);

    // len
    uint32_t col_len = MACH_READ_FROM(uint32_t, buf + offset);
    offset += sizeof(uint32_t);

    // table_ind
    uint32_t col_ind = MACH_READ_FROM(uint32_t, buf + offset);
    offset += sizeof(uint32_t);

    // nullable
    bool nullable = MACH_READ_FROM(bool, buf + offset);
    offset += sizeof(bool);

    // unique
    bool unique = MACH_READ_FROM(bool, buf + offset);
    offset += sizeof(bool);


    /* allocate object */
    if (type == kTypeChar) {
        column = new Column(column_name, type, col_len, col_ind, nullable, unique);
    } else {
        column = new Column(column_name, type, col_ind, nullable, unique);
    }
    return offset;
}

bool Column::CompareEqualTo(const Column *other) {
    if (name_ != other->GetName())
        return false;
    if (type_ != other->GetType())
        return false;
    if (len_ != other->GetLength())
        return false;
    if (table_ind_ != other->GetTableInd())
        return false;
    if (nullable_ != other->IsNullable())
        return false;
    if (unique_ != other->IsUnique())
        return false;

    return true;
}
