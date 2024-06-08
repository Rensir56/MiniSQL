#include "record/row.h"

#include "common/macros.h"

/**
 * TODO: Student Implement
 */
uint32_t Row::SerializeTo(char *buf, Schema *schema) const {    // why schema??
  ASSERT(schema != nullptr, "Invalid schema before serialize.");
  ASSERT(schema->GetColumnCount() == fields_.size(), "Fields size do not match schema's column size.");
  // replace with your code here
  uint32_t offset = 0;

    // Serialize page_id
    MACH_WRITE_TO(page_id_t, buf + offset, rid_.GetPageId());
    offset += sizeof(page_id_t);

    // Serialize slot_num
    MACH_WRITE_TO(uint32_t, buf + offset, rid_.GetSlotNum());
    offset += sizeof(uint32_t);

    // Serialize number of fields
    uint32_t num_fields = fields_.size();
    MACH_WRITE_TO(uint32_t, buf + offset, num_fields);
    offset += sizeof(uint32_t);

    // Serialize null bitmap placeholder
    uint32_t null_bitmap_offset = offset;
    uint32_t null_bitmap_size = (num_fields + 7) / 8;
    memset(buf + offset, 0, null_bitmap_size);
    offset += null_bitmap_size;

    // Serialize each field
    uint32_t index = 0;
    for (const Field* field : fields_) {
        // Serialize field type
        TypeId typeId = field->GetTypeId();
        uint32_t typeId_int = static_cast<uint32_t>(typeId);
        MACH_WRITE_TO(uint32_t , buf + offset, typeId_int);
        offset += sizeof(uint32_t);

        if (field->IsNull()) {
            // if field is null, set the corresponding bit in null bitmap to 1
            uint32_t byte_index = index / 8;
            uint32_t bit_index = index % 8;
            buf[null_bitmap_offset + byte_index] |= (1 << bit_index);
        } else {
            offset += field->SerializeTo(buf + offset);
        }
        index++;
    }

  return offset;
}

uint32_t Row::DeserializeFrom(char *buf, Schema *schema) {
  ASSERT(schema != nullptr, "Invalid schema before serialize.");
  ASSERT(fields_.empty(), "Non empty field in row.");
  // replace with your code here
  uint32_t offset = 0;

  // Deserialize page_id
  page_id_t  page_id;
  page_id = MACH_READ_FROM(page_id_t, buf + offset);
  offset += sizeof(page_id_t);

  // Deserialize slot_num
  uint32_t slot_num;
  slot_num = MACH_READ_FROM(uint32_t, buf + offset);
  offset += sizeof(uint32_t);

  rid_.Set(page_id, slot_num);

  // Deserialize number of fields
  uint32_t num_fields;
  num_fields = MACH_READ_FROM(uint32_t, buf + offset);
  offset += sizeof(uint32_t);

  // Deserialize null bitmap
  uint32_t null_bitmap_offset = offset;
  uint32_t null_bitmap_size = (num_fields + 7) / 8;
  offset += null_bitmap_size;

  // Deserialize each field
  for (uint32_t i = 0; i < num_fields; i++) {
      // Check if the i-th bit in null bitmap is set
      bool is_null = buf[null_bitmap_offset + i / 8] & (1 << (i % 8));

      // Deserialize field type
      uint32_t typeId_int = MACH_READ_FROM(uint32_t, buf + offset);
      TypeId typeId = static_cast<TypeId>(typeId_int);
      offset += sizeof(uint32_t);

      // Deserialize field value based on its type
      switch (typeId) {
          case TypeId::kTypeInt: {
              Field *field = new Field(kTypeInt);
              TypeInt typeInt;
              offset += typeInt.DeserializeFrom(buf + offset, &field, is_null);
              fields_.push_back(field);
              break;
          }
          case TypeId::kTypeFloat: {
              Field *field = new Field(kTypeFloat);
              TypeFloat typeFloat;
              offset += typeFloat.DeserializeFrom(buf + offset, &field, is_null);
              fields_.push_back(field);
              break;
          }
          case TypeId::kTypeChar: {
              Field *field = new Field(kTypeChar);
              TypeChar typeChar;
              offset += typeChar.DeserializeFrom(buf + offset, &field, is_null);
              fields_.push_back(field);
              break;
          }
          default:
              ASSERT(false, "Unsupported type for deserialization");
              break;
      }
  }

  
  return offset;
}

uint32_t Row::GetSerializedSize(Schema *schema) const {
  ASSERT(schema != nullptr, "Invalid schema before serialize.");
  ASSERT(schema->GetColumnCount() == fields_.size(), "Fields size do not match schema's column size.");
  // replace with your code here

  uint32_t serialized_size = 0;

  // page_id and slot_sum
  serialized_size += sizeof(page_id_t) + sizeof(uint32_t);

  // number of fields
  serialized_size += sizeof(uint32_t);

  // bitmap
  serialized_size += (fields_.size() + 7) / 8;

  // each field
  for (const Field* field : fields_) {
      // field type
      serialized_size += sizeof(uint32_t);

      // field
      if (!field->IsNull())
        serialized_size += field->GetSerializedSize();
      }

  return serialized_size;
}

bool Row::CompareEqualTo(const Row *other) {
    if (!(rid_ == other->rid_))
        return false;
    if (fields_.size() != other->GetFieldCount())
        return false;

    for (uint32_t i = 0; i < fields_.size(); i++) {
        if (!fields_[i]->CompareEquals(*other->GetField(i)))
            return false;
    }
    return true;
}

void Row::GetKeyFromRow(const Schema *schema, const Schema *key_schema, Row &key_row) {
  auto columns = key_schema->GetColumns();
  std::vector<Field> fields;
  uint32_t idx;
  for (auto column : columns) {
    schema->GetColumnIndex(column->GetName(), idx);
    fields.emplace_back(*this->GetField(idx));
  }
  key_row = Row(fields);
}
