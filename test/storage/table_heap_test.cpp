#include "storage/table_heap.h"

#include <unordered_map>
#include <vector>

#include "common/instance.h"
#include "gtest/gtest.h"
#include "record/field.h"
#include "record/schema.h"
#include "utils/utils.h"

static string db_file_name = "table_heap_test.db";
using Fields = std::vector<Field>;

TEST(TableHeapTest, TableHeapSampleTest) {
  // init testing instance
  remove(db_file_name.c_str());
  auto disk_mgr_ = new DiskManager(db_file_name);
  auto bpm_ = new BufferPoolManager(DEFAULT_BUFFER_POOL_SIZE, disk_mgr_);
  const int row_nums = 10000;
  // create schema
  std::vector<Column *> columns = {new Column("id", TypeId::kTypeInt, 0, false, false),
                                   new Column("name", TypeId::kTypeChar, 64, 1, true, false),
                                   new Column("account", TypeId::kTypeFloat, 2, true, false)};
  auto schema = std::make_shared<Schema>(columns);
  // create rows
  std::unordered_map<int64_t, Fields *> row_values;
  uint32_t size = 0;
  TableHeap *table_heap = TableHeap::Create(bpm_, schema.get(), nullptr, nullptr, nullptr);
  for (int i = 0; i < row_nums; i++) {
    int32_t len = RandomUtils::RandomInt(0, 64);
    char *characters = new char[len];
    RandomUtils::RandomString(characters, len);
    Fields *fields =
        new Fields{Field(TypeId::kTypeInt, i), Field(TypeId::kTypeChar, const_cast<char *>(characters), len, true),
                   Field(TypeId::kTypeFloat, RandomUtils::RandomFloat(-999.f, 999.f))};
    Row row(*fields);
    ASSERT_TRUE(table_heap->InsertTuple(row, nullptr));
    if (row_values.find(row.GetRowId().Get()) != row_values.end()) {
      std::cout << row.GetRowId().Get() << std::endl;
      ASSERT_TRUE(false);
    } else {
      row_values.emplace(row.GetRowId().Get(), fields);
      size++;
    }
    delete[] characters;
  }

  ASSERT_EQ(row_nums, row_values.size());
  ASSERT_EQ(row_nums, size);
  for (auto row_kv : row_values) {
    size--;
    Row row(RowId(row_kv.first));
    table_heap->GetTuple(&row, nullptr);
    ASSERT_EQ(schema.get()->GetColumnCount(), row.GetFields().size());
    for (size_t j = 0; j < schema.get()->GetColumnCount(); j++) {
      ASSERT_EQ(CmpBool::kTrue, row.GetField(j)->CompareEquals(row_kv.second->at(j)));
    }
    // free spaces
    delete row_kv.second;
  }
  ASSERT_EQ(size, 0);
}


// then UpdateTuple, compare new as true.
// then DeleteTuple and ApplyDelete, GetTuple should not be found
// begin and end leave them to exe part

TEST(TableHeapTest, TableHeapOperateTest) {
  remove(db_file_name.c_str());
  auto disk_mgr_ = new DiskManager(db_file_name);
  auto bpm_ = new BufferPoolManager(DEFAULT_BUFFER_POOL_SIZE, disk_mgr_);
  const int row_nums = 10000;
  // create schema
  std::vector<Column *> columns = {new Column("id", TypeId::kTypeInt, 0, false, false),
                                   new Column("name", TypeId::kTypeChar, 64, 1, true, false),
                                   new Column("account", TypeId::kTypeFloat, 2, true, false)};
  auto schema = std::make_shared<Schema>(columns);
  // create rows
  std::unordered_map<int64_t, Fields *> row_values;
  uint32_t size = 0;
  TableHeap *table_heap = TableHeap::Create(bpm_, schema.get(), nullptr, nullptr, nullptr);

  // Insert first
  for (int i = 0; i < row_nums; i++) {
    int32_t len = RandomUtils::RandomInt(0, 64);
    char *characters = new char[len];
    RandomUtils::RandomString(characters, len);
    Fields *fields =
        new Fields{Field(TypeId::kTypeInt, i), Field(TypeId::kTypeChar, const_cast<char *>(characters), len, true),
                   Field(TypeId::kTypeFloat, RandomUtils::RandomFloat(-999.f, 999.f))};
    Row row(*fields);
    ASSERT_TRUE(table_heap->InsertTuple(row, nullptr));

    if (row_values.find(row.GetRowId().Get()) != row_values.end()) {
      std::cout << row.GetRowId().Get() << std::endl;
      ASSERT_TRUE(false);
    } else {
      row_values.emplace(row.GetRowId().Get(), fields);
      size++;
    }
    delete[] characters;
  }

  // test UpdateTuple
  for (auto it = row_values.begin(); it != row_values.end(); ) {
    Row getRow1(RowId(it->first));
    table_heap->GetTuple(&getRow1, nullptr);

    int32_t len = RandomUtils::RandomInt(0, 64);
    char *characters = new char[len];
    RandomUtils::RandomString(characters, len);

    Fields *fields = new Fields{
        Field(TypeId::kTypeInt, len),
        Field(TypeId::kTypeChar, characters, len, true),
        Field(TypeId::kTypeFloat, RandomUtils::RandomFloat(-999.f, 999.f))
    };
    Row UpdateRow(*fields);

    if (table_heap->UpdateTuple(UpdateRow, getRow1.GetRowId(), nullptr)) {
      Row getRow2(UpdateRow.GetRowId());
      table_heap->GetTuple(&getRow2, nullptr);

      ASSERT_FALSE(getRow1.CompareEqualTo(&getRow2));

      ASSERT_TRUE(getRow2.CompareEqualTo(&UpdateRow));

      // Clean up the old Fields object
      delete it->second;

      // Erase the old entry
      auto old_it = it;
      ++it;
      row_values.erase(old_it);

      // Insert the new entry
      row_values.emplace(UpdateRow.GetRowId().Get(), fields);
    } else {
      delete fields;
      ++it;
    }

    delete[] characters;
  }

  // test DeleteTuple
  for (auto &row_kv : row_values) {
    Row deleteRow(RowId(row_kv.first));
    ASSERT_TRUE(table_heap->MarkDelete(deleteRow.GetRowId(), nullptr));
    table_heap->ApplyDelete(deleteRow.GetRowId(), nullptr);
    ASSERT_FALSE(table_heap->GetTuple(&deleteRow, nullptr));

    if (row_kv.second != nullptr) {
      // Clean up the allocated Fields object
      delete row_kv.second;
      row_kv.second = nullptr; // Avoid double deletion
    } else {
      std::cerr << "Pointer is already null for RowId: " << row_kv.first << std::endl;
    }
  }
}