#include "storage/table_iterator.h"

#include "common/macros.h"
#include "storage/table_heap.h"

/**
 * TODO: Student Implement
 */
TableIterator::TableIterator(TableHeap *table_heap, RowId rid, Txn *txn) {
  this->tableHeap = table_heap;
  this->rid = rid;
  this->txn = txn;

}

TableIterator::TableIterator(const TableIterator &other) {
  this->tableHeap = other.tableHeap;
  this->rid = other.rid;
  this->txn = other.txn;
  row = Row();
}

TableIterator::~TableIterator() {
}

bool TableIterator::operator==(const TableIterator &itr) const {
  if (this->tableHeap != itr.tableHeap) {
    return false;
  }
  if (this->rid.GetPageId() != itr.rid.GetPageId() || this->rid.GetSlotNum() != itr.rid.GetSlotNum()) {
    return false;
  }
  if (this->txn != itr.txn) {
    return false;
  }
  return true;
}

bool TableIterator::operator!=(const TableIterator &itr) const {
  return (!(*this == itr));
}

const Row &TableIterator::operator*() {
  row = Row();
  row.SetRowId(this->rid);
  this->tableHeap->GetTuple(&row, this->txn);

  return row;
}

Row *TableIterator::operator->() {
  row = Row();
  row.SetRowId(this->rid);
  this->tableHeap->GetTuple(&row, this->txn);

  return &row;
}

TableIterator &TableIterator::operator=(const TableIterator &itr) noexcept = default;

// ++iter
TableIterator &TableIterator::operator++() {
  page_id_t page_id = rid.GetPageId();
  auto page = reinterpret_cast<TablePage *>(tableHeap->buffer_pool_manager_->FetchPage(page_id));
  RowId next_rid;
  if (page->GetNextTupleRid(this->rid, &next_rid)) {
    this->rid = next_rid;
    tableHeap->buffer_pool_manager_->UnpinPage(page_id, false);
    return *this;
  }

  while (page->GetNextPageId() != INVALID_PAGE_ID) {
    page = reinterpret_cast<TablePage *>(tableHeap->buffer_pool_manager_->FetchPage(page->GetNextPageId()));
    if (page->GetFirstTupleRid(&next_rid)) {
      this->rid = next_rid;
      tableHeap->buffer_pool_manager_->UnpinPage(page->GetTablePageId(), false);
      return *this;
    }
  }
  tableHeap->buffer_pool_manager_->UnpinPage(page->GetTablePageId(), false);
  rid = RowId();
  txn = nullptr;
  return *this;
}

// iter++
TableIterator TableIterator::operator++(int) {
  page_id_t page_id = rid.GetPageId();
  RowId old_rid = rid;
  auto page = reinterpret_cast<TablePage *>(tableHeap->buffer_pool_manager_->FetchPage(page_id));
  RowId next_rid;
  if (page->GetNextTupleRid(this->rid, &next_rid)) {
    this->rid = next_rid;
    tableHeap->buffer_pool_manager_->UnpinPage(page_id, false);
    return TableIterator(tableHeap, old_rid, txn);
  }

  while (page->GetNextPageId() != INVALID_PAGE_ID) {
    page = reinterpret_cast<TablePage *>(tableHeap->buffer_pool_manager_->FetchPage(page->GetNextPageId()));
    if (page->GetFirstTupleRid(&next_rid)) {
      this->rid = next_rid;
      tableHeap->buffer_pool_manager_->UnpinPage(page->GetTablePageId(), false);
      return TableIterator(tableHeap, old_rid, txn);
    }
  }
  this->rid = RowId();
  this->txn = nullptr;
  return TableIterator(tableHeap, RowId(), nullptr);
}
