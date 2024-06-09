#include "storage/table_heap.h"

/**
 * TODO: Student Implement
 */
bool TableHeap::InsertTuple(Row &row, Txn *txn) {

  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(first_page_id_));
  if (page == nullptr) {
    page_id_t page_id;
    page = reinterpret_cast<TablePage *>(buffer_pool_manager_->NewPage(page_id));
    this->first_page_id_ = page_id;
  }
  bool good_insert = page->InsertTuple(row, schema_, txn, lock_manager_, log_manager_);

  while (!good_insert) {
    page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(page->GetNextPageId()));
    good_insert = page->InsertTuple(row, schema_, txn, lock_manager_, log_manager_);
    if (page->GetNextPageId() == INVALID_PAGE_ID && !good_insert) {
      page_id_t new_page_id;
      page = reinterpret_cast<TablePage *>(buffer_pool_manager_->NewPage(new_page_id));
    }
  }

  return true;
}

bool TableHeap::MarkDelete(const RowId &rid, Txn *txn) {
  // Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  // If the page could not be found, then abort the recovery.
  if (page == nullptr) {
    return false;
  }
  // Otherwise, mark the tuple as deleted.
  page->WLatch();
  page->MarkDelete(rid, txn, lock_manager_, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
  return true;
}

/**
 * TODO: Student Implement
 */
bool TableHeap::UpdateTuple(Row &row, const RowId &rid, Txn *txn) {
  Row old_row(row);
  page_id_t page_id = rid.GetPageId();
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(page_id));
  if (page == nullptr) {
    return false;
  }
  if (page->UpdateTuple(row, &old_row, schema_, txn, lock_manager_, log_manager_))
    return true;
  else
    return false;
}

/**
 * TODO: Student Implement
 */
void TableHeap::ApplyDelete(const RowId &rid, Txn *txn) {
  // Step1: Find the page which contains the tuple.
  // Step2: Delete the tuple from the page.
  page_id_t page_id = rid.GetPageId();
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(page_id));
  if (page == nullptr) {
    return;
  }
  page->ApplyDelete(rid, txn, log_manager_);
}

void TableHeap::RollbackDelete(const RowId &rid, Txn *txn) {
  // Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  assert(page != nullptr);
  // Rollback to delete.
  page->WLatch();
  page->RollbackDelete(rid, txn, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
}

/**
 * TODO: Student Implement
 */
bool TableHeap::GetTuple(Row *row, Txn *txn) {
  RowId rid = row->GetRowId();
  page_id_t page_id = rid.GetPageId();
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(page_id));

  if (page == nullptr) {
    return false;
  }

  if (page->GetTuple(row, schema_, txn, lock_manager_))
    return true;
  else
    return false;
}

void TableHeap::DeleteTable(page_id_t page_id) {
  if (page_id != INVALID_PAGE_ID) {
    auto temp_table_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(page_id));  // 删除table_heap
    if (temp_table_page->GetNextPageId() != INVALID_PAGE_ID)
      DeleteTable(temp_table_page->GetNextPageId());
    buffer_pool_manager_->UnpinPage(page_id, false);
    buffer_pool_manager_->DeletePage(page_id);
  } else {
    DeleteTable(first_page_id_);
  }
}

/**
 * TODO: Student Implement
 */
TableIterator TableHeap::Begin(Txn *txn) {
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(first_page_id_));
  if (page == nullptr)
    return TableIterator(nullptr, RowId(), nullptr);

  RowId rowId;
  while (!page->GetFirstTupleRid(&rowId) && page->GetNextPageId() != INVALID_PAGE_ID) {
    page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(page->GetNextPageId()));
  }
  if (page->GetFirstTupleRid(&rowId))
    return TableIterator(this, rowId, txn);

  return TableIterator(nullptr, RowId(), nullptr);
}

/**
 * TODO: Student Implement
 */
TableIterator TableHeap::End() {
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(first_page_id_));
  if (page == nullptr)
    return TableIterator(nullptr, RowId(), nullptr);

  while (page->GetNextPageId() != INVALID_PAGE_ID) {
    page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(page->GetNextPageId()));
  }

  RowId rowId;
  while (!page->GetFirstTupleRid(&rowId) && page != reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(first_page_id_))) {
    page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(page->GetPrevPageId()));
  }
  if (!page->GetFirstTupleRid(&rowId) && page == reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(first_page_id_)))
    return TableIterator(nullptr, RowId(), nullptr);

  // rowId got
  RowId cur_rid(rowId);
  RowId next_rid;
  while (1) {
    if (!page->GetNextTupleRid(cur_rid, &next_rid))
      break;
    cur_rid = next_rid;
  }

  return TableIterator(this, cur_rid, nullptr);   // why no txn?
}
