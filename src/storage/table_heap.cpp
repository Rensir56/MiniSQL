#include "storage/table_heap.h"

/**
 * TODO: Student Implement
 */
bool TableHeap::InsertTuple(Row &row, Txn *txn) {

  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(first_page_id_));
  page->WLatch();
  bool good_insert = page->InsertTuple(row, schema_, txn, lock_manager_, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
  while (!good_insert) {
    page_id_t next_id = page->GetNextPageId();
    if (next_id == INVALID_PAGE_ID) {
      page_id_t now_id = page->GetTablePageId();
      page_id_t new_id;
      TablePage *old_page = page;
      page = reinterpret_cast<TablePage *>(buffer_pool_manager_->NewPage(new_id));
      old_page->SetNextPageId(new_id);
      page->Init(new_id, now_id, log_manager_, txn);
      page->WLatch();
      good_insert = page->InsertTuple(row, schema_, txn, lock_manager_, log_manager_);
      page->WUnlatch();
      buffer_pool_manager_->UnpinPage(now_id, true);
    } else {
      page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(next_id));
      page->WLatch();
      good_insert = page->InsertTuple(row, schema_, txn, lock_manager_, log_manager_);
      page->WUnlatch();
      buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
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
  Row old_row(rid);
  page_id_t page_id = rid.GetPageId();
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(page_id));
  if (page == nullptr) {
    return false;
  }
  page->WLatch();
  TABLE_PAGE_UPDATE update = page->UpdateTuple(row, &old_row, schema_, txn, lock_manager_, log_manager_);
  if (update == TABLE_PAGE_UPDATE::TABLE_PAGE_UPDATE_SUCCESS) {
    row.SetRowId(rid);
    page->WUnlatch();
    buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
    return true;
  }
  else if (update == TABLE_PAGE_UPDATE::TABLE_PAGE_UPDATE_NEW_PAGE) {
    page->WUnlatch();
    buffer_pool_manager_->UnpinPage(page->GetTablePageId(), false);
    bool flag1;
    bool flag2;
    flag1 = MarkDelete(rid, txn);
    ApplyDelete(rid, txn);
    flag2 = InsertTuple(row, txn);
    return flag1 & flag2;
  } else {
    buffer_pool_manager_->UnpinPage(page->GetTablePageId(), false);
    return false;
  }

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
  page->WLatch();
  page->ApplyDelete(rid, txn, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
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

  if (page->GetTuple(row, schema_, txn, lock_manager_)) {
    row->SetRowId(rid);
    buffer_pool_manager_->UnpinPage(page_id, false);
    return true;
  }
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
    buffer_pool_manager_->UnpinPage(page->GetTablePageId(), false);
    page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(page->GetNextPageId()));
  }
  if (page->GetFirstTupleRid(&rowId)) {
    buffer_pool_manager_->UnpinPage(page->GetTablePageId(), false);
    return TableIterator(this, rowId, txn);
  }
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
    buffer_pool_manager_->UnpinPage(page->GetTablePageId(), false);
    page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(page->GetNextPageId()));
  }

  RowId rowId;
  while (!page->GetFirstTupleRid(&rowId) && page->GetTablePageId() != first_page_id_) {
    buffer_pool_manager_->UnpinPage(page->GetTablePageId(), false);
    page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(page->GetPrevPageId()));
  }
  if (!page->GetFirstTupleRid(&rowId) && page->GetTablePageId() != first_page_id_) {
    buffer_pool_manager_->UnpinPage(page->GetTablePageId(), false);
    return TableIterator(nullptr, RowId(), nullptr);
  }

  // rowId got
  RowId cur_rid(rowId);
  RowId next_rid;
  while (1) {
    if (!page->GetNextTupleRid(cur_rid, &next_rid))
      break;
    cur_rid = next_rid;
  }
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), false);
  return TableIterator(this, cur_rid, nullptr);   // why no txn?
}
