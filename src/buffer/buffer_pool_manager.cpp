#include "buffer/buffer_pool_manager.h"

#include "glog/logging.h"
#include "page/bitmap_page.h"

static const char EMPTY_PAGE_DATA[PAGE_SIZE] = {0};

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager) {
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size_);
  for (size_t i = 0; i < pool_size_; i++) {
    free_list_.emplace_back(i);
  }
}

BufferPoolManager::~BufferPoolManager() {
  for (auto page : page_table_) {
    FlushPage(page.first);
  }
  delete[] pages_;
  delete replacer_;
}

/**
 * TODO: Student Implement
 */
Page *BufferPoolManager::FetchPage(page_id_t page_id) {
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  // 2.     If R is dirty, write it back to the disk.
  // 3.     Delete R from the page table and insert P.
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
  frame_id_t new_frame_id;
  if (page_id == INVALID_PAGE_ID) {
    return nullptr;
  }
  //不存在请求页面
  if (page_table_.find(page_id) == page_table_.end()) {
    if((free_list_.size() == 0)&&(replacer_->Size() == 0)) {
      return nullptr;
    }
    if(free_list_.size() != 0) {
      new_frame_id = free_list_.front();
      free_list_.pop_front();
    }
    if(pages_[new_frame_id].IsDirty()) {
      disk_manager_->WritePage(pages_[new_frame_id].GetPageId(), pages_[new_frame_id].GetData());
      pages_[new_frame_id].is_dirty_ = false;
    }
    page_table_.erase(pages_[new_frame_id].GetPageId());
    page_table_.emplace(page_id, new_frame_id);
    disk_manager_->ReadPage(page_id, pages_[new_frame_id].GetData());
    pages_[new_frame_id].page_id_ = page_id;
    pages_[new_frame_id].pin_count_ = 1;
    pages_[new_frame_id].is_dirty_ = false;
    return &pages_[new_frame_id];
  }
  auto iter = page_table_.find(page_id);
  Page *page = &pages_[iter->second];
  replacer_->Pin(iter->second);
  page->pin_count_++;
  return page;
}

/**
 * TODO: Student Implement
 */
Page *BufferPoolManager::NewPage(page_id_t &page_id) {
  // 0.   Make sure you call AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.
  int a = 1;
  frame_id_t new_frame_id;
  if((free_list_.size() == 0)&&(replacer_->Size() == 0)) {
    return nullptr;
  }
  if(free_list_.size() != 0) {
    new_frame_id = free_list_.front();
    free_list_.pop_front();
  }
  else {
    if(!replacer_->Victim(&new_frame_id)){}
    a = 0;
  }
  Page *page = &pages_[new_frame_id];
  if((page->is_dirty_)&&(a == 0)) {
    disk_manager_->WritePage(page->GetPageId(), page->GetData());
    page->is_dirty_ = false;
  }
  if(a == 0) {
    page_table_.erase(page->GetPageId());
  }
  page->pin_count_ = 1;
  page->ResetMemory();
  page_id = AllocatePage();
  page->page_id_ = page_id;
  page_table_.emplace(page_id, new_frame_id);
  return page;
}

/**
 * TODO: Student Implement
 */
bool BufferPoolManager::DeletePage(page_id_t page_id) {
  // 0.   Make sure you call DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  if(page_id == INVALID_PAGE_ID) {
    return true;
  }
  if(page_table_.find(page_id) == page_table_.end()) {
    return true;
  }
  auto iter = page_table_.find(page_id);
  if(pages_[iter->second].GetPinCount() != 0) {
    return false;
  }
  if(pages_[iter->second].IsDirty()) {
    disk_manager_->WritePage(pages_[iter->second].GetPageId(), pages_[iter->second].GetData());
    pages_[iter->second].is_dirty_ = false;
  }
  DeallocatePage(page_id);
  pages_[iter->second].page_id_ = INVALID_PAGE_ID;
  pages_[iter->second].ResetMemory();
  page_table_.erase(page_id);
  free_list_.push_back(iter->second);
  return true;
}

/**
 * TODO: Student Implement
 */
bool BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty) {
  if(page_id == INVALID_PAGE_ID) {
    return false;
  }
  if(page_table_.find(page_id) == page_table_.end()) {
    return false;
  }
  auto iter = page_table_.find(page_id);
  if(pages_[iter->second].GetPinCount() == 0) {
    return false;
  }
  pages_[iter->second].pin_count_--;
  if(pages_[iter->second].GetPinCount() == 0) {
    replacer_->Unpin(iter->second);
  }
  if(is_dirty) {
    pages_[iter->second].is_dirty_ = true;
  }
  return true;
}

/**
 * TODO: Student Implement
 */
bool BufferPoolManager::FlushPage(page_id_t page_id) {
  if(page_id == INVALID_PAGE_ID) {
    return false;
  }
  if(page_table_.find(page_id) == page_table_.end()) {
    return false;
  }
  auto iter = page_table_.find(page_id);
  disk_manager_->WritePage(page_id, pages_[iter->second].GetData());
  pages_[iter->second].is_dirty_ = false;
  return true;
}

page_id_t BufferPoolManager::AllocatePage() {
  int next_page_id = disk_manager_->AllocatePage();
  return next_page_id;
}

void BufferPoolManager::DeallocatePage(__attribute__((unused)) page_id_t page_id) {
  disk_manager_->DeAllocatePage(page_id);
}

bool BufferPoolManager::IsPageFree(page_id_t page_id) {
  return disk_manager_->IsPageFree(page_id);
}

// Only used for debug
bool BufferPoolManager::CheckAllUnpinned() {
  bool res = true;
  for (size_t i = 0; i < pool_size_; i++) {
    if (pages_[i].pin_count_ != 0) {
      res = false;
      LOG(ERROR) << "page " << pages_[i].page_id_ << " pin count:" << pages_[i].pin_count_ << endl;
    }
  }
  return res;
}