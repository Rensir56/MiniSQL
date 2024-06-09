#include "page/b_plus_tree_internal_page.h"

#include "index/generic_key.h"

#define pairs_off (data_)
#define pair_size (GetKeySize() + sizeof(page_id_t))
#define key_off 0
#define val_off GetKeySize()

/**
 * TODO: Student Implement
 */
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 */
void InternalPage::Init(page_id_t page_id, page_id_t parent_id, int key_size, int max_size) {
  SetPageType(IndexPageType::INTERNAL_PAGE);
  SetSize(0);
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetMaxSize(max_size);
  SetKeySize(key_size);
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
GenericKey *InternalPage::KeyAt(int index) {
  return reinterpret_cast<GenericKey *>(pairs_off + index * pair_size + key_off);
}

void InternalPage::SetKeyAt(int index, GenericKey *key) {
  memcpy(pairs_off + index * pair_size + key_off, key, GetKeySize());
}

page_id_t InternalPage::ValueAt(int index) const {
  return *reinterpret_cast<const page_id_t *>(pairs_off + index * pair_size + val_off);
}

void InternalPage::SetValueAt(int index, page_id_t value) {
  *reinterpret_cast<page_id_t *>(pairs_off + index * pair_size + val_off) = value;
}

int InternalPage::ValueIndex(const page_id_t &value) const {
  for (int i = 0; i < GetSize(); ++i) {
    if (ValueAt(i) == value)
      return i;
  }
  return -1;
}

void *InternalPage::PairPtrAt(int index) {
  return KeyAt(index);
}

void InternalPage::PairCopy(void *dest, void *src, int pair_num) {
  memcpy(dest, src, pair_num * (GetKeySize() + sizeof(page_id_t)));
}
/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * Find and return the child pointer(page_id) which points to the child page
 * that contains input "key"
 * Start the search from the second key(the first key should always be invalid)
 * 用了二分查找
 */
page_id_t InternalPage::Lookup(const GenericKey *key, const KeyManager &KM) {
  int left = 1;  // Start from the second key
  int right = GetSize() - 1;  // GetSize() returns the number of keys

  while (left <= right) {
    int mid = left + (right - left) / 2;
    GenericKey *mid_key = KeyAt(mid);

    int cmp = KM.CompareKeys(mid_key, key);
    if (cmp == 0) {
      // Key is found, return the corresponding page_id
      return ValueAt(mid);
    } else if (cmp < 0) {
      left = mid + 1;
    } else {
      right = mid - 1;
    }
  }
  // Key is not found, return the page_id of the child that could contain the key
  return ValueAt(right);
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Populate new root page with old_value + new_key & new_value
 * When the insertion cause overflow from leaf page all the way upto the root
 * page, you should create a new root page and populate its elements.
 * NOTE: This method is only called within InsertIntoParent()(b_plus_tree.cpp)
 */
void InternalPage::PopulateNewRoot(const page_id_t &old_value, GenericKey *new_key, const page_id_t &new_value) {
  // Set the size to 2 as it will contain two child nodes
  SetSize(2);
  // Set the first value to old_value, the first key to new_key, and the second value to new_value
  SetValueAt(0, old_value);
  SetKeyAt(0, new_key);
  SetValueAt(1, new_value);
}

/*
 * Insert new_key & new_value pair right after the pair with its value ==
 * old_value
 * @return:  new size after insertion
 */
int InternalPage::InsertNodeAfter(const page_id_t &old_value, GenericKey *new_key, const page_id_t &new_value) {
  // Find the index of the old value
  int index = ValueIndex(old_value);
  if (index == -1) {
    // old_value not found, return current size
    return GetSize();
  }
  // Increase the size of the page
  IncreaseSize(1);
  // Shift all pairs after the found index one position to the right
  PairCopy(PairPtrAt(index + 1), PairPtrAt(index), (GetSize() - index));
  // Insert the new key-value pair at the correct position
  SetKeyAt(index + 1, new_key);
  SetValueAt(index + 1, new_value);
  // Return the new size
  return GetSize();
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 * buffer_pool_manager 是干嘛的？传给CopyNFrom()用于Fetch数据页
 */
void InternalPage::MoveHalfTo(InternalPage *recipient, BufferPoolManager *buffer_pool_manager) {
  int half = GetMinSize();
  recipient->CopyNFrom(PairPtrAt(half), GetMaxSize() - half, buffer_pool_manager);
  SetSize(half);
}

/* Copy entries into me, starting from {items} and copy {size} entries.
 * Since it is an internal page, for all entries (pages) moved, their parents page now changes to me.
 * So I need to 'adopt' them by changing their parent page id, which needs to be persisted with BufferPoolManger
 *
 */
void InternalPage::CopyNFrom(void *src, int size, BufferPoolManager *buffer_pool_manager) {
  // Calculate the starting position for copying in the current page
  int start_position = GetSize();
  // Copy the entries from the source to the current page
  PairCopy(PairPtrAt(start_position), src, size);
  // Update the size of the current page
  IncreaseSize(size);
  // Update the parent page id of the copied entries and persist the changes
  for (int i = start_position; i < GetSize(); i++) {
    page_id_t child_page_id = ValueAt(i);
    Page *child_page = buffer_pool_manager->FetchPage(child_page_id);
    if (child_page == nullptr) {
      throw std::runtime_error("Fail to fetch page");
    }
    BPlusTreePage *child_tree_page = reinterpret_cast<BPlusTreePage *>(child_page->GetData());
    child_tree_page->SetParentPageId(GetPageId());
    buffer_pool_manager->UnpinPage(child_page_id, true);
  }
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Remove the key & value pair in internal page according to input index(a.k.a
 * array offset)
 * NOTE: store key&value pair continuously after deletion
 */
void InternalPage::Remove(int index) {
  // Check if the index is valid
  if (index < 0 || index >= GetSize()) {
    throw std::out_of_range("Index out of range");
  }
  // Move all pairs after the index one position to the left
  PairCopy(PairPtrAt(index), PairPtrAt(index + 1), (GetSize() - index - 1));
  // Decrease the size of the page
  IncreaseSize(- 1);
}

/*
 * Remove the only key & value pair in internal page and return the value
 * NOTE: only call this method within AdjustRoot()(in b_plus_tree.cpp)
 */
page_id_t InternalPage::RemoveAndReturnOnlyChild() {
  // Check if the size is 1
  if (GetSize() != 1) {
    throw std::logic_error("This method should only be called when there is only one key-value pair in the internal page.");
  }
  // Get and save the value of the only key-value pair
  page_id_t value = ValueAt(0);
  // Set the size to 0
  SetSize(0);
  // Return the saved value
  return value;
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all of key & value pairs from this page to "recipient" page.
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the invariant.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient
 */
void InternalPage::MoveAllTo(InternalPage *recipient, GenericKey *middle_key, BufferPoolManager *buffer_pool_manager) {
  SetKeyAt(0, middle_key);
  recipient->CopyNFrom(PairPtrAt(0), GetSize(), buffer_pool_manager);
  SetSize(0);
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to tail of "recipient" page.
 *
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the invariant.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient
 */
void InternalPage::MoveFirstToEndOf(InternalPage *recipient, GenericKey *middle_key, BufferPoolManager *buffer_pool_manager) {
  recipient->CopyLastFrom(middle_key, ValueAt(0), buffer_pool_manager);
  Remove(0);
}

/* Append an entry at the end.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
void InternalPage::CopyLastFrom(GenericKey *key, const page_id_t value, BufferPoolManager *buffer_pool_manager) {
  // Insert the middle key at the end of the recipient page
  SetKeyAt(GetSize(), key);
  // Insert the value at the end of the recipient page
  SetValueAt(GetSize(),value);
  // Update the size of the recipient page
  IncreaseSize(1);
  // Update the parent page id of the moved page and persist the change
  Page *child_page = buffer_pool_manager->FetchPage(value);
  if (child_page == nullptr) {
    throw std::runtime_error("Fail to fetch page");
  }
  BPlusTreePage *child_tree_page = reinterpret_cast<BPlusTreePage *>(child_page->GetData());
  child_tree_page->SetParentPageId(GetPageId());
  buffer_pool_manager->UnpinPage(value, true);
}

/*
 * Remove the last key & value pair from this page to head of "recipient" page.
 * You need to handle the original dummy key properly, e.g. updating recipient’s array to position the middle_key at the
 * right place.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those pages that are
 * moved to the recipient
 */
void InternalPage::MoveLastToFrontOf(InternalPage *recipient, GenericKey *middle_key, BufferPoolManager *buffer_pool_manager) {
  recipient->SetKeyAt(0, middle_key);
  recipient->CopyFirstFrom(ValueAt(GetSize() - 1), buffer_pool_manager);
  recipient->SetKeyAt(0, KeyAt(GetSize() - 1));
  Remove(GetSize() - 1);
}

/* Append an entry at the beginning.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
void InternalPage::CopyFirstFrom(const page_id_t value, BufferPoolManager *buffer_pool_manager) {
  // Move all pairs in the current page one position to the right
  PairCopy(PairPtrAt(1), PairPtrAt(0), GetSize());
  // Insert the value at the beginning of the current page
  SetValueAt(0, value);
  // Update the size of the current page
  IncreaseSize(1);
  // Update the parent page id of the moved page and persist the change
  Page *child_page = buffer_pool_manager->FetchPage(value);
  if (child_page == nullptr) {
    throw std::runtime_error("Fail to fetch page");
  }
  BPlusTreePage *child_tree_page = reinterpret_cast<BPlusTreePage *>(child_page->GetData());
  child_tree_page->SetParentPageId(GetPageId());
  buffer_pool_manager->UnpinPage(child_page->GetPageId(), true);
}
