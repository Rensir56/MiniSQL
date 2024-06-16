#include "index/b_plus_tree.h"

#include <string>

#include "glog/logging.h"
#include "index/basic_comparator.h"
#include "index/generic_key.h"
#include "page/index_roots_page.h"

/**
 * TODO: Student Implement
 */
BPlusTree::BPlusTree(index_id_t index_id, BufferPoolManager *buffer_pool_manager, const KeyManager &KM,
                     int leaf_max_size, int internal_max_size)
    : index_id_(index_id),
      buffer_pool_manager_(buffer_pool_manager),
      processor_(KM),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size) {
}

void BPlusTree::Destroy(page_id_t current_page_id) {
    if (IsEmpty())
      return;
    auto page = buffer_pool_manager_->FetchPage(current_page_id);
    auto node = reinterpret_cast<BPlusTreePage *>(page->GetData());
    if (node->IsLeafPage()) {
      auto *leaf_node = reinterpret_cast<LeafPage *>(page->GetData());
      leaf_node->SetSize(0);
      buffer_pool_manager_->UnpinPage(current_page_id, true);
      buffer_pool_manager_->DeletePage(current_page_id);
      return;
    }
    auto *internal_node = reinterpret_cast<InternalPage *>(page->GetData());
    for (int i = 0; i < internal_node->GetSize(); ++i) {
      Destroy(internal_node->ValueAt(i));
    }
    internal_node->SetSize(0);
    buffer_pool_manager_->UnpinPage(current_page_id, true);
    buffer_pool_manager_->DeletePage(current_page_id);
    if (current_page_id == root_page_id_) {
      root_page_id_ = INVALID_PAGE_ID;
    }
}

/*
 * Helper function to decide whether current b+tree is empty
 */
bool BPlusTree::IsEmpty() const {
  // The B+ tree is empty if the root page ID is invalid
  return root_page_id_ == INVALID_PAGE_ID;
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */

 /**
 * SHIT
 **/
bool BPlusTree::GetValue(const GenericKey *key, std::vector<RowId> &result, Txn *transaction) {
  // Check the transaction state
  // if (transaction->GetState() == TxnState::kAborted) {
  //   throw TxnAbortException(transaction->GetTxnId(), AbortReason::kDeadlock);
  // }

  // Find the leaf page that contains the input key
  if (IsEmpty()) return false;
  Page *page = FindLeafPage(key,root_page_id_);
  auto *leaf_node = reinterpret_cast<LeafPage *>(page->GetData());

  // Lock the page for read
  page->RLatch();
  // transaction->GetSharedLockSet().insert(RowId(leaf_node->GetPageId()));

  RowId rid;
  // If the key is found in the leaf page
  if (leaf_node->Lookup(key, rid, processor_)) {
    // Add the value associated with the key to the result vector
    page->RUnlatch();
    // transaction->GetSharedLockSet().erase(RowId(leaf_node->GetPageId()));
    result.push_back(rid);
    buffer_pool_manager_->UnpinPage(leaf_node->GetPageId(), false);
    return true;
  } else {
    // If the key is not found in the leaf page
    page->RUnlatch();
    // transaction->GetSharedLockSet().erase(RowId(leaf_node->GetPageId()));
    buffer_pool_manager_->UnpinPage(leaf_node->GetPageId(), false);
    return false;
  }
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
bool BPlusTree::Insert(GenericKey *key, const RowId &value, Txn *transaction) {
  if (IsEmpty()) {
    StartNewTree(key, value);
    return true;
  }
  return InsertIntoLeaf(key, value, transaction);
}
/*
 * Insert constant key & value pair into an empty tree
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then update b+
 * tree's root page id and insert entry directly into leaf page.
 */
void BPlusTree::StartNewTree(GenericKey *key, const RowId &value) {
  // Caculate Size for new page
  leaf_max_size_ = (PAGE_SIZE - LEAF_PAGE_HEADER_SIZE) / (processor_.GetKeySize() + sizeof(RowId)) - 1;
  internal_max_size_ = (PAGE_SIZE - INTERNAL_PAGE_HEADER_SIZE) / (processor_.GetKeySize() + sizeof(RowId)) - 1;
  // Request a new page from the buffer pool manager
  Page *page = buffer_pool_manager_->NewPage(root_page_id_);
  // Check if the returned page is nullptr
  if (page == nullptr) {
    throw std::runtime_error("Out of memory");
  }
  // Cast the page data to a leaf page
  LeafPage *root = reinterpret_cast<LeafPage *>(page->GetData());
  // Initialize the root page
  root->Init(root_page_id_, INVALID_PAGE_ID, processor_.GetKeySize(), leaf_max_size_);
  // Insert the key-value pair into the root page
  root->Insert(key, value, processor_);
  // Unpin the root page
  UpdateRootPageId(1);
  buffer_pool_manager_->UnpinPage(root_page_id_, true);
}

/*
 * Insert constant key & value pair into leaf page
 * User needs to first find the right leaf page as insertion target, then look
 * through leaf page to see whether insert key exist or not. If exist, return
 * immediately, otherwise insert entry. Remember to deal with split if necessary.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */

 /**
 * SHIT
 **/
bool BPlusTree::InsertIntoLeaf(GenericKey *key, const RowId &value, Txn *transaction) {
  // Check the transaction state
  // if (transaction->GetState() == TxnState::kAborted) {
  //   throw TxnAbortException(transaction->GetTxnId(), AbortReason::kDeadlock);
  // }

  // Find the leaf page that should contain the input key
  Page *page = FindLeafPage(key, root_page_id_);
  auto *leaf_node = reinterpret_cast<LeafPage *>(page->GetData());

  // Lock the page for write
  page->WLatch();
  // transaction->GetExclusiveLockSet().insert(RowId(leaf_node->GetPageId()));

  // Check if the key already exists in the leaf page
  RowId rid;
  if (leaf_node->Lookup(key, rid, processor_)) {
    // If the key exists, unlock the page and return false
    page->WUnlatch();
    // transaction->GetExclusiveLockSet().erase(RowId(leaf_node->GetPageId()));
    buffer_pool_manager_->UnpinPage(leaf_node->GetPageId(), false);
    return false;
  }

  // Insert the key-value pair into the leaf page
  int new_size = leaf_node->Insert(key, value, processor_);

  // If the leaf page is full after the insertion, split it
  if (new_size >= leaf_max_size_) {
    LeafPage *new_leaf_node = Split(leaf_node, transaction);
    GenericKey *new_leaf_node_key =  new_leaf_node->KeyAt(0);
    InsertIntoParent(leaf_node, new_leaf_node_key, new_leaf_node, transaction);
    buffer_pool_manager_->UnpinPage(new_leaf_node->GetPageId(), true);
  } else {
    // Unlock the page and unpin it
    page->WUnlatch();
    // transaction->GetExclusiveLockSet().erase(RowId(leaf_node->GetPageId()));
    buffer_pool_manager_->UnpinPage(leaf_node->GetPageId(), true);
  }
  return true;
}

/*
 * Split input page and return newly created page.
 * Using template N to represent either internal page or leaf page.
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then move half
 * of key & value pairs from input page to newly created page
 */
BPlusTreeInternalPage *BPlusTree::Split(InternalPage *node, Txn *transaction) {
  // Check the transaction state
  // if (transaction->GetState() == TxnState::kAborted) {
  //   throw TxnAbortException(transaction->GetTxnId(), AbortReason::kDeadlock);
  // }

  // Request a new page from the buffer pool manager
  page_id_t new_page_id;
  auto new_page = buffer_pool_manager_->NewPage(new_page_id);
  // Check if the returned page is nullptr
  if (new_page == nullptr) {
    throw std::runtime_error("Out of memory");
  }
  // Cast the page data to an internal page
  auto *new_node = reinterpret_cast<InternalPage *>(new_page->GetData());
  // Initialize the new page
  new_node->Init(new_page->GetPageId(), node->GetParentPageId(), node->GetKeySize(), internal_max_size_);
  // Move half of the key-value pairs from the input page to the new page
  node->MoveHalfTo(new_node,buffer_pool_manager_);

  // Unpin the new page
  buffer_pool_manager_->UnpinPage(new_page_id,true);
  buffer_pool_manager_->UnpinPage(new_node->GetPageId(), true);
  return new_node;
}

BPlusTreeLeafPage *BPlusTree::Split(LeafPage *node, Txn *transaction) {
  // Check the transaction state
  // if (transaction->GetState() == TxnState::kAborted) {
  //   throw TxnAbortException(transaction->GetTxnId(), AbortReason::kDeadlock);
  // }

  // Request a new page from the buffer pool manager
  page_id_t new_page_id;
  auto new_page = buffer_pool_manager_->NewPage(new_page_id);
  // Check if the returned page is nullptr
  if (new_page == nullptr) {
    throw std::runtime_error("Out of memory");
  }
  // Cast the page data to a leaf page
  auto *new_node = reinterpret_cast<LeafPage *>(new_page->GetData());
  // Initialize the new page
  new_node->Init(new_page_id, node->GetParentPageId(), node->GetKeySize(), node->GetMaxSize());
  // Move half of the key-value pairs from the input page to the new page
  node->MoveHalfTo(new_node);

  // Add the new page to the transaction's write set
  // transaction->GetWriteSet().emplace_back(Operation::Type::INSERT, new_page_id, *new_node);
  new_node->SetParentPageId(node->GetParentPageId());
  new_node->SetNextPageId(node->GetNextPageId());
  node->SetNextPageId(new_node->GetPageId());
  // Unpin the new page
  buffer_pool_manager_->UnpinPage(new_page_id, true);
  buffer_pool_manager_->UnpinPage(new_node->GetPageId(), true);
  return new_node;
}

/*
 * Insert key & value pair into internal page after split
 * @param   old_node      input page from split() method
 * @param   key
 * @param   new_node      returned page from split() method
 * User needs to first find the parent page of old_node, parent node must be
 * adjusted to take info of new_node into account. Remember to deal with split
 * recursively if necessary.
 */
void BPlusTree::InsertIntoParent(BPlusTreePage *old_node, GenericKey *key, BPlusTreePage *new_node, Txn *transaction) {
  // Check the transaction state
  // if (transaction->GetState() == TxnState::kAborted) {
  //   throw TxnAbortException(transaction->GetTxnId(), AbortReason::kDeadlock);
  // }

  // If the old node is the root
  if (old_node->IsRootPage()) {
    // Create a new root
    Page *page = buffer_pool_manager_->NewPage(root_page_id_);
    if (page == nullptr) {
      throw std::runtime_error("Out of memory");
    }
    auto *root = reinterpret_cast<InternalPage *>(page->GetData());
    root->Init(root_page_id_, INVALID_PAGE_ID, old_node->GetKeySize(), internal_max_size_);
    root->PopulateNewRoot(old_node->GetPageId(), key, new_node->GetPageId());
    old_node->SetParentPageId(root->GetPageId());
    new_node->SetParentPageId(root->GetPageId());
    UpdateRootPageId(0);
    buffer_pool_manager_->UnpinPage(root->GetPageId(), true);
  } else {
    // If the old node is not the root
    page_id_t parent_page_id = old_node->GetParentPageId();
    Page *page = buffer_pool_manager_->FetchPage(parent_page_id);
    if (page == nullptr) {
      throw std::runtime_error("Out of memory");
    }
    auto *parent = reinterpret_cast<InternalPage *>(page->GetData());

    // Insert the new key-value pair into the parent
    int new_size = parent->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());
    new_node->SetParentPageId(parent_page_id);

    // If the parent is full after the insertion, split it
    if (new_size >= parent->GetMaxSize()) {
      InternalPage *new_internal = Split(parent, transaction);
      for (int i = 0; i < parent->GetSize(); ++i) {
        auto child = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(parent->ValueAt(i))->GetData());
        child->SetParentPageId(parent->GetPageId());
        buffer_pool_manager_->UnpinPage(child->GetPageId(), true);
      }
      for (int i = 0; i < new_internal->GetSize(); ++i) {
        auto child = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(new_internal->ValueAt(i))->GetData());
        child->SetParentPageId(new_internal->GetPageId());
        buffer_pool_manager_->UnpinPage(child->GetPageId(), true);
      }
      InsertIntoParent(parent, new_internal->KeyAt(0), new_internal, transaction);
      page->WUnlatch();
      buffer_pool_manager_->UnpinPage(new_internal->GetPageId(), true);
    } else {
      old_node->SetParentPageId(parent->GetPageId());
      new_node->SetParentPageId(parent->GetPageId());
      page->WUnlatch();
    }
    buffer_pool_manager_->UnpinPage(parent_page_id, true);
  }
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immediately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
void BPlusTree::Remove(const GenericKey *key, Txn *transaction) {}

/* todo
 * User needs to first find the sibling of input page. If sibling's size + input
 * page's size > page's max size, then redistribute. Otherwise, merge.
 * Using template N to represent either internal page or leaf page.
 * @return: true means target leaf page should be deleted, false means no
 * deletion happens
 */
template <typename N>
bool BPlusTree::CoalesceOrRedistribute(N *&node, Txn *transaction) {
  return false;
}

/*
 * Move all the key & value pairs from one page to its sibling page, and notify
 * buffer pool manager to delete this page. Parent page must be adjusted to
 * take info of deletion into account. Remember to deal with coalesce or
 * redistribute recursively if necessary.
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 * @param   parent             parent page of input "node"
 * @return  true means parent node should be deleted, false means no deletion happened
 */
bool BPlusTree::Coalesce(LeafPage *&neighbor_node, LeafPage *&node, InternalPage *&parent, int index,
                         Txn *transaction) {
  return false;
}

bool BPlusTree::Coalesce(InternalPage *&neighbor_node, InternalPage *&node, InternalPage *&parent, int index,
                         Txn *transaction) {
  return false;
}

/*
 * Redistribute key & value pairs from one page to its sibling page. If index ==
 * 0, move sibling page's first key & value pair into end of input "node",
 * otherwise move sibling page's last key & value pair into head of input
 * "node".
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 */
void BPlusTree::Redistribute(LeafPage *neighbor_node, LeafPage *node, int index) {
}
void BPlusTree::Redistribute(InternalPage *neighbor_node, InternalPage *node, int index) {
}
/*
 * Update root page if necessary
 * NOTE: size of root page can be less than min size and this method is only
 * called within coalesceOrRedistribute() method
 * case 1: when you delete the last element in root page, but root page still
 * has one last child
 * case 2: when you delete the last element in whole b+ tree
 * @return : true means root page should be deleted, false means no deletion
 * happened
 */
bool BPlusTree::AdjustRoot(BPlusTreePage *old_root_node) {
  return false;
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the left most leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
IndexIterator BPlusTree::Begin() {
  return IndexIterator();
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
IndexIterator BPlusTree::Begin(const GenericKey *key) {
   return IndexIterator();
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
IndexIterator BPlusTree::End() {
  return IndexIterator();
}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Find leaf page containing particular key, if leftMost flag == true, find
 * the left most leaf page
 * Note: the leaf page is pinned, you need to unpin it after use.
 */
Page *BPlusTree::FindLeafPage(const GenericKey *key, page_id_t page_id, bool leftMost) {
  // Fetch the page from the buffer pool manager
  Page *page = buffer_pool_manager_->FetchPage(page_id);
  if (page == nullptr) {
    throw std::runtime_error("Out of memory");
  }
  BPlusTreePage *node = reinterpret_cast<BPlusTreePage *>(page->GetData());
  // If the node is a leaf page, return it
  if (node->IsLeafPage()) {
    return page;
  }
  // If the node is an internal page
  InternalPage *internal = reinterpret_cast<InternalPage *>(node);
  page_id_t child_page_id;
  // If leftMost is true, get the first child
  if (leftMost) {
    child_page_id = internal->ValueAt(0);
  } else {
    // Otherwise, find the appropriate child to descend into
    child_page_id = internal->Lookup(key, processor_);
  }
  page->RUnlatch();
  // Unpin the current page
  buffer_pool_manager_->UnpinPage(page_id, false);
  // Recursively find the leaf page
  return FindLeafPage(key, child_page_id, leftMost);
}

/*
 * Update/Insert root page id in header page(where page_id = INDEX_ROOTS_PAGE_ID,
 * header_page isdefined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      default value is false. When set to true,
 * insert a record <index_name, current_page_id> into header page instead of
 * updating it.
 */
void BPlusTree::UpdateRootPageId(int insert_record) {
  // Fetch the header page from the buffer pool manager
  Page *page = buffer_pool_manager_->FetchPage(INDEX_ROOTS_PAGE_ID);
  // Check if the returned page is nullptr
  if (page == nullptr) {
    throw std::runtime_error("Out of memory");
  }
  // Cast the page data to a header page
  auto *header = reinterpret_cast<IndexRootsPage*>(page->GetData());
  // If insert_record is true, insert a record into the header page
  if (insert_record) {
    header->Insert(index_id_, root_page_id_);
  } else {
    // Otherwise, update the current page ID in the header page
    header->Update(index_id_, root_page_id_);
  }
  // Unpin the header page
  buffer_pool_manager_->UnpinPage(INDEX_ROOTS_PAGE_ID, true);
}

/**
 * This method is used for debug only, You don't need to modify
 */
void BPlusTree::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out, Schema *schema) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId()
        << ",Parent=" << leaf->GetParentPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      Row ans;
      processor_.DeserializeToKey(leaf->KeyAt(i), ans, schema);
      out << "<TD>" << ans.GetField(0)->toString() << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    auto *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId()
        << ",Parent=" << inner->GetParentPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        Row ans;
        processor_.DeserializeToKey(inner->KeyAt(i), ans, schema);
        out << ans.GetField(0)->toString();
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out, schema);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 */
void BPlusTree::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    auto *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
      bpm->UnpinPage(internal->ValueAt(i), false);
    }
  }
}

bool BPlusTree::Check() {
  bool all_unpinned = buffer_pool_manager_->CheckAllUnpinned();
  if (!all_unpinned) {
    LOG(ERROR) << "problem in page unpin" << endl;
  }
  return all_unpinned;
}