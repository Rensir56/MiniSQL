#include "concurrency/lock_manager.h"

#include <iostream>

#include "common/rowid.h"
#include "concurrency/txn.h"
#include "concurrency/txn_manager.h"

void LockManager::SetTxnMgr(TxnManager *txn_mgr) { txn_mgr_ = txn_mgr; }

/**
 * TODO: Student Implement
 */
bool LockManager::LockShared(Txn *txn, const RowId &rid) {
  std::unique_lock<std::mutex> lock(latch_);

  LockPrepare(txn, rid);

  CheckAbort(txn, lock_table_[rid]);

  lock_table_[rid].EmplaceLockRequest(txn->GetTxnId(), LockMode::kShared);
  txn->GetSharedLockSet().insert(rid);
  while (lock_table_[rid].is_writing_ || lock_table_[rid].is_upgrading_) {
    lock_table_[rid].cv_.wait(lock);
    CheckAbort(txn, lock_table_[rid]);
  }

  lock_table_[rid].sharing_cnt_++;

  return true;
}

/**
 * TODO: Student Implement
 */
bool LockManager::LockExclusive(Txn *txn, const RowId &rid) {
    std::unique_lock<std::mutex> lock(latch_);

    LockPrepare(txn, rid);

    CheckAbort(txn, lock_table_[rid]);

    lock_table_[rid].EmplaceLockRequest(txn->GetTxnId(), LockMode::kExclusive);
    txn->GetExclusiveLockSet().insert(rid);
    while (lock_table_[rid].sharing_cnt_ > 0 || lock_table_[rid].is_writing_) {
      lock_table_[rid].cv_.wait(lock);
      CheckAbort(txn, lock_table_[rid]);
    }

    lock_table_[rid].is_writing_ = true;

    return true;
}

/**
 * TODO: Student Implement
 */
bool LockManager::LockUpgrade(Txn *txn, const RowId &rid) {
    std::unique_lock<std::mutex> lock(latch_);

    if (txn->GetState() == TxnState::kShrinking) {
      txn->SetState(TxnState::kAborted);
      throw TxnAbortException(txn->GetTxnId(), AbortReason::kLockOnShrinking);
    } else if (txn->GetIsolationLevel() == IsolationLevel::kReadUncommitted) {
      txn->SetState(TxnState::kAborted);
      throw TxnAbortException(txn->GetTxnId(), AbortReason::kLockSharedOnReadUncommitted);
    }

    CheckAbort(txn, lock_table_[rid]);

    if (lock_table_[rid].is_upgrading_) {
      txn->SetState(TxnState::kAborted);
      throw TxnAbortException(txn->GetTxnId(), AbortReason::kUpgradeConflict);
    }

    // 将共享锁升级为排他锁
    lock_table_[rid].is_upgrading_ = true;
    lock_table_[rid].sharing_cnt_--;
    txn->GetSharedLockSet().erase(rid);
//    lock_table_[rid].req_list_.emplace_back(txn->GetTxnId(), LockMode::kExclusive);
//    lock_table_[rid].req_list_iter_map_[txn->GetTxnId()]->lock_mode_ = LockMode::kExclusive;  // ??
    lock_table_[rid].EraseLockRequest(txn->GetTxnId());
    lock_table_[rid].EmplaceLockRequest(txn->GetTxnId(), LockMode::kExclusive);
    txn->GetExclusiveLockSet().insert(rid);

    while (lock_table_[rid].sharing_cnt_ > 0 || lock_table_[rid].is_writing_) {
        lock_table_[rid].cv_.wait(lock);
        CheckAbort(txn, lock_table_[rid]);
    }

    lock_table_[rid].is_upgrading_ = false;
    lock_table_[rid].is_writing_ = true;
    return true;
}

/**
 * TODO: Student Implement
 */
bool LockManager::Unlock(Txn *txn, const RowId &rid) {
    std::unique_lock<std::mutex> lock(latch_);

    auto iter = lock_table_[rid].req_list_iter_map_.find(txn->GetTxnId());
    if (iter == lock_table_[rid].req_list_iter_map_.end()) {
        return false;
    }

    lock_table_[rid].EraseLockRequest(txn->GetTxnId());

    if (txn->GetExclusiveLockSet().count(rid) > 0) {
        lock_table_[rid].is_writing_ = false;
        lock_table_[rid].is_upgrading_ = false;
        lock_table_[rid].cv_.notify_all();
        txn->GetExclusiveLockSet().erase(rid);
    }

    if (txn->GetSharedLockSet().count(rid) > 0) {
        lock_table_[rid].sharing_cnt_--;
        lock_table_[rid].cv_.notify_all();
        txn->GetSharedLockSet().erase(rid);
    }
    if (txn->GetState() != TxnState::kAborted && txn->GetState() != TxnState::kCommitted) {
        txn->SetState(TxnState::kShrinking);
    }
    return true;
}

/**
 * TODO: Student Implement
 */
void LockManager::LockPrepare(Txn *txn, const RowId &rid) {

//    assert(txn->GetState() == TxnState::kGrowing);
//    if (txn->GetState() == TxnState::kAborted) {
//        throw TxnAbortException(txn->GetTxnId(), AbortReason::kDeadlock);
//    } else
        if (txn->GetState() == TxnState::kShrinking) {
        txn->SetState(TxnState::kAborted);
        throw TxnAbortException(txn->GetTxnId(), AbortReason::kLockOnShrinking);
    } else if (txn->GetIsolationLevel() == IsolationLevel::kReadUncommitted) {
        txn->SetState(TxnState::kAborted);
        throw TxnAbortException(txn->GetTxnId(), AbortReason::kLockSharedOnReadUncommitted);
    }
}

/**
 * TODO: Student Implement
 */
void LockManager::CheckAbort(Txn *txn, LockManager::LockRequestQueue &req_queue) {

    if (txn->GetState() == TxnState::kAborted) {
        req_queue.EraseLockRequest(txn->GetTxnId());
        req_queue.cv_.notify_all();
        throw TxnAbortException(txn->GetTxnId(), AbortReason::kDeadlock);
    }
//    txn_id_t txnId = txn->GetTxnId();
//    IsolationLevel isolationLevel = txn->GetIsolationLevel();

//    if (isolationLevel == IsolationLevel::kReadUncommitted) {
//        txn->SetState(TxnState::kAborted);
//        req_queue.EraseLockRequest(txnId);
//        req_queue.cv_.notify_all();
//        throw TxnAbortException(txnId, AbortReason::kLockSharedOnReadUncommitted);
//    }

//    for (const auto &request :req_queue.req_list_) {
//        if (request.txn_id_ == txnId) {
//          continue;
//        }
//
//        if ((request.lock_mode_ == LockMode::kExclusive) ||
//            (request.lock_mode_ == LockMode::kShared && req_queue.is_writing_)) {
//          txn->SetState(TxnState::kAborted);
//          req_queue.EraseLockRequest(txnId);
//          req_queue.cv_.notify_all();
//          throw TxnAbortException(txnId, AbortReason::kDeadlock);
//        }
//
////        if (txn->GetState() == TxnState::kShrinking) {    // ??
////          txn->SetState(TxnState::kAborted);
////          req_queue.EraseLockRequest(txnId);
////          req_queue.cv_.notify_all();
////          throw TxnAbortException(txnId, AbortReason::kDeadlock);
////        }
//    }
}

/**
 * TODO: Student Implement
 */
void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) {
    std::unique_lock<std::mutex> lock(latch_);

    if (waits_for_.find(t1) == waits_for_.end()) {
        auto &set = waits_for_[t1];
        set.insert(t2);
        return;
    }
    waits_for_[t1].insert(t2);
}

/**
 * TODO: Student Implement
 */
void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) {
    std::unique_lock<std::mutex> lock(latch_);

    if (waits_for_.find(t1) == waits_for_.end()) {
        return;
    }
    waits_for_[t1].erase(t2);
}

/**
 * TODO: Student Implement
 */
 bool LockManager::IsInStack(txn_id_t node) {
    std::stack<txn_id_t > temp_stack = visited_path_;
    while (!temp_stack.empty()) {
        if (temp_stack.top() == node) {
          return true;
        }
        temp_stack.pop();
    }
    return false;
 }

 bool LockManager::DFS(txn_id_t current_node) {
    visited_set_.insert(current_node);
    visited_path_.push(current_node);

    std::vector<txn_id_t> neighbors(waits_for_[current_node].begin(), waits_for_[current_node].end());
    std::sort(neighbors.begin(), neighbors.end());

    for (const auto &neighbor :neighbors) {
        if (visited_set_.find(neighbor) != visited_set_.end()) {
          if (IsInStack(neighbor)) {
            revisited_node_ = current_node;
            return true;
          }
        } else {
          if (DFS(neighbor)) {
            return true;
          }
        }
    }
    visited_path_.pop();
    return false;
 }


bool LockManager::HasCycle(txn_id_t &newest_tid_in_cycle) {
    std::unique_lock<std::mutex> lock(latch_);

    visited_set_.clear();
    visited_path_ = std::stack<txn_id_t>();

    revisited_node_ = INVALID_TXN_ID;

    std::vector<txn_id_t> nodes;
    for (const auto &entry : waits_for_) {
        nodes.push_back(entry.first);
    }
    // 对 keys 进行排序
    std::sort(nodes.begin(), nodes.end());

    for (const auto &entry : nodes) {
        if (visited_set_.find(entry) == visited_set_.end()) {
          if (DFS(entry)) {
//                  newest_tid_in_cycle = revisited_node_;
                  newest_tid_in_cycle = revisited_node_;
                  return true;
          }
        }
    }
    return false;
}

void LockManager::DeleteNode(txn_id_t txn_id) {
    waits_for_.erase(txn_id);

    auto *txn = txn_mgr_->GetTransaction(txn_id);

    for (const auto &row_id: txn->GetSharedLockSet()) {
        for (const auto &lock_req: lock_table_[row_id].req_list_) {
            if (lock_req.granted_ == LockMode::kNone) {
                RemoveEdge(lock_req.txn_id_, txn_id);
            }
        }
    }

    for (const auto &row_id: txn->GetExclusiveLockSet()) {
        for (const auto &lock_req: lock_table_[row_id].req_list_) {
            if (lock_req.granted_ == LockMode::kNone) {
                RemoveEdge(lock_req.txn_id_, txn_id);
            }
        }
    }
}

/**
 * TODO: Student Implement
 */
void LockManager::CreateWaitGraph() {
    waits_for_.clear();

    for (auto it = lock_table_.begin(); it != lock_table_.end(); it++) {
        const auto &entry = *it;
        const RowId &rid = entry.first;
        const LockRequestQueue &req_queue = entry.second;


        // TODO
        for (auto req_iter = req_queue.req_list_.begin(); req_iter != req_queue.req_list_.end(); req_iter++) {
            txn_id_t txnId = req_iter->txn_id_;
            LockMode lockMode = req_iter->lock_mode_;

            if (lockMode == LockMode::kShared) {
                for(auto next_req_iter = std::next(req_iter); next_req_iter != req_queue.req_list_.end(); next_req_iter++) {
                  txn_id_t next_txnId = next_req_iter->txn_id_;
                  LockMode next_lockMode = next_req_iter->lock_mode_;

                  if (next_lockMode == LockMode::kExclusive) {
                    AddEdge(txnId, next_txnId);
                  }
                }
            } else if (lockMode == LockMode::kExclusive) {
                  for (auto next_req_iter = std::next(req_iter); next_req_iter != req_queue.req_list_.end(); next_req_iter++) {
                    txn_id_t next_txnId = next_req_iter->txn_id_;
                    AddEdge(txnId, next_txnId);
                  }
            }
        }
    }


}


void LockManager::RunCycleDetection() {
  std::thread cycle_detection_thread([this]() {
      while (enable_cycle_detection_) {
        std::this_thread::sleep_for(cycle_detection_interval_);

        CreateWaitGraph();

        txn_id_t newest_tid_in_cycle = INVALID_TXN_ID;
        bool has_cycle = HasCycle(newest_tid_in_cycle);
        if (has_cycle) {
          std::cout << "Detected cycle involving transaction:" << newest_tid_in_cycle << std::endl;
          auto *txn = txn_mgr_->GetTransaction(newest_tid_in_cycle);
          {
            std::unique_lock<std::mutex> lock(latch_);
            txn->SetState(TxnState::kAborted);
          }
          for (const auto &rid : txn->GetSharedLockSet()) {
            lock_table_[rid].cv_.notify_all();
          }
          for (const auto &rid : txn->GetExclusiveLockSet()) {
            lock_table_[rid].cv_.notify_all();
          }
        }
      }
    });
  cycle_detection_thread.join();
}

/**
 * TODO: Student Implement
 */
std::vector<std::pair<txn_id_t, txn_id_t>> LockManager::GetEdgeList() {
    std::vector<std::pair<txn_id_t, txn_id_t>> result;

    for (const auto &entry : waits_for_) {
        txn_id_t t1 = entry.first;
        const auto &neighbors = entry.second;
        for (auto t2 : neighbors) {
            result.emplace_back(t1, t2);
        }
    }
    return result;
}
