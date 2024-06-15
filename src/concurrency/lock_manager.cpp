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

  while (lock_table_[rid].is_writing_ || lock_table_[rid].is_upgrading_) {
    lock_table_[rid].cv_.wait(lock);
    CheckAbort(txn, lock_table_[rid]);
  }

  lock_table_[rid].sharing_cnt_++;
  lock_table_[rid].req_list_.emplace_back(txn->GetTxnId(), LockMode::kShared);
  lock_table_[rid].req_list_iter_map_[txn->GetTxnId()] = --lock_table_[rid].req_list_.end();
  return true;
}

/**
 * TODO: Student Implement
 */
bool LockManager::LockExclusive(Txn *txn, const RowId &rid) {
    std::unique_lock<std::mutex> lock(latch_);

    LockPrepare(txn, rid);

    CheckAbort(txn, lock_table_[rid]);

    // may wrong?
    while (lock_table_[rid].sharing_cnt_ > 0 || lock_table_[rid].is_writing_) {
      lock_table_[rid].cv_.wait(lock);
      CheckAbort(txn, lock_table_[rid]);
    }

    lock_table_[rid].is_writing_ = true;
    lock_table_[rid].req_list_.emplace_back(txn->GetTxnId(), LockMode::kExclusive);
    lock_table_[rid].req_list_iter_map_[txn->GetTxnId()] = --lock_table_[rid].req_list_.end();
    return true;
}

/**
 * TODO: Student Implement
 */
bool LockManager::LockUpgrade(Txn *txn, const RowId &rid) {
    std::unique_lock<std::mutex> lock(latch_);

    CheckAbort(txn, lock_table_[rid]);

    // 将共享锁升级为排他锁
    lock_table_[rid].is_upgrading_ = true;
    lock_table_[rid].sharing_cnt_--;
    lock_table_[rid].req_list_iter_map_[txn->GetTxnId()]->lock_mode_ = LockMode::kExclusive;

    while (lock_table_[rid].sharing_cnt_ > 0 || lock_table_[rid].is_writing_) {
        lock_table_[rid].cv_.wait(lock);
        CheckAbort(txn, lock_table_[rid]);
    }

    lock_table_[rid].is_writing_ = true;
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

    lock_table_[rid].req_list_.erase(iter->second);
    lock_table_[rid].req_list_iter_map_.erase(iter);

    if (txn->GetExclusiveLockSet().count(rid) > 0) {
        lock_table_[rid].is_writing_ = false;
        lock_table_[rid].is_upgrading_ = false;
        lock_table_[rid].cv_.notify_all();
    }

    if (txn->GetSharedLockSet().count(rid) > 0) {
        lock_table_[rid].sharing_cnt_--;
        lock_table_[rid].cv_.notify_all();
    }

    return true;
}

/**
 * TODO: Student Implement
 */
void LockManager::LockPrepare(Txn *txn, const RowId &rid) {
    std::unique_lock<std::mutex> lock(latch_);

    assert(txn->GetState() == TxnState::kGrowing || txn->GetState() == TxnState::kShrinking);

    if (lock_table_.find(rid) == lock_table_.end()) {
        auto& req_list = lock_table_[rid];
        req_list.EmplaceLockRequest(txn->GetTxnId(), LockMode::kNone);
    }
}

/**
 * TODO: Student Implement
 */
void LockManager::CheckAbort(Txn *txn, LockManager::LockRequestQueue &req_queue) {
    std::unique_lock<std::mutex> lock(latch_);

    txn_id_t txnId = txn->GetTxnId();

    for (const auto &request :req_queue.req_list_) {
        if (request.txn_id_ == txnId) {
          continue;
        }

        if ((request.lock_mode_ == LockMode::kExclusive) ||
            (request.lock_mode_ == LockMode::kShared && req_queue.is_writing_)) {
          txn->SetState(TxnState::kAborted);
          req_queue.EraseLockRequest(txnId);
          req_queue.cv_.notify_all();
          throw TxnAbortException(txnId, AbortReason::kDeadlock);
        }

        if (txn->GetState() == TxnState::kShrinking) {
          txn->SetState(TxnState::kAborted);
          req_queue.EraseLockRequest(txnId);
          req_queue.cv_.notify_all();
          throw TxnAbortException(txnId, AbortReason::kDeadlock);
        }
    }
}

/**
 * TODO: Student Implement
 */
void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) {
    std::unique_lock<std::mutex> lock(latch_);

    if (waits_for_.find(t1) == waits_for_.end()) {
        auto &set = waits_for_[t1];

    }
    waits_for_[t1].insert(t2);
}

/**
 * TODO: Student Implement
 */
void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) {}

/**
 * TODO: Student Implement
 */
bool LockManager::HasCycle(txn_id_t &newest_tid_in_cycle) {}

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
void LockManager::RunCycleDetection() {}

/**
 * TODO: Student Implement
 */
std::vector<std::pair<txn_id_t, txn_id_t>> LockManager::GetEdgeList() {
    std::vector<std::pair<txn_id_t, txn_id_t>> result;
    return result;
}
