#ifndef MINISQL_RECOVERY_MANAGER_H
#define MINISQL_RECOVERY_MANAGER_H

#include <map>
#include <unordered_map>
#include <vector>

#include "recovery/log_rec.h"

using KvDatabase = std::unordered_map<KeyType, ValType>;
using ATT = std::unordered_map<txn_id_t, lsn_t>;

struct CheckPoint {
    lsn_t checkpoint_lsn_{INVALID_LSN};
    ATT active_txns_{};
    KvDatabase persist_data_{};

    inline void AddActiveTxn(txn_id_t txn_id, lsn_t last_lsn) { active_txns_[txn_id] = last_lsn; }

    inline void AddData(KeyType key, ValType val) { persist_data_.emplace(std::move(key), val); }
};

class RecoveryManager {
public:
    /**
    * TODO: Student Implement
    */
    void Init(CheckPoint &last_checkpoint) {
        persist_lsn_ = last_checkpoint.checkpoint_lsn_;
        active_txns_ = last_checkpoint.active_txns_;
        data_ = last_checkpoint.persist_data_;
    }

    /**
    * TODO: Student Implement
    */
    void RedoPhase() {
        auto a = log_recs_.begin();
        for (; a != log_recs_.end() && a->first < persist_lsn_; a++) {
        }
        for (; a != log_recs_.end(); a++) {
            LogRecPtr log_rec = a->second;
            if(log_rec->type_ == LogRecType::kInvalid) {
                active_txns_[log_rec->txn_id_] = log_rec->lsn_;
            } else if(log_rec->type_ == LogRecType::kInsert) {
                active_txns_[log_rec->txn_id_] = log_rec->lsn_;
                data_.emplace(log_rec->ins_key_, log_rec->ins_val_);
            } else if (log_rec->type_ == LogRecType::kUpdate) {
                active_txns_[log_rec->txn_id_] = log_rec->lsn_;
                data_.erase(log_rec->old_key_);
                data_[log_rec->new_key_] = log_rec->new_val_;
            } else if (log_rec->type_ == LogRecType::kDelete) {
                active_txns_[log_rec->txn_id_] = log_rec->lsn_;
                data_.erase(log_rec->del_key_);
            } else if (log_rec->type_ == LogRecType::kBegin) {
                active_txns_[log_rec->txn_id_] = log_rec->lsn_;
            } else if (log_rec->type_ == LogRecType::kCommit) {
                active_txns_[log_rec->txn_id_] = log_rec->lsn_;
                active_txns_.erase(log_rec->txn_id_);
            } else if (log_rec->type_ == LogRecType::kAbort) {
                active_txns_[log_rec->txn_id_] = log_rec->lsn_;
                rollback(log_rec->txn_id_);
                active_txns_.erase(log_rec->txn_id_);
            }
        } 
    }

    /**
    * TODO: Student Implement
    */
    void UndoPhase() {
        for(const auto it : active_txns_) {
            rollback(it.first);
        }
        active_txns_.clear();
    }

    void rollback(txn_id_t txn_id) {
        lsn_t last_lsn = active_txns_[txn_id];
        while(last_lsn != INVALID_LSN) {
            LogRecPtr log_rec = log_recs_[last_lsn];
            if (log_rec == nullptr) break;
            if(log_rec->type_ == LogRecType::kInsert) {
                data_.erase(log_rec->ins_key_);
            } else if (log_rec->type_ == LogRecType::kUpdate) {
                data_.erase(log_rec->new_key_);
                data_[log_rec->old_key_] = log_rec->old_val_;
            } else if (log_rec->type_ == LogRecType::kDelete) {
                data_[log_rec->del_key_] = log_rec->del_val_;
            }
            last_lsn = log_rec->prev_lsn_;
        }
    }
    // used for test only
    void AppendLogRec(LogRecPtr log_rec) { log_recs_.emplace(log_rec->lsn_, log_rec); }

    // used for test only
    inline KvDatabase &GetDatabase() { return data_; }

private:
    std::map<lsn_t, LogRecPtr> log_recs_{};
    lsn_t persist_lsn_{INVALID_LSN};
    ATT active_txns_{};
    KvDatabase data_{};  // all data in database
};

#endif  // MINISQL_RECOVERY_MANAGER_H
