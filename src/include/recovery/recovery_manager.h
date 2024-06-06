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
        // 初始化持久化数据
        data_ = last_checkpoint.persist_data_;

        // 初始化活跃事务
        active_txns_ = last_checkpoint.active_txns_;

        // 设置持久化LSN
        persist_lsn_ = last_checkpoint.checkpoint_lsn_;
    }

    /**
    * TODO: Student Implement
    */
    void RedoPhase() {
        // 从检查点处开始重做
        std::unordered_map<int, std::vector<LogRecPtr>> txn_logs; // 记录每个事务的日志
        for (auto it = log_recs_.upper_bound(persist_lsn_); it != log_recs_.end(); ++it) {
            LogRecPtr log_rec = it->second;
            txn_logs[log_rec->txn_id_].push_back(log_rec); // 记录事务日志
            switch (log_rec->type_) {
                case LogRecType::kInsert:
                    // 处理插入操作
                    data_[log_rec->new_key_] = log_rec->new_value_;
                    break;
                case LogRecType::kDelete:
                    // 处理删除操作
                    data_.erase(log_rec->old_key_);
                    break;
                case LogRecType::kUpdate:
                    // 处理更新操作
                    if (log_rec->old_key_ == log_rec->new_key_) {
                        // 如果旧键和新键相同，直接更新键的值
                        data_[log_rec->old_key_] = log_rec->new_value_;
                    } else {
                        // 如果旧键和新键不同，删除旧键，并添加新键
                        data_.erase(log_rec->old_key_);
                        data_[log_rec->new_key_] = log_rec->new_value_;
                    }
                    break;
                case LogRecType::kCommit:
                    // 提交事务，移除活跃事务
                    active_txns_.erase(log_rec->txn_id_);
                    break;
                case LogRecType::kAbort:
                    // 撤销事务的所有修改
                    for (const auto &txn_log : txn_logs[log_rec->txn_id_]) {
                        switch (txn_log->type_) {
                            case LogRecType::kInsert:
                                data_.erase(txn_log->new_key_);
                                break;
                            case LogRecType::kDelete:
                                data_[txn_log->old_key_] = txn_log->old_value_;
                                break;
                            case LogRecType::kUpdate:
                                data_[txn_log->old_key_] = txn_log->old_value_;
                                break;
                            default:
                                break;
                        }
                    }
                    active_txns_.erase(log_rec->txn_id_);
                    break;
                default:
                    // 其他类型的日志记录暂时忽略
                    break;
            }
            // 更新持久化LSN
            persist_lsn_ = log_rec->lsn_;
        }
    }


    /**
    * TODO: Student Implement
    */
    void UndoPhase() {
        // 从最后一个日志记录开始，对所有的日志记录进行撤销
        for (auto it = log_recs_.rbegin(); it != log_recs_.rend(); ++it) {
            LogRecPtr log_rec = it->second;
            switch (log_rec->type_) {
                case LogRecType::kInsert:
                    // 插入操作的逆操作是删除键
                        data_.erase(log_rec->new_key_);
                break;
                case LogRecType::kDelete:
                    // 删除操作的逆操作是插入键
                        data_[log_rec->old_key_] = log_rec->old_value_;
                break;
                case LogRecType::kUpdate:
                    // 更新操作的逆操作是用旧值恢复键
                        data_[log_rec->old_key_] = log_rec->old_value_;
                // 如果键变更了，恢复新键
                if (log_rec->old_key_ != log_rec->new_key_) {
                    data_.erase(log_rec->new_key_);
                }
                break;
                case LogRecType::kCommit:
                    // 提交操作的逆操作是将事务重新添加到活跃事务列表中
                        active_txns_.insert(std::make_pair(log_rec->txn_id_, log_rec->lsn_));
                break;
                case LogRecType::kAbort:
                    // 中止操作的逆操作是将事务从活跃事务列表中移除
                        active_txns_.erase(log_rec->txn_id_);
                break;
                default:
                    // 其他类型的日志记录暂时忽略
                        break;
            }
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
