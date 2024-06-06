#ifndef MINISQL_LOG_REC_H
#define MINISQL_LOG_REC_H

#include <unordered_map>
#include <utility>

#include "common/config.h"
#include "common/rowid.h"
#include "record/row.h"

enum class LogRecType {
    kInvalid,
    kInsert,
    kDelete,
    kUpdate,
    kBegin,
    kCommit,
    kAbort,
};

// used for testing only
using KeyType = std::string;
using ValType = int32_t;

/**
 * TODO: Student Implement
 */
struct LogRec {
    LogRec() = default;

    LogRecType type_{LogRecType::kInvalid};
    lsn_t lsn_{INVALID_LSN};
    lsn_t prev_lsn_{INVALID_LSN};
    txn_id_t txn_id_{INVALID_TXN_ID}; // 事务ID

    KeyType old_key_;     // 操作的旧键
    KeyType new_key_;     // 操作的新键
    ValType old_value_{};   // 更新操作的旧值
    ValType new_value_{};   // 更新操作的新值

    // 用于记录事务ID对应的上一个LSN，用于Undo操作
    static std::unordered_map<txn_id_t, lsn_t> prev_lsn_map_;

    // 用于记录下一个LSN，用于分配给新的日志记录
    static lsn_t next_lsn_;
};

std::unordered_map<txn_id_t, lsn_t> LogRec::prev_lsn_map_ = {};
lsn_t LogRec::next_lsn_ = 0;

typedef std::shared_ptr<LogRec> LogRecPtr;

/**
 * TODO: Student Implement
 */
static LogRecPtr CreateInsertLog(txn_id_t txn_id, KeyType key, ValType val) {
    auto log_rec = std::make_shared<LogRec>();
    log_rec->type_ = LogRecType::kInsert;
    log_rec->prev_lsn_ = LogRec::prev_lsn_map_[txn_id];
    LogRec::prev_lsn_map_[txn_id] = LogRec::next_lsn_;
    log_rec->lsn_ = LogRec::next_lsn_++;
    log_rec->txn_id_ = txn_id;
    log_rec->new_key_ = std::move(key);
    log_rec->new_value_ = val;
    return log_rec;
}

/**
 * TODO: Student Implement
 */
static LogRecPtr CreateDeleteLog(txn_id_t txn_id, KeyType del_key, ValType del_val) {
    auto log_rec = std::make_shared<LogRec>();
    log_rec->type_ = LogRecType::kDelete;
    log_rec->prev_lsn_ = LogRec::prev_lsn_map_[txn_id];
    LogRec::prev_lsn_map_[txn_id] = LogRec::next_lsn_;
    log_rec->lsn_ = LogRec::next_lsn_++;
    log_rec->txn_id_ = txn_id;
    log_rec->old_key_ = std::move(del_key);
    log_rec->old_value_ = del_val;
    return log_rec;
}

/**
 * TODO: Student Implement
 */
static LogRecPtr CreateUpdateLog(txn_id_t txn_id, KeyType old_key, ValType old_val, KeyType new_key, ValType new_val) {
    auto log_rec = std::make_shared<LogRec>();
    log_rec->type_ = LogRecType::kUpdate;
    log_rec->prev_lsn_ = LogRec::prev_lsn_map_[txn_id];
    LogRec::prev_lsn_map_[txn_id] = LogRec::next_lsn_;
    log_rec->lsn_ = LogRec::next_lsn_++;
    log_rec->txn_id_ = txn_id;
    log_rec->old_key_ = std::move(old_key);
    log_rec->old_value_ = old_val;
    log_rec->new_key_ = std::move(new_key);
    log_rec->new_value_ = new_val;
    return log_rec;
}

/**
 * TODO: Student Implement
 */
static LogRecPtr CreateBeginLog(txn_id_t txn_id) {
    auto log_rec = std::make_shared<LogRec>();
    log_rec->type_ = LogRecType::kBegin;
    log_rec->lsn_ = LogRec::next_lsn_++;
    log_rec->prev_lsn_ = INVALID_LSN;
    log_rec->txn_id_ = txn_id;
    LogRec::prev_lsn_map_[txn_id] = log_rec->lsn_;
    return log_rec;
}

/**
 * TODO: Student Implement
 */
static LogRecPtr CreateCommitLog(txn_id_t txn_id) {
    auto log_rec = std::make_shared<LogRec>();
    log_rec->type_ = LogRecType::kCommit;
    log_rec->prev_lsn_ = LogRec::prev_lsn_map_[txn_id];
    LogRec::prev_lsn_map_[txn_id] = LogRec::next_lsn_;
    log_rec->lsn_ = LogRec::next_lsn_++;
    log_rec->txn_id_ = txn_id;
    return log_rec;
}

/**
 * TODO: Student Implement
 */
static LogRecPtr CreateAbortLog(txn_id_t txn_id) {
    auto log_rec = std::make_shared<LogRec>();
    log_rec->type_ = LogRecType::kAbort;
    log_rec->prev_lsn_ = LogRec::prev_lsn_map_[txn_id];
    LogRec::prev_lsn_map_[txn_id] = LogRec::next_lsn_;
    log_rec->lsn_ = LogRec::next_lsn_++;
    log_rec->txn_id_ = txn_id;
    return log_rec;
}

#endif  // MINISQL_LOG_REC_H
