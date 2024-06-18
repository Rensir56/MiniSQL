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
    LogRec(LogRecType type, lsn_t lsn, txn_id_t txn_id, lsn_t prev_lsn)
    : type_(type), lsn_(lsn), txn_id_(txn_id), prev_lsn_(prev_lsn) {}


    LogRecType type_{LogRecType::kInvalid};
    lsn_t lsn_{INVALID_LSN};
    lsn_t prev_lsn_{INVALID_LSN};
    txn_id_t txn_id_{INVALID_TXN_ID};

    /* used for insert only */
    KeyType ins_key_{};
    ValType ins_val_{};
    /* used for delete only */
    KeyType del_key_{};
    ValType del_val_{};
    /* used for update only */
    KeyType old_key_{};
    ValType old_val_{};
    KeyType new_key_{};
    ValType new_val_{};

    /* used for testing only */
    static std::unordered_map<txn_id_t, lsn_t> prev_lsn_map_;
    static lsn_t next_lsn_;

    static lsn_t Getprevlsn(txn_id_t txn_id, lsn_t lsn) {
      auto prev_lsn = prev_lsn_map_.find(txn_id);
      if(prev_lsn == prev_lsn_map_.end()) {
        prev_lsn_map_.emplace(txn_id, lsn);
        return INVALID_LSN;
      } else {
        lsn_t temp = prev_lsn->second;
        prev_lsn->second = lsn;
        return temp;
      }
    }
};

std::unordered_map<txn_id_t, lsn_t> LogRec::prev_lsn_map_ = {};
lsn_t LogRec::next_lsn_ = 0;

typedef std::shared_ptr<LogRec> LogRecPtr;

/**
 * TODO: Student Implement
 */
static LogRecPtr CreateInsertLog(txn_id_t txn_id, KeyType ins_key, ValType ins_val) {
    lsn_t lsn = LogRec::next_lsn_++;
    LogRecPtr log_rec = std::make_shared<LogRec>(LogRecType::kInsert, lsn, txn_id, LogRec::Getprevlsn(txn_id, lsn));
    log_rec->ins_key_ = ins_key;
    log_rec->ins_val_ = ins_val;
    return log_rec;
}

/**
 * TODO: Student Implement
 */
static LogRecPtr CreateDeleteLog(txn_id_t txn_id, KeyType del_key, ValType del_val) {
    lsn_t lsn = LogRec::next_lsn_++;
    LogRecPtr log_rec = std::make_shared<LogRec>(LogRecType::kDelete, lsn, txn_id, LogRec::Getprevlsn(txn_id, lsn));
    log_rec->del_key_ = del_key;
    log_rec->del_val_ = del_val;
    return log_rec;
}

/**
 * TODO: Student Implement
 */
static LogRecPtr CreateUpdateLog(txn_id_t txn_id, KeyType old_key, ValType old_val, KeyType new_key, ValType new_val) {
    lsn_t lsn = LogRec::next_lsn_++;
    LogRecPtr log_rec = std::make_shared<LogRec>(LogRecType::kUpdate, lsn, txn_id, LogRec::Getprevlsn(txn_id, lsn));
    log_rec->old_key_ = old_key;
    log_rec->old_val_ = old_val;
    log_rec->new_key_ = new_key;
    log_rec->new_val_ = new_val;
    return log_rec;
}

/**
 * TODO: Student Implement
 */
static LogRecPtr CreateBeginLog(txn_id_t txn_id) {
    lsn_t lsn = LogRec::next_lsn_++;
    LogRecPtr log_rec = std::make_shared<LogRec>(LogRecType::kBegin, lsn, txn_id, LogRec::Getprevlsn(txn_id, lsn));
    return log_rec;
}

/**
 * TODO: Student Implement
 */
static LogRecPtr CreateCommitLog(txn_id_t txn_id) {
    lsn_t lsn = LogRec::next_lsn_++;
    LogRecPtr log_rec = std::make_shared<LogRec>(LogRecType::kCommit, lsn, txn_id, LogRec::Getprevlsn(txn_id, lsn));
    return log_rec;
}

/**
 * TODO: Student Implement
 */
static LogRecPtr CreateAbortLog(txn_id_t txn_id) {
    lsn_t lsn = LogRec::next_lsn_++;
    LogRecPtr log_rec = std::make_shared<LogRec>(LogRecType::kAbort, lsn, txn_id, LogRec::Getprevlsn(txn_id, lsn));
    return log_rec;
}

#endif  // MINISQL_LOG_REC_H
