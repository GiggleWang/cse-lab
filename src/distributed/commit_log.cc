#include <algorithm>

#include "common/bitmap.h"
#include "common/config.h"
#include "distributed/commit_log.h"
#include "distributed/metadata_server.h"
#include "filesystem/directory_op.h"
#include "metadata/inode.h"
#include <chrono>

namespace chfs {
/**
 * `CommitLog` part
 */
// {Your code here}
CommitLog::CommitLog(std::shared_ptr<BlockManager> bm,
                     bool is_checkpoint_enabled)
    : is_checkpoint_enabled_(is_checkpoint_enabled), bm_(bm) {
  this->max_log_entry_num = kLogBlockCnt * DiskBlockSize / sizeof(LogEntry);
  this->startPtr = this->bm_->unsafe_get_block_ptr() +
                   (this->bm_->total_blocks() - kLogBlockCnt) * DiskBlockSize;
  this->entry_num = 0;
  this->txn_num = 0;
  this->latest_txn = 0;
  this->begin_txn = 0;
}

CommitLog::~CommitLog() {}

// {Your code here}
auto CommitLog::get_log_entry_num() -> usize {
  return txn_num;

}

// {Your code here}
auto CommitLog::append_log(txn_id_t txn_id,
                           std::vector<std::shared_ptr<BlockOperation>> ops)
    -> void {
  std::lock_guard<std::mutex> lock(log_mtx);
  for (auto op : ops) {
    auto log_entries = reinterpret_cast<LogEntry *>(startPtr);
    log_entries[entry_num].block_id = op->block_id_;
    log_entries[entry_num].txn_id = txn_id;
    std::memcpy(log_entries[entry_num].new_block_state,
                op->new_block_state_.data(), DiskBlockSize);
    entry_num++;
  }
  auto sync_res =
      this->bm_->sync_memory(this->startPtr, entry_num * sizeof(LogEntry));
  if (sync_res.is_err()) {
    std::cout << "error in sync_res in append log" << std::endl;
  }
  this->txn_num++;
  this->latest_txn = txn_id;
}

// {Your code here}
auto CommitLog::commit_log(txn_id_t txn_id) -> void {
  //将log里面的transaction设为完成的id
  std::lock_guard<std::mutex> lock(log_mtx);
  auto log_entries = reinterpret_cast<LogEntry *>(startPtr);
  for (int i = 0; i < entry_num; i++) {
    if (log_entries[i].txn_id == txn_id) {
      log_entries[i].txn_id = this->FINISHED_TXN_ID;
    }
  }
  auto sync_res =
      this->bm_->sync_memory(this->startPtr, entry_num * sizeof(LogEntry));
  if (sync_res.is_err()) {
    std::cout << "error in sync_res in commit log" << std::endl;
  }
  this->txn_num--;
}

// {Your code here}
auto CommitLog::checkpoint() -> void {
  std::lock_guard<std::mutex> lock(log_mtx);
  LogEntry *log_entries = reinterpret_cast<LogEntry *>(startPtr);
  std::vector<LogEntry> unfinished_log_data;
  for (usize i = 0; i < entry_num; i++) {
    if (log_entries[i].txn_id != this->FINISHED_TXN_ID) {
      unfinished_log_data.push_back(log_entries[i]); // 保留未完成的条目
    } else {
      auto write_res = bm_->write_block(log_entries[i].block_id,
                                        log_entries[i].new_block_state);
      if (!write_res.is_ok()) {
        std::cerr << "Failed to persist block " << log_entries[i].block_id
                  << " during checkpoint" << std::endl;
      }
    }
  }
  std::memset(log_entries, 0, entry_num * sizeof(LogEntry));
  std::copy(unfinished_log_data.begin(), unfinished_log_data.end(),
            log_entries);
  entry_num = unfinished_log_data.size();
  auto flush_res =
      bm_->sync_memory(this->startPtr, entry_num * sizeof(LogEntry));
  ;
  if (flush_res.is_err()) {
    std::cerr << "Error flushing log during checkpoint" << std::endl;
  }
}

// {Your code here}
auto CommitLog::recover() -> void {
  std::lock_guard<std::mutex> lock(log_mtx);
  LogEntry *current_log = reinterpret_cast<LogEntry *>(startPtr);
  for (usize index = 0; index < entry_num; ++index) {
    LogEntry &entry = current_log[index];
    if (entry.block_id == this->FINISHED_TXN_ID) {
      continue;
    }
    auto result = bm_->write_block(entry.block_id, entry.new_block_state);
    if (!result.is_ok()) {
      // std::cerr << "Error: Unable to restore block ID: " << entry.block_id
      //           << " at log index: " << index << std::endl;
      continue;
    }
  }
  // std::cout << "Recovery complete. Restored " << entry_num << " log entries."
  // << std::endl;
}

txn_id_t CommitLog::new_txn_id() { return latest_txn + 1; }
}; // namespace chfs
