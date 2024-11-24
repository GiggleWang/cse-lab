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
                   (this->bm_->block_size() - kLogBlockCnt) * DiskBlockSize;
  this->entry_num = 0;
}

CommitLog::~CommitLog() {}

// {Your code here}
auto CommitLog::get_log_entry_num() -> usize {
  // TODO: Implement this function.
  UNIMPLEMENTED();
  return 0;
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
  this->bm_->sync_memory(this->startPtr, entry_num * sizeof(LogEntry));
}

// {Your code here}
auto CommitLog::commit_log(txn_id_t txn_id) -> void {
  // TODO: Implement this function.
  UNIMPLEMENTED();
}

// {Your code here}
auto CommitLog::checkpoint() -> void {
  // TODO: Implement this function.
  UNIMPLEMENTED();
}

// {Your code here}
auto CommitLog::recover() -> void {
  std::lock_guard<std::mutex> lock(log_mtx);
  LogEntry *current_log = reinterpret_cast<LogEntry *>(startPtr);
  for (usize index = 0; index < entry_num; ++index) {
    LogEntry &entry = current_log[index];
    if (entry.block_id == KInvalidBlockID) {
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
}; // namespace chfs