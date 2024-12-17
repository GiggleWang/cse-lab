#pragma once

#include "block/manager.h"
#include "common/macros.h"
#include "filesystem/operations.h"
#include "rsm/config.h"
#include "rsm/raft/protocol.h"
#include <cstring>
#include <mutex>
#include <vector>
namespace chfs {

/**
 * RaftLog uses a BlockManager to manage the data..
 */
template <typename Command> class RaftLog {
public:
  RaftLog(std::shared_ptr<BlockManager> bm, bool should_recover, int current_term,
          int support_id);
  ~RaftLog();
  /* Lab3: Your code here */
  void term_and_support_id_update(int current_term, int support_id);
  void log_entry_update(std::vector<LogEntry<Command>> &data);
  void recover(int &current_term, int &voted_for,
               std::vector<LogEntry<Command>> &data);

private:
  std::shared_ptr<BlockManager> bm_;
  std::mutex mtx;
  /* Lab3: Your code here */
  std::shared_ptr<FileOperation> file_operation_ptr;
};

template <typename Command> RaftLog<Command>::~RaftLog() {
  /* Lab3: Your code here */
}

/* Lab3: Your code here */
template <typename Command>
RaftLog<Command>::RaftLog(std::shared_ptr<BlockManager> bm, bool should_recover,
                          int current_term, int support_id)
    : bm_(bm) {
  /* Lab3: Your code here */
  if (!should_recover) {
    file_operation_ptr.reset(new FileOperation(bm_, MAX_INODE_NUM));
    // for TERM_AND_SUPPORT_INODE_ID
    file_operation_ptr->alloc_inode(InodeType::FILE);
    // for LOG_ENTRY_INODE_ID
    file_operation_ptr->alloc_inode(InodeType::FILE);
    this->term_and_support_id_update(current_term, support_id);
  } else {
    // recover
    this->file_operation_ptr = FileOperation::create_from_raw(bm_).unwrap();
  }
}

template <typename Command>
void RaftLog<Command>::term_and_support_id_update(int current_term,
                                                  int support_id) {
  this->mtx.lock();
  std::vector<int> new_vector = {current_term, support_id};
  std::vector<u8> u8_vec(reinterpret_cast<u8 *>(new_vector.data()),
                         reinterpret_cast<u8 *>(new_vector.data()) +
                             sizeof(int) * 2);
  this->file_operation_ptr->write_file(TERM_AND_SUPPORT_INODE_ID, u8_vec);
  this->mtx.unlock();
}

template <typename Command>
void RaftLog<Command>::log_entry_update(std::vector<LogEntry<Command>> &data) {
  this->mtx.lock();
  std::vector<u8> u8_vec;
  for (const auto &entry : data) {
    const u8 *ptr = reinterpret_cast<const u8 *>(&entry);
    u8_vec.insert(u8_vec.end(), const_cast<u8 *>(ptr),
                  const_cast<u8 *>(ptr) + sizeof(LogEntry<Command>));
  }
  file_operation_ptr->write_file(LOG_ENTRY_INODE_ID, u8_vec);
  this->mtx.unlock();
}

template <typename Command>
void RaftLog<Command>::recover(int &current_term, int &support_id,
                               std::vector<LogEntry<Command>> &data) {
  this->mtx.lock();
  auto term_and_support_data =
      file_operation_ptr->read_file(TERM_AND_SUPPORT_INODE_ID).unwrap();
  auto *term_and_support =
      reinterpret_cast<const int *>(term_and_support_data.data());
  current_term = term_and_support[0];
  support_id = term_and_support[1];
  auto log_data = file_operation_ptr->read_file(LOG_ENTRY_INODE_ID).unwrap();
  size_t log_entry_num = log_data.size() / sizeof(LogEntry<Command>);
  auto *log_entry_data =
      reinterpret_cast<const LogEntry<Command> *>(log_data.data());
  // Assign log entries to the output vector
  data.assign(log_entry_data, log_entry_data + log_entry_num);
  this->mtx.unlock();
}

} /* namespace chfs */