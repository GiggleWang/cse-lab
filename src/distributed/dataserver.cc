#include "distributed/dataserver.h"
#include "common/util.h"
#include <algorithm>
#include <vector>

namespace chfs {

auto DataServer::initialize(std::string const &data_path) {
  /**
   * At first check whether the file exists or not.
   * If so, which means the distributed chfs has
   * already been initialized and can be rebuilt from
   * existing data.
   */
  bool is_initialized = is_file_exist(data_path);
  auto bm = std::shared_ptr<BlockManager>(
      new BlockManager(data_path, KDefaultBlockCnt));
  auto num_of_version_block =
      (KDefaultBlockCnt * sizeof(version_t)) / bm->block_size();
  if (is_initialized) {
    block_allocator_ =
        std::make_shared<BlockAllocator>(bm, num_of_version_block, false);
  } else {
    // We need to reserve some blocks for storing the version of each block
    block_allocator_ = std::shared_ptr<BlockAllocator>(
        new BlockAllocator(bm, num_of_version_block, true));
  }

  // Initialize the RPC server and bind all handlers
  server_->bind("read_data", [this](block_id_t block_id, usize offset,
                                    usize len, version_t version) {
    return this->read_data(block_id, offset, len, version);
  });
  server_->bind("write_data", [this](block_id_t block_id, usize offset,
                                     std::vector<u8> &buffer) {
    return this->write_data(block_id, offset, buffer);
  });
  server_->bind("alloc_block", [this]() { return this->alloc_block(); });
  server_->bind("free_block", [this](block_id_t block_id) {
    return this->free_block(block_id);
  });

  // Launch the rpc server to listen for requests
  server_->run(true, num_worker_threads);
}

DataServer::DataServer(u16 port, const std::string &data_path)
    : server_(std::make_unique<RpcServer>(port)) {
  initialize(data_path);
}

DataServer::DataServer(std::string const &address, u16 port,
                       const std::string &data_path)
    : server_(std::make_unique<RpcServer>(address, port)) {
  initialize(data_path);
}

DataServer::~DataServer() { server_.reset(); }

// {Your code here}
auto DataServer::read_data(block_id_t block_id, usize offset, usize len,
                           version_t version) -> std::vector<u8> {
  const auto BLOCK_SIZE = block_allocator_->bm->block_size();
  std::vector<u8> res_buf(0);
  if (block_id >= block_allocator_->bm->total_blocks() ||
      offset >= BLOCK_SIZE) {
    return res_buf;
  }
  if (offset + len > BLOCK_SIZE) {
    len = BLOCK_SIZE - offset;
  }
  const auto KVersionPerBlock = BLOCK_SIZE / sizeof(version_t);
  const auto version_block_id = block_id / KVersionPerBlock;
  const auto version_in_block_idx = block_id % KVersionPerBlock;
  std::vector<u8> version_buf(BLOCK_SIZE);
  auto read_version_block_res =
      block_allocator_->bm->read_block(version_block_id, version_buf.data());
  if (read_version_block_res.is_err()) {
    return res_buf;
  }
  auto version_arr = reinterpret_cast<version_t *>(version_buf.data());
  if (version_arr[version_in_block_idx] != version) {
    return res_buf;
  }
  std::vector<u8> block_data(BLOCK_SIZE);
  auto result = block_allocator_->bm->read_block(block_id, block_data.data());
  if (result.is_err()) {
    return res_buf;
  }
  res_buf.resize(len);
  std::copy(block_data.begin() + offset, block_data.begin() + offset + len,
            res_buf.begin());
  return res_buf;
}

// {Your code here}
auto DataServer::write_data(block_id_t block_id, usize offset,
                            std::vector<u8> &buffer) -> bool {
  auto write_result = this->block_allocator_->bm->write_partial_block(
      block_id, buffer.data(), offset, buffer.size());
  if (write_result.is_err()) {
    return false;
  }
  return true;
}

// {Your code here}
auto DataServer::alloc_block() -> std::pair<block_id_t, version_t> {
  auto result = this->block_allocator_->allocate();
  if (result.is_err()) {
    return {0, 0};
  }
  auto block_id = result.unwrap();
  const auto block_size = block_allocator_->bm->block_size();
  const auto version_per_block = block_size / sizeof(version_t);
  auto version_block_id = block_id / version_per_block;
  auto version_block_offset = block_id % version_per_block;
  std::vector<u8> buffer(block_size);
  auto read_res =
      block_allocator_->bm->read_block(version_block_id, buffer.data());
  if (read_res.is_err()) {
    return {block_id, 0};
  }
  auto version_p = reinterpret_cast<version_t *>(buffer.data());
  auto new_version = version_p[version_block_offset] + 1;
  version_p[version_block_offset] = new_version;
  auto write_res = block_allocator_->bm->write_partial_block(
      version_block_id,
      reinterpret_cast<u8 *>(&version_p[version_block_offset]),
      version_block_offset * sizeof(version_t), sizeof(version_t));
  if (write_res.is_err()) {
    return {block_id, new_version - 1};
  }
  return {block_id, new_version};
}

auto DataServer::free_block(block_id_t block_id) -> bool {
  const auto BLOCK_SIZE = block_allocator_->bm->block_size();
  auto deallocate_res = this->block_allocator_->deallocate(block_id);
  if (deallocate_res.is_err()) {
    return false;
  }
  const auto KVersionPerBlock = BLOCK_SIZE / sizeof(version_t);
  const auto version_block_id = block_id / KVersionPerBlock;
  const auto version_in_block_idx = block_id % KVersionPerBlock;
  std::vector<u8> version_buf(BLOCK_SIZE);
  auto read_version_block_res =
      block_allocator_->bm->read_block(version_block_id, version_buf.data());
  if (read_version_block_res.is_err()) {
    return false;
  }
  auto version_arr = reinterpret_cast<version_t *>(version_buf.data());
  auto new_version = version_arr[version_in_block_idx] + 1;
  version_arr[version_in_block_idx] = new_version;
  auto write_version_block_res =
      block_allocator_->bm->write_block(version_block_id, version_buf.data());
  if (write_version_block_res.is_err()) {
    return false;
  }
  return true;
}
} // namespace chfs