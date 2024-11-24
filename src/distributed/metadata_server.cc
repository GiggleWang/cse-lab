#include "distributed/metadata_server.h"
#include "common/util.h"
#include "filesystem/directory_op.h"
#include "metadata/inode.h"
#include "metadata/manager.h"
#include <bits/types/FILE.h>
#include <fstream>
#include <tuple>
#include <vector>

namespace chfs {

inline auto MetadataServer::bind_handlers() {
  server_->bind("mknode",
                [this](u8 type, inode_id_t parent, std::string const &name) {
                  return this->mknode(type, parent, name);
                });
  server_->bind("unlink", [this](inode_id_t parent, std::string const &name) {
    return this->unlink(parent, name);
  });
  server_->bind("lookup", [this](inode_id_t parent, std::string const &name) {
    return this->lookup(parent, name);
  });
  server_->bind("get_block_map",
                [this](inode_id_t id) { return this->get_block_map(id); });
  server_->bind("alloc_block",
                [this](inode_id_t id) { return this->allocate_block(id); });
  server_->bind("free_block",
                [this](inode_id_t id, block_id_t block, mac_id_t machine_id) {
                  return this->free_block(id, block, machine_id);
                });
  server_->bind("readdir", [this](inode_id_t id) { return this->readdir(id); });
  server_->bind("get_type_attr",
                [this](inode_id_t id) { return this->get_type_attr(id); });
}

inline auto MetadataServer::init_fs(const std::string &data_path) {
  /**
   * Check whether the metadata exists or not.
   * If exists, we wouldn't create one from scratch.
   */
  bool is_initialed = is_file_exist(data_path);

  auto block_manager = std::shared_ptr<BlockManager>(nullptr);
  if (is_log_enabled_) {
    block_manager =
        std::make_shared<BlockManager>(data_path, KDefaultBlockCnt, true);
  } else {
    block_manager = std::make_shared<BlockManager>(data_path, KDefaultBlockCnt);
  }

  CHFS_ASSERT(block_manager != nullptr, "Cannot create block manager.");

  if (is_initialed) {
    auto origin_res = FileOperation::create_from_raw(block_manager);
    std::cout << "Restarting..." << std::endl;
    if (origin_res.is_err()) {
      std::cerr << "Original FS is bad, please remove files manually."
                << std::endl;
      exit(1);
    }

    operation_ = origin_res.unwrap();
  } else {
    operation_ = std::make_shared<FileOperation>(block_manager,
                                                 DistributedMaxInodeSupported);
    std::cout << "We should init one new FS..." << std::endl;
    /**
     * If the filesystem on metadata server is not initialized, create
     * a root directory.
     */
    auto init_res = operation_->alloc_inode(InodeType::Directory);
    if (init_res.is_err()) {
      std::cerr << "Cannot allocate inode for root directory." << std::endl;
      exit(1);
    }

    CHFS_ASSERT(init_res.unwrap() == 1, "Bad initialization on root dir.");
  }

  running = false;
  num_data_servers =
      0; // Default no data server. Need to call `reg_server` to add.

  if (is_log_enabled_) {
    if (may_failed_)
      operation_->block_manager_->set_may_fail(true);
    commit_log = std::make_shared<CommitLog>(operation_->block_manager_,
                                             is_checkpoint_enabled_);
  }

  bind_handlers();

  /**
   * The metadata server wouldn't start immediately after construction.
   * It should be launched after all the data servers are registered.
   */
}

MetadataServer::MetadataServer(u16 port, const std::string &data_path,
                               bool is_log_enabled, bool is_checkpoint_enabled,
                               bool may_failed)
    : is_log_enabled_(is_log_enabled), may_failed_(may_failed),
      is_checkpoint_enabled_(is_checkpoint_enabled) {
  server_ = std::make_unique<RpcServer>(port);
  init_fs(data_path);
  if (is_log_enabled_) {
    commit_log = std::make_shared<CommitLog>(operation_->block_manager_,
                                             is_checkpoint_enabled);
  }
}

MetadataServer::MetadataServer(std::string const &address, u16 port,
                               const std::string &data_path,
                               bool is_log_enabled, bool is_checkpoint_enabled,
                               bool may_failed)
    : is_log_enabled_(is_log_enabled), may_failed_(may_failed),
      is_checkpoint_enabled_(is_checkpoint_enabled) {
  server_ = std::make_unique<RpcServer>(address, port);
  init_fs(data_path);
  if (is_log_enabled_) {
    commit_log = std::make_shared<CommitLog>(operation_->block_manager_,
                                             is_checkpoint_enabled);
  }
}

// {Your code here}
auto MetadataServer::mknode(u8 type, inode_id_t parent, const std::string &name)
    -> inode_id_t {
  std::lock_guard<std::mutex> lock(mtx);
  InodeType inode_type = InodeType::Unknown;
  if (type == DirectoryType) {
    inode_type = InodeType::Directory;
  }
  if (type == RegularFileType) {
    inode_type = InodeType::FILE;
  }
  if (inode_type == InodeType::Unknown) {
    return KInvalidInodeID;
  }
  auto mk_res = this->operation_->mk_helper(parent, name.data(), inode_type);
  if (mk_res.is_err()) {
    return KInvalidInodeID;
  }
  return mk_res.unwrap();
}

// {Your code here}
auto MetadataServer::unlink(inode_id_t parent, const std::string &name)
    -> bool {
  std::lock_guard<std::mutex> lock(mtx);
  auto type_res = this->operation_->gettype(parent);
  if (type_res.is_err()) {
    return false;
  }
  auto type = type_res.unwrap();
  if (type == InodeType::Directory) {
    auto look_res = this->operation_->lookup(parent, name.data());
    if (look_res.is_err()) {
      return false;
    }
    auto res = this->operation_->unlink(parent, name.data());
    if (res.is_err()) {
      return false;
    }
    return true;
  }
  return false;
}

// {Your code here}
auto MetadataServer::lookup(inode_id_t parent, const std::string &name)
    -> inode_id_t {
  auto look_res = this->operation_->lookup(parent, name.data());
  if (look_res.is_err()) {
    return KInvalidInodeID;
  }
  return look_res.unwrap();
}

// {Your code here}
auto MetadataServer::get_block_map(inode_id_t id) -> std::vector<BlockInfo> {
  std::lock_guard<std::mutex> lock(mtx);
  if (this->operation_->inode_manager_->get_max_inode_supported() < id ||
      id == KInvalidBlockID) {
    return {};
  }
  // read the inode
  auto inode_res = this->operation_->inode_manager_->get(id);
  if (inode_res.is_err()) {
    return {};
  }
  block_id_t inode_block_id = inode_res.unwrap();
  const auto block_size = this->operation_->block_manager_->block_size();
  std::vector<u8> inode(block_size);
  auto block_res = this->operation_->block_manager_->read_block(inode_block_id,
                                                                inode.data());
  if (block_res.is_err()) {
    return {};
  }
  auto inode_ptr = reinterpret_cast<Inode *>(inode.data());
  if (inode_ptr->get_type() != InodeType::FILE) {
    return {};
  }
  //遍历block数组
  auto nblocks = inode_ptr->get_nblocks();
  int i = 0;
  std::vector<BlockInfo> ret_blockinfo;
  for (; i < nblocks; i += 2) {
    if (inode_ptr->blocks[i] == KInvalidInodeID) {
      break;
    }
    auto block_id = inode_ptr->blocks[i];
    auto mac_id = inode_ptr->blocks[i + 1];
    // auto version_id = 0; /*will be implemented later*/
    auto target_mac_find = clients_.find(mac_id);
    if (target_mac_find == clients_.end()) {
      return {};
    }
    auto KVersionPerBlock = block_size / sizeof(version_t);
    auto version_block_id = block_id / KVersionPerBlock;
    auto version_in_block_idx = block_id % KVersionPerBlock;
    auto read_res = target_mac_find->second->call(
        "read_data", version_block_id, version_in_block_idx * sizeof(version_t),
        sizeof(version_t), 0);
    if (read_res.is_err()) {
      return {};
    }
    auto read_vec = read_res.unwrap()->as<std::vector<u8>>();
    auto version_ptr = reinterpret_cast<version_t *>(read_vec.data());
    version_t version_id = *version_ptr;
    ret_blockinfo.push_back({block_id, mac_id, version_id});
  }
  return ret_blockinfo;
}

// {Your code here}
auto MetadataServer::allocate_block(inode_id_t id) -> BlockInfo {
  std::lock_guard<std::mutex> lock(mtx);
  BlockInfo invalidBlockInfo = {KInvalidBlockID, 0, 0};

  if (this->operation_->inode_manager_->get_max_inode_supported() < id ||
      id == KInvalidBlockID) {
    return invalidBlockInfo;
  }

  // read the inode
  auto inode_res = this->operation_->inode_manager_->get(id);
  if (inode_res.is_err()) {
    return invalidBlockInfo;
  }
  block_id_t inode_block_id = inode_res.unwrap();
  const auto block_size = this->operation_->block_manager_->block_size();
  std::vector<u8> inode(block_size);
  auto block_res = this->operation_->block_manager_->read_block(inode_block_id,
                                                                inode.data());
  if (block_res.is_err()) {
    return invalidBlockInfo;
  }
  auto inode_ptr = reinterpret_cast<Inode *>(inode.data());
  if (inode_ptr->get_type() != InodeType::FILE) {
    return invalidBlockInfo;
  }

  //遍历block数组，查找是否还有多余的空闲块
  auto nblocks = inode_ptr->get_nblocks();
  int i = 0;
  for (; i < nblocks; i += 2) {
    if (inode_ptr->blocks[i] == KInvalidBlockID) {
      break;
    }
  }
  if (i > nblocks) {
    std::cout << "no free nblocks" << std::endl;
    return invalidBlockInfo;
  }

  //分配block
  auto clients_size = clients_.size();
  auto random_client = generator.rand(0, clients_size - 1);
  auto it = clients_.begin();
  std::advance(it, random_client);
  auto mac_id = it->first;
  std::shared_ptr<RpcClient> target_mac = it->second;
  auto alloc_res = target_mac->call("alloc_block");
  if (alloc_res.is_err()) {
    return invalidBlockInfo;
  }
  auto [block_id, version_id] =
      alloc_res.unwrap()->as<std::pair<chfs::block_id_t, chfs::version_t>>();
  if (block_id == KInvalidBlockID) {
    return invalidBlockInfo;
  }
  inode_ptr->set_block_direct(i, block_id);
  inode_ptr->set_block_direct(i + 1, mac_id);
  inode_ptr->set_size(inode_ptr->get_size() + block_size);
  auto write_res = this->operation_->block_manager_->write_block(inode_block_id,
                                                                 inode.data());
  if (write_res.is_err()) {
    return invalidBlockInfo;
  }
  return BlockInfo(block_id, mac_id, version_id);
}

// {Your code here}
auto MetadataServer::free_block(inode_id_t id, block_id_t block_id,
                                mac_id_t machine_id) -> bool {
  std::lock_guard<std::mutex> lock(mtx);
  if (this->operation_->inode_manager_->get_max_inode_supported() < id ||
      id == KInvalidBlockID) {
    return false;
  }
  auto inode_res = this->operation_->inode_manager_->get(id);
  if (inode_res.is_err()) {
    return false;
  }
  block_id_t inode_block_id = inode_res.unwrap();
  const auto block_size = this->operation_->block_manager_->block_size();
  std::vector<u8> inode(block_size);
  auto block_res = this->operation_->block_manager_->read_block(inode_block_id,
                                                                inode.data());
  if (block_res.is_err()) {
    return false;
  }
  auto inode_ptr = reinterpret_cast<Inode *>(inode.data());
  if (inode_ptr->get_type() != InodeType::FILE) {
    return false;
  }

  //遍历数组，查找需要delete的块
  auto nblocks = inode_ptr->get_nblocks();
  int i = 0;
  for (; i < nblocks; i += 2) {
    if (inode_ptr->blocks[i] == block_id &&
        inode_ptr->blocks[i + 1] == machine_id) {
      break;
    }
  }
  if (i > nblocks) {
    std::cout << "the block to free not found" << std::endl;
    return false;
  }

  auto target = this->clients_.find(machine_id);
  if (target == this->clients_.end()) {
    return false;
  }
  auto target_mac = target->second;
  auto delete_res = target_mac->call("free_block", block_id);
  if (delete_res.is_err()) {
    return false;
  }
  auto res = delete_res.unwrap()->as<bool>();
  if (!res) {
    return false;
  }
  //把后面的block往前挪
  for (i += 2; i < inode_ptr->get_nblocks(); i += 2) {
    inode_ptr->set_block_direct(i - 2, inode_ptr->blocks[i]);
    inode_ptr->set_block_direct(i - 1, inode_ptr->blocks[i + 1]);
  }
  inode_ptr->set_block_direct(i - 2, KInvalidBlockID);
  inode_ptr->set_block_direct(i - 1, KInvalidBlockID);
  inode_ptr->set_size(inode_ptr->get_size() - block_size);

  auto write_back = this->operation_->block_manager_->write_block(
      inode_block_id, inode.data());
  if (write_back.is_err()) {
    return false;
  }
  return true;
}

// {Your code here}
auto MetadataServer::readdir(inode_id_t node)
    -> std::vector<std::pair<std::string, inode_id_t>> {
  std::vector<std::pair<std::string, inode_id_t>> ret_vec;
  std::list<DirectoryEntry> list;
  auto read_res = read_directory(operation_.get(), node, list);
  if (read_res.is_err()) {
    return ret_vec;
  }
  for (const auto &entry : list) {
    std::pair<std::string, inode_id_t> vec_element(entry.name, entry.id);
    ret_vec.push_back(vec_element);
  }
  return ret_vec;
}

// {Your code here}
auto MetadataServer::get_type_attr(inode_id_t id)
    -> std::tuple<u64, u64, u64, u64, u8> {
  std::lock_guard<std::mutex> lock(mtx);
  std::tuple<u64, u64, u64, u64, u8> invalide_res = {0, 0, 0, 0, 0};
  auto type_attr_res = this->operation_->get_type_attr(id);
  if (type_attr_res.is_err()) {
    return invalide_res;
  }
  std::pair<InodeType, FileAttr> type_attr_pair = type_attr_res.unwrap();
  auto type = type_attr_pair.first;
  u8 ret_type = 0;
  if (type == InodeType::FILE) {
    ret_type = RegularFileType;
  }
  if (type == InodeType::Directory) {
    ret_type = DirectoryType;
  }
  auto attr = type_attr_pair.second;
  return {attr.size, attr.atime, attr.mtime, attr.ctime, ret_type};
}

auto MetadataServer::reg_server(const std::string &address, u16 port,
                                bool reliable) -> bool {
  std::lock_guard<std::mutex> lock(mtx);
  num_data_servers += 1;
  auto cli = std::make_shared<RpcClient>(address, port, reliable);
  clients_.insert(std::make_pair(num_data_servers, cli));

  return true;
}

auto MetadataServer::run() -> bool {
  if (running)
    return false;

  // Currently we only support async start
  server_->run(true, num_worker_threads);
  running = true;
  return true;
}

} // namespace chfs
