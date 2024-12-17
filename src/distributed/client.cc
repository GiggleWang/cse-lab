#include "distributed/client.h"
#include "common/macros.h"
#include "common/util.h"
#include "distributed/metadata_server.h"

namespace chfs {

ChfsClient::ChfsClient() : num_data_servers(0) {}

auto ChfsClient::reg_server(ServerType type, const std::string &address,
                            u16 port, bool reliable) -> ChfsNullResult {
  switch (type) {
  case ServerType::DATA_SERVER:
    num_data_servers += 1;
    data_servers_.insert({num_data_servers, std::make_shared<RpcClient>(
                                                address, port, reliable)});
    break;
  case ServerType::METADATA_SERVER:
    metadata_server_ = std::make_shared<RpcClient>(address, port, reliable);
    break;
  default:
    std::cerr << "Unknown Type" << std::endl;
    exit(1);
  }

  return KNullOk;
}

// {Your code here}
auto ChfsClient::mknode(FileType type, inode_id_t parent,
                        const std::string &name) -> ChfsResult<inode_id_t> {
  auto mknode_res = this->metadata_server_->call(
      "mknode", static_cast<u8>(type), parent, name);
  if (mknode_res.is_err()) {
    return mknode_res.unwrap_error();
  }
  auto mknode_id = mknode_res.unwrap()->as<inode_id_t>();
  if (mknode_id == KInvalidInodeID) {
    return ErrorType::INVALID;
  }
  return ChfsResult<inode_id_t>(mknode_id);
}

// {Your code here}
auto ChfsClient::unlink(inode_id_t parent, std::string const &name)
    -> ChfsNullResult {
  auto unlink_res = this->metadata_server_->call("unlink", parent, name);
  if (unlink_res.is_err()) {
    return unlink_res.unwrap_error();
  }
  auto unlink_ok = unlink_res.unwrap()->as<bool>();
  if (!unlink_ok) {
    return ErrorType::DONE;
  }
  return KNullOk;
}

// {Your code here}
auto ChfsClient::lookup(inode_id_t parent, const std::string &name)
    -> ChfsResult<inode_id_t> {
  auto lookup_res = metadata_server_->call("lookup", parent, name);
  if (lookup_res.is_err()) {
    return lookup_res.unwrap_error();
  }
  auto inode_id = lookup_res.unwrap()->as<inode_id_t>();
  // if (inode_id == KInvalidInodeID) {
  //   return ChfsResult<inode_id_t>(ErrorType::INVALID);
  // }
  return ChfsResult<inode_id_t>(inode_id);
}

// {Your code here}
auto ChfsClient::readdir(inode_id_t id)
    -> ChfsResult<std::vector<std::pair<std::string, inode_id_t>>> {
  auto readdir_res = metadata_server_->call("readdir", id);
  if (readdir_res.is_err()) {
    return readdir_res.unwrap_error();
  }
  auto pair_vec = readdir_res.unwrap()
                      ->as<std::vector<std::pair<std::string, inode_id_t>>>();
  return ChfsResult<std::vector<std::pair<std::string, inode_id_t>>>(pair_vec);
}

// {Your code here}
auto ChfsClient::get_type_attr(inode_id_t id)
    -> ChfsResult<std::pair<InodeType, FileAttr>> {
  auto get_type_attr_res = metadata_server_->call("get_type_attr", id);
  if (get_type_attr_res.is_err()) {
    return get_type_attr_res.unwrap_error();
  }
  auto [size, atime, mtime, ctime, type] =
      get_type_attr_res.unwrap()->as<std::tuple<u64, u64, u64, u64, u8>>();
  InodeType ret_type = InodeType::Unknown;
  if (type == DirectoryType) {
    ret_type = InodeType::Directory;
  }
  if (type == RegularFileType) {
    ret_type = InodeType::FILE;
  }
  return ChfsResult<std::pair<InodeType, FileAttr>>(
      {ret_type, FileAttr{atime, mtime, ctime, size}});
}

/**
 * Read and Write operations are more complicated.
 */
// {Your code here}
auto ChfsClient::read_file(inode_id_t id, usize offset, usize size)
    -> ChfsResult<std::vector<u8>> {
  std::vector<u8> ret_vec;
  auto block_map_res = metadata_server_->call("get_block_map", id);
  if (block_map_res.is_err()) {
    return block_map_res.unwrap_error();
  }
  auto block_map = block_map_res.unwrap()->as<std::vector<chfs::BlockInfo>>();
  auto block_size = DiskBlockSize;
  auto start_block_id = offset / block_size;
  auto start_block_offset = offset % block_size;
  auto end_block_id = (offset + size) / block_size;
  auto end_block_offset = (offset + size) % block_size;
  if (end_block_offset == 0) {
    end_block_id--;
    end_block_offset = block_size;
  }
  if (end_block_id > block_map.size()) {
    return ErrorType::INVALID_ARG;
  }
  for (auto iterator = block_map.begin() + start_block_id;
       iterator != block_map.begin() + end_block_id + 1; iterator++) {
    block_id_t block_id = std::get<0>(*iterator);
    mac_id_t mac_id = std::get<1>(*iterator);
    version_t version_id = std::get<2>(*iterator);
    auto target_mac_find = this->data_servers_.find(mac_id);
    if (target_mac_find == this->data_servers_.end()) {
      return ErrorType::INVALID;
    }
    auto target_mac = target_mac_find->second;
    auto read_res =
        target_mac->call("read_data", block_id, 0, block_size, version_id);
    if (read_res.is_err()) {
      return read_res.unwrap_error();
    }
    auto read_vector = read_res.unwrap()->as<std::vector<u8>>();
    if (start_block_id == end_block_id) {
      ret_vec.insert(ret_vec.end(), read_vector.begin() + start_block_offset,
                     read_vector.begin() + end_block_offset);
    } else {
      if (iterator == block_map.begin() + start_block_id) {
        ret_vec.insert(ret_vec.end(), read_vector.begin() + start_block_offset,
                       read_vector.end());
      }
      if (iterator == block_map.begin() + end_block_id) {
        ret_vec.insert(ret_vec.end(), read_vector.begin(),
                       read_vector.begin() + end_block_offset);
      }
      if (iterator != block_map.begin() + start_block_id &&
          iterator != block_map.begin() + end_block_id) {
        ret_vec.insert(ret_vec.end(), read_vector.begin(), read_vector.end());
      }
    }
  }
  if (ret_vec.size() != size) {
    return ErrorType::INVALID;
  }
  return ChfsResult<std::vector<u8>>(ret_vec);
}

// {Your code here}
auto ChfsClient::write_file(inode_id_t id, usize offset, std::vector<u8> data)
    -> ChfsNullResult {
  auto block_map_res = metadata_server_->call("get_block_map", id);
  if (block_map_res.is_err()) {
    return block_map_res.unwrap_error();
  }
  auto size = data.size();
  auto block_map = block_map_res.unwrap()->as<std::vector<chfs::BlockInfo>>();
  auto block_size = DiskBlockSize;
  auto start_block_id = offset / block_size;
  auto start_block_offset = offset % block_size;
  auto end_block_id = (offset + size) / block_size;
  auto end_block_offset = (offset + size) % block_size;
  if (end_block_offset == 0) {
    end_block_id--;
    end_block_offset = block_size;
  }
  if (end_block_id >= block_map.size()) {
    auto map_size = block_map.size();
    for (int i = map_size; i <= end_block_id; i++) {
      // allocate block
      auto alloc_res = metadata_server_->call("alloc_block", id);
      if (alloc_res.is_err()) {
        return alloc_res.unwrap_error();
      }
      auto [block_id, mac_id, version_id] = alloc_res.unwrap()->as<BlockInfo>();
      if (block_id == KInvalidBlockID) {
        return ErrorType::INVALID;
      }
      block_map.push_back({block_id, mac_id, version_id});
    }
  }
  auto write_offset = 0;
  for (auto iterator = block_map.begin() + start_block_id;
       iterator != block_map.begin() + end_block_id + 1; iterator++) {
    block_id_t block_id = std::get<0>(*iterator);
    mac_id_t mac_id = std::get<1>(*iterator);
    // version_t version_id = std::get<2>(*iterator);
    auto target_mac_find = this->data_servers_.find(mac_id);
    if (target_mac_find == this->data_servers_.end()) {
      return ErrorType::INVALID;
    }
    auto target_mac = target_mac_find->second;
    usize w_start = 0, w_end = block_size;
    if (iterator == block_map.begin() + start_block_id) {
      w_start = start_block_offset;
    }
    if (iterator == block_map.begin() + end_block_id) {
      w_end = end_block_offset;
    }
    std::vector<u8> buffer =
        std::vector<u8>(data.begin() + write_offset,
                        data.begin() + write_offset + w_end - w_start);
    auto write_res = target_mac->call("write_data", block_id, w_start, buffer);
    if (write_res.is_err())
      return write_res.unwrap_error();
    auto ok = write_res.unwrap()->as<bool>();
    if (!ok)
      return ErrorType::DONE;
    write_offset += (w_end - w_start);
  }
  return KNullOk;
}

// {Your code here}
auto ChfsClient::free_file_block(inode_id_t id, block_id_t block_id,
                                 mac_id_t mac_id) -> ChfsNullResult {
  auto free_file_block_res =
      metadata_server_->call("free_block", id, block_id, mac_id);
  if (free_file_block_res.is_err()) {
    return free_file_block_res.unwrap_error();
  }
  auto is_success = free_file_block_res.unwrap()->as<bool>();
  if (!is_success) {
    return ChfsNullResult(ErrorType::INVALID);
  }
  return KNullOk;
}

} // namespace chfs