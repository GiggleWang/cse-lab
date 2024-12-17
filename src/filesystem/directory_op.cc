#include <algorithm>
#include <sstream>

#include "filesystem/directory_op.h"

namespace chfs {

/**
 * Some helper functions
 */
auto string_to_inode_id(std::string &data) -> inode_id_t {
  std::stringstream ss(data);
  inode_id_t inode;
  ss >> inode;
  return inode;
}

auto inode_id_to_string(inode_id_t id) -> std::string {
  std::stringstream ss;
  ss << id;
  return ss.str();
}

// {Your code here}
auto dir_list_to_string(const std::list<DirectoryEntry> &entries)
    -> std::string {
  std::ostringstream oss;
  usize cnt = 0;
  for (const auto &entry : entries) {
    oss << entry.name << ':' << entry.id;
    if (cnt < entries.size() - 1) {
      oss << '/';
    }
    cnt += 1;
  }
  return oss.str();
}

// {Your code here}
auto append_to_directory(std::string src, std::string filename, inode_id_t id)
    -> std::string {
  //       Append the new directory entry to `src`.
  if (!src.empty()) {
    src += '/';
  }
  src += filename + ':' + inode_id_to_string(id);
  return src;
}
void parse_directory(std::string &src, std::list<DirectoryEntry> &list) {
  std::istringstream iss(src);
  std::string entry;

  while (std::getline(iss, entry, '/')) {
    auto delimiter_pos = entry.find(':');
    if (delimiter_pos != std::string::npos) {
      std::string name = entry.substr(0, delimiter_pos);
      std::string inode_str = entry.substr(delimiter_pos + 1);
      inode_id_t id = string_to_inode_id(inode_str);
      list.emplace_back(DirectoryEntry{name, id});
    }
  }
}

// {Your code here}
auto rm_from_directory(std::string src, std::string filename) -> std::string {

  auto res = std::string("");
  //       Remove the directory entry from `src`.
  std::list<DirectoryEntry> list;
  parse_directory(src, list);

  list.remove_if([&filename](const DirectoryEntry &entry) {
    return entry.name == filename;
  });

  return dir_list_to_string(list);
}
/**
 * { Your implementation here }
 */
auto read_directory(FileOperation *fs, inode_id_t id,
                    std::list<DirectoryEntry> &list) -> ChfsNullResult {
  auto read_result = fs->read_file(id); // 读取目录文件内容
  if (!read_result.is_ok()) {
    return read_result.unwrap_error(); // 如果读取失败，返回错误
  }
  auto res = read_result.unwrap();
  std::string directory_content(res.begin(),
                                res.end()); // 将读取到的字节内容转换为字符串
  parse_directory(directory_content, list); // 解析目录内容

  return KNullOk;
}

// {Your code here}
auto FileOperation::lookup(inode_id_t id, const char *name)
    -> ChfsResult<inode_id_t> {
  std::list<DirectoryEntry> entries;
  auto read_result = read_directory(this, id, entries); // 读取目录内容
  if (!read_result.is_ok()) {
    return ChfsResult<inode_id_t>(
        ErrorType::NotExist); // 如果目录读取失败，返回错误
  }

  for (const auto &entry : entries) {
    if (entry.name == name) { // 如果找到对应文件名，返回 inode ID
      return ChfsResult<inode_id_t>(entry.id);
    }
  }
  return ChfsResult<inode_id_t>(
      ErrorType::NotExist); // 如果没有找到，返回 NotExist 错误
}

// {Your code here}
auto FileOperation::mk_helper(inode_id_t id, const char *name, InodeType type)
    -> ChfsResult<inode_id_t> {
  // 1. Check if `name` already exists in the parent.
  //    If already exist, return ErrorType::AlreadyExist.
  // 2. Create the new inode.
  // 3. Append the new entry to the parent directory.
  auto lookup_result = lookup(id, name);
  if (lookup_result.is_ok()) {
    return ChfsResult<inode_id_t>(
        ErrorType::AlreadyExist); // 如果文件名已存在，返回 AlreadyExist 错误
  }
  auto inode_result = alloc_inode(type);
  if (!inode_result.is_ok()) {
    return inode_result.unwrap_error(); // 分配 inode 失败时返回错误
  }
  std::list<DirectoryEntry> list;
  auto read_res = read_directory(this, id, list);
  if (read_res.is_err()) {
    return read_res.unwrap_error();
  }
  auto dir_str = dir_list_to_string(list);
  auto inode_id = inode_result.unwrap();
  auto new_dir_str = append_to_directory(dir_str, name, inode_id);

  std::vector<u8> content(new_dir_str.begin(), new_dir_str.end());
  auto write_res = write_file(id, content); // 将更新后的目录内容写回
  if (write_res.is_err()) {
    return write_res.unwrap_error();
  }
  return ChfsResult<inode_id_t>(
      static_cast<inode_id_t>(inode_id)); // 返回新创建的 inode ID
}

// {Your code here}
auto FileOperation::unlink(inode_id_t parent, const char *name)
    -> ChfsNullResult {

  // 1. Remove the file, you can use the function `remove_file`
  // 2. Remove the entry from the directory.
  // 1. 删除文件，使用 remove_file 函数
  auto lookup_result = lookup(parent, name);
  if (!lookup_result.is_ok()) {
    return lookup_result.unwrap_error(); // 如果文件不存在，返回 ENOENT
  }

  inode_id_t file_inode = lookup_result.unwrap();
  auto remove_result = remove_file(file_inode); // 删除文件
  if (!remove_result.is_ok()) {
    return remove_result.unwrap_error(); // 删除文件失败时返回错误
  }

  // 2. 从目录中移除条目
  std::list<DirectoryEntry> list;
  auto read_res = read_directory(this, parent, list);
  if (read_res.is_err()) {
    return ChfsNullResult(read_res.unwrap_error());
  }
  auto new_dir = rm_from_directory(dir_list_to_string(list), name);
  auto write_res =
      write_file(parent, std::vector<u8>(new_dir.begin(), new_dir.end()));
  if (write_res.is_err()) {
    return ChfsNullResult(write_res.unwrap_error());
  }
  return KNullOk;
}

} // namespace chfs
