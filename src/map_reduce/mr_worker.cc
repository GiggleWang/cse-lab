#include <chrono>
#include <fstream>
#include <iostream>
#include <mutex>
#include <sstream>
#include <stdio.h>
#include <stdlib.h>
#include <string>
#include <thread>
#include <unistd.h>
#include <unordered_map>
#include <vector>

#include "distributed/client.h"
#include "map_reduce/protocol.h"

namespace mapReduce {
#define my_sleep(num)                                                          \
  std::this_thread::sleep_for(std::chrono::milliseconds(num))
Worker::Worker(MR_CoordinatorConfig config) {
  mr_client =
      std::make_unique<chfs::RpcClient>(config.ip_address, config.port, true);
  outPutFile = config.resultFile;
  chfs_client = config.client;
  work_thread = std::make_unique<std::thread>(&Worker::doWork, this);
  // Lab4: Your code goes here (Optional).
}

void Worker::doMap(int index, const std::string &filename) {
  // Lab4: Your code goes here.
  std::string str = this->get_from_file(filename);
  auto all_map = Map(str);
  std::sort(all_map.begin(), all_map.end(),
            [](const KeyVal &a, const KeyVal &b) { return a.key < b.key; });
  std::string out_res;
  for (auto result : all_map) {
    out_res.append(result.key + " " + result.val + '\n');
  }
  std::string my_name = "mr" + std::to_string(index);
  auto file_node =
      this->chfs_client->mknode(chfs::ChfsClient::FileType::REGULAR, 1, my_name)
          .unwrap();
  this->write_to_file(file_node, out_res);
  this->doSubmit(mapReduce::mr_tasktype::MAP, index);
}

void Worker::doReduce(int index, int nfiles) {
  // Lab4: Your code goes here.
  const int letters_total = 26;
  int chunk_size = (letters_total + this->n_reduce - 1) / this->n_reduce;
  int begin = std::min(chunk_size * index, letters_total);
  int end = std::min(begin + chunk_size, letters_total);
  auto reduce_inode = chfs_client
                          ->mknode(chfs::ChfsClient::FileType::REGULAR, 1,
                                   "rd-" + std::to_string(index))
                          .unwrap();
  std::vector<KeyVal> reduce_target;
  for (int i = 0; i < nfiles; ++i) {
    std::stringstream ss(this->get_from_file("mr" + std::to_string(i)));
    std::string key, value;
    while (ss >> key >> value) {
      if (!key.empty()) {
        char first_char = std::toupper(key[0]);
        if (first_char >= ('A' + begin) && first_char < ('A' + end)) {
          reduce_target.emplace_back(key, value);
        }
      }
    }
  }
  std::map<std::string, std::vector<std::string>> grouped_data;
  for (const auto &kv : reduce_target) {
    grouped_data[kv.key].push_back(kv.val);
  }
  std::stringstream output;
  for (const auto &[key, values] : grouped_data) {
    output << key << " " << Reduce(key, values) << " ";
  }
  std::string result = output.str();
  std::cout << "reduce in " << index << std::endl;
  std::cout << result.substr(0, 50) << "... (total length: " << result.length()
            << ")" << std::endl;
  this->write_to_file(reduce_inode, result);
  this->doSubmit(REDUCE, index);
}
void Worker::doMerge(int nReduce) {
  std::string merged_content; // 用于存储所有文件的内容

  // 收集所有文件的内容
  for (int i = nReduce - 1; i >= 0; i--) {
    std::string name = "rd-" + std::to_string(i);
    std::string file_in = this->get_from_file(name);
    merged_content += file_in; // 追加每个文件的内容
  }

  // 一次性写入合并后的内容
  this->write_to_file(this->chfs_client->lookup(1, outPutFile).unwrap(),
                      merged_content);

  this->doSubmit(MERGE, 0);
}

void Worker::doSubmit(mr_tasktype taskType, int index) {
  // Lab4: Your code goes here.
  mr_client->call(SUBMIT_TASK, static_cast<int>(taskType), index);
}

void Worker::stop() {
  shouldStop = true;
  work_thread->join();
}

void Worker::doWork() {
  while (!shouldStop) {
    // Lab4: Your code goes here.
    AskTaskReply reply =
        this->mr_client->call(ASK_TASK, 0).unwrap()->as<AskTaskReply>();
    auto type = reply.type;
    switch (type) {
    case NONE: {
      my_sleep(1);
      break;
    }
    case MAP: {
      this->doMap(reply.index, reply.filename);
      break;
    }
    case REDUCE: {
      this->n_reduce = reply.n_reduce;
      this->doReduce(reply.index, reply.n_file);
      break;
    }
    case MERGE: {
      this->n_reduce = reply.n_reduce;
      this->doMerge(reply.n_reduce);
      break;
    }
    }
  }
}
void Worker::write_to_file(int node_id, std::string out_res) {
  std::vector<chfs::u8> content(out_res.begin(), out_res.end());
  chfs_client->write_file(node_id, 0, content);
}
std::string Worker::get_from_file(std::string file) {
  auto inode = this->chfs_client->lookup(1, file).unwrap();
  auto inode_type_attr = this->chfs_client->get_type_attr(inode).unwrap();
  auto length = inode_type_attr.second.size;
  auto words = this->chfs_client->read_file(inode, 0, length).unwrap();
  std::string str;
  str.assign(words.begin(), words.end());
  return str;
}
} // namespace mapReduce