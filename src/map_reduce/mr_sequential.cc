#include <algorithm>
#include <string>
#include <utility>
#include <vector>

#include "map_reduce/protocol.h"

namespace mapReduce {
SequentialMapReduce::SequentialMapReduce(
    std::shared_ptr<chfs::ChfsClient> client,
    const std::vector<std::string> &files_, std::string resultFile) {
  chfs_client = std::move(client);
  files = files_;
  outPutFile = resultFile;
  // Your code goes here (optional)
}

void SequentialMapReduce::doWork() {
  // Your code goes here
  // Map
  std::vector<KeyVal> all_map;
  for (auto file : files) {
    auto inode = this->chfs_client->lookup(1, file).unwrap();
    auto inode_type_attr = this->chfs_client->get_type_attr(inode).unwrap();
    auto length = inode_type_attr.second.size;
    auto words = this->chfs_client->read_file(inode, 0, length).unwrap();
    std::string str;
    str.assign(words.begin(), words.end());
    auto map_res = Map(str);
    all_map.insert(all_map.end(), map_res.begin(), map_res.end());
  }
  //   sort

  std::sort(all_map.begin(), all_map.end(),
            [](const KeyVal &a, const KeyVal &b) { return a.key < b.key; });

  // reduce
  std::vector<KeyVal> reduced_results;
  std::string current_key = "";
  std::vector<std::string> current_values;

  for (const auto &kv : all_map) {
    if (kv.key != current_key && !current_key.empty()) {
      // Perform reduce on the current group
      auto reduced_value = Reduce(current_key, current_values);
      reduced_results.push_back(KeyVal{current_key, reduced_value});
      current_values.clear();
    }

    // Update key and values
    current_key = kv.key;
    current_values.push_back(kv.val);
  }

  // Perform reduce for the last group
  if (!current_values.empty()) {
    auto reduced_value = Reduce(current_key, current_values);
    reduced_results.push_back(KeyVal{current_key, reduced_value});
  }
  std::string out_res;
  for (auto result : reduced_results) {
    out_res.append(result.key + " " + result.val + '\n');
  }
  this->write_to_output_file(out_res);
}
void SequentialMapReduce::write_to_output_file(std::string out_res) {
  auto out_file = chfs_client->lookup(1, outPutFile).unwrap();
  std::vector<chfs::u8> content(out_res.begin(), out_res.end());
  chfs_client->write_file(out_file, 0, content);
}
} // namespace mapReduce