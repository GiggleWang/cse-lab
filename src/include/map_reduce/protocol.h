#include "distributed/client.h"
#include "librpc/client.h"
#include "librpc/server.h"
#include <mutex>
#include <string>
#include <utility>
#include <vector>

// Lab4: Free to modify this file

namespace mapReduce {
struct AskTaskReply;
struct KeyVal {
  KeyVal(const std::string &key, const std::string &val) : key(key), val(val) {}
  KeyVal() {}
  std::string key;
  std::string val;
};

enum mr_tasktype { NONE = 0, MAP, REDUCE, MERGE };

std::vector<KeyVal> Map(const std::string &content);

std::string Reduce(const std::string &key,
                   const std::vector<std::string> &values);

const std::string ASK_TASK = "ask_task";
const std::string SUBMIT_TASK = "submit_task";

struct MR_CoordinatorConfig {
  uint16_t port;
  std::string ip_address;
  std::string resultFile;
  std::shared_ptr<chfs::ChfsClient> client;

  MR_CoordinatorConfig(std::string ip_address, uint16_t port,
                       std::shared_ptr<chfs::ChfsClient> client,
                       std::string resultFile)
      : port(port), ip_address(std::move(ip_address)), resultFile(resultFile),
        client(std::move(client)) {}
};

class SequentialMapReduce {
public:
  SequentialMapReduce(std::shared_ptr<chfs::ChfsClient> client,
                      const std::vector<std::string> &files,
                      std::string resultFile);
  void doWork();

private:
  std::shared_ptr<chfs::ChfsClient> chfs_client;
  std::vector<std::string> files;
  std::string outPutFile;
  void write_to_output_file(std::string out_res);
  std::string get_from_file(std::string file);
};
enum MyState { UNSTART, START, FINISH };
class Coordinator {
public:
  Coordinator(MR_CoordinatorConfig config,
              const std::vector<std::string> &files, int nReduce);
  AskTaskReply askTask(int);
  int submitTask(int taskType, int index);
  bool Done();

private:
  std::vector<std::string> files;
  std::mutex mtx;
  bool isFinished;
  std::unique_ptr<chfs::RpcServer> rpc_server;
  std::vector<MyState> map_state;
  std::vector<MyState> reduce_state;
  MyState merge_state;
  int nReduce;
  int findFirstNotState(const std::vector<MyState> &states, MyState target) {
    for (int i = 0; i < states.size(); i++) {
      if (states[i] < target) {
        return i;
      }
    }
    return -1;
  }
};

class Worker {
public:
  explicit Worker(MR_CoordinatorConfig config);
  void doWork();
  void stop();

private:
  void doMap(int index, const std::string &filename);
  void doReduce(int index, int nfiles);
  void doSubmit(mr_tasktype taskType, int index);
  void doMerge(int nReduce);
  void write_to_file(int node_id, std::string out_res);
  std::string get_from_file(std::string file);
  int n_reduce;
  std::string outPutFile;
  std::unique_ptr<chfs::RpcClient> mr_client;
  std::shared_ptr<chfs::ChfsClient> chfs_client;
  std::unique_ptr<std::thread> work_thread;
  bool shouldStop = false;
};
struct AskTaskReply {
  int type;
  int index = -1;
  std::string filename;
  int n_reduce = -1;
  int n_file = -1;
public:
  AskTaskReply() = default;

  // 添加 msgpack 序列化支持
  MSGPACK_DEFINE(type, index, filename, n_reduce, n_file);
  // 静态工厂方法
  static AskTaskReply CreateMapTask(int index, std::string filename) {
    AskTaskReply reply;
    reply.type = mapReduce::mr_tasktype::MAP;
    reply.index = index;
    reply.filename = std::move(filename);
    return reply;
  }

  static AskTaskReply CreateReduceTask(int index, int n_reduce, int n_file) {
    AskTaskReply reply;
    reply.type = mapReduce::mr_tasktype::REDUCE;
    reply.index = index;
    reply.n_reduce = n_reduce;
    reply.n_file = n_file;
    return reply;
  }

  static AskTaskReply CreateMergeTask(int n_reduce) {
    AskTaskReply reply;
    reply.type = mapReduce::mr_tasktype::MERGE;
    reply.n_reduce = n_reduce;
    return reply;
  }

  static AskTaskReply CreateNoneTask() {
    AskTaskReply reply;
    reply.type = mapReduce::mr_tasktype::NONE;
    return reply;
  }
};
} // namespace mapReduce