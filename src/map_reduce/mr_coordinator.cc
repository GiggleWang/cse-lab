#include <bits/types/sig_atomic_t.h>
#include <mutex>
#include <stdio.h>
#include <stdlib.h>
#include <string>
#include <unistd.h>
#include <vector>

#include "map_reduce/protocol.h"

namespace mapReduce {
AskTaskReply Coordinator::askTask(int) {
  // Lab4 : Your code goes here.
  // Free to change the type of return value.
  std::unique_lock<std::mutex> uniqueLock(this->mtx);
  if (this->findFirstNotState(this->map_state, FINISH) != -1) {
    // 说明MAP还没有结束
    auto find_res = this->findFirstNotState(this->map_state, START);
    if (find_res != -1) {
      this->map_state[find_res] = START;
      std::cout<<"MAP "<<find_res<<" start\n";
      return AskTaskReply::CreateMapTask(find_res, files[find_res]);
    } else {
      return AskTaskReply::CreateNoneTask();
    }
  }
  std::cout<<"==========MAP finished============\n";
  if (this->findFirstNotState(this->reduce_state, FINISH) != -1) {
    // 说明REDUCE还没有结束
    auto find_res = this->findFirstNotState(this->reduce_state, START);
    if (find_res != -1) {
      this->reduce_state[find_res] = START;
      std::cout<<"REDUCE "<<find_res<<" start\n";
      return AskTaskReply::CreateReduceTask(find_res, nReduce, files.size());
    } else {
      return AskTaskReply::CreateNoneTask();
    }
  }
  std::cout<<"==========REDUCE finished============\n";
  if (this->merge_state != FINISH) {
    if (this->merge_state != START) {
    std::cout<<"~~~~~~~~~~~~merge start~~~~~~~~~~~~~~~~~\n";
      this->merge_state = START;
      return AskTaskReply::CreateMergeTask(this->nReduce);
    } else {
      return AskTaskReply::CreateNoneTask();
    }
  }
  std::cout << "[askTask]error\n";
  return AskTaskReply::CreateNoneTask();
}

int Coordinator::submitTask(int taskType, int index) {
  // Lab4 : Your code goes here.
  std::unique_lock<std::mutex> uniqueLock(this->mtx);
  switch (taskType) {
  case MAP: {
    this->map_state[index] = FINISH;
    break;
  }
  case REDUCE: {
    this->reduce_state[index] = FINISH;
    break;
  }
  case MERGE: {
    this->merge_state = FINISH;
    this->isFinished = true;
    break;
  }
  }
  return 0;
}

// mr_coordinator calls Done() periodically to find out
// if the entire job has finished.
bool Coordinator::Done() {
  std::unique_lock<std::mutex> uniqueLock(this->mtx);
  return this->isFinished;
}

// create a Coordinator.
// nReduce is the number of reduce tasks to use.
Coordinator::Coordinator(MR_CoordinatorConfig config,
                         const std::vector<std::string> &files, int nReduce) {
  this->files = files;
  this->isFinished = false;
  // Lab4: Your code goes here (Optional).
  this->nReduce = nReduce;
  this->map_state.resize(files.size(), UNSTART);
  this->reduce_state.resize(nReduce, UNSTART);
  this->merge_state = UNSTART;
  rpc_server =
      std::make_unique<chfs::RpcServer>(config.ip_address, config.port);
  rpc_server->bind(ASK_TASK, [this](int i) { return this->askTask(i); });
  rpc_server->bind(SUBMIT_TASK, [this](int taskType, int index) {
    return this->submitTask(taskType, index);
  });
  rpc_server->run(true, 1);
}
} // namespace mapReduce