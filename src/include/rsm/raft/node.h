#pragma once

#include <algorithm>
#include <atomic>
#include <chrono>
#include <ctime>
#include <filesystem>
#include <memory>
#include <mutex>
#include <stdarg.h>
#include <thread>
#include <unistd.h>

#include "block/manager.h"
#include "librpc/client.h"
#include "librpc/server.h"
#include "rsm/config.h"
#include "rsm/raft/log.h"
#include "rsm/raft/protocol.h"
#include "rsm/state_machine.h"
#include "utils/thread_pool.h"

namespace chfs {

enum class RaftRole { Follower, Candidate, Leader };

struct RaftNodeConfig {
  int node_id;
  uint16_t port;
  std::string ip_address;
};

template <typename StateMachine, typename Command> class RaftNode {

#define RAFT_LOG(fmt, args...)                                                 \
  do {                                                                         \
    auto now = std::chrono::duration_cast<std::chrono::milliseconds>(          \
                   std::chrono::system_clock::now().time_since_epoch())        \
                   .count();                                                   \
    char buf[512];                                                             \
    sprintf(buf, "[%ld][%s:%d][node %d term %d role %d] " fmt "\n", now,       \
            __FILE__, __LINE__, my_id, current_term, role, ##args);            \
    thread_pool->enqueue([=]() { std::cerr << buf; });                         \
  } while (0);

public:
  RaftNode(int node_id, std::vector<RaftNodeConfig> node_configs);
  ~RaftNode();

  /* interfaces for test */
  void set_network(std::map<int, bool> &network_availablility);
  void set_reliable(bool flag);
  int get_list_state_log_num();
  int rpc_count();
  std::vector<u8> get_snapshot_direct();

  /*
   * Start the raft node.
   * Please make sure all of the rpc request handlers have been registered
   * before this method.
   */
  auto start() -> int;

  /*
   * Stop the raft node.
   */
  auto stop() -> int;

  /* Returns whether this node is the leader, you should also return the current
   * term. */
  auto is_leader() -> std::tuple<bool, int>;

  /* Checks whether the node is stopped */
  auto is_stopped() -> bool;

  /*
   * Send a new command to the raft nodes.
   * The returned tuple of the method contains three values:
   * 1. bool:  True if this raft node is the leader that successfully appends
   * the log, false If this node is not the leader.
   * 2. int: Current term.
   * 3. int: Log index.
   */
  auto new_command(std::vector<u8> cmd_data, int cmd_size)
      -> std::tuple<bool, int, int>;

  /* Save a snapshot of the state machine and compact the log. */
  auto save_snapshot() -> bool;

  /* Get a snapshot of the state machine */
  auto get_snapshot() -> std::vector<u8>;

  /* Internal RPC handlers */
  auto request_vote(RequestVoteArgs arg) -> RequestVoteReply;
  auto append_entries(RpcAppendEntriesArgs arg) -> AppendEntriesReply;
  auto install_snapshot(InstallSnapshotArgs arg) -> InstallSnapshotReply;

  /* RPC helpers */
  void send_request_vote(int target, RequestVoteArgs arg);
  void handle_request_vote_reply(int target, const RequestVoteArgs arg,
                                 const RequestVoteReply reply);

  void send_append_entries(int target, AppendEntriesArgs<Command> arg);
  void handle_append_entries_reply(int target,
                                   const AppendEntriesArgs<Command> arg,
                                   const AppendEntriesReply reply);

  void send_install_snapshot(int target, InstallSnapshotArgs arg);
  void handle_install_snapshot_reply(int target, const InstallSnapshotArgs arg,
                                     const InstallSnapshotReply reply);

  /* background workers */
  void run_background_ping();
  void run_background_election();
  void run_background_commit();
  void run_background_apply();

  /* Data structures */
  bool network_stat; /* for test */

  std::mutex mtx;         /* A big lock to protect the whole data structure. */
  std::mutex clients_mtx; /* A lock to protect RpcClient pointers */
  std::unique_ptr<ThreadPool> thread_pool;
  std::unique_ptr<RaftLog<Command>> log_storage; /* To persist the raft log. */
  std::unique_ptr<StateMachine> state; /*  The state machine that applies the
                                          raft log, e.g. a kv store. */

  std::unique_ptr<RpcServer>
      rpc_server; /* RPC server to recieve and handle the RPC requests. */
  std::map<int, std::unique_ptr<RpcClient>>
      rpc_clients_map; /* RPC clients of all raft nodes including this node. */
  std::vector<RaftNodeConfig> node_configs; /* Configuration for all nodes */
  int my_id; /* The index of this node in rpc_clients, start from 0. */

  std::atomic_bool stopped;

  RaftRole role;
  int current_term;
  int leader_id;

  std::unique_ptr<std::thread> background_election;
  std::unique_ptr<std::thread> background_ping;
  std::unique_ptr<std::thread> background_commit;
  std::unique_ptr<std::thread> background_apply;

  /* Lab3: Your code here */
  int support_id;
  int granted_votes;
  int commit_idx;
  int last_applied;

  unsigned long last_time;
  std::unique_ptr<int[]> next_index;
  std::unique_ptr<int[]> match_index;
  // Just a list in memory, volatile
  std::vector<LogEntry<Command>> log_vector;
  void start_background_threads() {
    const std::array<
        std::pair<std::unique_ptr<std::thread> &, void (RaftNode::*)()>, 4>
        threads{{{background_election, &RaftNode::run_background_election},
                 {background_ping, &RaftNode::run_background_ping},
                 {background_commit, &RaftNode::run_background_commit},
                 {background_apply, &RaftNode::run_background_apply}}};

    for (const auto &[thread, func] : threads) {
      thread = std::make_unique<std::thread>(func, this);
    }
  }

  void join_background_threads() {
    for (auto *thread : {&background_election, &background_ping,
                         &background_commit, &background_apply}) {
      if (thread->get() && (*thread)->joinable()) {
        (*thread)->join();
      }
    }
  }
  void update_last_time() {
    this->last_time = std::chrono::duration_cast<std::chrono::milliseconds>(
                          std::chrono::system_clock::now().time_since_epoch())
                          .count();
  }
  void maybe_update_commit_idx() {
    // 如果日志为空，直接返回
    if (log_vector.empty()) {
      return;
    }
    const int latest_index = log_vector.back().index;
    // 从最新索引向前遍历，寻找可以提交的索引
    for (int candidate_index = latest_index; candidate_index > commit_idx;
         --candidate_index) {
      if (log_vector[candidate_index].term != current_term) {
        continue;
      }
      int matched_count = 1; // Leader 自身总是匹配的
      for (const auto &[node_id, client] : rpc_clients_map) {
        if (node_id == my_id) {
          continue; // 跳过自身
        }
        if (match_index[node_id] >= candidate_index) {
          ++matched_count;
        }
        // 如果超过半数节点匹配，更新 commit_idx 并退出
        if (matched_count >= (rpc_clients_map.size() / 2 + 1)) {
          commit_idx = candidate_index;
          return;
        }
      }
    }
  }
  /**
   * @brief
   * 检查当前节点的日志是否与requestVoteArgs的日志同步或者当前日志晚于其日志
   * 也就是“candidate’s log is at least as up-to-date as receiver’s log”
   */
  bool check_log_up_to_date(RequestVoteArgs requestVoteArgs) {
    LogEntry<Command> last_log = this->log_vector.back();
    auto last_log_term = last_log.term;
    if (last_log_term < requestVoteArgs.lastLogTerm) {
      return true;
    }
    // 如果当前节点的term比request_vote_args的还大
    if (last_log_term > requestVoteArgs.lastLogTerm) {
      return false;
    }
    // 如果当前节点的term于request_vote_args的一致，并且index比他大
    if (last_log_term == requestVoteArgs.lastLogTerm) {
      // FIXME: maybe have some wrong here
      if (this->log_vector.size() - 1 > requestVoteArgs.lastLogIndex) {
        return false;
      }
    }
    return true;
  }
  /**
   * @brief 重设support_id
   *
   * @param id
   */
  void set_support_id(int id) {
    this->support_id = id;
    log_storage->updateMetaData(current_term, support_id);
  }
};

template <typename StateMachine, typename Command>
RaftNode<StateMachine, Command>::RaftNode(int node_id,
                                          std::vector<RaftNodeConfig> configs)
    : network_stat(true), node_configs(configs), my_id(node_id), stopped(true),
      role(RaftRole::Follower), current_term(0), leader_id(-1), support_id(-1),
      granted_votes(0), commit_idx(0), last_applied(0) {
  auto my_config = node_configs[my_id];

  /* launch RPC server */
  rpc_server =
      std::make_unique<RpcServer>(my_config.ip_address, my_config.port);

  /* Register the RPCs. */
  rpc_server->bind(RAFT_RPC_START_NODE, [this]() { return this->start(); });
  rpc_server->bind(RAFT_RPC_STOP_NODE, [this]() { return this->stop(); });
  rpc_server->bind(RAFT_RPC_CHECK_LEADER,
                   [this]() { return this->is_leader(); });
  rpc_server->bind(RAFT_RPC_IS_STOPPED,
                   [this]() { return this->is_stopped(); });
  rpc_server->bind(RAFT_RPC_NEW_COMMEND,
                   [this](std::vector<u8> data, int cmd_size) {
                     return this->new_command(data, cmd_size);
                   });
  rpc_server->bind(RAFT_RPC_SAVE_SNAPSHOT,
                   [this]() { return this->save_snapshot(); });
  rpc_server->bind(RAFT_RPC_GET_SNAPSHOT,
                   [this]() { return this->get_snapshot(); });

  rpc_server->bind(RAFT_RPC_REQUEST_VOTE, [this](RequestVoteArgs arg) {
    return this->request_vote(arg);
  });
  rpc_server->bind(RAFT_RPC_APPEND_ENTRY, [this](RpcAppendEntriesArgs arg) {
    return this->append_entries(arg);
  });
  rpc_server->bind(RAFT_RPC_INSTALL_SNAPSHOT, [this](InstallSnapshotArgs arg) {
    return this->install_snapshot(arg);
  });

  /* Lab3: Your code here */
  state = std::make_unique<StateMachine>();
  thread_pool = std::make_unique<ThreadPool>(32);
  last_time = std::chrono::time_point_cast<std::chrono::milliseconds>(
                  std::chrono::high_resolution_clock::now())
                  .time_since_epoch()
                  .count();
  next_index.reset(new int[configs.size()]);
  match_index.reset(new int[configs.size()]);
  /**
   * Notice that the first log index is 1 instead of 0.
   * To simplify the programming, you can append an empty log entry to the logs
   * at the very beginning. And since the 'lastApplied' index starts from 0, the
   * first empty log entry will never be applied to the state machine.
   */
  LogEntry<Command> init_entry;
  init_entry.term = 0;
  init_entry.index = 0;
  log_vector.push_back(init_entry);

  // RaftLog for persistence
  std::string node_log_filename =
      "/tmp/raft_log/node" + std::to_string(node_id);
  bool is_recover = is_file_exist(node_log_filename);
  auto block_manager =
      std::shared_ptr<BlockManager>(new BlockManager(node_log_filename));
  log_storage = std::make_unique<RaftLog<Command>>(block_manager, is_recover);
  if (is_recover) {
    log_storage->recover(current_term, support_id, log_vector);
  } else {
    log_storage->updateMetaData(current_term, support_id);
    log_storage->updateLogs(log_vector);
  }

  RAFT_LOG("A new raft node init");
  rpc_server->run(true, configs.size());
}

template <typename StateMachine, typename Command>
RaftNode<StateMachine, Command>::~RaftNode() {
  stop();

  next_index.reset();
  match_index.reset();
  thread_pool.reset();
  rpc_server.reset();
  state.reset();
  log_storage.reset();
}

/******************************************************************

                        RPC Interfaces

*******************************************************************/

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::start() -> int {
  /* Lab3: Your code here */
  // RAFT_LOG("[node %d] start", this->my_id);
  for (const auto &config : node_configs) {
    rpc_clients_map.emplace(
        config.node_id,
        std::make_unique<RpcClient>(config.ip_address, config.port, true));
  }
  this->stopped = false;
  this->start_background_threads();
  return 0;
}

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::stop() -> int {
  /* Lab3: Your code here */
  // RAFT_LOG("[node %d] stop", this->my_id);
  this->stopped = true;
  this->join_background_threads();
  return 0;
}

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::is_leader() -> std::tuple<bool, int> {
  /* Lab3: Your code here */
  // RAFT_LOG("[node %d] is_leader", this->my_id);
  return {role == RaftRole::Leader, current_term};
}

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::is_stopped() -> bool {
  return stopped.load();
}

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::new_command(std::vector<u8> cmd_data,
                                                  int cmd_size)
    -> std::tuple<bool, int, int> {
  /* Lab3: Your code here */
  std::unique_lock<std::mutex> lock(this->mtx);
  if (this->role == RaftRole::Leader) {
    Command command;
    command.deserialize(cmd_data, command.size());
    LogEntry<Command> entry(log_vector.size(), this->current_term, command);
    log_vector.push_back(entry);
    log_storage->updateLogs(log_vector);
    return {true, this->current_term, log_vector.size() - 1};
  }
  return std::make_tuple(false, -1, -1);
}

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::save_snapshot() -> bool {
  /* Lab3: Your code here */
  return true;
}

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::get_snapshot() -> std::vector<u8> {
  /* Lab3: Your code here */
  return std::vector<u8>();
}

/******************************************************************

                         Internal RPC Related

*******************************************************************/

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::request_vote(RequestVoteArgs args)
    -> RequestVoteReply {

  /* Lab3: Your code here */
  std::unique_lock<std::mutex> lock(this->mtx);
  // RAFT_LOG("node %d: request_vote", this->my_id);
  RequestVoteReply reject_request_vote_reply(this->current_term, false,
                                             this->my_id);
  // 如果args.term < this->current_term，拒绝
  if (args.term < this->current_term) {
    return reject_request_vote_reply;
  }
  // 如果本轮已经投票（且不是当前id），拒绝
  if (this->current_term == args.term && this->support_id != -1) {
    if (this->support_id != args.candidateId) {
      return reject_request_vote_reply;
    }
  }
  this->role = RaftRole::Follower;
  this->current_term = args.term;
  this->set_support_id(-1);
  // 如果log没有uptodate，拒绝
  if (!this->check_log_up_to_date(args)) {
    // FIXME:
    return reject_request_vote_reply;
  }
  this->set_support_id(args.candidateId);
  this->leader_id = args.candidateId;
  this->update_last_time();
  // FIXME:
  return RequestVoteReply(this->current_term, true, this->my_id);
  // return RequestVoteReply();
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::handle_request_vote_reply(
    int target, const RequestVoteArgs arg, const RequestVoteReply reply) {
  /* Lab3: Your code here */
  std::unique_lock<std::mutex> lock(this->mtx);
  this->update_last_time();
  // 已经不是候选人状态
  if (this->role != RaftRole::Candidate) {
    return;
  }
  // 自己已经过了任期,是一张废票
  if (this->current_term > arg.term) {
    return;
  }
  if (this->current_term < reply.term) {
    // 说明已经输了这个任期的选举
    this->set_support_id(-1);
    this->role = RaftRole::Follower;
    this->current_term = reply.term;
    return;
  }
  // 票有效，处理票
  if (reply.voteGranted) {
    this->granted_votes++;
    if (this->granted_votes >= rpc_clients_map.size() / 2 + 1) {
      // 赢得选举
      this->role = RaftRole::Leader;
      for (int i = 0; i < node_configs.size(); i++) {
        this->match_index[i] = 0;
        this->next_index[i] = this->log_vector.size();
      }
    }
  }
}

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::append_entries(
    RpcAppendEntriesArgs rpc_arg) -> AppendEntriesReply {
  /* Lab3: Your code here */
  std::unique_lock<std::mutex> lock(mtx);
  bool is_meta_changed = false;
  bool is_log_changed = false;
  AppendEntriesReply reply;
  AppendEntriesArgs<Command> arg =
      transform_rpc_append_entries_args<Command>(rpc_arg);
  if (leader_id == arg.leader_id) {
    last_time = std::chrono::time_point_cast<std::chrono::milliseconds>(
                    std::chrono::high_resolution_clock::now())
                    .time_since_epoch()
                    .count();
  }
  //! debug//
  // RAFT_LOG("Receive Real AppendEntriesArgs from %d", rpc_arg.leaderId);
  //! debug//
  if (arg.term > current_term) {
    if (leader_id != arg.leader_id) {
      leader_id = arg.leader_id;
      last_time = std::chrono::time_point_cast<std::chrono::milliseconds>(
                      std::chrono::high_resolution_clock::now())
                      .time_since_epoch()
                      .count();
    }
    role = RaftRole::Follower;
    current_term = arg.term;
    support_id = -1;
    is_meta_changed = true;
    /**
     * Reply false if log doesn’t contain an entry at prevLogIndex
     * whose term matches prevLogTerm
     */
    if (log_vector.size() - 1 < arg.lastLogIndex ||
        log_vector[arg.lastLogIndex].term != arg.lastLogTerm) {
      //! debug//
      // RAFT_LOG("Reply false! my last index:%lu, my last term:%d,
      // prevLogIndex:%d, prevLogTerm:%d", log_entry_list.size() - 1,
      // log_entry_list[log_entry_list.size() - 1].term, arg.prevLogIndex,
      // arg.prevLogTerm);
      //! debug//
      reply.term = current_term;
      reply.append_successfully = false;
    } else {
      /**
       * Now we promise that the entry before(including) prevLog is the same
       * between Follower and Leader. So we just need to append new entries from
       * (prevIndex + 1) If Follower has inconsitent entry, discard them
       */
      log_vector.resize(arg.lastLogIndex + 1);
      std::vector<LogEntry<Command>> new_entry_list = arg.log_vector;
      for (const LogEntry<Command> &entry : new_entry_list) {
        log_vector.push_back(entry);
      }
      is_log_changed = true;
      /**
       * If leaderCommit > commitIndex, set commitIndex =
       * min(leaderCommit, index of last new entry)
       */
      int last_new_entry_index = log_vector.size() - 1;
      if (arg.lastCommit > commit_idx) {
        commit_idx = arg.lastCommit < last_new_entry_index
                         ? arg.lastCommit
                         : last_new_entry_index;
      }
      reply.term = current_term;
      reply.append_successfully = true;
    }
  } else if (arg.term == current_term) {
    if (leader_id != arg.leader_id) {
      leader_id = arg.leader_id;
      last_time = std::chrono::time_point_cast<std::chrono::milliseconds>(
                      std::chrono::high_resolution_clock::now())
                      .time_since_epoch()
                      .count();
    }
    role = RaftRole::Follower;
    /**
     * Reply false if log doesn’t contain an entry at prevLogIndex
     * whose term matches prevLogTerm
     */
    if (log_vector.size() - 1 < arg.lastLogIndex ||
        log_vector[arg.lastLogIndex].term != arg.lastLogTerm) {
      //! debug//
      // RAFT_LOG("Reply false! my last index:%lu, my last term:%d,
      // prevLogIndex:%d, prevLogTerm:%d", log_entry_list.size() - 1,
      // log_entry_list[log_entry_list.size() - 1].term, arg.prevLogIndex,
      // arg.prevLogTerm);
      //! debug//
      reply.term = current_term;
      reply.append_successfully = false;
    } else {
      /**
       * Now we promise that the entry before(including) prevLog is the same
       * between Follower and Leader. So we just need to append new entries from
       * (prevIndex + 1) If Follower has inconsitent entry, discard them
       */
      /**
       * But there is a very corner case here, if it receives a empty
       * list(heartbeat) do we need to erase behind?
       */
      log_vector.resize(arg.lastLogIndex + 1);
      std::vector<LogEntry<Command>> new_entry_list = arg.log_vector;
      for (const LogEntry<Command> &entry : new_entry_list) {
        log_vector.push_back(entry);
      }
      is_log_changed = true;
      //! debug//
      // RAFT_LOG("log entry list size now:%lu, arg.prevLogIndex: %d, entry list
      // size: %lu", log_entry_list.size(), arg.prevLogIndex,
      // new_entry_list.size());
      //! debug//
      /**
       * If leaderCommit > commitIndex, set commitIndex =
       * min(leaderCommit, index of last new entry)
       */
      int last_new_entry_index = log_vector.size() - 1;
      if (arg.lastCommit > commit_idx) {
        commit_idx = arg.lastCommit < last_new_entry_index
                         ? arg.lastCommit
                         : last_new_entry_index;
      }
      reply.term = current_term;
      reply.append_successfully = true;
    }
  } else {
    reply.term = current_term;
    reply.append_successfully = false;
  }
  //[ persistency ]//
  if (is_meta_changed) {
    log_storage->updateMetaData(current_term, support_id);
  }
  if (is_log_changed) {
    log_storage->updateLogs(log_vector);
  }
  //[ persistency ]//
  return reply;
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::handle_append_entries_reply(
    int node_id, const AppendEntriesArgs<Command> arg,
    const AppendEntriesReply reply) {
  /* Lab3: Your code here */
  std::unique_lock<std::mutex> lock(mtx);
  last_time = std::chrono::time_point_cast<std::chrono::milliseconds>(
                  std::chrono::high_resolution_clock::now())
                  .time_since_epoch()
                  .count();
  /** This means this term has been finished.
   *  A new candidate has been trying to become a new leader.
   *  Or even has been become a new leader.
   *  Now it should give up leader and become follower again.
   *  And then it don't have ability to do anything about this reply.
   */
  if (reply.term > current_term) {
    role = RaftRole::Follower;
    current_term = reply.term;
    support_id = -1;
    //! debug//
    RAFT_LOG("I am follower now");
    //! debug//
    //[ persistency ]//
    log_storage->updateMetaData(current_term, support_id);
    //[ persistency ]//
    return;
  }
  if (role != RaftRole::Leader) {
    // If this server has been not a leader anymore, it don't have ability to do
    // anything about this reply.
    return;
  }
  // if(reply.isHeartbeat){
  //     return;
  // }
  if (reply.append_successfully == false) {
    /**
     * There are two situation will cause reply's false
     * 1. arg.term < reply.term
     * 2. prevLogIndex's entry is inconsistent which means next_index of this
     * follower should be smaller
     */
    if (reply.term > arg.term) {
      /**
       * We can't promise that arg.term is the same as current_term so we need
       * to handle it specifically here
       */
      return;
    } else {

      //! debug//
      // RAFT_LOG("%d's next_index - 1 : %d", node_id, next_index[node_id] - 1);
      // RAFT_LOG("%d, %d, %d", arg.term, arg.prevLogTerm, arg.prevLogIndex);
      //! debug//
      next_index[node_id] = next_index[node_id] - 1;
    }
  } else {
    //! Attention: we must choose bigger one, arg.prevLogIndex +
    //! arg.logEntryList.size() can be smaller than now match_index[node_id]
    //! Because a former reply can be received later!!!
    int reply_match_index = arg.lastLogIndex + arg.log_vector.size();
    if (reply_match_index > match_index[node_id]) {
      match_index[node_id] = arg.lastLogIndex + arg.log_vector.size();
    }
    //! debug//
    // RAFT_LOG("%d's next_index old:%d, new:%d", node_id, next_index[node_id],
    // match_index[node_id] + 1);
    //! debug//
    next_index[node_id] = match_index[node_id] + 1;
    /**
     * If there exists an N such that N > commitIndex, a majority
     * of matchIndex[i] ≥ N, and log[N].term == currentTerm: set commitIndex = N
     */
    const auto MACHINE_NUM = rpc_clients_map.size();
    const auto LAST_INDEX = log_vector.size() - 1;
    for (int N = LAST_INDEX; N > commit_idx; --N) {
      if (log_vector[N].term != current_term) {
        break;
      }
      // This node itself must match
      int num_of_matched = 1;
      for (auto map_it = rpc_clients_map.begin();
           map_it != rpc_clients_map.end(); ++map_it) {
        if (map_it->first == my_id) {
          continue;
        }
        if (match_index[map_it->first] >= N) {
          ++num_of_matched;
        }
        if (num_of_matched >= MACHINE_NUM / 2 + 1) {
          commit_idx = N;
          break;
        }
      }
      if (commit_idx == N) {
        break;
      }
    }
  }
  return;
}

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::install_snapshot(InstallSnapshotArgs args)
    -> InstallSnapshotReply {
  /* Lab3: Your code here */
  return InstallSnapshotReply();
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::handle_install_snapshot_reply(
    int node_id, const InstallSnapshotArgs arg,
    const InstallSnapshotReply reply) {
  /* Lab3: Your code here */
  return;
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::send_request_vote(int target_id,
                                                        RequestVoteArgs arg) {
  std::unique_lock<std::mutex> clients_lock(clients_mtx);
  if (rpc_clients_map[target_id] == nullptr ||
      rpc_clients_map[target_id]->get_connection_state() !=
          rpc::client::connection_state::connected) {
    return;
  }

  auto res = rpc_clients_map[target_id]->call(RAFT_RPC_REQUEST_VOTE, arg);
  clients_lock.unlock();
  if (res.is_ok()) {
    handle_request_vote_reply(target_id, arg,
                              res.unwrap()->as<RequestVoteReply>());
  } else {
    // RPC fails
  }
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::send_append_entries(
    int target_id, AppendEntriesArgs<Command> arg) {
  std::unique_lock<std::mutex> clients_lock(clients_mtx);
  if (rpc_clients_map[target_id] == nullptr ||
      rpc_clients_map[target_id]->get_connection_state() !=
          rpc::client::connection_state::connected) {
    return;
  }

  RpcAppendEntriesArgs rpc_arg = transform_append_entries_args(arg);
  auto res = rpc_clients_map[target_id]->call(RAFT_RPC_APPEND_ENTRY, rpc_arg);
  clients_lock.unlock();
  if (res.is_ok()) {
    handle_append_entries_reply(target_id, arg,
                                res.unwrap()->as<AppendEntriesReply>());
  } else {
    // RPC fails
  }
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::send_install_snapshot(
    int target_id, InstallSnapshotArgs arg) {
  std::unique_lock<std::mutex> clients_lock(clients_mtx);
  if (rpc_clients_map[target_id] == nullptr ||
      rpc_clients_map[target_id]->get_connection_state() !=
          rpc::client::connection_state::connected) {
    return;
  }

  auto res = rpc_clients_map[target_id]->call(RAFT_RPC_INSTALL_SNAPSHOT, arg);
  clients_lock.unlock();
  if (res.is_ok()) {
    handle_install_snapshot_reply(target_id, arg,
                                  res.unwrap()->as<InstallSnapshotReply>());
  } else {
    // RPC fails
  }
}

/******************************************************************

                        Background Workers

*******************************************************************/

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::run_background_election() {
  // Periodly check the liveness of the leader.

  // Work for followers and candidates.

  /* Uncomment following code when you finish */
  while (true) {
    {
      if (is_stopped()) {
        return;
      }
      /* Lab3: Your code here */

      {
        std::unique_lock<std::mutex> lock(this->mtx);
        // 生成一个150-300的随机数
        long long random_timeout = rand() % (ELECTION_TIMEOUT_UPPER_BOUND -
                                             ELECTION_TIMEOUT_LOWER_BOUND + 1) +
                                   ELECTION_TIMEOUT_LOWER_BOUND;

        long long latest_time = random_timeout + this->last_time;
        long long current_time =
            std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::system_clock::now().time_since_epoch())
                .count();
        if (current_time > latest_time) {
          this->role = RaftRole::Candidate;
          this->current_term++;
          this->granted_votes = 1;
          this->update_last_time();
          this->set_support_id(this->my_id);
          const auto &last_log = log_vector.back();
          RequestVoteArgs vote_request(current_term, my_id, last_log.index,
                                       last_log.term);
          for (const auto &[target_id, _] : rpc_clients_map) {
            if (target_id == my_id)
              continue;
            thread_pool->enqueue(&RaftNode::send_request_vote, this, target_id,
                                 vote_request);
          }
        }
      }
      std::this_thread::sleep_for(
          std::chrono::milliseconds(BACKEND_ELECTION_INTERVAL));
    }
  }
  return;
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::run_background_commit() {
  // Periodly send logs to the follower.

  // Only work for the leader.

  /* Uncomment following code when you finish */
  while (true) {
    {
      if (is_stopped()) {
        return;
      }
      /* Lab3: Your code here */
      mtx.lock();
      if (role == RaftRole::Leader) {
        for (auto map_it = rpc_clients_map.begin();
             map_it != rpc_clients_map.end(); ++map_it) {
          if (map_it->first == my_id) {
            continue;
          }
          if (next_index[map_it->first] < log_vector.size()) {
            //! debug//
            // RAFT_LOG("%d machine's next index is %d, log entry list
            // size:%zu", map_it->first, next_index[map_it->first],
            // log_entry_list.size());
            //! debug//
            std::vector<LogEntry<Command>> append_entry_list;
            append_entry_list.clear();
            for (int i = next_index[map_it->first]; i < log_vector.size();
                 ++i) {
              append_entry_list.push_back(log_vector[i]);
            }
            AppendEntriesArgs<Command> args;
            args.term = current_term;
            args.leader_id = my_id;
            args.lastLogIndex = next_index[map_it->first] - 1;
            //! debug//
            // RAFT_LOG("send request to %d node, prev log index: %d",
            // map_it->first, args.prevLogIndex);
            //! debug//
            args.lastLogTerm = log_vector[args.lastLogIndex].term;
            args.lastCommit = commit_idx;
            //! debug//
            // RAFT_LOG("Leader's commit_index: %d", commit_index);
            //! debug//
            args.log_vector = append_entry_list;
            thread_pool->enqueue(&RaftNode::send_append_entries, this,
                                 map_it->first, args);
          }
        }
      }
      mtx.unlock();
      std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
  }

  return;
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::run_background_apply() {
  // Periodly apply committed logs the state machine

  // Work for all the nodes.

  /* Uncomment following code when you finish */
  while (true) {
    {
      if (is_stopped()) {
        return;
      }
      /* Lab3: Your code here */
      mtx.lock();
      if (last_applied < commit_idx) {
        for (int i = last_applied + 1; i <= commit_idx; ++i) {
          state->apply_log(log_vector[i].command);
        }
        last_applied = commit_idx;
      }
      mtx.unlock();
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
  }

  return;
}

// template <typename StateMachine, typename Command>
// void RaftNode<StateMachine, Command>::run_background_ping() {
//   // Periodly send empty append_entries RPC to the followers.

//   // Only work for the leader.

//   /* Uncomment following code when you finish */
//   RAFT_LOG("background ping start");
//   while (true) {
//     {
//       if (is_stopped()) {
//         RAFT_LOG("background ping end");
//         return;
//       }
//       /* Lab3: Your code here */
//       mtx.lock();
//       if (role == RaftRole::Leader) {
//         // Send heartbeat to every Follower
//         for (auto map_it = rpc_clients_map.begin();
//              map_it != rpc_clients_map.end(); ++map_it) {
//           // Don't need to send heartbeat to itself
//           int target_id = map_it->first;
//           if (target_id == my_id) {
//             continue;
//           }
//           AppendEntriesArgs<Command> heartbeat;
//           heartbeat.term = current_term;
//           heartbeat.leader_id = my_id;
//           heartbeat.lastLogIndex = next_index[map_it->first] - 1;
//           heartbeat.lastLogTerm = log_vector[heartbeat.lastLogIndex].term;
//           heartbeat.lastCommit = commit_idx;
//           heartbeat.log_vector.clear();
//           thread_pool->enqueue(&RaftNode::send_append_entries, this, target_id,
//                                heartbeat);
//         }
//       }
//       mtx.unlock();
//       // We choose heartbeat interval 300 ms. So every servers' timeout limit
//       // will range from 300ms~600ms
//       std::this_thread::sleep_for(std::chrono::milliseconds(300));
//     }
//   }
//   return;
// }

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::run_background_ping() {
  // Periodly send empty append_entries RPC to the followers.

  // Only work for the leader.

  /* Uncomment following code when you finish */
  while (true) {
    {
      if (is_stopped()) {
        return;
      }
      /* Lab3: Your code here */
      {
        std::unique_lock<std::mutex> lock(this->mtx);
        // only work for the leader
        if (this->role != RaftRole::Leader) {
          continue;
        }
        for (const auto &[target_id, _] : rpc_clients_map) {
          if (target_id == my_id)
            continue;
          if (next_index[target_id] > 0) {
            // FIXME:
            thread_pool->enqueue(
                &RaftNode::send_append_entries, this, target_id,
                AppendEntriesArgs<Command>(
                    this->my_id, this->current_term, next_index[target_id] - 1,
                    log_vector[next_index[target_id] - 1].term,
                    this->commit_idx));
          }
        }
      }
      // 暂停一段时间
      std::this_thread::sleep_for(
          std::chrono::milliseconds(HEART_BEAT_INTERVAL));
    }
  }

  return;
}

/******************************************************************

                          Test Functions (must not edit)

*******************************************************************/

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::set_network(
    std::map<int, bool> &network_availability) {
  std::unique_lock<std::mutex> clients_lock(clients_mtx);

  /* turn off network */
  if (!network_availability[my_id]) {
    for (auto &&client : rpc_clients_map) {
      if (client.second != nullptr)
        client.second.reset();
    }

    return;
  }

  for (auto node_network : network_availability) {
    int node_id = node_network.first;
    bool node_status = node_network.second;

    if (node_status && rpc_clients_map[node_id] == nullptr) {
      RaftNodeConfig target_config;
      for (auto config : node_configs) {
        if (config.node_id == node_id)
          target_config = config;
      }

      rpc_clients_map[node_id] = std::make_unique<RpcClient>(
          target_config.ip_address, target_config.port, true);
    }

    if (!node_status && rpc_clients_map[node_id] != nullptr) {
      rpc_clients_map[node_id].reset();
    }
  }
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::set_reliable(bool flag) {
  std::unique_lock<std::mutex> clients_lock(clients_mtx);
  for (auto &&client : rpc_clients_map) {
    if (client.second) {
      client.second->set_reliable(flag);
    }
  }
}

template <typename StateMachine, typename Command>
int RaftNode<StateMachine, Command>::get_list_state_log_num() {
  /* only applied to ListStateMachine*/
  std::unique_lock<std::mutex> lock(mtx);

  return state->num_append_logs;
}

template <typename StateMachine, typename Command>
int RaftNode<StateMachine, Command>::rpc_count() {
  int sum = 0;
  std::unique_lock<std::mutex> clients_lock(clients_mtx);

  for (auto &&client : rpc_clients_map) {
    if (client.second) {
      sum += client.second->count();
    }
  }

  return sum;
}

template <typename StateMachine, typename Command>
std::vector<u8> RaftNode<StateMachine, Command>::get_snapshot_direct() {
  if (is_stopped()) {
    return std::vector<u8>();
  }

  std::unique_lock<std::mutex> lock(mtx);

  return state->snapshot();
}

} // namespace chfs