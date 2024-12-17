#pragma once

#include "rpc/msgpack.hpp"
#include "rsm/raft/log.h"

namespace chfs {

const std::string RAFT_RPC_START_NODE = "start node";
const std::string RAFT_RPC_STOP_NODE = "stop node";
const std::string RAFT_RPC_NEW_COMMEND = "new commend";
const std::string RAFT_RPC_CHECK_LEADER = "check leader";
const std::string RAFT_RPC_IS_STOPPED = "check stopped";
const std::string RAFT_RPC_SAVE_SNAPSHOT = "save snapshot";
const std::string RAFT_RPC_GET_SNAPSHOT = "get snapshot";

const std::string RAFT_RPC_REQUEST_VOTE = "request vote";
const std::string RAFT_RPC_APPEND_ENTRY = "append entries";
const std::string RAFT_RPC_INSTALL_SNAPSHOT = "install snapshot";

template <typename Command> struct LogEntry {
  int index;
  int term;
  Command command;
  LogEntry<Command>() {}
  LogEntry<Command>(int index, int term, Command command = Command())
      : index(index), term(term), command(command) {}
};

struct RequestVoteArgs {
  /* Lab3: Your code here */
  int term;
  int candidateId;
  int lastLogIndex;
  int lastLogTerm;
  RequestVoteArgs(int term, int candidateId, int lastLogIndex, int lastLogTerm)
      : term(term), candidateId(candidateId), lastLogIndex(lastLogIndex),
        lastLogTerm(lastLogTerm) {}
  RequestVoteArgs() {}
  MSGPACK_DEFINE(term, candidateId, lastLogIndex, lastLogTerm)
};

struct RequestVoteReply {
  /* Lab3: Your code here */
  int term;
  bool voteGranted;
  int followerId;
  RequestVoteReply(int term, bool voteGranted, int followerId)
      : term(term), voteGranted(voteGranted), followerId(followerId) {}
  RequestVoteReply() {}
  MSGPACK_DEFINE(term, voteGranted, followerId)
};

template <typename Command> struct AppendEntriesArgs {
  /* Lab3: Your code here */
  int leader_id;
  int lastLogIndex;
  int lastLogTerm;
  int term;
  int lastCommit;
  std::vector<LogEntry<Command>> log_vector;
  AppendEntriesArgs<Command>() {}
  AppendEntriesArgs<Command>(int leader_id, int term, int lastLogIndex,
                             int lastLogTerm, int lastCommit)
      : leader_id(leader_id), lastLogIndex(lastLogIndex),
        lastLogTerm(lastLogTerm), term(term), lastCommit(lastCommit) {
    this->log_vector.clear();
  }
};

struct RpcAppendEntriesArgs {
  /* Lab3: Your code here */
  int leader_id;
  int lastLogIndex;
  int lastLogTerm;
  int term;
  int lastCommit;
  std::vector<int> log_index_vector;
  std::vector<int> log_term_vector;
  std::vector<std::vector<u8>> log_command_vector;
  RpcAppendEntriesArgs() {}
  RpcAppendEntriesArgs(int leader_id, int term, int lastLogIndex,
                       int lastLogTerm, int lastCommit)
      : leader_id(leader_id), lastLogIndex(lastLogIndex),
        lastLogTerm(lastLogTerm), term(term), lastCommit(lastCommit) {
    this->log_index_vector.clear();
    this->log_term_vector.clear();
    this->log_command_vector.clear();
  }
  MSGPACK_DEFINE(term, leader_id, lastLogIndex, lastLogTerm, lastCommit,
                 log_index_vector, log_term_vector, log_command_vector)
};

template <typename Command>
RpcAppendEntriesArgs
transform_append_entries_args(const AppendEntriesArgs<Command> &arg) {
  /* Lab3: Your code here */
  RpcAppendEntriesArgs rpc_args(arg.leader_id, arg.term, arg.lastLogIndex,
                                arg.lastLogTerm, arg.lastCommit);
  for (const auto &entry : arg.log_vector) {
    rpc_args.log_term_vector.push_back(entry.term);
    rpc_args.log_index_vector.push_back(entry.index);
    rpc_args.log_command_vector.push_back(
        entry.command.serialize(entry.command.size()));
  }
  return rpc_args;
}

template <typename Command>
AppendEntriesArgs<Command>
transform_rpc_append_entries_args(const RpcAppendEntriesArgs &rpc_arg) {
  /* Lab3: Your code here */
  AppendEntriesArgs<Command> arg(rpc_arg.leader_id, rpc_arg.term,
                                 rpc_arg.lastLogIndex, rpc_arg.lastLogTerm,
                                 rpc_arg.lastCommit);
  // 确保三个vector的长度相同
  size_t entry_count = rpc_arg.log_term_vector.size();
  assert(entry_count == rpc_arg.log_index_vector.size());
  assert(entry_count == rpc_arg.log_command_vector.size());
  for (size_t i = 0; i < entry_count; i++) {
    Command cmd;
    cmd.deserialize(rpc_arg.log_command_vector[i], cmd.size());
    LogEntry<Command> entry{rpc_arg.log_index_vector[i],
                            rpc_arg.log_term_vector[i], cmd};
    arg.log_vector.push_back(entry);
  }

  return arg;
}

struct AppendEntriesReply {
  /* Lab3: Your code here */
  int term;
  bool append_successfully;
  AppendEntriesReply() {}
  AppendEntriesReply(int term, int append_successfully)
      : term(term), append_successfully(append_successfully) {}
  AppendEntriesReply(int leader_id, int term, int append_successfully)
      : term(term), append_successfully(append_successfully) {}
  MSGPACK_DEFINE(term, append_successfully)
};

struct InstallSnapshotArgs {
  /* Lab3: Your code here */

  MSGPACK_DEFINE(

  )
};

struct InstallSnapshotReply {
  /* Lab3: Your code here */

  MSGPACK_DEFINE(

  )
};

} /* namespace chfs */