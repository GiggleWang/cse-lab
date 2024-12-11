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

struct RequestVoteArgs {
  /* Lab3: Your code here */
  int term;         // candidate’s term
  int candidateId;  // candidate requesting vote
  int lastLogIndex; // index of candidate’s last log entry
  int lastLogTerm;  // term of candidate’s last log entry
  RequestVoteArgs(int term, int candidateId, int lastLogIndex, int lastLogTerm)
      : term(term), candidateId(candidateId), lastLogIndex(lastLogIndex),
        lastLogTerm(lastLogTerm) {}
  RequestVoteArgs() {}
  MSGPACK_DEFINE(term, candidateId, lastLogIndex, lastLogTerm)
};

struct RequestVoteReply {
  /* Lab3: Your code here */
  int term;         // currentTerm, for candidate to update itself
  bool voteGranted; // true means candidate received vote
  int followerId;
  RequestVoteReply(int term, bool voteGranted, int followerId)
      : term(term), voteGranted(voteGranted), followerId(followerId) {}
  RequestVoteReply() {}
  MSGPACK_DEFINE(term, voteGranted, followerId)
};

template <typename Command> struct AppendEntriesArgs {
  /* Lab3: Your code here */
};

struct RpcAppendEntriesArgs {
  /* Lab3: Your code here */

  MSGPACK_DEFINE(

  )
};

template <typename Command>
RpcAppendEntriesArgs
transform_append_entries_args(const AppendEntriesArgs<Command> &arg) {
  /* Lab3: Your code here */
  return RpcAppendEntriesArgs();
}

template <typename Command>
AppendEntriesArgs<Command>
transform_rpc_append_entries_args(const RpcAppendEntriesArgs &rpc_arg) {
  /* Lab3: Your code here */
  return AppendEntriesArgs<Command>();
}

struct AppendEntriesReply {
  /* Lab3: Your code here */

  MSGPACK_DEFINE(

  )
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

template <typename Command> struct LogEntry {
  int index;
  int term;
  Command command;
};

} /* namespace chfs */