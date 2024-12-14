// leader心跳间隔
const int HEART_BEAT_INTERVAL = 300;
// 触发重新选举的时间下界
const int ELECTION_TIMEOUT_LOWER_BOUND = 300;
// 触发重新选举的时间上界
const int ELECTION_TIMEOUT_UPPER_BOUND = 1000;
// run_background_election的循环间隔
const int BACKEND_ELECTION_INTERVAL = 100;
// run_background_commit的循环间隔
const int BACKEND_COMMIT_INTERVAL = 50;
// run_background_apply的循环间隔
const int BACKEND_APPLY_INTERVAL = 10;
// log中记录term_and_support_id的inode
const int TERM_AND_SUPPORT_INODE_ID = 1;
// log中记录log_entry的inode
const int LOG_ENTRY_INODE_ID = 2;
const int MAX_INODE_NUM = 16;