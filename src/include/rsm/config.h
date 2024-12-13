// leader心跳间隔
const int HEART_BEAT_INTERVAL = 300;
// 触发重新选举的时间下界
const int ELECTION_TIMEOUT_LOWER_BOUND = 300;
// 触发重新选举的时间上界
const int ELECTION_TIMEOUT_UPPER_BOUND = 1000;
// run_background_election的循环间隔
const int BACKEND_ELECTION_INTERVAL = 100;