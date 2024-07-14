package raft

type WaitApply struct {
	done  bool
	index uint64
	ch    chan struct{}
}
type RaftLog struct {
	logEnties        []*pb.LogEntry // 未提交日志
	storage          Storage        // 已提交日志存储
	commitIndex      uint64         // 提交进度
	lastAppliedIndex uint64         // 最后提交日志
	lastAppliedTerm  uint64         // 最后提交日志任期
	lastAppendIndex  uint64         // 最后追加日志
	logger           *zap.SugaredLogger
}
