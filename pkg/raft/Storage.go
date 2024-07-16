package raft

type Storage interface {
	Append(entries []*pb.LogEntry)                         // 进行追加
	GetEntries(startIndex, endIndex uint64) []*pb.LogEntry // 获取日志
	GetTerm(index uint64) uint64                           //获取任期
	GetLastLogIndexAndTerm() (uint64, uint64)              // 获取最后一条日志的索引和任期
	Close()                                                // 进行关闭
}

// NewRaftLog 初始化raft的日志实例
func NewRaftLog(storage Storage, logger *zap.SugaredLogger) *RaftLog {
	// 先进行获取
	lastIndex, lastTerm := storage.GetLastLogIndexAndTerm()

	return &RaftLog{
		logEnties:        make([]*pb.LogEntry, 0),
		storage:          storage,
		commitIndex:      lastIndex,
		lastAppliedIndex: lastIndex,
		lastAppliedTerm:  lastTerm,
		lastAppendIndex:  lastIndex,
		logger:           logger,
	}
}
