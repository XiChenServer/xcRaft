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

// HasPrevLog 实现一致性检查，已持久化的日志必然与leader一致，检查一致时只需检查内存中日志切片，存在以下几种情况：
//
// 节点日志中有找到leader上次追加日志
//
// 为节点追加的最后一条日志
// 为节点内存切片中的某条日志
//
// 节点网络波动，导致未响应leader，leader重发了记录，清除重复日志记录
// 节点作为leader期间有部分日志未同步到其他节点就失效，集群重新选举，导致后续日志不一致，清除冲突日志(内存中后续日志)
//
// 为节点最后提交日志
//
// 如内存中存在日志记录，则内存中的记录皆不一致，清除内存日志记录
//
// 节点未找到leader上次追加日志
//
// 存在相同日志编号记录，任不相同
//
// 节点作为leader期间有部分日志未同步到其他节点就失效，集群重新选举，导致使用了相同日志编号，清除冲突日志(相同任期的日志)，从节点未冲突部分开始重发
//
// 没有相同日志编号记录
//
// 日志缺失，需从最后提交开始重发
// 存在上个日志：检查本地日志记录是否与领导者的日志记录相匹配
func (l *RaftLog) HasPrevLog(lastIndex, lastTerm uint64) bool {
	if lastIndex == 0 {
		return true
	}
	var term uint64
	size := len(l.logEnties)
	if size > 0 {

		lastlog := l.logEnties[size-1]
		if lastlog.Index == lastIndex {
			term = lastlog.Term
		} else if lastlog.Index > lastIndex {
			// 检查最后提交
			if lastIndex == l.lastAppliedIndex { // 已提交日志必然一致
				l.logEnties = l.logEnties[:0]
				return true
			} else if lastIndex > l.lastAppliedIndex {
				// 检查未提交日志
				for i, entry := range l.logEnties[:size] {
					if entry.Index == lastIndex {
						term = entry.Term
						// 将leader上次追加后日志清理
						// 网络异常未收到响应导致leader重发日志/leader重选举使旧leader未同步数据失效
						l.logEnties = l.logEnties[:i+1]
						break
					}
				}
			}
		}
	} else if lastIndex == l.lastAppliedIndex {
		return true
	}

	b := term == lastTerm
	if !b {
		l.logger.Debugf("最新日志: %d, 任期: %d ,本地记录任期: %d", lastIndex, lastTerm, term)
		if term != 0 { // 当日志与leader不一致，删除内存中不一致数据同任期日志记录
			for i, entry := range l.logEnties {
				if entry.Term == term {
					l.logEnties = l.logEnties[:i]
					break
				}
			}
		}
	}
	return b
}

// AppendEntry 添加日志条目
func (l *RaftLog) AppendEntry(entry []*pb.LogEntry) {

	size := len(entry)
	if size == 0 {
		return
	}
	//进行添加
	l.logEnties = append(l.logEnties, entry...)
	// 设置最后一次追加的索引
	l.lastAppendIndex = entry[size-1].Index
}

// Apply 日志提交
func (l *RaftLog) Apply(lastCommit, lastLogIndex uint64) {
	// 更新可提交索引
	if lastCommit > l.commitIndex {
		//更新成两个之中的最小值
		if lastLogIndex > lastCommit {
			l.commitIndex = lastCommit
		} else {
			l.commitIndex = lastLogIndex
		}
	}

	// 提交索引
	if l.commitIndex > l.lastAppliedIndex { // 检查 l.commitIndex 是否大于 l.lastAppliedIndex，以确定是否有新的条目需要提交
		n := 0
		for i, entry := range l.logEnties {
			if l.commitIndex >= entry.Index {
				n = i
			} else {
				break
			}
		}
		entries := l.logEnties[:n+1]
		//进行持久化
		l.storage.Append(entries)
		l.lastAppliedIndex = l.logEnties[n].Index
		l.lastAppliedTerm = l.logEnties[n].Term
		l.logEnties = l.logEnties[n+1:]

		l.NotifyReadIndex()
	}
}
