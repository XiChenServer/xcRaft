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

func (l *RaftLog) GetEntries(index uint64, maxSize int) []*pb.LogEntry {
	// 请求日志已提交，从存储获取
	if index <= l.lastAppliedIndex {
		// 如果当前索引值加上最大条目数大于等于最后一条已应用的索引值，
		//则结束索引值为 lastAppliedIndex + 1，表示获取到最后一条已应用条目的下一条。
		//否则，结束索引值为 index + MAX_APPEND_ENTRY_SIZE，表示获取从当前索引值开始的最大条目数。
		endIndex := index + MAX_APPEND_ENTRY_SIZE
		if endIndex >= l.lastAppliedIndex {
			endIndex = l.lastAppliedIndex + 1
		}
		return l.storage.GetEntries(index, endIndex)
	} else { // 请求日志未提交,从数组获取
		var entries []*pb.LogEntry
		for i, entry := range l.logEnties {
			//找到索引值为 index 的条目，并将其后面的条目切片作为结果返回。
			//如果 logEntries 数组的长度减去当前条目索引值之后大于 maxSize，
			//则返回从当前条目的索引值开始的最大条目数切片；
			//否则，返回从当前条目的索引值开始到数组末尾的所有条目。
			if entry.Index == index {
				if len(l.logEnties)-i > maxSize {
					entries = l.logEnties[i : i+maxSize]
				} else {
					entries = l.logEnties[i:]
				}
				break
			}
		}
		return entries
	}
}

func (c *Cluster) AppendEntry(id uint64, lastIndex uint64) {
	p := c.progress[id]
	if p != nil {
		p.AppendEntry(lastIndex)
	}
}
