package raft

type ReplicaProgress struct {
	MatchIndex     uint64   // 已接收日志
	NextIndex      uint64   // 下次发送日志
	pending        []uint64 // 未发送完成日志
	prevResp       bool     // 上次日志发送结果
	maybeLostIndex uint64   // 可能丢失的日志,记上次发送未完以重发
}

// IsPause 检查发送状态，如上次发送未完成，暂缓发送
func (rp *ReplicaProgress) IsPause() bool {
	return (!rp.prevResp && len(rp.pending) > 0)
}

func (rp *ReplicaProgress) AppendEntry(lastIndex uint64) {
	rp.pending = append(rp.pending, lastIndex)
	// 上次发送成功时，假设本次也会成功，如发送失败再回退发送进度
	if rp.prevResp {
		rp.NextIndex = lastIndex + 1
	}
}
