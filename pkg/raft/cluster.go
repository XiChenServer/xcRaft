package raft

type VoteResult int

// 选取状态
const (
	Voting VoteResult = iota
	VoteWon
	VoteLost
)

type Cluster struct {
	voteResp map[uint64]bool             // 投票节点
	progress map[uint64]*ReplicaProgress // 各节点进度
	logger   *zap.SugaredLogger
}

// Foreach 遍历节点进度
func (c *Cluster) Foreach(f func(id uint64, p *ReplicaProgress)) {
	for id, p := range c.progress {
		f(id, p)
	}
}

// ResetVoteResult 重置 Cluster 结构体中的投票结果字典，确保它被清理干净，以便下次投票时使用
func (c *Cluster) ResetVoteResult() {
	c.voteResp = make(map[uint64]bool)
}

// Vote 更新 Cluster 结构体中的投票记录
func (c *Cluster) Vote(id uint64, granted bool) {
	c.voteResp[id] = granted
}

// CheckVoteResult 检查选举的结果
func (c *Cluster) CheckVoteResult() VoteResult {
	granted := 0
	reject := 0
	// 统计承认/拒绝数量
	for _, v := range c.voteResp {
		if v {
			granted++
		} else {
			reject++
		}
	}

	// most := len(c.progress)/2 + 1
	half := len(c.progress) / 2
	// 多数承认->赢得选举
	if granted >= half+1 {
		return VoteWon
	} else if reject >= half { // 半数拒绝，选举失败
		return VoteLost
	}
	// 尚在选举
	return Voting
}