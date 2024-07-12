package raft

import (
	"math/rand"
	"strconv"
)

// 最大的日志条目记录发送数
const MAX_LOG_ENTRY_SEND = 1000

// raft节点类型
type RaftState int

// raft 节点类型
const (
	CANDIDATE_STATE RaftState = iota
	FOLLOWER_STATE
	LEADER_STATE
)

type Raft struct {
	id                    uint64
	state                 RaftState             // 节点类型
	leader                uint64                // leader id
	currentTerm           uint64                // 当前任期
	voteFor               uint64                // 投票对象
	raftlog               *RaftLog              // 日志
	cluster               *Cluster              // 集群节点
	electionTimeout       int                   // 选取周期
	heartbeatTimeout      int                   // 心跳周期
	randomElectionTimeout int                   // 随机选取周期
	electtionTick         int                   // 选取时钟
	hearbeatTick          int                   // 心跳时钟
	Tick                  func()                // 时钟函数,Leader为心跳时钟，其他为选取时钟
	hanbleMessage         func(*pb.RaftMessage) // 消息处理函数,按节点状态对应不同处理
	Msg                   []*pb.RaftMessage     // 待发送消息
	ReadIndex             []*ReadIndexResp      // 检查Leader完成的readindex
	logger                *zap.SugaredLogger
}

// 选取时钟跳动
func (r *Raft) TickElection() {
	//在每次调用的时候时钟加一
	r.electtionTick++
	// 判断这个计时器有没有大于等于随机选举超时值
	if r.electtionTick >= r.randomElectionTimeout {
		// 将计时器置零
		r.electtionTick = 0
		//如果是一个候选人的话，就进行广播请求投票
		if r.state == CANDIDATE_STATE {
			r.BroadcastRequestVote()
		}
		// 如果是从节点的话，就需要变成候选人，因为一个周期没有收到消息了
		if r.state == FOLLOWER_STATE {
			r.SwitchCandidate()
		}
	}
}

func (r *Raft) SwitchCandidate() {
	//变为竞选者节点
	r.state = CANDIDATE_STATE
	//这里表示没有领导节点
	r.leader = 0
	//随机选举周期 等于 基础选举周期+随机选举周期
	r.randomElectionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)
	//这里的时钟函数是选取时钟函数
	r.Tick = r.TickElection
	//处理消息的函数也是竞选者的
	r.hanbleMessage = r.HandleCandidateMessage
	//进行广播进行选举
	r.BroadcastRequestVote()
	//对于选取时钟的计时器进行初始化
	r.electtionTick = 0
	r.logger.Debugf("成为候选者, 任期 %d , 选举周期 %d s", r.currentTerm, r.randomElectionTimeout)
}

func (r *Raft) BroadcastRequestVote() {
	//将当前的任期更加
	r.currentTerm++
	//投票给自己
	r.voteFor = r.id
	//重置集群的投票结果，这可能涉及清除以前选举周期的投票信息。
	r.cluster.ResetVoteResult()
	//为自己投赞成票。这通常是作为候选人的第一个动作。
	r.cluster.Vote(r.id, true)

	r.logger.Infof("%s 发起投票", strconv.FormatUint(r.id, 16))
	//遍历集群中的每个节点（除了自己），并为每个节点发送一个 RequestVote 消息
	r.cluster.Foreach(func(id uint64, p *ReplicaProgress) {
		if id == r.id {
			return
		}
		//获取最后一次发送者最后一条日志条目的索引和任期号
		lastLogIndex, lastLogTerm := r.raftlog.GetLastLogIndexAndTerm()
		//进行发送消息
		r.send(&pb.RaftMessage{
			MsgType:      pb.MessageType_VOTE,
			Term:         r.currentTerm,
			From:         r.id,
			To:           id,
			LastLogIndex: lastLogIndex,
			LastLogTerm:  lastLogTerm,
		})
	})
}

// 将数据添加到消息切片，后续在外部读取，发送给其他节点
func (r *Raft) send(msg *pb.RaftMessage) {
	r.Msg = append(r.Msg, msg)
}

func (r *Raft) ReciveRequestVote(mTerm, mCandidateId, mLastLogTerm, mLastLogIndex uint64) (success bool) {
	// 获取本地节点的最后一条日志的索引和任期号
	lastLogIndex, lastLogTerm := r.raftlog.GetLastLogIndexAndTerm()
	// 如果本地节点尚未投票或者已经投票给当前候选人
	if r.voteFor == 0 || r.voteFor == mCandidateId {
		// 检查请求投票的候选人的任期号是否大于本地节点的当前任期号
		// 并且其最后一条日志的任期号和索引都大于或等于本地节点的最后一条日志的任期号和索引
		if mTerm > r.currentTerm && mLastLogTerm >= lastLogTerm && mLastLogIndex >= lastLogIndex {
			r.voteFor = mCandidateId
			success = true
		}
	}

	r.logger.Debugf("候选人: %s, 投票: %t ", strconv.FormatUint(mCandidateId, 16), success)
	// 向请求投票的候选人发送VoteResponse消息
	// 消息内容包括消息类型、任期号、发送者ID、接收者ID、最后一条日志索引、最后一条日志任期号以及投票结果
	r.send(&pb.RaftMessage{
		MsgType:      pb.MessageType_VOTE_RESP,
		Term:         mTerm,
		From:         r.id,
		To:           mCandidateId,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
		Success:      success,
	})
	return
}

// ReciveVoteResp 获取节点投票响应
func (r *Raft) ReciveVoteResp(from, term, lastLogTerm, lastLogIndex uint64, success bool) {
	// 获取最后一次的日志的索引
	leaderLastLogIndex, _ := r.raftlog.GetLastLogIndexAndTerm()
	// 记录发送者ID和投票结果
	r.cluster.Vote(from, success)
	//重置 Cluster 结构体中记录的来自特定节点的日志索引，确保所有节点的日志索引与当前领导者的日志索引同步。
	r.cluster.ResetLogIndex(from, lastLogIndex, leaderLastLogIndex)
	// 检查投票结果
	voteRes := r.cluster.CheckVoteResult()
	// 选举成功
	if voteRes == VoteWon {
		r.logger.Debugf("节点 %s 发起投票, 赢得选举", strconv.FormatUint(r.id, 16))
		// 遍历 Cluster 结构体中的 voteResp 字典，对于那些没有投票给当前节点的节点（其值为 false），通过 ResetLogIndex 方法重置它们的日志索引
		for k, v := range r.cluster.voteResp {
			if !v {
				r.cluster.ResetLogIndex(k, lastLogIndex, leaderLastLogIndex)
			}
		}
		// 自己的角色转换为领导者
		r.SwitchLeader()
		// 广播 AppendEntries 消息给集群中的所有其他节点，开始日志复制的过程。
		r.BroadcastAppendEntries()
	} else if voteRes == VoteLost { //输掉了选举
		r.logger.Debugf("节点 %s 发起投票, 输掉选举", strconv.FormatUint(r.id, 16))
		// 不支持任何特定的候选者
		r.voteFor = 0
		// 重置 Cluster 结构体中的投票结果
		r.cluster.ResetVoteResult()
	}
}
