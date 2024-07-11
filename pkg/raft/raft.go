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

	r.cluster.Foreach(func(id uint64, p *ReplicaProgress) {
		if id == r.id {
			return
		}
		lastLogIndex, lastLogTerm := r.raftlog.GetLastLogIndexAndTerm()
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
