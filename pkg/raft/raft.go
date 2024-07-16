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

// BroadcastAppendEntries 进行广播追加的日志记录
func (r *Raft) BroadcastAppendEntries() {
	r.cluster.Foreach(func(id uint64, _ *ReplicaProgress) {
		//这里是通过迭代的方式发送的，如果是自己的话，就进行返回
		if id == r.id {
			return
		}
		r.SendAppendEntries(id)
	})
}

// SendAppendEntries 发送追加的日志记录
func (r *Raft) SendAppendEntries(to uint64) {
	p := r.cluster.progress[to]
	if p == nil || p.IsPause() {
		return
	}

	nextIndex := r.cluster.GetNextIndex(to)
	lastLogIndex := nextIndex - 1
	lastLogTerm := r.raftlog.GetTerm(lastLogIndex)
	maxSize := MAX_LOG_ENTRY_SEND
	// 如果上一次发送失败的情况下，最多只能在发送一个了
	if !p.prevResp {
		maxSize = 1
	}
	// var entries []*pb.LogEntry
	// 获取日志条目
	entries := r.raftlog.GetEntries(nextIndex, maxSize)
	size := len(entries)
	if size > 0 {
		//  更新集群中目标节点的进度信息
		r.cluster.AppendEntry(to, entries[size-1].Index)
	}

	r.send(&pb.RaftMessage{
		MsgType:      pb.MessageType_APPEND_ENTRY,
		Term:         r.currentTerm,
		From:         r.id,
		To:           to,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
		LastCommit:   r.raftlog.commitIndex,
		Entry:        entries,
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

// HandleMessage 消息处理
func (r *Raft) HandleMessage(msg *pb.RaftMessage) {
	if msg == nil {
		return
	}

	// 消息任期小于节点任期,拒绝消息: 1、网络延迟，节点任期是集群任期; 2、网络断开,节点增加了任期，集群任期是消息任期
	if msg.Term < r.currentTerm {
		r.logger.Debugf("收到来自 %s 过期 (%d) %s 消息 ", strconv.FormatUint(msg.From, 16), msg.Term, msg.MsgType)
		return
	} else if msg.Term > r.currentTerm { // 如果消息的任期大于当前的任期
		// 消息非请求投票，集群发生选举，新任期产生
		if msg.MsgType != pb.MessageType_VOTE {
			// 日志追加、心跳、快照为leader发出，，节点成为该leader追随者
			if msg.MsgType == pb.MessageType_APPEND_ENTRY || msg.MsgType == pb.MessageType_HEARTBEAT || msg.MsgType == pb.MessageType_INSTALL_SNAPSHOT {
				r.SwitchFollower(msg.From, msg.Term)
			} else { // 变更节点为追随者，等待leader消息
				r.SwitchFollower(msg.From, 0)
			}
		}
	}

	r.hanbleMessage(msg)
}

// HandleCandidateMessage canidate消息处理
func (r *Raft) HandleCandidateMessage(msg *pb.RaftMessage) {
	switch msg.MsgType {
	//发起投票
	case pb.MessageType_VOTE:
		grant := r.ReciveRequestVote(msg.Term, msg.From, msg.LastLogTerm, msg.LastLogIndex)
		if grant { // 投票后重置选举时间
			r.electtionTick = 0
		}
		//投票回应
	case pb.MessageType_VOTE_RESP:
		r.ReciveVoteResp(msg.From, msg.Term, msg.LastLogTerm, msg.LastLogIndex, msg.Success)
	case pb.MessageType_HEARTBEAT:
		r.SwitchFollower(msg.From, msg.Term)
		r.ReciveHeartbeat(msg.From, msg.Term, msg.LastLogIndex, msg.LastCommit, msg.Context)
	case pb.MessageType_APPEND_ENTRY:
		r.SwitchFollower(msg.From, msg.Term)
		r.ReciveAppendEntries(msg.From, msg.Term, msg.LastLogTerm, msg.LastLogIndex, msg.LastCommit, msg.Entry)
	default:
		r.logger.Debugf("收到 %s 异常消息 %s 任期 %d", strconv.FormatUint(msg.From, 16), msg.MsgType, msg.Term)
	}
}

// ReciveHeartbeat 心跳处理
func (r *Raft) ReciveHeartbeat(mFrom, mTerm, mLastLogIndex, mLastCommit uint64, context []byte) {
	lastLogIndex, _ := r.raftlog.GetLastLogIndexAndTerm()
	r.raftlog.Apply(mLastCommit, lastLogIndex)

	r.send(&pb.RaftMessage{
		MsgType: pb.MessageType_HEARTBEAT_RESP,
		Term:    r.currentTerm,
		From:    r.id,
		To:      mFrom,
		Context: context,
	})
}

// SwitchLeader 切换leader
func (r *Raft) SwitchLeader() {
	r.logger.Debugf("成为领导者, 任期: %d", r.currentTerm)

	r.state = LEADER_STATE
	r.leader = r.id
	r.voteFor = 0
	// r.cluster.ResetVoteResult()
	//更新时钟Tick()为心跳时钟处理方法
	r.Tick = r.TickHeartbeat
	//消息处理为leader消息处理方法
	r.hanbleMessage = r.HandleLeaderMessage
	r.electtionTick = 0
	r.hearbeatTick = 0
	r.cluster.Reset()
}

// HandleLeaderMessage leader消息处理方法
func (r *Raft) HandleLeaderMessage(msg *pb.RaftMessage) {
	switch msg.MsgType {
	case pb.MessageType_PROPOSE:
		r.AppendEntry(msg.Entry)
	case pb.MessageType_VOTE:
		r.ReciveRequestVote(msg.Term, msg.From, msg.LastLogTerm, msg.LastLogIndex)
	case pb.MessageType_VOTE_RESP:
		break
	case pb.MessageType_HEARTBEAT_RESP:
		r.ReciveHeartbeatResp(msg.From, msg.Term, msg.LastLogIndex, msg.Context)
	case pb.MessageType_APPEND_ENTRY_RESP:
		r.ReciveAppendEntriesResult(msg.From, msg.Term, msg.LastLogIndex, msg.Success)
	default:
		r.logger.Debugf("收到 %s 异常消息 %s 任期 %d", strconv.FormatUint(msg.From, 16), msg.MsgType, msg.Term)
	}
}

// TickHeartbeat 心跳时钟
func (r *Raft) TickHeartbeat() {
	//心跳加一
	r.hearbeatTick++

	lastIndex, _ := r.raftlog.GetLastLogIndexAndTerm()
	if r.hearbeatTick >= r.heartbeatTimeout {
		r.hearbeatTick = 0
		r.BroadcastHeartbeat(nil)
		r.cluster.Foreach(func(id uint64, p *ReplicaProgress) {
			if id == r.id {
				return
			}

			pendding := len(p.pending)
			// 重发消息，重发条件：
			// 上次消息发送未响应且当前有发送未完成,且上次心跳该消息就已处于等待响应状态
			// 当前无等待响应消息，且节点下次发送日志小于leader最新日志
			if !p.prevResp && pendding > 0 && p.MaybeLogLost(p.pending[0]) || (pendding == 0 && p.NextIndex <= lastIndex) {
				p.pending = nil
				r.SendAppendEntries(id)
			}
		})
	}
}

// BroadcastHeartbeat 心跳广播
func (r *Raft) BroadcastHeartbeat(context []byte) {
	r.cluster.Foreach(func(id uint64, p *ReplicaProgress) {
		if id == r.id {
			return
		}
		lastLogIndex := p.NextIndex - 1
		lastLogTerm := r.raftlog.GetTerm(lastLogIndex)
		r.send(&pb.RaftMessage{
			MsgType:      pb.MessageType_HEARTBEAT,
			Term:         r.currentTerm,
			From:         r.id,
			To:           id,
			LastLogIndex: lastLogIndex,
			LastLogTerm:  lastLogTerm,
			LastCommit:   r.raftlog.commitIndex,
			Context:      context,
		})
	})
}

// SwitchFollower 切换follower方法
func (r *Raft) SwitchFollower(leaderId, term uint64) {
	//切换状态以及相关的信息
	r.state = FOLLOWER_STATE
	r.leader = leaderId
	r.currentTerm = term
	// 重置投票，意味着没有进行投票
	r.voteFor = 0
	// 设置一个随机的选举超时时间。每个节点的选举超时时间都是随机的，这有助于避免多个节点同时开始新选举的问题。
	r.randomElectionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)
	r.Tick = r.TickElection                   // 从原来的选举超时检查转换为新的选举超时检查函数。
	r.hanbleMessage = r.HandleFollowerMessage //更改成追随者的角色消息处理函数
	r.electtionTick = 0                       // 选举超时计数器重置为 0
	r.cluster.Reset()

	r.logger.Debugf("成为追随者, 领导者 %s, 任期 %d , 选举周期 %d s", strconv.FormatUint(leaderId, 16), term, r.randomElectionTimeout)
}

// HandleFollowerMessage follower消息处理
func (r *Raft) HandleFollowerMessage(msg *pb.RaftMessage) {
	switch msg.MsgType {
	case pb.MessageType_VOTE:
		grant := r.ReciveRequestVote(msg.Term, msg.From, msg.LastLogTerm, msg.LastLogIndex)
		if grant {
			r.electtionTick = 0
		}
	case pb.MessageType_HEARTBEAT:
		r.electtionTick = 0
		r.ReciveHeartbeat(msg.From, msg.Term, msg.LastLogIndex, msg.LastCommit, msg.Context)
	case pb.MessageType_APPEND_ENTRY:
		r.electtionTick = 0
		r.ReciveAppendEntries(msg.From, msg.Term, msg.LastLogTerm, msg.LastLogIndex, msg.LastCommit, msg.Entry)
	default:
		r.logger.Debugf("收到 %s 异常消息 %s 任期 %d", strconv.FormatUint(msg.From, 16), msg.MsgType, msg.Term)
	}
}

// NewRaft 实例化raft
func NewRaft(id uint64, peers map[uint64]string, logger *zap.SugaredLogger) *Raft {
	raftlog := NewRaftLog(storage, logger)
	raft := &Raft{
		id:               id,
		currentTerm:      raftlog.lastAppliedTerm,
		raftlog:          raftlog,
		cluster:          NewCluster(peers, raftlog.commitIndex, logger),
		electionTimeout:  10,
		heartbeatTimeout: 5,
		logger:           logger,
	}

	logger.Infof("实例: %s ,任期: %d ", strconv.FormatUint(raft.id, 16), raft.currentTerm)
	raft.SwitchFollower(0, raft.currentTerm)

	return raft
}

// 追加日志的条目
func (r *Raft) AppendEntry(entries []*pb.LogEntry) {
	// 从日志中取得最新日志编号，遍历待追加日志，设置日志编号
	lastLogIndex, _ := r.raftlog.GetLastLogIndexAndTerm()
	for i, entry := range entries {
		entry.Index = lastLogIndex + 1 + uint64(i)
		entry.Term = r.currentTerm
	}
	// 追加日志到内存切片
	//更新leader追加进度
	r.raftlog.AppendEntry(entries)
	r.cluster.UpdateLogIndex(r.id, entries[len(entries)-1].Index)
	// 广播日志到集群
	r.BroadcastAppendEntries()
}

func (r *Raft) ReciveAppendEntries(mLeader, mTerm, mLastLogTerm, mLastLogIndex, mLastCommit uint64, mEntries []*pb.LogEntry) {
	var accept bool
	// 检查本地日志是否包含与 leader 发送的 LastLogIndex 和 LastLogTerm 匹配的条目
	if !r.raftlog.HasPrevLog(mLastLogIndex, mLastLogTerm) { // 检查节点日志是否与leader一致
		r.logger.Infof("节点未含有上次追加日志: Index: %d, Term: %d ", mLastLogIndex, mLastLogTerm)
		accept = false
	} else {
		r.raftlog.AppendEntry(mEntries)
		accept = true
	}

	lastLogIndex, lastLogTerm := r.raftlog.GetLastLogIndexAndTerm()
	r.raftlog.Apply(mLastCommit, lastLogIndex)
	r.send(&pb.RaftMessage{
		MsgType:      pb.MessageType_APPEND_ENTRY_RESP,
		Term:         r.currentTerm,
		From:         r.id,
		To:           mLeader,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
		Success:      accept,
	})
}

func (r *Raft) ReciveAppendEntriesResult(from, term, lastLogIndex uint64, success bool) {
	leaderLastLogIndex, _ := r.raftlog.GetLastLogIndexAndTerm()
	if success {
		r.cluster.AppendEntryResp(from, lastLogIndex)
		if lastLogIndex > r.raftlog.commitIndex {
			// 取已同步索引更新到lastcommit
			if r.cluster.CheckCommit(lastLogIndex) {
				prevApplied := r.raftlog.lastAppliedIndex
				r.raftlog.Apply(lastLogIndex, lastLogIndex)
				r.BroadcastAppendEntries()
			}
		} else if len(r.raftlog.waitQueue) > 0 {
			r.raftlog.NotifyReadIndex()
		}
		if r.cluster.GetNextIndex(from) <= leaderLastLogIndex {
			r.SendAppendEntries(from)
		}
	} else {
		r.logger.Infof("节点 %s 追加日志失败, Leader记录节点最新日志: %d ,节点最新日志: %d ", strconv.FormatUint(from, 16), r.cluster.GetNextIndex(from)-1, lastLogIndex)

		r.cluster.ResetLogIndex(from, lastLogIndex, leaderLastLogIndex)
		r.SendAppendEntries(from)
	}
}
