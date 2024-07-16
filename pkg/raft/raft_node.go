package raft

import (
	"context"
	"time"
)

// 内部消息接收流程： grpc server -> raftNode.recvc -> raft.handleMessage()
//客户端消息接收流程: raft server -> raftNode.propc -> raft.AppendEntry()
//消息发送流程：raft.send() -> raft.Msg -> raftNode.sendc -> grpc server

type RaftNode struct {
	raft   *Raft                  // raft实例
	recvc  chan *pb.RaftMessage   // 一般消息接收通道
	propc  chan *pb.RaftMessage   // 提议消息接收通道
	sendc  chan []*pb.RaftMessage // 消息发送通道
	stopc  chan struct{}          // 停止
	ticker *time.Ticker           // 定时器(选取、心跳)
	logger *zap.SugaredLogger
}

// NewRaftNode 初始化各个通道
func NewRaftNode(id uint64, peers map[uint64]string, logger *zap.SugaredLogger) *RaftNode {

	node := &RaftNode{
		raft:   NewRaft(id, storage, peers, logger),
		recvc:  make(chan *pb.RaftMessage),
		propc:  make(chan *pb.RaftMessage),
		sendc:  make(chan []*pb.RaftMessage),
		stopc:  make(chan struct{}),
		ticker: time.NewTicker(time.Second),
		logger: logger,
	}

	node.Start()
	return node
}

// Start raftNode主循环
func (n *RaftNode) Start() {
	go func() {
		var propc chan *pb.RaftMessage
		var sendc chan []*pb.RaftMessage

		for {
			var msgs []*pb.RaftMessage
			// 存在待发送消息，启用发送通道以发送
			if len(n.raft.Msg) > 0 {
				msgs = n.raft.Msg
				sendc = n.sendc
			} else { // 无消息发送隐藏发送通道
				sendc = nil
			}
			// 这里是使用select case 和channel共同操作，对于时钟，发送通道，接收通道，提案通道，选择一个执行
			select {
			case <-n.ticker.C:
				n.raft.Tick()
			case msg := <-n.recvc:
				n.raft.HandleMessage(msg)
			case msg := <-propc:
				n.raft.HandleMessage(msg)
			case sendc <- msgs:
				n.raft.Msg = nil
			case <-n.stopc:
				return
			}
		}
	}()
}

// Process 将数据添加到读取通道
func (n *RaftNode) Process(ctx context.Context, msg *pb.RaftMessage) error {
	var ch chan *pb.RaftMessage
	if msg.MsgType == pb.MessageType_PROPOSE {
		ch = n.propc
	} else {
		ch = n.recvc
	}

	select {
	case ch <- msg:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// SendChan 发送数据
func (n *RaftNode) SendChan() chan []*pb.RaftMessage {
	return n.sendc
}
func (n *RaftNode) Propose(ctx context.Context, entries []*pb.LogEntry) error {
	msg := &pb.RaftMessage{
		MsgType: pb.MessageType_PROPOSE,
		Term:    n.raft.currentTerm,
		Entry:   entries,
	}
	return n.Process(ctx, msg)
}
