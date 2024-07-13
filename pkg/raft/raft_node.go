package raft

import "time"

type RaftNode struct {
	raft   *Raft                  // raft实例
	recvc  chan *pb.RaftMessage   // 一般消息接收通道
	propc  chan *pb.RaftMessage   // 提议消息接收通道
	sendc  chan []*pb.RaftMessage // 消息发送通道
	stopc  chan struct{}          // 停止
	ticker *time.Ticker           // 定时器(选取、心跳)
	logger *zap.SugaredLogger
}
