package server

import (
	"context"
	"fmt"
	"io"
	"strconv"
	"xcRaft/pkg/raft"
)

type RaftServer struct {
	pb.RaftServer

	id           uint64
	name         string
	peerAddress  string
	raftServer   *grpc.Server
	incomingChan chan *pb.RaftMessage
	peers        map[uint64]*Peer
	node         *raft.RaftNode
	close        bool
	stopc        chan struct{}
	logger       *zap.SugaredLogger
}

func (s *RaftServer) Consensus(stream pb.Raft_ConsensusServer) error {
	msg, err := stream.Recv()
	if err == io.EOF {
		s.logger.Debugf("流读取结束")
		return nil
	}
	if err != nil {
		s.logger.Debugf("流读取异常: %v", err)
		return err
	}
	return s.addServerPeer(stream, msg)
}

func (s *RaftServer) addServerPeer(stream pb.Raft_ConsensusServer, msg *pb.RaftMessage) error {

	p, isMember := s.peers[msg.From]
	if !isMember {
		s.logger.Debugf("收到非集群节点 %s 消息 %s", strconv.FormatUint(msg.From, 16), msg.String())

		p = NewPeer(msg.From, "", s.incomingChan, s.metric, s.logger)
		s.tmpPeers[msg.From] = p
		s.node.Process(context.Background(), msg)
		p.Recv()
		return fmt.Errorf("非集群节点")
	}

	s.logger.Debugf("添加 %s 读写流", strconv.FormatUint(msg.From, 16))
	if p.SetStream(stream) {
		s.node.Process(context.Background(), msg)
		p.Recv()
	}
	return nil
}
