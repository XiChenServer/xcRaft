package server

import (
	"context"
	"crypto/sha1"
	"encoding/binary"
	"fmt"
	"io"
	"net"
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

// 解析来源编号
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
	// 没有临时保存
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

// 处理
func (s *RaftServer) handle() {
	go func() {
		for {
			select {
			case <-s.stopc:
				return
			case msgs := <-s.node.SendChan():
				s.sendMsg(msgs)
			case msg := <-s.incomingChan:
				s.process(msg)
			}
		}
	}()
}

func (s *RaftServer) sendMsg(msgs []*pb.RaftMessage) {
	msgMap := make(map[uint64][]*pb.RaftMessage, len(s.peers)-1)

	for _, msg := range msgs {
		if s.peers[msg.To] == nil {
			p := s.tmpPeers[msg.To]
			if p != nil {
				p.send(msg)
			}
			p.Stop()
			delete(s.tmpPeers, msg.To)
			continue
		} else {
			if msgMap[msg.To] == nil {
				msgMap[msg.To] = make([]*pb.RaftMessage, 0)
			}
			msgMap[msg.To] = append(msgMap[msg.To], msg)
		}
	}
	for k, v := range msgMap {
		if len(v) > 0 {
			s.peers[k].SendBatch(v)
		}
	}
}

func (s *RaftServer) process(msg *pb.RaftMessage) (err error) {
	defer func() {
		if reason := recover(); reason != nil {
			err = fmt.Errorf("处理消息 %s 失败:%v", msg.String(), reason)
		}
	}()
	return s.node.Process(context.Background(), msg)
}

// 进行启动
func (s *RaftServer) Start() {

	lis, err := net.Listen("tcp", s.peerAddress)
	if err != nil {
		s.logger.Errorf("对等节点服务器失败: %v", err)
	}
	var opts []grpc.ServerOption
	s.raftServer = grpc.NewServer(opts...)

	s.logger.Infof("对等节点服务器启动成功 %s", s.peerAddress)

	pb.RegisterRaftServer(s.raftServer, s)

	s.handle()

	err = s.raftServer.Serve(lis)
	if err != nil {
		s.logger.Errorf("Raft内部服务器关闭: %v", err)
	}
}

type Config struct {
	Name        string
	PeerAddress string
	Peers       map[string]string
	Logger      *zap.SugaredLogger
}

func GenerateNodeId(name string) uint64 {
	hash := sha1.Sum([]byte(name))
	return binary.BigEndian.Uint64(hash[:8])
}

func Bootstrap(conf *Config) *RaftServer {

	var nodeId uint64
	var node *raft.RaftNode
	servers := make(map[uint64]*Peer, len(conf.Peers))

	if len(peers) != 0 {
		nodeId = GenerateNodeId(conf.Name)
		node = raft.NewRaftNode(nodeId, storage, peers, conf.Logger)
	} else {
		peers = make(map[uint64]string, len(conf.Peers))
		// 遍历节点配置，生成各节点id
		for name, address := range conf.Peers {
			id := GenerateNodeId(name)
			peers[id] = address
			if name == conf.Name {
				nodeId = id
			}
		}
		node = raft.NewRaftNode(nodeId, storage, peers, conf.Logger)
		node.InitMember(peers)
	}

	incomingChan := make(chan *pb.RaftMessage)
	// 初始化远端节点配置
	for id, address := range peers {
		conf.Logger.Infof("集群成员 %s 地址 %s", strconv.FormatUint(id, 16), address)
		if id == nodeId {
			continue
		}
		servers[id] = NewPeer(id, address, incomingChan, conf.Logger)
	}

	server := &RaftServer{
		logger:      conf.Logger,
		id:          nodeId,
		name:        conf.Name,
		peerAddress: conf.PeerAddress,
		peers:       servers,
		node:        node,
		stopc:       make(chan struct{}),
	}

	return server
}
