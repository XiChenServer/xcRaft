package server

import (
	"context"
	"io"
	"strconv"
	"sync"
	"xcRaft/pkg/raft"
)

type Stream interface {
	Send(*pb.RaftMessage) error
	Recv() (*pb.RaftMessage, error)
}

type Remote struct {
	address      string
	conn         *grpc.ClientConn
	client       pb.RaftClient
	clientStream pb.Raft_ConsensusClient
	serverStream pb.Raft_ConsensusServer
}

type Peer struct {
	mu     sync.Mutex
	wg     sync.WaitGroup // 等待组，用于等待 Peer 结构体相关的所有操作完成，确保在执行某些清理或结束操作时，所有相关的任务都已经完成
	id     uint64
	node   *raft.RaftNode       // raft节点实例
	stream Stream               // grpc双向流
	recvc  chan *pb.RaftMessage // 流读取数据发送通道
	remote *Remote              // 远端连接信息
	close  bool                 // 是否准备关闭
	logger *zap.SugaredLogger
}

// 发送消息
func (p *Peer) send(msg *pb.RaftMessage) {
	if msg == nil {
		return
	}
	// 如果双方的流没有建立好，先进行连接
	if p.stream == nil {
		if err := p.Connect(); err != nil {
			return
		}
	}

	if err := p.stream.Send(msg); err != nil {
		p.logger.Errorf("发送消息 %s 到 %s 失败 ,日志数量: %d %v", msg.MsgType.String(), strconv.FormatUint(msg.To, 16), len(msg.Entry), err)
		return
	}
}

// SendBatch 向一个特定的Peer批量发送
func (p *Peer) SendBatch(msgs []*pb.RaftMessage) {
	for _, msg := range msgs {
		p.send(msg)
	}
}

// SetStream 设置流到对等节点信息中
func (p *Peer) SetStream(stream pb.Raft_ConsensusServer) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	// 如果为空，表示当前没有流关联到这个 Peer
	if p.stream == nil {
		p.stream = stream
		p.remote.serverStream = stream
		//这里检查，看是否存在上一个客户端流，如果有的话，需要进行释放资源
		if p.remote.clientStream != nil {
			p.remote.clientStream.CloseSend()
			p.remote.conn.Close()
			p.remote.clientStream = nil
			p.remote.conn = nil
		}

		return true
	}
	return false
}

// Connect 进行连接
func (p *Peer) Connect() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	// 如果为空，表示当前没有流关联到这个 Peer
	if p.stream != nil {
		return nil
	}
	// 如果为空，说明还没有建立到远程节点的连接。
	if p.remote.conn == nil {
		var opts []grpc.DialOption
		opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))

		conn, err := grpc.Dial(p.remote.address, opts...)
		if err != nil {
			p.logger.Errorf("创建连接 %s 失败: %v", strconv.FormatUint(p.id, 16), err)
			return err
		}

		p.remote.conn = conn
		p.remote.client = pb.NewRaftClient(conn)
	}

	return p.Reconnect()
}

// 用于重连
func (p *Peer) Reconnect() error {

	if p.remote.clientStream != nil {
		p.remote.clientStream.CloseSend()
		p.remote.clientStream = nil
		p.stream = nil
	}

	stream, err := p.remote.client.Consensus(context.Background())
	// var delay time.Duration
	for err != nil {
		p.logger.Errorf("连接raft服务 %s 失败: %v", p.remote.address, err)
		return err
	}

	p.logger.Debugf("创建 %s 读写流", strconv.FormatUint(p.id, 16))
	p.stream = stream
	p.remote.clientStream = stream

	go p.Recv()
	return nil
}

// 循环从流中读取raft消息
func (p *Peer) Recv() {
	// 接收消息
	for {
		msg, err := p.stream.Recv()
		if err == io.EOF {
			p.stream = nil
			p.logger.Errorf("读取 %s 流结束", strconv.FormatUint(p.id, 16))
			return
		}

		if err != nil {
			p.stream = nil
			p.logger.Errorf("读取 %s 流失败： %v", strconv.FormatUint(p.id, 16), err)
			return
		}
		//将数据发送给raft server
		p.recvc <- msg
	}
}

func (p *Peer) SendBatch(msgs []*pb.RaftMessage) {
	p.wg.Add(1)
	var appEntryMsg *pb.RaftMessage
	var propMsg *pb.RaftMessage
	for _, msg := range msgs {
		if msg.MsgType == pb.MessageType_APPEND_ENTRY {
			if appEntryMsg == nil {
				appEntryMsg = msg
			} else {
				size := len(appEntryMsg.Entry)
				if size == 0 || len(msg.Entry) == 0 || appEntryMsg.Entry[size-1].Index+1 == msg.Entry[0].Index {
					appEntryMsg.LastCommit = msg.LastCommit
					appEntryMsg.Entry = append(appEntryMsg.Entry, msg.Entry...)
				} else if appEntryMsg.Entry[0].Index >= msg.Entry[0].Index {
					appEntryMsg = msg
				}
			}
		} else if msg.MsgType == pb.MessageType_PROPOSE {
			if propMsg == nil {
				propMsg = msg
			} else {
				propMsg.Entry = append(propMsg.Entry, msg.Entry...)
			}
		} else {
			p.send(msg)
		}
	}

	if appEntryMsg != nil {
		p.send(appEntryMsg)
	}

	if propMsg != nil {
		p.send(propMsg)
	}
	p.wg.Done()
}
