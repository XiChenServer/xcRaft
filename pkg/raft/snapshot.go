package raft

import (
	"fmt"
	"os"
	"path"
	"strconv"
)

type SnapshotSegment struct {
	LastIncludeIndex uint64                  // 最后包含日志
	LastIncludeTerm  uint64                  // 最后包含任期
	datac            []chan *lsm.RawNodeData // 数据读取通道
}

// 快照文件
type SnapshotFile struct {
	fd      *os.File
	level   int    // sst level
	segment int    // 文件在快照对应片段序号(SnapshotSegment.datac 下标)
	offset  uint64 // 已读取偏移
	done    bool   // 是否读取完成
}

type Snapshot struct {
	dir              string
	data             *lsm.Tree                // lsm 保存实际数据
	lastIncludeIndex uint64                   // 最后包含日志
	lastIncludeTerm  uint64                   // 最后包含任期
	installingSnap   map[string]*SnapshotFile // 对应快照文件
	logger           *zap.SugaredLogger
}

func (ss *Snapshot) GetSegment(index uint64) (chan *pb.Snapshot, error) {
	size := int64(4 * 1000 * 1000)
	send := make([]*SnapshotSegment, 0)
	tree := ss.data.GetNodes()
	var find bool

	// 0层文件最后包含日志完整，可单个发送
	for i := len(tree[0]) - 1; i >= 0; i-- {
		n := tree[0][i]
		lastIndex, lastTerm, err := getLastIncludeIndexAndTerm(n)
		if err != nil {
			return nil, fmt.Errorf("获取需发送快照失败: %v", err)
		}
		if lastIndex <= index {
			find = true
			break
		}
		ss.logger.Debugf("日志 %d 对应快照文件 %d_%d, 最后日志 %d 任期 %d", index, n.Level, n.SeqNo, lastIndex, lastTerm)
		send = append(send, &SnapshotSegment{
			LastIncludeIndex: lastIndex,
			LastIncludeTerm:  lastTerm,
			datac:            []chan *lsm.RawNodeData{n.ReadRaw(size)},
		})
	}

	if !find {
		// 非0层文件，最后包含日志在lsm合并时会按大小拆分，最后包含日志存在误差，需发送全部
		for i, level := range tree[1:] {
			var lastIndex uint64
			var lastTerm uint64
			for _, n := range level {
				nodeLastIndex, nodeLastTerm, err := getLastIncludeIndexAndTerm(n)
				if err != nil {
					return nil, fmt.Errorf("获取需发送快照失败: %v", err)
				}
				if nodeLastIndex > lastIndex {
					lastIndex = nodeLastIndex
					lastTerm = nodeLastTerm
				}
			}
			if lastIndex > 0 {
				datac := make([]chan *lsm.RawNodeData, len(level))
				for j, n := range level {
					datac[j] = n.ReadRaw(size)
				}
				send = append(send, &SnapshotSegment{
					LastIncludeIndex: lastIndex,
					LastIncludeTerm:  lastTerm,
					datac:            datac,
				})
			}
		}
	}
	snapc := make(chan *pb.Snapshot)
	go ss.readSnapshot(send, snapc)
	return snapc, nil
}
func (ss *Snapshot) readSnapshot(send []*SnapshotSegment, snapc chan *pb.Snapshot) {
	defer close(snapc)
	// 倒序遍历待发送快照，逐个读取文件发送
	for i := len(send) - 1; i >= 0; i-- {
		for j := len(send[i].datac) - 1; j >= 0; j-- {
			readc := send[i].datac[j]
			for {
				data := <-readc
				if data == nil {
					break
				}
				if data.Err != nil {
					ss.logger.Errorf("读取快照文件 %d_%d 失败: %v", data.Level, data.SeqNo, data.Err)
					return
				}
				snap := &pb.Snapshot{
					LastIncludeIndex: send[i].LastIncludeIndex,
					LastIncludeTerm:  send[i].LastIncludeTerm,
					Level:            uint32(data.Level),
					Segment:          uint32(j),
					Data:             data.Data,
					Offset:           uint64(data.Offset),
					Done:             data.Done,
				}
				snapc <- snap
				if data.Done {
					break
				}
			}
		}
	}
}
func (n *Node) ReadRaw(perSize int64) chan *RawNodeData {
	readc := make(chan *RawNodeData)
	remain := n.FileSize
	var offset int64
	var data []byte
	var err error
	var done bool

	n.wg.Add(1)
	go func() {
		defer func() {
			close(readc)
			n.wg.Done()
		}()
		for remain > 0 {
			if remain > perSize {
				data, err = n.sr.Read(offset, perSize)
			} else {
				data, err = n.sr.Read(offset, remain)
				if err == nil {
					done = true
				}
			}
			if err != nil {
				err = fmt.Errorf("读取 %d_%d_%s 数据失败: %v", n.Level, n.SeqNo, n.Extra, err)
			}
			readc <- &RawNodeData{Level: n.Level, SeqNo: n.SeqNo, Offset: offset, Data: data, Done: done, Err: err}
			if err != nil {
				break
			} else {
				readSize := int64(len(data))
				offset += readSize
				remain -= readSize
			}
		}
	}()
	return readc
}
func (ss *Snapshot) AddSnapshotSegment(segment *pb.Snapshot) (bool, error) {
	var err error
	var sf *SnapshotFile
	tmpPath := path.Join(ss.dir, "tmp")

	if ss.installingSnap == nil {
		ss.installingSnap = make(map[string]*SnapshotFile)
	}

	extra := fmt.Sprintf("%s@%d", strconv.FormatUint(segment.LastIncludeIndex, 16), segment.LastIncludeTerm)
	file := fmt.Sprintf("%d_%s_%d.sst", segment.Level, extra, segment.Segment)

	// 片段偏移为0,新建文件
	if segment.Offset == 0 {
		if _, err := os.Stat(tmpPath); err != nil {
			os.Mkdir(tmpPath, os.ModePerm)
		}
		filePath := path.Join(tmpPath, file)
		// 文件已存在，关闭旧文件写入并删除文件
		old, exsit := ss.installingSnap[file]
		if exsit {
			old.fd.Close()
		}
		os.Remove(filePath)
		// 创建临时文件，保存句柄
		fd, err := os.OpenFile(filePath, os.O_WRONLY|os.O_CREATE, 0644)
		if err != nil {
			ss.logger.Errorf("创建临时快照文件%s失败:%v", file, err)
			return false, err
		}
		sf = &SnapshotFile{fd: fd, level: int(segment.Level), segment: int(segment.Segment), offset: 0}
		ss.installingSnap[file] = sf
	} else { // 偏移不为0,查找已存在文件
		sf = ss.installingSnap[file]
		if sf == nil {
			ss.logger.Errorf("未找到临时快照文件%s", file)
			return false, err
		}
		if sf.offset != segment.Offset {
			ss.logger.Errorf("临时快照文件%s 偏移与接收段偏移不一致", file)
			return false, err
		}
	}
	// 写入片段到文件
	n, err := sf.fd.Write(segment.Data)
	if err != nil {
		ss.logger.Errorf("写入临时快照文件%s失败:%v", file, err)
		return false, err
	}

	// 片段写入完成
	if segment.Done {
		sf.fd.Close()
		sf.done = true
		if segment.Level == 0 { // 文件为第0层，单个文件为快照,合并到lsm
			ss.data.Merge(0, extra, path.Join(tmpPath, file))
			delete(ss.installingSnap, file)
			ss.lastIncludeIndex = segment.LastIncludeIndex
			ss.lastIncludeTerm = segment.LastIncludeTerm
			return true, nil
		} else { // 快照不为0层，存在多个文件，片段序号0表示最后一个文件
			var complete bool
			done := true
			// 检查同层是否所有文件传输完成
			for _, v := range ss.installingSnap {
				if v.level == int(segment.Level) {
					done = done && v.done
					if v.segment == 0 {
						complete = true
					}
				}
			}
			// 全部文件传输完成，合并所有文件到层
			if complete && done {
				for k, v := range ss.installingSnap {
					ss.data.Merge(v.level, extra, path.Join(tmpPath, k))
					delete(ss.installingSnap, k)
				}
				ss.lastIncludeIndex = segment.LastIncludeIndex
				ss.lastIncludeTerm = segment.LastIncludeTerm
				return true, nil
			}
		}
	} else {
		sf.offset += uint64(n)
	}
	return false, err
}

func (t *Tree) Merge(level int, extra string, filePath string) error {
	// 强制合并指定层之前数据
	if level > 0 && level < t.conf.MaxLevel {
		for i := 0; i < level; i++ {
			for len(t.tree[i]) > 0 {
				err := t.compaction(i)
				if err != nil {
					return err
				}
			}
		}
	}
	// 移动&重命名文件
	newFile := formatName(level, t.NextSeqNo(level), extra)
	os.Rename(filePath, path.Join(t.conf.Dir, newFile))

	// 加载文件数据
	t.LoadNode(newFile)
	return nil
}
