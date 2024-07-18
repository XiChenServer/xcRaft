package raft

import (
	"fmt"
	"io/fs"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
)

type Storage interface {
	Append(entries []*pb.LogEntry)                         // 进行追加
	GetEntries(startIndex, endIndex uint64) []*pb.LogEntry // 获取日志
	GetTerm(index uint64) uint64                           //获取任期
	GetLastLogIndexAndTerm() (uint64, uint64)              // 获取最后一条日志的索引和任期
	Close()                                                // 进行关闭
}

// NewRaftLog 初始化raft的日志实例
func NewRaftLog(storage Storage, logger *zap.SugaredLogger) *RaftLog {
	// 先进行获取
	lastIndex, lastTerm := storage.GetLastLogIndexAndTerm()

	return &RaftLog{
		logEnties:        make([]*pb.LogEntry, 0),
		storage:          storage,
		commitIndex:      lastIndex,
		lastAppliedIndex: lastIndex,
		lastAppliedTerm:  lastTerm,
		lastAppendIndex:  lastIndex,
		logger:           logger,
	}
}

type RaftStorage struct {
	encoding            Encoding           // 日志编解码
	walw                *wal.WalWriter     // 预写日志
	logEntries          *skiplist.SkipList // raft 日志
	logState            *skiplist.SkipList // kv 数据
	immutableLogEntries *skiplist.SkipList // 上次/待写入快照 raft日志
	immutableLogState   *skiplist.SkipList // 上次/待写入快照 kv 数据
	snap                *Snapshot          // 快照实例
	stopc               chan struct{}      // 停止通道
	logger              *zap.SugaredLogger
}

func (rs *RaftStorage) Append(entries []*pb.LogEntry) {
	for _, entry := range entries {
		logKey, logValue := rs.encoding.EncodeLogEntry(entry)
		// 日志编号作为key，日志内容作为值
		rs.walw.Write(logKey, logValue)
		rs.logEntries.Put(logKey, logValue)

		k, v := rs.encoding.DecodeLogEntryData(entry.Data)
		if k != nil {
			rs.logState.Put(k, v)
		}
	}
	rs.MakeSnapshot(false)
}

// MakeSnapshot 制作快照
func (rs *RaftStorage) MakeSnapshot(force bool) {
	if rs.logState.Size() > LOG_SNAPSHOT_SIZE || (force && rs.logState.Size() > 0) {
		oldWalw := rs.walw
		walw, err := rs.walw.Next()
		if err != nil {
			oldWalw = nil
			rs.logger.Errorf("新建预写日志失败: %v", err)
		} else {
			rs.walw = walw
		}

		rs.immutableLogEntries = rs.logEntries
		rs.immutableLogState = rs.logState
		rs.logEntries = skiplist.NewSkipList()
		rs.logState = skiplist.NewSkipList()

		go func(w *wal.WalWriter, logState *skiplist.SkipList, logEntries *skiplist.SkipList) {
			k, v := logEntries.GetMax()
			entry := rs.encoding.DecodeLogEntry(k, v)

			rs.snap.MakeSnapshot(logState, entry.Index, entry.Term)
			if oldWalw != nil {
				oldWalw.Finish()
			}
		}(oldWalw, rs.immutableLogState, rs.immutableLogEntries)
	}
}

func (ss *Snapshot) MakeSnapshot(logState *skiplist.SkipList, lastIndex, lastTerm uint64) {
	ss.data.FlushRecord(skiplist.NewSkipListIter(logState), fmt.Sprintf("%s@%s", strconv.FormatUint(lastIndex, 16), strconv.FormatUint(lastTerm, 16)))
	ss.lastIncludeIndex = lastIndex
	ss.lastIncludeTerm = lastTerm
}
func (rs *RaftStorage) GetEntries(startIndex, endIndex uint64) []*pb.LogEntry {

	if startIndex < rs.snap.lastIncludeIndex {
		rs.logger.Infof("日志 %d 已压缩到快照: %d", startIndex, rs.snap.lastIncludeIndex)
		return nil
	}

	startByte := rs.encoding.EncodeIndex(startIndex)
	endByte := rs.encoding.EncodeIndex(endIndex)

	kvs := rs.logEntries.GetRange(startByte, endByte)
	ret := make([]*pb.LogEntry, len(kvs))
	for i, kv := range kvs {
		ret[i] = rs.encoding.DecodeLogEntry(kv.Key, kv.Value)
	}
	return ret
}
func NewRaftStorage(dir string, encoding Encoding, logger *zap.SugaredLogger) *RaftStorage {
	// 保证文件夹存在
	if _, err := os.Stat(dir); err != nil {
		os.Mkdir(dir, os.ModePerm)
	}

	snapConf := lsm.NewConfig(path.Join(dir, "snapshot"), logger)
	snapConf.SstSize = LOG_SNAPSHOT_SIZE

	// 从文件夹恢复快照状态
	snap, err := NewSnapshot(snapConf)
	if err != nil {
		logger.Errorf("读取快照失败", err)
	}

	// 从raft日志还原实际数据
	logEntries, w := restoreLogEntries(dir, encoding, snap, logger)
	logState, _, _ := encoding.DecodeLogEntries(logEntries)

	s := &RaftStorage{
		walw:       w,
		logEntries: logEntries,
		logState:   logState,
		snap:       snap,
		notifyc:    make(chan []*pb.MemberChange),
		stopc:      make(chan struct{}),
		encoding:   encoding,
		logger:     logger,
	}

	lastIndex, lastTerm := s.GetLastLogIndexAndTerm()
	logger.Infof("存储最后日志 %d 任期 %d ,快照最后日志 %d 任期 %d ", lastIndex, lastTerm, snap.lastIncludeIndex, snap.lastIncludeTerm)
	// 定时刷新预写日志
	s.checkFlush()

	return s
}

func restoreLogEntries(dir string, encoding Encoding, snap *Snapshot, logger *zap.SugaredLogger) (*skiplist.SkipList, *wal.WalWriter) {
	walDir := path.Join(dir, "wal")
	if _, err := os.Stat(walDir); err != nil {
		os.Mkdir(walDir, os.ModePerm)
	}

	memLogs := make(map[int]*skiplist.SkipList, 1)
	wals := *new(sort.IntSlice)

	// 文件处理回调
	callbacks := []func(string, fs.FileInfo){
		func(name string, fileInfo fs.FileInfo) {
			info := strings.Split(name, ".")
			if len(info) != 2 {
				return
			}
			seqNo, err := strconv.Atoi(info[0])
			if err != nil {
				return
			}

			// 文件为wal类型时，尝试还原日志
			if info[1] == "wal" {
				file := path.Join(walDir, strconv.Itoa(seqNo)+".wal")
				db, err := wal.Restore(file)
				if err != nil {
					logger.Errorf("还原 %s 失败:%v", file, err)
				}
				if db != nil {
					wals = append(wals, seqNo)
					memLogs[seqNo] = db
				}
			}
		},
	}

	// 扫描文件夹，执行回调
	if err := utils.CheckDir(walDir, callbacks); err != nil {
		logger.Errorf("打开db文件夹 %s 失败: %v", walDir, err)
	}

	var logEntries *skiplist.SkipList

	var seq int
	// 重新排序预写日志序号
	wals.Sort()
	// 取最新序号预写日志继续使用
	if wals.Len() > 0 {
		seq = wals[wals.Len()-1]
		logEntries = memLogs[seq]
		delete(memLogs, seq)
	}
	if logEntries == nil {
		logEntries = skiplist.NewSkipList()
	}

	// 打开预写日志wal
	w, err := wal.NewWalWriter(walDir, seq, logger)
	if err != nil {
		logger.Errorf("创建wal writer失败: %v", walDir, err)
	}

	// 将旧预写日志更新到快照
	for seq, logEntry := range memLogs {
		snap.MakeSnapshot(encoding.DecodeLogEntries(logEntry))
		os.Remove(path.Join(walDir, strconv.Itoa(seq)+".wal"))
	}
	return logEntries, w
}
