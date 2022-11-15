package server

import (
	"context"
	"github.com/coreos/etcd/pkg/fileutil"
	"github.com/coreos/etcd/pkg/types"
	"github.com/coreos/etcd/raft"
	"github.com/coreos/etcd/raft/raftpb"
	"github.com/coreos/etcd/rafthttp"
	"github.com/coreos/etcd/snap"
	"github.com/coreos/etcd/wal"
	"github.com/coreos/etcd/wal/walpb"
	"os"
	"time"
)

type raftNode struct {
	config   *RaftConfig
	proposeC <-chan []byte
	commitC  chan []byte
	stopC    chan struct{}

	shutdownC <-chan struct{}
	node      raft.Node

	ticker      *time.Ticker
	transporter rafthttp.Transporter
	raftStorage *raft.MemoryStorage

	wal         *wal.WAL
	snapshotter *snap.Snapshotter

	logger raft.Logger
}

type RaftConfig struct {
	Id            uint64
	ElectionTick  int
	HeartbeatTick int
	Logger        raft.Logger
	WalDir        string
	SnapDir       string
	Join          bool
}

func NewRaftNode(config *RaftConfig, peers []raft.Peer, proposeC <-chan []byte, shutdownC <-chan struct{}) (*raftNode, <-chan []byte) {
	if !fileutil.Exist(config.SnapDir) {
		if err := os.Mkdir(config.SnapDir, 0750); err != nil {
			config.Logger.Fatalf("create snap dir failed %v", err)
		}
	}
	hasOldWal := wal.Exist(config.WalDir)
	rn := &raftNode{
		config:      config,
		proposeC:    proposeC,
		shutdownC:   shutdownC,
		ticker:      time.NewTicker(time.Millisecond * 50),
		commitC:     make(chan []byte),
		logger:      config.Logger,
		raftStorage: raft.NewMemoryStorage(),
		stopC:       make(chan struct{}),
	}
	rn.snapshotter = snap.New(rn.config.SnapDir)
	rn.wal = rn.replayWal()
	raftConfig := raft.Config{
		ID:              config.Id,
		ElectionTick:    config.ElectionTick,
		HeartbeatTick:   config.HeartbeatTick,
		Storage:         rn.raftStorage,
		MaxSizePerMsg:   4096,
		MaxInflightMsgs: 256,
		Logger:          config.Logger,
	}

	rn.transporter = &rafthttp.Transport{
		ID:        types.ID(config.Id),
		ClusterID: 0x1000,
		Raft:      rn,
		ErrorC:    make(chan error),
	}
	if hasOldWal || rn.config.Join {
		rn.logger.Infof("use old wal...")
		rn.node = raft.RestartNode(&raftConfig)
	} else {
		rn.node = raft.StartNode(&raftConfig, peers)
	}
	return rn, rn.commitC
}

func (rn *raftNode) run() {
	defer func() {
		if err := rn.wal.Close(); err != nil {
			rn.logger.Errorf("close wal failed %v", err)
		}
		close(rn.stopC)
	}()

	go func() {
		for {
			select {
			case prop, ok := <-rn.proposeC:
				if !ok {
					return
				}
				rn.logger.Debugf("propose %v", prop)
				if err := rn.node.Propose(context.TODO(), prop); err != nil {
					panic(err)
				}
			}
		}
	}()

	for {
		select {
		case <-rn.ticker.C:
			rn.node.Tick()
		case rd := <-rn.node.Ready():
			rn.logger.Debugf("ready %v", rd)
			if err := rn.raftStorage.Append(rd.Entries); err != nil {
				rn.logger.Error(err)
			}
			if err := rn.wal.Save(rd.HardState, rd.Entries); err != nil {
				rn.logger.Error(err)
			}

			if !raft.IsEmptySnap(rd.Snapshot) {
				if err := rn.snapshotter.SaveSnap(rd.Snapshot); err != nil {
					rn.logger.Error(err)
				}
			}
			for _, entry := range rd.CommittedEntries {
				if entry.Type == raftpb.EntryConfChange {
					var cc raftpb.ConfChange
					if err := cc.Unmarshal(entry.Data); err != nil {
						rn.logger.Error(err)
					}
					rn.node.ApplyConfChange(cc)
				} else {
					rn.logger.Debug(entry.Type, entry.Data)
					if len(entry.Data) > 0 {
						rn.commitC <- entry.Data
					}
				}
			}
			rn.transporter.Send(rd.Messages)
			rn.node.Advance()
		case <-rn.shutdownC:
			rn.logger.Infof("receive shutdown...")
			close(rn.commitC)
			rn.node.Stop()
			return
		}
	}
}

func (rn *raftNode) Process(ctx context.Context, m raftpb.Message) error {
	if err := rn.node.Step(ctx, m); err != nil {
		rn.logger.Error(err)
		return err
	}
	return nil
}

func (rn *raftNode) IsIDRemoved(id uint64) bool {
	panic("not implemented")
}

func (rn *raftNode) ReportUnreachable(id uint64) {
	panic("not implemented")
}

func (rn *raftNode) ReportSnapshot(id uint64, status raft.SnapshotStatus) {
	panic("not implemented")
}

// loadSnapshot load newest wal snapshot form wal file.
func (rn *raftNode) loadSnapshot() *raftpb.Snapshot {
	if wal.Exist(rn.config.WalDir) {
		rn.logger.Infof("valid snapshot from wal...")
		walSnaps, err := wal.ValidSnapshotEntries(rn.config.WalDir)
		if err != nil {
			rn.logger.Error(err)
		}
		s, err := rn.snapshotter.LoadNewestAvailable(walSnaps)
		if err != nil {
			rn.logger.Error(err)
		}
		return s
	}
	return &raftpb.Snapshot{}
}

func (rn *raftNode) replayWal() *wal.WAL {
	snapshot := rn.loadSnapshot()
	w := rn.openWal(snapshot)
	_, state, entries, err := w.ReadAll()
	if err != nil {
		rn.logger.Fatalf("failed to read wal %v", err)
	}
	if err := rn.raftStorage.Append(entries); err != nil {
		rn.logger.Fatalf("failed to append entries %v", err)
	}
	rn.logger.Infof("append %d entries.", len(entries))
	if err := rn.raftStorage.SetHardState(state); err != nil {
		rn.logger.Fatalf("failed to set hard state %v", err)
	}
	rn.logger.Infof("set hard state %v", state)
	return w
}

// openWal open a wal form snapshot.
func (rn *raftNode) openWal(snapshot *raftpb.Snapshot) *wal.WAL {
	if !wal.Exist(rn.config.WalDir) {
		rn.logger.Infof("create dir for wal...")
		if err := os.Mkdir(rn.config.WalDir, 0750); err != nil {
			rn.logger.Fatalf("cannot create dir for wal %v", err)
		}
		rn.logger.Infof("create wal...")
		w, err := wal.Create(rn.config.WalDir, nil)
		if err != nil {
			rn.logger.Fatalf("cannot create wal %v", err)
		}
		if err := w.Close(); err != nil {
			rn.logger.Fatalf("close wal error %v", err)
		}
	}

	walSnap := walpb.Snapshot{}

	if snapshot != nil {
		walSnap.Index = snapshot.Metadata.Index
		walSnap.Term = snapshot.Metadata.Term
	}
	rn.logger.Infof("loading wal at term %d and index %d", walSnap.Term, walSnap.Index)
	w, err := wal.Open(rn.config.WalDir, walSnap)
	if err != nil {
		rn.logger.Fatalf("cannot open wal %v", err)
	}
	return w
}

func (rn *raftNode) Stop() <-chan struct{} {
	return rn.stopC
}
