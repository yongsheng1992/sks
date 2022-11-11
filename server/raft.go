package server

import (
	"context"
	"github.com/coreos/etcd/pkg/types"
	"github.com/coreos/etcd/raft"
	"github.com/coreos/etcd/raft/raftpb"
	"github.com/coreos/etcd/rafthttp"
	"time"
)

type raftNode struct {
	proposeC <-chan []byte
	commitC  chan []byte

	shutdownC <-chan struct{}
	node      raft.Node

	ticker      *time.Ticker
	transporter rafthttp.Transporter
	storage     *raft.MemoryStorage
	logger      raft.Logger
}

type RaftConfig struct {
	Id            uint64
	ElectionTick  int
	HeartbeatTick int
	Logger        raft.Logger
}

func NewRaftNode(config *RaftConfig, peers []raft.Peer, proposeC <-chan []byte, shutdownC <-chan struct{}) (*raftNode, <-chan []byte) {
	rn := &raftNode{
		proposeC:  proposeC,
		shutdownC: shutdownC,
		ticker:    time.NewTicker(time.Millisecond * 50),
		commitC:   make(chan []byte),
		logger:    config.Logger,
		storage:   raft.NewMemoryStorage(),
	}

	raftConfig := raft.Config{
		ID:              config.Id,
		ElectionTick:    config.ElectionTick,
		HeartbeatTick:   config.HeartbeatTick,
		Storage:         rn.storage,
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
	rn.node = raft.StartNode(&raftConfig, peers)
	return rn, rn.commitC
}

func (rn *raftNode) run() {

	go func() {
		for {
			select {
			case prop := <-rn.proposeC:
				rn.logger.Debugf("propose %v", prop)
				if err := rn.node.Propose(context.TODO(), prop); err != nil {
					panic(err)
				}
			case <-rn.shutdownC:
				return
			}
		}
	}()

	for {
		select {
		case <-rn.ticker.C:
			rn.node.Tick()
		case rd := <-rn.node.Ready():
			if err := rn.storage.Append(rd.Entries); err != nil {
				rn.logger.Error(err)
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
