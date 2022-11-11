package server

import (
	"context"
	"github.com/coreos/etcd/pkg/types"
	"github.com/coreos/etcd/raft"
	"github.com/coreos/etcd/raft/raftpb"
	"github.com/coreos/etcd/rafthttp"
	"log"
	"os"
	"time"
)

type raftNode struct {
	proposeC <-chan []byte
	commitC  chan []byte

	shutdownC <-chan struct{}
	node      raft.Node

	ticker    *time.Ticker
	transport *rafthttp.Transport
	storage   *raft.MemoryStorage
	logger    raft.Logger
}

func NewRaftNode(proposeC <-chan []byte, shutdownC <-chan struct{}) (*raftNode, <-chan []byte) {
	rn := &raftNode{
		proposeC:  proposeC,
		shutdownC: shutdownC,
		ticker:    time.NewTicker(time.Millisecond * 50),
		commitC:   make(chan []byte),
	}
	rn.storage = raft.NewMemoryStorage()
	defaultLogger := &raft.DefaultLogger{Logger: log.New(os.Stderr, "raft", log.LstdFlags)}
	defaultLogger.EnableDebug()
	logger := raft.Logger(defaultLogger)
	raftConfig := raft.Config{
		ID:              0x01,
		ElectionTick:    10,
		HeartbeatTick:   1,
		Storage:         rn.storage,
		MaxSizePerMsg:   4096,
		MaxInflightMsgs: 256,
		Logger:          logger,
	}
	peers := []raft.Peer{{ID: 0x01}}

	rn.logger = logger
	rn.transport = &rafthttp.Transport{
		ID:        types.ID(1),
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
			rn.transport.Send(rd.Messages)
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
