package server

import (
	"github.com/coreos/etcd/raft"
	"go.uber.org/zap"
	"log"
	"os"
	"time"
)

func Serve(proposeC chan []byte, shutDownC chan struct{}, errorC chan error) {

	defaultLogger := &raft.DefaultLogger{Logger: log.New(os.Stderr, "raft", log.LstdFlags)}

	config := &RaftConfig{
		Id:             1,
		ElectionTick:   10,
		HeartbeatTick:  1,
		WalDir:         "./tmp/wal",
		SnapDir:        "./tmp/snap",
		SnapCount:      10,
		CatchupEntries: 10,
		Logger:         defaultLogger,
	}
	rn, commitC := NewRaftNode(config, []raft.Peer{{ID: config.Id}}, proposeC, shutDownC)
	go rn.run()

	logger, _ := zap.NewProduction()
	kvConfig := &KVConfig{
		ticks:  time.Millisecond * 10,
		logger: logger,
	}
	kv := newKVStore(kvConfig, proposeC, commitC)
	serverHTTP(kv, errorC)
}
