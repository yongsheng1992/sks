package distributed_log

import (
	"fmt"
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
	"github.com/yongsheng1992/sks/log"
	"os"
	"path/filepath"
	"time"
)

type DistributedLog struct {
	log.Config
	log  *log.Log
	raft *raft.Raft
}

func NewDistributedLog(dataDir string, config log.Config) (*DistributedLog, error) {
	dl := &DistributedLog{
		Config: config,
	}

	if err := dl.setupLog(dataDir); err != nil {
		return nil, err
	}

	if err := dl.setupRaft(dataDir); err != nil {
		return nil, err
	}

	return dl, nil
}

func (dl *DistributedLog) setupLog(dataDir string) error {
	logDir := filepath.Join(dataDir, "log")
	if err := os.MkdirAll(logDir, 0755); err != nil {
		return nil
	}
	l, err := log.NewLog(logDir, dl.Config)
	if err != nil {
		return err
	}
	dl.log = l
	return nil
}

func (dl *DistributedLog) setupRaft(dataDir string) error {
	raftConfig := &raft.Config{}
	fsm := newFsm(dl.log)

	logStore, err := dl.setupLogStore(dataDir)
	if err != nil {
		return err
	}

	stableStore, err := raftboltdb.NewBoltStore(filepath.Join(dataDir, "raft", "stable"))
	if err != nil {
		return err
	}

	// todo move retain to config
	snapshotStore, err := raft.NewFileSnapshotStore(filepath.Join(dataDir, "raft"), 1, os.Stderr)

	transport := raft.NewNetworkTransport(dl.Config.Raft.SteamLayer, 5, 10*time.Second, os.Stderr)
	if err != nil {
		return err
	}

	dl.raft, err = raft.NewRaft(raftConfig, fsm, logStore, stableStore, snapshotStore, transport)
	if err != nil {
		return err
	}

	hasState, err := raft.HasExistingState(logStore, stableStore, snapshotStore)
	if err != nil {
		return err
	}

	if dl.Config.Raft.Bootstrap && !hasState {
		config := raft.Configuration{
			Servers: []raft.Server{{
				ID:      raftConfig.LocalID,
				Address: transport.LocalAddr(),
			}},
		}
		err = dl.raft.BootstrapCluster(config).Error()
	}
	return nil
}

func (dl *DistributedLog) setupLogStore(dataDir string) (*logStore, error) {
	lsConfig := dl.Config
	lsDir := filepath.Join(dataDir, "raft", "log")
	return newLogStore(lsDir, lsConfig)
}

func (dl *DistributedLog) WaitForLeader(timeout time.Duration) error {
	tickCh := time.After(timeout)
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-tickCh:
			return fmt.Errorf("timed out")
		case <-ticker.C:
			l, _ := dl.raft.LeaderWithID()
			if l != "" {
				return nil
			}
		}
	}
}

func (dl *DistributedLog) Close() error {
	f := dl.raft.Shutdown()
	if err := f.Error(); err != nil {
		return err
	}
	return dl.log.Close()
}