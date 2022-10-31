package distributed_log

import (
	"bytes"
	"fmt"
	"github.com/golang/protobuf/proto"
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
	log_v1 "github.com/yongsheng1992/sks/api/v1"
	"github.com/yongsheng1992/sks/log"
	"os"
	"path/filepath"
	"time"
)

type Config struct {
	Raft struct {
		raft.Config
		SteamLayer *StreamLayer
		Bootstrap  bool
	}
	log.Config
}

type DistributedLog struct {
	Config
	log  *log.Log
	raft *raft.Raft
}

func NewDistributedLog(dataDir string, config Config) (*DistributedLog, error) {
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
		return err
	}
	l, err := log.NewLog(logDir, dl.Config.Config)
	if err != nil {
		return err
	}
	dl.log = l
	return nil
}

func (dl *DistributedLog) setupRaft(dataDir string) error {
	raftConfig := dl.Config.Raft.Config
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

	dl.raft, err = raft.NewRaft(&raftConfig, fsm, logStore, stableStore, snapshotStore, transport)
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
	lsConfig := dl.Config.Config
	lsConfig.Segment.InitialOffset = 1
	lsDir := filepath.Join(dataDir, "raft", "log")
	if err := os.MkdirAll(lsDir, 0755); err != nil {
		return nil, err
	}
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

func (dl *DistributedLog) Join(id, addr string) error {
	configFuture := dl.raft.GetConfiguration()
	if err := configFuture.Error(); err != nil {
		return err
	}
	serverId := raft.ServerID(id)
	serverAddr := raft.ServerAddress(addr)

	for _, srv := range configFuture.Configuration().Servers {
		if srv.ID == serverId || srv.Address == serverAddr {
			return nil
		}
		removeFuture := dl.raft.RemoveServer(serverId, 0, 0)
		if err := removeFuture.Error(); err != nil {
			return err
		}
	}

	addFuture := dl.raft.AddVoter(serverId, serverAddr, 0, 0)
	return addFuture.Error()
}

func (dl *DistributedLog) Close() error {
	f := dl.raft.Shutdown()
	if err := f.Error(); err != nil {
		return err
	}
	return dl.log.Close()
}

func (dl *DistributedLog) Leave(id string) error {
	future := dl.raft.RemoveServer(raft.ServerID(id), 0, 0)
	return future.Error()
}

func (dl *DistributedLog) Append(record *log_v1.Record) (uint64, error) {
	res, err := dl.apply(AppendRequestType, &log_v1.ProduceRequest{Record: record})

	if err != nil {
		return 0, err
	}

	return res.(*log_v1.ProduceResponse).Offset, nil
}

func (dl *DistributedLog) apply(reqType RequestType, req proto.Message) (interface{}, error) {
	var buf bytes.Buffer
	_, err := buf.Write([]byte{byte(reqType)})
	if err != nil {
		return nil, err
	}

	b, err := proto.Marshal(req)
	if err != nil {
		return nil, err
	}

	_, err = buf.Write(b)
	if err != nil {
		return nil, err
	}

	timeout := 10 * time.Second
	future := dl.raft.Apply(buf.Bytes(), timeout)
	if err := future.Error(); err != nil {
		return nil, err
	}

	res := future.Response()
	if err, ok := res.(error); ok {
		return nil, err
	}
	return res, nil
}

func (dl *DistributedLog) Read(offset uint64) (*log_v1.Record, error) {
	return dl.log.Read(offset)
}
