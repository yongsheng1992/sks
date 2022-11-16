package server

import (
	"fmt"
	"github.com/coreos/etcd/raft"
	"github.com/coreos/etcd/raft/raftpb"
	"github.com/stretchr/testify/require"
	"log"
	"os"
	"testing"
	"time"
)

func TestSingleNode(t *testing.T) {
	defer func() {
		fmt.Println("remove all")
		_ = os.RemoveAll("./wal")
		_ = os.RemoveAll("./snap")
	}()
	proposeC := make(chan []byte)
	shutdownC := make(chan struct{})

	defaultLogger := &raft.DefaultLogger{Logger: log.New(os.Stderr, "raft", log.LstdFlags)}
	defaultLogger.EnableDebug()
	logger := raft.Logger(defaultLogger)

	config := RaftConfig{
		Id:            1,
		ElectionTick:  10,
		HeartbeatTick: 1,
		Logger:        logger,
		WalDir:        "./wal",
		SnapDir:       "./snap",
	}

	rn, commitC := NewRaftNode(&config, []raft.Peer{{ID: config.Id}}, proposeC, shutdownC)

	go rn.run()

	timer := time.Tick(time.Second * 5)
	for {
		if rn.node.Status().RaftState == raft.StateLeader {
			break
		}
		select {
		case <-timer:
			t.Errorf("elect timeout...")
			return
		default:
			time.Sleep(time.Second * 1)
		}
	}
	msgs := []string{
		"hello",
		"world",
	}

	for _, msg := range msgs {
		proposeC <- []byte(msg)
	}
	ticker := time.Tick(time.Second * 3)
	commits := make([]string, 0)
	go func() {
		i := 0
		for {
			select {
			case data, ok := <-commitC:
				if !ok {
					return
				}
				for _, item := range data {
					require.Equal(t, string(item), msgs[i])
					commits = append(commits, string(item))
					i++
				}
			}
		}
	}()
	<-ticker
	shutdownC <- struct{}{}
	<-rn.Stop()

	rn, commitC = NewRaftNode(&config, []raft.Peer{{ID: config.Id}}, proposeC, shutdownC)
	first, err := rn.raftStorage.FirstIndex()
	require.NoError(t, err)
	last, err := rn.raftStorage.LastIndex()
	require.NoError(t, err)
	entries, err := rn.raftStorage.Entries(first, last+1, 4096)
	require.NoError(t, err)
	commits1 := make([]string, 0)
	for _, entry := range entries {
		fmt.Println(entry.Type)
		if entry.Type == raftpb.EntryNormal && len(entry.Data) > 0 {
			commits1 = append(commits1, string(entry.Data))
		}
	}
	require.Equal(t, commits, commits1)
}
