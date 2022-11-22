package server

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"github.com/coreos/etcd/raft"
	"github.com/stretchr/testify/require"
	"log"
	"math/rand"
	"os"
	"testing"
	"time"
)

func TestSingleNode(t *testing.T) {
	defer func() {
		fmt.Println("remove all")
		if err := os.RemoveAll("./wal"); err != nil {
			fmt.Println(err)
		}
		if err := os.RemoveAll("./snap"); err != nil {
			fmt.Println(err)
		}
	}()
	proposeC := make(chan []byte)
	shutdownC := make(chan struct{})
	commits := make([]string, 0)

	defaultLogger := &raft.DefaultLogger{Logger: log.New(os.Stderr, "raft", log.LstdFlags)}
	defaultLogger.EnableDebug()
	logger := raft.Logger(defaultLogger)

	config := RaftConfig{
		Id:             1,
		ElectionTick:   10,
		HeartbeatTick:  1,
		Logger:         logger,
		WalDir:         "./wal",
		SnapDir:        "./snap",
		SnapCount:      10,
		CatchupEntries: 10,
		GetSnapshot: func() ([]byte, error) {
			var buf bytes.Buffer
			err := gob.NewEncoder(&buf).Encode(commits)
			return buf.Bytes(), err
		},
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

	msgs := make([]string, 0)

	for i := 0; i < 1001; i++ {
		msgs = append(msgs, randStr(i%26+1))
	}
	//msgs = append(msgs, "hello", "world")

	waitC := make(chan struct{})
	go func() {
		for _, msg := range msgs {
			time.Sleep(time.Microsecond * time.Duration(rand.Intn(100)*1000))
			proposeC <- []byte(msg)
		}
		waitC <- struct{}{}
	}()

	go func() {
		i := 0
		for {
			select {
			case commit, ok := <-commitC:
				rn.logger.Debug(fmt.Sprintf("commit %v", commit))
				if !ok {
					return
				}
				for _, item := range commit.data {
					require.Equal(t, msgs[i], string(item))
					commits = append(commits, string(item))
					i++
				}
				if commit.applyDoneC != nil {
					close(commit.applyDoneC)
				}
			}
		}
	}()
	<-waitC
	ticker := time.Tick(time.Second * 3)
	<-ticker
	shutdownC <- struct{}{}
	<-rn.Stop()
	appliedIndex := rn.snapshotIndex
	rn, commitC = NewRaftNode(&config, []raft.Peer{{ID: config.Id}}, proposeC, shutdownC)
	first, err := rn.raftStorage.FirstIndex()
	require.NoError(t, err)
	last, err := rn.raftStorage.LastIndex()
	require.NoError(t, err)
	rn.logger.Debugf("[%d: %d]", first, last)
	require.NoError(t, err)

	require.Equal(t, appliedIndex, rn.appliedIndex)
	err = rn.wal.Close()
	require.NoError(t, err)
}
