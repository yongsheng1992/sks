package server

import (
	"github.com/coreos/etcd/raft"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestSingleNode(t *testing.T) {
	proposeC := make(chan []byte)
	shutdownC := make(chan struct{})
	rn, commitC := NewRaftNode(proposeC, shutdownC)
	go rn.run()
	for rn.node.Status().RaftState != raft.StateLeader {
		time.Sleep(time.Second * 1)
	}

	msgs := []string{
		"hello",
		"world",
	}

	for _, msg := range msgs {
		proposeC <- []byte(msg)
	}

	ticker := time.Tick(time.Second * 1)
	i := 0
	for {
		select {
		case data := <-commitC:
			require.Equal(t, string(data), msgs[i])
			i++
		case <-ticker:
			shutdownC <- struct{}{}
			return
		}
	}
}
