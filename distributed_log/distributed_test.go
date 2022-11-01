package distributed_log

import (
	"fmt"
	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"
	log_v1 "github.com/yongsheng1992/sks/api/v1"
	"io/ioutil"
	"net"
	"os"
	"reflect"
	"testing"
	"time"
)

func TestMultipleNodes(t *testing.T) {
	var logs []*DistributedLog
	var nodeCount int

	nodeCount = 3
	ports := []int{9001, 9002, 9003}

	for i := 0; i < nodeCount; i++ {
		dateDir, err := ioutil.TempDir("", "distributed-log-test")
		require.NoError(t, err)
		defer func(dir string) {
			_ = os.RemoveAll(dir)
		}(dateDir)

		ln, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", ports[i]))
		require.NoError(t, err)

		config := Config{}
		config.Raft.SteamLayer = NewStreamLayer(ln, nil, nil)
		config.Raft.LocalID = raft.ServerID(fmt.Sprintf("%d", i))
		config.Raft.HeartbeatTimeout = 50 * time.Millisecond
		config.Raft.ElectionTimeout = 50 * time.Millisecond
		config.Raft.LeaderLeaseTimeout = 50 * time.Millisecond
		config.Raft.CommitTimeout = 5 * time.Millisecond
		config.Raft.ProtocolVersion = raft.ProtocolVersionMax
		config.Raft.MaxAppendEntries = 20
		config.Raft.SnapshotInterval = 10 * time.Second

		if i == 0 {
			config.Raft.Bootstrap = true
		}

		l, err := NewDistributedLog(dateDir, config)
		require.NoError(t, err)

		if i != 0 {
			err = logs[0].Join(fmt.Sprintf("%d", i), ln.Addr().String())
			require.NoError(t, err)
		} else {
			err = l.WaitForLeader(3 * time.Second)
			require.NoError(t, err)
		}

		logs = append(logs, l)
	}

	records := []*log_v1.Record{
		{
			Value: []byte("first"),
		},
		{
			Value: []byte("second"),
		},
	}

	for _, record := range records {
		off, err := logs[0].Append(record)
		require.NoError(t, err)

		require.Eventually(t, func() bool {
			for j := 0; j < nodeCount; j++ {
				got, err := logs[j].Read(off)
				if err != nil {
					return false
				}
				record.Offset = off

				if !reflect.DeepEqual(got.Value, record.Value) {
					return false
				}
			}
			return true
		}, 500*time.Millisecond, 50*time.Millisecond)
	}

	servers, err := logs[0].GetServers()
	require.NoError(t, err)
	require.Equal(t, 3, len(servers))

	require.True(t, servers[0].IsLeader)
	require.False(t, servers[1].IsLeader)
	require.False(t, servers[2].IsLeader)

	err = logs[0].Leave("1")
	require.NoError(t, err)

	time.Sleep(50 * time.Millisecond)

	servers, err = logs[0].GetServers()
	require.Equal(t, 2, len(servers))
	require.True(t, servers[0].IsLeader)
	require.False(t, servers[1].IsLeader)

	off, err := logs[0].Append(&log_v1.Record{Value: []byte("third")})
	require.NoError(t, err)

	time.Sleep(50 * time.Millisecond)

	record, err := logs[1].Read(off)
	require.IsType(t, log_v1.ErrOffsetOutOfRange{}, err)
	require.Nil(t, record)

	record, err = logs[2].Read(off)
	require.NoError(t, err)
	require.Equal(t, record.Value, []byte("third"))
	require.Equal(t, off, record.Offset)

}
