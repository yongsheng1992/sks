package agent

import (
	"context"
	"crypto/tls"
	"fmt"
	"github.com/stretchr/testify/require"
	logv1 "github.com/yongsheng1992/sks/api/v1"
	"github.com/yongsheng1992/sks/config"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"io/ioutil"
	"os"
	"testing"
	"time"
)

const (
	Addr = "127.0.0.1"
	Port = 9001
)

func TestAgent(t *testing.T) {
	serverTLSConfig, err := config.SetupTLSConfig(config.TLSConfig{
		CertFile:   config.ServerCertFile,
		KeyFile:    config.ServerKeyFile,
		CAFile:     config.CAFile,
		Server:     true,
		ServerAddr: Addr,
	})
	require.NoError(t, err)

	peerTLSConfig, err := config.SetupTLSConfig(config.TLSConfig{
		CertFile:   config.ClientCertFile,
		KeyFile:    config.ClientKeyFile,
		CAFile:     config.CAFile,
		Server:     false,
		ServerAddr: Addr,
	})
	require.NoError(t, err)
	port := Port

	var agents []*Agent
	for i := 0; i < 3; i++ {
		bindAddr := fmt.Sprintf("%s:%d", Addr, port)
		port++
		rpcPort := port
		port++

		dataDir, err := ioutil.TempDir("", "agent-test-log")
		require.NoError(t, err)

		var startJoinAddrs []string
		if i != 0 {
			startJoinAddrs = append(startJoinAddrs, agents[0].BindAddr)
		}
		//require.Nil(t, startJoinAddrs)
		agent, err := New(Config{
			NodeName:        fmt.Sprintf("%d", i),
			StartJoinAddrs:  startJoinAddrs,
			BindAddr:        bindAddr,
			RPCPort:         rpcPort,
			DataDir:         dataDir,
			ServerTLSConfig: serverTLSConfig,
			PeerTLSConfig:   peerTLSConfig,
		})

		require.NoError(t, err)
		agents = append(agents, agent)
	}

	defer func() {
		for _, agent := range agents {
			err := agent.Shutdown()
			require.NoError(t, err)
			require.NoError(t, os.RemoveAll(agent.DataDir))
		}
	}()

	time.Sleep(3 * time.Second)

	leaderClient := client(t, agents[0], peerTLSConfig)
	ctx := context.Background()
	produceResp, err := leaderClient.Produce(ctx, &logv1.ProduceRequest{
		Record: &logv1.Record{
			Value: []byte("foo"),
		},
	})
	require.NoError(t, err)

	consumeResp, err := leaderClient.Consume(ctx, &logv1.ConsumeRequest{
		Offset: produceResp.Offset,
	})
	require.NoError(t, err)

	require.Equal(t, consumeResp.Record.Value, []byte("foo"))

	time.Sleep(3 * time.Second)

	followerClient := client(t, agents[1], peerTLSConfig)
	consumeResp, err = followerClient.Consume(ctx, &logv1.ConsumeRequest{
		Offset: produceResp.Offset,
	})

	require.NoError(t, err)
	require.Equal(t, consumeResp.Record.Value, []byte("foo"))
}

func client(t *testing.T, agent *Agent, tlsConfig *tls.Config) logv1.LogClient {
	t.Helper()
	var opts []grpc.DialOption
	creds := credentials.NewTLS(tlsConfig)
	opts = append(opts, grpc.WithTransportCredentials(creds))

	rpcAddr, err := agent.RPCAddr()
	require.NoError(t, err)

	conn, err := grpc.Dial(rpcAddr, opts...)
	require.NoError(t, err)

	return logv1.NewLogClient(conn)
}
