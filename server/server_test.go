package server

import (
	"context"
	"github.com/stretchr/testify/require"
	logv1 "github.com/yongsheng1992/sks/api/v1"
	"github.com/yongsheng1992/sks/config"
	"github.com/yongsheng1992/sks/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
	"io/ioutil"
	"net"
	"testing"
)

func TestServer(t *testing.T) {
	for scenario, fn := range map[string]func(t *testing.T, client logv1.LogClient, cfg *Config){
		"produce/consume a message to/form the log succeeds": testProduceAndConsume,
		"produce/consume stream succeeds":                    testProduceAndConsumeStream,
		"consume past log boundary fails":                    testPastLogBoundaryFail,
	} {
		t.Run(scenario, func(t *testing.T) {
			client, cfg, teardown := setupTest(t, nil)
			defer teardown()
			fn(t, client, cfg)
		})
	}
}

func setupClientTLS(t *testing.T, addr string) (*grpc.ClientConn, logv1.LogClient) {
	t.Helper()
	tlsConfig, err := config.SetupTLSConfig(config.TLSConfig{
		CAFile:   config.CAFile,
		CertFile: config.ClientCertFile,
		KeyFile:  config.ClientKeyFile,
		Server:   false,
		//ServerAddr: addr,
	})

	require.NoError(t, err)
	creds := credentials.NewTLS(tlsConfig)
	opts := []grpc.DialOption{grpc.WithTransportCredentials(creds)}
	cc, err := grpc.Dial(addr, opts...)
	require.NoError(t, err)

	client := logv1.NewLogClient(cc)
	return cc, client
}

func setupServerTLSOptions(t *testing.T, addr string) grpc.ServerOption {
	t.Helper()
	tlsConfig, err := config.SetupTLSConfig(config.TLSConfig{
		CAFile:     config.CAFile,
		CertFile:   config.ServerCertFile,
		KeyFile:    config.ServerKeyFile,
		Server:     true,
		ServerAddr: addr,
	})
	require.NoError(t, err)

	creds := credentials.NewTLS(tlsConfig)
	return grpc.Creds(creds)
}

func setupTest(t *testing.T, fn func(config *Config)) (client logv1.LogClient, cfg *Config, teardown func()) {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	dir, err := ioutil.TempDir("", "server-test")
	require.NoError(t, err)

	cl, err := log.NewLog(dir, log.Config{
		Segment: struct {
			MaxStoreBytes uint64
			MaxIndexBytes uint64
			InitialOffset uint64
		}{MaxStoreBytes: 32, MaxIndexBytes: 126, InitialOffset: 0},
	})

	cfg = &Config{
		CommitLog: cl,
	}

	if fn != nil {
		fn(cfg)
	}

	tlsOption := setupServerTLSOptions(t, "127.0.0.1")
	server, err := NewGRPCServer(cfg, tlsOption)
	require.NoError(t, err)
	go func() {
		_ = server.Serve(l)
	}()

	cc, client := setupClientTLS(t, l.Addr().String())

	return client, cfg, func() {
		server.Stop()
		cc.Close()
		l.Close()
		cl.Remove()
	}
}

func testProduceAndConsume(t *testing.T, client logv1.LogClient, cfg *Config) {
	ctx := context.Background()

	req := &logv1.ProduceRequest{
		Record: &logv1.Record{
			Value: []byte("hello world"),
		},
	}
	res, err := client.Produce(ctx, req)
	require.NoError(t, err)
	require.Equal(t, uint64(0), res.Offset)

	req1 := &logv1.ConsumeRequest{
		Offset: res.Offset,
	}

	res1, err := client.Consume(ctx, req1)
	require.NoError(t, err)
	require.Equal(t, req.Record.Value, res1.Record.Value)
	require.Equal(t, uint64(0), res1.Record.Offset)
}

func testProduceAndConsumeStream(t *testing.T, client logv1.LogClient, cfg *Config) {
	ctx := context.Background()
	records := []*logv1.Record{
		{
			Value:  []byte("first message"),
			Offset: 0,
		},
		{
			Value:  []byte("second message"),
			Offset: 1,
		},
	}

	stream, err := client.ProduceStream(ctx)
	require.NoError(t, err)

	{
		for _, record := range records {
			err := stream.Send(&logv1.ProduceRequest{Record: record})
			require.NoError(t, err)

			res, err := stream.Recv()
			require.NoError(t, err)

			require.Equal(t, record.Offset, res.Offset)
		}
	}
	{
		stream, err := client.ConsumeStream(ctx, &logv1.ConsumeRequest{Offset: 0})
		require.NoError(t, err)

		for _, record := range records {
			res, err := stream.Recv()
			require.NoError(t, err)
			require.Equal(t, record.Offset, res.Record.Offset)
			require.Equal(t, record.Value, res.Record.Value)
			require.Equal(t, res.Record, &logv1.Record{
				Value:  record.Value,
				Offset: record.Offset,
			})
		}
	}
}

func testPastLogBoundaryFail(t *testing.T, client logv1.LogClient, cfg *Config) {
	ctx := context.Background()
	msg := "hello world"
	produce, err := client.Produce(ctx, &logv1.ProduceRequest{
		Record: &logv1.Record{
			Value: []byte(msg),
		},
	})

	require.NoError(t, err)

	consume, err := client.Consume(ctx, &logv1.ConsumeRequest{Offset: produce.Offset + 1})
	require.Error(t, err)
	require.Nil(t, consume)

	got := status.Code(err)
	want := status.Code(logv1.ErrOffsetOutOfRange{}.GRPCStatus().Err())
	require.Equal(t, got, want)
}
