package server

import (
	"github.com/stretchr/testify/require"
	log_v1 "github.com/yongsheng1992/sks/api/v1"
	"github.com/yongsheng1992/sks/log"
	"google.golang.org/grpc"
	"io/ioutil"
	"net"
	"testing"
)

func TestServer(t *testing.T) {
	for scenario, fn := range map[string]func(t *testing.T, client log_v1.LogClient, cfg *Config){
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

func setupTest(t *testing.T, fn func(config *Config)) (client log_v1.LogClient, cfg *Config, teardown func()) {
	t.Helper()

	l, err := net.Listen("tcp", ":0")
	require.NoError(t, err)

	clientOpts := []grpc.DialOption{grpc.WithInsecure()}
	cc, err := grpc.Dial(l.Addr().String(), clientOpts...)
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

	server, err := NewGRPCServer(cfg)
	require.NoError(t, err)
	go func() {
		_ = server.Serve(l)
	}()

	client = log_v1.NewLogClient(cc)

	return client, cfg, func() {
		server.Stop()
		cc.Close()
		l.Close()
		cl.Remove()
	}
}

func testProduceAndConsume(t *testing.T, client log_v1.LogClient, cfg *Config) {

}

func testProduceAndConsumeStream(t *testing.T, client log_v1.LogClient, cfg *Config) {

}

func testPastLogBoundaryFail(t *testing.T, client log_v1.LogClient, cfg *Config) {

}
