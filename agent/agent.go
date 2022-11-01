package agent

import (
	"crypto/tls"
	"fmt"
	"github.com/hashicorp/raft"
	"github.com/yongsheng1992/sks/discovery"
	"github.com/yongsheng1992/sks/distributed_log"
	"github.com/yongsheng1992/sks/server"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"net"
	"sync"
	"time"
)

type Config struct {
	ServerTLSConfig *tls.Config
	PeerTLSConfig   *tls.Config
	DataDir         string
	Bootstrap       bool
	// BindAddr used for serf
	BindAddr       string
	RPCPort        int
	RaftPort       int
	NodeName       string
	StartJoinAddrs []string
}

type Agent struct {
	Config

	log        *distributed_log.DistributedLog
	server     *grpc.Server
	membership *discovery.Membership

	shutdown     bool
	shutdowns    chan struct{}
	shutdownLock sync.Mutex
}

func (c Config) Host() (string, error) {
	host, _, err := net.SplitHostPort(c.BindAddr)
	return host, err
}

func (c Config) RPCAddr() (string, error) {
	host, err := c.Host()
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%s:%d", host, c.RPCPort), nil
}

func (c Config) RaftAddr() (string, error) {
	host, err := c.Host()
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%s:%d", host, c.RaftPort), nil
}

func New(config Config) (*Agent, error) {
	agent := &Agent{
		Config:    config,
		shutdowns: make(chan struct{}),
	}

	setups := []func() error{
		agent.setupLog,
		agent.setupServer,
		agent.setupMembership,
	}

	for _, fn := range setups {
		if err := fn(); err != nil {
			return nil, err
		}
	}
	return agent, nil
}

func (agent *Agent) setupLog() error {
	var err error
	raftAddr, err := agent.RaftAddr()
	if err != nil {
		return err
	}

	ln, err := net.Listen("tcp", raftAddr)
	if err != nil {
		return nil
	}

	logConfig := distributed_log.Config{}
	logConfig.Raft.SteamLayer = distributed_log.NewStreamLayer(ln, agent.ServerTLSConfig, agent.PeerTLSConfig)
	logConfig.Raft.LocalID = raft.ServerID(agent.NodeName)
	logConfig.Raft.Bootstrap = agent.Config.Bootstrap
	logConfig.Raft.HeartbeatTimeout = 50 * time.Millisecond
	logConfig.Raft.ElectionTimeout = 50 * time.Millisecond
	logConfig.Raft.LeaderLeaseTimeout = 50 * time.Millisecond
	logConfig.Raft.CommitTimeout = 5 * time.Millisecond
	logConfig.Raft.ProtocolVersion = raft.ProtocolVersionMax
	logConfig.Raft.MaxAppendEntries = 20
	logConfig.Raft.SnapshotInterval = 10 * time.Second

	agent.log, err = distributed_log.NewDistributedLog(agent.DataDir, logConfig)
	if err != nil {
		return err
	}

	if agent.Config.Bootstrap {
		return agent.log.WaitForLeader(3 * time.Second)
	}

	return nil
}

func (agent *Agent) setupServer() error {
	var err error
	serverConfig := &server.Config{
		CommitLog: agent.log,
	}
	var opts []grpc.ServerOption

	if agent.ServerTLSConfig != nil {
		creds := credentials.NewTLS(agent.ServerTLSConfig)
		opts = append(opts, grpc.Creds(creds))
	}
	agent.server, err = server.NewGRPCServer(serverConfig, opts...)
	if err != nil {
		return err
	}

	rpcAddr, err := agent.RPCAddr()
	ln, err := net.Listen("tcp", rpcAddr)
	if err != nil {
		return err
	}
	go func() {
		if err := agent.server.Serve(ln); err != nil {
			_ = agent.Shutdown()
		}
	}()
	return nil
}

func (agent *Agent) setupMembership() error {
	raftAddr, err := agent.RaftAddr()
	if err != nil {
		return err
	}

	agent.membership, err = discovery.New(agent.log, discovery.Config{
		NodeName: agent.NodeName,
		BindAddr: agent.BindAddr,
		Tags: map[string]string{
			"rpc_addr": raftAddr,
		},
		StartJoinAddr: agent.StartJoinAddrs,
	})
	return err
}

func (agent *Agent) Shutdown() error {
	agent.shutdownLock.Lock()
	defer agent.shutdownLock.Unlock()

	agent.shutdown = true
	close(agent.shutdowns)

	shutdown := []func() error{
		agent.membership.Leave,
		func() error {
			agent.server.GracefulStop()
			return nil
		},
		agent.log.Close,
	}

	for _, fn := range shutdown {
		if err := fn(); err != nil {
			return err
		}
	}
	return nil
}
