package agent

import (
	"crypto/tls"
	"fmt"
	logv1 "github.com/yongsheng1992/sks/api/v1"
	"github.com/yongsheng1992/sks/discovery"
	"github.com/yongsheng1992/sks/log"
	"github.com/yongsheng1992/sks/server"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"net"
	"sync"
)

type Config struct {
	ServerTLSConfig *tls.Config
	PeerTLSConfig   *tls.Config
	DataDir         string

	// BindAddr used for serf
	BindAddr       string
	RPCPort        int
	NodeName       string
	StartJoinAddrs []string
}

type Agent struct {
	Config

	log        *log.Log
	server     *grpc.Server
	membership *discovery.Membership
	replicator *log.Replicator

	shutdown     bool
	shutdowns    chan struct{}
	shutdownLock sync.Mutex
}

func (c Config) RPCAddr() (string, error) {
	host, _, err := net.SplitHostPort(c.BindAddr)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%s:%d", host, c.RPCPort), nil
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
	agent.log, err = log.NewLog(agent.DataDir, log.Config{})
	return err
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
	rpcAddr, err := agent.RPCAddr()
	if err != nil {
		return err
	}

	var opts []grpc.DialOption
	if agent.PeerTLSConfig != nil {
		crds := credentials.NewTLS(agent.PeerTLSConfig)
		opts = append(opts, grpc.WithTransportCredentials(crds))
	}

	conn, err := grpc.Dial(rpcAddr, opts...)
	if err != nil {
		return err
	}

	client := logv1.NewLogClient(conn)
	agent.replicator = &log.Replicator{
		DialOptions: opts,
		LocalServer: client,
	}
	agent.membership, err = discovery.New(agent.replicator, discovery.Config{
		NodeName: agent.NodeName,
		BindAddr: agent.BindAddr,
		Tags: map[string]string{
			"rpc_addr": rpcAddr,
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
		agent.replicator.Close,
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
