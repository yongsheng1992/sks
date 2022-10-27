package log

import (
	"crypto/tls"
	"github.com/hashicorp/raft"
	"net"
	"os"
	"path/filepath"
)

type StreamLayer struct {
	ln              net.Listener
	serverTLSConfig *tls.Config
	peerTLSConfig   *tls.Config
}

func NewStreamLayer(ln net.Listener, serverTLSConfig *tls.Config, peerTLSConfig *tls.Config) *StreamLayer {
	return &StreamLayer{
		ln:              ln,
		serverTLSConfig: serverTLSConfig,
		peerTLSConfig:   peerTLSConfig,
	}
}

type DistributedLog struct {
	Config
	log  *Log
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
		return nil
	}
	l, err := NewLog(logDir, dl.Config)
	if err != nil {
		return err
	}
	dl.log = l
	return nil
}

func (dl *DistributedLog) setupRaft(dataDir string) error {
	return nil
}
