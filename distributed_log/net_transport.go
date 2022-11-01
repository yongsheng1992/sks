package distributed_log

import (
	"bytes"
	"crypto/tls"
	"fmt"
	"github.com/hashicorp/raft"
	"net"
	"time"
)

var _ raft.StreamLayer = (*StreamLayer)(nil)

const RaftRPC = 1

type StreamLayer struct {
	ln              net.Listener
	serverTLSConfig *tls.Config
	peerTLSConfig   *tls.Config
}

func NewStreamLayer(ln net.Listener, serverTLSConfig *tls.Config, peerTLSConfig *tls.Config) *StreamLayer {
	return &StreamLayer{ln: ln, serverTLSConfig: serverTLSConfig, peerTLSConfig: peerTLSConfig}
}

func (sl *StreamLayer) Dial(address raft.ServerAddress, timeout time.Duration) (net.Conn, error) {
	dialer := &net.Dialer{Timeout: timeout}
	var conn, err = dialer.Dial("tcp", string(address))
	if err != nil {
		return nil, err
	}

	_, err = conn.Write([]byte{byte(RaftRPC)})
	if err != nil {
		return nil, err
	}

	if sl.peerTLSConfig != nil {
		conn = tls.Client(conn, sl.peerTLSConfig)
	}
	return conn, nil
}

func (sl *StreamLayer) Accept() (net.Conn, error) {
	conn, err := sl.ln.Accept()
	if err != nil {
		return nil, err
	}

	b := make([]byte, 1)
	_, err = conn.Read(b)
	if err != nil {
		return nil, err
	}

	if bytes.Compare(b, []byte{byte(RaftRPC)}) != 0 {
		return nil, fmt.Errorf("not a raft rpc")
	}

	if sl.serverTLSConfig != nil {
		return tls.Server(conn, sl.serverTLSConfig), nil
	}
	return conn, nil
}

func (sl *StreamLayer) Close() error {
	return sl.ln.Close()
}

func (sl *StreamLayer) Addr() net.Addr {
	return sl.ln.Addr()
}
