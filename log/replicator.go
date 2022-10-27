package log

import (
	"context"
	logv1 "github.com/yongsheng1992/sks/api/v1"
	"google.golang.org/grpc"
	"sync"
)

type Replicator struct {
	mu          sync.Mutex
	LocalServer logv1.LogClient
	DialOptions []grpc.DialOption
	Servers     map[string]chan struct{}
	closed      bool
	close       chan struct{}
}

func (r *Replicator) init() {
	if r.close == nil {
		r.close = make(chan struct{})
	}
	if r.Servers == nil {
		r.Servers = make(map[string]chan struct{})
	}
}

func (r *Replicator) Join(name, addr string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.init()
	if r.closed {
		return nil
	}

	if _, ok := r.Servers[name]; ok {
		return nil
	}
	r.Servers[name] = make(chan struct{})
	go r.replicate(name, r.Servers[name])
	return nil
}

// replicate records from the given addr. So this time, this server can not produce record from outside client
func (r *Replicator) replicate(addr string, leave chan struct{}) {
	cc, err := grpc.Dial(addr, r.DialOptions...)
	if err != nil {
		return
	}

	client := logv1.NewLogClient(cc)

	ctx := context.Background()
	stream, err := client.ConsumeStream(ctx, &logv1.ConsumeRequest{Offset: 0})

	if err != nil {
		return
	}

	records := make(chan *logv1.Record)

	go func() {
		for {
			recv, err := stream.Recv()
			if err != nil {
				return
			}
			records <- recv.Record
		}
	}()

	for {
		select {
		case <-r.close:
			return
		case <-leave:
			return
		case record := <-records:
			_, err = r.LocalServer.Produce(ctx, &logv1.ProduceRequest{Record: record})
			if err != nil {
				return
			}
		}
	}
}

// Leave leaves one server
func (r *Replicator) Leave(name string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.init()
	if _, ok := r.Servers[name]; !ok {
		return nil
	}

	close(r.Servers[name])
	delete(r.Servers, name)
	return nil
}

func (r *Replicator) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.closed {
		return nil
	}
	r.closed = true
	close(r.close)
	return nil
}
