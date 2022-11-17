package server

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"github.com/coreos/etcd/pkg/wait"
	"github.com/coreos/etcd/snap"
	"go.uber.org/zap"
	"math/rand"
	"sync"
	"time"
)

const (
	Put    = "put"
	Delete = "delete"
	Get    = "get"
)

type kvstore struct {
	mux         sync.RWMutex
	proposeC    chan<- []byte
	kvStore     map[string]string
	snapshotter *snap.Snapshotter

	wait wait.Wait

	logger *zap.Logger
}

type KVConfig struct {
	logger *zap.Logger
}

func newKVStore(config KVConfig, proposeC chan<- []byte, commitC <-chan [][]byte) *kvstore {
	store := &kvstore{
		proposeC: proposeC,
		kvStore:  map[string]string{},
		wait:     wait.New(),
		logger:   config.logger,
	}
	go store.apply(commitC)
	return store
}

type Command struct {
	ID  uint64
	Op  string
	Key string
	Val string
}

func (store *kvstore) Get(key string) (string, bool) {
	store.mux.RLock()
	defer store.mux.RUnlock()

	val, exist := store.kvStore[key]
	return val, exist
}

func (store *kvstore) Put(key string, val string) error {
	store.logger.Debug(fmt.Sprintf("put key:%s value:%s", key, val))
	var buf bytes.Buffer
	cmd := Command{
		ID:  store.requestID(),
		Op:  "put",
		Key: key,
		Val: val,
	}
	if err := gob.NewEncoder(&buf).Encode(cmd); err != nil {
		return err
	}
	store.proposeC <- buf.Bytes()
	ch := store.wait.Register(cmd.ID)
	<-ch
	return nil
}

func (store *kvstore) apply(commitC <-chan [][]byte) {
	for {
		select {
		case data, ok := <-commitC:
			store.logger.Debug("receive commit log...", zap.Uint("len", uint(len(data))))
			if !ok {
				return
			}
			if len(data) == 0 {
				continue
			}
			for _, item := range data {
				cmd := Command{}
				if err := gob.NewDecoder(bytes.NewReader(item)).Decode(&cmd); err != nil {
				}
				store.logger.Debug(fmt.Sprintf("receive cmd %v", cmd))
				switch cmd.Op {
				case Put:
					store.mux.Lock()
					store.kvStore[cmd.Key] = cmd.Val
					store.mux.Unlock()
				case Delete:
					store.mux.Lock()
					delete(store.kvStore, cmd.Key)
					store.mux.Unlock()
				}
				store.wait.Trigger(cmd.ID, struct{}{})
			}
		}
	}
}

func (store *kvstore) requestID() uint64 {
	rand.Seed(time.Now().UnixNano())
	return rand.Uint64()
}
