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

	wait   wait.Wait
	ticker *time.Ticker
	logger *zap.Logger

	config *KVConfig
}

type KVConfig struct {
	logger *zap.Logger
	ticks  time.Duration
}

func newKVStore(config *KVConfig, proposeC chan<- []byte, commitC <-chan [][]byte) *kvstore {
	store := &kvstore{
		proposeC: proposeC,
		kvStore:  map[string]string{},
		wait:     wait.New(),
		logger:   config.logger,
		ticker:   time.NewTicker(config.ticks),
		config:   config,
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
	store.ticker.Reset(store.config.ticks)
	ch := store.wait.Register(cmd.ID)
	store.proposeC <- buf.Bytes()
	for {
		select {
		case <-ch:
			return nil
		case <-store.ticker.C:
			store.logger.Warn("wait trigger timeout", zap.Uint64("request_id", cmd.ID))
		}
	}
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
					store.logger.Error("decode data error")
				}
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

// getSnapshot create snapshot at this point.
func (store *kvstore) getSnapshot() ([]byte, error) {
	store.mux.Lock()
	defer store.mux.Unlock()

	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(store.kvStore); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (store *kvstore) requestID() uint64 {
	rand.Seed(time.Now().UnixNano())
	return rand.Uint64()
}
