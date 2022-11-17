package server

import (
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"sync"
	"testing"
)

func TestKVStore(t *testing.T) {
	proposeC := make(chan []byte)
	commitC := make(chan [][]byte)
	config := KVConfig{
		logger: zap.NewExample(),
	}
	kv := newKVStore(config, proposeC, commitC)

	go func() {
		for {
			select {
			case prop, ok := <-proposeC:
				if !ok {
					return
				}
				commitC <- [][]byte{prop}
			}
		}
	}()

	for i := 0; i < 10000; i++ {
		err := kv.Put("foo", "bar")
		require.NoError(t, err)
	}

	val, exist := kv.Get("foo")
	require.True(t, exist)
	require.Equal(t, "bar", val)
}

func BenchmarkKvstore_Put(b *testing.B) {
	logger, _ := zap.NewProduction()
	config := KVConfig{
		logger: logger,
	}
	proposeC := make(chan []byte)
	commitC := make(chan [][]byte)

	kv := newKVStore(config, proposeC, commitC)

	go func() {
		for {
			select {
			case prop, ok := <-proposeC:
				if !ok {
					return
				}
				commitC <- [][]byte{prop}
			}
		}
	}()

	//rand.Seed(int64(b.N))

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = kv.Put("bar", "for")
	}
}

func BenchmarkMap_Put(b *testing.B) {
	m := make(map[string]string)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		m["foo"] = "bar"
	}
}

func BenchmarkMap_PutWithMutex(b *testing.B) {
	m := make(map[string]string)
	var mux sync.RWMutex
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		mux.Lock()
		m["foo"] = "bar"
		mux.Unlock()
	}
}
