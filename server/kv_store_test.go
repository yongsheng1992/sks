package server

import (
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"math/rand"
	"sync"
	"testing"
	"time"
)

var letters = []byte("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func randStr(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}

func TestKVStore(t *testing.T) {
	proposeC := make(chan []byte)
	commitC := make(chan [][]byte)
	logger, _ := zap.NewProduction()
	config := KVConfig{
		logger: logger,
		ticks:  time.Millisecond * 10,
	}
	m := make(map[string]string)

	kv := newKVStore(&config, proposeC, commitC)

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
		key := randStr(i % 26)
		value := randStr(i % 26)
		m[key] = value
		err := kv.Put(key, value)
		require.NoError(t, err)
	}

	snapshot, err := kv.getSnapshot()
	require.NoError(t, err)

	newKv := newKVStore(&config, proposeC, commitC)
	err = newKv.loadSnapshot(snapshot)
	require.NoError(t, err)

	for key, value := range m {
		val, exist := kv.Get(key)
		require.True(t, exist)
		require.Equal(t, value, val)
	}
}

func BenchmarkKvstore_Put(b *testing.B) {
	logger, _ := zap.NewProduction()
	config := KVConfig{
		logger: logger,
		ticks:  time.Millisecond * 20,
	}
	proposeC := make(chan []byte)
	commitC := make(chan [][]byte)

	kv := newKVStore(&config, proposeC, commitC)

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
