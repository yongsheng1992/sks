package log

import (
	"github.com/stretchr/testify/require"
	log_v1 "github.com/yongsheng1992/sks/api/v1"
	"io"
	"io/ioutil"
	"os"
	"testing"
)

func TestSegment(t *testing.T) {
	t.Helper()
	dir, _ := ioutil.TempDir("", "segment-test")
	defer func(name string) {
		err := os.Remove(name)
		if err != nil {

		}
	}(dir)

	want := &log_v1.Record{
		Value: []byte("hello world"),
	}
	c := Config{}
	c.Segment.MaxStoreBytes = 1024
	c.Segment.MaxIndexBytes = entWidth * 3
	seg, err := newSegment(dir, 16, c)
	require.NoError(t, err)
	require.Equal(t, uint64(16), seg.baseOffset, seg.nextOffset)

	for i := uint64(0); i < 3; i++ {
		off, err := seg.Append(want)
		require.NoError(t, err)
		require.Equal(t, 16+i, off)

		got, err := seg.Read(off)
		require.Equal(t, want.Value, got.Value)
	}

	_, err = seg.Append(want)
	require.Equal(t, io.EOF, err)
	require.Equal(t, true, seg.IsMaxed())

	c.Segment.MaxStoreBytes = uint64(len(want.Value) * 3)
	c.Segment.MaxIndexBytes = 1024

	seg, err = newSegment(dir, 16, c)
	require.NoError(t, err)
	require.True(t, seg.IsMaxed())

	err = seg.Remove()
	require.NoError(t, err)

	seg, err = newSegment(dir, 16, c)
	require.NoError(t, err)
	require.False(t, seg.IsMaxed())
}
