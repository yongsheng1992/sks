package log

import (
	"github.com/stretchr/testify/require"
	"io/ioutil"
	"os"
	"testing"
)

func TestIndex(t *testing.T) {
	f, err := ioutil.TempFile("", "index_test_file")
	require.NoError(t, err)
	defer func(name string) {
		_ = os.Remove(name)
	}(f.Name())

	idx, err := newIndex(f)
	require.NoError(t, err)
	_, _, err = idx.Read(-1)
	require.Error(t, err)

	entries := []struct {
		Off uint32
		Pos uint64
	}{
		{
			Off: 0,
			Pos: 0,
		},
		{
			Off: 1,
			Pos: 10,
		},
	}

	for _, entry := range entries {
		err = idx.Write(entry.Off, entry.Pos)
		require.NoError(t, err)
		out, pos, err := idx.Read(int64(entry.Off))
		require.NoError(t, err)
		require.Equal(t, entry.Off, out)
		require.Equal(t, entry.Pos, pos)
	}

	err = idx.Close()
	require.NoError(t, err)

	f, err = os.OpenFile(f.Name(), os.O_RDWR, 0600)
	idx, err = newIndex(f)
	require.NoError(t, err)

	out, pos, err := idx.Read(-1)
	require.NoError(t, err)
	require.Equal(t, entries[1].Off, out)
	require.Equal(t, entries[1].Pos, pos)

}
