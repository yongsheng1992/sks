package log

import (
	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/require"
	log_v1 "github.com/yongsheng1992/sks/api/v1"
	"io/ioutil"
	"testing"
)

func TestLog(t *testing.T) {
	for scenario, fn := range map[string]func(t *testing.T, log *Log){
		"append and read a record succeeds": testLogAppend,
		"offset out of range error":         testOutOfRangeErr,
		"init with existing segment":        testInitWithExistingSegment,
		"reader":                            testReader,
		"trucate":                           testTruncate,
	} {
		t.Run(scenario, func(t *testing.T) {
			c := Config{}
			c.Segment.MaxStoreBytes = 32
			c.Segment.MaxIndexBytes = 1024
			dir, _ := ioutil.TempDir("", "store-test")
			log, err := NewLog(dir, c)
			require.NoError(t, err)
			fn(t, log)
		})
	}
}

func testLogAppend(t *testing.T, log *Log) {
	record := &log_v1.Record{
		Value: []byte("hello world"),
	}

	off, err := log.Append(record)
	require.NoError(t, err)
	require.Equal(t, uint64(0), off)

	read, err := log.Read(off)
	require.NoError(t, err)
	require.Equal(t, record.Value, read.Value)
}

func testOutOfRangeErr(t *testing.T, log *Log) {
	record, err := log.Read(1)
	apiErr := err.(log_v1.ErrOffsetOutOfRange)
	require.Error(t, err)
	require.Nil(t, record)
	require.Equal(t, uint64(1), apiErr.Offset)
}

func testInitWithExistingSegment(t *testing.T, log *Log) {
	record := &log_v1.Record{
		Value: []byte("hello, world"),
	}
	for i := 0; i < 3; i++ {
		_, err := log.Append(record)
		require.NoError(t, err)
	}

	require.NoError(t, log.Close())

	off := log.LowestOffset()
	require.Equal(t, uint64(0), off)
	off = log.HighestOffset()
	require.Equal(t, uint64(2), off)

	nl, err := NewLog(log.Dir, log.Config)
	require.NoError(t, err)

	require.Equal(t, uint64(0), nl.LowestOffset())
	require.Equal(t, uint64(2), nl.HighestOffset())
}

func testReader(t *testing.T, log *Log) {
	record := &log_v1.Record{
		Value: []byte("hello world"),
	}
	_, err := log.Append(record)
	require.NoError(t, err)

	reader := log.Reader()
	b, err := ioutil.ReadAll(reader)
	readRecord := log_v1.Record{}
	err = proto.Unmarshal(b[LenWidth:], &readRecord)
	require.NoError(t, err)
	require.Equal(t, record.Value, readRecord.Value)
}

func testTruncate(t *testing.T, log *Log) {
	record := &log_v1.Record{
		Value: []byte("hello world"),
	}
	for i := 0; i < 4; i++ {
		_, err := log.Append(record)
		require.NoError(t, err)
	}

	off := log.LowestOffset()
	require.Equal(t, uint64(0), off)

	err := log.Truncate(2)
	require.NoError(t, err)
	off = log.LowestOffset()
	require.Equal(t, uint64(2), off)
}
