package distributed_log

import (
	"github.com/hashicorp/raft"
	log_v1 "github.com/yongsheng1992/sks/api/v1"
	"github.com/yongsheng1992/sks/log"
)

var _ raft.LogStore = (*logStore)(nil)

type logStore struct {
	log *log.Log
}

func newLogStore(dir string, config log.Config) (*logStore, error) {
	l, err := log.NewLog(dir, config)
	if err != nil {
		return nil, err
	}
	return &logStore{log: l}, nil
}

func (ls *logStore) FirstIndex() (uint64, error) {
	return ls.log.LowestOffset(), nil
}

func (ls *logStore) LastIndex() (uint64, error) {
	return ls.log.HighestOffset(), nil
}

func (ls *logStore) GetLog(index uint64, out *raft.Log) error {
	record, err := ls.log.Read(index)
	if err != nil {
		return err
	}

	out.Data = record.Value
	out.Index = record.Offset
	out.Type = raft.LogType(record.Type)
	out.Term = record.Term

	return nil
}

func (ls *logStore) StoreLog(record *raft.Log) error {
	return ls.StoreLogs([]*raft.Log{record})
}

func (ls *logStore) StoreLogs(records []*raft.Log) error {
	for _, record := range records {
		r := &log_v1.Record{
			Value:  record.Data,
			Offset: record.Index,
			Type:   uint32(record.Type),
			Term:   record.Term,
		}
		_, err := ls.log.Append(r)
		if err != nil {
			return err
		}
	}
	return nil
}

func (ls *logStore) DeleteRange(min, max uint64) error {
	return ls.log.Truncate(max)
}
