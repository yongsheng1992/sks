package distributed_log

import (
	"bytes"
	"github.com/golang/protobuf/proto"
	"github.com/hashicorp/raft"
	log_v1 "github.com/yongsheng1992/sks/api/v1"
	log "github.com/yongsheng1992/sks/log"
	"io"
)

type RequestType uint8

const (
	AppendRequestType RequestType = 0
)

var _ raft.FSM = (*fsm)(nil)

var _ raft.FSM = (*fsm)(nil)

type fsm struct {
	log *log.Log
}

func newFsm(l *log.Log) *fsm {
	return &fsm{log: l}
}

func (m *fsm) Apply(log *raft.Log) interface{} {
	logData := log.Data
	reqType := RequestType(logData[0])
	switch reqType {
	case AppendRequestType:
		return m.applyAppend(logData[1:])
	}
	return nil
}

func (m *fsm) applyAppend(data []byte) interface{} {
	var req log_v1.ProduceRequest
	err := proto.Unmarshal(data, &req)
	if err != nil {
		return err
	}

	offset, err := m.log.Append(req.Record)
	if err != nil {
		return err
	}

	return &log_v1.ProduceResponse{Offset: offset}
}

func (m *fsm) Snapshot() (raft.FSMSnapshot, error) {
	r := m.log.Reader()
	return &snapshot{reader: r}, nil
}

func (m *fsm) Restore(snapshot io.ReadCloser) error {
	b := make([]byte, log.LenWidth)
	var buf bytes.Buffer
	for i := 0; ; i++ {
		// read length of a record value
		_, err := io.ReadFull(snapshot, b)
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		// read value of a record
		size := int64(log.ByteOrder.Uint64(b))
		if _, err := io.CopyN(&buf, snapshot, size); err != nil {
			return err
		}

		record := &log_v1.Record{}
		if err = proto.Unmarshal(buf.Bytes(), record); err != nil {
			return nil
		}

		if i == 0 {
			m.log.Config.Segment.InitialOffset = record.Offset
			if err := m.log.Reset(); err != nil {
				return err
			}
		}

		if _, err := m.log.Append(record); err != nil {
			return nil
		}
		buf.Reset()
	}

	return nil
}
