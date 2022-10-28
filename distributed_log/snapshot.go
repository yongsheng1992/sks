package distributed_log

import (
	"github.com/hashicorp/raft"
	"io"
)

var _ raft.FSMSnapshot = (*snapshot)(nil)

type snapshot struct {
	reader io.Reader
}

func (s *snapshot) Persist(sink raft.SnapshotSink) error {
	if _, err := io.Copy(sink, s.reader); err != nil {
		_ = sink.Close()
		return err
	}
	return sink.Close()
}

func (s *snapshot) Release() {

}
