package log

import (
	log_v1 "github.com/yongsheng1992/sks/api/v1"
	"io"
	"io/ioutil"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"
)

const (
	defaultMaxMaxStoreBytes = 1024
	defaultMaxIndexBytes    = 1024
)

type Log struct {
	mu     sync.RWMutex
	Dir    string
	Config Config

	activeSegment *segment
	segments      []*segment
}

func NewLog(dir string, config Config) (*Log, error) {
	if config.Segment.MaxIndexBytes == 0 {
		config.Segment.MaxStoreBytes = defaultMaxMaxStoreBytes
	}
	if config.Segment.MaxIndexBytes == 0 {
		config.Segment.MaxIndexBytes = defaultMaxIndexBytes
	}

	l := &Log{
		Dir:    dir,
		Config: config,
	}
	return l, l.setup()
}

func (l *Log) setup() error {
	files, err := ioutil.ReadDir(l.Dir)
	if err != nil {
		return err
	}

	var baseOffsets []uint64

	for _, file := range files {
		offStr := strings.TrimSuffix(
			file.Name(),
			path.Ext(file.Name()),
		)
		off, err := strconv.ParseUint(offStr, 10, 0)
		if err != nil {
			// todo log this error with warn level
			continue
		}
		baseOffsets = append(baseOffsets, off)
	}

	sort.Slice(baseOffsets, func(i, j int) bool {
		return baseOffsets[i] < baseOffsets[j]
	})

	// There are two files with the same prefix(baseOffset)
	for i := 0; i < len(baseOffsets); i = i + 2 {
		if err := l.newSegment(baseOffsets[i]); err != nil {
			return err
		}
	}

	if l.segments == nil {
		if err := l.newSegment(l.Config.Segment.InitialOffset); err != nil {
			return err
		}
	}
	return nil
}

func (l *Log) newSegment(offset uint64) error {
	seg, err := newSegment(l.Dir, offset, l.Config)
	if err != nil {
		return err
	}
	l.segments = append(l.segments, seg)
	l.activeSegment = seg
	return nil
}

func (l *Log) Append(record *log_v1.Record) (uint64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	off, err := l.activeSegment.Append(record)
	if err != nil {
		return 0, err
	}

	if l.activeSegment.IsMaxed() {
		err = l.newSegment(off + 1)
	}

	return off, err
}

func (l *Log) Read(off uint64) (*log_v1.Record, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	var seg *segment

	// todo use binary search improve performance
	for _, segment := range l.segments {
		if off >= segment.baseOffset && off < segment.nextOffset {
			seg = segment
			break
		}
	}

	// todo compare off with nextOffset
	if seg == nil {
		return nil, log_v1.ErrOffsetOutOfRange{Offset: off}
	}
	return seg.Read(off)
}

func (l *Log) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	for _, segment := range l.segments {
		if err := segment.Close(); err != nil {
			return err
		}
	}

	return nil
}

func (l *Log) Remove() error {
	if err := l.Close(); err != nil {
		return err
	}
	return os.RemoveAll(l.Dir)
}

func (l *Log) Reset() error {
	if err := l.Remove(); err != nil {
		return err
	}
	return l.setup()
}

func (l *Log) LowestOffset() uint64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.segments[0].baseOffset
}

func (l *Log) HighestOffset() uint64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	off := l.activeSegment.nextOffset
	if off == 0 {
		return 0
	}
	return off - 1
}

func (l *Log) Truncate(lowest uint64) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	var i int
	var segment *segment
	for i, segment = range l.segments {
		if lowest < segment.nextOffset-1 {
			break
		}
		if err := segment.Remove(); err != nil {
			return err
		}
	}
	l.segments = l.segments[i:]
	return nil
}

type originReader struct {
	*store
	off int64
}

func (o *originReader) Read(p []byte) (int, error) {
	n, err := o.ReadAt(p, o.off)
	o.off += int64(n)
	return n, err
}

func (l *Log) Reader() io.Reader {
	l.mu.Lock()
	defer l.mu.Unlock()

	reads := make([]io.Reader, len(l.segments))
	for i, segment := range l.segments {
		reads[i] = &originReader{
			segment.store,
			0,
		}
	}

	return io.MultiReader(reads...)
}
