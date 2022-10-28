package log

import (
	"io"
	"os"
	"syscall"
)

// offset 索引小标
// pos 对于在store文件中的位置
var (
	offWidth uint64 = 4
	posWidth uint64 = 8
	entWidth        = offWidth + posWidth
)

type index struct {
	*os.File
	mmap []byte
	size uint64
}

func newIndex(file *os.File, c Config) (*index, error) {
	index := &index{
		File: file,
	}

	fi, err := file.Stat()
	if err != nil {
		return nil, err
	}
	index.size = uint64(fi.Size())

	if err := os.Truncate(file.Name(), int64(c.Segment.MaxIndexBytes)); err != nil {
		return nil, err
	}

	b, err := syscall.Mmap(int(file.Fd()), 0, int(c.Segment.MaxIndexBytes), syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		return nil, err
	}

	index.mmap = b
	return index, nil
}

func (i *index) Close() error {
	err := syscall.Munmap(i.mmap)
	if err != nil {
		return err
	}

	if err := i.File.Sync(); err != nil {
		return err
	}

	if err := i.File.Truncate(int64(i.size)); err != nil {
		return err
	}

	return i.File.Close()
}

// 给定索引，获取索引对应的偏移量
func (i *index) Read(in int64) (out uint32, pos uint64, err error) {
	if i.size == 0 {
		return 0, 0, io.EOF
	}

	if in == -1 {
		out = uint32(i.size/entWidth) - 1
	} else {
		out = uint32(in)
	}

	pos = uint64(out) * entWidth
	if i.size < pos+entWidth {
		return 0, 0, io.EOF
	}
	out = ByteOrder.Uint32(i.mmap[pos : pos+offWidth])
	pos = ByteOrder.Uint64(i.mmap[pos+offWidth : pos+entWidth])
	return out, pos, nil
}

func (i *index) Write(off uint32, pos uint64) error {
	if uint64(len(i.mmap)) < i.size+entWidth {
		return io.EOF
	}
	ByteOrder.PutUint32(i.mmap[i.size:i.size+offWidth], off)
	ByteOrder.PutUint64(i.mmap[i.size+offWidth:i.size+entWidth], pos)
	i.size += entWidth

	return nil
}
