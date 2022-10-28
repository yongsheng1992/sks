package log

import (
	"github.com/hashicorp/raft"
	"github.com/yongsheng1992/sks/distributed_log"
)

type Config struct {
	Raft struct {
		raft.Config
		SteamLayer *distributed_log.StreamLayer
		Bootstrap  bool
	}
	Segment struct {
		MaxStoreBytes uint64
		MaxIndexBytes uint64
		InitialOffset uint64
	}
}
