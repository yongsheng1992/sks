package log

import "github.com/hashicorp/raft"

type Config struct {
	Rafts struct {
		raft.Config
		SteamLayer *StreamLayer
		Bootstrap  bool
	}
	Segment struct {
		MaxStoreBytes uint64
		MaxIndexBytes uint64
		InitialOffset uint64
	}
}
