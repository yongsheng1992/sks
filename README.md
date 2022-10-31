# sks

一个分布式的kv服务。大部分代码来自[proglog](https://github.com/travisjeffery/proglog)，它是[Distributed Services with Go](https://pragprog.com/titles/tjgo/distributed-services-with-go/)的项目源码。

## log package

主要涉及以下结构体：

* Record 需要写入的记录。Record具有全局唯一的序列号。
* Store Record追加写入的文件。
* Index Record的索引文件，加快Record在Store中的定位。
* Segment 关联Store和Index的抽象。
* Log 关联多个Segment的抽象。

## Raft

Raft的工作就是保证日志（只能追加写的文件）在大多数副本中保证一致。这里的日志可以看做是RSM的操作记录，通过重新执行日志即可构建状态一致的RSM。
在实际应用中，需要解决日志执行时间过长和占用存储空间的问题。所以会在指定时间，对当前的RSM做一次快照，快照只是关系状态，远比日志文件要小，这样可以加快RSM的构建，新副本加入集群的时候也会加快。

[hashicorp/raft](https://github.com/hashicorp/raft) 创建Raft节点的方法：
```go
package raft

func NewRaft(
	conf *Config, // 配置文件
	fsm FSM, // 即RSM，需要执行提交后的日志。即commit后的apply
	logs LogStore, // raft日志保存
	stable StableStore, // raft node的状态保存。node的状态（follower/candidate/leader)，当前的任期这些数据需要持久化
	snaps SnapshotStore, // raft 快照保存
	trans Transport, // raft 通信
	)(*Raft, error) {
	//
	return nil, nil
}
```

`FSM`的具体实现比较灵活，它是一个在内存的对象，可以是map、tree等类型的。`FSM`和`SnapshotStore`是强相关的。

# Debug

raft下标是从1开始的。注意`log.Config.InitialOffset`配置。

