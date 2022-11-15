# sks

一个分布式的kv服务。大部分代码来自[proglog](https://github.com/travisjeffery/proglog)，它是[Distributed Services with Go](https://pragprog.com/titles/tjgo/distributed-services-with-go/)的项目源码。

## Raft

使用[etcd-raft](https://github.com/etcd-io/etcd/tree/main/raft)来实现一致性。书上使用的[hashicorp-raft]()，有以下问题： 
* 无法扩展对ConfChange这里消息的应用。hashicorp的实现中，消息体只有node的地址，没有端口信息。这样应用监听到有node加入/离开到集群，但是无法确定应用的端口。
书上使用连接复用来解决这个问题。
* 没有etcd的实现使用广泛

## 计划

* [ ] raft集成
  * [x] 单节点使用。通过`proposeC`提交消息，通过`commitC`应用消息
  * [x] 单节点wal
  * [ ] 单节点快照
  * [ ] 多节点使用、wal和快照
