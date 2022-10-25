# sks

一个分布式的kv服务。大部分代码来自[proglog](https://github.com/travisjeffery/proglog)，它是[Distributed Services with Go](https://pragprog.com/titles/tjgo/distributed-services-with-go/)的项目源码。

## log package

主要涉及以下结构体：

* Record 需要写入的记录。Record具有全局唯一的序列号。
* Store Record追加写入的文件。
* Index Record的索引文件，加快Record在Store中的定位。
* Segment 关联Store和Index的抽象。
* Log 关联多个Segment的抽象。


