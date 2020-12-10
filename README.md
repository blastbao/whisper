whisper
============

It's a distribute system writing in golang for a large number of(10billion+) small files store/read.

------------------
### There are four roles deployed in different hosts.

> mediator, monitor and control the cluster

> agent, node server in charge of saving/reading bytes from disks

> center, meta data storage

> client, provide http service
# whisper






mediator 类似 zk ，用长连接维护活跃列表，支持注册 Watcher 。
