package center

import (
	"github.com/blastbao/whisper/common"
	"github.com/blastbao/whisper/mediator"
	"strconv"
)


// Center 同 Mediator 建立长连接，当接收到 Mediator 的请求时，会执行下面的 Handler 。
//
// CMD_MED_NEW_INDEX: 创建新的 Index 对象
// CMD_MED_INDEX_INFO: 遍历所有 c.indexes ，取出所含数据条数
// CMD_MED_PERSIST_INDEX:  将 c.indexes 索引持久化到索引文件
// CMD_MED_SET_MASTER:
// CMD_MED_CONNECT_OTHER_CENTER: 创建 rpc client 并添加到 cs.clientList2OtherCenter 中。
//
//
func (cs *CenterServer) LetMediate(mediatorHost string) {
	cs.mc = &mediator.NetClient{}

	// new-index
	cs.mc.AddHandler(
		CMD_MED_NEW_INDEX,
		func(p mediator.Pack) mediator.Pack {
			// 回包
			r := mediator.Pack{}
			r.Command = CMD_MED_NEW_INDEX
			dir := string(p.Body)
			// 创建新的 Index 对象
			newIndexId, e := cs.Center.NewIndex(dir)
			if e != nil {
				r.Flag = false
				r.Msg = e.Error()
			} else {
				// 执行成功，返回创建成功的 IndexId
				r.Body = []byte(strconv.Itoa(newIndexId))
				r.Flag = true
			}
			return r
		},
	)

	cs.mc.AddHandler(
		CMD_MED_INDEX_INFO,
		func(p mediator.Pack) mediator.Pack {

			// 回包
			r := mediator.Pack{}
			r.Command = CMD_MED_INDEX_INFO

			// 遍历所有 indexes ，取出所含数据条数
			m := cs.Center.RecordCounts()

			// 序列化返回值
			body, e := common.Enc(m)
			if e != nil {
				r.Flag = false
				r.Msg = e.Error()
			} else {
				r.Body = body
				r.Flag = true
			}

			return r
		},
	)

	cs.mc.AddHandler(
		CMD_MED_PERSIST_INDEX,
		func(p mediator.Pack) mediator.Pack {

			// 回包
			r := mediator.Pack{}
			r.Command = CMD_MED_PERSIST_INDEX

			// 将内存索引持久化到索引文件
			e := cs.Center.Persist()
			if e != nil {
				r.Flag = false
				r.Msg = e.Error()
			} else {
				r.Flag = true
			}

			return r
		},
	)

	cs.mc.AddHandler(
		CMD_MED_SET_MASTER,
		func(p mediator.Pack) mediator.Pack {

			// 判断是否为主节点
			cs.IsMaster = "true" == string(p.Body)

			// 主节点
			if cs.IsMaster {
				// 如果未在执行 PutBack，需启动协程来处理
				if !cs.isRunningPutback {
					cs.chPackRecordPutback = make(chan PackRecord)
					go cs.putback2Slave()
					common.Log.Info("center server put back is running")
				// 如果正在执行 PutBack，无需启动
				} else {
					common.Log.Info("center server put back is already running")
				}
			// 从节点
			} else {
				// 如果正在执行 PutBack ，则关闭 chPackRecordPutback 管道。
				if cs.isRunningPutback {
					close(cs.chPackRecordPutback)
				}
			}

			// 回包
			r := mediator.Pack{}
			r.Command = mediator.CMD_NO_RETURN
			r.Flag = true
			return r
		},
	)

	cs.mc.AddHandler(
		CMD_MED_CONNECT_OTHER_CENTER,
		func(p mediator.Pack) mediator.Pack {
			// 目标 addr
			addr := string(p.Body)
			// 创建 rpc client 并添加到 cs.clientList2OtherCenter 中。
			cs.connect2OtherCenter(addr)
			// 回包
			r := mediator.Pack{}
			r.Command = mediator.CMD_NO_RETURN
			r.Flag = true
			return r
		},
	)

	if e := cs.mc.Start(mediatorHost + ":" + strconv.Itoa(common.SERVER_PORT_MEDIATOR)); e != nil {
		// 如果启动失败，打印日志后退出。
		common.Log.Error("center server mediator client started failed", e)
		return
	} else {
		// 如果启动成功，把本地地址和 CenterHost 映射关系知会到 mediator 。
		common.Log.Info("center server mediator client started")
		cs.mc.Send(mediator.Pack{Command: mediator.CMD_MAPPING_HOST, Body: []byte(cs.CenterHost)})
	}
}
