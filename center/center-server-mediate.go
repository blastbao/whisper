package center

import (
	"github.com/blastbao/whisper/common"
	"github.com/blastbao/whisper/mediator"
	"strconv"
)

func (cs *CenterServer) LetMediate(mediatorHost string) {
	cs.mc = &mediator.NetClient{}


	// new-data
	cs.mc.AddHandler(
		CMD_MED_NEW_DATA,
		func(p mediator.Pack) mediator.Pack {
			// 回包
			r := mediator.Pack{}
			r.Command = CMD_MED_NEW_DATA
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
		CMD_MED_DATA_INFO,
		func(p mediator.Pack) mediator.Pack {

			// 回包
			r := mediator.Pack{}
			r.Command = CMD_MED_DATA_INFO

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
		CMD_MED_PERSIST_DATA,
		func(p mediator.Pack) mediator.Pack {
			r := mediator.Pack{}
			r.Command = CMD_MED_PERSIST_DATA

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

			cs.IsMaster = "true" == string(p.Body)

			// true
			if cs.IsMaster {

				//
				if !cs.isRunningPutback {
					cs.chPackRecordPutback = make(chan PackRecord)
					go cs.putback2Slave()
					common.Log.Info("center server put back is running")
				} else {
					common.Log.Info("center server put back is already running")
				}


			} else {

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

			addr := string(p.Body)
			cs.connect2OtherCenter(addr)

			r := mediator.Pack{}
			r.Command = mediator.CMD_NO_RETURN
			r.Flag = true
			return r
		},
	)

	if e := cs.mc.Start(mediatorHost + ":" + strconv.Itoa(common.SERVER_PORT_MEDIATOR)); e != nil {
		common.Log.Error("center server mediator client started failed", e)
		return
	} else {
		common.Log.Info("center server mediator client started")
		cs.mc.Send(mediator.Pack{Command: mediator.CMD_MAPPING_HOST, Body: []byte(cs.CenterHost)})
	}
}
