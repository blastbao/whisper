package center

import (
	"common"
	"mediator"
	"strconv"
)

func (this *CenterServer) LetMediate(mediatorHost string) {
	this.mc = &mediator.NetClient{}
	this.mc.AddHandler(CMD_MED_NEW_DATA, func(p mediator.Pack) mediator.Pack {
		r := mediator.Pack{}
		r.Command = CMD_MED_NEW_DATA

		dir := string(p.Body)
		newDataId, e := this.Center.NewData(dir)
		if e != nil {
			r.Flag = false
			r.Msg = e.Error()
		} else {
			r.Body = []byte(strconv.Itoa(newDataId))
			r.Flag = true
		}

		return r
	})

	this.mc.AddHandler(CMD_MED_DATA_INFO, func(p mediator.Pack) mediator.Pack {
		r := mediator.Pack{}
		r.Command = CMD_MED_DATA_INFO

		m := this.Center.DataInfo()
		body, e := common.Enc(m)
		if e != nil {
			r.Flag = false
			r.Msg = e.Error()
		} else {
			r.Body = body
			r.Flag = true
		}

		return r
	})

	this.mc.AddHandler(CMD_MED_PERSIST_DATA, func(p mediator.Pack) mediator.Pack {
		r := mediator.Pack{}
		r.Command = CMD_MED_PERSIST_DATA

		e := this.Center.Persist()
		if e != nil {
			r.Flag = false
			r.Msg = e.Error()
		} else {
			r.Flag = true
		}

		return r
	})

	this.mc.AddHandler(CMD_MED_SET_MASTER, func(p mediator.Pack) mediator.Pack {
		this.IsMaster = "true" == string(p.Body)

		if this.IsMaster {
			if !this.isRunningPutback {
				this.chPackRecordPutback = make(chan PackRecord)
				go this.putback2Slave()
				common.Log.Info("center server put back is running")
			} else {
				common.Log.Info("center server put back is already running")
			}
		} else {
			if this.isRunningPutback {
				close(this.chPackRecordPutback)
			}
		}

		r := mediator.Pack{}
		r.Command = mediator.CMD_NO_RETURN
		r.Flag = true
		return r
	})

	this.mc.AddHandler(CMD_MED_CONNECT_OTHER_CENTER, func(p mediator.Pack) mediator.Pack {
		addr := string(p.Body)
		this.connect2OtherCenter(addr)

		r := mediator.Pack{}
		r.Command = mediator.CMD_NO_RETURN
		r.Flag = true
		return r
	})

	if e := this.mc.Start(mediatorHost + ":" + strconv.Itoa(common.SERVER_PORT_MEDIATOR)); e != nil {
		common.Log.Error("center server mediator client started failed", e)
		return
	} else {
		common.Log.Info("center server mediator client started")
		this.mc.Send(mediator.Pack{Command: mediator.CMD_MAPPING_HOST, Body: []byte(this.CenterHost)})
	}
}
