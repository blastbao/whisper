package client

import (
	"github.com/blastbao/whisper/agent"
	"github.com/blastbao/whisper/center"
	"github.com/blastbao/whisper/common"
	"errors"
	"github.com/valyala/gorpc"
	"time"
)

const CONN_TIMEOUT_MILLIS = 20

// net client using gorpc
type Connect struct {
	addr    string
	c       *gorpc.Client
	timeout int
}

func (this *Connect) Start() {
	this.c = gorpc.NewTCPClient(this.addr)
	this.c.Start()

	common.Log.Info("client to node server connected", this.addr)
}

func (this *Connect) Close() {
	if this.c != nil {
		this.c.Stop()
		common.Log.Info("client to node server disconnected", this.addr)
	}
}

func (this *Connect) Upload(oid string, body []byte, mime int, ch chan bool) {
	pack := center.PackRecord{}
	pack.Command = agent.AGENT_SERVER_COMMAND_SAVE

	pack.Body = body
	pack.Rec = center.Record{Oid: oid, Mime: mime}

	var error error
	var resp interface{}
	if this.timeout != 0 {
		resp, error = this.c.CallTimeout(pack, time.Duration(this.timeout)*time.Millisecond)
	} else {
		resp, error = this.c.Call(pack)
	}

	if error != nil {
		if !error.(*gorpc.ClientError).Timeout {
			common.Log.Error("client upload timeout", oid, error)
		} else {
			common.Log.Error("client upload error", oid, error)
		}

		ch <- false
		return
	}

	if resp == nil {
		common.Log.Error("client upload result is nil", oid)
		ch <- false
		return
	}

	packReturn := resp.(center.PackRecord)
	if packReturn.Flag {
		ch <- true
	} else {
		common.Log.Error("client upload error", oid, mime, len(body), packReturn.Msg)
		ch <- false
	}
}

func (this *Connect) Download(rec center.Record) (body []byte, err error) {
	pack := center.PackRecord{}
	pack.Command = agent.AGENT_SERVER_COMMAND_GET
	pack.Rec = rec

	resp, e := this.c.Call(pack)
	if e != nil {
		common.Log.Error("client download error", rec, e)
		err = e
		return
	}

	packReturn := resp.(center.PackRecord)
	if packReturn.Flag {
		return packReturn.Body, nil
	} else {
		common.Log.Error("client download error", rec, packReturn.Msg)
		return body, errors.New(packReturn.Msg)
	}
}
