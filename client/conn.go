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
	c       *gorpc.Client // node svr connection
	timeout int
}

func (c *Connect) Start() {
	c.c = gorpc.NewTCPClient(c.addr)
	c.c.Start()

	common.Log.Info("client to node server connected", c.addr)
}

func (c *Connect) Close() {
	if c.c != nil {
		c.c.Stop()
		common.Log.Info("client to node server disconnected", c.addr)
	}
}


// 上传 Record 到 nodeSvr
func (c *Connect) Upload(oid string, body []byte, mime int, ch chan bool) {

	// 构造上传请求
	pack := center.PackRecord{}
	pack.Command = agent.AGENT_SERVER_COMMAND_SAVE
	pack.Body = body
	pack.Rec = center.Record{Oid: oid, Mime: mime}

	var error error
	var resp interface{}
	if c.timeout != 0 {
		resp, error = c.c.CallTimeout(pack, time.Duration(c.timeout)*time.Millisecond)
	} else {
		resp, error = c.c.Call(pack)
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

// 从 nodeSvr 下载 Record
func (c *Connect) Download(rec center.Record) (body []byte, err error) {
	pack := center.PackRecord{}
	pack.Command = agent.AGENT_SERVER_COMMAND_GET
	pack.Rec = rec

	resp, e := c.c.Call(pack)
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
