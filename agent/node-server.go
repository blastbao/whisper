package agent

import (
	"bytes"
	"container/list"
	"encoding/gob"
	"strconv"
	"sync"

	"github.com/blastbao/whisper/center"
	"github.com/blastbao/whisper/common"
	"github.com/blastbao/whisper/mediator"
	"github.com/valyala/gorpc"
)

const (
	AGENT_SERVER_COMMAND_SAVE  = "save"
	AGENT_SERVER_COMMAND_GET   = "get"
	AGENT_SERVER_COMMAND_CLOSE = "close"
)

type NodeServer struct {
	s    *gorpc.Server       // from client
	c    *gorpc.Client       // to center server
	mc   *mediator.NetClient // to mediator
	node *Node
}

func (this *NodeServer) handler(clientAddr string, request interface{}) interface{} {
	pack := request.(center.PackRecord)
	packReturn := center.PackRecord{}

	if AGENT_SERVER_COMMAND_SAVE == pack.Command {
		rec := pack.Rec

		recSaved, e := this.node.SaveLocal(rec.Oid, pack.Body)
		if e != nil {
			packReturn.Flag = false
			packReturn.Msg = "node server save local error - " + e.Error()

			return packReturn
		}
		recSaved.Oid = rec.Oid
		recSaved.Mime = rec.Mime

		packReq := center.PackRecord{Command: center.CMD_PUT_RECORD, Rec: recSaved}
		_, e = this.c.Call(packReq)
		if e != nil {
			// reset local, monitor check is better
			//this.Node.ResetLocal(recSaved)

			packReturn.Flag = false
			packReturn.Msg = "node server put rec error - " + e.Error()
			return packReturn
		}

		packReturn.Flag = true
	} else if AGENT_SERVER_COMMAND_GET == pack.Command {
		rec := pack.Rec
		body, error := this.node.Get(rec)
		if error != nil {
			packReturn.Flag = false
			packReturn.Msg = error.Error()

			return packReturn
		}
		packReturn.Body = body
		packReturn.Flag = true
	} else if AGENT_SERVER_COMMAND_CLOSE == pack.Command {
		this.s.Stop()
	} else {
		packReturn.Flag = false
		packReturn.Msg = "command found match - save/get/close"

		return packReturn
	}

	return packReturn
}

func (this *NodeServer) Start(mediatorHost, nodeHost string) {
	rec := center.Record{BlockId: 0}
	gob.Register(center.PackRecord{Command: "", Body: []byte{0}, Rec: rec, Msg: "", Flag: false})

	this.node = &Node{}

	addr := nodeHost + ":" + strconv.Itoa(common.SERVER_PORT_AGENT)
	this.s = gorpc.NewTCPServer(addr, this.handler)
	if e := this.s.Start(); e != nil {
		common.Log.Error("node server started failed", e)
		return
	} else {
		common.Log.Info("node server started - " + addr)
	}

	this.LetMediate(mediatorHost)
}

func (this *NodeServer) LetMediate(mediatorHost string) {
	this.mc = &mediator.NetClient{}

	if e := this.mc.Start(mediatorHost + ":" + strconv.Itoa(common.SERVER_PORT_MEDIATOR)); e != nil {
		common.Log.Error("node server mediator client started failed", e)
		return
	} else {
		common.Log.Info("node server mediator client started")
	}

	// connect to center server
	this.mc.Watch("node-server-connect-to-center", func(value, valueOld []byte) {
		centerServerAddr := string(value)
		common.Log.Info("node server center addr is " + centerServerAddr)

		if this.c != nil {
			common.Log.Info("node server center client is already connected - " + this.c.Addr)
			if this.c.Addr == centerServerAddr {
				return
			}

			common.Log.Info("node server center client is stopping")
			this.c.Stop()
		}

		this.ConnectToCenter(centerServerAddr)
	})

	// refresh latest block info
	this.mc.Watch("node-server-block-refresh", func(value, valueOld []byte) {
		arr := bytes.Split(value, common.SP)

		blocks := list.New()
		for _, b := range arr {
			if len(b) == 0 {
				continue
			}
			var block mediator.Block
			e := common.Dec(b, &block)
			if e != nil {
				common.Log.Error("node server block refresh decode error", e)
				return
			}

			common.Log.Info("node server block refresh get block", block)

			blockWrapper := &BlockInServer{block, new(sync.Mutex), false}
			blocks.PushBack(blockWrapper)
		}

		this.node.Blocks = blocks
	})
}

func (this *NodeServer) ConnectToCenter(addr string) {
	this.c = gorpc.NewTCPClient(addr)
	this.c.Start()
	common.Log.Info("node server center client connected")
}

func (this *NodeServer) Close() {
	if this.s != nil {
		common.Log.Info("node server stoped")
		this.s.Stop()
	}
	if this.c != nil {
		common.Log.Info("node server center client stoped")
		this.c.Stop()
	}
	if this.mc != nil {
		common.Log.Info("node server mediator client stoped")
		this.mc.Close()
	}
}
