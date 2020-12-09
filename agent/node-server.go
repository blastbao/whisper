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



//
func (ns *NodeServer) handler(clientAddr string, request interface{}) interface{} {


	pack := request.(center.PackRecord)
	packReturn := center.PackRecord{}

	if AGENT_SERVER_COMMAND_SAVE == pack.Command {
		rec := pack.Rec

		recSaved, e := ns.node.SaveLocal(rec.Oid, pack.Body)
		if e != nil {
			packReturn.Flag = false
			packReturn.Msg = "node server save local error - " + e.Error()

			return packReturn
		}
		recSaved.Oid = rec.Oid
		recSaved.Mime = rec.Mime

		packReq := center.PackRecord{Command: center.CMD_PUT_RECORD, Rec: recSaved}
		_, e = ns.c.Call(packReq)
		if e != nil {
			// reset local, monitor check is better
			//ns.Node.ResetLocal(recSaved)

			packReturn.Flag = false
			packReturn.Msg = "node server put rec error - " + e.Error()
			return packReturn
		}

		packReturn.Flag = true
	} else if AGENT_SERVER_COMMAND_GET == pack.Command {
		rec := pack.Rec
		body, error := ns.node.Get(rec)
		if error != nil {
			packReturn.Flag = false
			packReturn.Msg = error.Error()

			return packReturn
		}
		packReturn.Body = body
		packReturn.Flag = true
	} else if AGENT_SERVER_COMMAND_CLOSE == pack.Command {
		ns.s.Stop()
	} else {
		packReturn.Flag = false
		packReturn.Msg = "command found match - save/get/close"

		return packReturn
	}

	return packReturn
}

func (ns *NodeServer) Start(mediatorHost, nodeHost string) {
	rec := center.Record{BlockId: 0}
	gob.Register(center.PackRecord{Command: "", Body: []byte{0}, Rec: rec, Msg: "", Flag: false})

	ns.node = &Node{}

	addr := nodeHost + ":" + strconv.Itoa(common.SERVER_PORT_AGENT)
	ns.s = gorpc.NewTCPServer(addr, ns.handler)
	if e := ns.s.Start(); e != nil {
		common.Log.Error("node server started failed", e)
		return
	} else {
		common.Log.Info("node server started - " + addr)
	}

	ns.LetMediate(mediatorHost)
}

func (ns *NodeServer) LetMediate(mediatorHost string) {
	ns.mc = &mediator.NetClient{}

	if e := ns.mc.Start(mediatorHost + ":" + strconv.Itoa(common.SERVER_PORT_MEDIATOR)); e != nil {
		common.Log.Error("node server mediator client started failed", e)
		return
	} else {
		common.Log.Info("node server mediator client started")
	}

	// connect to center server
	ns.mc.Watch("node-server-connect-to-center", func(value, valueOld []byte) {
		centerServerAddr := string(value)
		common.Log.Info("node server center addr is " + centerServerAddr)

		if ns.c != nil {
			common.Log.Info("node server center client is already connected - " + ns.c.Addr)
			if ns.c.Addr == centerServerAddr {
				return
			}

			common.Log.Info("node server center client is stopping")
			ns.c.Stop()
		}

		ns.ConnectToCenter(centerServerAddr)
	})

	// refresh latest block info
	ns.mc.Watch("node-server-block-refresh", func(value, valueOld []byte) {
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

		ns.node.Blocks = blocks
	})
}

func (ns *NodeServer) ConnectToCenter(addr string) {
	ns.c = gorpc.NewTCPClient(addr)
	ns.c.Start()
	common.Log.Info("node server center client connected")
}

func (ns *NodeServer) Close() {
	if ns.s != nil {
		common.Log.Info("node server stoped")
		ns.s.Stop()
	}
	if ns.c != nil {
		common.Log.Info("node server center client stoped")
		ns.c.Stop()
	}
	if ns.mc != nil {
		common.Log.Info("node server mediator client stoped")
		ns.mc.Close()
	}
}
