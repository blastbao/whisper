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

	// 上传数据到 NodeSvr
	// (1) 把 record 数据保存到本地，得到存储的详情 recSaved 。
	// (2) 把存储详情 recSaved 上报到 Center ，Center 会维护相关索引。
	if AGENT_SERVER_COMMAND_SAVE == pack.Command {

		record := pack.Rec

		// 把 record 数据保存到本地，得到存储的详情 recSaved 。
		recSaved, e := ns.node.SaveLocal(record.Oid, pack.Body)
		if e != nil {
			packReturn.Flag = false
			packReturn.Msg = "node server save local error - " + e.Error()
			return packReturn
		}

		// 填充信息
		recSaved.Oid = record.Oid
		recSaved.Mime = record.Mime

		// 把存储详情 recSaved 上报到 Center ，Center 会维护相关索引。
		packReq := center.PackRecord{Command: center.CMD_PUT_RECORD, Rec: recSaved}
		_, e = ns.c.Call(packReq)
		if e != nil {
			// reset local, monitor check is better
			//ns.Node.ResetLocal(recSaved)
			packReturn.Flag = false
			packReturn.Msg = "node server put rec error - " + e.Error()
			return packReturn
		}

		// 返回成功
		packReturn.Flag = true


	// 从 NodeSvr 下载数据
	// (1) 去指定块 record.BlockId 读取 record 的数据。
	} else if AGENT_SERVER_COMMAND_GET == pack.Command {

		record := pack.Rec
		// 去指定块 record.BlockId 读取 record 的数据
		body, error := ns.node.Get(record)
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

	rec := center.Record{
		BlockId: 0,
	}

	gob.Register(
		center.PackRecord{
			Command: "",
			Body: []byte{0},
			Rec: rec,
			Msg: "",
			Flag: false,
		},
	)

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
	ns.mc.Watch(
		"node-server-connect-to-center",
		func(value, valueOld []byte) {

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
		},
	)

	// refresh latest block info
	ns.mc.Watch(
		"node-server-block-refresh",

		func(value, valueOld []byte) {

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
		},
	)
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
