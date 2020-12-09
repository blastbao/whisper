package client

import (
	"bytes"
	"github.com/blastbao/whisper/center"
	"github.com/blastbao/whisper/common"
	"errors"
	"github.com/valyala/gorpc"
	"github.com/blastbao/whisper/mediator"
	"strconv"
	"strings"
)

// filling rate/visit load/in different disks/in different hosts
const (
	STRATEGY_FILLING_RATE = 1
	STRATEGY_VISIT_AVG    = 2
	STRATEGY_DIR_PART     = 3
	STRATEGY_ADDR_PART    = 4
	COPY_NUMBER_DEFAULT   = 2
)

type Client struct {
	HostLocal     string
	Conf          ConnConf
	BlockInfoList mediator.BlockList
	connectList   []*Connect          // to node server servers
	c             *gorpc.Client       // to center server
	mc            *mediator.NetClient // to mediator
}

type ConnConf struct {
	Stratigy int
	CopyNum  int
	DataId   int // for balance
}

func (this *Client) Start(mediatorHost string) {
	this.HostLocal = common.GetLocalAddr()
	this.Conf = ConnConf{STRATEGY_FILLING_RATE, 1, 1}
	this.LetMediate(mediatorHost)
}

func (this *Client) LetMediate(mediatorHost string) {
	this.mc = &mediator.NetClient{}

	if e := this.mc.Start(mediatorHost + ":" + strconv.Itoa(common.SERVER_PORT_MEDIATOR)); e != nil {
		common.Log.Error("client mediator client started failed", e)
		return
	} else {
		common.Log.Info("client mediator client started")
	}

	// refresh latest block info
	this.mc.Watch("client-block-refresh", func(value, valueOld []byte) {
		arr := bytes.Split(value, common.SP)

		blocks := []*mediator.Block{}
		for _, b := range arr {
			if len(b) == 0 {
				continue
			}
			var block mediator.Block
			e := common.Dec(b, &block)
			if e != nil {
				common.Log.Error("client block refresh decode error", e)
				return
			}

			common.Log.Info("client block refresh get block", block)
			blocks = append(blocks, &block)
		}

		this.BlockInfoList = blocks
	})

	// refresh conf
	this.mc.Watch("client-conf-refresh", func(value, valueOld []byte) {
		var conf ConnConf
		e := common.Dec(value, &conf)

		if e != nil {
			common.Log.Info("client conf refresh error", e)
		} else {
			common.Log.Info("client conf refreshed", conf)
			this.Conf = conf
		}
	})

	// connect to center server
	this.mc.Watch("client-connect-to-center", func(value, valueOld []byte) {
		if this.c != nil {
			common.Log.Info("client center client is stopping")
			this.c.Stop()
		}

		centerServerAddr := string(value)
		common.Log.Info("client center addr is " + centerServerAddr)

		this.ConnectToCenter(centerServerAddr)
	})

	// connect to node server
	this.mc.Watch("client-connect-to-node-server", func(value, valueOld []byte) {
		str := string(value)
		this.ConnectToNodeServer(str)
	})
}

func (this *Client) ConnectToCenter(addr string) {
	this.c = gorpc.NewTCPClient(addr)
	this.c.Start()
	common.Log.Info("client center client connected")
}

func (this *Client) ConnectToNodeServer(str string) {
	addrs := strings.Split(str, ",")
	common.Log.Info("client to node servers ready to connect - " + str)

	alreadyConnectedAddrs := []string{}
	for _, connect := range this.connectList {
		if common.ContainsStr(addrs, connect.addr) {
			common.Log.Info("client to node server already connected - " + connect.addr)
			alreadyConnectedAddrs = append(alreadyConnectedAddrs, connect.addr)
		} else {
			common.Log.Info("client to node server is disconnecting - " + connect.addr)
			connect.Close()

			// remove from this.connectList TODO
		}
	}

	if len(this.connectList) == 0 {
		this.connectList = make([]*Connect, len(addrs))
	}
	for i, addr := range addrs {
		if !common.ContainsStr(alreadyConnectedAddrs, addr) {
			common.Log.Info("client to node server is connecting - " + addr)
			this.connectList[i] = &Connect{addr: addr}
			this.connectList[i].Start()
		}
	}
}

func (this *Client) Close() {
	for _, connect := range this.connectList {
		common.Log.Info("client to node server is disconnecting - " + connect.addr)
		connect.Close()
	}
	if this.c != nil {
		common.Log.Info("client center client stoped")
		this.c.Stop()
	}
	if this.mc != nil {
		common.Log.Info("client mediator client stoped")
		this.mc.Close()
	}
}

func (this *Client) getTargetConnect(addr string) *Connect {
	for _, connect := range this.connectList {
		if strings.HasPrefix(connect.addr, addr) {
			return connect
		}
	}

	return nil
}

func (this *Client) getTargetBlock(blockId int) (block *mediator.Block) {
	for _, block := range this.BlockInfoList {
		if block.BlockId == blockId {
			return block
		}
	}

	return nil
}

// get blocks for writing TODO
func (this *Client) getTargetBlocks() (arr []*mediator.Block) {
	arr = make([]*mediator.Block, this.Conf.CopyNum+1)

	len := len(this.BlockInfoList)
	if this.Conf.Stratigy == STRATEGY_FILLING_RATE {
		for i := 0; i <= this.Conf.CopyNum; i++ {
			if len > i {
				arr[i] = this.BlockInfoList[i]
			}
		}
	} else if this.Conf.Stratigy == STRATEGY_DIR_PART {
		// should be in different disk
	} else if this.Conf.Stratigy == STRATEGY_ADDR_PART {
		// should be in different host
	} else if this.Conf.Stratigy == STRATEGY_VISIT_AVG {
	}

	return arr
}

func (this *Client) Get(oid string) (body []byte, mime int, err error) {
	// use goroutine as an option
	body, mime, err = this.GetOne(oid + "_0")
	if err == nil {
		return
	}

	for i := 1; i <= int(this.Conf.CopyNum); i++ {
		common.Log.Info("client try fetch time " + strconv.Itoa(i) + " for " + oid)
		body, mime, err = this.GetOne(oid + "_" + strconv.Itoa(i))
		if err == nil {
			return
		}
	}

	err = errors.New("client get failed")
	return
}

func (this *Client) GetOne(oid string) (body []byte, mime int, err error) {
	resp, e := this.c.Call(center.PackRecord{Command: center.CMD_GET_OID_META, Oid: oid})
	if e != nil {
		err = e
		return
	}

	pack := resp.(center.PackRecord)
	if !pack.Flag {
		err = errors.New(pack.Msg)
		return
	}

	rec := pack.Rec
	mime = rec.Mime

	block := this.getTargetBlock(rec.BlockId)
	if block == nil {
		err = errors.New("client target block not found " + strconv.Itoa(rec.BlockId))
		return
	}

	connect := this.getTargetConnect(block.Addr)
	if connect == nil {
		err = errors.New("client target connect not found " + block.Addr)
		return
	}

	body, err = connect.Download(rec)
	return
}

func (this *Client) Save(body []byte, mime int) (oid string, err error) {
	oid = center.GenOidNoSuffix(this.Conf.DataId, this.Conf.CopyNum)

	blocks := this.getTargetBlocks()
	chs := make([]chan bool, len(blocks))
	for i, block := range blocks {
		if block == nil {
			err = errors.New("client not enough block to save")
			return
		}

		connect := this.getTargetConnect(block.Addr)
		if connect == nil {
			msg := "client save but connect not found " + block.Addr
			common.Log.Error(msg)
			err = errors.New(msg)
			return
		}

		chs[i] = make(chan bool, 1)
		// main is the first and end width _0
		oidCopy := oid + "_" + strconv.Itoa(i)
		go connect.Upload(oidCopy, body, mime, chs[i])
	}

	// every should be writing done
	for i, ch := range chs {
		isOk := <-ch

		if !isOk {
			go func() {
				_, e := this.c.Call(center.PackRecord{Command: center.CMD_CHANGE_OID_STATUS, Oid: oid,
					Status: common.STATUS_RECORD_DISABLE})
				if e != nil {
					common.Log.Error("client write fail then disable oid status error", oid, e)
				}
			}()

			msg := "client write fail " + oid + " - " + blocks[i].Addr
			common.Log.Error(msg)
			return oid, errors.New(msg)
		}

	}

	return oid, nil
}

func (this *Client) Del(oid string) error {
	_, e := this.c.Call(center.PackRecord{Command: center.CMD_CHANGE_OID_STATUS, Oid: oid,
		Status: common.STATUS_RECORD_DEL})
	return e
}
