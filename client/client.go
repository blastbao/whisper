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

func (c *Client) Start(mediatorHost string) {
	c.HostLocal = common.GetLocalAddr()
	c.Conf = ConnConf{STRATEGY_FILLING_RATE, 1, 1}
	c.LetMediate(mediatorHost)
}

func (c *Client) LetMediate(mediatorHost string) {

	c.mc = &mediator.NetClient{}

	if e := c.mc.Start(mediatorHost + ":" + strconv.Itoa(common.SERVER_PORT_MEDIATOR)); e != nil {
		common.Log.Error("client mediator client started failed", e)
		return
	} else {
		common.Log.Info("client mediator client started")
	}

	// refresh latest block info
	c.mc.Watch(
		"client-block-refresh",
		func(value, valueOld []byte) {

			arr := bytes.Split(value, common.SP)

			var blocks []*mediator.Block
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

			c.BlockInfoList = blocks
		},
	)

	// refresh conf
	c.mc.Watch(
		"client-conf-refresh",
		func(value, valueOld []byte) {
			var conf ConnConf
			e := common.Dec(value, &conf)

			if e != nil {
				common.Log.Info("client conf refresh error", e)
			} else {
				common.Log.Info("client conf refreshed", conf)
				c.Conf = conf
			}
		},
	)

	// connect to center server
	c.mc.Watch(
		"client-connect-to-center",
		func(value, valueOld []byte) {
			if c.c != nil {
				common.Log.Info("client center client is stopping")
				c.c.Stop()
			}

			centerServerAddr := string(value)
			common.Log.Info("client center addr is " + centerServerAddr)

			c.ConnectToCenter(centerServerAddr)
		},
	)

	// connect to node server
	c.mc.Watch(
		"client-connect-to-node-server",
		func(value, valueOld []byte) {
			str := string(value)
			c.ConnectToNodeServer(str)
		},
	)

}

func (c *Client) ConnectToCenter(addr string) {
	c.c = gorpc.NewTCPClient(addr)
	c.c.Start()
	common.Log.Info("client center client connected")
}

func (c *Client) ConnectToNodeServer(nodeAddrs string) {

	addrs := strings.Split(nodeAddrs, ",")
	common.Log.Info("client to node servers ready to connect - " + nodeAddrs)


	// 遍历已建立的连接列表，如果有连接不属于 addrs 列表，则从 移除并关闭它。
	var alreadyConnectedAddrs []string
	for _, connect := range c.connectList {
		if common.ContainsStr(addrs, connect.addr) {
			common.Log.Info("client to node server already connected - " + connect.addr)
			alreadyConnectedAddrs = append(alreadyConnectedAddrs, connect.addr)
		} else {
			common.Log.Info("client to node server is disconnecting - " + connect.addr)
			connect.Close()
			// todo: remove from c.connectList
			// c.connectList = append(c.connectList[0:i], c.connectList[i+1:]...)
		}
	}

	if len(c.connectList) == 0 {
		c.connectList = make([]*Connect, len(addrs))
	}

	// 新建连接
	for i, addr := range addrs {
		if !common.ContainsStr(alreadyConnectedAddrs, addr) {
			common.Log.Info("client to node server is connecting - " + addr)
			c.connectList[i] = &Connect{addr: addr}
			c.connectList[i].Start()
		}
	}
}

func (c *Client) Close() {

	for _, connect := range c.connectList {
		common.Log.Info("client to node server is disconnecting - " + connect.addr)
		connect.Close()
	}


	if c.c != nil {
		common.Log.Info("client center client stoped")
		c.c.Stop()
	}
	if c.mc != nil {
		common.Log.Info("client mediator client stoped")
		c.mc.Close()
	}
}

func (c *Client) getTargetConnect(addr string) *Connect {
	for _, connect := range c.connectList {
		if strings.HasPrefix(connect.addr, addr) {
			return connect
		}
	}

	return nil
}

func (c *Client) getTargetBlock(blockId int) (block *mediator.Block) {
	for _, block := range c.BlockInfoList {
		if block.BlockId == blockId {
			return block
		}
	}

	return nil
}

// get blocks for writing TODO
func (c *Client) getTargetBlocks() (arr []*mediator.Block) {
	arr = make([]*mediator.Block, c.Conf.CopyNum+1)

	len := len(c.BlockInfoList)
	if c.Conf.Stratigy == STRATEGY_FILLING_RATE {
		for i := 0; i <= c.Conf.CopyNum; i++ {
			if len > i {
				arr[i] = c.BlockInfoList[i]
			}
		}
	} else if c.Conf.Stratigy == STRATEGY_DIR_PART {
		// should be in different disk
	} else if c.Conf.Stratigy == STRATEGY_ADDR_PART {
		// should be in different host
	} else if c.Conf.Stratigy == STRATEGY_VISIT_AVG {
	}

	return arr
}

func (c *Client) Get(oid string) (body []byte, mime int, err error) {
	// use goroutine as an option
	body, mime, err = c.GetOne(oid + "_0")
	if err == nil {
		return
	}

	for i := 1; i <= int(c.Conf.CopyNum); i++ {
		common.Log.Info("client try fetch time " + strconv.Itoa(i) + " for " + oid)
		body, mime, err = c.GetOne(oid + "_" + strconv.Itoa(i))
		if err == nil {
			return
		}
	}

	err = errors.New("client get failed")
	return
}

func (c *Client) GetOne(oid string) (body []byte, mime int, err error) {
	resp, e := c.c.Call(center.PackRecord{Command: center.CMD_GET_OID_META, Oid: oid})
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

	block := c.getTargetBlock(rec.BlockId)
	if block == nil {
		err = errors.New("client target block not found " + strconv.Itoa(rec.BlockId))
		return
	}

	connect := c.getTargetConnect(block.Addr)
	if connect == nil {
		err = errors.New("client target connect not found " + block.Addr)
		return
	}

	body, err = connect.Download(rec)
	return
}

func (c *Client) Save(body []byte, mime int) (oid string, err error) {
	oid = center.GenOidNoSuffix(c.Conf.DataId, c.Conf.CopyNum)

	blocks := c.getTargetBlocks()
	chs := make([]chan bool, len(blocks))
	for i, block := range blocks {
		if block == nil {
			err = errors.New("client not enough block to save")
			return
		}

		connect := c.getTargetConnect(block.Addr)
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
				_, e := c.c.Call(center.PackRecord{Command: center.CMD_CHANGE_OID_STATUS, Oid: oid,
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

func (c *Client) Del(oid string) error {
	_, e := c.c.Call(center.PackRecord{Command: center.CMD_CHANGE_OID_STATUS, Oid: oid,
		Status: common.STATUS_RECORD_DEL})
	return e
}
