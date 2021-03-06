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
	Stratigy int  	// 路由策略
	CopyNum  int	// 副本数
	IndexId  int 	// 写入的 Index // for balance
}


//
func (c *Client) Start(mediatorHost string) {
	c.HostLocal = common.GetLocalAddr()
	c.Conf = ConnConf{STRATEGY_FILLING_RATE, 1, 1}
	c.LetMediate(mediatorHost)
}

// 同 mediator server 建立连接并注册了一组 Watchers 。
func (c *Client) LetMediate(mediatorHost string) {

	// 同 mediator server 建立连接
	c.mc = &mediator.NetClient{}
	if e := c.mc.Start(mediatorHost + ":" + strconv.Itoa(common.SERVER_PORT_MEDIATOR)); e != nil {
		common.Log.Error("client mediator client started failed", e)
		return
	} else {
		common.Log.Info("client mediator client started")
	}

	// 注册 watcher 到 mediator server 上，当收到 trigger 时，client 会自动调用回调函数。

	// refresh latest block info
	c.mc.Watch(
		"client-block-refresh",
		func(value, valueOld []byte) {

			// 解析块列表
			arr := bytes.Split(value, common.SP)
			var blocks []*mediator.Block
			for _, b := range arr {

				if len(b) == 0 {
					continue
				}

				// 反序列化
				var block mediator.Block
				if err := common.Dec(b, &block); err != nil {
					common.Log.Error("client block refresh decode error", err)
					return
				}

				common.Log.Info("client block refresh get block", block)
				blocks = append(blocks, &block)
			}

			// 刷新块信息列表
			c.BlockInfoList = blocks
		},
	)

	// refresh conf
	c.mc.Watch(
		"client-conf-refresh",
		func(value, valueOld []byte) {

			// 解析配置
			var conf ConnConf
			e := common.Dec(value, &conf)

			if e != nil {
				common.Log.Info("client conf refresh error", e)
			} else {
				common.Log.Info("client conf refreshed", conf)
				// 更新配置
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

			// 获取 center 地址
			centerServerAddr := string(value)
			common.Log.Info("client center addr is " + centerServerAddr)
			// 建立连接
			c.ConnectToCenter(centerServerAddr)
		},
	)

	// connect to node server
	c.mc.Watch(
		"client-connect-to-node-server",
		func(value, valueOld []byte) {
			str := string(value)
			// 建立连接
			c.ConnectToNodeServer(str)
		},
	)
}

//
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


// 查询 node server 的连接
func (c *Client) getTargetConnect(addr string) *Connect {
	for _, connect := range c.connectList {
		if strings.HasPrefix(connect.addr, addr) {
			return connect
		}
	}

	return nil
}

// 查询 blockId 的块信息
func (c *Client) getTargetBlock(blockId int) (block *mediator.Block) {
	for _, block := range c.BlockInfoList {
		if block.BlockId == blockId {
			return block
		}
	}

	return nil
}

// get blocks for writing TODO
//
// 从 c.BlockInfoList 中取出 c.Conf.CopyNum+1 个 Block ，用于写入数据。
func (c *Client) getTargetBlocks() (arr []*mediator.Block) {


	arr = make([]*mediator.Block, c.Conf.CopyNum+1)

	len := len(c.BlockInfoList)
	if c.Conf.Stratigy == STRATEGY_FILLING_RATE {
		for i := 0; i <= c.Conf.CopyNum; i++ {
			if i < len {
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


// 多副本下载
func (c *Client) Get(oid string) (body []byte, mime int, err error) {

	// use goroutine as an option

	// 查询第一个副本
	body, mime, err = c.GetOne(oid + "_0")
	if err == nil {
		return
	}

	// 查询其它副本
	for i := 1; i <= int(c.Conf.CopyNum); i++ {
		common.Log.Info("client try fetch time " + strconv.Itoa(i) + " for " + oid)
		body, mime, err = c.GetOne(oid + "_" + strconv.Itoa(i))
		if err == nil {
			return
		}
	}

	// 报错
	err = errors.New("client get failed")
	return
}

// 下载
//
// (1) 从 center svr 查询 oid 对应的 Record 信息
// (2) 根据 Record.BlockId 确定 record 归属的块 block
// (3) 根据 block.Addr 确定 block 归属的 NodeSvr 地址，获取同该 NodeSvr 的网络连接
// (4) 从 NodeSvr 读取 Record 详细数据
func (c *Client) GetOne(oid string) (body []byte, mime int, err error) {

	// 调用 center svr 查询 oid 对应的 saveRecord 信息
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

	// 在 c.BlockInfoList 中查询 blockId 的块信息
	block := c.getTargetBlock(rec.BlockId)
	if block == nil {
		err = errors.New("client target block not found " + strconv.Itoa(rec.BlockId))
		return
	}

	// 获取块 block 存储的 node svr 地址
	connect := c.getTargetConnect(block.Addr)
	if connect == nil {
		err = errors.New("client target connect not found " + block.Addr)
		return
	}

	// 去 node svr 下载 record
	body, err = connect.Download(rec)
	return
}

// 多副本保存
func (c *Client) Save(body []byte, mime int) (oid string, err error) {

	// oid = indexId_copyNum_RandInt_RandInt
	oid = center.GenOidNoSuffix(c.Conf.IndexId, c.Conf.CopyNum)

	// 从 c.BlockInfoList 中取出 c.Conf.CopyNum+1 个 Block ，用于写入数据。
	blocks := c.getTargetBlocks()

	// 监听写入结果
	chs := make([]chan bool, len(blocks))

	// 往每个 block 中写入新数据（多副本）
	for i, block := range blocks {

		if block == nil {
			err = errors.New("client not enough block to save")
			return
		}

		// 获取 block 所在 NodeSvr 的地址
		connect := c.getTargetConnect(block.Addr)
		if connect == nil {
			msg := "client save but connect not found " + block.Addr
			common.Log.Error(msg)
			err = errors.New(msg)
			return
		}

		// 结果管道
		chs[i] = make(chan bool, 1)

		// main is the first and end width _0
		// 副本号
		oidCopy := oid + "_" + strconv.Itoa(i)

		// 后台上传数据到 NodeSvr
		go connect.Upload(oidCopy, body, mime, chs[i])
	}


	// every should be writing done
	for i, ch := range chs {

		isOk := <-ch

		// 如果上传失败，调用 Center Svr 将数据 oid 的状态置为不可用
		if !isOk {

			go func() {
				_, e := c.c.Call(
					center.PackRecord{
						Command: center.CMD_CHANGE_OID_STATUS,
						Oid: oid,
						Status: common.STATUS_RECORD_DISABLE,
					},
				)
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
	// 调用 Center Svr 将数据 oid 的状态置为已删除
	_, e := c.c.Call(
		center.PackRecord{
			Command: center.CMD_CHANGE_OID_STATUS,
			Oid: oid,
			Status: common.STATUS_RECORD_DEL,
		},
	)
	return e
}
