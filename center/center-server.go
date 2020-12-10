package center

import (
	"encoding/gob"
	"os"
	"strconv"
	"strings"
	"sync"

	"github.com/blastbao/whisper/common"
	"github.com/blastbao/whisper/mediator"
	"github.com/valyala/gorpc"
)

const (
	PUT_BACK_LOG_FILE  = "center-server-put-back.log"
	CMD_CLOSE          = "close"
	CMD_PUTBACK_PREFIX = "putback-"
	CMD_PUT_RECORD     = "save-rec"

	CMD_GET_OID_META      = "get-oid-meta"
	CMD_CHANGE_OID_STATUS = "change-oid-status"

	// command from mediator server
	CMD_MED_CONNECT_OTHER_CENTER = "connect-2-other-center"
	CMD_MED_SET_MASTER           = "set-master"
	CMD_MED_NEW_INDEX            = "new-data"
	CMD_MED_PERSIST_INDEX        = "persist-data"
	CMD_MED_INDEX_INFO           = "data-info"
)

// TODO, add other command if need slaves to keep the same
var need2SyncSlaveCmd []string = []string{CMD_PUT_RECORD, CMD_CHANGE_OID_STATUS}

type PackRecord struct {
	// 命令字
	Command string
	// 请求体
	Body []byte // node server to client
	// Record ID
	Oid string // for get or update status
	// 状态更新
	Status int // for update status
	// Record
	Rec Record // set input / get output
	// 返回码 成功/失败
	Flag bool
	// 返回信息
	Msg string
}

// command dispatch
type CenterServerHandlerFn func(p PackRecord) PackRecord

type CenterServerHandler struct {
	Command string                // 命令字
	Fn      CenterServerHandlerFn // 函数
}

// master/slave model, raft is better
type CenterServer struct {
	IsMaster bool

	// 存储了一组索引对象
	Center     *Center
	CenterHost string

	// 同其它 Center Svr 的连接，如果本节点为 master 则其它截节点为 slave 。
	clientList2OtherCenter []*gorpc.Client // connect to other center instance

	s  *gorpc.Server      // 服务对象
	mc *mediator.NetClient // to mediator

	Handlers                   []*CenterServerHandler
	chPackRecordPutback        chan PackRecord
	mutexWriteLog4SlaveRecover *sync.Mutex // write log when notify slave to recover failed
	closeWg                    sync.WaitGroup
	isRunningPutback           bool
}

// rpc main handler
func (cs *CenterServer) handler(clientAddr string, request interface{}) interface{} {

	// from node server / client / mediator
	p := request.(PackRecord)

	// make sure other slave center server handle ok

	// 如果当前为主节点，且 p.Command 在命令列表中
	if cs.IsMaster && common.ContainsStr(need2SyncSlaveCmd, p.Command) {
		// 逐个 rpc 调用 slave 从节点
		for _, c := range cs.clientList2OtherCenter {

			common.Log.Info("center server sync master to slave pack", cs.CenterHost, c.Addr, p)

			// 调用 rpc
			resp, err := c.Call(p)
			if err != nil {
				// 出错回包
				packReturn := PackRecord{}
				packReturn.Flag = false
				packReturn.Msg = err.Error()
				return packReturn
			}

			respPack := resp.(PackRecord)
			if !respPack.Flag {
				// 失败回包
				return respPack
			}

			// 成功，do nothing
		}
	}

	// 如果命令是 "putback-xxx"，则把 p.Record 保存到对应的 Index 中。
	if strings.HasPrefix(p.Command, CMD_PUTBACK_PREFIX) {
		return cs.putback(p)
	}

	//
	var packReturn PackRecord
	for _, h := range cs.Handlers {

		// 忽略
		if h.Command != p.Command {
			continue
		}

		// 处理
		packReturn = h.Fn(p)
		common.Log.Debug("center server handler match - " + p.Command)
		break
	}

	// slave ok but master not ok

	// 运行至此，意味着 Slaves 都已执行成功，如果 Master 执行失败，则应该放到管道里面。
	if cs.IsMaster && !packReturn.Flag {
		cs.chPackRecordPutback <- p
	}

	return packReturn
}

// recover, usually it's a slave, because master process failed so need slave to "rollback"
// 恢复，通常是一个从机，因为主进程失败了，所以需要从机来 "回滚"
func (cs *CenterServer) putback(p PackRecord) PackRecord {

	r := PackRecord{}

	cmdRaw := p.Command[len(CMD_PUTBACK_PREFIX):]

	// 如果命令是 "PUT_RECORD"
	if CMD_PUT_RECORD == cmdRaw {

		// 取出 Record
		record := p.Rec

		// 更新状态为 "Delete"
		record.Status = common.STATUS_RECORD_DEL

		// 取出 Record 关联信息
		oidInfo := GetOidInfo(record.Oid)

		// 根据 indexId 查询 index ，然后把 record 保存到 index 中。
		e := cs.Center.Set(oidInfo.IndexId, record)

		// 构造返回值
		if e != nil {
			r.Flag = false
			r.Msg = e.Error()
		} else {
			r.Flag = true
		}
	}

	return r
}

func (cs *CenterServer) Start(mediatorHost, centerHost string) {

	gob.Register(
		PackRecord{
			Command: "",
			Body:    []byte{0},
			Rec: Record{
				BlockId: 0,
			},
			Msg:  "",
			Flag: false,
		},
	)

	// 参数检查
	if cs.Center == nil {
		common.Log.Info("start center server failed as center is nil")
		return
	}

	cs.mutexWriteLog4SlaveRecover = new(sync.Mutex)

	// if not including port, add default
	// 设置 CenterSvr 服务监听地址
	addr := centerHost
	if !strings.Contains(addr, ":") {
		addr = addr + ":" + strconv.Itoa(common.SERVER_PORT_CENTER)
	}
	cs.CenterHost = addr

	// 启动 CenterSvr 服务
	cs.s = gorpc.NewTCPServer(addr, cs.handler)
	if e := cs.s.Start(); e != nil {
		common.Log.Error("center server started failed", e)
	} else {
		common.Log.Info("center server started - " + addr)
	}

	// 同 Mediator 建立长连接，当接收到 Mediator 的请求时，会执行相应的 Handler 。
	cs.LetMediate(mediatorHost)
}

func (cs *CenterServer) putback2Slave() {
	cs.isRunningPutback = true
	cs.closeWg.Add(1)

	for {

		pack, ok := <-cs.chPackRecordPutback

		// 收到有效数据，需要同步给 slaves
		if ok {

			pack.Command = CMD_PUTBACK_PREFIX + pack.Command

			for _, c := range cs.clientList2OtherCenter {

				resp, e := c.Call(pack)

				// 调用出错，写入执行日志
				if e != nil {
					common.Log.Error("center server put back 2 slave error", e, pack)
					cs.writePutbackLog(pack)
				} else {
					// 调用成功，但是执行失败，写入执行日志
					packReturn := resp.(PackRecord)
					if !packReturn.Flag {
						common.Log.Error("center server put back 2 slave fail", packReturn.Msg, pack)
						cs.writePutbackLog(pack)
					}
				}
			}

			// 管道已关闭，则应该退出循环，结束协程。
		} else {
			common.Log.Info("center server put back is stopping")
			// 退出前更新状态
			cs.closeWg.Done()
			cs.isRunningPutback = false
			break
		}
	}
}

func (cs *CenterServer) writePutbackLog(pack PackRecord) error {

	cs.mutexWriteLog4SlaveRecover.Lock()
	defer cs.mutexWriteLog4SlaveRecover.Unlock()

	// 获取 home 目录下的 "center-server-put-back.log" 文件路径。
	fn := common.GetUserHomeFile(PUT_BACK_LOG_FILE)
	bb, e := common.Enc(pack)
	if e != nil {
		common.Log.Error("center server put back 2 slave write log error when encode", e, pack)
		return e
	}

	// 把 PackRecord 写入到该文件。
	e = common.Write2File(bb, fn, os.O_APPEND)
	if e != nil {
		common.Log.Error("center server put back 2 slave write log error", e, pack)
	}

	return e
}

func (cs *CenterServer) Close() {

	// 停止后台协程
	if cs.isRunningPutback && cs.chPackRecordPutback != nil {
		close(cs.chPackRecordPutback)
	}

	cs.closeWg.Wait()

	// 关闭所有的 client
	for _, c := range cs.clientList2OtherCenter {
		common.Log.Info("center server client to other closed - " + c.Addr)
		c.Stop()
	}

	// 停止 rpc server
	if cs.s != nil {
		common.Log.Info("center server closed")
		cs.s.Stop()
	}
}

// 创建 rpc client 并添加到 cs.clientList2OtherCenter 中。
func (cs *CenterServer) connect2OtherCenter(addr string) {
	c := gorpc.NewTCPClient(addr)
	c.Start()
	cs.clientList2OtherCenter = append(cs.clientList2OtherCenter, c)

	common.Log.Info("center server client to other server connected - " + addr)
}
