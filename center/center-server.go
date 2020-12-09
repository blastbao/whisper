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
	PUT_BACK_LOG_FILE     = "center-server-put-back.log"
	CMD_CLOSE             = "close"
	CMD_PUTBACK_PREFIX    = "putback-"
	CMD_PUT_RECORD        = "save-rec"

	CMD_GET_OID_META      = "get-oid-meta"
	CMD_CHANGE_OID_STATUS = "change-oid-status"

	// command from mediator server
	CMD_MED_CONNECT_OTHER_CENTER = "connect-2-other-center"
	CMD_MED_SET_MASTER           = "set-master"
	CMD_MED_NEW_DATA             = "new-data"
	CMD_MED_PERSIST_DATA         = "persist-data"
	CMD_MED_DATA_INFO            = "data-info"
)

// TODO, add other command if need slaves to keep the same
var need2SyncSlaveCmd []string = []string{CMD_PUT_RECORD, CMD_CHANGE_OID_STATUS}

type PackRecord struct {
	Command string
	Body    []byte // node server to client
	Oid     string // for get or update status
	Status  int    // for update status
	Rec     Record // set input / get output
	Flag    bool
	Msg     string
}

// command dispatch
type CenterServerHandlerFn func(p PackRecord) PackRecord

type CenterServerHandler struct {
	Command string
	Fn      CenterServerHandlerFn
}

// master/slave model, raft is better
type CenterServer struct {
	IsMaster                   bool
	Center                     *Center
	CenterHost                 string

	//
	clientList2OtherCenter     []*gorpc.Client // connect to other center instance

	s                          *gorpc.Server
	mc                         *mediator.NetClient // to mediator
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


	// 如果当前为主节点


	//
	if strings.HasPrefix(p.Command, CMD_PUTBACK_PREFIX) {
		return cs.putback(p)
	}

	//
	var packReturn PackRecord
	for _, h := range cs.Handlers {

		if h.Command != p.Command {
			continue
		}

		packReturn = h.Fn(p)
		common.Log.Debug("center server handler match - " + p.Command)
		break
	}

	// slave ok but master not ok
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
	if CMD_PUT_RECORD == cmdRaw {

		p.Rec.Status = common.STATUS_RECORD_DEL

		oidInfo := GetOidInfo(p.Rec.Oid)

		e := cs.Center.Set(oidInfo.DataId, p.Rec)

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

	rec := Record{BlockId: 0}

	gob.Register(PackRecord{Command: "", Body: []byte{0}, Rec: rec, Msg: "", Flag: false})

	if cs.Center == nil {
		common.Log.Info("start center server failed as center is nil")
		return
	}

	cs.mutexWriteLog4SlaveRecover = new(sync.Mutex)

	// if not including port, add default
	addr := centerHost
	if !strings.Contains(addr, ":") {
		addr = addr + ":" + strconv.Itoa(common.SERVER_PORT_CENTER)
	}

	cs.CenterHost = addr

	cs.s = gorpc.NewTCPServer(addr, cs.handler)
	if e := cs.s.Start(); e != nil {
		common.Log.Error("center server started failed", e)
	} else {
		common.Log.Info("center server started - " + addr)
	}

	cs.LetMediate(mediatorHost)
}

func (cs *CenterServer) putback2Slave() {
	cs.isRunningPutback = true
	cs.closeWg.Add(1)
	for {
		pack, more := <-cs.chPackRecordPutback
		if more {
			pack.Command = CMD_PUTBACK_PREFIX + pack.Command

			for _, c := range cs.clientList2OtherCenter {
				resp, e := c.Call(pack)
				if e != nil {
					common.Log.Error("center server put back 2 slave error", e, pack)
					cs.writePutbackLog(pack)
				} else {
					packReturn := resp.(PackRecord)
					if !packReturn.Flag {
						common.Log.Error("center server put back 2 slave fail", packReturn.Msg, pack)
						cs.writePutbackLog(pack)
					}
				}
			}
		} else {
			cs.closeWg.Done()
			common.Log.Info("center server put back is stopping")
			cs.isRunningPutback = false
			break
		}
	}
}

func (cs *CenterServer) writePutbackLog(pack PackRecord) error {
	cs.mutexWriteLog4SlaveRecover.Lock()
	defer cs.mutexWriteLog4SlaveRecover.Unlock()

	fn := common.GetUserHomeFile(PUT_BACK_LOG_FILE)
	bb, e := common.Enc(pack)
	if e != nil {
		common.Log.Error("center server put back 2 slave write log error when encode", e, pack)
		return e
	}
	e = common.Write2File(bb, fn, os.O_APPEND)
	if e != nil {
		common.Log.Error("center server put back 2 slave write log error", e, pack)
	}
	return e
}

func (cs *CenterServer) Close() {

	if cs.isRunningPutback && cs.chPackRecordPutback != nil {
		close(cs.chPackRecordPutback)
	}
	cs.closeWg.Wait()

	for _, c := range cs.clientList2OtherCenter {
		common.Log.Info("center server client to other closed - " + c.Addr)
		c.Stop()
	}

	if cs.s != nil {
		common.Log.Info("center server closed")
		cs.s.Stop()
	}
}

func (cs *CenterServer) connect2OtherCenter(addr string) {
	c := gorpc.NewTCPClient(addr)
	c.Start()
	cs.clientList2OtherCenter = append(cs.clientList2OtherCenter, c)

	common.Log.Info("center server client to other server connected - " + addr)
}
