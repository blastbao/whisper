package mediator

import (
	"bytes"
	"github.com/blastbao/whisper/common"
	"net"
	"strconv"
	"strings"
	"time"
)

var WatcherStatusOk, WatcherStatusFail int = 1, 10

// if group is all, notify all clients, otherwise trigger one client in a group
var WatcherGroupAll string = "all"

type WatcherCallback func(value, valueOld []byte)

// client side watcher info
type Watcher struct {
	group    string
	key      string
	callback WatcherCallback
	status   int // 0 init 1 register ok 10 register fail
}

// server side watcher info
type WatcherInfo struct {
	group string
	key   string
}

type PackProcessFn func(p Pack) Pack
type CmdClientHandler struct {
	Cmd string
	Fn  PackProcessFn
}

type NetClient struct {
	conn        net.Conn
	handlers    []CmdClientHandler
	watcherList []*Watcher
	chTrigger   chan Trigger
}

func (nc *NetClient) Start(addr string) error {
	nc.watcherList = []*Watcher{}

	// 建立通过 mediator 的 tcp 长连接
	conn, e := net.Dial("tcp", addr)
	if e != nil {
		return e
	} else {
		common.Log.Info("net client connected - " + addr)
	}

	nc.conn = conn
	nc.chTrigger = make(chan Trigger)

	nc.addBaseHandler()

	// 处理 mediator serve 推送的请求
	go nc.handle(conn)
	// 监听 mediator serve 推送的 trigger 事件，并调用对应回调函数
	go nc.handleTrigger()
	return nil
}

func (nc *NetClient) Close() error {
	if nc.conn != nil {
		common.Log.Info("net client is closing")
		return nc.conn.Close()
	}

	close(nc.chTrigger)
	return nil
}

func (nc *NetClient) AddHandler(cmd string, fn PackProcessFn) {
	for i, f := range nc.handlers {
		if f.Cmd == cmd {
			nc.handlers[i] = CmdClientHandler{Cmd: cmd, Fn: fn}
			break
		}
	}

	nc.handlers = append(nc.handlers, CmdClientHandler{Cmd: cmd, Fn: fn})
	common.Log.Info("net client handler number after add one - " + cmd + " - " + strconv.Itoa(len(nc.handlers)))
}

func (nc *NetClient) addBaseHandler() {
	nc.handlers = []CmdClientHandler{}
	// mediator 会定时检查连接是否活跃，此时回复 mediator 本连接还活跃。
	nc.AddHandler(
		CMD_CHECK_ALIVE,
		func(p Pack) Pack {
			secondsOfClient := strconv.Itoa(int(time.Now().Unix()))
			return Pack{Command: CMD_REPLY_ALIVE, Body: []byte(secondsOfClient)}
		},
	)
}

func (nc *NetClient) handleTrigger() {
	for {
		// 监听 trigger 事件
		t := <-nc.chTrigger
		common.Log.Debug("net client recieve trigger event", t)

		// 调用 trigger 事件回调函数
		for _, w := range nc.watcherList {
			if w.group == t.group && w.key == t.key && w.status == WatcherStatusOk {
				common.Log.Info("net client watcher triggered - " + w.group + "," + w.key)
				w.callback(t.value, t.valueOld)
			}
		}
	}
}


// 不断从长连接 conn 中读取 mediator server 发来的请求并处理。
func (nc *NetClient) handle(conn net.Conn) {
	for {

		// 读取数据
		data := make([]byte, ReadLenOnce)
		length, e := conn.Read(data)
		if e != nil {
			common.Log.Error("net client handle conn read error", conn.RemoteAddr(), e)
			conn.Close()
			return
		}
		body := data[:length]
		common.Log.Debug("net client recieve from server", body)

		arr := bytes.Split(body, common.CL)
		for _, one := range arr {

			// 0 means it's the last one
			if len(one) == 0 {
				continue
			}

			// 解析请求
			var pack Pack
			e = common.Dec(one, &pack)
			if e != nil {
				common.Log.Error("net client recieve pack error", e)
			} else {
				// some special pack handle

				// 预处理
				isNext := nc.packPreHandle(pack)
				// 需要进一步处理
				if isNext {
					// 根据 f.Cmd 查找 handler ，然后调用 handler.Fn() 处理 pack 请求。
					packReturn := nc.process(pack)
					// 返回响应值
					if packReturn.Command != CMD_NO_RETURN {
						r, _ := common.Enc(&packReturn)
						r = append(r, common.CL...)
						conn.Write(r)
					}
				}
			}
		}
	}
}

func (nc *NetClient) process(pack Pack) Pack {
	// 根据 f.Cmd 查找 handler ，然后调用 handler.Fn() 处理 pack 请求。
	for _, f := range nc.handlers {
		if f.Cmd == pack.Command {
			return f.Fn(pack)
		}
	}
	common.Log.Warning("net client found no handler for", pack.Command)
	return PACK_NO_RETURN
}

// 预处理
func (nc *NetClient) packPreHandle(pack Pack) bool {

	// server 通知有 watcher 事件发生，写入到 Trigger 管道。
	if pack.Command == CMD_TRIGGER_WATCHER {
		var t Trigger
		DecTri(pack.Body, &t)
		nc.chTrigger <- t
		return false // 不需要后续处理

    // 收到 "注册 watcher 成功" 的响应，设置回调函数。
	} else if pack.Command == CMD_ADD_WATCHER_DONE {

		groupKey := string(pack.Body)
		common.Log.Info("net client add watch done - " + groupKey)

		arr := strings.Split(groupKey, ",")
		if len(arr) != 2 {
			common.Log.Warning("net client add watcher fail as group/key not given", groupKey)
			return false
		}

		nc.setRegisterWatcher(groupKey)
		return false // 不需要后续处理
	}

	// continue to execute
	return true // 需要后续处理
}

// 发包
func (nc *NetClient) Send(pack Pack) error {
	body, e := common.Enc(&pack)
	if e != nil {
		return e
	}
	body = append(body, common.CL...)// add CL
	_, e = nc.conn.Write(body)
	return e
}

// 注册 watcher
func (nc *NetClient) WatchInGroup(group, key string, callback WatcherCallback) {
	common.Log.Info("net client add watcher - " + group + "," + key)
	// 先把 watcher 添加到本地列表中，等收到 server 回复后，修改状态为 "WatcherStatusOk" 从而可以进行回调处理。
	w := &Watcher{ group: group, key: key, callback: callback }
	nc.watcherList = append(nc.watcherList, w)
	// 调用 server 接口，注册 watcher 。
	nc.Send(Pack{Command: CMD_REGISTER_WATCHER, Body: []byte(group + "," + key)})
}

// 注册 watcher
func (nc *NetClient) Watch(key string, callback WatcherCallback) {
	nc.WatchInGroup(WatcherGroupAll, key, callback)
}

// 等收到 server 回复后，修改状态为 "WatcherStatusOk" 从而可以进行回调处理。
func (nc *NetClient) setRegisterWatcher(groupKey string) {
	for _, w := range nc.watcherList {
		if (w.group + "," + w.key) == groupKey {
			w.status = WatcherStatusOk
			common.Log.Info("net client watcher register ok - " + groupKey)
			break
		}
	}
}
