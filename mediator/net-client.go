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

func (this *NetClient) Start(addr string) error {
	this.watcherList = []*Watcher{}

	conn, e := net.Dial("tcp", addr)
	if e != nil {
		return e
	} else {
		common.Log.Info("net client connected - " + addr)
	}
	this.conn = conn
	this.chTrigger = make(chan Trigger)

	this.addBaseHandler()

	go this.handle(conn)
	go this.handleTrigger()
	return nil
}

func (this *NetClient) Close() error {
	if this.conn != nil {
		common.Log.Info("net client is closing")
		return this.conn.Close()
	}

	close(this.chTrigger)
	return nil
}

func (this *NetClient) AddHandler(cmd string, fn PackProcessFn) {
	for i, f := range this.handlers {
		if f.Cmd == cmd {
			this.handlers[i] = CmdClientHandler{Cmd: cmd, Fn: fn}
			break
		}
	}

	this.handlers = append(this.handlers, CmdClientHandler{Cmd: cmd, Fn: fn})
	common.Log.Info("net client handler number after add one - " +
		cmd + " - " + strconv.Itoa(len(this.handlers)))
}

func (this *NetClient) addBaseHandler() {
	this.handlers = []CmdClientHandler{}
	this.AddHandler(CMD_CHECK_ALIVE, func(p Pack) Pack {
		secondsOfClient := strconv.Itoa(int(time.Now().Unix()))
		return Pack{Command: CMD_REPLY_ALIVE, Body: []byte(secondsOfClient)}
	})
}

func (this *NetClient) handleTrigger() {
	for {
		t := <-this.chTrigger
		common.Log.Debug("net client recieve trigger event", t)

		for _, w := range this.watcherList {
			if w.group == t.group && w.key == t.key && w.status == WatcherStatusOk {
				common.Log.Info("net client watcher triggered - " + w.group + "," + w.key)
				w.callback(t.value, t.valueOld)
			}
		}
	}
}

func (this *NetClient) handle(conn net.Conn) {
	for {
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

			var pack Pack
			e = common.Dec(one, &pack)
			if e != nil {
				common.Log.Error("net client recieve pack error", e)
			} else {
				// some special pack handle
				isNext := this.packPreHandle(pack)
				if isNext {
					packReturn := this.process(pack)
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

func (this *NetClient) process(pack Pack) Pack {
	for _, f := range this.handlers {
		if f.Cmd == pack.Command {
			return f.Fn(pack)
		}
	}

	common.Log.Warning("net client found no handler for", pack.Command)
	return PACK_NO_RETURN
}

func (this *NetClient) packPreHandle(pack Pack) bool {
	if pack.Command == CMD_TRIGGER_WATCHER {
		var t Trigger
		DecTri(pack.Body, &t)

		this.chTrigger <- t
		return false
	} else if pack.Command == CMD_ADD_WATCHER_DONE {
		groupKey := string(pack.Body)
		common.Log.Info("net client add watch done - " + groupKey)

		arr := strings.Split(groupKey, ",")
		if len(arr) != 2 {
			common.Log.Warning("net client add watcher fail as group/key not given", groupKey)
			return false
		}

		this.setRegisterWatcher(groupKey)
		return false
	}

	// continue to execute
	return true
}

func (this *NetClient) Send(pack Pack) error {
	body, e := common.Enc(&pack)
	if e != nil {
		return e
	}

	// add CL
	body = append(body, common.CL...)
	_, e = this.conn.Write(body)
	return e
}

func (this *NetClient) WatchInGroup(group, key string, callback WatcherCallback) {
	common.Log.Info("net client add watcher - " + group + "," + key)

	w := &Watcher{group: group, key: key, callback: callback}
	this.watcherList = append(this.watcherList, w)
	this.Send(Pack{Command: CMD_REGISTER_WATCHER, Body: []byte(group + "," + key)})
}

func (this *NetClient) Watch(key string, callback WatcherCallback) {
	this.WatchInGroup(WatcherGroupAll, key, callback)
}

func (this *NetClient) setRegisterWatcher(groupKey string) {
	for _, w := range this.watcherList {
		if (w.group + "," + w.key) == groupKey {
			w.status = WatcherStatusOk
			common.Log.Info("net client watcher register ok - " + groupKey)
			break
		}
	}
}
