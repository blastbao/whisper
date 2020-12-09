package mediator

import (
	"bytes"
	"github.com/blastbao/whisper/common"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
)

var TriggerDelaySec int = 3
var ConnKeepaliveSec int = 30
var ReadLenOnce int = 1024

type PackServerProcessFn func(p Pack, conn net.Conn) Pack
type WatchRegisterCallbackFn func() (value, valueOld []byte)

type CmdHandler struct {
	Cmd string
	Fn  PackServerProcessFn
}

type NetServer struct {
	listener                 *net.TCPListener
	cc                       []net.Conn
	handlers                 []CmdHandler
	watcherKeys              map[string][]WatcherInfo // key is removeAddr
	watcherRegisterCallbacks map[string]WatchRegisterCallbackFn
	hostAddrs                map[string]string // host mapping as client addr
	chAliveReply             chan string
	tickerCheckAlive         *time.Ticker
	AliveCheckLogWriter      *common.BufferWriter
	closeWg                  sync.WaitGroup
}

func (this *NetServer) Start(ip string, port int) error {
	listener, e := net.ListenTCP("tcp", &net.TCPAddr{net.ParseIP(ip), port, ""})
	if e != nil {
		return e
	}
	common.Log.Info("net server started", ip, port)

	this.listener = listener
	this.cc = []net.Conn{}
	this.handlers = []CmdHandler{}
	this.watcherKeys = make(map[string][]WatcherInfo)
	this.watcherRegisterCallbacks = make(map[string]WatchRegisterCallbackFn)
	this.hostAddrs = make(map[string]string)
	this.chAliveReply = make(chan string)

	this.addBaseHandler()

	go func() {
		for {
			conn, e := this.listener.Accept()
			if e != nil {
				common.Log.Error("net server accept error", e)
				if common.IsErrorMatch(e, "use of closed network connection") {
					break
				}
				continue
			}

			e = this.setupKeepalive(conn)
			if e != nil {
				common.Log.Error("net server set keepalive error", e)
				conn.Close()
				continue
			}

			this.cc = append(this.cc, conn)
			common.Log.Info("net server found client connected - " + conn.RemoteAddr().String())
			common.Log.Info("net server client number - " + strconv.Itoa(len(this.cc)))

			go this.handle(conn)
		}
	}()

	go this.checkAliveLog()
	go this.checkAlivePub()

	return nil
}

func (this *NetServer) Close() error {
	for _, conn := range this.cc {
		this.disconnect(conn)
	}
	common.Log.Info("net server is waiting for clients to disconnect")
	this.closeWg.Wait()

	if this.listener != nil {
		common.Log.Info("net server is closing")
		return this.listener.Close()
	}

	if this.tickerCheckAlive != nil {
		common.Log.Info("net server start check alive loop is stopping")
		this.tickerCheckAlive.Stop()
		this.tickerCheckAlive = nil
	}

	return nil
}

func (this *NetServer) disconnect(conn net.Conn) {
	for i, c := range this.cc {
		if conn.RemoteAddr().String() == c.RemoteAddr().String() {
			this.cc = append(this.cc[:i], this.cc[i+1:]...)
			this.closeWg.Done()
			common.Log.Info("net server disconnect client - " + conn.RemoteAddr().String())
			break
		}
	}

	common.Log.Info("net server disconnect client but skip - " + conn.RemoteAddr().String())
}

func (this *NetServer) setupKeepalive(conn net.Conn) error {
	tcpConn := conn.(*net.TCPConn)
	if e := tcpConn.SetKeepAlive(true); e != nil {
		return e
	}
	if e := tcpConn.SetKeepAlivePeriod(time.Duration(ConnKeepaliveSec) * time.Second); e != nil {
		return e
	}
	return nil
}

func (this *NetServer) AddWatchCallback(group, key string, fn WatchRegisterCallbackFn) {
	groupKey := group + "," + key
	_, ok := this.watcherRegisterCallbacks[groupKey]
	if ok {
		delete(this.watcherRegisterCallbacks, groupKey)
	}
	this.watcherRegisterCallbacks[groupKey] = fn
	common.Log.Info("net server add watch register callback done - " + groupKey)
}

func (this *NetServer) AddHandler(cmd string, fn PackServerProcessFn) {
	for i, f := range this.handlers {
		if f.Cmd == cmd {
			this.handlers[i] = CmdHandler{Cmd: cmd, Fn: fn}
			break
		}
	}

	this.handlers = append(this.handlers, CmdHandler{Cmd: cmd, Fn: fn})
	common.Log.Info("net server handler number after add one - " +
		cmd + " - " + strconv.Itoa(len(this.handlers)))
}

func (this *NetServer) addBaseHandler() {
	// check client alive
	this.AddHandler(CMD_REPLY_ALIVE, func(p Pack, conn net.Conn) Pack {
		secondsOfClient := string(p.Body)
		secondsOfServer := time.Now().Unix()

		clientHost := this.GetClientHostByRemoteAddr(conn.RemoteAddr().String())
		this.chAliveReply <- secondsOfClient + "," + strconv.Itoa(int(secondsOfServer)) +
			"," + clientHost
		return PACK_NO_RETURN
	})

	// register client watch
	this.AddHandler(CMD_REGISTER_WATCHER, func(p Pack, conn net.Conn) Pack {
		groupKey := string(p.Body)
		common.Log.Warning("net server register watcher doing", groupKey)

		arr := strings.Split(groupKey, ",")
		if len(arr) != 2 {
			common.Log.Warning("net server register watcher group/key not given", groupKey)
			return PACK_NO_RETURN
		}

		removeAddr := conn.RemoteAddr().String()
		_, ok := this.watcherKeys[removeAddr]
		if !ok {
			this.watcherKeys[removeAddr] = []WatcherInfo{}
		}
		this.watcherKeys[removeAddr] = append(this.watcherKeys[removeAddr],
			WatcherInfo{group: arr[0], key: arr[1]})

		// later tigger
		fn, ok := this.watcherRegisterCallbacks[groupKey]
		if ok {
			go func(groupInner, keyInner string) {
				time.Sleep(time.Duration(TriggerDelaySec) * time.Second)
				common.Log.Info("net server trigger when client after watch register", groupInner, keyInner)

				value, valueOld := fn()
				this.Trigger(Trigger{groupInner, keyInner, value, valueOld})
			}(arr[0], arr[1])
		}

		return Pack{Command: CMD_ADD_WATCHER_DONE, Body: p.Body}
	})

	// link client custom id to it's remote addr so u can notify by custom id
	// host is not enough because client port is rand
	this.AddHandler(CMD_MAPPING_HOST, func(p Pack, conn net.Conn) Pack {
		hostAddr := string(p.Body)
		remoteAddr := conn.RemoteAddr().String()
		this.hostAddrs[hostAddr] = remoteAddr

		common.Log.Info("net server add host addr mapping - " + hostAddr + " to " + remoteAddr)
		return Pack{Command: CMD_MAPPING_HOST, Flag: true}
	})

	// make a notify/trigger/pub
	this.AddHandler(CMD_DO_NOTIFY, func(p Pack, conn net.Conn) Pack {
		isOk := false

		// bytes join together
		arr := bytes.Split(p.Body, common.SP)
		len := len(arr)
		if len == 3 {
			// pub + command + body
			if "pub" == string(arr[0]) {
				this.Pub(Pack{Command: string(arr[1]), Body: arr[2]})
				isOk = true
			}
		} else if len == 4 {
			// notify + client host(remote addr) + command + body
			if "notify" == string(arr[0]) {
				this.Notify(string(arr[1]), Pack{Command: string(arr[2]), Body: arr[3]})
				isOk = true
			}
		} else if len == 5 {
			// tri + group + key + value + valueOld
			if "tri" == string(arr[0]) {
				this.Tri(string(arr[1]), string(arr[2]), arr[3], arr[4])
				isOk = true
			}
		}

		return Pack{Command: CMD_DO_NOTIFY, Flag: isOk}
	})
}

func (this *NetServer) handleEach(pack Pack, conn net.Conn) Pack {
	for _, f := range this.handlers {
		if f.Cmd == pack.Command {
			return f.Fn(pack, conn)
		}
	}

	common.Log.Warning("net server found no handler for", pack.Command)
	return PACK_NO_RETURN
}

func (this *NetServer) handle(conn net.Conn) {
	this.closeWg.Add(1)
	for {
		data := make([]byte, ReadLenOnce)
		length, e := conn.Read(data)
		if e != nil {
			if e == io.EOF {
				// client closed
				common.Log.Error("net server found conn closed - " + conn.RemoteAddr().String())
				this.disconnect(conn)
			} else {
				common.Log.Error("net server handle conn read error - "+conn.RemoteAddr().String(), e)
				conn.Close()
				this.disconnect(conn)
			}
			return
		}
		body := data[:length]
		common.Log.Debug("net server recieve from client", body)

		arr := bytes.Split(body, common.CL)
		for _, one := range arr {
			if len(one) == 0 {
				continue
			}

			var pack Pack
			e = common.Dec(one, &pack)
			if e != nil {
				packReturn := Pack{}
				packReturn.Flag = false
				packReturn.Msg = e.Error()
				r, _ := common.Enc(&packReturn)
				// add CL
				r = append(r, common.CL...)
				conn.Write(r)
			} else {
				if CMD_CLOSE == pack.Command {
					this.Close()
				} else if CMD_QUIT == pack.Command {
					common.Log.Error("net server found conn closed - " + conn.RemoteAddr().String())
					this.disconnect(conn)
				} else {
					packReturn := this.handleEach(pack, conn)
					if packReturn.Command != CMD_NO_RETURN {
						r, _ := common.Enc(&packReturn)
						// add CL
						r = append(r, common.CL...)
						conn.Write(r)
					}
				}
			}
		}

	}
}

func (this *NetServer) ListClients() []string {
	r := []string{}
	for _, c := range this.cc {
		r = append(r, c.RemoteAddr().String())
	}
	return r
}

func (this *NetServer) Tri(group, key string, value, valueOld []byte) []string {
	if "" == group {
		group = WatcherGroupAll
	}
	return this.Trigger(Trigger{group, key, value, valueOld})
}

// need trigger different clients in balance TODO
func (this *NetServer) Trigger(t Trigger) []string {
	addrs := []string{}
	for removeAddr, ww := range this.watcherKeys {
		for _, w := range ww {
			if t.group != w.group || t.key != w.key {
				continue
			}

			if t.group != WatcherGroupAll {
				addrs = append(addrs, removeAddr)
			} else {
				// only one
				if len(addrs) == 0 {
					addrs = append(addrs, removeAddr)
				} else {
					break
				}
			}
		}
	}
	common.Log.Info("net server trigger - " +
		t.group + "," + t.key + " - " + strings.Join(addrs, ","))

	for _, removeAddr := range addrs {
		body := EncTri(&t)
		this.Notify(removeAddr, Pack{Command: CMD_TRIGGER_WATCHER, Body: body, Flag: true})
	}

	return addrs
}

func (this *NetServer) Notify(remoteAddr string, p Pack) {
	body, _ := common.Enc(&p)

	isExist := false
	for _, c := range this.cc {
		addrConn := c.RemoteAddr().String()
		if addrConn == remoteAddr || addrConn == this.hostAddrs[remoteAddr] {
			isExist = true

			// add CL
			body = append(body, common.CL...)
			c.Write(body)
			break
		}
	}

	if !isExist {
		common.Log.Warning("remote addr not exists in clients", remoteAddr)
	}
}

func (this *NetServer) Pub(p Pack) {
	body, _ := common.Enc(&p)
	for _, c := range this.cc {
		// add CL
		body = append(body, common.CL...)
		c.Write(body)
	}
}

func (this *NetServer) checkAlivePub() {
	common.Log.Info("net server start check alive loop")

	ticker := time.NewTicker(time.Duration(common.CheckAliveInterval) * time.Second)
	this.tickerCheckAlive = ticker

	for {
		select {
		case <-ticker.C:
			common.Log.Info("net server check alive publishing")
			this.Pub(Pack{Command: CMD_CHECK_ALIVE})
		}
	}

}

func (this *NetServer) checkAliveLog() {
	common.Log.Info("net server start check alive reply loop")

	for {
		str := <-this.chAliveReply
		if this.AliveCheckLogWriter != nil {
			this.AliveCheckLogWriter.Write(str)
		} else {
			common.Log.Info("net server check alive - " + str)
		}
	}
}

func (this *NetServer) GetClientHostByRemoteAddr(remoteAddr string) string {
	for key, val := range this.hostAddrs {
		if val == remoteAddr {
			return key
		}
	}

	return remoteAddr
}
