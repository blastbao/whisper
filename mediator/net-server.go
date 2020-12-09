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

func (ns *NetServer) Start(ip string, port int) error {

	listener, e := net.ListenTCP(
		"tcp",
		&net.TCPAddr{
			net.ParseIP(ip),
			port,
			"",
		},
	)
	if e != nil {
		return e
	}

	common.Log.Info("net server started", ip, port)

	ns.listener = listener
	ns.cc = []net.Conn{}
	ns.handlers = []CmdHandler{}
	ns.watcherKeys = make(map[string][]WatcherInfo)
	ns.watcherRegisterCallbacks = make(map[string]WatchRegisterCallbackFn)
	ns.hostAddrs = make(map[string]string)
	ns.chAliveReply = make(chan string)

	ns.addBaseHandler()

	go func() {

		for {
			// 接收 Connect 请求
			conn, e := ns.listener.Accept()
			if e != nil {
				common.Log.Error("net server accept error", e)
				if common.IsErrorMatch(e, "use of closed network connection") {
					break
				}
				continue
			}

			// 设置长连接(心跳 30s)
			e = ns.setupKeepalive(conn)
			if e != nil {
				common.Log.Error("net server set keepalive error", e)
				conn.Close()
				continue
			}

			// 保存长连接
			ns.cc = append(ns.cc, conn)
			common.Log.Info("net server found client connected - " + conn.RemoteAddr().String())
			common.Log.Info("net server client number - " + strconv.Itoa(len(ns.cc)))

			// 后台处理长连接的请求
			go ns.handle(conn)
		}
	}()


	go ns.checkAliveLog()
	go ns.checkAlivePub()

	return nil
}

func (ns *NetServer) Close() error {

	// 逐个关闭长连接
	for _, conn := range ns.cc {
		ns.disconnect(conn)
	}

	common.Log.Info("net server is waiting for clients to disconnect")
	// 等待所有长连接完成关闭
	ns.closeWg.Wait()

	// 关闭 listener
	if ns.listener != nil {
		common.Log.Info("net server is closing")
		return ns.listener.Close()
	}

	// 关闭定时器
	if ns.tickerCheckAlive != nil {
		common.Log.Info("net server start check alive loop is stopping")
		ns.tickerCheckAlive.Stop()
		ns.tickerCheckAlive = nil
	}

	return nil
}

func (ns *NetServer) disconnect(conn net.Conn) {
	for i, c := range ns.cc {
		// 查找目标连接
		if conn.RemoteAddr().String() == c.RemoteAddr().String() {
			// 将 conn 从 ns.cc 中移除
			ns.cc = append(ns.cc[:i], ns.cc[i+1:]...)
			// 长连接数目减 1
			ns.closeWg.Done()
			common.Log.Info("net server disconnect client - " + conn.RemoteAddr().String())
			break
		}
	}

	common.Log.Info("net server disconnect client but skip - " + conn.RemoteAddr().String())
}

func (ns *NetServer) setupKeepalive(conn net.Conn) error {
	tcpConn := conn.(*net.TCPConn)
	if e := tcpConn.SetKeepAlive(true); e != nil {
		return e
	}
	if e := tcpConn.SetKeepAlivePeriod(time.Duration(ConnKeepaliveSec) * time.Second); e != nil {
		return e
	}
	return nil
}

func (ns *NetServer) AddWatchCallback(group, key string, fn WatchRegisterCallbackFn) {
	groupKey := group + "," + key

	// 若已存在，则先移除旧 watcher
	_, ok := ns.watcherRegisterCallbacks[groupKey]
	if ok {
		delete(ns.watcherRegisterCallbacks, groupKey)
	}

	// 添加新 watcher
	ns.watcherRegisterCallbacks[groupKey] = fn
	common.Log.Info("net server add watch register callback done - " + groupKey)
}

func (ns *NetServer) AddHandler(cmd string, fn PackServerProcessFn) {

	// 若 cmd 已经存在，更新 cmd 的 handler
	for i, f := range ns.handlers {
		if f.Cmd == cmd {
			ns.handlers[i] = CmdHandler{Cmd: cmd, Fn: fn}
			break
		}
	}

	// 若 cmd 尚未存在，新建并插入
	ns.handlers = append(ns.handlers, CmdHandler{Cmd: cmd, Fn: fn})
	common.Log.Info("net server handler number after add one - " + cmd + " - " + strconv.Itoa(len(ns.handlers)))
}

func (ns *NetServer) addBaseHandler() {


	// check client alive
	// 为维持长连接活跃，客户端会发 alive 包，服务端打印日志。
	ns.AddHandler(
		CMD_REPLY_ALIVE,
		func(p Pack, conn net.Conn) Pack {
			secondsOfClient := string(p.Body)
			secondsOfServer := time.Now().Unix()
			clientHost := ns.GetClientHostByRemoteAddr(conn.RemoteAddr().String())
			ns.chAliveReply <- secondsOfClient + "," + strconv.Itoa(int(secondsOfServer)) + "," + clientHost
			return PACK_NO_RETURN
		},
	)

	// register client watcher
	// 长连接客户端注册新 watcher ，当 {group, key} 上有变更时，会通知它们。
	ns.AddHandler(

		// 添加 Watcher
		CMD_REGISTER_WATCHER,
		func(p Pack, conn net.Conn) Pack {

			// 请求解析
			groupKey := string(p.Body)
			common.Log.Warning("net server register watcher doing", groupKey)

			arr := strings.Split(groupKey, ",")
			if len(arr) != 2 {
				common.Log.Warning("net server register watcher group/key not given", groupKey)
				return PACK_NO_RETURN
			}
			group, key := arr[0], arr[1]

			removeAddr := conn.RemoteAddr().String()
			if _, ok := ns.watcherKeys[removeAddr]; !ok {
				ns.watcherKeys[removeAddr] = []WatcherInfo{}
			}

			// 注册 Watcher: removeAddr 正在监听 {group, key} 上的变更。
			ns.watcherKeys[removeAddr] = append(ns.watcherKeys[removeAddr], WatcherInfo{group, key})

			// later tigger
			//
			// 获取 groupKey 上的回调函数，如果存在，则延迟触发。
			if fn, ok := ns.watcherRegisterCallbacks[groupKey]; ok {
				go func(group, key string) {
					common.Log.Info("net server trigger when client after watch register", group, key)
					// 延迟 3 秒
					time.Sleep(time.Duration(TriggerDelaySec) * time.Second)
					// 执行回调
					value, valueOld := fn()
					// 触发回调
					ns.Trigger(Trigger{group, key, value, valueOld})
				}(group, key)
			}

			// 构造响应
			return Pack{Command: CMD_ADD_WATCHER_DONE, Body: p.Body}
		},
	)

	// link client custom id to it's remote addr so u can notify by custom id
	// host is not enough because client port is rand
	//
	// 执行 RemoteAddr 的映射，将 hostAddr 映射到 remoteAddr 。
	ns.AddHandler(
		CMD_MAPPING_HOST,
		func(p Pack, conn net.Conn) Pack {
			hostAddr := string(p.Body)
			remoteAddr := conn.RemoteAddr().String()
			ns.hostAddrs[hostAddr] = remoteAddr

			common.Log.Info("net server add host addr mapping - " + hostAddr + " to " + remoteAddr)
			return Pack{Command: CMD_MAPPING_HOST, Flag: true}
		},
	)

	// make a notify/trigger/pub
	//
	// 创建 通知、广播、事件。
	ns.AddHandler(

		CMD_DO_NOTIFY,

		func(p Pack, conn net.Conn) Pack {

			isOk := false

			// bytes join together
			arr := bytes.Split(p.Body, common.SP)
			len := len(arr)
			if len == 3 {
				// pub + command + body
				if "pub" == string(arr[0]) {
					ns.Pub(Pack{Command: string(arr[1]), Body: arr[2]})
					isOk = true
				}
			} else if len == 4 {
				// notify + client host(remote addr) + command + body
				if "notify" == string(arr[0]) {
					ns.Notify(string(arr[1]), Pack{Command: string(arr[2]), Body: arr[3]})
					isOk = true
				}
			} else if len == 5 {
				// tri + group + key + value + valueOld
				if "tri" == string(arr[0]) {
					ns.Tri(string(arr[1]), string(arr[2]), arr[3], arr[4])
					isOk = true
				}
			}

			return Pack{Command: CMD_DO_NOTIFY, Flag: isOk}
		},
	)
}

func (ns *NetServer) handleEach(pack Pack, conn net.Conn) Pack {

	// 遍历 handler 查找目标 Command ，找到后调用对应 Fn()
	for _, f := range ns.handlers {
		if f.Cmd == pack.Command {
			return f.Fn(pack, conn)
		}
	}

	common.Log.Warning("net server found no handler for", pack.Command)
	return PACK_NO_RETURN
}

func (ns *NetServer) handle(conn net.Conn) {

	// 长连接数目 +1
	ns.closeWg.Add(1)

	for {

		// 读取数据
		data := make([]byte, ReadLenOnce)
		length, e := conn.Read(data)
		if e != nil {
			if e == io.EOF {
				// client closed
				common.Log.Error("net server found conn closed - " + conn.RemoteAddr().String())
				ns.disconnect(conn)
			} else {
				common.Log.Error("net server handle conn read error - "+conn.RemoteAddr().String(), e)
				conn.Close()
				ns.disconnect(conn)
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

			// 数据解析
			var pack Pack
			e = common.Dec(one, &pack)


			// 出错处理
			if e != nil {
				packReturn := Pack{}
				packReturn.Flag = false
				packReturn.Msg = e.Error()
				r, _ := common.Enc(&packReturn)
				// add CL
				r = append(r, common.CL...)
				conn.Write(r)

			// 执行 pack.Command 命令
			} else {

				// 关闭 NetServer
				if CMD_CLOSE == pack.Command {
					ns.Close()

				// 关闭长连接 conn
				} else if CMD_QUIT == pack.Command {
					common.Log.Error("net server found conn closed - " + conn.RemoteAddr().String())
					ns.disconnect(conn)

				// 执行其它 Command
				} else {
					packReturn := ns.handleEach(pack, conn)
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

// 获取在线长连接的 RemoteAddrs
func (ns *NetServer) ListClients() []string {
	var r []string
	for _, c := range ns.cc {
		r = append(r, c.RemoteAddr().String())
	}
	return r
}

// 触发正在监听 group, key 上变更的 watcher
func (ns *NetServer) Tri(group, key string, value, valueOld []byte) []string {
	if "" == group {
		group = WatcherGroupAll
	}
	return ns.Trigger(Trigger{group, key, value, valueOld})
}

// need trigger different clients in balance TODO
func (ns *NetServer) Trigger(t Trigger) []string {

	var addrs []string

	// 每个 remoteAddr 可能关联多个 Watchers ，需要遍历每个 remoteAddr 关联的 Watchers ，
	// 如果其中某个 Watcher 和 t.group/t.key 相匹配，就需要回调通知这个 remoteAddr 。
	for remoteAddr, watchers := range ns.watcherKeys {

		for _, watcher := range watchers {

			// 匹配检查
			if t.group != watcher.group || t.key != watcher.key {
				continue
			}

			// ???
			if t.group != WatcherGroupAll {
				addrs = append(addrs, remoteAddr)
			} else {
				// only one
				if len(addrs) == 0 {
					addrs = append(addrs, remoteAddr)
				} else {
					break
				}
			}
		}
	}
	common.Log.Info("net server trigger - " + t.group + "," + t.key + " - " + strings.Join(addrs, ","))

	// 遍历所有需要通知的 addrs ，将 t 序列化后发送给它们。
	for _, removeAddr := range addrs {
		body := EncTri(&t)
		ns.Notify(removeAddr, Pack{Command: CMD_TRIGGER_WATCHER, Body: body, Flag: true})
	}

	return addrs
}

func (ns *NetServer) Notify(remoteAddr string, p Pack) {
	// 二进制序列化
	body, _ := common.Enc(&p)

	// 遍历所有长连接，如果找到匹配地址 remoteAddr 的 conn ，则将数据 p 发送给它。
	isExist := false
	for _, c := range ns.cc {
		connAddr := c.RemoteAddr().String()
		if connAddr == remoteAddr || connAddr == ns.hostAddrs[remoteAddr] {
			isExist = true
			c.Write(append(body, common.CL...)) // add CL
			break
		}
	}

	if !isExist {
		common.Log.Warning("remote addr not exists in clients", remoteAddr)
	}
}

func (ns *NetServer) Pub(p Pack) {
	// 二进制序列化
	body, _ := common.Enc(&p)
	// 遍历所有长连接，向每个长连接发送数据 p 。
	for _, c := range ns.cc {
		_, _ = c.Write(append(body, common.CL...))	// add CL
	}
}

func (ns *NetServer) checkAlivePub() {
	common.Log.Info("net server start check alive loop")

	// 创建定时器
	ticker := time.NewTicker(time.Duration(common.CheckAliveInterval) * time.Second)
	ns.tickerCheckAlive = ticker

	for {
		select {
		case <-ticker.C:
			common.Log.Info("net server check alive publishing")
			// 定时广播消息到每个长连接
			ns.Pub(Pack{Command: CMD_CHECK_ALIVE})
		}
	}
}

func (ns *NetServer) checkAliveLog() {
	common.Log.Info("net server start check alive reply loop")
	for {
		// 从管道中获取 AliveReply ，然后打印到日志中。
		str := <-ns.chAliveReply
		if ns.AliveCheckLogWriter != nil {
			ns.AliveCheckLogWriter.Write(str)
		} else {
			common.Log.Info("net server check alive - " + str)
		}
	}
}

func (ns *NetServer) GetClientHostByRemoteAddr(remoteAddr string) string {
	for key, val := range ns.hostAddrs {
		if val == remoteAddr {
			return key
		}
	}

	return remoteAddr
}
