package client

import (
	"github.com/blastbao/whisper/agent"
	"bytes"
	"github.com/blastbao/whisper/center"
	"github.com/blastbao/whisper/common"
	"io/ioutil"
	"github.com/blastbao/whisper/mediator"
	"strconv"
	"testing"
	"time"
)

var startClient bool = true

func TestClientNodeServerConnect(t *testing.T) {

	local := "localhost"
	baseDir := "D:/tmp/test-whisper-dir"

	block := mediator.Block{
		BlockId: 1,
		DataId: 1,
		Addr: local, // 本 block 存储在 localhost 主机上
		Dir: baseDir + "/node-store",
		Size: 64 * 1024 * 1024,
	}
	b, _ := common.Enc(&block)

	block2 := mediator.Block{
		BlockId: 2,
		DataId: 1,
		Addr: local, // 本 block 存储在 localhost 主机上
		Dir: baseDir + "/node-store",
		Size: 64 * 1024 * 1024,
	}
	b2, _ := common.Enc(&block2)

	buf := &bytes.Buffer{}
	buf.Write(b)
	buf.Write(common.SP)
	buf.Write(b2)
	blockBytes := buf.Bytes()


	// 启动 Mediator ，并从 baseDir 加载 Block 索引。
	m := &mediator.Mediator{}
	m.Start(local, baseDir)
	if e := m.Load(); e != nil {
		common.Log.Error("mediator load block error", e)
		t.Fatal("mediator load failed", e)
	}

	// 创建 Center ，并从 baseDir 加载 Record 索引。
	centerData := &center.Center{}
	if e := centerData.Load(baseDir); e != nil {
		t.Fatal("center load failed", e)
	}

	// 启动 CenterServer
	cs := &center.CenterServer{}
	cs.Center = centerData
	cs.Start(local, local)

	// 启动 NodeServer
	nodeServer := &agent.NodeServer{}
	nodeServer.Start(local, local)


	// 创建 client ，使其同 mediator server 建立连接并注册了一组 Watchers 。
	var cc *Client
	if startClient {
		cc = &Client{}
		cc.Start(local)
	}


	defer func() {
		if cc != nil {
			cc.Close()
		}
		nodeServer.Close()
		cs.Close()
		m.Close()
	}()

	//
	go func() {
		time.Sleep(5 * time.Second)

		centerServerRemoteAddr := local + ":" + strconv.Itoa(common.SERVER_PORT_CENTER)

		// 通过 Mediator 通知所有 NodeSvr 连接到 CenterSvr ，其地址存储在 blockBytes 中。
		m.Server.Tri("", "node-server-connect-to-center", []byte(centerServerRemoteAddr), nil)
		// 通过 Mediator 通知所有 NodeSvr 刷新块列表，块列表存储在 blockBytes 中。
		m.Server.Tri("", "node-server-block-refresh", blockBytes, nil)

		if startClient {
			nodeServerHosts := local + ":" + strconv.Itoa(common.SERVER_PORT_AGENT)
			// 通过 Mediator 通知 client 刷新块列表
			m.Server.Tri("", "client-block-refresh", blockBytes, []byte{})
			// 通过 Mediator 通知 client 连接到 CenterSvr
			m.Server.Tri("", "client-connect-to-center", []byte(centerServerRemoteAddr), nil)
			// 通过 Mediator 通知 client 连接到 nodeServers
			m.Server.Tri("", "client-connect-to-node-server", []byte(nodeServerHosts), nil)
		}
	}()


	if startClient {

		go func() {
			time.Sleep(10 * time.Second)

			// 读取本地数据
			filePath := "D:/tmp/test.jpg"
			body, error := ioutil.ReadFile(filePath)
			if error != nil {
				common.Log.Info("test client read image file failed", error)
				return
			}
			common.Log.Info("test client image file size", len(body))

			// 上传数据到 Block 中
			oid, error := cc.Save(body, common.MIME_JPG)
			common.Log.Info("test client save file result", oid, error)
			if error == nil {
				// 上传成功，读取数据信息
				bb, mm, error := cc.Get(oid)
				common.Log.Info("test client get file result", len(bb), mm, error)
			}
		}()
	}

	time.Sleep(15 * time.Second)
}
