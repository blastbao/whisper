package client

import (
	"agent"
	"bytes"
	"center"
	"common"
	"io/ioutil"
	"mediator"
	"strconv"
	"testing"
	"time"
)

var startClient bool = true

func TestClientNodeServerConnect(t *testing.T) {
	local := "localhost"
	baseDir := "D:/tmp/test-whisper-dir"

	block := mediator.Block{BlockId: 1, DataId: 1, Addr: local,
		Dir: baseDir + "/node-store", Size: 64 * 1024 * 1024}
	b, _ := common.Enc(&block)

	block2 := mediator.Block{BlockId: 2, DataId: 1, Addr: local,
		Dir: baseDir + "/node-store", Size: 64 * 1024 * 1024}
	b2, _ := common.Enc(&block2)

	buf := &bytes.Buffer{}
	buf.Write(b)
	buf.Write(common.SP)
	buf.Write(b2)
	blockBytes := buf.Bytes()

	m := &mediator.Mediator{}
	m.Start(local, baseDir)
	if e := m.Load(); e != nil {
		common.Log.Error("mediator load block error", e)
		t.Fatal("mediator load failed", e)
	}

	// center server start
	centerData := &center.Center{}
	if e := centerData.Load(baseDir); e != nil {
		t.Fatal("center load failed", e)
	}

	cs := &center.CenterServer{}
	cs.Center = centerData
	cs.Start(local, local)

	// node server start
	nodeServer := &agent.NodeServer{}
	nodeServer.Start(local, local)

	// client
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

	go func() {
		time.Sleep(5 * time.Second)

		centerServerRemoteAddr := local + ":" + strconv.Itoa(common.SERVER_PORT_CENTER)

		blankBytes := []byte{}
		m.Server.Tri("", "node-server-connect-to-center", []byte(centerServerRemoteAddr), blankBytes)
		m.Server.Tri("", "node-server-block-refresh", blockBytes, blankBytes)

		if startClient {
			nodeServerHosts := local + ":" + strconv.Itoa(common.SERVER_PORT_AGENT)

			m.Server.Tri("", "client-block-refresh", blockBytes, blankBytes)
			m.Server.Tri("", "client-connect-to-center", []byte(centerServerRemoteAddr), blankBytes)
			m.Server.Tri("", "client-connect-to-node-server", []byte(nodeServerHosts), blankBytes)
		}
	}()

	if startClient {
		go func() {
			time.Sleep(10 * time.Second)
			// test image file
			filePath := "D:/tmp/test.jpg"
			body, error := ioutil.ReadFile(filePath)
			if error != nil {
				common.Log.Info("test client read image file failed", error)
				return
			}
			common.Log.Info("test client image file size", len(body))

			oid, error := cc.Save(body, common.MIME_JPG)
			common.Log.Info("test client save file result", oid, error)

			if error == nil {
				bb, mm, error := cc.Get(oid)
				common.Log.Info("test client get file result", len(bb), mm, error)
			}
		}()
	}

	time.Sleep(15 * time.Second)
}
