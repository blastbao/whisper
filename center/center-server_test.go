package center

import (
	"github.com/blastbao/whisper/common"
	"github.com/valyala/gorpc"
	"github.com/blastbao/whisper/mediator"
	"testing"
	"time"
)

var isRunClient = true
var isUsingSlave = true

func TestCenterServer(t *testing.T) {
	local := "localhost"
	baseDir := "D:/tmp/test-whisper-dir"
	mainCenterAddr := local + ":9769"
	slaveCenterAddr := local + ":9770"

	m := &mediator.Mediator{}
	m.Start(local, baseDir)
	if e := m.Load(); e != nil {
		common.Log.Error("mediator load block error", e)
		t.Fatal("mediator load failed", e)
	}
	defer m.Close()

	center := &Center{}
	e := center.Load(baseDir)
	if e != nil {
		t.Fatal("center load failed", e)
	}

	s := &CenterServer{}
	s.Center = center
	AddHandler2CenterServer(s)
	s.Start(local, mainCenterAddr)
	defer s.Close()

	s2 := &CenterServer{}
	s2.Center = center
	AddHandler2CenterServer(s2)
	s2.Start(local, local)
	defer s2.Close()

	m.Server.Notify(s.CenterHost, mediator.Pack{Command: CMD_MED_SET_MASTER, Body: []byte("true")})
	if isUsingSlave {
		m.Server.Notify(s.CenterHost, mediator.Pack{Command: CMD_MED_CONNECT_OTHER_CENTER, Body: []byte(slaveCenterAddr)})
	}

	time.Sleep(2 * time.Second)

	if isRunClient {
		c := gorpc.NewTCPClient(mainCenterAddr)
		c.Start()
		defer c.Stop()

		rec := NewBlockBeginRecord(1, 1)
		oid := rec.Oid

		resp, error := c.Call(PackRecord{Command: CMD_PUT_RECORD, Rec: rec})
		if error == nil {
			common.Log.Info("response for put", resp)
		} else {
			t.Fatal(error)
		}

		resp, error = c.Call(PackRecord{Command: CMD_GET_OID_META, Oid: oid})
		if error == nil {
			common.Log.Info("response for get", resp)
		} else {
			t.Fatal(error)
		}

		resp, error = c.Call(PackRecord{Command: CMD_CHANGE_OID_STATUS, Oid: oid,
			Status: common.STATUS_RECORD_DEL})
		if error == nil {
			common.Log.Info("response for update status", resp)
		} else {
			t.Fatal(error)
		}

		time.Sleep(2 * time.Second)
	}
}
