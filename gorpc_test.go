package main

import (
	"github.com/blastbao/whisper/common"
	"github.com/valyala/gorpc"
	"io"
	"testing"
	"time"
)

type onConnectRwcWrapper struct {
	rwc io.ReadWriteCloser
}

func (w *onConnectRwcWrapper) Read(p []byte) (int, error) {
	n, err := w.rwc.Read(p)
	return n, err
}

func (w *onConnectRwcWrapper) Write(p []byte) (int, error) {
	return w.rwc.Write(p)
}

func (w *onConnectRwcWrapper) Close() error {
	conn = nil
	return w.rwc.Close()
}

func handler(clientAddr string, request interface{}) interface{} {
	str := request.(string)
	if "2" == str {
		//conn.Write([]byte("???"))
		common.Log.Info("from client", str)
	}

	return request
}

var conn *onConnectRwcWrapper

func TestGorpc(t *testing.T) {
	addr := "localhost:9777"
	s := gorpc.NewTCPServer(addr, handler)
	if e := s.Start(); e != nil {
		common.Log.Info("server started", addr)

		s.OnConnect = func(remoteAddr string, rwc io.ReadWriteCloser) (io.ReadWriteCloser, error) {
			conn := &onConnectRwcWrapper{rwc: rwc}
			return conn, nil
		}
	}
	defer s.Stop()

	c := gorpc.NewTCPClient(addr)
	c.Start()
	common.Log.Info("connected", addr)
	resp, _ := c.Call("2")
	common.Log.Info("resp", resp)
	defer c.Stop()

	time.Sleep(time.Duration(10) * time.Second)
}
