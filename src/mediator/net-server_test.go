package mediator

import (
	"common"
	"net"
	"testing"
	"time"
)

func TestNetServer(t *testing.T) {
	s := &NetServer{}
	s.AliveCheckLogWriter = common.NewBufferWriter("D:/tmp/net-server-log.txt", 40)
	if e := s.Start("localhost", 9778); e != nil {
		t.Fatal(e)
	}
	defer s.AliveCheckLogWriter.Flush()
	defer s.Close()

	s.AddHandler("aaa", func(p Pack, conn net.Conn) Pack {
		common.Log.Info("u say aaa?")
		return PACK_NO_RETURN
	})
	s.AddHandler("bbb", func(p Pack, conn net.Conn) Pack {
		common.Log.Info("u say bbb?")
		return PACK_NO_RETURN
	})

	s.AddWatchCallback(WatcherGroupAll, "dog-changed", func() (v, v2 []byte) {
		return []byte("init2"), []byte("init1")
	})

	c := &NetClient{}
	defer c.Close()

	go func() {
		time.Sleep(time.Duration(2) * time.Second)

		if e := c.Start("localhost:9778"); e != nil {
			common.Log.Error("client start error", e)
			t.Fatal(e)
		}
		c.AddHandler("zzz", func(p Pack) Pack {
			common.Log.Info("..." + p.Command)
			return PACK_NO_RETURN
		})
		c.AddHandler("yyy", func(p Pack) Pack {
			common.Log.Info("..." + p.Command)
			return PACK_NO_RETURN
		})
		c.Send(Pack{Command: CMD_MAPPING_HOST, Body: []byte("local-client")})

		c.Watch("dog-changed", func(value, valueOld []byte) {
			common.Log.Info("warning !! dog-changed",
				string(value), string(valueOld))
		})

		e := c.Send(Pack{Command: "aaa"})
		if e != nil {
			t.Fatal(e)
		}

		time.Sleep(time.Duration(10) * time.Second)
		e = c.Send(Pack{Command: CMD_QUIT})
		if e != nil {
			t.Fatal(e)
		}
	}()

	time.Sleep(time.Duration(5) * time.Second)
	s.Notify("local-client", Pack{Command: "yyy"})
	s.Tri("", "dog-changed", []byte("111"), []byte("222"))

	p := Pack{Command: "zzz"}
	p.Body = []byte("this is a detail content whatever")
	s.Pub(p)

	time.Sleep(time.Duration(20) * time.Second)
}
