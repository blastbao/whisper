package mediator

import (
	"bytes"
	"common"
)

const (
	CMD_CHECK_ALIVE      = "?"
	CMD_REPLY_ALIVE      = "!"
	CMD_NO_RETURN        = "0"
	CMD_CLOSE            = "-1"
	CMD_QUIT             = "100"
	CMD_ADD_WATCHER_DONE = "200"
	CMD_TRIGGER_WATCHER  = "201"
	CMD_REGISTER_WATCHER = "202"
	CMD_MAPPING_HOST     = "300"
	CMD_DO_NOTIFY        = "400"
)

type Pack struct {
	Command string
	Body    []byte
	Flag    bool
	Msg     string
}

var PACK_NO_RETURN Pack = Pack{Command: CMD_NO_RETURN}

type Trigger struct {
	group    string
	key      string
	value    []byte
	valueOld []byte
}

// []byte encoding fail when using msgpack or binary
var SP_TRI []byte = []byte{'|', '|'}

func EncTri(t *Trigger) []byte {
	b := bytes.Buffer{}
	b.Write([]byte(t.group))
	b.Write(SP_TRI)
	b.Write([]byte(t.key))
	b.Write(SP_TRI)
	b.Write(t.value)
	b.Write(SP_TRI)
	b.Write(t.valueOld)
	return b.Bytes()
}

func DecTri(b []byte, t *Trigger) {
	arr := bytes.Split(b, SP_TRI)
	if len(arr) != 4 {
		common.Log.Warning("decode trigger arr", arr)
		return
	}

	t.group = string(arr[0])
	t.key = string(arr[1])
	t.value = arr[2]
	t.valueOld = arr[3]
}

func BuildNotifyCmdBody4Pub(cmd string, body []byte) []byte {
	buf := &bytes.Buffer{}
	buf.WriteString("pub")
	buf.Write(common.SP)
	buf.WriteString(cmd)
	buf.Write(common.SP)
	buf.Write(body)
	return buf.Bytes()
}

func BuildNotifyCmdBody4Notify(remoteAddr, cmd string, body []byte) []byte {
	buf := &bytes.Buffer{}
	buf.WriteString("notify")
	buf.Write(common.SP)
	buf.WriteString(remoteAddr)
	buf.Write(common.SP)
	buf.WriteString(cmd)
	buf.Write(common.SP)
	buf.Write(body)
	return buf.Bytes()
}

func BuildNotifyCmdBody4Tri(group, key string, value, valueOld []byte) []byte {
	buf := &bytes.Buffer{}
	buf.WriteString("tri")
	buf.Write(common.SP)
	buf.WriteString(group)
	buf.Write(common.SP)
	buf.WriteString(key)
	buf.Write(common.SP)
	buf.Write(value)
	buf.Write(common.SP)
	buf.Write(valueOld)
	return buf.Bytes()
}
