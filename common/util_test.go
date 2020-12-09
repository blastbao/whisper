package common

import (
	"bytes"
	"testing"
	"time"
)

func TestGenMd5(t *testing.T) {
	b := make([]byte, 100)
	for i := 0; i < 100; i++ {
		b[i] = byte(i)
	}

	md5 := GenMd5(b)

	if !CheckMd5(b, md5) {
		t.Fatal("gen md5 error")
	}

	b[0] = '1'
	if CheckMd5(b, md5) {
		t.Fatal("gen md5 error")
	}
}

func TestCmpInt(t *testing.T) {
	Log.Info("", CmpInt(1, 2))
	Log.Info("", CmpInt(2, 2))
	Log.Info("", CmpInt(2, 3))
}

func TestCmpInt64(t *testing.T) {
	Log.Info("", CmpInt64(int64(1), int64(2)))
}

func TestCmpStr(t *testing.T) {
	Log.Info("", CmpStr("abc", "abd"))
	Log.Info("", CmpStr("bcd", "azz"))
}

type Pack struct {
	Command string
	Body    []byte
	Flag    bool
	Msg     string
}

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
		Log.Info("decode trigger arr", arr)
		return
	}

	t.group = string(arr[0])
	t.key = string(arr[1])
	t.value = arr[2]
	t.valueOld = arr[3]
}

func TestEncDec(t *testing.T) {
	body, e := Enc(&Pack{Command: "xxx"})
	Log.Info("encoding", body, e)

	var p Pack
	e = Dec(body, &p)
	Log.Info("decoding", p, e)

	bb, e := Enc([]byte{'1', '2'})
	Log.Info("encoding bytes", bb)

	body2 := EncTri(&Trigger{"", "xx", []byte("aaa"), []byte("bbb")})
	Log.Info("encoding", body2)

	var tt Trigger
	DecTri(body2, &tt)
	Log.Info("decoding", tt)
}

func TestCompressDepress(t *testing.T) {
	data := []byte("a long time story and content is unknown, a long time story and content is unknown")

	Log.Info("before compress, the content len is", len(data))

	body, e := Compress(data)
	if e != nil {
		t.Fatal(e)
	} else {
		Log.Info("after compress, the content len is", len(body))
	}

	if raw, e := Depress(body); e != nil {
		Log.Error("depress error", e)
		t.Fatal(e)
	} else {
		Log.Info("depress recover", string(raw))
	}
}

func TestTrace(t *testing.T) {
	defer End(Trace("test cost"))

	time.Sleep(1e9 * 2)
}
