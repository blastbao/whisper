package mediator

import (
	"bytes"
	"github.com/blastbao/whisper/common"
	"testing"
)

func TestBlockEncDec(t *testing.T) {
	local := "127.0.0.1"
	baseDir := "D:/tmp/test-whisper-dir"

	block := Block{BlockId: 1, DataId: 1, Addr: local,
		Dir: baseDir + "/node-store", Size: 64 * 1024 * 1024}
	b, _ := common.Enc(&block)

	block2 := Block{BlockId: 2, DataId: 1, Addr: local,
		Dir: baseDir + "/node-store", Size: 64 * 1024 * 1024}
	b2, _ := common.Enc(&block2)

	buf := &bytes.Buffer{}
	buf.Write(b)
	buf.Write(common.SP)
	buf.Write(b2)
	blockBytes := buf.Bytes()

	arr := bytes.Split(blockBytes, common.SP)
	for _, one := range arr {
		var b2 Block
		common.Dec(one, &b2)

		common.Log.Info("", b2)
	}
}
