package mediator

import (
	"github.com/blastbao/whisper/common"
	"testing"
)

func TestMediatorNewWBlock(t *testing.T) {



	baseDir := "D:/tmp/test-whisper-dir"

	m := &Mediator{}
	m.Start(common.LOCALHOST, baseDir)
	if e := m.Load(); e != nil {
		common.Log.Error("mediator load block error", e)
		t.Fatal(e)
	}

	// 创建 Block 并添加到 m.BlockTree 索引
	blockId, e := m.NewBlock(1, "localhost", baseDir+"/data-100", 64*1024*1024)
	if e != nil {
		common.Log.Error("mediator new block error", e)
		t.Fatal(e)
	} else {
		common.Log.Info("mediator new block id", blockId)
	}

	// 持久化
	e = m.Persist()
	if e != nil {
		common.Log.Error("mediator persist error", e)
		t.Fatal(e)
	} else {
		common.Log.Info("mediator persist ok")
	}

}
