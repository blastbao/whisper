package mediator

import (
	"bytes"
	"github.com/blastbao/whisper/common"
	"github.com/cznic/b"
	"io"
	"io/ioutil"
	"os"
	"sync"
)

type Mediator struct {
	Dir       string
	BlockTree *b.Tree
	Server    *NetServer
	mutex     *sync.Mutex
	// 1host 10 * 4T / 64M(block size) ~= 625000 blocks
	// 10 hosts ~= 6,500,000 blocks, this is BlockTree length
}

func (this *Mediator) Start(host, dir string) {
	this.Dir = dir
	this.BlockTree = b.TreeNew(common.CmpInt)
	this.mutex = new(sync.Mutex)

	this.Server = &NetServer{}
	if e := this.Server.Start(host, common.SERVER_PORT_MEDIATOR); e != nil {
		common.Log.Error("mediator server start error", e)
	} else {

	}
}

func (this *Mediator) Close() {
	if this.Server != nil {
		this.Server.Close()
	}
}

func (this *Mediator) getPersistFile() string {
	return this.Dir + "/mediator.data"
}

// write to file
func (this *Mediator) Persist() (err error) {
	this.mutex.Lock()
	defer this.mutex.Unlock()

	if this.BlockTree.Len() == 0 {
		fn := this.getPersistFile()
		return os.Remove(fn)
	}

	en, e := this.BlockTree.SeekFirst()
	if e != nil {
		err = e
		return
	}

	buf := &bytes.Buffer{}

	for {
		_, v, e := en.Next()
		if e != nil {
			if e != io.EOF {
				err = e
				return
			}

			break
		}

		block := v.(Block)
		bb, e := common.Enc(&block)
		if e != nil {
			err = e
			return
		}

		buf.Write(bb)
		buf.Write(common.SP)
	}

	bb := buf.Bytes()
	if len(bb) > 0 {
		e = common.Write2File(bb, this.getPersistFile(), os.O_WRONLY)
		if e != nil {
			err = e
			return
		}
	}

	return nil
}

func (this *Mediator) Load() (err error) {
	this.mutex.Lock()
	defer this.mutex.Unlock()

	fn := this.getPersistFile()
	bb, e := ioutil.ReadFile(fn)
	if e != nil {
		common.Log.Error("mediator load error but skip", fn, e)
		return nil
	}

	for _, b := range bytes.Split(bb, common.SP) {
		if len(b) == 0 {
			continue
		}

		var block Block
		common.Dec(b, &block)

		this.BlockTree.Set(block.BlockId, block)
	}

	common.Log.Info("mediator loaded block number", this.BlockTree.Len())
	return nil
}

func (this *Mediator) NewBlock(dataId int, addr string, dir string, size int) (blockId int, err error) {
	this.mutex.Lock()
	defer this.mutex.Unlock()

	blockIdMax := 0
	if this.BlockTree.Len() != 0 {
		en, e := this.BlockTree.SeekFirst()
		if e != nil {
			err = e
			return
		}

		for {
			k, _, e := en.Next()
			if e != nil {
				if e != io.EOF {
					err = e
					return
				}

				break
			}

			blockId := k.(int)
			if blockId > blockIdMax {
				blockIdMax = blockId
			}
		}
	}

	newBlockId := blockIdMax + 1

	block := Block{BlockId: newBlockId, DataId: dataId, Addr: addr, Dir: dir, Size: size}
	this.BlockTree.Set(newBlockId, block)

	return newBlockId, nil
}
