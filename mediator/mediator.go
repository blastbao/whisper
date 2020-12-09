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




// 1 host 10 * 4T / 64M(block size) ~= 625000 blocks
// 10 hosts ~= 6,500,000 blocks, this is BlockTree length

type Mediator struct {
	Dir       string
	BlockTree *b.Tree
	Server    *NetServer
	mutex     *sync.Mutex
}

func (m *Mediator) Start(host, dir string) {

	m.Dir = dir
	m.BlockTree = b.TreeNew(common.CmpInt)
	m.mutex = new(sync.Mutex)
	m.Server = &NetServer{}

	if e := m.Server.Start(host, common.SERVER_PORT_MEDIATOR); e != nil {
		common.Log.Error("mediator server start error", e)
	} else {
		common.Log.Info("mediator server start success")
	}
}

func (m *Mediator) Close() {
	if m.Server != nil {
		m.Server.Close()
	}
}

func (m *Mediator) getPersistFile() string {
	return m.Dir + "/mediator.data"
}

// write to file
func (m *Mediator) Persist() (err error) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if m.BlockTree.Len() == 0 {
		fn := m.getPersistFile()
		return os.Remove(fn)
	}

	en, e := m.BlockTree.SeekFirst()
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
		e = common.Write2File(bb, m.getPersistFile(), os.O_WRONLY)
		if e != nil {
			err = e
			return
		}
	}

	return nil
}

func (m *Mediator) Load() (err error) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	fn := m.getPersistFile()
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

		m.BlockTree.Set(block.BlockId, block)
	}

	common.Log.Info("mediator loaded block number", m.BlockTree.Len())
	return nil
}

func (m *Mediator) NewBlock(dataId int, addr string, dir string, size int) (blockId int, err error) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	blockIdMax := 0
	if m.BlockTree.Len() != 0 {
		en, e := m.BlockTree.SeekFirst()
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
	m.BlockTree.Set(newBlockId, block)

	return newBlockId, nil
}
