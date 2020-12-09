package agent

import (
	"container/list"
	"errors"
	"os"
	"strconv"
	"sync"
	"time"


	"github.com/blastbao/whisper/mediator"
	"github.com/blastbao/whisper/center"
	"github.com/blastbao/whisper/common"
)

const (
	BLOCK_FILE_NAME_PRE      = "block_"
	GET_BLOCK_MAX_LOOP_TIMES = 10 // if get write lock fail, sleep 1ms and loop next
	GET_BLOCK_LOCK_PAUSE     = 1e6
)

type BlockInServer struct {
	mediator.Block
	mutex     *sync.Mutex // write lock
	isWriting bool
}

func (this *BlockInServer) GetFilePath() string {
	return this.Dir + "/" + BLOCK_FILE_NAME_PRE + strconv.Itoa(this.BlockId)
}

// store/read files from disk
type Node struct {
	// block list, may be more than 10000 in a host, suppose 1 host has 10 disks, one disk is 4T
	// 10 * 4 * 1024 * 1024 / 64 ~= 655360, change data structure if necessary
	Blocks *list.List
	Status string
}

// get one block to store, dir(mounted disk) should be different 4 copies
func (this *Node) getFitBlock(oid string, len int, beginLoop int) *BlockInServer {
	if beginLoop > GET_BLOCK_MAX_LOOP_TIMES {
		return nil
	}
	needLoopAgain := false
	for e := this.Blocks.Front(); e != nil; e = e.Next() {
		block := e.Value.(*BlockInServer)
		left := block.Size - block.End
		if left < len {
			continue
		}

		if !block.isWriting {
			return block
		} else {
			// if this block is writing data, sleep and loop again
			needLoopAgain = true
		}
	}

	if needLoopAgain {
		common.Log.Info("node find block loop again", beginLoop+1)
		time.Sleep(GET_BLOCK_LOCK_PAUSE)
		return this.getFitBlock(oid, len, beginLoop+1)
	}
	return nil
}

func (this *Node) getBlock(blockId int) (b *BlockInServer, err error) {
	for e := this.Blocks.Front(); e != nil; e = e.Next() {
		block := e.Value.(*BlockInServer)
		if block.BlockId == blockId {
			return block, nil
		}
	}
	return nil, errors.New("node error as block not found")
}

func (this *Node) Get(rec center.Record) (b []byte, err error) {
	block, error := this.getBlock(rec.BlockId)
	if error != nil {
		err = error
		return
	}

	fn := block.GetFilePath()
	_, error = os.Stat(fn)
	isExists := error == nil || os.IsExist(error)

	if !isExists {
		err = errors.New("node get error as block file not found")
		return
	}

	file, error := os.OpenFile(fn, os.O_RDONLY, 0666)
	if error != nil {
		err = error
		return
	}
	defer file.Close()

	// read fully one time?
	bb := make([]byte, rec.Len)
	_, error = file.ReadAt(bb, int64(rec.Offset))
	if error != nil {
		err = error
		return
	}

	return bb, nil
}

func (this *Node) SaveLocal(oid string, b []byte) (rec center.Record, err error) {
	var len int = len(b)
	block := this.getFitBlock(oid, len, 0)
	if block == nil {
		err = errors.New("node save error as no block space left")
		return
	}

	block.isWriting = true
	block.mutex.Lock()
	defer func() {
		block.mutex.Unlock()
		block.isWriting = false
	}()

	fn := block.GetFilePath()
	_, error := os.Stat(fn)
	isExists := error == nil || os.IsExist(error)

	if !isExists {
		file, error := os.Create(fn)
		if error != nil {
			err = error
			return
		}
		_, error = file.WriteAt(b, 0)
		if error != nil {
			err = error
			return
		}
		defer file.Close()
	} else {
		file, error := os.OpenFile(fn, os.O_WRONLY, 0666)
		if error != nil {
			err = error
			return
		}
		_, error = file.WriteAt(b, int64(block.End))
		if error != nil {
			err = error
			return
		}
		defer file.Close()
	}

	rec = center.Record{BlockId: block.BlockId, Md5: common.GenMd5(b), Offset: block.End, Len: len}
	// change end position
	block.End += len

	return
}

// read full 4 copy, need lock first
func (this *Node) LockBlock(blockId int) {
	block, e := this.getBlock(blockId)
	if e != nil {
		common.Log.Error("node lock block error as block not found - " + strconv.Itoa(blockId))
		return
	}

	block.isWriting = true
	block.mutex.Lock()
}

func (this *Node) UnlockBlock(blockId int) {
	block, e := this.getBlock(blockId)
	if e != nil {
		common.Log.Error("node lock block error as block not found - " + strconv.Itoa(blockId))
		return
	}

	block.mutex.Unlock()
	block.isWriting = false
}

func (this *Node) ReadFull(blockId int) (b []byte, err error) {
	block, e := this.getBlock(blockId)
	if e != nil {
		err = e
		return
	}

	fn := block.GetFilePath()
	fi, e := os.Stat(fn)
	isExists := e == nil || os.IsExist(e)
	if !isExists {
		err = errors.New("node read full error as block file not found")
		return
	}

	file, e := os.OpenFile(fn, os.O_RDONLY, 0666)
	if e != nil {
		err = e
		return
	}
	defer file.Close()

	// read fully one time?
	bb := make([]byte, fi.Size())
	_, e = file.Read(bb)
	if e != nil {
		err = e
		return
	}

	return bb, nil
}
