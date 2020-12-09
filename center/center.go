package center

import (
	"github.com/blastbao/whisper/common"
	"errors"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"sync"
)

// provide index read write service
type Center struct {
	// suppose 100, memory cost 100 * 200M(100w records each data) = 20G
	dataList []*Data
	mutex    *sync.Mutex
}

func (this *Center) Load(dir string) (err error) {
	common.Log.Info("center load index data from " + dir)

	this.dataList = []*Data{}
	reg := regexp.MustCompile("data_(\\d+)$")
	e := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if info.IsDir() && reg.MatchString(path) {
			arr := reg.FindStringSubmatch(path)
			dataId, _ := strconv.Atoi(arr[1])

			d := &Data{}
			e := d.Init(dataId, path)
			if e != nil {
				common.Log.Error("center load data init error", path, e)
				return e
			}

			e = d.Load()
			if e != nil {
				common.Log.Error("center load data load error", path, e)
				return e
			}
			this.dataList = append(this.dataList, d)
		}
		return err
	})
	if e != nil {
		err = e
		return
	}

	common.Log.Error("center load data ok")
	this.Dump()

	return nil
}

func (this *Center) Dump() {
	common.Log.Info("center data list length", len(this.dataList))
	for _, d := range this.dataList {
		common.Log.Info("data info", d.Id, d.Len())
	}
}

func (this *Center) DataInfo() map[int]int {
	r := make(map[int]int)
	for _, d := range this.dataList {
		r[d.Id] = d.Len()
	}
	return r
}

func (this *Center) GetRecordsByBlockId(blockId int) (r RecordList, err error) {
	if this.dataList == nil {
		err = errors.New("center data list is nil")
		return
	}

	r = []Record{}
	for _, d := range this.dataList {
		list, e := d.GetRecordsByBlockId(blockId)
		if e != nil {
			err = e
			return
		}

		r = append(r, list...)
	}

	return
}

func (this *Center) Set(dataId int, rec Record) (err error) {
	for _, d := range this.dataList {
		if d.Id == dataId {
			return d.Set(rec)
			break
		}
	}

	return errors.New("center target data id not found" + strconv.Itoa(dataId))
}

func (this *Center) Get(dataId int, oid string) (rec Record, err error) {
	for _, d := range this.dataList {
		if d.Id == dataId {
			return d.Get(oid)
			break
		}
	}

	err = errors.New("center target data id not found" + strconv.Itoa(dataId))
	return
}

func (this *Center) NewData(dir string) (dataId int, err error) {
	this.mutex.Lock()
	defer this.mutex.Unlock()

	dataId = 0
	for _, d := range this.dataList {
		if d.Id > dataId {
			dataId = d.Id
		}
	}
	dataId++

	d := &Data{}
	err = d.Init(dataId, dir)

	return
}

func (this *Center) Persist() (err error) {
	for _, d := range this.dataList {
		err = d.Persist()
		if err != nil {
			return
		}
	}

	return nil
}
