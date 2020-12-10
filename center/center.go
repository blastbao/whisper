package center

import (
	"errors"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"sync"

	"github.com/blastbao/whisper/common"
)




// provide index read write service
type Center struct {
	// suppose 100, memory cost 100 * 200M(100w records each data) = 20G
	indexes []*Index
	mutex   *sync.Mutex
}

// 加载索引
func (c *Center) Load(dir string) error {
	common.Log.Info("center load index data from " + dir)

	c.indexes = []*Index{}
	reg := regexp.MustCompile("data_(\\d+)$")

	// 遍历目录 dir 下所有文件
	err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {

		// 如果是 data_xxx 目录
		if info.IsDir() && reg.MatchString(path) {

			// 从目录名中解析出 index_id
			arr := reg.FindStringSubmatch(path)
			idxId, _ := strconv.Atoi(arr[1])

			// 构造 Index 对象并初始化
			d := &Index{}
			e := d.Init(idxId, path)
			if e != nil {
				common.Log.Error("center load data init error", path, e)
				return e
			}

			// 加载数据
			e = d.Load()
			if e != nil {
				common.Log.Error("center load data load error", path, e)
				return e
			}

			// 将 Index 对象添加到数据列表 c.indexes 中
			c.indexes = append(c.indexes, d)
		}

		return err
	})
	if err != nil {
		return err
	}

	common.Log.Error("center load data ok")
	c.Dump()

	return nil
}

// 打印 c.indexes 的描述信息
func (c *Center) Dump() {
	common.Log.Info("center data list length", len(c.indexes))
	for _, d := range c.indexes {
		common.Log.Info("data info", d.Id, d.Len())
	}
}

// 获取 data_id => data_cnt
func (c *Center) RecordCounts() map[int]int {
	r := make(map[int]int)
	// 遍历所有 indexes ，取出所含数据条数
	for _, d := range c.indexes {
		r[d.Id] = d.Len()
	}
	return r
}

// 获取 Center 所含的索引中 BlockId 等于 blockId 的 records 。
func (c *Center) GetRecordsByBlockId(blockId int) (RecordList, error) {

	// 索引列表为空，报错返回
	if c.indexes == nil {
		return nil, errors.New("center data list is nil")
	}

	var res RecordList
	// 遍历每个索引对象
	for _, index := range c.indexes {
		// 获取索引中 BlockId 等于 blockId 的 records 。
		records, err := index.GetRecordsByBlockId(blockId)
		if err != nil {
			return nil, err
		}
		res = append(res, records...)
	}

	return res, nil
}

func (c *Center) Set(idxId int, rec Record) (err error) {
	// 根据 indexId 查询 index ，然后把 record 保存到 index 中。
	for _, d := range c.indexes {
		if d.Id == idxId {
			return d.Set(rec)
		}
	}

	return errors.New("center target index id not found" + strconv.Itoa(idxId))
}

func (c *Center) Get(idxId int, oid string) (rec Record, err error) {

	// 根据 indexId 查询 index ，然后从 index 中取出 oid 对应的 record 。
	for _, d := range c.indexes {
		if d.Id == idxId {
			return d.Get(oid)
		}
	}

	err = errors.New("center target index id not found" + strconv.Itoa(idxId))
	return
}


// 创建新的 Index 对象，指定保存到 dir 目录中。
func (c *Center) NewIndex(dir string) (int, error) {

	c.mutex.Lock()
	defer c.mutex.Unlock()

	// 获取最大 index id
	maxIdxId := 0
	for _, d := range c.indexes {
		if d.Id > maxIdxId {
			maxIdxId = d.Id
		}
	}

	maxIdxId++

	// 创建新的 Index 对象
	d := &Index{}
	if err := d.Init(maxIdxId, dir); err != nil {
		return 0, err
	}
	return maxIdxId, nil
}

// 持久化
func (c *Center) Persist() error {
	for _, d := range c.indexes {
		if err := d.Persist(); err != nil {
			return err
		}
	}

	return nil
}
