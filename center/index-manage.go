package center

import (
	"io"
	"sort"
)

func (index *Index) Filter(fn func(one Record) bool) (RecordList, error) {

	index.mutex.Lock()
	defer index.mutex.Unlock()

	en, err := index.IndexTree.SeekFirst()
	if err != nil {
		return nil, err
	}

	// 遍历索引树
	var list RecordList
	for {
		// 从索引中读取 kv
		k, v, e := en.Next()
		if e != nil {
			if e != io.EOF {
				return nil, err
			}
			break
		}

		oid := k.(string)
		rec := v.(Record)

		// 添加到 list 中
		if fn(rec) {
			rec.Oid = oid
			list = append(list, rec)
		}
	}

	// 按 BlockId、Offset 排序
	sort.Sort(list)
	return list, nil
}

// redundance is better? thought it will cost more memory
func (index *Index) GetRecordsByBlockId(blockId int) (list RecordList, err error) {
	// 获取索引 IndexTree 中所有 BlockId 等于 blockId 的 records 。
	return index.Filter(
		func(one Record) bool {
			return one.BlockId == blockId
		},
	)
}

func (index *Index) ChangeBlock(blockId, newBlockId int) error {

	if blockId == newBlockId {
		return nil
	}

	// 获取索引 IndexTree 中所有 BlockId 等于 blockId 的 records 。
	records, e := index.GetRecordsByBlockId(blockId)
	if e != nil {
		return e
	}

	// 逐个更新 blockId
	for _, record := range records {
		record.BlockId = newBlockId
	}

	// 逐个将 recs 更新到索引
	return index.SetBatch(records)
}
