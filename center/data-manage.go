package center

import (
	"io"
	"sort"
)

func (this *Data) Filter(fn func(one Record) bool) (list RecordList, err error) {
	this.mutex.Lock()
	defer this.mutex.Unlock()

	en, e := this.IndexTree.SeekFirst()
	if e != nil {
		err = e
		return
	}

	list = []Record{}
	for {
		k, v, e := en.Next()
		if e != nil {
			if e != io.EOF {
				err = e
				return
			}

			break
		}

		oid := k.(string)
		rec := v.(Record)

		if fn(rec) {
			rec.Oid = oid
			list = append(list, rec)
		}
	}

	sort.Sort(list)
	return
}

// redundance is better? thought it will cost more memory
func (this *Data) GetRecordsByBlockId(blockId int) (list RecordList, err error) {
	return this.Filter(func(one Record) bool {
		return one.BlockId == blockId
	})
}

func (this *Data) ChangeBlock(blockId, newBlockId int) error {
	if blockId == newBlockId {
		return nil
	}

	list, e := this.GetRecordsByBlockId(blockId)
	if e != nil {
		return e
	}

	for _, rec := range list {
		rec.BlockId = newBlockId
	}

	return this.SetBatch(list)
}
