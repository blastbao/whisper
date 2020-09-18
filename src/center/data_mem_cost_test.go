package center

import (
	"common"
	"fmt"
	"io/ioutil"
	"strconv"
	"testing"
	"time"
)

var writeLog bool = false

func makeData(times int, md5 []byte, ch chan<- *Data) (err error) {
	d := &Data{}
	d.Init(1, "D:/tmp/test-whisper-dir/test-mem-cost-data")

	setTag, setStartTime := common.Trace("set cost")
	for i := 0; i < times; i++ {
		rec := Record{}
		rec.Oid = "oid_" + strconv.Itoa(i)
		rec.BlockId = 1
		rec.Len = 1024 * 66
		rec.Offset = i * rec.Len
		rec.Mime = common.MIME_JPG
		rec.Expired = time.Now().Unix() + 3600
		rec.Md5 = md5
		rec.Created = time.Now().Unix()

		if error := d.SetWithLog(rec, writeLog); error != nil {
			common.Log.Error("set record error", error)
			err = error
			return
		}
	}
	common.End(setTag, setStartTime)

	ch <- d
	return
}

func TestDataCostMem(t *testing.T) {
	// test image file
	filePath := "D:/tmp/test.jpg"
	body, error := ioutil.ReadFile(filePath)
	if error != nil {
		common.Log.Error("read image file failed", error)
		return
	}
	common.Log.Info("image file size", len(body))

	md5 := common.GenMd5(body)

	// one goroutine，write log is too slow
	// 1w cost 6M mem / 10 seconds
	// 20w cost 76M mem / 110 seconds

	// not write log
	// 100 * 1w cost 211M mem
	dataNum := 10
	eachTimes := 10000

	chs := make([]chan *Data, dataNum)
	for i := 0; i < dataNum; i++ {
		chs[i] = make(chan *Data, 1)

		go makeData(eachTimes, md5, chs[i])
	}

	for _, ch := range chs {
		d := <-ch
		fmt.Println("tree length", d.Len())
	}

	time.Sleep(time.Duration(10) * time.Second)
}
