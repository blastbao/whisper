package center

import (
	"github.com/blastbao/whisper/common"
	"strconv"
	"testing"
)

var doSet bool = false
var doPersist bool = false
var doLoad bool = true

// tree length
// can be 100w
var times int = 100

// if write log switch on
// 100w persist cost 15s if save to 1 file
// 100w persist cost 15s if save to 10 files using goroutine, not faster because iterate tree in the same way

// if write log switch off
// 100w persist cost 3s if save to 10 files, load cost 8s

func TestDataSet(t *testing.T) {
	d := &Index{}
	d.Init(2, "D:/tmp/test-whisper-dir/data_2")

	if doSet {
		setTag, setStartTime := common.Trace("set cost")
		for i := 0; i < times; i++ {
			rec := Record{}
			rec.Oid = "oid_" + strconv.Itoa(i)
			rec.BlockId = 1
			rec.Len = 100
			rec.Offset = 100
			rec.Mime = common.MIME_JPG
			rec.Expired = 1234
			rec.Md5 = []byte("1234")
			rec.Created = 1234

			if error := d.SetWithLog(rec, false); error != nil {
				t.Fatal(error)
			}
		}
		common.End(setTag, setStartTime)
	}

	if doPersist {
		persistTag, persistStartTime := common.Trace("persist cost")
		if error := d.Persist(); error != nil {
			t.Fatal(error)
		}
		common.End(persistTag, persistStartTime)
	}
}

func TestDataLoad(t *testing.T) {
	d := &Index{}
	d.Init(2, "D:/tmp/test-whisper-dir/data_2")

	if doLoad {
		// 100w load cost 77s and 1.3G using one goroutine
		defer common.End(common.Trace("load cost"))
		if error := d.Load(); error != nil {
			t.Fatal(error)
		} else {
			common.Log.Info("loaded length", d.IndexTree.Len())
			rec, e := d.Get("oid_1")
			common.Log.Info("loaded one record info", rec, e)
		}
	}
}
