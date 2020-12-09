package center

import (
	"bytes"
	"github.com/blastbao/whisper/common"
	"math/rand"
	"strconv"
	"strings"
	"time"
)

type OidInfo struct {
	IndexId int
	CopyNum int
	Seq     int // copies' sequence
}

// record one file's position
type Record struct {
	// concat like dataId_copyNum_randInt6len_randInt6len_seq
	Oid     string
	BlockId int
	Md5     []byte
	Offset  int		//
	Len     int
	Mime    int
	Created int64
	Expired int64
	Status  int
}

var r = rand.New(rand.NewSource(time.Now().UnixNano()))

// indexId_copyNum_RandInt_RandInt_0
func GenOid(indexId, copyNum int) string {
	return GenOidNoSuffix(indexId, copyNum) + "_0"
}

// indexId_copyNum_RandInt_RandInt
func GenOidNoSuffix(indexId, copyNum int) string {
	return strconv.Itoa(indexId) + "_" +
		   strconv.Itoa(copyNum) + "_" +
		   strconv.Itoa(r.Intn(100000)) + "_" +
		   strconv.Itoa(r.Intn(100000))
}


//
func GetOidInfo(oid string) OidInfo {
	arr := strings.Split(oid, "_")

	info := OidInfo{}
	// TODO
	if len(arr) != 5 {
		common.Log.Warning("invalid oid", oid)
		return info
	}

	info.IndexId, _ = strconv.Atoi(arr[0])
	info.CopyNum, _ = strconv.Atoi(arr[1])
	info.Seq, _ = strconv.Atoi(arr[4])

	return info
}

func GetOidSiblings(oid string) []string {
	cc := 0
	len := len(oid)

	copyNum := 0

	buf := &bytes.Buffer{}
	for i := 0; i < len; i++ {
		ch := string(oid[i])
		buf.WriteString(ch)
		if ch == "_" {
			cc++
			if cc == 4 {
				break
			}
		} else {
			if cc == 1 {
				copyNum, _ = strconv.Atoi(ch)
			}
		}
	}

	if copyNum == 0 {
		common.Log.Warning("invalid oid", oid)
		copyNum = 1
	}

	arr := make([]string, copyNum+1)
	prefix := buf.String()
	for i := 0; i <= copyNum; i++ {
		arr[i] = prefix + strconv.Itoa(i)
	}

	return arr
}

func NewBlockBeginRecord(dataId, blockId int) Record {
	return Record{Oid: GenOid(dataId, 0), Offset: 0, Len: 10, Status: common.STATUS_RECORD_BLOCK_BEGIN}
}

func GetIndexFrom(body []byte, rec *Record) (err error) {
	return common.Dec(body, rec)
}

func ConvIndexTo(rec Record) (body []byte, err error) {
	return common.Enc(&rec)
}

type RecordList []Record

// implements sort interface
func (rs RecordList) Len() int {
	return len(rs)
}

// 按照 BlockId、Offset 排序
func (rs RecordList) Less(i, j int) bool {
	b1 := rs[i]
	b2 := rs[j]

	if b1.BlockId != b2.BlockId {
		return b1.BlockId < b2.BlockId
	}

	return b1.Offset < b2.Offset
}

func (rs RecordList) Swap(i, j int) {
	temp := rs[i]
	rs[i] = rs[j]
	rs[j] = temp
}

func (rs RecordList) FilterByStatus(status int) RecordList {
	var res RecordList
	for _, rec := range rs {
		if rec.Status == status {
			res = append(rs, rec)
		}
	}
	return res
}
