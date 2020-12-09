package center

import (
	"bytes"
	"github.com/blastbao/whisper/common"
	"errors"
	"github.com/cznic/b"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	MAX_TREE_LEN                       = 100 * 10000 // max btree length
	PERSIST_EACH_FILE_RECORD_NUM_LIMIT = 10000 * 10  // persist use more than one file to write
	INDEX_FILE_PRE                     = "index_"
	INDEX_LOG_FILE_PRE                 = "index_log_"
	ACTION_ADD                         = "add"
	ACTION_UPDATE                      = "update"
)

// main data structure
type Data struct {
	Id               int
	Dir              string
	IndexTree        *b.Tree // key is oid, value is Index
	OidMd5Tree       *b.Tree // key is md5, value is oid
	OidCreatedTree   *b.Tree // key is created time, value is oid
	LastModifyMillis time.Time
	mutex            *sync.Mutex
}

func (this *Data) Init(id int, dir string) error {
	this.Id = id
	this.Dir = dir
	this.mutex = new(sync.Mutex)

	return this.generateLogFile()
}

func (this *Data) generateLogFile() error {
	fn := this.getWriteLogFile()
	fi, error := os.Stat(fn)
	isExists := error == nil || os.IsExist(error)

	if !isExists {
		file, error := os.Create(fn)
		if error != nil {
			return error
		}

		this.LastModifyMillis = time.Now()
		defer file.Close()
	} else {
		this.LastModifyMillis = fi.ModTime()
	}

	return nil
}

func (this *Data) getPersistFile() string {
	return this.Dir + "/" + INDEX_FILE_PRE + strconv.Itoa(this.Id)
}

func (this *Data) getWriteLogFile() string {
	return this.Dir + "/" + INDEX_LOG_FILE_PRE + strconv.Itoa(this.Id)
}

func (this *Data) loadEachSync(fn string, indexTree, oidMd5Tree, oidCreatedTree *b.Tree) error {
	_, e := os.Stat(fn)
	isExists := e == nil || os.IsExist(e)

	if !isExists {
		common.Log.Info("center data no index file to load", this.Id, fn)
	} else {
		common.Log.Info("center data begin to load index file", this.Id, fn)
		bb, e := ioutil.ReadFile(fn)
		if e != nil {
			return e
		}

		bbDepressed, e := common.Depress(bb)
		if e != nil {
			return e
		}

		e = this.appendIndexFromBytes(bbDepressed, indexTree, oidMd5Tree, oidCreatedTree)
		if e != nil {
			return e
		}
	}

	return nil
}

func (this *Data) Load() (err error) {
	if this.Id == 0 {
		err = errors.New("center data load error as no id given")
		return
	}
	this.mutex.Lock()
	defer this.mutex.Unlock()

	indexTree := b.TreeNew(common.CmpStr)
	oidMd5Tree := b.TreeNew(common.CmpByte)
	oidCreatedTree := b.TreeNew(common.CmpInt64)

	rawPersistFilePath := INDEX_FILE_PRE + strconv.Itoa(this.Id)
	exp := rawPersistFilePath + "_part\\d+$"
	reg := regexp.MustCompile(exp)

	files := make([]string, 1, 1)
	e := filepath.Walk(this.Dir, func(path string, info os.FileInfo, err error) error {
		if strings.HasSuffix(path, rawPersistFilePath) || reg.MatchString(path) {
			files = append(files, path)
		}
		return err
	})
	if e != nil {
		err = e
		return
	}

	common.Log.Info("center data load from files", files)
	fileNum := len(files)

	for i := 1; i < fileNum; i++ {
		e = this.loadEachSync(files[i], indexTree, oidMd5Tree, oidCreatedTree)
		if e != nil {
			common.Log.Info("center data load part error", files[i], e)
			err = e
			return
		} else {
			common.Log.Info("center data load part ok", files[i])
		}
	}

	// read from log
	e = this.appendIndexFromLogFile(indexTree, oidMd5Tree, oidCreatedTree)
	if e != nil {
		err = e
		return
	}

	this.IndexTree = indexTree
	this.OidMd5Tree = oidMd5Tree
	this.OidCreatedTree = oidCreatedTree

	return nil
}

func (this *Data) appendIndexFromLogFile(indexTree, oidMd5Tree, oidCreatedTree *b.Tree) (err error) {
	fn := this.getWriteLogFile()
	common.Log.Info("center data begin get from log " + fn)
	_, error := os.Stat(fn)
	isExists := error == nil || os.IsExist(error)
	if !isExists {
		return
	}

	bb, error := ioutil.ReadFile(fn)
	if error != nil {
		err = error
		return
	}

	return this.appendIndexFromBytes(bb, indexTree, oidMd5Tree, oidCreatedTree)
}

func (this *Data) appendIndexFromBytes(bb []byte, indexTree, oidMd5Tree, oidCreatedTree *b.Tree) (err error) {
	arr := bytes.Split(bb, common.SP)
	common.Log.Info("center data load split number", len(arr))
	for _, recBinary := range arr {
		// 0 means it's the last one
		if len(recBinary) == 0 {
			continue
		}

		var rec Record
		error := GetIndexFrom(recBinary, &rec)

		if error != nil {
			common.Log.Error("center data load record error", recBinary, error)
			err = error
			return
		}

		oid := rec.Oid
		indexTree.Set(oid, rec)
		oidMd5Tree.Set(rec.Md5, oid)
		oidCreatedTree.Set(rec.Created, oid)
	}

	return nil
}

func (this *Data) persistEachSync(bb []byte, suf string) error {
	fn := this.getPersistFile() + suf
	bbCompressed, e := common.Compress(bb)
	if e != nil {
		return e
	}
	return common.Write2File(bbCompressed, fn, os.O_WRONLY)
}

// write to file
func (this *Data) Persist() (err error) {
	if this.IndexTree == nil {
		return errors.New("center data persisi error as index tree not exist")
	}
	this.mutex.Lock()
	defer this.mutex.Unlock()

	en, e := this.IndexTree.SeekFirst()
	if e != nil {
		err = e
		return
	}

	buf := &bytes.Buffer{}

	j := 0
	fileIndex := 0
	for {
		k, v, e := en.Next()
		if e != nil {
			if e != io.EOF {
				err = e
				return
			}

			// last
			arr := buf.Bytes()
			if len(arr) > 0 {
				common.Log.Info("center data begin persist part", this.Id, this.Dir, j)
				e = this.persistEachSync(buf.Bytes(), "")
				if e != nil {
					err = e
					return
				}
			}

			break
		}

		oid := k.(string)
		rec := v.(Record)
		rec.Oid = oid

		bb, e := ConvIndexTo(rec)
		if e != nil {
			err = e
			return
		}

		buf.Write(bb)
		buf.Write(common.SP)

		if (j+1)%PERSIST_EACH_FILE_RECORD_NUM_LIMIT == 0 {
			common.Log.Info("center data begin persist part", this.Id, this.Dir, j+1)

			e = this.persistEachSync(buf.Bytes(), "_part"+strconv.Itoa(fileIndex))
			if e != nil {
				err = e
				return
			}
			buf.Reset()
			fileIndex++
		}

		j++
	}

	// move log file as bak
	fn := this.getWriteLogFile()
	_, e = os.Stat(fn)
	isExists := e == nil || os.IsExist(e)
	if isExists {
		suf := "_bak_" + strconv.FormatInt(time.Now().Unix(), 10)
		e = os.Rename(fn, fn+suf)

		if e != nil {
			err = e
			return
		}
	}

	return this.generateLogFile()
}

func (this *Data) Set(rec Record) (err error) {
	return this.SetBatch([]Record{rec})
}

func (this *Data) SetWithLog(rec Record, writeLog bool) (err error) {
	return this.SetBatchWithLog([]Record{rec}, writeLog)
}

func (this *Data) SetBatch(recs []Record) (err error) {
	return this.SetBatchWithLog(recs, true)
}

func (this *Data) SetBatchWithLog(recs []Record, writeLog bool) (err error) {
	this.mutex.Lock()
	defer this.mutex.Unlock()

	this.LastModifyMillis = time.Now()
	if this.IndexTree == nil {
		this.IndexTree = b.TreeNew(common.CmpStr)
		this.OidMd5Tree = b.TreeNew(common.CmpByte)
		this.OidCreatedTree = b.TreeNew(common.CmpInt64)
	}

	if this.IndexTree.Len() >= MAX_TREE_LEN {
		return errors.New("data error as exceed max length")
	}

	// batch flush and using chan (a log system kafka etc) can improve throughput but worsen response time
	if writeLog {
		e := this.WriteLog(recs)
		if e != nil {
			err = e
			return
		}
	}

	for _, rec := range recs {
		oid := rec.Oid
		this.IndexTree.Set(oid, rec)
		this.OidMd5Tree.Set(rec.Md5, oid)
		this.OidCreatedTree.Set(rec.Created, oid)
	}

	return nil
}

func (this *Data) WriteLog(recs []Record) error {
	fn := this.getWriteLogFile()
	_, error := os.Stat(fn)
	isExists := error == nil || os.IsExist(error)
	if !isExists {
		return errors.New("center data log file not exists for data " + strconv.Itoa(this.Id))
	}

	file, error := os.OpenFile(fn, os.O_APPEND, 0666)
	if error != nil {
		return error
	}
	defer file.Close()

	buf := &bytes.Buffer{}
	for _, rec := range recs {
		body, error := ConvIndexTo(rec)
		if error != nil {
			return error
		}

		buf.Write(body)
		buf.Write(common.SP)
	}

	_, error = file.Write(buf.Bytes())
	return error
}

func (this *Data) Get(oid string) (rec Record, err error) {
	v, ok := this.IndexTree.Get(oid)
	if !ok {
		err = errors.New("center data get but not found " + oid)
		return
	}

	return v.(Record), nil
}

func (this *Data) Len() int {
	if this.IndexTree == nil {
		return 0
	}

	return this.IndexTree.Len()
}
