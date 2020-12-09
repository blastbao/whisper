package center

import (
	"bytes"
	"errors"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/blastbao/whisper/common"
	"github.com/cznic/b"
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
type Index struct {
	// index id
	Id               int
	// 存储目录
	Dir              string
	// oid => rec
	IndexTree        *b.Tree // key is oid, value is Index
	// Md5 => oid
	OidMd5Tree       *b.Tree // key is md5, value is oid
	// CTime => oid
	OidCreatedTree   *b.Tree // key is created time, value is oid
	// MTime
	LastModifyMillis time.Time
	mutex            *sync.Mutex
}

func (index *Index) Init(id int, dir string) error {
	index.Id = id
	index.Dir = dir
	index.mutex = new(sync.Mutex)

	return index.generateLogFile()
}


// 创建日志文件
func (index *Index) generateLogFile() error {

	fd := index.getWriteLogFile()
	fi, error := os.Stat(fd)

	isExists := error == nil || os.IsExist(error)

	// 不存在则创建，并更新修改时间
	if !isExists {
		file, error := os.Create(fd)
		if error != nil {
			return error
		}
		defer file.Close()
		index.LastModifyMillis = time.Now()
	// 存在则获取修改时间
	} else {
		index.LastModifyMillis = fi.ModTime()
	}
	return nil
}

// dir/index_{dataId}
func (index *Index) getPersistFile() string {
	return index.Dir + "/" + INDEX_FILE_PRE + strconv.Itoa(index.Id)
}

// dir/index_log_{dataId}
func (index *Index) getWriteLogFile() string {
	return index.Dir + "/" + INDEX_LOG_FILE_PRE + strconv.Itoa(index.Id)
}

// 加载索引文件
func (index *Index) loadEachSync(fileName string, indexTree, oidMd5Tree, oidCreatedTree *b.Tree) error {

	_, e := os.Stat(fileName)
	isExists := e == nil || os.IsExist(e)

	// 若索引文件不存在，打印日志。
	if !isExists {
		common.Log.Info("center index no index file to load", index.Id, fileName)
	} else {
		common.Log.Info("center index begin to load index file", index.Id, fileName)

		// 读取索引文件
		content, e := ioutil.ReadFile(fileName)
		if e != nil {
			return e
		}

		// 解压缩
		raw, e := common.Depress(content)
		if e != nil {
			return e
		}

		// 将索引数据同步到索引中
		e = index.appendIndexFromBytes(raw, indexTree, oidMd5Tree, oidCreatedTree)
		if e != nil {
			return e
		}
	}

	return nil
}

func (index *Index) Load() error {

	if index.Id == 0 {
		return errors.New("center index load error as no id given")
	}

	index.mutex.Lock()
	defer index.mutex.Unlock()

	// 创建三个索引结果
	indexTree := b.TreeNew(common.CmpStr)
	oidMd5Tree := b.TreeNew(common.CmpByte)
	oidCreatedTree := b.TreeNew(common.CmpInt64)

	// 原始索引文件：index_{dataID}
	rawPersistFilePath := INDEX_FILE_PRE + strconv.Itoa(index.Id)

	// 原始索引文件：index_{dataID}_part{xxx}
	exp := rawPersistFilePath + "_part\\d+$"

	// 正则表达式
	reg := regexp.MustCompile(exp)

	// 遍历 index.Dir ，获取其中所有 "以 index_{dataID} 为后缀" 或者 "匹配 index_{dataID}_part{xxx} 模式" 的索引文件
	files := make([]string, 1, 1)
	if err := filepath.Walk(
		index.Dir,
		func(path string, info os.FileInfo, e error) error {
			if strings.HasSuffix(path, rawPersistFilePath) || reg.MatchString(path) {
				files = append(files, path)
			}
			return e
		},
	); err != nil {
		return err
	}

	common.Log.Info("center index load from files", files)

	// 逐个文件进行加载
	for _, file := range files {
		//
		if err := index.loadEachSync(file, indexTree, oidMd5Tree, oidCreatedTree); err != nil {
			common.Log.Info("center index load part error", file, err)
			return err
		}
		common.Log.Info("center index load part ok", file)
	}

	// read from log
	if err := index.appendIndexFromLogFile(indexTree, oidMd5Tree, oidCreatedTree); err != nil {
		return err
	}

	index.IndexTree = indexTree
	index.OidMd5Tree = oidMd5Tree
	index.OidCreatedTree = oidCreatedTree

	return nil
}

func (index *Index) appendIndexFromLogFile(indexTree, oidMd5Tree, oidCreatedTree *b.Tree) error {

	// 读取日志文件
	fn := index.getWriteLogFile()
	common.Log.Info("center index begin get from log " + fn)

	// 不存在直接返回
	_, err := os.Stat(fn)
	isExists := err == nil || os.IsExist(err)
	if !isExists {
		return nil
	}

	// 读取日志文件内容
	bb, err := ioutil.ReadFile(fn)
	if err != nil {
		return err
	}

	// 将日志数据 bb 同步到索引中
	return index.appendIndexFromBytes(bb, indexTree, oidMd5Tree, oidCreatedTree)
}

// 将索引数据 bb 同步到索引中
func (index *Index) appendIndexFromBytes(bb []byte, indexTree, oidMd5Tree, oidCreatedTree *b.Tree) error {

	// 按分隔符切割，得到一组索引项
	records := bytes.Split(bb, common.SP)
	common.Log.Info("center index load split number", len(records))

	// 将 records 逐个同步到索引 indexTree/oidMd5Tree/oidCreatedTree 中
	for _, recBinary := range records {

		// 0 means it's the last one
		if len(recBinary) == 0 {
			continue
		}

		// 反序列化，得到 Record 对象
		var rec Record
		if err := GetIndexFrom(recBinary, &rec); err != nil {
			common.Log.Error("center index load record error", recBinary, err)
			return err
		}

		// 同步到索引
		oid := rec.Oid
		indexTree.Set(oid, rec)
		oidMd5Tree.Set(rec.Md5, oid)
		oidCreatedTree.Set(rec.Created, oid)

	}

	return nil
}


func (index *Index) persistEachSync(raw []byte, part string) error {

	// 文件名: dir/index_{dataId}_{part{xxx}}
	fn := index.getPersistFile() + part

	// 数据压缩
	compressed, e := common.Compress(raw)
	if e != nil {
		return e
	}

	// 写入文件
	return common.Write2File(compressed, fn, os.O_WRONLY)
}

// write to file
//
//
func (index *Index) Persist() error {

	// 主索引不存在，报错
	if index.IndexTree == nil {
		return errors.New("center index persist error as index tree not exist")
	}

	index.mutex.Lock()
	defer index.mutex.Unlock()

	// 定位到首元素
	en, e := index.IndexTree.SeekFirst()
	if e != nil {
		return e
	}

	buf := &bytes.Buffer{}

	j := 0
	fileIndex := 0

	// 遍历索引元素，把所含每个 record 序列化后存储到索引文件中
	for {

		k, v, e := en.Next()
		if e != nil {

			if e != io.EOF {
				return e
			}

			// last
			arr := buf.Bytes()
			if len(arr) > 0 {
				common.Log.Info("center index begin persist part", index.Id, index.Dir, j)
				// 将 buf 写入到索引文件 dir/index_{dataId} 文件中。
				if e := index.persistEachSync(buf.Bytes(), ""); e != nil {
					return e
				}
			}

			break
		}

		// k,v
		oid := k.(string)
		rec := v.(Record)
		rec.Oid = oid

		// 序列化成二进制，binary.Marshal(rec)
		bb, e := ConvIndexTo(rec)
		if e != nil {
			return e
		}

		// 写入数据
		buf.Write(bb)

		// 写入分隔符
		buf.Write(common.SP)

		// 判断是否要分文件
		if (j+1)%PERSIST_EACH_FILE_RECORD_NUM_LIMIT == 0 {
			common.Log.Info("center index begin persist part", index.Id, index.Dir, j+1)
			// 存储到索引文件 dir/index_{dataId}_part{xxx} 中
			if err := index.persistEachSync(buf.Bytes(), "_part"+strconv.Itoa(fileIndex)); err != nil {
				return err
			}
			// 清空 buf
			buf.Reset()
			// 文件下标++
			fileIndex++
		}

		// 数据条数++
		j++
	}


	// move log file as bak

	// 将当前的日志文件 dir/index_log_{dataId} 修改为 dir/index_log_{dataId}_bak_timestamp 。
	fn := index.getWriteLogFile()
	_, e = os.Stat(fn)
	isExists := e == nil || os.IsExist(e)
	if isExists {
		suf := "_bak_" + strconv.FormatInt(time.Now().Unix(), 10)
		if err := os.Rename(fn, fn+suf) ; err != nil {
			return err
		}
	}

	// 生成新的日志文件
	return index.generateLogFile()
}

func (index *Index) Set(rec Record) (err error) {
	return index.SetBatch([]Record{rec})
}

func (index *Index) SetWithLog(rec Record, writeLog bool) (err error) {
	return index.SetBatchWithLog([]Record{rec}, writeLog)
}

func (index *Index) SetBatch(recs []Record) (err error) {
	return index.SetBatchWithLog(recs, true)
}

//
func (index *Index) SetBatchWithLog(recs []Record, writeLog bool) error {

	index.mutex.Lock()
	defer index.mutex.Unlock()

	// 更近最近修改时间
	index.LastModifyMillis = time.Now()

	// 初始化各个索引
	if index.IndexTree == nil {
		index.IndexTree = b.TreeNew(common.CmpStr)
		index.OidMd5Tree = b.TreeNew(common.CmpByte)
		index.OidCreatedTree = b.TreeNew(common.CmpInt64)
	}

	// 索引规模限制
	if index.IndexTree.Len() >= MAX_TREE_LEN {
		return errors.New("index error as exceed max length")
	}

	// batch flush and using chan (a log system kafka etc) can improve throughput but worsen response time
	// 批量刷新和使用管道（如 kafka 等）可以提高吞吐量，但会增加耗时。
	if writeLog {
		// 将 recs 写入日志
		if err := index.WriteLog(recs); err != nil {
			return err
		}
	}

	// 将 recs 逐个写入索引
	for _, rec := range recs {
		oid := rec.Oid
		// ID => rec
		index.IndexTree.Set(oid, rec)
		// Md5 => ID
		index.OidMd5Tree.Set(rec.Md5, oid)
		// CTime => ID
		index.OidCreatedTree.Set(rec.Created, oid)
	}

	return nil
}


// 将 recs 写入日志
func (index *Index) WriteLog(recs []Record) error {

	// 获取日志文件
	fn := index.getWriteLogFile()
	_, error := os.Stat(fn)
	isExists := error == nil || os.IsExist(error)
	if !isExists {
		return errors.New("center index log file not exists for index " + strconv.Itoa(index.Id))
	}

	// 追加写入
	file, error := os.OpenFile(fn, os.O_APPEND, 0666)
	if error != nil {
		return error
	}
	defer file.Close()

	// 把 recs 写入临时内存缓存
	buf := &bytes.Buffer{}
	for _, rec := range recs {
		// 二进制序列化
		body, error := ConvIndexTo(rec)
		if error != nil {
			return error
		}
		// 写入数据
		buf.Write(body)
		// 写入分隔符
		buf.Write(common.SP)
	}

	// 写入文件
	_, error = file.Write(buf.Bytes())
	return error
}

// 根据 id 从 IndexTree 获取 record
func (index *Index) Get(oid string) (rec Record, err error) {
	v, ok := index.IndexTree.Get(oid)
	if !ok {
		err = errors.New("center index get but not found " + oid)
		return
	}
	return v.(Record), nil
}

// 获取 IndexTree 所含 record 数目
func (index *Index) Len() int {
	if index.IndexTree == nil {
		return 0
	}
	return index.IndexTree.Len()
}
