package common

import (
	"bytes"
	"compress/flate"
	"crypto/md5"
	"github.com/alecthomas/binary"
	"io"
	"io/ioutil"
	"net"
	"os"
	"os/user"
	"strconv"
	"strings"
	"time"
)

// generate a simple indentity id for this byte array
func GenMd5(b []byte) []byte {
	len := len(b)

	buf := bytes.Buffer{}
	buf.WriteString(strconv.Itoa(len))

	var step int = len / 10

	for i := 0; i < len-1; i = i + step {
		buf.WriteByte(b[i])
	}

	t := md5.New()
	t.Write(buf.Bytes())

	return t.Sum(nil)
}

func CheckMd5(b []byte, md5 []byte) bool {
	r := GenMd5(b)

	return bytes.Compare(r, md5) == 0
}

func CmpInt(a, b interface{}) int {
	return a.(int) - b.(int)
}

func CmpInt64(a, b interface{}) int {
	return int(a.(int64) - b.(int64))
}

func CmpByte(a, b interface{}) int {
	return bytes.Compare(a.([]byte), b.([]byte))
}

func CmpStr(a, b interface{}) int {
	s1 := a.(string)
	s2 := b.(string)

	len1 := len(s1)
	len2 := len(s2)

	if len1 != len2 {
		return len1 - len2
	}

	for i := 0; i < len1; i++ {
		ch1 := int(s1[i])
		ch2 := int(s2[i])

		if ch1 != ch2 {
			return ch1 - ch2
		}

	}

	return 0
}

// 检查 slice 中是否包含 item
func ContainsStr(slice []string, item string) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}

// struct serialize
func Enc(v interface{}) (b []byte, err error) {
	return binary.Marshal(v)
}

func Dec(b []byte, v interface{}) error {
	return binary.Unmarshal(b, v)
}

// compress
func Compress(b []byte) (r []byte, err error) {
	var buf bytes.Buffer

	w, e := flate.NewWriter(&buf, flate.BestCompression)
	defer w.Close()
	if e != nil {
		err = e
		return
	}

	_, e = w.Write(b)
	if e != nil {
		err = e
		return
	}

	e = w.Flush()
	if e != nil {
		err = e
		return
	}

	return ioutil.ReadAll(&buf)
}

func Depress(b []byte) (r []byte, err error) {
	reader := flate.NewReader(bytes.NewReader(b))
	defer reader.Close()
	r, e := ioutil.ReadAll(reader)
	if e != nil && e != io.ErrUnexpectedEOF {
		err = e
		return
	}

	return
}

func GetUserHomeFile(fileName string) string {
	user, e := user.Current()
	if e != nil {
		return "/" + fileName
	}

	return user.HomeDir + "/" + fileName
}

// file io utils
func Write2File(b []byte, fn string, writeType int) error {
	_, error := os.Stat(fn)
	isExists := error == nil || os.IsExist(error)

	var file *os.File
	if !isExists {
		file, error = os.Create(fn)
		if error != nil {
			return error
		}
	} else {
		// os.O_WRONLY or
		file, error = os.OpenFile(fn, writeType, 0666)
		if error != nil {
			return error
		}
	}
	defer file.Close()

	_, error = file.Write(b)
	if error != nil {
		return error
	}

	return nil
}

// get host
func GetLocalAddr() string {
	addrs, e := net.InterfaceAddrs()
	if e != nil {
		panic(e)
	}
	var addr string
	for _, one := range addrs {
		addr = one.String()
	}
	return addr
}

func IsErrorMatch(e error, msg string) bool {
	return strings.Contains(e.Error(), msg)
}

// performance time trace
func Trace(s string) (string, time.Time) {
	Log.Info("START:", s)
	return s, time.Now()
}

func End(s string, startTime time.Time) {
	elapsed := time.Since(startTime)
	Log.Info("trace end: %s, elapsed %f secs\n", s, elapsed.Seconds())
}
