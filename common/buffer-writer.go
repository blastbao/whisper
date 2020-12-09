package common

import (
	"bytes"
	"os"
	"sync"
)

const DEFAULT_BUFFER_SIZE = 30

type BufferWriter struct {
	fn         string
	bb         *bytes.Buffer
	count      int
	bufferSize int
	mutex      *sync.Mutex
}

func NewBufferWriter(fn string, bufferSize int) *BufferWriter {
	r := &BufferWriter{}
	r.fn = fn
	r.count = 0
	r.bufferSize = bufferSize
	r.bb = &bytes.Buffer{}
	r.mutex = new(sync.Mutex)
	return r
}

func (bw *BufferWriter) Flush() (err error) {
	bw.mutex.Lock()
	defer bw.mutex.Unlock()

	e := Write2File(bw.bb.Bytes(), bw.fn, os.O_APPEND)
	if e != nil {
		err = e
		return
	}

	bw.count = 0
	bw.bb.Reset()

	return
}

func (bw *BufferWriter) Write(msg string) (err error) {
	_, e := bw.bb.WriteString(msg)
	if e != nil {
		err = e
		return
	}
	bw.count++
	bw.bb.WriteByte('\n')

	if bw.count >= bw.bufferSize {
		return bw.Flush()
	}

	return
}
