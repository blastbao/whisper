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

func (this *BufferWriter) Flush() (err error) {
	this.mutex.Lock()
	defer this.mutex.Unlock()

	e := Write2File(this.bb.Bytes(), this.fn, os.O_APPEND)
	if e != nil {
		err = e
		return
	}

	this.count = 0
	this.bb.Reset()

	return
}

func (this *BufferWriter) Write(msg string) (err error) {
	_, e := this.bb.WriteString(msg)
	if e != nil {
		err = e
		return
	}
	this.count++
	this.bb.WriteByte('\n')

	if this.count >= this.bufferSize {
		return this.Flush()
	}

	return
}
