package woocache

import (
	"errors"
	"fmt"
)

var ErrOutOfRange = errors.New("out of range")

type RingBuf struct {
	begin int64
	end   int64
	data  []byte
	index int
}

func NewRingBuf(size int, begin int64) (rb RingBuf) {
	rb.data = make([]byte, size)
	rb.Reset(begin)
	return
}

func (rb *RingBuf) Reset(begin int64) {
	rb.begin = begin
	rb.end = begin
	rb.index = 0
}

func (rb *RingBuf) Dump() []byte {
	dump := make([]byte, len(rb.data))
	copy(dump, rb.data)
	return dump
}

func (rb *RingBuf) String() string {
	return fmt.Sprintf("[size:%v, start:%v, end:%v, index:%v]", len(rb.data), rb.begin, rb.end, rb.index)
}

func (rb *RingBuf) Size() int64 {
	return int64(len(rb.data))
}

func (rb *RingBuf) Begin() int64 {
	return rb.begin
}

func (rb *RingBuf) End() int64 {
	return rb.end
}

// TODO
func (rb *RingBuf) ReadAt(p []byte, off int64) (n int, e error) {
	if off < rb.begin || off > rb.end {
		e = ErrOutOfRange
		return
	}
	return 0, nil
}

// TODO
func (rb *RingBuf) Slice(off, length int64) ([]byte, error) {
	return nil, nil
}

func (rb *RingBuf) getDataOff(off int64) int {
	var dataOff int
	if (rb.end - rb.begin) < int64(len(rb.data)) {
		dataOff = int(off - rb.begin)
	} else {
		dataOff = rb.index + int(off-rb.begin)
	}
	if dataOff >= len(rb.data) {
		dataOff -= len(rb.data)
	}
	return dataOff
}

func (rb *RingBuf) Write(p []byte) (n int, err error) {
	if len(p) > len(rb.data) {
		err = ErrOutOfRange
		return
	}
	for n < len(p) {
		written := copy(rb.data[rb.index:], p[n:])
		rb.end += int64(written)
		rb.index += written
		n += written
		if rb.index >= len(rb.data) {
			rb.index -= len(rb.data)
		}
	}
	if int(rb.end-rb.begin) > len(rb.data) {
		rb.begin = rb.end - int64(len(rb.data))
	}
	return
}

func (rb *RingBuf) WriteAt(p []byte, off int64) (n int, err error) {
	if off+int64(len(p)) > rb.end || off < rb.begin {
		err = ErrOutOfRange
		return
	}
	writeOff := rb.getDataOff(off)
	writeEnd := writeOff + int(rb.end-off)
	if writeEnd <= len(rb.data) {
		n = copy(rb.data[writeOff:writeEnd], p)
	} else {
		n = copy(rb.data[writeOff:], p)
		if n < len(p) {
			n += copy(rb.data[:writeEnd-len(rb.data)], p[n:])
		}
	}
	return
}

func (rb *RingBuf) Skip(length int64) {
	rb.end += length
	rb.index += int(length)
	for rb.index >= len(rb.data) {
		rb.index -= len(rb.data)
	}
	if int(rb.end-rb.begin) > len(rb.data) {
		rb.begin = rb.end - int64(len(rb.data))
	}
}
