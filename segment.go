package woocache

import (
	"errors"
)

const (
	HASH_ENTRY_SIZE = 16
	ENTRY_HDR_SIZE  = 24
)

var ErrLargeKey = errors.New("The key is larger than 65535")
var ErrLargeEntry = errors.New("The entry size is larger than 1/1024 of cache size")
var ErrNotFound = errors.New("Entry not found")

type entryPtr struct {
	offset   int64
	hash16   uint16
	keyLen   uint16
	reserved uint32
}

type entryHdr struct {
	accessTime uint32
	expireAt   uint32
	keyLen     uint16
	hashLen    uint16
	valLen     uint32
	valCap     uint32
	deleted    bool
	slotId     uint8
	reserved   uint16
}

type segment struct {
	rb            RingBuf
	segId         int
	_             int32
	missCount     int64
	hitCount      int64
	entryCount    int64
	totalCount    int64
	totalTime     int64
	timer         Timer
	totalEvacuate uint64
	totalExpired  uint64
	overwrites    uint64
	touched       uint64
	vacuumLen     int64
	slotLens      [256]int32
	slotCap       int32
	slotsData     []entryPtr
}

func newSegment(bufSize int, segId int, timer Timer) (seg segment) {
	seg.rb = NewRingBuf(bufSize, 0)
	seg.segId = segId
	seg.timer = timer
	seg.vacuumLen = int64(bufSize)
	seg.slotCap = 1
	seg.slotsData = make([]entryPtr, 256*seg.slotCap)
	return
}

func (seg *segment) set(key, value []byte, hashVal, expireSeconds uint32) (err error) {
	return nil
}
