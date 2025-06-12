package woocache

import (
	"errors"
	"sync/atomic"
	"unsafe"
)

const (
	HASH_ENTRY_SIZE = 16
	ENTRY_HDR_SIZE  = 24
	MAX_KEY_LEN     = 65535
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
	hash16     uint16
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

func (seg *segment) set(key, value []byte, hashVal uint64, expireSeconds int) (err error) {
	if len(key) > MAX_KEY_LEN {
		return ErrLargeKey
	}
	maxKeyValLen := len(seg.rb.data)/4 - ENTRY_HDR_SIZE
	if len(key)+len(value) > maxKeyValLen {
		return ErrLargeEntry
	}
	slotId := uint8(hashVal >> 8)
	hash16 := uint16(hashVal >> 16)
	slot := seg.getSlot(slotId)
	idx, match := seg.lookup(slot, hash16, key)

	now := seg.timer.Now()
	expireAt := uint32(0)
	if expireSeconds > 0 {
		expireAt = now + uint32(expireSeconds)
	}

	var hdrBuf [ENTRY_HDR_SIZE]byte
	hdr := (*entryHdr)(unsafe.Pointer(&hdrBuf[0]))
	if match {
		matchedPtr := &slot[idx]
		seg.rb.ReadAt(hdrBuf[:], matchedPtr.offset)
		hdr.slotId = slotId
		hdr.hash16 = hash16
		hdr.keyLen = uint16(len(key))
		originAcessTime := hdr.accessTime
		hdr.accessTime = now
		hdr.expireAt = expireAt
		hdr.valLen = uint32(len(value))
		if hdr.valCap >= hdr.valLen {
			atomic.AddInt64(&seg.totalTime, int64(hdr.accessTime-originAcessTime))
			seg.rb.WriteAt(hdrBuf[:], matchedPtr.offset)
			seg.rb.WriteAt(value, matchedPtr.offset+int64(matchedPtr.keyLen)+ENTRY_HDR_SIZE)
			atomic.AddUint64(&seg.overwrites, 1)
			return
		}
		seg.delEntryPtr(slotId, slot, idx)
		match = false
		for hdr.valCap < hdr.valLen {
			hdr.valCap *= 2
		}
		if hdr.valCap > uint32(maxKeyValLen-len(key)) {
			hdr.valCap = uint32(maxKeyValLen - len(key))
		}
	} else {
		hdr.slotId = slotId
		hdr.hash16 = hash16
		hdr.keyLen = uint16(len(key))
		hdr.accessTime = now
		hdr.expireAt = expireAt
		hdr.valLen = uint32(len(value))
		hdr.valCap = uint32(len(value))
		if hdr.valCap == 0 {
			hdr.valCap = 1
		}
	}

	entryLen := ENTRY_HDR_SIZE + int64(len(key)) + int64(hdr.valCap)
	slotModified := seg.evacuate(entryLen, slotId, now)
	if slotModified {
		slot := seg.getSlot(slotId)
		idx, match = seg.lookup(slot, hash16, key)
	}
	newOff := seg.rb.End()
	seg.insertEntryPtr(slotId, hash16, newOff, idx, hdr.keyLen)
	seg.rb.Write(hdrBuf[:])
	seg.rb.Write(key)
	seg.rb.Write(value)
	seg.rb.Skip(int64(hdr.valCap - hdr.valLen))
	atomic.AddInt64(&seg.totalTime, int64(now))
	atomic.AddInt64(&seg.totalCount, 1)
	seg.vacuumLen -= entryLen
	return
}

func (seg *segment) evacuate(entryLen int64, slotId uint8, now uint32) (slotModified bool) {
	var oldHdrBuf [ENTRY_HDR_SIZE]byte
	consecutiveEvacuate := 0
	for seg.vacuumLen < entryLen {
		oldOff := seg.vacuumLen + seg.rb.End() - seg.rb.Size()
		seg.rb.ReadAt(oldHdrBuf[:], oldOff)
		oldHdr := (*entryHdr)(unsafe.Pointer(&oldHdrBuf[0]))
		oldEntryLen := ENTRY_HDR_SIZE + int64(oldHdr.keyLen) + int64(oldHdr.valCap)
		if oldHdr.deleted {
			consecutiveEvacuate = 0
			atomic.AddInt64(&seg.totalTime, -int64(oldHdr.accessTime))
			atomic.AddInt64(&seg.totalCount, -1)
			seg.vacuumLen += oldEntryLen
			continue
		}
		expired := isExpired(oldHdr.expireAt, now)
		leastRecentUsed := int64(oldHdr.accessTime)*atomic.LoadInt64(&seg.totalCount) <= atomic.LoadInt64(&seg.totalTime)
		if expired || leastRecentUsed || consecutiveEvacuate > 5 {
			seg.delEntryPtrByOffset(oldHdr.slotId, oldHdr.hash16, oldOff)
			if oldHdr.slotId == slotId {
				slotModified = true
			}
			consecutiveEvacuate = 0
			atomic.AddInt64(&seg.totalTime, -int64(oldHdr.accessTime))
			atomic.AddInt64(&seg.totalCount, -1)
			seg.vacuumLen += oldEntryLen
			if expired {
				atomic.AddUint64(&seg.totalExpired, 1)
			} else {
				atomic.AddUint64(&seg.totalEvacuate, 1)
			}
		} else {
			newOff := seg.rb.Evacuate(oldOff, int(oldEntryLen))
			seg.updateEntryPtr(oldHdr.slotId, oldHdr.hash16, oldOff, newOff)
			consecutiveEvacuate++
			atomic.AddUint64(&seg.totalEvacuate, 1)
		}
	}
	return
}

func (seg *segment) lookup(slot []entryPtr, hash16 uint16, key []byte) (idx int, match bool) {
	idx = entryPtrIdx(slot, hash16)
	for idx < len(slot) {
		ptr := &slot[idx]
		if ptr.hash16 != hash16 {
			break
		}
		match := int(ptr.keyLen) == len(key) && seg.rb.EqualAt(key, ptr.offset+ENTRY_HDR_SIZE)
		if match {
			return
		}
		idx++
	}
	return
}

func (seg *segment) lookupByOffset(slot []entryPtr, hash16 uint16, offset int64) (idx int, match bool) {
	idx = entryPtrIdx(slot, hash16)
	for idx < len(slot) {
		ptr := &slot[idx]
		if ptr.hash16 != hash16 {
			break
		}
		match = offset == ptr.offset
		if match {
			return
		}
		idx++
	}
	return
}

func entryPtrIdx(slot []entryPtr, hash16 uint16) (idx int) {
	high := len(slot) - 1
	for idx <= high {
		mid := (idx + high) / 2
		oldEntry := &slot[mid]
		if oldEntry.hash16 >= hash16 {
			high = mid - 1
		} else {
			idx = mid + 1
		}
	}
	return
}

func (seg *segment) expand() {
	newSlotData := make([]entryPtr, seg.slotCap*2*256)
	for i := 0; i < 256; i++ {
		off := int32(i) * seg.slotCap
		copy(newSlotData[2*off:], seg.slotsData[off:off+seg.slotLens[i]])
	}
	seg.slotCap *= 2
	seg.slotsData = newSlotData
}

func (seg *segment) updateEntryPtr(slotId uint8, hash16 uint16, oldOff int64, newOff int64) {
	slot := seg.getSlot(slotId)
	idx, match := seg.lookupByOffset(slot, hash16, oldOff)
	if !match {
		return
	}
	ptr := &slot[idx]
	ptr.offset = newOff
}

func (seg *segment) delEntryPtr(slotId uint8, slot []entryPtr, idx int) {
	offset := slot[idx].offset
	var entryHdrBuf [ENTRY_HDR_SIZE]byte
	seg.rb.ReadAt(entryHdrBuf[:], offset)
	entryHdr := (*entryHdr)(unsafe.Pointer(&entryHdrBuf[0]))
	entryHdr.deleted = true
	seg.rb.WriteAt(entryHdrBuf[:], offset)
	copy(slot[idx:], slot[idx+1:])
	seg.slotLens[slotId]--
	atomic.AddInt64(&seg.entryCount, -1)
}

func (seg *segment) delEntryPtrByOffset(slotId uint8, hash16 uint16, offset int64) {
	slot := seg.getSlot(slotId)
	idx, match := seg.lookupByOffset(slot, hash16, offset)
	if !match {
		return
	}
	seg.delEntryPtr(slotId, slot, idx)
}

func (seg *segment) insertEntryPtr(slotId uint8, hash16 uint16, offset int64, idx int, keyLen uint16) {
	if seg.slotLens[slotId] == seg.slotCap {
		seg.expand()
	}
	seg.slotLens[slotId]++
	atomic.AddInt64(&seg.entryCount, 1)
	slot := seg.getSlot(slotId)
	copy(slot[idx+1:], slot[idx:])
	slot[idx].hash16 = hash16
	slot[idx].offset = offset
	slot[idx].keyLen = keyLen
}

func (seg *segment) getSlot(slotId uint8) []entryPtr {
	slotOff := int32(slotId) * seg.slotCap
	return seg.slotsData[slotOff : slotOff+seg.slotLens[slotId] : slotOff+seg.slotCap]
}

func isExpired(keyExpireAt, now uint32) bool {
	return keyExpireAt != 0 && keyExpireAt <= now
}
