package woocache

import (
	"github.com/cespare/xxhash/v2"
	"sync"
)

const (
	segmentCount    = 256
	segmentAndOpVal = 255
	minBufSize      = 512 * 1024
)

type Cache struct {
	locks    [segmentCount]sync.RWMutex
	segments [segmentCount]segment
}

func NewCache(size int) *Cache {
	return NewCacheCustomTimer(size, DefaultTimer{})
}

func NewCacheCustomTimer(size int, timer Timer) (cache *Cache) {
	if size < minBufSize {
		size = minBufSize
	}
	if timer == nil {
		timer = DefaultTimer{}
	}
	cache = &Cache{}
	for i := 0; i < segmentCount; i++ {
		cache.segments[i] = newSegment(size/segmentCount, i, timer)
	}
	return
}

func hashFunc(data []byte) uint64 {
	return xxhash.Sum64(data)
}

func (cache *Cache) Set(key, value []byte, expireSecond int) (err error) {
	hashVal := hashFunc(key)
	segID := hashVal & segmentAndOpVal
	cache.locks[segID].Lock()
	defer cache.locks[segID].Unlock()
	err = cache.segments[segID].set(key, value, hashVal, expireSecond)
	return
}
