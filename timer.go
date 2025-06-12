package woocache

import "time"

type Timer interface {
	Now() uint32
}

type DefaultTimer struct{}

func (timer DefaultTimer) Now() uint32 {
	return getUnixTime()
}

func getUnixTime() uint32 {
	return uint32(time.Now().Unix())
}
