package db

import (
	"crypto/md5"
	"fmt"
	"hash"
	"io"
	"time"
)

//IdGen is responsible for generating unique IDs
type IdGen struct {
	h hash.Hash
}

//NewIDGen constructs a fresh IdGen instance
func NewIDGen() *IdGen {
	return &IdGen{
		h: md5.New(),
	}
}

//GetID constructs an ID
func (ig *IdGen) GetID(data string) string {
	ig.h.Reset()
	io.WriteString(ig.h, data)
	dataHash := ig.h.Sum(nil)
	timestamp := time.Now().UnixNano() / 100

	strTimestamp := fmt.Sprintf("%d", timestamp)
	strHash := fmt.Sprintf("%x", dataHash)

	id := strHash[:8] + strTimestamp[len(strTimestamp)-6:]
	return id
}
