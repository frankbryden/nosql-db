package db

import (
	"crypto/md5"
	"fmt"
	"hash"
	"io"
	"time"
)

type IdGen struct {
	h hash.Hash
}

func NewIdGen() *IdGen {
	return &IdGen{
		h: md5.New(),
	}
}

func (ig *IdGen) GetId(data string) string {
	ig.h.Reset()
	io.WriteString(ig.h, data)
	dataHash := ig.h.Sum(nil)
	timestamp := time.Now().UnixNano() / 100

	strTimestamp := fmt.Sprintf("%d", timestamp)
	strHash := fmt.Sprintf("%x", dataHash)

	id := strHash[:8] + strTimestamp[len(strTimestamp)-6:]
	return id
}
