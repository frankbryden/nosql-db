package db

import (
	"crypto/md5"
	"encoding/hex"
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
	//Compute an ID from the data and the timestamp

	//Write data
	io.WriteString(ig.h, data)

	//Write timestamp
	timestamp := time.Now().UnixNano() / 100
	strTimestamp := fmt.Sprintf("%d", timestamp)
	io.WriteString(ig.h, strTimestamp)

	//Get hash
	dataHash := ig.h.Sum(nil)
	strHash := fmt.Sprintf("%x", dataHash)

	return strHash
}

//GetHash computes the md5 hash of the input string
func (ig *IdGen) GetHash(data string) string {
	algorithm := md5.New()
	algorithm.Write([]byte(data))
	return hex.EncodeToString(algorithm.Sum(nil))
}
