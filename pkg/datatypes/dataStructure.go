package datatypes

import (
	"errors"
	"log"
	"strconv"
	"strings"
)

//IndexFileExtension is the file extension of the index file
const IndexFileExtension = ".index"

//AttributeFileExtension is the file extension of the attribute file
const AttributeFileExtension = ".attr"

//IndexEntrySize is the size in bytes of a single entry in the index file
const IndexEntrySize = 30

//IdLength is the size in bytes of a single ID in index/attr files
const IdLength = 14

//LinkedListPointerSize is the size in bytes of a single pointer (offset) in the attr file
const LinkedListPointerSize = 5

//LinkedListPointerOffset is the offset from beginning of entry to pointer location
const LinkedListPointerOffset = IdLength + LinkedListPointerSize

//IndexEntry represents an entry in the index file
type IndexEntry struct {
	offset int64
	size   int
	id     string
}

//IndexTable is an in-memory copy of the index file
type IndexTable struct {
	table map[string]IndexData
}

//IndexData is the data associated with an IndexEntry
type IndexData struct {
	Offset int64
	Size   int
}

//AttributesEntry represents an entry in the attributes file
type AttributesEntry struct {
	name string
	ids  []string
}

//WriteableRepr is a representation of an index entry as found in the index file
func (ie *IndexEntry) WriteableRepr() []byte {
	var builder strings.Builder

	builder.WriteString(ie.id)
	builder.WriteString(":")
	builder.WriteString(strconv.Itoa(int(ie.offset)))
	builder.WriteString(":")
	builder.WriteString(strconv.Itoa(ie.size))

	//Pad the final section with zeroes so each entry is the same length
	desiredLen := IndexEntrySize - builder.Len()
	for i := 0; i < desiredLen-1; i++ {
		builder.WriteByte(0)
	}

	builder.WriteString(";")

	return []byte(builder.String())
}

func (ie *IndexEntry) GetIndexData() IndexData {
	return IndexData{
		Offset: ie.offset,
		Size:   ie.size,
	}
}

func FromWriteableRepr(data string) *IndexEntry {
	parts := strings.Split(data, ":")
	offset, offsetErr := strconv.Atoi(parts[1])
	//Trailing zeroes (used for padding) need to be removed
	size, sizeErr := strconv.Atoi(strings.Trim(parts[2][:len(parts[2])-1], "\x00"))
	if offsetErr != nil || sizeErr != nil {
		var errorMsg string
		if offsetErr != nil {
			errorMsg = offsetErr.Error()
		} else {
			errorMsg = sizeErr.Error()
		}
		log.Fatal("Invalid Data:" + errorMsg)
	}
	return &IndexEntry{
		offset: int64(offset),
		size:   size,
		id:     parts[0],
	}
}

func NewIndexEntry(offset int64, size int, id string) *IndexEntry {
	return &IndexEntry{
		offset: offset,
		size:   size,
		id:     id,
	}
}

func LoadTable(data string) *IndexTable {
	table := make(map[string]IndexData)
	for i := 0; i < len(data); i += IndexEntrySize {
		//Get raw data in the form: 3b0d2e8c691600:2064:48;
		rawData := data[i : i+IndexEntrySize]
		//Pass to parser and obtain IndexEntry object
		ie := FromWriteableRepr(rawData)
		//Insert into map
		table[ie.id] = ie.GetIndexData()
	}
	return &IndexTable{
		table: table,
	}
}

//Insert entry into index table
func (it *IndexTable) Insert(ie *IndexEntry) {
	it.table[ie.id] = ie.GetIndexData()
}

//Remove entry from index table
func (it *IndexTable) Remove(id string) {
	delete(it.table, id)
}

//Get returns an object with `id` representing an item in the index file
func (it *IndexTable) Get(id string) (IndexData, error) {
	indexData, found := it.table[id] //db.getOffsetFromId(idStr)

	if found {
		log.Println(found)
		return indexData, nil
	} else {
		return indexData, errors.New(id + " not found")
	}
}

//GetAllIds returns a list containing every single ID present in the DB
func (it *IndexTable) GetAllIds() []string {
	keys := make([]string, len(it.table))

	i := 0
	for k := range it.table {
		keys[i] = k
		i++
	}
	return keys
}
