package datatypes

import (
	"errors"
	"log"
	"strconv"
	"strings"
)

//DBFileExtension is the file extension of the index file
const DBFileExtension = ".db"

//IndexFileExtension is the file extension of the index file
const IndexFileExtension = ".index"

//AttributeFileExtension is the file extension of the attribute file
const AttributeFileExtension = ".attr"

//IDLength is the size in bytes of a single ID in index/attr files
const IDLength = 32

//IndexEntrySize is the size in bytes of a single entry in the index file
const IndexEntrySize = IDLength + 20

//LinkedListPointerSize is the size in bytes of a single pointer (offset) in the attr file
const LinkedListPointerSize = 5

//LinkedListPointerOffset is the offset from beginning of entry to pointer location
const LinkedListPointerOffset = IDLength + LinkedListPointerSize

//IndexEntry represents an entry in the index file
type IndexEntry struct {
	//offset is in the db file, whereas indexFileOffset is in the index file
	offset          int64
	indexFileOffset int64
	size            int
	_id             string
}

//IndexTable is an in-memory copy of the index file
type IndexTable struct {
	table map[string]IndexData
}

//IndexData is the data associated with an IndexEntry
type IndexData struct {
	Offset, IndexFileOffset int64
	Size                    int
}

//AttributesEntry represents an entry in the attributes file
type AttributesEntry struct {
	name string
	ids  []string
}

//JS represents a json object in go's primitives
type JS map[string]interface{}

//WriteableRepr is a representation of an index entry as found in the index file
func (ie *IndexEntry) WriteableRepr() []byte {
	var builder strings.Builder

	log.Printf("Writeable repr with id: %s", ie._id)

	builder.WriteString(ie._id)
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
		Offset:          ie.offset,
		IndexFileOffset: ie.indexFileOffset,
		Size:            ie.size,
	}
}

//FromWriteableRepr constructs an IndexEntry instance from a segment of the index file.
//returns an error if non-nil error if segment is empty (null bytes)
func FromWriteableRepr(data string, indexFileOffset int64) (*IndexEntry, error) {
	parts := strings.Split(data, ":")
	//If the current segment is empty (previously deleted index entry)
	if len(parts) == 1 {
		return nil, errors.New("Empty index entry")
	}
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
		offset:          int64(offset),
		indexFileOffset: indexFileOffset,
		size:            size,
		_id:             parts[0],
	}, nil
}

//NewIndexEntry constructs an IndexEntry object from required parameters.
func NewIndexEntry(offset int64, indexFileOffset int64, size int, _id string) *IndexEntry {
	return &IndexEntry{
		offset:          offset,
		indexFileOffset: indexFileOffset,
		size:            size,
		_id:             _id,
	}
}

//SetIndexFileOffset used by function writing to index file, as IndexEntry object is
//created before the file offset is known
func (ie *IndexEntry) SetIndexFileOffset(indexFileOffset int64) {
	log.Printf("Setting indexFileOffset of object with id %s to %d", ie._id, indexFileOffset)
	ie.indexFileOffset = indexFileOffset
}

//GetOffset returns the offset to the start of the underlying object in the db file
func (ie *IndexEntry) GetOffset() int64 {
	return ie.offset
}

//GetSize returns the number of bytes the underlying object occupies in the db file
func (ie *IndexEntry) GetSize() int {
	return ie.size
}

//LoadTable from index file contents
func LoadTable(data string) *IndexTable {
	table := make(map[string]IndexData)
	for i := 0; i < len(data); i += IndexEntrySize {
		//Get raw data in the form: 3b0d2e8c691600:2064:48;
		rawData := data[i : i+IndexEntrySize]
		//Pass to parser and obtain IndexEntry object
		ie, err := FromWriteableRepr(rawData, int64(i))

		if err != nil {
			//this segment is empty, skip
			continue
		}
		//Insert into map
		table[ie._id] = ie.GetIndexData()
	}
	return &IndexTable{
		table: table,
	}
}

//Insert entry into index table
func (it *IndexTable) Insert(ie *IndexEntry) {
	it.table[ie._id] = ie.GetIndexData()
}

//Remove entry from index table
func (it *IndexTable) Remove(id string) {
	delete(it.table, id)
}

//Get returns an object with `id` representing an item in the index file
func (it *IndexTable) Get(_id string) (IndexData, error) {
	indexData, found := it.table[_id] //db.getOffsetFromId(idStr)

	if found {
		return indexData, nil
	} else {
		return indexData, errors.New(_id + " not found")
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
