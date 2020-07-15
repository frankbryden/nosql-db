package db

import (
	"encoding/json"
	"errors"
	"io"
	"log"
	"nosqldb/pkg/datatypes"
	"os"
	"strconv"
	"strings"
)

const dbFile = "mydb.db"

type Access struct {
	state          string
	dbFile         *os.File
	dbFileOffset   int
	indexFile      *os.File
	indexTable     *datatypes.IndexTable
	attributesFile *os.File
	idGen          *IdGen
}

func openFile(fileName string) *os.File {
	file, err := os.OpenFile(fileName, os.O_RDWR, 0777)
	if err != nil {
		log.Fatal(err)
		return nil
	}
	return file
}

func getJson(data string) map[string]interface{} {
	var dat map[string]interface{}
	if err := json.Unmarshal([]byte(data), &dat); err != nil {
		panic(err)
	}
	return dat
}

func getFileContents(f *os.File) string {
	fileContents := make([]byte, getFileSize(f))
	f.Read(fileContents)
	return string(fileContents)
}

func NewAccess(fileName string) *Access {
	dbFile := openFile(fileName)
	indexFile := openFile(fileName + ".index")
	attributesFile := openFile(fileName + ".attr")
	return &Access{
		state:          "ready",
		dbFile:         dbFile,
		dbFileOffset:   getFileSize(dbFile),
		indexFile:      indexFile,
		indexTable:     datatypes.LoadTable(getFileContents(indexFile)),
		attributesFile: attributesFile,
		idGen:          NewIdGen(),
	}
}

func getFilePos(f *os.File) int64 {
	offset, err := f.Seek(0, io.SeekCurrent)
	if err != nil {
		log.Fatal(err)
	}
	return offset
}

func (db *Access) getDbFilePos() int {
	offset, err := db.dbFile.Seek(0, io.SeekCurrent)
	if err != nil {
		log.Fatal(err)
	}
	return int(offset)
}

func getFileSize(f *os.File) int {
	info, err := f.Stat()
	if err != nil {
		log.Fatal(err)
	}
	return int(info.Size())
}

func (db *Access) WriteToFile(data []byte) int {
	db.dbFile.Seek(0, 2)
	n, err := db.dbFile.Write(data)
	if err != nil {
		log.Fatal(err)
	}
	db.dbFile.Sync()
	return n
}

func (db *Access) Write(data string) (error, string) {
	//I guess it makes sense to convert string -> JSON, to write JSON to file
	//but as file takes string in the end, we'd be doing string -> JSON -> string
	//so we'll cheat for now and just write the string directly
	/*var dat map[string]interface{}

	byt := []byte(`{"name":"simon","age":55}`)


	fmt.Println(dat)
	for k, v := range dat {
		fmt.Print("k " + k + ", v ")
		fmt.Println(v)
	}*/
	dat := getJson(data)
	entryId := db.idGen.GetId(data)
	dat["id"] = entryId

	if jsonData, err := json.Marshal(dat); err != nil {
		log.Fatal(err)
		return err, ""
	} else {

		log.Println("Writing at offset " + strconv.Itoa(db.getDbFilePos()))

		n := db.WriteToFile(jsonData)
		log.Println("Wrote " + strconv.Itoa(n) + " bytes")

		//Store information about entry. Will write this to the index file
		indexEntry := datatypes.NewIndexEntry(db.getDbFilePos()-n, n, entryId)

		//Write to index file
		db.WriteIndex(indexEntry)

		//We now need to write to attributes file
		db.writeAttributes(dat)

		//Increment the file offset by n (number of bytes written)
		db.dbFileOffset += n

		log.Println("Wrote " + string(jsonData))
	}

	return nil, entryId
}

func (db *Access) WriteIndex(ie *datatypes.IndexEntry) {
	//Write to disk but ALSO to in-memory table
	db.indexFile.Write(ie.WriteableRepr())
	db.indexFile.Sync()

	//in-memory table
	db.indexTable.Insert(ie)
}

func (db *Access) writeAttributes(data map[string]interface{}) {
	id := data["id"].(string)
	delete(data, "id")
	//get offset of start of attribute chain (if exists)
	for k := range data {
		log.Println("writing key " + k)
		db.writeAttribute("/"+k, id)
	}
}

func (db *Access) writeAttribute(key string, id string) {
	startOffset, _ := db.findAttributeOffset(key)

	//zeroes needed for offset placeholder (linkedlist pointer)
	zeroes := make([]byte, datatypes.LinkedListPointerSize)

	//If startOffset is -1, this attribute has never been seen previously.
	//As such, we need to write the HEAD of the linked list.
	if startOffset < 0 {
		log.Println(key + " not found. Writing HEAD...")
		//Write key:ID:\0\0\0\0
		offset, _ := db.attributesFile.Seek(0, 2)
		db.attributesFile.WriteString(key + ":" + id + ":")
		db.attributesFile.Write(zeroes)
		endOffset, _ := db.attributesFile.Seek(0, 1)
		log.Printf("Wrote from byte %d to byte %d", offset, endOffset)
		log.Println("Wrote " + key)
	} else {
		//Tail offset points to the first null byte after the separator (':')
		//For example, 3b0d2e8c691600:\0\0\0\0
		//  offset would point here    ^
		//We are going to have to write the offset to the item we're about to write.
		//Let's write the item, get its offset, then write the offset at pos=tailOffset
		tailOffset := db.getAttrListTailOffset(startOffset)
		if tailOffset < 0 {
			log.Printf("HEAD is alone, starting at %d", startOffset)
			//BUG FIX here! the startOffset is right at the beginning of attr entry.
			//As such, we need to shift by ID + and LLPointerSize (end of offset space)
			tailOffset = startOffset + datatypes.LinkedListPointerOffset
		} else {
			tailOffset += datatypes.LinkedListPointerOffset
		}

		//Write ID:\0\0\0\0
		db.attributesFile.Seek(0, 2)
		currentKeyOffset := getFilePos(db.attributesFile)
		db.attributesFile.WriteString(id + ":")
		db.attributesFile.Write(zeroes)

		//Write offset
		offsetBytes := []byte(strconv.Itoa(int(currentKeyOffset)))
		db.attributesFile.WriteAt(offsetBytes, tailOffset-int64(len(offsetBytes))+1)
		log.Printf("Wrote %d at %d (start = %d, len = %d)", currentKeyOffset, tailOffset, tailOffset-int64(len(offsetBytes))+1, len(offsetBytes))
	}

	db.attributesFile.Sync()

}

func (db *Access) Read(data string) ([]map[string]interface{}, error) {
	if len(data) == 0 {
		err := errors.New("Empty request")
		return nil, err
	}
	query := getJson(data)

	if id, ok := query["id"]; ok {
		if idStr, ok := id.(string); ok {
			log.Println("query for id " + idStr)
			object := db.getSingleObjectFromId(idStr)
			return []map[string]interface{}{object}, nil
		}
	} else {
		return db.getFilteredData(query), nil
	}
	return []map[string]interface{}{getJson("{\"success\": \"read successful\"}")}, nil
}

/*
getFilteredData works by applying a
Filter for each attribute: obtain as many lists as attributes in filter
Perform inner-join on lists
That is the final result of the query
*/
func (db *Access) getFilteredData(query map[string]interface{}) []map[string]interface{} {
	var filteredLists [][]map[string]interface{}
	for k, v := range query {
		filteredLists = append(filteredLists, db.applySingleFilter(k, v))
	}
	return filteredLists[0]
}

func (db *Access) applySingleFilter(key string, value interface{}) []map[string]interface{} {
	//List of ids of objects containing attribute `key`
	ids := db.getAllIdsFromAttributeName(key)

	//List of objects which contain attribute `key`
	objectsWithAttr := db.getAllObjectsFromIds(ids)

	//TODO may need to rethink this, based on performance cost.
	//repeatedly appending is heavily inefficient in the worst-case scenario (filter selects all elements)
	var filteredObjects []map[string]interface{}

	for _, obj := range objectsWithAttr {
		if obj[key] == value {
			log.Println("MATCH!")
			filteredObjects = append(filteredObjects, obj)
		} else {
			log.Println("No match")
		}
		log.Println(obj[key])
		log.Println(value)
	}
	return filteredObjects
}

func (db *Access) getAllObjectsFromIds(ids []string) []map[string]interface{} {
	objects := make([]map[string]interface{}, len(ids))
	for i, id := range ids {
		objects[i] = db.getSingleObjectFromId(id)
	}
	return objects
}

func (db *Access) getSingleObjectFromId(id string) map[string]interface{} {
	indexData, err := db.indexTable.Get(id)
	if err != nil {
		//TODO we might want to change this to a less dramatic error handler
		//Looking at the func above, a query for 100IDs (for example) will fail
		//if a single item is missing. Not good.
		log.Panic(err)
	}
	log.Println("Found matching id at offset " + strconv.Itoa(indexData.Offset))
	dbData := db.readDbData(&indexData)
	return getJson(dbData)
}

func (db *Access) getOffsetFromId(id string) int {
	endPos := getFileSize(db.indexFile)
	defer db.indexFile.Seek(0, endPos)

	//Start from beginning
	db.indexFile.Seek(0, 0)

	currentId := ""
	for currentId != id {
		currentId = db.getNextIdFromIndexFile()
		break
	}
	return 150
}

func (db *Access) getNextIdFromIndexFile() string {
	//Loop over the file, skipping by EntryIndexSize bytes every time,
	//and looking at the first IdLength bytes (to compare IDs)

	data := make([]byte, getFileSize(db.indexFile))
	db.indexFile.Read(data)
	curIndex := 0
	//startIndex := 0
	for rune(data[curIndex]) != ';' {
		curIndex++
	}

	//indexEntry := 4
	log.Println("Found a ; at " + strconv.Itoa(curIndex))
	return "id"
}

//returns offset, id (of first item in attribute list)
func (db *Access) findAttributeOffset(attribute string) (int64, string) {
	// The attribute file will be organised in a linked list
	// For example,
	// name:{id}:29
	// age:{id}:34
	// {id}:46
	// etc. Initially, attrbutes are set with n null bytes (where n will depend on how many items we're storing)
	// To find all IDs, simply find first instance of attribute, then traverse the singly linked list
	reachedEnd := false
	attrRaw := []byte(attribute)
	db.attributesFile.Seek(int64(len(attribute)), 0)
	for !reachedEnd {
		//Seek backwards by attribute length. Simple trick to avoid the issue where search item is missed because split
		//in half where next portion is fetched
		filePos, _ := db.attributesFile.Seek(-int64(len(attribute)), 1)
		log.Println("Starting at pos=" + strconv.Itoa(int(filePos)))
		data := make([]byte, 256)
		n, err := db.attributesFile.Read(data)
		log.Println("Read " + strconv.Itoa(n) + " bytes")
		if err != nil {
			log.Println(err)
			reachedEnd = true
		}
		attrIndex := 0
		for i := 0; i < n; i++ {
			if data[i] == attrRaw[attrIndex] {
				if attrIndex == len(attrRaw)-1 {
					log.Println("Found " + attribute + " at pos = " + strconv.Itoa(int(filePos)))

					//We then extract the two pieces of information
					//as per above, in the format `name:{id}:29`

					/*
						Two reads looks better, but slower. One read, then split in memory. Faster.
							id := make([]byte, datatypes.IdLength)
							pointer := make([]byte, datatypes.LinkedListPointerSize)
					*/

					//Skip first separator (we are on final character of attribute name, skip it, and skip first ':', hence +2)
					offset, id := db.readSingleAttrItem(filePos + 2)

					//TODO because of the following code segment, a positive offset will be returned.
					//As a result, readSingleAttrItem will have to be called again to figure out that the
					//current offset points to the tail of the attr.
					//call chain can be reduced by a full cycle by avoiding this
					if offset > 0 {
						log.Printf("Head has nodes after, starting at offset %d")
						return offset, id
					} else {
						return filePos + 2, "" //filePos + datatypes.IdLength + datatypes.LinkedListPointerSize + 2, ""
					}
				}
				attrIndex++
			} else {
				/*log.Println("Following 2 not equal")
				log.Println(data[i])
				log.Println(attrRaw[attrIndex])*/
				attrIndex = 0
			}
			filePos++
		}
		return -1, ""
	}
	return -1, ""
}

//Takes an offset, and returns an id and offset to next item
func (db *Access) readSingleAttrItem(offset int64) (int64, string) {
	//The +1 comes from the middle separator (':')
	data := make([]byte, datatypes.IdLength+datatypes.LinkedListPointerSize+1)

	//skip first separator
	log.Printf("Reading %d bytes at offset %d", len(data), offset)
	db.attributesFile.ReadAt(data, offset)

	//split along separator
	parts := strings.Split(string(data), ":")

	log.Println(data)
	log.Println(parts)
	log.Println(len(parts))

	for i := 0; i < len(parts); i++ {
		log.Printf("%d -> %s", i, parts[i])
	}

	//extract data
	id := parts[0]
	pointer, err := strconv.Atoi(strings.Trim(parts[1], "\x00"))

	if err != nil {
		log.Println(err)
		return -1, id
	}

	return int64(pointer), id
}

//getAllIdsFromAttributeName returns all ids of objects containign attrName
func (db *Access) getAllIdsFromAttributeName(attrName string) []string {
	startOffset, id := db.findAttributeOffset(attrName)
	ids := db.getAllIdsFromAttributeOffset(startOffset)
	return append(ids, id)
}

//Inner function: exposed by two functions below.
func (db *Access) traverseAttributesLinkedList(startOffset int64, includeIds bool) (int64, []string) {

	var ids []string
	offset, id := db.readSingleAttrItem(startOffset)
	lastOffset := offset

	for offset > 0 {
		lastOffset = offset
		log.Println(offset)
		if includeIds {
			ids = append(ids, id)
		}
		offset, id = db.readSingleAttrItem(offset)
	}
	ids = append(ids, id)
	log.Printf("offset = %d, lastOffset = %d", offset, lastOffset)
	//lastOffset corresponds to the beginning of the entry.
	//As a result, needs to be shifted by ID_LEN + 1 (separator) + POINTER_SIZE
	//UPDATE: that did not work...I feel like something in that style is required though
	return lastOffset, ids
}

//Used for Querying data
func (db *Access) getAllIdsFromAttributeOffset(startOffset int64) []string {
	_, ids := db.traverseAttributesLinkedList(startOffset, true)
	return ids
}

//Used for Writing data
func (db *Access) getAttrListTailOffset(startOffset int64) int64 {
	offset, _ := db.traverseAttributesLinkedList(startOffset, false)
	return offset
}

func (db *Access) readDbData(indexData *datatypes.IndexData) string {
	data := make([]byte, indexData.Size)
	db.dbFile.ReadAt(data, int64(indexData.Offset))
	return string(data)
}
