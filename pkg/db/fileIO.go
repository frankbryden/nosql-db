package db

import (
	"encoding/json"
	"errors"
	"io"
	"log"
	"nosql-db/pkg/datatypes"
	"nosql-db/pkg/util"
	"os"
	"strconv"
	"strings"
)

const dbFile = "mydb.db"

//JS represents a json object in go's primitives
type JS map[string]interface{}

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

func getJson(data string) JS {
	var dat JS
	if err := json.Unmarshal([]byte(data), &dat); err != nil {
		//TODO handle this more graciously. Namely, check if it is a
		//JSON formatting issue, and return error to user.
		//UPDATE: this will definitely be needed if, when deleting elements from the db,
		//this is only done in the index file, and not the attribute file. In that case,
		//lookups in the attribute file will give hits to IDs which no longer exist in the
		//index file.
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
	dbFile := getFile(fileName)
	indexFile := getFile(fileName + datatypes.IndexFileExtension)
	attributesFile := getFile(fileName + datatypes.AttributeFileExtension)
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

//getFile returns a file in R/W mode. Will create if it does not exist.
func getFile(filename string) *os.File {
	if util.FileExists(filename) {
		return openFile(filename)
	}
	file, err := os.Create(filename)
	if err != nil {
		log.Fatal(err)
	}
	file.Close()
	return openFile(filename)
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

//WriteToFile writes data to the end of the database file
func (db *Access) WriteToFile(data []byte) int {
	db.dbFile.Seek(0, 2)
	n, err := db.dbFile.Write(data)
	if err != nil {
		log.Fatal(err)
	}
	db.dbFile.Sync()
	return n
}

//Write data to the database. `data` is a raw JSON string
func (db *Access) Write(data string) (string, error) {
	dat := getJson(data)
	entryId := db.idGen.GetId(data)
	dat["id"] = entryId

	flattened := util.FlattenJSON(dat)
	log.Println(flattened)

	if jsonData, err := json.Marshal(dat); err != nil {
		log.Fatal(err)
		return "", err
	} else {

		log.Println("Writing at offset " + strconv.Itoa(db.getDbFilePos()))

		n := db.WriteToFile(jsonData)
		log.Println("Wrote " + strconv.Itoa(n) + " bytes")

		//Store information about entry. Will write this to the index file
		indexEntry := datatypes.NewIndexEntry(int64(db.getDbFilePos()-n), n, entryId)

		//Write to index file
		db.WriteIndex(indexEntry)

		//We now need to write to attributes file
		db.writeAttributes(flattened)

		//Increment the file offset by n (number of bytes written)
		db.dbFileOffset += n

		log.Println("Wrote " + string(jsonData))
	}

	return entryId, nil
}

//WriteIndex takes an IndexEntry and writes it to the index file
func (db *Access) WriteIndex(ie *datatypes.IndexEntry) {
	//Write to disk but ALSO to in-memory table
	db.indexFile.Write(ie.WriteableRepr())
	db.indexFile.Sync()

	//in-memory table
	db.indexTable.Insert(ie)
}

//DeleteIndex takes an IndexEntry and deletes it from the index file
func (db *Access) DeleteIndex(id string) {
	//Delete from disk but ALSO from in-memory table
	indexData, err := db.indexTable.Get(id)
	if err != nil {
		log.Fatal("Attempting to delete object with id " + id + " not in database")
	}
	tape := make([]byte, datatypes.IndexEntrySize)
	log.Printf("Writing %d bytes at offset %d", len(tape), indexData.Offset)
	db.indexFile.Seek(indexData.Offset, 0)
	db.indexFile.Write(make([]byte, datatypes.IndexEntrySize))
	db.indexFile.Sync()

	//in-memory table
	db.indexTable.Remove(id)
}

func (db *Access) writeAttributes(data JS) {
	log.Printf("%s, (%v)", "writeAttributes", data)
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
		//log.Printf("Wrote %d at %d (start = %d, len = %d)", currentKeyOffset, tailOffset, tailOffset-int64(len(offsetBytes))+1, len(offsetBytes))
	}

	db.attributesFile.Sync()

}

//Read from the database, filtering the data based on `data`
func (db *Access) Read(data string) ([]JS, error) {
	if len(data) == 0 {
		err := errors.New("Empty request")
		return nil, err
	}
	query := getJson(data)

	return db.retrieveFromQuery(query)
}

//Update all entries from the database which match the filter in `data`
func (db *Access) Update(data string) JS {
	js := make(JS)
	return js
}

//Delete all entries matching the filter in `data`
func (db *Access) Delete(data string) (JS, error) {
	if len(data) == 0 {
		err := errors.New("Empty request")
		return nil, err
	}
	query := getJson(data)

	toDelete, err := db.retrieveFromQuery(query)

	if err != nil {
		return nil, err
	}

	for _, item := range toDelete {
		db.DeleteIndex(item["id"].(string))
	}

	result := make(JS)
	result["deleteCount"] = len(toDelete)
	return result, nil
}

func (db *Access) retrieveFromQuery(query JS) ([]JS, error) {
	if id, ok := query["id"]; ok {
		if idStr, ok := id.(string); ok {
			log.Println("query for id " + idStr)
			object := db.getSingleObjectFromId(idStr)
			return []JS{object}, nil
		}
	} else {
		return db.getFilteredData(util.FlattenJSON(query)), nil
	}

	return []JS{JS{"Yeah": "hey"}}, nil
}

/*
getFilteredData works by applying a
Filter for each attribute: obtain as many lists as attributes in filter
Perform inner-join on lists
That is the final result of the query
UPDATE: better method, involving less disk-reading = better performance.
	-> acquire IDs oj objects for each attribute
	-> then perform inner-join
	-> Now fetch data based on remaining IDs
	-> now we can filter on the data with all attributes directly,
	   as we know each object contains all the requested attributes
*/
func (db *Access) getFilteredData(query JS) []JS {
	//In the case of an empty query `{}`, return all objects stored in db
	if len(query) == 0 {
		return db.getAllObjects()
	}
	//var filteredLists [][]JS

	//Will hold a mapping from attribute name -> list of IDs of objects containing that attribute
	//attributesIDs := make(map[string][]string)

	//Array of all IDs which contain at least one of the attributes in query.
	//Will be narrowed down to only IDs which contain ALL the attributes in query by subsequent inner-join
	var attributesIDs [][]string

	//fill above map
	for k := range query {
		attributesIDs = append(attributesIDs, db.getAllIdsFromAttributeName(k))
	}

	//Inner-join the map
	objectIDsWithFilterAttr := util.InnerJoin(attributesIDs)
	//After the above inner-join, every single object in objectIDsWithFilterAttr (well, the objects
	//referred to by the IDs) has every single attribute contained in the query

	return db.applyFilter(objectIDsWithFilterAttr, query)
}

//applyFilter gets objects from db based on `ids`, and only keeps objects whose attributes/values match
//those in `filter`
func (db *Access) applyFilter(ids []string, filter JS) []JS {
	log.Printf("%s, (%v, %v)", "applyFilter", ids, filter)
	//Every item in objects will have at least all the attributes in filter
	objects := db.getAllObjectsFromIds(ids)

	log.Println(objects)

	//TODO may need to rethink this, based on performance cost.
	//repeatedly appending is heavily inefficient in the worst-case scenario (filter selects all elements)
	var filteredObjects []JS

	for _, obj := range objects {
		//keeps track of wether current object matches filter
		match := true

		//TODO Using flattened is a neat hack which meant the Read aspect of nested-objects
		//was super easy to implement, but it will make updating impossible.
		//Updating will require traversal of the original object, so might as well use that here
		//and write the function now.
		flattened := util.FlattenJSON(obj)

		for key, value := range filter {
			//Check wether this is a nested query
			if strings.Contains(key, ".") {
				//TODO might be a bit hard, probably some notation or something we can use to do this
				//but we're gonna have to go through object, where nesting level depends on len(parts).
				//parts := strings.Split(key, ".")
			}
			if flattened[key] != value {
				match = false
				break
			}
		}
		if match {
			filteredObjects = append(filteredObjects, obj)
			log.Println("MATCH!")
		}

	}
	return filteredObjects

}

func (db *Access) getAllObjects() []JS {
	return db.getAllObjectsFromIds(db.indexTable.GetAllIds())
}

func (db *Access) getAllObjectsFromIds(ids []string) []JS {
	log.Println(ids)
	log.Printf("(len = %d)", len(ids)) //len = 2 here
	objects := make([]JS, len(ids))
	for i, id := range ids {
		objects[i] = db.getSingleObjectFromId(id)
	}
	return objects
}

func (db *Access) getSingleObjectFromId(id string) JS {
	log.Printf("looking up object with id = '%s'", id)
	indexData, err := db.indexTable.Get(id)
	if err != nil {
		//TODO we might want to change this to a less dramatic error handler
		//Looking at the func above, a query for 100IDs (for example) will fail
		//if a single item is missing. Not good.
		//UPDATE: lol indeed I just got to that situation.
		log.Panic(err)
	}
	log.Println("Found matching id at offset " + strconv.Itoa(int(indexData.Offset)))
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
	chunkSize := 256
	db.attributesFile.Seek(int64(len(attribute)), 0)
	for !reachedEnd {
		//Seek backwards by attribute length. Simple trick to avoid the issue where search item is missed because split
		//in half where next portion is fetched
		filePos, _ := db.attributesFile.Seek(-int64(len(attribute)), 1)
		data := make([]byte, chunkSize)
		n, err := db.attributesFile.Read(data)
		if err != nil {
			log.Println(err)
			reachedEnd = true
		}
		attrIndex := 0
		for i := 0; i < n; i++ {
			if data[i] == attrRaw[attrIndex] {
				if attrIndex == len(attrRaw)-1 {
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
		//return -1, ""
		if n < chunkSize {
			return -1, ""
		}
	}
	return -1, ""
}

//Takes an offset, and returns an id and offset to next item
func (db *Access) readSingleAttrItem(offset int64) (int64, string) {
	//The +1 comes from the middle separator (':')
	data := make([]byte, datatypes.IdLength+datatypes.LinkedListPointerSize+1)

	//skip first separator
	db.attributesFile.ReadAt(data, offset)

	//split along separator
	parts := strings.Split(string(data), ":")

	//extract data
	id := parts[0]
	pointer, err := strconv.Atoi(strings.Trim(parts[1], "\x00"))

	if err != nil {
		return -1, id
	}

	return int64(pointer), id
}

//getAllIdsFromAttributeName returns all ids of objects containign attrName
func (db *Access) getAllIdsFromAttributeName(attrName string) []string {
	log.Printf("%s, (%s)", "getAllIdsFromAttributeName", attrName)
	startOffset, id := db.findAttributeOffset(attrName)
	ids := db.getAllIdsFromAttributeOffset(startOffset)
	log.Printf("Got %d ids, but adding '%s'", len(ids), id)
	//TODO implement a better fix, one that takes into account what's actually
	//going on in the code
	if id == "" {
		return ids
	} else {
		return append(ids, id)
	}
}

//Inner function: exposed by two functions below.
func (db *Access) traverseAttributesLinkedList(startOffset int64, includeIds bool) (int64, []string) {

	var ids []string
	offset, id := db.readSingleAttrItem(startOffset)
	lastOffset := offset

	for offset > 0 {
		lastOffset = offset
		if includeIds {
			ids = append(ids, id)
		}
		offset, id = db.readSingleAttrItem(offset)
	}
	ids = append(ids, id)
	//lastOffset corresponds to the beginning of the entry.
	//As a result, needs to be shifted by ID_LEN + 1 (separator) + POINTER_SIZE
	//UPDATE: that did not work...I feel like something in that style is required though
	return lastOffset, ids
}

//Used for Querying data
func (db *Access) getAllIdsFromAttributeOffset(startOffset int64) []string {
	log.Printf("%s, (%d)", "getAllIdsFromAttributeOffset", startOffset)
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

//TODO return a bool with success
func (db *Access) deleteSingleItemFromId(id string) {

}
