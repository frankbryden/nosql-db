package db

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"nosql-db/pkg/datatypes"
	"nosql-db/pkg/util"
	"os"
	"strconv"
	"strings"
)

const dbFile = "mydb.db"

//Access the underlying db with common CRUD operations
type Access struct {
	state       string
	fileHandles *FileHandles
	indexTable  *datatypes.IndexTable
	idGen       *IdGen
}

//FileHandles to underlying database files
type FileHandles struct {
	dbFile         *os.File
	indexFile      *os.File
	attributesFile *os.File
}

func openFile(fileName string) *os.File {
	file, err := os.OpenFile(fileName, os.O_RDWR, 0777)
	if err != nil {
		log.Fatal(err)
		return nil
	}
	return file
}

func getFileContents(f *os.File) string {
	fileContents := make([]byte, getFileSize(f))
	f.Read(fileContents)
	return string(fileContents)
}

//NewAccess constructs an Access instance from a db name
func NewAccess(collectionEntry CollectionEntry) *Access {
	fileHandles := NewFileHandles(collectionEntry)
	return &Access{
		state:       "ready",
		fileHandles: fileHandles,
		indexTable:  datatypes.LoadTable(getFileContents(fileHandles.indexFile)),
		idGen:       NewIDGen(),
	}
}

//NewFileHandles constructs a FileHandles instance from a db name
func NewFileHandles(collectionEntry CollectionEntry) *FileHandles {
	path := collectionEntry.path + string(os.PathSeparator) + collectionEntry.name
	dbFile := getFile(path + datatypes.DBFileExtension)
	indexFile := getFile(path + datatypes.IndexFileExtension)
	attributesFile := getFile(path + datatypes.AttributeFileExtension)
	return &FileHandles{
		dbFile:         dbFile,
		indexFile:      indexFile,
		attributesFile: attributesFile,
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
	offset, err := db.fileHandles.dbFile.Seek(0, io.SeekCurrent)
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
	db.fileHandles.dbFile.Seek(0, 2)
	n, err := db.fileHandles.dbFile.Write(data)
	if err != nil {
		log.Fatal(err)
	}
	db.fileHandles.dbFile.Sync()
	return n
}

//Write data to the database. `data` is a raw JSON string
func (db *Access) Write(data string) (string, error) {
	dat := util.GetJSON(data)
	entryID := db.idGen.GetID(data)
	//if this is a fresh object, give it an ID and write the new entry to the index file.
	//if not, use the old ID and write the index file entry using the Update of IndexFile
	var freshObject bool
	if _, ok := dat["id"]; ok {
		freshObject = false
	} else {
		freshObject = true
	}
	if freshObject {
		dat["id"] = entryID
	} else {
		entryID = dat["id"].(string)
	}
	_id := db.idGen.GetHash(entryID)
	dat["_id"] = _id

	log.Printf("_id = %s", dat["_id"])

	flattened := util.FlattenJSON(dat)

	delete(dat, "_id")

	jsonData, err := json.Marshal(dat)

	if err != nil {
		log.Fatal(err)
		return "Invalid JSON object", err
	}

	log.Println("Writing at offset " + strconv.Itoa(db.getDbFilePos()))

	n := db.WriteToFile(jsonData)
	log.Println("Wrote " + strconv.Itoa(n) + " bytes")

	//If we are updating an object, then update the entry in the index file. For that, get its offset in the
	//offset file.
	indexFileOffset := int64(-1)
	if !freshObject {
		indexData, err := db.indexTable.Get(entryID)
		if err != nil {
			//this means it is an id defined by the user. Write this document as if it were new i.e. at the end of
			//the db file
		} else {
			indexFileOffset = indexData.IndexFileOffset
		}
	}
	//Store information about entry. Will write this to the index file
	indexEntry := datatypes.NewIndexEntry(int64(db.getDbFilePos()-n), indexFileOffset, n, _id)

	log.Printf("Writing indexentry %v", indexEntry)

	//Write to index file
	db.WriteIndex(indexEntry)

	//We now need to write to attributes file
	db.writeAttributes(flattened)

	log.Println("Wrote " + string(jsonData))

	return entryID, nil
}

//WriteIndex takes an IndexEntry and writes it to the index file
//returns offset of write start
func (db *Access) WriteIndex(ie *datatypes.IndexEntry) int64 {
	//TODO fix this func with appropriate seeking based on ie.
	//Also, need to update map and not insert when ID already there (update).
	log.Printf("ie object: %v", ie)
	//Get write start (value to be returned)
	var offset int64
	if ie.GetIndexData().IndexFileOffset == -1 {
		offset, _ = db.fileHandles.indexFile.Seek(0, 2)
	} else {
		offset, _ = db.fileHandles.indexFile.Seek(ie.GetIndexData().IndexFileOffset, 0)
	}

	//Write to disk but ALSO to in-memory table
	db.fileHandles.indexFile.Write(ie.WriteableRepr())
	db.fileHandles.indexFile.Sync()

	//Update indexEntry object with obtained offset
	ie.SetIndexFileOffset(offset)

	//in-memory table
	db.indexTable.Insert(ie)

	return offset
}

//DeleteIndex takes an IndexEntry and deletes it from the index file
func (db *Access) DeleteIndex(id string) {
	//Delete from disk but ALSO from in-memory table
	indexData, err := db.indexTable.Get(id)
	if err != nil {
		log.Fatal("Attempting to delete object with id " + id + " not in database")
	}
	tape := make([]byte, datatypes.IndexEntrySize)
	log.Printf("Writing %d bytes at offset %d", len(tape), indexData.IndexFileOffset)
	db.fileHandles.indexFile.Seek(indexData.IndexFileOffset, 0)
	db.fileHandles.indexFile.Write(make([]byte, datatypes.IndexEntrySize))
	db.fileHandles.indexFile.Sync()

	//in-memory table
	db.indexTable.Remove(id)
}

//DeleteFromDBFile deletes an entry from the db file given an IndexEntry
func (db *Access) DeleteFromDBFile(id *datatypes.IndexData) error {
	db.fileHandles.dbFile.Seek(id.Offset, 0)
	db.fileHandles.dbFile.Write(make([]byte, id.Size))
	db.fileHandles.indexFile.Sync()

	return nil
}

func (db *Access) writeAttributes(data datatypes.JS) {
	log.Printf("%s, (%v)", "writeAttributes", data)
	_id := data["_id"].(string)
	delete(data, "id")
	delete(data, "_id")
	//get offset of start of attribute chain (if exists)
	for k := range data {
		log.Println("writing key " + k)
		db.writeAttribute("/"+k, _id)
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
		offset, _ := db.fileHandles.attributesFile.Seek(0, 2)
		db.fileHandles.attributesFile.WriteString(key + ":" + id + ":")
		db.fileHandles.attributesFile.Write(zeroes)
		endOffset, _ := db.fileHandles.attributesFile.Seek(0, 1)
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
		db.fileHandles.attributesFile.Seek(0, 2)
		currentKeyOffset := getFilePos(db.fileHandles.attributesFile)
		db.fileHandles.attributesFile.WriteString(id + ":")
		db.fileHandles.attributesFile.Write(zeroes)

		//Write offset
		offsetBytes := []byte(strconv.Itoa(int(currentKeyOffset)))
		db.fileHandles.attributesFile.WriteAt(offsetBytes, tailOffset-int64(len(offsetBytes))+1)
		//log.Printf("Wrote %d at %d (start = %d, len = %d)", currentKeyOffset, tailOffset, tailOffset-int64(len(offsetBytes))+1, len(offsetBytes))
	}

	db.fileHandles.attributesFile.Sync()

}

//Read from the database, filtering the data based on `data`
func (db *Access) Read(data string) ([]datatypes.JS, error) {
	if len(data) == 0 {
		err := errors.New("Empty request")
		return nil, err
	}
	query := util.GetJSON(data)

	return db.retrieveFromQuery(query)
}

//Update entry with id=`id` from the databas
func (db *Access) Update(id, data string) (datatypes.JS, error) {
	idQuery := fmt.Sprintf("{\"id\":\"%s\"}", id)
	objects, e := db.Read(idQuery)
	if e != nil {
		log.Printf("Object with id %s not found", id)
		return nil, fmt.Errorf("Object with id %s not found", id)
	}

	if len(objects) > 1 {
		log.Printf("Ambiguous query matches more than one record: %v", objects)
		return nil, fmt.Errorf("Ambiguous query matches more than one record: %v", objects)
	}

	object := objects[0]

	patchObj := util.GetJSON(data)
	updated := util.MergeRFC7396(object, patchObj)
	updatedStr, _ := json.MarshalIndent(updated, "", "\t")
	updatedRawBytes, _ := json.Marshal(updated)
	log.Printf("After update we have\n%s", updatedStr)

	//For now, we'll delete the original object and write the new one as a new entry.
	//Later, we'll overwrite the new object over the original if the lengths match (probably a fairly uncommon case)
	//Even later, we'll see if we can come up with clever techniques for optimising file space.
	//UPDATE: Ah. Something I hadn't thought of. We want to preserve the id, meaning it's not a simple case of delete+write
	//the updated object, as that gives it a fresh new ID. Instead, we need a new method who's job is specifically that:
	//   - Write an updated object to the db file
	//   - DO NOT write a new entry to the index file; instead, update the entry with the ID of the old object
	//UPDATE 2: Do not delete. Deleting simply removes the corresponding index entry - leaving the db file unchanged.
	//The second step of the update is overwriting the said index entry, meaning it must not be deleted.
	//UPDATE 3: Ok we need to delete from db file. The issue is querying by attributes other than ID ignore the IndexFile,
	//meaning the info that the object has been deleted is lost.
	//UPDATE 4: No we do not. The searching by attributes is done through the attribute file, meaning it's from there.
	//For now, we simply remove duplicate IDs.

	//Write
	_, err := db.Write(string(updatedRawBytes))
	if err != nil {
		return patchObj, err
	}
	//Return final object.
	return patchObj, nil
}

//Delete all entries matching the filter in `data`
func (db *Access) Delete(data string) (datatypes.JS, error) {
	if len(data) == 0 {
		err := errors.New("Empty request")
		return nil, err
	}
	query := util.GetJSON(data)

	toDelete, err := db.retrieveFromQuery(query)

	if err != nil {
		return nil, err
	}

	for _, item := range toDelete {
		objectID := item["id"].(string)
		id, err := db.indexTable.Get(objectID)
		if err != nil {
			return nil, err
		}
		db.DeleteFromDBFile(&id)
		db.DeleteIndex(item["id"].(string))
	}

	result := make(datatypes.JS)
	result["deleteCount"] = len(toDelete)
	return result, nil
}

func (db *Access) retrieveFromQuery(query datatypes.JS) ([]datatypes.JS, error) {
	if id, ok := query["id"]; ok {
		if idStr, ok := id.(string); ok {
			//Obtain internal _id from "user-space" id
			_id := db.idGen.GetHash(idStr)
			log.Printf("query for id %s (true id is %s)", _id, idStr)
			jsObj, err := db.getSingleObjectFromID(_id)
			if err != nil {
				//obj no longer exists
				return nil, errors.New("Object does not exist")
			}
			object := jsObj
			return []datatypes.JS{object}, nil
		}
	} else {
		return db.getFilteredData(util.FlattenJSON(query)), nil
	}

	return []datatypes.JS{datatypes.JS{"Yeah": "hey"}}, nil
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
func (db *Access) getFilteredData(query datatypes.JS) []datatypes.JS {
	//In the case of an empty query `{}`, return all objects stored in db
	if len(query) == 0 {
		return db.getAllObjects()
	}
	//var filteredLists [][]datatypes.JS

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

	//filter out duplicates (necassry, atm anyways, as we do not remove entries from the attributes file, meaning
	//reading by attribute will return duplicates if objects have been deleted/updated)
	uniqueObjectIDs := util.UniqueIDs(objectIDsWithFilterAttr)

	return db.applyFilter(uniqueObjectIDs, query)
}

//applyFilter gets objects from db based on `ids`, and only keeps objects whose attributes/values match
//those in `filter`
func (db *Access) applyFilter(ids []string, filter datatypes.JS) []datatypes.JS {
	log.Printf("%s, (%v, %v)", "applyFilter", ids, filter)
	//Every item in objects will have at least all the attributes in filter
	objects := db.getAllObjectsFromIds(ids)

	log.Println(objects)

	//TODO may need to rethink this, based on performance cost.
	//repeatedly appending is heavily inefficient in the worst-case scenario (filter selects all elements)
	var filteredObjects []datatypes.JS

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

func (db *Access) getAllObjects() []datatypes.JS {
	return db.getAllObjectsFromIds(db.indexTable.GetAllIds())
}

func (db *Access) getAllObjectsFromIds(ids []string) []datatypes.JS {
	log.Println(ids)
	log.Printf("(len = %d)", len(ids)) //len = 2 here
	objects := make([]datatypes.JS, len(ids))
	for i, id := range ids {
		jsObj, err := db.getSingleObjectFromID(id)
		if err != nil {
			//obj no longer exists
			continue
		}
		objects[i] = jsObj
	}
	return objects
}

//getSingleObjectFromID returns JS instance from db given `id`
func (db *Access) getSingleObjectFromID(id string) (datatypes.JS, error) {
	indexData, err := db.indexTable.Get(id)
	if err != nil {
		//TODO we might want to change this to a less dramatic error handler
		//Looking at the func above, a query for 100IDs (for example) will fail
		//if a single item is missing. Not good.
		//UPDATE: lol indeed I just got to that situation.
		//UPDATE: okkk deletion implemented, time to fix this.
		return nil, errors.New("Object deleted or non-existent")
	}
	log.Println("Found matching id at offset " + strconv.Itoa(int(indexData.Offset)))
	dbData := db.readDbData(&indexData)
	return util.GetJSON(dbData), nil
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
	db.fileHandles.attributesFile.Seek(int64(len(attribute)), 0)
	for !reachedEnd {
		//Seek backwards by attribute length. Simple trick to avoid the issue where search item is missed because split
		//in half where next portion is fetched
		filePos, _ := db.fileHandles.attributesFile.Seek(-int64(len(attribute)), 1)
		data := make([]byte, chunkSize)
		n, err := db.fileHandles.attributesFile.Read(data)
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
							id := make([]byte, datatypes.IDLength)
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
					}
					return filePos + 2, ""
				}
				attrIndex++
			} else {
				attrIndex = 0
			}
			filePos++
		}
		if n < chunkSize {
			return -1, ""
		}
	}
	return -1, ""
}

//Takes an offset, and returns an id and offset to next item
func (db *Access) readSingleAttrItem(offset int64) (int64, string) {
	//The +1 comes from the middle separator (':')
	data := make([]byte, datatypes.IDLength+datatypes.LinkedListPointerSize+1)

	//skip first separator
	db.fileHandles.attributesFile.ReadAt(data, offset)

	//split along separator
	parts := strings.Split(string(data), ":")

	//empty file or non-existent data
	if len(parts) < 2 {
		return -1, ""
	}

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
	}
	return append(ids, id)
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
	db.fileHandles.dbFile.ReadAt(data, int64(indexData.Offset))
	return string(data)
}
