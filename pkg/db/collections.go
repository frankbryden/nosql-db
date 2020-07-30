package db

import (
	"log"
	"nosql-db/pkg/util"
	"os"
	"strings"
)

//CollectionEntry represents the textual info surrounding a collection
type CollectionEntry struct {
	name string
	path string
}

//Collection represents a single database. This software can support multiple databases,
//or `Collections`
type Collection struct {
	entry CollectionEntry
	Db    *Access
}

//InitCollections creates the collections folder if it does not exist yet
func InitCollections() {
	homePath := GetCollectionsHomePath()
	log.Printf("Checking at %s...", homePath)
	if !util.FolderExists(homePath) {
		os.Mkdir(homePath, 0755)
		log.Printf("Created new directory at %s", homePath)
	} else {
		log.Println("Already exists.")
	}
}

//ListCollections returns a slice of collection entries
func ListCollections() []CollectionEntry {
	collectionsHomePath := GetCollectionsHomePath()
	f, err := os.Open(collectionsHomePath)
	if err != nil {
		log.Fatal(err)
	}
	dirnames, err := f.Readdirnames(0)
	entries := make([]CollectionEntry, len(dirnames))
	if err != nil {
		log.Fatal(err)
	}
	for i, dirname := range dirnames {
		parts := strings.Split(dirname, string(os.PathSeparator))
		collectionName := parts[len(parts)-1]
		entries[i] = CollectionEntry{
			name: collectionName,
			path: collectionsHomePath + string(os.PathSeparator) + dirname,
		}
		log.Printf("Found collection %s at %s", entries[i].name, entries[i].path)
	}
	return entries
}

//GetCollectionsHomePath returns the absolute path to the root directory of
//collections.
func GetCollectionsHomePath() string {
	path, err := os.UserHomeDir()
	if err != nil {
		log.Fatal(err)
	}
	return path + string(os.PathSeparator) + "nosqldbData"
}

//CreateCollection if it doesn't already exist.
//Returns the collecion if it was created, nil otherwise
func CreateCollection(name string) *Collection {
	homePath := GetCollectionsHomePath()
	collectionPath := homePath + string(os.PathSeparator) + name
	if !util.FolderExists(collectionPath) {
		os.Mkdir(collectionPath, 0755)
		log.Printf("Created collection at %s", collectionPath)
		collectionEntry := CollectionEntry{
			name: name,
			path: collectionPath,
		}
		return NewCollection(collectionEntry)
	}
	log.Printf("Collection %s already exists", name)
	return nil
}

//NewCollection creates a collection instance from a collection entry instance
func NewCollection(collectionEntry CollectionEntry) *Collection {
	access := NewAccess(collectionEntry)
	return &Collection{
		entry: collectionEntry,
		Db:    access,
	}
}

//LoadCollections returns a mapping from
//collection name to Collection object
func LoadCollections() map[string]Collection {
	collectionMap := make(map[string]Collection)
	for _, collectionEntry := range ListCollections() {
		collectionMap[collectionEntry.name] = *NewCollection(collectionEntry)
	}
	return collectionMap
}

//GetName returns name
func (e CollectionEntry) GetName() string {
	return e.name
}
