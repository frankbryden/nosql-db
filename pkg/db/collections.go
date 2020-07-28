package db

import (
	"log"
	"nosql-db/pkg/datatypes"
	"nosql-db/pkg/util"
	"os"
)

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

//ListCollections returns a slice of Collections contained in the collections
//home path.
func ListCollections() []datatypes.Collection {
	return make([]datatypes.Collection, 3)
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
