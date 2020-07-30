package main

import (
	"log"
	"nosql-db/pkg/api"
	"nosql-db/pkg/db"
)

func main() {
	log.SetFlags(log.Lshortfile | log.Ltime)

	db.InitCollections()
	s := api.NewServer()
	s.Start()

}
