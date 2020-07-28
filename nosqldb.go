package main

import (
	"log"
	"nosql-db/pkg/api"
	"nosql-db/pkg/db"
)

func main() {
	log.SetFlags(log.Lshortfile | log.Ltime)

	db.InitCollections()
	dbAcc := db.NewAccess("my.db")
	s := api.NewServer(dbAcc)
	s.Start()

}
