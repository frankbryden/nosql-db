package main

import (
	"fmt"
	"log"
	"nosql-db/pkg/api"
	"nosql-db/pkg/db"
)

func main() {
	/*f, err := os.OpenFile("hey.db", os.O_RDWR, 0777)
	if err != nil {
		log.Fatal(err)
	}
	data := make([]byte, 20)
	log.Println(f.Read(data))
	log.Println(string(data))
	f.Seek(0, 2)
	f.WriteString("hey")
	f.Sync()

	log.Println(f.Read(data))
	log.Println(string(data))
	f.Sync()
	f.Close()*/
	log.SetFlags(log.Lshortfile | log.Ltime)

	fmt.Println("hey there world")
	dbAcc := db.NewAccess("my.db")
	s := api.NewServer(dbAcc)
	s.Start()

}
