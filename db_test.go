package main

import (
	"log"
	"nosql-db/pkg/api"
	"nosql-db/pkg/db"
	"os"
	"runtime/pprof"
	"testing"
)

func TestDbWrite(t *testing.T) {
	//Test Data
	collectionName := "personal"
	bodyStr := "{ \"name\": \"Jo\", \"lastname\": \"Walker\", \"age\": 53, \"brother\": { \"name\": \"Simon\", \"age\": 55, \"bike\": \"VTT\" } }"

	//Test setup
	f, err := os.Create("C:\\Users\\Frankie\\Desktop\\cpu.prof")
	if err != nil {
		log.Fatal("could not create CPU profile: ", err)
	}
	defer f.Close() // error handling omitted for example
	if err := pprof.StartCPUProfile(f); err != nil {
		log.Fatal("could not start CPU profile: ", err)
	}
	defer pprof.StopCPUProfile()

	//Actual testing
	db.InitCollections()
	s := api.NewServer()

	collection := s.MapCollection(collectionName)

	for i := 0; i < 100; i++ {
		collection.Db.Write(bodyStr)
	}

}

func TestDbRead(t *testing.T) {
	//Test Data
	collectionName := "personal"
	bodyStr := "{ \"brother\": { \"name\": \"Simon\" } }"

	//Test setup
	f, err := os.Create("C:\\Users\\Frankie\\Desktop\\cpu_read.prof")
	if err != nil {
		log.Fatal("could not create CPU profile: ", err)
	}
	defer f.Close() // error handling omitted for example
	if err := pprof.StartCPUProfile(f); err != nil {
		log.Fatal("could not start CPU profile: ", err)
	}
	defer pprof.StopCPUProfile()

	//Actual testing
	db.InitCollections()
	s := api.NewServer()

	collection := s.MapCollection(collectionName)

	for i := 0; i < 100; i++ {
		collection.Db.Read(bodyStr)
	}

}
