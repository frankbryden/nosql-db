package api

import (
	"log"
	"net/http"
)

type SyncServer struct {
	requests chan RequestData
}

type RequestData struct {
	r    *http.Request
	resp http.ResponseWriter
	done chan int
}

func (s *SyncServer) ServeHTTP(resp http.ResponseWriter, r *http.Request) {
	log.Printf("%s request", r.Method)
	done := make(chan int)
	s.requests <- RequestData{
		r:    r,
		resp: resp,
		done: done,
	}
	log.Print("Waiting for channel to be cleared")
	<-done
	log.Print("Done")
}
