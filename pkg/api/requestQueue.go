package api

import (
	"log"
	"net/http"
)

//SyncServer is responsible of merging requests from multiple goroutines into a single-threaded queue
type SyncServer struct {
	requests chan RequestData
}

//RequestData will be pased to the single-threaded request handler by the SyncServer
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
	<-done
}
