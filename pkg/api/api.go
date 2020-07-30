package api

import (
	"encoding/json"
	"io/ioutil"
	"log"
	"net/http"
	"nosql-db/pkg/db"
	"strings"
)

type Server struct {
	collectionsMapping map[string]db.Collection
}

func NewServer() *Server {
	return &Server{
		collectionsMapping: db.LoadCollections(),
	}
}

func (s *Server) Start() {
	http.Handle("/", s)
	log.Fatal(http.ListenAndServe("127.0.0.1:80", nil))
}

func getBodyStr(resp http.ResponseWriter, r *http.Request) string {
	body, err := ioutil.ReadAll(r.Body)

	if err != nil {
		resp.Write([]byte("could not ready body"))
		return ""
	}

	return string(body)
}

func (s *Server) CollectionsListReq(resp http.ResponseWriter, r *http.Request) {
	collectionEntries := db.ListCollections()
	entryNames := make([]string, len(collectionEntries))
	for i, entry := range collectionEntries {
		entryNames[i] = entry.GetName()
	}
	errMsg := ""
	if jsonBody, jsonErr := json.Marshal(entryNames); jsonErr == nil {
		resp.Write(jsonBody)
	} else {
		errMsg = jsonErr.Error()
	}

	if errMsg != "" {
		resp.Write([]byte("{\"error\":\"" + errMsg + "\"}"))
	}
}

func (s *Server) WriteReq(collectionName string, resp http.ResponseWriter, r *http.Request) {
	bodyStr := getBodyStr(resp, r)

	id, err := s.collectionsMapping[collectionName].Db.Write(bodyStr)
	//var errMsg string
	errMsg := ""
	responseBody := make(map[string]string)
	if err == nil {
		responseBody["id"] = id
		if jsonBody, jsonErr := json.Marshal(responseBody); jsonErr == nil {
			resp.Write(jsonBody)
		} else {
			errMsg = err.Error()
		}
	} else {
		errMsg = err.Error()
	}

	if errMsg != "" {
		resp.Write([]byte("{\"error\":\"" + errMsg + "\"}"))
	}

}

func (s *Server) ReadReq(collectionName string, resp http.ResponseWriter, r *http.Request) {
	bodyStr := getBodyStr(resp, r)

	objects, err := s.collectionsMapping[collectionName].Db.Read(bodyStr)

	errMsg := ""

	if err == nil {
		/*rawBody, err := json.RawMessage(objects).MarshalJSON()
		if err != nil {
			errMsg = err.Error()
		} else {
			responseBody["objects"] = string(rawBody)
			log.Println("Raw Body followed by the string() version")
			log.Println(rawBody)
			log.Println(string(rawBody))
		}*/
		if len(objects) == 0 {
			resp.Write([]byte("{}"))
		} else if jsonBody, jsonErr := json.Marshal(objects); jsonErr == nil {
			log.Println(objects)
			resp.Write(jsonBody)
		} else {
			errMsg = err.Error()
		}

	} else {
		errMsg = err.Error()
	}

	if errMsg != "" {
		resp.Write([]byte("{\"error\":\"" + errMsg + "\"}"))
	}
}

//DeleteReq serves requests on the delete endpoint/resource
func (s *Server) DeleteReq(collectionName string, resp http.ResponseWriter, r *http.Request) {
	bodyStr := getBodyStr(resp, r)

	result, err := s.collectionsMapping[collectionName].Db.Delete(bodyStr)

	errMsg := ""

	if err == nil {
		/*rawBody, err := json.RawMessage(objects).MarshalJSON()
		if err != nil {
			errMsg = err.Error()
		} else {
			responseBody["objects"] = string(rawBody)
			log.Println("Raw Body followed by the string() version")
			log.Println(rawBody)
			log.Println(string(rawBody))
		}*/
		if jsonBody, jsonErr := json.Marshal(result); jsonErr == nil {
			log.Println(result)
			resp.Write(jsonBody)
		} else {
			errMsg = err.Error()
		}

	} else {
		errMsg = err.Error()
	}

	if errMsg != "" {
		resp.Write([]byte("{\"error\":\"" + errMsg + "\"}"))
	}
}

func (s *Server) ServeHTTP(resp http.ResponseWriter, r *http.Request) {
	// Immediate solution, not the prettiest:
	// 1. Split incoming path by /
	// 2. branch by if-branches, where each nested if is a step further in the array generated by the split
	//in the future, have a map (with nested structure) containing a mapping from path to method reference

	log.Println(r.Method + " at " + r.URL.EscapedPath())
	resp.Header().Set("Content-Type", "application/json")

	escaped := r.URL.EscapedPath()
	split := strings.Split(escaped, "/")
	switch split[1] {
	case "status":
		resp.Write([]byte("running"))
		break
	case "version":
		resp.Write([]byte("1.0"))
		break
	case "collections":
		if len(split) > 2 {
			collectionName := split[2]
			switch split[3] {
			case "create":
				s.WriteReq(collectionName, resp, r)
				break
			case "read":
				s.ReadReq(collectionName, resp, r)
				break
			case "delete":
				s.DeleteReq(collectionName, resp, r)
				break
			}
		} else {
			s.CollectionsListReq(resp, r)
			break
		}

	default:
		log.Printf("'%s' did not match any path", split[0])
	}
}
