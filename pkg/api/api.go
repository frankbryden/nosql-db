package api

import (
	"encoding/json"
	"errors"
	"io/ioutil"
	"log"
	"net/http"
	"nosql-db/pkg/datatypes"
	"nosql-db/pkg/db"
	"nosql-db/pkg/util"
	"strings"
)

//Server is capable of handling API requests
type Server struct {
	collectionsMapping map[string]db.Collection
	requests           chan RequestData
	requestHandler     SyncServer
}

//NewServer constructs a Server instance
func NewServer() *Server {
	requests := make(chan RequestData)
	return &Server{
		collectionsMapping: db.LoadCollections(),
		requests:           requests,
		requestHandler: SyncServer{
			requests: requests,
		},
	}
}

//Start the server
func (s *Server) Start() {
	http.Handle("/", &s.requestHandler)
	go s.ProcessRequestQueue()
	log.Fatal(http.ListenAndServe(":9999", nil))
}

//Stop the server
func (s *Server) Stop() {
	log.Print("Closing requests channel and shutting down server...")
	close(s.requests)
	log.Print("Done")
}

func getBodyStr(resp http.ResponseWriter, r *http.Request) string {
	body, err := ioutil.ReadAll(r.Body)

	if err != nil {
		resp.Write([]byte("could not ready body"))
		return ""
	}

	return string(body)
}

//CollectionsListReq replies with list of collections
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

//CreateCollectionReq serves collection creation requests
func (s *Server) CreateCollectionReq(resp http.ResponseWriter, r *http.Request) {
	bodyStr := getBodyStr(resp, r)

	js := util.GetJSON(bodyStr)
	collectionName, ok := js["name"].(string)

	errMsg := ""
	//double check received name is indeed a string
	if !ok {
		errMsg = "'name' is not of type string"
	} else {
		createdCollection := db.CreateCollection(collectionName)
		if createdCollection != nil {
			s.collectionsMapping[collectionName] = *createdCollection
		}
	}

	if errMsg != "" {
		resp.Write([]byte("{\"error\":\"" + errMsg + "\"}"))
	} else {
		resp.WriteHeader(http.StatusNoContent)
	}
}

//WriteReq serves database write requests in a specified collection
func (s *Server) WriteReq(collectionName string, resp http.ResponseWriter, r *http.Request) {
	bodyStr := getBodyStr(resp, r)
	var err error
	var id string
	if collection, ok := s.collectionsMapping[collectionName]; ok {
		id, err = collection.Db.Write(bodyStr)
	} else {
		err = errors.New("no collection named '" + collectionName + "'")
	}

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

//ReadReq serves database write requests in a specified collection
func (s *Server) ReadReq(collectionName string, resp http.ResponseWriter, r *http.Request) {
	bodyStr := getBodyStr(resp, r)

	var err error
	var objects []datatypes.JS
	if collection, ok := s.collectionsMapping[collectionName]; ok {
		objects, err = collection.Db.Read(bodyStr)
	} else {
		err = errors.New("no collection named '" + collectionName + "'")
	}

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

//UpdateReq serves requests on the delete endpoint/resource
func (s *Server) UpdateReq(collectionName, id string, resp http.ResponseWriter, r *http.Request) {
	errMsg := ""

	if r.Method != http.MethodPatch {
		errMsg = "Only PATCH is supported at this endpoint"
	} else {
		bodyStr := getBodyStr(resp, r)

		var err error
		var result datatypes.JS
		if collection, ok := s.collectionsMapping[collectionName]; ok {
			result, err = collection.Db.Update(id, bodyStr)
		} else {
			err = errors.New("no collection named '" + collectionName + "'")
		}

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
	}

	if errMsg != "" {
		resp.Write([]byte("{\"error\":\"" + errMsg + "\"}"))
	}
}

//ProcessRequestQueue goes through the request queue, passing the requests to ServeRequests, one by one
func (s *Server) ProcessRequestQueue() {
	log.Print("Now listening")
	for rd := range s.requests {
		s.ServeRequests(rd.resp, rd.r)
		rd.done <- 1
	}
}

func (s *Server) ServeRequests(resp http.ResponseWriter, r *http.Request) {
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
	case "shutdown":
		s.Stop()
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
			default:
				log.Printf("Assuming %s is an ID", split[3])
				s.UpdateReq(collectionName, split[3], resp, r)
			}
		} else {
			switch r.Method {
			case http.MethodGet:
				s.CollectionsListReq(resp, r)
				break
			case http.MethodPost:
				s.CreateCollectionReq(resp, r)
				break
			}

			break
		}

	default:
		log.Printf("'%s' did not match any path", split[0])
	}
}

//MapCollection maps a collectionName to a collection object
func (s *Server) MapCollection(collectionName string) db.Collection {
	return s.collectionsMapping[collectionName]
}
