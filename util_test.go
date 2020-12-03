package main

import (
	"encoding/json"
	"log"
	"nosql-db/pkg/util"
	"reflect"
	"testing"
	"time"
)

func TestInnerJoin(t *testing.T) {
	s := make([][]string, 3)
	s[0] = []string{"hey", "hi", "yo", "yep", "yop", "Yo"}
	s[1] = []string{"hey", "hi", "salut", "yep", "ciao"}
	s[2] = []string{"hey", "hi", "bonjour", "bonsoir", "au revoir"}
	got := util.InnerJoin(s)
	expected := []string{"hey", "hi"}
	if !reflect.DeepEqual(got, expected) {
		t.Errorf("Expected %v, got %v", expected, got)
	}
}

func TestJSONFlatten(t *testing.T) {
	data := "{\"name\": \"Jo\",\"lastname\": \"Walker\",\"age\": 53, \"brother\": {\"name\": \"Simon\",\"age\": 55,\"bike\": \"VTT\"}}"
	dataExpected := "{\"name\": \"Jo\",\"lastname\": \"Walker\",\"age\": 53, \"brother.name\": \"Simon\",\"brother.age\": 55,\"brother.bike\": \"VTT\"}"

	jsData := util.GetJSON(data)
	jsFlattened := util.FlattenJSON(jsData)
	jsExpected := util.GetJSON(dataExpected)

	if !reflect.DeepEqual(jsFlattened, jsExpected) {
		t.Errorf("Expected %v, got %v", jsExpected, jsFlattened)
	}

}

func TestJSONMerge(t *testing.T) {
	log.SetFlags(log.Lshortfile | log.Ltime)
	input := util.GetJSON("{\"title\": \"Goodbye!\",\"author\" : {\"givenName\" : \"John\",\"familyName\" : \"Doe\"},\"tags\":[ \"example\", \"sample\" ],\"content\": \"This will be unchanged\"}")
	patch := util.GetJSON("{\"title\": \"Hello!\",\"phoneNumber\": \"+01-123-456-7890\",\"author\": {\"familyName\": null},\"tags\": [ \"example\" ]}")
	expected := util.GetJSON("{\"title\": \"Hello!\",\"author\" : {\"givenName\" : \"John\"},\"tags\": [ \"example\" ],\"content\": \"This will be unchanged\",\"phoneNumber\": \"+01-123-456-7890\"}")

	out := util.MergeRFC7396(input, patch)

	if !reflect.DeepEqual(out, expected) {
		expectedStr, _ := json.MarshalIndent(expected, "", "\t")
		outStr, _ := json.MarshalIndent(out, "", "\t")
		t.Errorf("Expected\n%s, got\n%s", expectedStr, outStr)
	}
}

func TestWorkerRun(t *testing.T) {
	a := 0
	worker := util.NewWorker(func() {
		log.Printf("Hello!")
		a++
	}, 2*time.Second)
	worker.Start()
	<-time.After(11 * time.Second)
	worker.Stop()

	if a != 17 {
		t.Errorf("A should be 2 after 2 iterations, instead we have a = %d", a)
	}
}
