package main

import (
	"encoding/json"
	"nosql-db/pkg/util"
	"reflect"
	"testing"
)

func getJson(data string) map[string]interface{} {
	var dat map[string]interface{}
	if err := json.Unmarshal([]byte(data), &dat); err != nil {
		//TODO handle this more graciously. Namely, check if it is a
		//JSON formatting issue, and return error to user.
		//UPDATE: this will definitely be needed if, when deleting elements from the db,
		//this is only done in the index file, and not the attribute file. In that case,
		//lookups in the attribute file will give hits to IDs which no longer exist in the
		//index file.
		panic(err)
	}
	return dat
}

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

	jsData := getJson(data)
	jsFlattened := util.FlattenJSON(jsData)
	jsExpected := getJson(dataExpected)

	if !reflect.DeepEqual(jsFlattened, jsExpected) {
		t.Errorf("Expected %v, got %v", jsExpected, jsFlattened)
	}

}
