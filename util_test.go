package main

import (
	"nosql-db/pkg/util"
	"reflect"
	"testing"
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
